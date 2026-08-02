//! The clustered admin front (issue #9, Ch. 4): the thin listener that owns the
//! public admin address when `--cluster` is on.
//!
//! Upstream's `AdminApiServer` builds its router privately — there is no public
//! router or middleware seam (verified at v0.15.0) — so interception happens a
//! listener earlier instead: the core admin binds loopback, this front binds the
//! public address, **terminates** the config-mutating routes into
//! [`ControlOp`]s on the Raft log, and reverse-proxies everything else to the
//! core admin byte-for-byte. With `--cluster` off this module is never
//! constructed and the core admin binds the public address itself — the parity
//! bar (#37) rides on that path having zero new code.
//!
//! What terminates here is exactly the *replicated-config* surface: imposter
//! create/replace/delete, stub CRUD, and enable/disable (config since
//! upstream #817 — a pause must survive restarts and converge fleet-wide,
//! #15). Runtime-state mutations (scenarios, spaces, recorded-request
//! deletes) stay proxied to the local engine — node-local today by design,
//! tracked by #16.
//!
//! Mutation responses are rendered by re-reading the just-applied state through
//! the loopback admin (`GET /imposters/:port` after the barrier), so the body
//! shape is upstream's own — no parallel projection code to drift.
//!
//! Concurrency (#46, extended to route tables by #210): a single-imposter write
//! may carry an `If-Match` header naming the revision it expects — either the
//! exact `Rift-Cluster-Revision` token (`default:<port>@<revision>`) or a bare
//! revision integer. A route-table write (`PUT /front-door/routes`, `DELETE
//! /front-door/routes/{id}`) may carry the *portless* form
//! (`default@<revision>`), which `GET /front-door/routes` answers so a client
//! has something to condition on; a tenant whose table was never written reads
//! as revision `0`. The route-table revision is per tenant, not per route: a
//! `PUT` replaces the set as a unit and a `DELETE` stamps the same revision, so
//! either invalidates an outstanding precondition. Absent, a
//! write stays last-writer-wins (the pre-#46 default, unchanged): index-
//! addressed and list-replace stub edits are a read-modify-write of this
//! node's applied state committed as a full `PutImposter`, so two concurrent
//! writers to the same imposter clobber each other and a lagging follower can
//! base its write on stale state. A stale or mismatched `If-Match` refuses
//! with `409 resource conflict`; a collection-wide mutation (`PUT
//! /imposters`, `DELETE /imposters`) cannot carry one — there is no single
//! record to condition on — and answers `400 bad data` instead. The
//! precondition is evaluated inside the state machine's `apply` (not here),
//! so it holds even when the write lands on a follower and forwards to the
//! leader. Residual window: the precondition guards the *revision*, not this
//! node's read basis — an index-addressed edit conditioned on the current
//! revision but accepted by a node whose applied state still lags that
//! revision synthesizes its `PutImposter` from the stale local read and
//! passes the check. The default `ready-nodes` write barrier keeps that
//! window to the barrier timeout; route conditioned index edits to the
//! leader (or use by-id edits, which carry only the edited stub) when that
//! matters. A keyed retry (same `Idempotency-Key`) of a `409` dedups to that
//! same `409` by design — rebase and retry with a fresh key. Mixed-version
//! caveat: a pre-#46 replica ignores `expected_revision` and applies
//! unconditionally, so don't send `If-Match` until every node in the fleet
//! has upgraded.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use http_body_util::{BodyExt, Full, Limited, combinators::BoxBody};
use hyper::body::{Bytes, Incoming};
use hyper::header::{HeaderName, HeaderValue};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rand::RngCore;
use rift_cluster::audit_export::ExportStatus;
use rift_cluster::control::Role;
use rift_cluster::control::{
    self, ControlOp, ControlRequest, PreconditionTarget, StubEdit, StubEditScript,
};
use rift_cluster::decorate::{
    HEADER_BIND_FAILURES, HEADER_OP_ID, HEADER_REVISION, HEADER_WARNINGS,
};
use rift_cluster::{
    ControlOutcome, ControlResponse, FLEET_SCOPE, NodeError, RaftNode, SESSION_KEY_BYTES,
    SessionKey, TenantId,
};
use rift_cluster_base::seams::{
    ErrorKind, ImposterConfig, RiftScriptConfig, RouteTable, SCOPE_HEADER, ScriptBaseDir, Stub,
    classify as classify_upstream, config_uses_script_surface, error_response_typed,
    resolve_scripts, resolve_stub_scripts, validate_stub, validate_stubs,
};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::authz::{self, Action, Decision, Denial};
use crate::cli::WriteBarrier;
#[cfg(feature = "console")]
use crate::console;
use crate::fleet;
use crate::openapi;
use crate::principal;
use crate::readiness::Readiness;
use crate::session;
use crate::tenancy;

/// Largest admin request body the front accepts on a terminated route. The
/// proxied path streams and is not subject to this.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// How long a terminated write may take to commit (forwarding included) before
/// the client gets the `timeout` error shape. Distinct from the barrier
/// timeout, which begins after the commit and degrades to a warning.
const WRITE_DEADLINE: Duration = Duration::from_secs(10);

type FrontBody = BoxBody<Bytes, hyper::Error>;

/// Everything the front needs besides the node itself.
pub struct FrontConfig {
    /// The public admin address to bind (what the operator pointed clients at);
    /// a `host:port` string because the core CLI accepts hostnames.
    pub public_addr: String,
    /// The loopback address the core admin actually bound.
    pub upstream_admin: SocketAddr,
    /// The admin API key, when one is configured. Maps to a synthetic
    /// principal bound `TenantAdmin` on `default` (RFC-002 §3.4) — checked
    /// against every request, terminated or proxied, by this front's own
    /// RBAC gate (issue #161).
    pub api_key: Option<String>,
    /// Also bind the legacy API key's synthetic principal `FleetAdmin` on the
    /// fleet scope (`--cluster-legacy-key-is-fleet-admin`). Defaults to true
    /// for this release — see `docs/rift-cluster-server.md`'s migration schedule.
    pub legacy_key_is_fleet_admin: bool,
    /// Whether `--allowInjection` is on. Terminated writes are gated on the
    /// same classifier the core admin applies before storing.
    pub allow_injection: bool,
    /// Resolution base for `_rift.script` `file:` refs on terminated writes
    /// (upstream #356); absent ⇒ any `file:` ref is refused.
    pub scripts_dir: Option<PathBuf>,
    pub barrier: WriteBarrier,
    pub barrier_timeout: Duration,
    /// `--cluster-admin-async`: answer 202 + op id right after parking, and
    /// let the submit run in the background.
    pub admin_async: bool,
    /// This node's audit-exporter status (issue #164), for
    /// `GET /admin/audit/sink`. `None` when the composition root never spawned
    /// an `AuditExporter` — the route still answers, just without a status
    /// attached, the same way it does for a follower (see
    /// `tenancy::AuditSinkView`'s doc).
    pub export_status: Option<Arc<ExportStatus>>,
    /// This node's startup-readiness latch, threaded through so `/_fleet/health` (RFC-006 §5.2,
    /// issue #185) can report the same state `/readyz` does without a second latch to keep in
    /// sync.
    pub readiness: Arc<Readiness>,
}

/// A bound, serving admin front.
pub struct AdminFront {
    local_addr: SocketAddr,
    task: JoinHandle<()>,
    /// Cancelled by `shutdown` BEFORE it aborts, so the drop guard can tell an
    /// expected ending from a death.
    ///
    /// Known limit: a panic landing in the window between this being cancelled
    /// and the abort taking effect is classified as the requested stop, so
    /// `wait` answers `Ok`. It is still logged — `shutdown` inspects the
    /// `JoinError` itself and reports a non-cancelled one — and it can only
    /// happen while the process is already tearing down on purpose, so it
    /// cannot produce the live-but-deaf node this seam exists to catch.
    shutdown_requested: CancellationToken,
    /// Fired once the accept loop's outcome has been published.
    done: CancellationToken,
    /// The first `wait` caller takes the error; later callers get `Ok(())`
    /// (`anyhow::Error` is not `Clone`).
    outcome: Arc<Mutex<Option<anyhow::Result<()>>>>,
}

impl AdminFront {
    /// The address actually bound (resolves an ephemeral `:0` request).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stop serving and release the port.
    pub async fn shutdown(self) {
        self.shutdown_requested.cancel();
        self.task.abort();
        if let Err(e) = self.task.await
            && !e.is_cancelled()
        {
            tracing::error!(error = %e, "admin front accept task ended abnormally");
        }
        // Defensive: an abort that kills the task before the drop guard
        // publishes must not strand a waiter.
        self.done.cancel();
    }

    /// Resolves when the accept loop stops. `Err` means it died without anyone
    /// asking it to; a requested shutdown resolves `Ok(())`.
    ///
    /// Takes `&self`, not `self`: `serve_until` races this and must still own
    /// the front afterwards so the graceful leave can shut it down.
    ///
    /// The error goes to the first caller only (`anyhow::Error` is not
    /// `Clone`); later callers get `Ok(())`.
    pub async fn wait(&self) -> anyhow::Result<()> {
        self.done.cancelled().await;
        // Recovered, not asserted, for the same reason the drop guard recovers:
        // this seam exists to turn a dead accept loop into an error a caller can
        // act on, so it must never become a panic in that caller instead.
        self.outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or(Ok(()))
    }

    #[cfg(test)]
    pub(crate) fn abort_without_shutdown(&self) {
        self.task.abort();
    }
}

/// Releases `AdminFront::wait` callers however the accept-loop task ends —
/// normal exit (never happens by design), panic unwind, or `shutdown`'s abort.
///
/// The accept loop backs off and retries forever on systemic accept failure,
/// by design — it does not exit normally. That leaves this guard as the sole
/// publisher of an outcome, mirroring the upstream `ReleaseWaiters` idiom in
/// `rift-http-proxy::admin_api::server`.
struct ReleaseWaiters {
    done: CancellationToken,
    outcome: Arc<Mutex<Option<anyhow::Result<()>>>>,
    shutdown_requested: CancellationToken,
}

impl Drop for ReleaseWaiters {
    fn drop(&mut self) {
        // Recover from a poisoned lock rather than panicking: this runs during
        // unwind, where a second panic would abort the process.
        let mut slot = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() && !self.shutdown_requested.is_cancelled() {
            *slot = Some(Err(anyhow::anyhow!(
                "admin front accept loop terminated unexpectedly"
            )));
        }
        drop(slot);
        self.done.cancel();
    }
}

/// Per-request context, shared by clone into each connection task.
struct FrontState {
    /// `Weak` for the same reason as [`crate::cluster_api::NodeSlot`]: the node
    /// must never be kept alive by the surfaces that serve it.
    node: Weak<RaftNode>,
    upstream_admin: SocketAddr,
    api_key: Option<String>,
    legacy_key_is_fleet_admin: bool,
    allow_injection: bool,
    scripts_dir: Option<PathBuf>,
    barrier: WriteBarrier,
    barrier_timeout: Duration,
    admin_async: bool,
    export_status: Option<Arc<ExportStatus>>,
    readiness: Arc<Readiness>,
    /// Streams proxied requests through unchanged (SSE included).
    proxy: Client<hyper_util::client::legacy::connect::HttpConnector, Incoming>,
    /// Issues the internal re-reads mutation responses are rendered from.
    fetch: Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
}

/// Bind the public admin address and start serving.
pub async fn bind(config: FrontConfig, node: &Arc<RaftNode>) -> std::io::Result<AdminFront> {
    let listener = TcpListener::bind(config.public_addr.as_str()).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(
        %local_addr,
        upstream = %config.upstream_admin,
        "clustered admin front listening (config mutations replicate, the rest proxies)"
    );

    let state = Arc::new(FrontState {
        node: Arc::downgrade(node),
        upstream_admin: config.upstream_admin,
        api_key: config.api_key,
        legacy_key_is_fleet_admin: config.legacy_key_is_fleet_admin,
        allow_injection: config.allow_injection,
        scripts_dir: config.scripts_dir,
        barrier: config.barrier,
        barrier_timeout: config.barrier_timeout,
        admin_async: config.admin_async,
        export_status: config.export_status,
        readiness: config.readiness,
        proxy: Client::builder(TokioExecutor::new()).build_http(),
        fetch: Client::builder(TokioExecutor::new()).build_http(),
    });

    let shutdown_requested = CancellationToken::new();
    let done = CancellationToken::new();
    let outcome: Arc<Mutex<Option<anyhow::Result<()>>>> = Arc::new(Mutex::new(None));
    // Built here, *before* the spawn, and moved into the task. `tokio::spawn`
    // only queues a future, so one aborted before its first poll is dropped
    // without a line of its body running — a guard constructed inside would
    // never exist, and the death `wait` reports would be lost in silence. A
    // captured value lives in the future's initial state instead, so dropping
    // it unpolled still runs this `Drop`.
    let release = ReleaseWaiters {
        done: done.clone(),
        outcome: Arc::clone(&outcome),
        shutdown_requested: shutdown_requested.clone(),
    };

    let task = tokio::spawn(async move {
        let _release = release;
        // Same accept-loop shape as the probe listener: back off on systemic
        // accept failure, reap finished connections, never orphan a task.
        let mut backoff = Duration::from_millis(1);
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let accepted = tokio::select! {
                Some(joined) = connections.join_next(), if !connections.is_empty() => {
                    if let Err(e) = joined
                        && e.is_panic()
                    {
                        tracing::error!(error = %e, "admin front connection task panicked");
                    }
                    continue;
                }
                accepted = listener.accept() => accepted,
            };
            let stream = match accepted {
                Ok((stream, _)) => {
                    backoff = Duration::from_millis(1);
                    stream
                }
                Err(e) => {
                    tracing::warn!(error = %e, ?backoff, "admin front accept failed; backing off");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                    continue;
                }
            };
            let state = Arc::clone(&state);
            connections.spawn(async move {
                let service = service_fn(move |req| {
                    let state = Arc::clone(&state);
                    async move { Ok::<_, std::convert::Infallible>(handle(state, req).await) }
                });
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    // Routine for dropped keep-alives; the client saw its
                    // responses or its own error either way.
                    tracing::debug!(error = %e, "admin front connection ended with an error");
                }
            });
        }
    });

    Ok(AdminFront {
        local_addr,
        task,
        shutdown_requested,
        done,
        outcome,
    })
}

/// The config-mutating routes the front terminates. Everything else proxies.
///
/// The test-only `EnumDiscriminants` derive exists for the route-parity gate (issue #184): it lets
/// `openapi::parity` prove its representative list covers every variant. Without it the compiler
/// forces a `contract_route` arm for a new variant but not an entry in the list the parity test
/// actually compares, so a new route could ship undocumented with every test still green.
#[derive(Debug)]
#[cfg_attr(test, derive(strum::EnumDiscriminants))]
#[cfg_attr(test, strum_discriminants(derive(strum::EnumIter, Ord, PartialOrd)))]
pub(crate) enum Terminated {
    Create,
    ReplaceAllImposters,
    DeleteAllImposters,
    DeleteImposter(u16),
    AddStub(u16),
    ReplaceStubs(u16),
    ReplaceStubAt(u16, usize),
    DeleteStubAt(u16, usize),
    ReplaceStubById(u16, String),
    DeleteStubById(u16, String),
    SetEnabled(u16, bool),
    /// Whole-table replace of the front door's route table (issue #131).
    /// There is no upstream `/front-door/routes` to proxy to (U-11's admin
    /// CRUD was deferred), so this is provided here, not there.
    PutRoutes,
    DeleteRoute(String),
    /// RFC-002 §5's tenancy admin surface (issue #162). Every one of these
    /// **terminates**, reads included — there is no upstream `/admin/tenants`
    /// to proxy to, exactly as with `GET /front-door/routes`.
    Tenancy(tenancy::Route),
}

/// The tenant a terminated route is authorized against, when the route names
/// one itself.
///
/// `None` means "use `X-Rift-Tenant`", which is right for every resource route:
/// the header selects which of the caller's bindings they are acting under.
/// It is wrong for the tenancy surface, where the tenant is a **path segment**
/// naming the record being administered. Authorizing `/admin/tenants/b/...`
/// against the header would let a `TenantAdmin` of `a` administer `b` by
/// sending one header — the confused-deputy shape RFC-002 §8.1 exists to
/// close, reached through the one surface where the header is not the subject.
/// The imposter port this request addresses, if it addresses exactly one.
///
/// Feeds the ownership gate in [`authorize_action`]'s `Allow` arm (issue #182). `None` means there
/// is no single port to check ownership of, which is the right answer for three different reasons:
///
/// - **`Create`** — the port is in the body, not the route, and the imposter does not exist yet.
///   A create that collides with another tenant's port is refused by the state machine's own
///   `port_claimed_by_another_tenant` check, which is where fleet-unique ports (RFC-002 §3.2) are
///   actually enforced.
/// - **Set-level imposter ops** (`ReplaceAllImposters`, `DeleteAllImposters`) — these act on the
///   caller's own tenant's set, which `build_mutation` already scopes by the resolved tenant.
/// - **Routes and the tenancy surface** — not port-addressed at all, and both are read through
///   tenant-arg paths that honour the tenant.
fn addressed_port(kind: &Terminated) -> Option<u16> {
    match kind {
        Terminated::DeleteImposter(port)
        | Terminated::AddStub(port)
        | Terminated::ReplaceStubs(port)
        | Terminated::ReplaceStubAt(port, _)
        | Terminated::DeleteStubAt(port, _)
        | Terminated::ReplaceStubById(port, _)
        | Terminated::DeleteStubById(port, _)
        | Terminated::SetEnabled(port, _) => Some(*port),
        Terminated::Create
        | Terminated::ReplaceAllImposters
        | Terminated::DeleteAllImposters
        | Terminated::PutRoutes
        | Terminated::DeleteRoute(_)
        | Terminated::Tenancy(_) => None,
    }
}

fn scope_for(kind: &Terminated) -> Option<TenantId> {
    match kind {
        Terminated::Tenancy(route) => route.scope(),
        Terminated::Create
        | Terminated::ReplaceAllImposters
        | Terminated::DeleteAllImposters
        | Terminated::DeleteImposter(_)
        | Terminated::AddStub(_)
        | Terminated::ReplaceStubs(_)
        | Terminated::ReplaceStubAt(_, _)
        | Terminated::DeleteStubAt(_, _)
        | Terminated::ReplaceStubById(_, _)
        | Terminated::DeleteStubById(_, _)
        | Terminated::SetEnabled(_, _)
        | Terminated::PutRoutes
        | Terminated::DeleteRoute(_) => None,
    }
}

pub(crate) fn classify(method: &Method, path: &str, query: Option<&str>) -> Option<Terminated> {
    // The tenancy surface first: it is EE-only and terminates in full, so it
    // must never fall through to the imposter classifiers or the proxy.
    if let Some(route) = tenancy::classify(method, path, query) {
        return Some(Terminated::Tenancy(route));
    }
    if path == "/front-door/routes" {
        // `GET` is a read, not a mutation — it terminates in `handle` directly
        // rather than through this (write-only) classifier.
        return match *method {
            Method::PUT => Some(Terminated::PutRoutes),
            _ => None,
        };
    }
    if let Some(id) = path.strip_prefix("/front-door/routes/") {
        return match *method {
            Method::DELETE if !id.is_empty() => Some(Terminated::DeleteRoute(id.to_owned())),
            _ => None,
        };
    }
    if path == "/imposters" {
        return match *method {
            Method::POST => Some(Terminated::Create),
            Method::PUT => Some(Terminated::ReplaceAllImposters),
            Method::DELETE => Some(Terminated::DeleteAllImposters),
            _ => None,
        };
    }
    let rest = path.strip_prefix("/imposters/")?;
    let segments: Vec<&str> = rest.split('/').collect();
    let port: u16 = segments.first()?.parse().ok()?;
    match segments.as_slice() {
        [_] if *method == Method::DELETE => Some(Terminated::DeleteImposter(port)),
        [_, "enable"] if *method == Method::POST => Some(Terminated::SetEnabled(port, true)),
        [_, "disable"] if *method == Method::POST => Some(Terminated::SetEnabled(port, false)),
        [_, "stubs"] => match *method {
            Method::POST => Some(Terminated::AddStub(port)),
            Method::PUT => Some(Terminated::ReplaceStubs(port)),
            _ => None,
        },
        [_, "stubs", "by-id", id] if !id.is_empty() => match *method {
            Method::PUT => Some(Terminated::ReplaceStubById(port, (*id).to_owned())),
            Method::DELETE => Some(Terminated::DeleteStubById(port, (*id).to_owned())),
            _ => None,
        },
        [_, "stubs", index] => {
            let index: usize = index.parse().ok()?;
            match *method {
                Method::PUT => Some(Terminated::ReplaceStubAt(port, index)),
                Method::DELETE => Some(Terminated::DeleteStubAt(port, index)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Map a terminated route to the action that authorizes it (RFC-002 §4.1).
///
/// Exhaustive with **no wildcard arm**, on purpose (issue #161's explicit
/// acceptance criterion): a [`Terminated`] variant added without a line here
/// fails to compile instead of silently authorizing as nothing.
fn action_for(kind: &Terminated) -> Action {
    match kind {
        Terminated::Create => Action::ImposterWrite,
        // Mirrors upstream's own reasoning for `PUT /imposters` (see
        // `rift_cluster_base::seams::classify`'s doc): a whole-set replace is
        // destructive regardless of method — it reconciles the set toward
        // the payload, so `{"imposters":[]}` removes everything — so an
        // Editor who may write but not delete must not reach it through the
        // collection route.
        Terminated::ReplaceAllImposters => Action::ImposterDelete,
        Terminated::DeleteAllImposters => Action::ImposterDelete,
        Terminated::DeleteImposter(_) => Action::ImposterDelete,
        Terminated::AddStub(_) => Action::StubWrite,
        Terminated::ReplaceStubs(_) => Action::StubWrite,
        Terminated::ReplaceStubAt(_, _) => Action::StubWrite,
        Terminated::DeleteStubAt(_, _) => Action::StubWrite,
        Terminated::ReplaceStubById(_, _) => Action::StubWrite,
        Terminated::DeleteStubById(_, _) => Action::StubWrite,
        Terminated::SetEnabled(_, _) => Action::LifecycleToggle,
        // The front-door route table (issue #131) predates RFC-002 and has no
        // action of its own in its closed §4.1 list. Treated as an ordinary
        // imposter-tier config write pending a dedicated action.
        Terminated::PutRoutes => Action::ImposterWrite,
        Terminated::DeleteRoute(_) => Action::ImposterWrite,
        Terminated::Tenancy(route) => route.action(),
    }
}

async fn handle(state: Arc<FrontState>, req: Request<Incoming>) -> Response<FrontBody> {
    let path = req.uri().path().to_owned();

    // Gateway traffic (`/__rift/:port/...`) is data-plane, not admin, and is
    // a stated non-goal for authentication (RFC-002 §7): requiring a
    // credential here would force every app under test to carry an admin
    // identity. Guarded explicitly, ahead of every classifier below — both
    // `classify` (write-only, never matches this prefix) and upstream's own
    // `classify` (which returns `None` for it too) would already exempt it,
    // but a future change to either must not be able to silently start
    // gating it.
    if path.starts_with("/__rift/") {
        return proxy(state, req, None).await;
    }

    // `GET /console` / `GET /console/*` (RFC-006 §7, issue #186): the embedded SPA, served from
    // `web/dist` behind the default-off `console` feature. Ahead of `classify` because it is not a
    // config route at all, and unauthenticated because the shell *is* the login UI (§5.3) — see
    // `console`'s module doc for why that is safe and what enforces it.
    //
    // With the feature off this arm does not exist, so `/console` proxies upstream and 404s exactly
    // as it did before C3; `tests/console_off.rs` asserts that on every ordinary CI run.
    #[cfg(feature = "console")]
    if console::matches(&path) {
        return console::serve(req.method(), &path);
    }

    // `POST /session` / `DELETE /session` (RFC-006 §5.3, issue #185): minting and clearing a
    // console session cookie. Neither is a `Terminated` route — a login is a credential exchange,
    // not a config mutation, and a logout touches no replicated state at all — so both are
    // handled directly here, ahead of `classify`, the same way `/admin/whoami` is.
    if path == "/session" {
        return match *req.method() {
            Method::POST => session_login(&state, req).await,
            Method::DELETE => session_logout(),
            _ => typed_error(
                StatusCode::METHOD_NOT_ALLOWED,
                ErrorKind::BadData,
                "/session supports POST (login) and DELETE (logout) only",
            ),
        };
    }

    // `/_fleet/*` (RFC-006 §5.2, issue #185): the same members/health/op-status projection
    // `/_cluster/*` answers, re-exposed on the admin port so an operator working the admin API
    // does not also need a cluster-port credential to ask "is this node healthy". Gated at
    // `Action::ClusterAdmin` — a settled design decision (FleetAdmin only, not per-tenant) —
    // through the same `authorize_action` chokepoint as every other route, so it inherits the
    // CSRF gate and the bypass/401 handling for free.
    if let Some(route) = fleet::classify(req.method(), &path) {
        return match authorize_action(&state, &req, Action::ClusterAdmin, None, None) {
            Ok(_) => {
                let Some(node) = state.node.upgrade() else {
                    return typed_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        ErrorKind::Unavailable,
                        "cluster node is shutting down",
                    );
                };
                match fleet::body(&route, &node, &state.readiness) {
                    Ok(Some(body)) => match serde_json::to_vec(&body) {
                        Ok(bytes) => buffered_response(
                            StatusCode::OK,
                            Bytes::from(bytes),
                            json_content_type(),
                        )
                        .unwrap_or_else(|response| response),
                        Err(e) => internal(&e.to_string()),
                    },
                    // The one case `fleet::body` can 404 on: a well-formed but unknown op id.
                    Ok(None) => typed_error(
                        StatusCode::NOT_FOUND,
                        ErrorKind::NoSuchResource,
                        "Not Found",
                    ),
                    Err(e) => internal(&e),
                }
            }
            Err(response) => response,
        };
    }

    // `GET /front-door/routes` is a state-machine read, not a mutation: it
    // never reaches `classify` (write-only) or `proxy` (there is no upstream
    // `/front-door/routes` to proxy to — U-11's admin CRUD was deferred).
    if req.method() == Method::GET && path == "/front-door/routes" {
        // No port: the route table is read per tenant, not per imposter.
        return match authorize_action(&state, &req, Action::ImposterRead, None, None) {
            Ok((tenant, ..)) => read_routes(&state, &req, &tenant).await,
            Err(response) => response,
        };
    }

    // `GET /admin/whoami` is the one admin route with **no** action (RFC-002
    // §4.1): it reports the caller's own identity and bindings and nothing
    // else, so there is nothing to authorize beyond having authenticated.
    // Handled ahead of `classify` for that reason — routing it through
    // `authorize_action` would require inventing an action for it, and any
    // action would be the wrong answer for a principal reading only itself.
    if req.method() == Method::GET && path == tenancy::WHOAMI_PATH {
        return match authenticate(&state, &req) {
            Ok(authenticated) => {
                let resolved = authenticated
                    .as_ref()
                    .map(|authenticated| &authenticated.resolved);
                // The principal's own name, for a console to render instead of the
                // `key:<sha256-hex>` id. Read here rather than threaded out of `authenticate`
                // because only this route wants it.
                //
                // A storage error is **not** folded into "no name": that would be
                // indistinguishable from the legacy identity, which legitimately has none. It
                // propagates to a 500, which is also nearly unreachable — `resolve_bindings`
                // already read this exact row to authenticate, so a failure here means the
                // control plane broke between the two reads, and a 500 is then the honest answer.
                let display_name = match resolved {
                    Some(resolved) if !principal::is_legacy_identity(&resolved.principal_id) => {
                        let Some(node) = state.node.upgrade() else {
                            return typed_error(
                                StatusCode::SERVICE_UNAVAILABLE,
                                ErrorKind::Unavailable,
                                "cluster node is shutting down",
                            );
                        };
                        match node.principal(&resolved.principal_id) {
                            Ok(stored) => stored.map(|stored| stored.display_name),
                            Err(e) => return internal(&e.to_string()),
                        }
                    }
                    // The legacy `--api-key` principal is minted in code, and the bypass has no
                    // principal at all. Neither has a row, so neither has a name.
                    _ => None,
                };
                match tenancy::whoami_body(resolved, display_name) {
                    Ok(body) => {
                        buffered_response(StatusCode::OK, Bytes::from(body), json_content_type())
                            .unwrap_or_else(|response| response)
                    }
                    Err(e) => internal(&e),
                }
            }
            Err(response) => response,
        };
    }

    // `GET /openapi.json` publishes the hand-authored contract (RFC-006 §5.1, issue #184).
    //
    // Authenticated but **actionless**, the same posture as `/admin/whoami` above: the document
    // describes the shape of the admin surface, so serving it to an unauthenticated scanner would
    // hand out a map of every tenancy and audit route for free. It carries no tenant data, so it
    // needs no action and no tenant resolution either — any authenticated principal reads the same
    // bytes.
    if req.method() == Method::GET && path == "/openapi.json" {
        return match authenticate(&state, &req) {
            Ok(_) => match openapi::contract_json() {
                Ok(body) => {
                    buffered_response(StatusCode::OK, Bytes::from(body), json_content_type())
                        .unwrap_or_else(|response| response)
                }
                // A contract that will not parse is a broken build, and `500` is the honest answer.
                // Answering `200` with `{}` would publish a lie to a generated client and surface
                // as a mystery in *its* codegen rather than here.
                Err(e) => internal(e),
            },
            Err(response) => response,
        };
    }

    if let Some(kind) = classify(req.method(), &path, req.uri().query()) {
        let scope = scope_for(&kind);
        return match authorize_action(
            &state,
            &req,
            action_for(&kind),
            scope.as_ref(),
            addressed_port(&kind),
        ) {
            Ok((tenant, principal_id, bindings)) => {
                terminate(state, req, kind, tenant, principal_id, bindings).await
            }
            Err(response) => response,
        };
    }

    // Proxied: authorize against upstream's own classification when the path
    // is one it recognizes. `None` means "not an authorizable admin route" —
    // an unmatched path, or the gateway prefix already handled above — and
    // must never read as a *denial* (RFC-002 §4.3): there is no action to
    // check. It must still be **authenticated**, though — otherwise an
    // unmatched path would answer whatever the proxied backend gives an
    // anonymous caller instead of `401`, turning it into an unauthenticated
    // route-existence oracle (the exact leak upstream's own hook-ordering
    // contract exists to close, reproduced here for the routes upstream's
    // classifier does not cover).
    match classify_upstream(req.method(), &path) {
        Some(target) => {
            let action = principal::map_action(
                target.action,
                path.starts_with("/admin/imposters/"),
                target.space.is_some(),
                target.params.iter().any(|(name, _)| *name == "scenario"),
            );
            // The port upstream's own classifier parsed out of the path. This is what makes the
            // ownership gate cover the *proxied* reads — the ones that go verbatim to a local
            // engine now binding every tenant's imposters, and so the ones where a missing check
            // would be a live cross-tenant read.
            match authorize_action(&state, &req, action, None, port_param(&target.params)) {
                // The tenant this front just decided rides along as
                // upstream's own `x-rift-scope` header, so `EeAuthorizer` —
                // the loopback's independent defence-in-depth check — sees
                // the *same* tenant this decision was made against, rather
                // than defaulting to `default` for lack of any signal. Without
                // this, every proxied request for a principal not *also*
                // bound to `default` would clear this gate and then be
                // refused a second time at the loopback for the wrong reason.
                Ok((tenant, ..)) => {
                    // Read the marker's inputs before `state` and `req` move into `proxy`.
                    let degraded = local_bind_failure(&state, &target.params);
                    // Likewise the editor's token (C5, #188): the same applied state the write
                    // path's precondition will check, read before the move for the same reason.
                    let token =
                        imposter_read_token(&state, req.method(), &path, &tenant, &target.params);
                    // The imposter *list* is the one proxied read the ownership gate above cannot
                    // cover: it names no port, so there is nothing to check ownership of — and the
                    // engine it proxies to now binds every tenant's imposters, so the verbatim
                    // response would hand the caller the whole fleet's. Filtering the one response
                    // body is done rather than terminating the read and fanning out per port: it
                    // is far less machinery, and it leaves the streaming routes (SSE) untouched,
                    // which buffering in `proxy` itself would not.
                    let list_read = req.method() == Method::GET && is_imposter_listing(&path);
                    let owned = if list_read {
                        match tenant_owned_ports(&state, &tenant) {
                            Ok(ports) => Some(ports),
                            Err(response) => return response,
                        }
                    } else {
                        None
                    };
                    let mut response = proxy(state, req, Some(&tenant)).await;
                    if let Some(owned) = owned {
                        response = filter_imposter_list(response, &owned).await;
                    }
                    if let Some(reason) = degraded {
                        set_header(&mut response, HEADER_BIND_FAILURES, &reason);
                    }
                    if let Some(token) = token {
                        set_header(&mut response, HEADER_REVISION, &token);
                    }
                    return response;
                }
                Err(response) => return response,
            }
        }
        None => {
            if let Err(response) = authenticate(&state, &req) {
                return response;
            }
        }
    }
    proxy(state, req, None).await
}

/// `GET /front-door/routes`: `tenant`'s current route table, read straight from the state machine.
/// This is the front door's *only* read path (issue #131) — upstream never shipped a `GET` to proxy
/// to, so unlike every other read in this module, there is no loopback re-read to fall back on.
///
/// Tenant-addressed as of issue #182: each tenant reads its own table, where it previously read the
/// default tenant's whatever it was bound to.
async fn read_routes(
    state: &Arc<FrontState>,
    _req: &Request<Incoming>,
    tenant: &TenantId,
) -> Response<FrontBody> {
    let Some(node) = state.node.upgrade() else {
        return typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            "cluster node is shutting down",
        );
    };
    // Table and revision together, from one state-machine snapshot (issue
    // #210): the revision is what a client feeds back as `If-Match`, so it must
    // describe the very bytes answered here and not a later table.
    let (table, revision) = match node.route_table_with_revision(tenant.as_str()) {
        Ok(pair) => pair,
        Err(e) => return internal(&e.to_string()),
    };
    let body = match serde_json::to_vec(&table) {
        Ok(body) => body,
        Err(e) => return internal(&e.to_string()),
    };
    let mut response =
        match buffered_response(StatusCode::OK, Bytes::from(body), json_content_type()) {
            Ok(response) | Err(response) => response,
        };
    // The same token shape the write path emits for a portless mutation,
    // hardcoded `default` tenant segment and all — a read whose token the write
    // path would refuse is worse than no token at all.
    set_header(
        &mut response,
        HEADER_REVISION,
        &format!("{}@{revision}", TenantId::default()),
    );
    response
}

/// Authenticate the request and authorize `action` against it (RFC-002 §4.3,
/// §8.1, §8.4) — the single gate every admin request passes through, whether
/// it will be terminated, proxied, or is the front door's own read.
///
/// `Ok((tenant, principal_id))`: `tenant` is the tenant the caller is
/// authorized to act as — the `Decision::Allow` tenant, echoed here so the
/// §8.1 create path can record the *same* value as ownership rather than
/// re-reading `X-Rift-Tenant` a second time (checking and recording from two
/// reads is exactly the confused-deputy bug the header exists to avoid).
/// `principal_id` is who to attribute the request to for U-10 (`None` only
/// under the bypass below, where there is no principal to name).
///
/// Bypass: when the fleet defines no principal and no `--api-key` is
/// configured, every request is allowed against the requested tenant — the
/// pre-#161 open-admin-plane behavior, preserved so an upgrade does not start
/// denying an unauthenticated fleet (`rift_cluster_no_principals` makes this
/// state visible on `/metrics`).
///
/// Fail closed throughout: a state-machine read that errors becomes a `500`,
/// never a fallthrough to allow.
/// What a passed authorization yields: the tenant the caller may act as, who
/// they are (for U-10 attribution), and the bindings the decision was made
/// against — the last so a route that must filter *rows* by tenant
/// (`GET /admin/audit`) derives that filter from the same bindings the decision
/// used, rather than re-resolving the credential and risking a second, subtly
/// different answer.
type Authorized = (TenantId, Option<String>, Vec<(TenantId, Role)>);

/// `scope`, when given, is the tenant the *route itself* names (the tenancy
/// surface's path segment — see [`scope_for`]); it replaces `X-Rift-Tenant`
/// for that request. `None` reads the header, which is the behaviour every
/// resource route wants.
#[allow(clippy::result_large_err)]
fn authorize_action(
    state: &FrontState,
    req: &Request<Incoming>,
    action: Action,
    scope: Option<&TenantId>,
    // The single imposter port this request addresses, if any — see `addressed_port`. Feeds the
    // ownership gate in the `Allow` arm below. Separate from `scope` on purpose: `scope` is which
    // *tenant record* the route names, this is which *resource* it touches.
    addressed_port: Option<u16>,
) -> Result<Authorized, Response<FrontBody>> {
    let authenticated = authenticate(state, req)?;
    // The CSRF gate (RFC-006 §5.3) already ran inside `authenticate` for a cookie-resolved
    // identity, before this function ever sees it — there is nothing left here that needs to know
    // which credential carried the request.
    let resolved = authenticated.map(|authenticated| authenticated.resolved);
    let Some(resolved) = resolved else {
        // The bypass (see `authenticate`'s doc): there is no principal to
        // check a role against, and no binding to intersect `X-Rift-Tenant`
        // against either, so the header is not read here — every op still
        // targets `default`, exactly as it did before RBAC existed. Reading
        // the header under bypass would be a real (if harmless-today)
        // observable change from "nothing changes on upgrade": an
        // unauthenticated caller's tenant claim would start being honored
        // the moment a fleet upgrades, before anyone configured any
        // authorization data at all.
        //
        // The route-named `scope` IS honored here, unlike the header: it is not
        // a caller's claim about who they are acting as, it is which record the
        // request addresses, and answering `/admin/tenants/b/principals` from
        // tenant `default` would be wrong rather than merely conservative.
        // No bindings under the bypass: there is no principal, so there is
        // nothing for a tenant filter to narrow to. A caller reading the audit
        // stream on an unenforced fleet sees the fleet, which is the same thing
        // every other route already gives them.
        return Ok((scope.cloned().unwrap_or_default(), None, Vec::new()));
    };
    let requested = scope.cloned().unwrap_or_else(|| requested_tenant(req));

    match authz::decide(&resolved.bindings, action, &requested) {
        Decision::Allow { tenant } => {
            // Ownership gate (issue #182). `decide` has confirmed this principal holds `action` in
            // `tenant`; what it cannot know is whether the *resource* being addressed belongs to
            // that tenant. Ports are fleet-unique across tenants (RFC-002 §3.2), so a port names
            // exactly one imposter fleet-wide and a caller can address another tenant's imposter
            // simply by knowing its number.
            //
            // This replaces issue #161's fail-closed guard, which refused every non-default tenant
            // outright because the read/sync paths were default-only — storing was not serving.
            // Those paths are tenant-aware as of this change, so the blanket refusal is gone; what
            // remains necessary is this narrower check. **The gate had to land before the guard was
            // removed, not after**: for the window between them the bypass would be real.
            //
            // Here, in the one choke point every admin request passes through — terminated,
            // proxied, and the front door's own read — for the same reason the old guard was: a
            // per-route check is how one route gets missed, and the proxied reads are the dangerous
            // ones. They go verbatim to a local engine that now binds *every* tenant's imposters,
            // so without this an authorized `acme` caller could read `beta`'s imposter by port.
            //
            // Refuses unless the port is owned by *this* tenant — so "owned by someone else" and
            // "owned by nobody" answer identically. Letting an unowned port fall through to
            // upstream would look harmless (nobody's data is behind it) but is a cross-tenant
            // **existence oracle**: upstream's own 404 names the port ("No imposter exists on port
            // N") while this gate's says only "Not Found", so sweeping the range would map exactly
            // which ports other tenants hold. §8.4's property is that a probe cannot tell "not
            // yours" from "not there", and two different 404 bodies tell it.
            //
            // The cost is that a caller reading a genuinely absent port in their *own* tenant now
            // gets the terse body instead of upstream's descriptive one. That is the right trade:
            // the descriptive message is a convenience, indistinguishability is a contract.
            //
            // Creates are unaffected — `Terminated::Create` is not port-addressed, so it never
            // reaches here; a cross-tenant port collision is refused by the state machine's own
            // `port_claimed_by_another_tenant` check, which is where fleet-uniqueness is enforced.
            //
            // The refusal is §8.4's indistinguishable 404, identical to `NotBoundToTenant` below.
            //
            // This applies to `default` symmetrically. Before this change `default` saw everything
            // because everything *was* default's; now a `default` Editor no longer sees `acme`'s
            // imposters. That is a behaviour change, and it is the correct one.
            if let Some(port) = addressed_port {
                let owner = state
                    .node
                    .upgrade()
                    .ok_or_else(|| {
                        typed_error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            ErrorKind::Unavailable,
                            "cluster node is shutting down",
                        )
                    })?
                    .owning_tenant(port)
                    .map_err(|e| internal(&e.to_string()))?;
                if owner != Some(tenant.clone()) {
                    return Err(tenant_boundary_not_found());
                }
            }
            Ok((tenant, Some(resolved.principal_id), resolved.bindings))
        }
        // `decide` never returns this variant itself (it assumes bindings
        // are already resolved) — `authenticate` is what actually produces a
        // `401` from a bad or absent credential.
        Decision::Deny(Denial::Unauthenticated) => Err(unauthorized()),
        // RFC-002 §8.4: must render byte-identical to the tenant-serving
        // guard above — see `tenant_boundary_not_found`'s doc for why, and
        // for what this 404 actually is (not a stand-in for any specific
        // route's genuine not-found).
        Decision::Deny(Denial::NotBoundToTenant) => Err(tenant_boundary_not_found()),
        // Safe to be specific: the caller already knows the tenant exists
        // (they are bound to it), so naming the missing role leaks nothing
        // RFC-002 §8.4 is protecting.
        Decision::Deny(Denial::InsufficientRole { role, .. }) => Err(typed_error(
            StatusCode::FORBIDDEN,
            ErrorKind::InsufficientAccess,
            &format!("role {role:?} does not grant {}", action.as_str()),
        )),
    }
}

/// RFC-002 §8.4's indistinguishable 404 — the one body [`authorize_action`]
/// renders for both `Denial::NotBoundToTenant` and the tenant-serving guard
/// above it, so the two can never drift into bytes a client could tell apart.
/// `typed_error` with `ErrorKind::NoSuchResource` is the helper a real
/// not-found on this surface renders through, but the message is this
/// front's own fixed string, not any specific route's genuine one — upstream's
/// own imposter 404 names the port (`"Imposter not found on port {port}"`,
/// not `"Not Found"`), so this is not a byte-for-byte stand-in for that
/// response. It only has to be indistinguishable from *itself*, which a
/// shared function call guarantees in a way two hand-written call sites do
/// not.
fn tenant_boundary_not_found() -> Response<FrontBody> {
    typed_error(
        StatusCode::NOT_FOUND,
        ErrorKind::NoSuchResource,
        "Not Found",
    )
}

/// The `Cookie` name a session token rides in (RFC-006 §5.3, issue #185).
const SESSION_COOKIE_NAME: &str = "rift_session";

/// The CSRF header [`csrf_gate`] requires on a state-changing, cookie-authenticated request. Any
/// value counts — its presence is what matters (it proves the caller is same-origin JavaScript
/// that could read a response header or set a custom one, which a cross-site form submission or
/// `<img>`/`<script>` tag cannot do), not its content.
const CSRF_HEADER: &str = "x-rift-csrf";

/// What [`authenticate`] resolved a request to.
///
/// A newtype rather than a bare [`principal::Resolved`] so the CSRF decision stays *inside*
/// `authenticate`: the gate (RFC-006 §5.3) runs on the cookie branch only — a bearer cannot be
/// attached by a victim's browser, which is the entire attack — and by the time a caller holds one
/// of these, that decision has already been made and there is nothing left for it to re-derive.
struct Authenticated {
    resolved: principal::Resolved,
}

/// Resolve the request's credential to a principal, without checking any
/// action against it. Used directly for a route with no classified action to
/// check (RFC-002 §4.3's `None` case) — mirroring upstream's own hook
/// ordering, where authentication runs unconditionally and only the
/// authorization *hook* is skipped when nothing was classified — and as the
/// first step of [`authorize_action`], so the two never resolve a credential
/// two different ways.
///
/// `Ok(Some(authenticated))` is an authenticated principal. `Ok(None)` is the
/// bypass: the fleet defines no principal and no `--api-key` is configured,
/// so there is nobody to check a credential against — the pre-#161
/// open-admin-plane behavior. `Err` is `401` (or `500` on a state-machine
/// read failure — fail closed, never a fallthrough to allow), or — new in
/// #185 — `403` from the CSRF gate below.
///
/// # The bearer path is byte-identical to before #185
///
/// When an `Authorization` header is present, the block below runs the exact call and the exact
/// three-way match this function ran before session cookies existed, and returns from inside
/// that `if`. The cookie branch (RFC-006 §5.3) is only ever reached when there is **no**
/// `Authorization` header at all — sessions are strictly additive, never a second opinion on a
/// request a bearer credential already resolved.
#[allow(clippy::result_large_err)]
fn authenticate(
    state: &FrontState,
    req: &Request<Incoming>,
) -> Result<Option<Authenticated>, Response<FrontBody>> {
    let Some(node) = state.node.upgrade() else {
        return Err(typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            "cluster node is shutting down",
        ));
    };
    let credential = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    if let Some(credential) = credential {
        // See "The bearer path is byte-identical to before #185" above.
        return match principal::resolve_bindings(
            &node,
            state.api_key.as_deref(),
            state.legacy_key_is_fleet_admin,
            Some(credential),
        ) {
            Ok(Some(resolved)) => Ok(Some(Authenticated { resolved })),
            Ok(None) => match principal::should_bypass(&node, state.api_key.as_deref()) {
                Ok(true) => Ok(None),
                Ok(false) => Err(unauthorized()),
                Err(e) => Err(internal(&e.to_string())),
            },
            Err(e) => Err(internal(&e.to_string())),
        };
    }

    // No `Authorization` header: try the session cookie (RFC-006 §5.3, issue #185), falling back
    // to the same bypass-or-401 the bearer branch above would have when there is no cookie
    // either — an unauthenticated request is refused exactly as it was before #185 shipped.
    match resolve_cookie(&node, req) {
        Ok(Some(resolved)) => {
            csrf_gate(req)?;
            Ok(Some(Authenticated { resolved }))
        }
        Ok(None) => match principal::should_bypass(&node, state.api_key.as_deref()) {
            Ok(true) => Ok(None),
            Ok(false) => Err(unauthorized()),
            Err(e) => Err(internal(&e.to_string())),
        },
        // `resolve_cookie`'s `Err` is already the rendered response (a `500` from a
        // state-machine read failure) — propagate it as-is rather than re-wrapping it.
        Err(response) => Err(response),
    }
}

/// Resolve the `rift_session` cookie to a principal's bindings (RFC-006 §5.3, issue #185).
///
/// `Ok(None)` flattens every "not authenticated by cookie" case alike — no cookie present, no
/// signing key committed yet, a token that fails [`session::verify`] for any reason, a principal
/// since deleted or disabled — the same flattening `principal::resolve_bindings` already applies
/// to a bad bearer credential, and for the same reason: the caller cannot act on *why*, only on
/// whether a principal resolved.
///
/// [`session::verify`] proves authentication only (see that module's doc). The bindings below
/// are read fresh from applied state on every call — never cached alongside the verified
/// identity — so a principal disabled or unbound after the cookie was issued loses access on its
/// very next request, not merely at its next login. This is deliberately the same shape
/// `resolve_bindings` already uses for the bearer path; do not special-case the cookie path with
/// a cache anywhere in this function (issue #165's `c25_key_revocation_survives_a_partition`
/// exists to catch exactly that mutant).
#[allow(clippy::result_large_err)]
fn resolve_cookie(
    node: &RaftNode,
    req: &Request<Incoming>,
) -> Result<Option<principal::Resolved>, Response<FrontBody>> {
    let Some(token) = session_cookie(req) else {
        return Ok(None);
    };
    let Some(key) = node.session_key().map_err(|e| internal(&e.to_string()))? else {
        // No console login has ever minted a signing key on this fleet, so no cookie this node
        // issued could exist — but a client can still present garbage, and that is `None`, not
        // an error.
        return Ok(None);
    };
    let principal_id = match session::verify(&key, &token, now_secs()) {
        Ok(principal_id) => principal_id,
        Err(_) => return Ok(None),
    };
    // Deliberately the *same* function the bearer path's session branch uses, rather than a second
    // copy of "load principal, reject if disabled, read bindings fresh". Two copies of a
    // disabled-check in an authentication path is precisely the pair that drifts, and the one that
    // stops rejecting is the one nobody notices.
    principal::resolve_stored_principal(node, principal_id).map_err(|e| internal(&e.to_string()))
}

/// The `rift_session` cookie's raw value, if the request carries one. No percent-decoding: the
/// token alphabet (base64url plus `.`) never needs it.
fn session_cookie(req: &Request<Incoming>) -> Option<String> {
    cookie_value(req.headers())
}

/// The cookie lookup against a bare header map, so the proxy leg — which has already destructured
/// the request into parts — can reach it too.
fn cookie_value(headers: &hyper::HeaderMap) -> Option<String> {
    let raw = headers.get(hyper::header::COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name.trim() == SESSION_COOKIE_NAME).then(|| value.trim().to_owned())
    })
}

/// RFC-006 §5.3's CSRF gate: a request authenticated **by cookie** that is state-changing
/// (anything but `GET`/`HEAD`/`OPTIONS`) must carry [`CSRF_HEADER`] (any value) or is refused
/// with `403`. A bearer credential is exempt — see [`Authenticated`]'s doc.
///
/// Called from inside [`authenticate`] itself, on the one branch that resolves a cookie, so
/// every caller of `authenticate` — `authorize_action` (and therefore every terminated and
/// proxied route, plus the `/_fleet/*` routes added by #185) and the direct callers
/// (`/admin/whoami`, `/openapi.json`, `/session`) — gets this for free. There is no second call
/// site to add and no route that reaches a cookie-resolved identity without passing through it:
/// a route that "terminates early" still had to call `authenticate` (or `authorize_action`,
/// which calls it) to have a cookie-resolved identity to terminate with in the first place.
#[allow(clippy::result_large_err)]
fn csrf_gate(req: &Request<Incoming>) -> Result<(), Response<FrontBody>> {
    if matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS) {
        return Ok(());
    }
    if req.headers().contains_key(CSRF_HEADER) {
        return Ok(());
    }
    Err(typed_error(
        StatusCode::FORBIDDEN,
        ErrorKind::InsufficientAccess,
        &format!(
            "state-changing requests authenticated by session cookie must carry {CSRF_HEADER}"
        ),
    ))
}

/// Seconds since the Unix epoch, floored to `0` on a pre-epoch clock — the same convention
/// [`mint`]'s op-issuing sibling uses, so a session's `iat`/`exp` and an op's `issued_at_secs`
/// degrade the same way under a broken clock rather than in two different directions.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Session cookies (RFC-006 §5.3, issue #185)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct SessionLoginBody {
    #[serde(rename = "apiKey")]
    api_key: String,
}

/// `POST /session`: exchange an API key for a session cookie (RFC-006 §5.3, issue #185).
///
/// Credential verification is exactly `authenticate`'s bearer path — `principal::resolve_bindings`,
/// the same argon2id lookup — called here directly rather than reimplemented, so there is only
/// ever one way to check an API key on this front (the issue's explicit requirement). What is
/// new is only what happens *after* a key checks out: minting a signed cookie instead of
/// authorizing the one request that presented it.
async fn session_login(state: &Arc<FrontState>, req: Request<Incoming>) -> Response<FrontBody> {
    let Some(node) = state.node.upgrade() else {
        return typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            "cluster node is shutting down",
        );
    };

    let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return typed_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorKind::RequestTooLarge,
                &format!("admin request body refused: {e}"),
            );
        }
    };
    let login: SessionLoginBody = match serde_json::from_slice(&body) {
        Ok(login) => login,
        Err(e) => {
            return typed_error(
                StatusCode::BAD_REQUEST,
                ErrorKind::BadData,
                &format!("invalid request JSON: {e}"),
            );
        }
    };

    // Byte-for-byte the same check `authenticate`'s bearer branch runs against an `Authorization`
    // header — see this function's doc.
    let resolved = match principal::resolve_bindings(
        &node,
        state.api_key.as_deref(),
        state.legacy_key_is_fleet_admin,
        Some(login.api_key.as_str()),
    ) {
        Ok(Some(resolved)) => resolved,
        Ok(None) => return unauthorized(),
        Err(e) => return internal(&e.to_string()),
    };

    // The legacy `--api-key` (RFC-002 §3.4) resolves to a *synthetic* identity with no principal
    // row behind it. A session token names a principal and every later request re-reads that row to
    // get current bindings, so a cookie minted for the synthetic id would authenticate exactly
    // never: the login would answer `200` with a real `Set-Cookie`, and every subsequent request
    // would be `401` with nothing to distinguish it from a rotated key or a skewed clock.
    //
    // Refused explicitly rather than papered over, and refused *here* rather than by letting the
    // cookie fail later, because a `200` for a credential exchange that cannot produce a usable
    // credential is the silent-fallback shape this codebase treats as a defect.
    if principal::is_legacy_identity(resolved.principal_id.as_str()) {
        return typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            "the legacy --api-key cannot hold a console session: it names no principal, so a \
             session minted for it could never be resolved back to one. Create a principal \
             (POST /admin/tenants/{tenant}/principals) and log in with its key.",
        );
    }

    let key = match ensure_session_key(state, &node, resolved.principal_id.as_str()).await {
        Ok(key) => key,
        Err(response) => return response,
    };
    let token = session::mint(
        &key,
        resolved.principal_id.as_str(),
        now_secs(),
        session::SESSION_TTL_SECS,
    );

    let mut response = match buffered_response(StatusCode::OK, Bytes::new(), None) {
        Ok(response) => response,
        Err(response) => return response,
    };
    match set_session_cookie(&mut response, &token, session::SESSION_TTL_SECS) {
        Ok(()) => response,
        Err(response) => response,
    }
}

/// `DELETE /session`: clear the session cookie (RFC-006 §5.3, issue #185).
///
/// Unconditional and unauthenticated on purpose: logging out never fails, whether or not the
/// caller holds a live session — there is no server-side state to invalidate (a session is
/// nothing but a signed claim the fleet did not have to remember making), only a cookie the
/// browser is told to stop sending. Requiring a valid session first would turn "log out of an
/// already-expired session" into a `401` instead of the no-op it should be.
fn session_logout() -> Response<FrontBody> {
    let mut response = match buffered_response(StatusCode::NO_CONTENT, Bytes::new(), None) {
        Ok(response) => response,
        Err(response) => return response,
    };
    match clear_session_cookie(&mut response) {
        Ok(()) => response,
        Err(response) => response,
    }
}

/// The fleet's session-signing key, minting one first if no console login has ever committed one
/// (issue #185). The only branch of this front's entire session surface that is a Raft write —
/// every other login reads the record this one commits.
async fn ensure_session_key(
    state: &FrontState,
    node: &Arc<RaftNode>,
    principal_id: &str,
) -> Result<SessionKey, Response<FrontBody>> {
    if let Some(key) = node.session_key().map_err(|e| internal(&e.to_string()))? {
        return Ok(key);
    }

    let mut bytes = [0u8; SESSION_KEY_BYTES];
    rand::thread_rng().fill_bytes(&mut bytes);
    let op = ControlOp::SessionKeyPut {
        tenant: TenantId::new(FLEET_SCOPE),
        key: session::hex_encode(&bytes),
    };
    // R4's usual order (validate, park durably, submit) even though a locally-generated key can
    // only fail this for a programming error in this function, not anything a caller controls —
    // every other write on this front validates before parking, and there is no reason for the
    // one write minting a *signing* key to be the exception.
    if let Err(reason) = control::validate(&op) {
        return Err(refusal_response(&reason));
    }
    let op_id = Uuid::new_v4();
    let request = mint(op_id, op, None, Some(principal_id.to_owned()));
    if let Err(e) = node.park_intent(&request) {
        return Err(typed_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::InternalError,
            &format!("cannot durably accept the write: {e}"),
        ));
    }

    let committed = match tokio::time::timeout(WRITE_DEADLINE, node.submit(request)).await {
        Err(_) => {
            node.request_replay();
            return Err(typed_error(
                StatusCode::GATEWAY_TIMEOUT,
                ErrorKind::Timeout,
                "write did not commit within the deadline; parked for replay",
            ));
        }
        Ok(Err(NodeError::Unavailable(detail))) => {
            node.request_replay();
            return Err(typed_error(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorKind::Unavailable,
                &format!("no quorum / leader unreachable (parked for replay): {detail}"),
            ));
        }
        Ok(Err(e)) => {
            node.request_replay();
            return Err(typed_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorKind::InternalError,
                &e.to_string(),
            ));
        }
        Ok(Ok(response)) => response,
    };
    if let Err(e) = node.unpark_intent(&op_id) {
        tracing::error!(%op_id, error = %e, "op terminal but could not unpark");
    }
    if let ControlOutcome::Failed { reason } = &committed.outcome {
        return Err(refusal_response(reason));
    }

    // This node must see its own write before the re-read below can be trusted: `submit`
    // guarantees the op committed on a quorum, not that *this* node's local state machine has
    // caught up to it — a follower forwards to the leader and gets the leader's answer back.
    node.await_local_applied(committed.revision, state.barrier_timeout)
        .await;

    // Re-read rather than trust the bytes generated above: two concurrent first logins can both
    // observe `None` at the top of this function and both submit a `SessionKeyPut`. Both commit —
    // the op is an unconditional overwrite, not a compare-and-swap — so only the *second* one to
    // apply is the row every node actually agrees on, and it is not necessarily this call's own.
    match node.session_key().map_err(|e| internal(&e.to_string()))? {
        Some(key) => Ok(key),
        None => Err(internal(
            "session key committed but not yet visible on this node",
        )),
    }
}

/// Render the `Set-Cookie` header for a freshly minted session token.
///
/// `HttpOnly` (unreachable from `document.cookie`, so XSS cannot exfiltrate it), `Secure` (never
/// sent over plaintext HTTP), `SameSite=Strict` (never attached to a cross-site navigation or
/// subrequest at all). The last is most of what makes [`csrf_gate`]'s job small: `SameSite=Strict`
/// already blocks the classic top-level-navigation CSRF; the gate closes the same-site
/// XHR/`fetch` case a `SameSite` cookie alone does not (a same-site page can still issue a
/// request the browser *will* attach the cookie to).
#[allow(clippy::result_large_err)]
fn set_session_cookie(
    response: &mut Response<FrontBody>,
    token: &str,
    ttl_secs: u64,
) -> Result<(), Response<FrontBody>> {
    let value = format!(
        "{SESSION_COOKIE_NAME}={token}; HttpOnly; Secure; SameSite=Strict; Max-Age={ttl_secs}; Path=/"
    );
    let header = HeaderValue::from_str(&value).map_err(|e| internal(&e.to_string()))?;
    response
        .headers_mut()
        .append(hyper::header::SET_COOKIE, header);
    Ok(())
}

/// The logout half of [`set_session_cookie`]: the same attributes, an empty value, and
/// `Max-Age=0` — the standard way to tell a browser to forget a cookie immediately.
#[allow(clippy::result_large_err)]
fn clear_session_cookie(response: &mut Response<FrontBody>) -> Result<(), Response<FrontBody>> {
    let value =
        format!("{SESSION_COOKIE_NAME}=; HttpOnly; Secure; SameSite=Strict; Max-Age=0; Path=/");
    let header = HeaderValue::from_str(&value).map_err(|e| internal(&e.to_string()))?;
    response
        .headers_mut()
        .append(hyper::header::SET_COOKIE, header);
    Ok(())
}

/// The tenant a request asks to act as: `X-Rift-Tenant` when present, else
/// the default tenant. RFC-002 §8.1: this header **selects among the
/// principal's existing bindings; it never grants one** — `authorize_action`
/// only ever uses this to intersect against bindings already loaded from the
/// state machine, never to widen them.
fn requested_tenant(req: &Request<Incoming>) -> TenantId {
    req.headers()
        .get("x-rift-tenant")
        .and_then(|v| v.to_str().ok())
        .map(TenantId::new)
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Proxy path
// ---------------------------------------------------------------------------

/// Forward `req` to the loopback admin unchanged and stream the response
/// back. `scope`, when given, is the tenant `authorize_action` already
/// decided this request against — stamped onto upstream's own
/// `x-rift-scope` header (see `set_scope_header`'s doc for why: the loopback
/// admin's `EeAuthorizer` needs it, or it independently re-derives `default`
/// from having no signal at all).
///
/// `scope: None` (the gateway prefix and the unclassified-upstream-path case
/// in `handle`) **removes** any `x-rift-scope` the client sent, rather than
/// forwarding it untouched — see the removal below for why: it is the same
/// confused-deputy hazard `set_scope_header` closes for the `Some` case,
/// just reached by a different caller.
/// `<port>=<reason>` when this node could not realize the addressed imposter's port, else `None`
/// (issue #143).
///
/// This is what makes a `200` from a bind-diverged node honest. The imposter is in the local port
/// map and answers every in-process route, so the read genuinely succeeds — but on *this* node it
/// is reachable only through the front door and the gateway, never on its own port, and nothing in
/// the core-shaped response body says so. The body stays core-shaped deliberately (the U-8 seam is
/// headers-only), so the divergence is reported as a header.
///
/// Absent when the port is healthy: a marker on every read would be noise, not a signal.
/// The imposter port upstream's classifier parsed out of a proxied route, if it named one.
///
/// Domain-optional parse: most admin routes carry no `port` param at all. When one is present it
/// was rendered from a `u16` by `AuthzTarget::with_port`, so the round trip cannot realistically
/// fail — but a `None` here is safe either way, because both callers treat "no port" as "nothing
/// port-specific to do": the ownership gate has no resource to check, and the bind-failure marker
/// has no imposter to describe.
fn port_param(params: &[(&'static str, String)]) -> Option<u16> {
    params
        .iter()
        .find(|(name, _)| *name == "port")
        .and_then(|(_, value)| value.parse().ok())
}

/// Every port `tenant` owns a committed config for, on this node's applied state.
#[allow(clippy::result_large_err)]
fn tenant_owned_ports(
    state: &FrontState,
    tenant: &TenantId,
) -> Result<std::collections::BTreeSet<u16>, Response<FrontBody>> {
    let Some(node) = state.node.upgrade() else {
        return Err(typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            "cluster node is shutting down",
        ));
    };
    Ok(node
        .configured_ports()
        .map_err(|e| internal(&e.to_string()))?
        .into_iter()
        .filter(|(owner, _)| owner == tenant)
        .map(|(_, port)| port)
        .collect())
}

/// Narrow a proxied imposter listing to `owned`.
///
/// Upstream answers with the whole engine, which since issue #182 binds every tenant's imposters —
/// so without this an authorized caller would read the fleet's imposters rather than their own.
/// Applied to `default` too: it no longer sees other tenants' imposters, which is the intended
/// behaviour change.
///
/// **Fails closed.** If the body cannot be read or is not the shape we expect, the listing is
/// refused rather than forwarded: passing through a body we could not filter is precisely the
/// cross-tenant leak this exists to close, and a broken listing is a far better outcome than a
/// silently over-broad one.
async fn filter_imposter_list(
    response: Response<FrontBody>,
    owned: &std::collections::BTreeSet<u16>,
) -> Response<FrontBody> {
    let (parts, body) = response.into_parts();
    // Only a success body is a listing; an error body has no imposters to leak and is passed
    // through so upstream's own error rendering survives.
    if !parts.status.is_success() {
        return Response::from_parts(parts, body);
    }
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => return internal(&format!("reading the imposter listing to filter it: {e}")),
    };
    match narrow_imposter_listing(&bytes, owned) {
        Ok(body) => buffered_response(parts.status, Bytes::from(body), json_content_type())
            .unwrap_or_else(|response| response),
        Err(e) => internal(&e),
    }
}

/// Whether `path` addresses the imposter *collection*, whose body lists every imposter the local
/// engine holds — and which, since this issue made the engine bind every tenant, spans the fleet.
///
/// Named once so the proxied read and the post-mutation re-read cannot disagree about which paths
/// need narrowing. Them disagreeing is exactly how one of the two shipped unfiltered.
fn is_imposter_listing(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path);
    matches!(path, "/imposters" | "/admin/imposters")
}

/// The single-imposter read's `If-Match` token, or `None` when this read is not that route or the
/// applied state holds no record to condition on (C5, issue #188).
///
/// Only `GET /imposters/{port}` is decorated. The listing names no single conditionable record; the
/// sub-resource reads (`/requests`, `/stubs`, …) inherit their imposter's record but handing the
/// same token out on five paths invites conditioning a write on a read of something else. One
/// route, one token, same grammar the write path emits — a token this front's own `parse_if_match`
/// would refuse is worse than none.
///
/// Absence over error throughout: the read itself is served by the engine and must not start
/// failing because the token lookup could not run. A dead node handle here is the same
/// degraded-but-serving case `local_bind_failure` documents.
fn imposter_read_token(
    state: &FrontState,
    method: &Method,
    path: &str,
    tenant: &TenantId,
    params: &[(&'static str, String)],
) -> Option<String> {
    if *method != Method::GET || !is_single_imposter_read(path) {
        return None;
    }
    let port = port_param(params)?;
    let node = state.node.upgrade()?;
    let revision = node
        .imposter_revision(tenant.as_str(), port)
        // Absence-over-error is right (the read itself still serves; the console just disables
        // conditional saves), but a storage failure must not vanish on the way to it — the corrupt
        // -record case inside `imposter_revision` already logs at error level, and a redb read
        // failure deserves the same trail rather than a silent None.
        .inspect_err(|e| tracing::warn!(tenant = tenant.as_str(), port, error = %e, "imposter read serves without a revision token"))
        .ok()
        .flatten()?;
    // The literal `default` tenant segment, matching the write path's emission at its single site —
    // an intentionally shared quirk, not an oversight; see `RiftClusterRevision`'s contract note.
    Some(format!("{}:{port}@{revision}", TenantId::default()))
}

/// Whether `path` is exactly the single-imposter read, `/imposters/{port}` — the one proxied read
/// whose response carries a conditionable record's revision.
fn is_single_imposter_read(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path);
    let mut segments = path.trim_start_matches('/').split('/');
    segments.next() == Some("imposters")
        && segments.next().is_some_and(|p| p.parse::<u16>().is_ok())
        && segments.next().is_none()
}

/// Drop every entry from an imposter listing whose port `owned` does not contain.
///
/// The one implementation behind both narrowing sites. `Err` is a message the caller renders as a
/// 500 — **never** a fallback to the unfiltered body, because forwarding a listing that could not
/// be filtered is precisely the leak this exists to close.
fn narrow_imposter_listing(
    bytes: &[u8],
    owned: &std::collections::BTreeSet<u16>,
) -> Result<Vec<u8>, String> {
    let mut doc: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|_| "the imposter listing was not JSON, so it could not be filtered by tenant")?;
    let imposters = doc
        .get_mut("imposters")
        .and_then(|v| v.as_array_mut())
        .ok_or("the imposter listing had no `imposters` array, so it could not be filtered")?;
    // An entry whose port will not parse is dropped, not kept: this filter is an authorization
    // boundary, and a classifier that cannot classify must treat the input as the dangerous class.
    imposters.retain(|entry| {
        entry
            .get("port")
            .and_then(serde_json::Value::as_u64)
            .and_then(|p| u16::try_from(p).ok())
            .is_some_and(|port| owned.contains(&port))
    });
    serde_json::to_vec(&doc).map_err(|e| format!("re-encoding the filtered imposter listing: {e}"))
}

fn local_bind_failure(state: &FrontState, params: &[(&'static str, String)]) -> Option<String> {
    let port: u16 = port_param(params)?;
    let Some(node) = state.node.upgrade() else {
        // Loud rather than quiet: everywhere else in this file a dead node handle is an explicit
        // 503, and this is the one place it would instead mean "no marker" — i.e. a possibly
        // degraded node answering 200 with nothing saying so. The read itself is already served, so
        // failing it now would be worse; a warning is what keeps the omission traceable.
        tracing::warn!(
            port,
            "cluster node handle is gone; cannot report whether this port is bind-diverged"
        );
        return None;
    };
    // `bind_failure`, NOT `apply_failures`: only a port the engine holds but never bound is serving
    // in-process, which is what this header asserts. The general failure map also carries parse,
    // enable and stub-patch failures, and for those the imposter is not in the map at all — marking
    // such a read as bind divergence would point an operator at the wrong cause entirely.
    let reason = node.bind_failure(port)?;
    Some(format!("{port}={reason}"))
}

async fn proxy(
    state: Arc<FrontState>,
    req: Request<Incoming>,
    scope: Option<&TenantId>,
) -> Response<FrontBody> {
    let (mut parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", |paq| paq.as_str())
        .to_owned();
    let uri: Uri = match format!("http://{}{}", state.upstream_admin, path_and_query).parse() {
        Ok(uri) => uri,
        Err(e) => {
            return typed_error(
                StatusCode::BAD_REQUEST,
                ErrorKind::BadData,
                &format!("request target does not re-target: {e}"),
            );
        }
    };
    parts.uri = uri;
    // A cookie-authenticated request reaches upstream with no `Authorization` header, and
    // upstream's authorizer seam re-resolves from that value alone — so present the session token
    // as the credential. `principal::resolve_bindings` accepts it, which is what lets a console
    // session read a proxied route (`GET /imposters` and friends) at all. Only filled in when the
    // client sent no `Authorization` of its own, so a bearer request is untouched.
    if !parts.headers.contains_key("authorization")
        && let Some(token) = cookie_value(&parts.headers)
        && let Ok(value) = HeaderValue::from_str(&token)
    {
        parts.headers.insert("authorization", value);
    }
    match scope {
        Some(tenant) => set_scope_header(&mut parts.headers, tenant),
        // Unconditional remove, not skip: the client's own request headers
        // are forwarded verbatim otherwise, so a caller-supplied
        // `x-rift-scope` would ride straight through to the loopback's
        // `EeAuthorizer` unexamined — the same confused-deputy shape
        // `set_scope_header` closes when a scope IS decided, just reached
        // through the `None` callers (the gateway prefix and any upstream
        // path `handle` could not classify) instead. Not exploitable today
        // only because upstream's own `classify` also returns `None` for
        // both of those paths — i.e. the invariant currently holds by an
        // upstream implementation detail, not by anything this front
        // enforces — so a future change to either classifier must not be
        // able to silently start trusting a client's own scope claim.
        None => {
            parts.headers.remove(HeaderName::from_static(SCOPE_HEADER));
        }
    }
    match state.proxy.request(Request::from_parts(parts, body)).await {
        // The response body streams through as-is — buffering here would break
        // the admin SSE streams.
        Ok(response) => response.map(BodyExt::boxed),
        Err(e) => typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            &format!("local admin backend unreachable: {e}"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Terminated path
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AddStubBody {
    stub: Stub,
    #[serde(default)]
    index: Option<usize>,
}

#[derive(Deserialize)]
struct ReplaceStubsBody {
    stubs: Vec<Stub>,
}

#[derive(Deserialize)]
struct ReplaceAllBody {
    #[serde(default)]
    imposters: Vec<ImposterConfig>,
}

async fn terminate(
    state: Arc<FrontState>,
    req: Request<Incoming>,
    kind: Terminated,
    tenant: TenantId,
    principal_id: Option<String>,
    bindings: Vec<(TenantId, Role)>,
) -> Response<FrontBody> {
    let Some(node) = state.node.upgrade() else {
        return typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            "cluster node is shutting down",
        );
    };

    // The tenancy surface takes its own path from here. It shares the gate
    // above (authentication, authorization, the §8.4 404) — which is the part
    // that must not be duplicated — but none of what follows: there is no port
    // to condition an `If-Match` on, no `_rift.script` to resolve, and no
    // loopback route to re-read for the render.
    if let Terminated::Tenancy(route) = kind {
        return terminate_tenancy(&state, &node, req, route, principal_id, &tenant, &bindings)
            .await;
    }

    // Authorization already ran in `handle`, once, for every admin request —
    // terminated, proxied, or the front door's own read. Nothing here
    // re-checks it.
    //
    // The internal re-read below still has to *carry* a credential, though: it goes back through
    // upstream, whose authorizer seam re-resolves from the `Authorization` value alone
    // (`AuthzRequest` exposes nothing else, and it lives in the vendored submodule). A
    // cookie-authenticated request has no such header, so the session token is presented in its
    // place — `principal::resolve_bindings` accepts it, and the same principal resolves on both
    // legs. Without this the write commits and the render re-read is refused, so the client is told
    // `403` about a change that actually landed.
    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or_else(|| cookie_value(req.headers()));

    let host = req.headers().get("host").cloned();
    let idempotency = req
        .headers()
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let if_match = match req.headers().get("if-match") {
        None => None,
        // A precondition the front cannot even read must refuse — silently
        // treating it as absent would apply the write unconditionally, the
        // exact lost-update the header exists to prevent.
        Some(value) => match value.to_str() {
            Ok(value) => Some(value.to_owned()),
            Err(_) => {
                return typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    "If-Match is not readable ASCII; expected default:<port>@<revision> or a bare revision",
                );
            }
        },
    };
    let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return typed_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorKind::RequestTooLarge,
                &format!("admin request body refused: {e}"),
            );
        }
    };

    match build_and_run(
        &state,
        &node,
        kind,
        &tenant,
        &body,
        auth.as_deref(),
        host.as_ref(),
        idempotency.as_deref(),
        if_match.as_deref(),
        principal_id,
    )
    .await
    {
        Ok(response) => response,
        Err(response) => response,
    }
}

/// One terminated mutation: what to commit, and how to answer afterwards.
struct Mutation {
    ops: Vec<ControlOp>,
    /// The port label for the revision header; `None` for collection-wide ops.
    port: Option<u16>,
    /// How the success response is rendered.
    render: Render,
}

enum Render {
    /// `GET` this loopback path after the barrier and answer with its body.
    FetchAfter { path: String, status: StatusCode },
    /// Answer with a body captured *before* the ops committed (deletes answer
    /// with what was removed).
    Captured {
        body: Bytes,
        content_type: Option<HeaderValue>,
        status: StatusCode,
    },
}

/// Build the mutation for `kind`, pre-validate it, commit it op by op, run the
/// barrier, and render the response. Errors are already client-shaped.
#[allow(clippy::too_many_arguments)]
async fn build_and_run(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    kind: Terminated,
    tenant: &TenantId,
    body: &[u8],
    auth: Option<&str>,
    host: Option<&HeaderValue>,
    idempotency: Option<&str>,
    if_match: Option<&str>,
    principal_id: Option<String>,
) -> Result<Response<FrontBody>, Response<FrontBody>> {
    let is_batch = matches!(kind, Terminated::ReplaceAllImposters);
    let mut mutation = build_mutation(state, node, kind, tenant, body, auth, host).await?;

    // Pre-validate every op before committing any: a multi-op mutation (PUT
    // /imposters) must not tear half the fleet's config down and then refuse
    // the other half. The state machine re-runs the same checks on apply.
    for op in &mutation.ops {
        if let Err(reason) = control::validate(op) {
            return Err(refusal_response(&reason));
        }
    }

    // A precondition can only ever address a revision the state machine
    // actually stores: a single imposter, or (issue #210) a tenant's route
    // table. A mutation addressing neither is refused before anything is minted
    // or parked.
    let expected_revision = match if_match {
        Some(raw) => Some(parse_if_match(raw, precondition_port(&mutation)?)?),
        None => None,
    };

    if !state.allow_injection {
        for op in &mutation.ops {
            if op_uses_script_surface(op) {
                return Err(injection_disallowed());
            }
        }
    }

    // Resolve `_rift.script` file:/ref: sources (upstream #356), then validate
    // what resolution produced (#57) — both after the gate and before parking,
    // so nothing unresolved or unparseable is ever parked, replayed, or
    // replicated, and a gated request never touches the filesystem.
    let script_base = front_script_base(state.scripts_dir.as_deref());
    // Payload index for each `PUT /imposters` op — upstream's
    // `imposter[{idx}]` label. Upserts precede prune deletes in
    // `build_mutation`, so counting `PutImposter`s reproduces it.
    let mut next_put = 0usize;
    let batch_indices: Vec<Option<usize>> = mutation
        .ops
        .iter()
        .map(|op| {
            (is_batch && matches!(op, ControlOp::PutImposter { .. })).then(|| {
                let index = next_put;
                next_put += 1;
                index
            })
        })
        .collect();
    // Two passes, not one interleaved: upstream resolves every imposter in a
    // batch before validating any of them, so a payload carrying both an
    // unresolvable ref and an unparseable script reports the *resolution*
    // failure. Interleaving would report whichever op came first instead.
    for (op, batch_index) in mutation.ops.iter_mut().zip(&batch_indices) {
        resolve_op_scripts(op, node, &script_base, *batch_index)?;
    }
    for (op, batch_index) in mutation.ops.iter().zip(&batch_indices) {
        validate_op_scripts(op, *batch_index)?;
    }

    // Mint deterministically from the client's Idempotency-Key (when given),
    // then park every op durably BEFORE submitting any (R4): once parked, the
    // op survives a crash and the replay loop finishes what this request
    // cannot — including the tail of a multi-op sequence.
    let base = base_op_id(idempotency);
    let total = mutation.ops.len();
    let requests: Vec<ControlRequest> = mutation
        .ops
        .into_iter()
        .enumerate()
        .map(|(index, op)| {
            mint(
                op_id_for(base, index, total),
                op,
                expected_revision,
                principal_id.clone(),
            )
        })
        .collect();
    for request in &requests {
        if let Err(e) = node.park_intent(request) {
            // Refusing is the only honest answer: R4's promise is exactly that
            // an accepted op is durable, and this one could not be made so.
            return Err(typed_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorKind::InternalError,
                &format!("cannot durably accept the write: {e}"),
            ));
        }
    }

    if state.admin_async {
        let node = Arc::clone(node);
        let op_ids: Vec<Uuid> = requests.iter().map(|request| request.op_id).collect();
        let background = requests;
        tokio::spawn(async move {
            for request in background {
                let op_id = request.op_id;
                match node.submit(request).await {
                    Ok(_) => {
                        if let Err(e) = node.unpark_intent(&op_id) {
                            tracing::error!(%op_id, error = %e, "applied but could not unpark");
                        }
                    }
                    Err(e) => {
                        // The replay loop owns it from here — and is woken now
                        // rather than left to its periodic sweep. Nothing else
                        // will rouse it: this node has a leader (the submit
                        // reached one to fail against), so the leader-transition
                        // trigger will not fire, and the client is holding a 202
                        // that promised this would apply (#83).
                        tracing::warn!(%op_id, error = %e, "async submit failed; intent stays parked");
                        node.request_replay();
                        return;
                    }
                }
            }
        });
        // `opIds` is what `GET /_cluster/ops/:id` can actually answer for: a
        // multi-op mutation parks only the derived ids, never the base — a
        // client polling the bare base of a PUT /imposters would 404 forever.
        let body = serde_json::json!({
            "opId": base.to_string(),
            "opIds": op_ids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
        })
        .to_string();
        let mut response =
            buffered_response(StatusCode::ACCEPTED, Bytes::from(body), json_content_type())?;
        set_header(&mut response, HEADER_OP_ID, &base.to_string());
        return Ok(response);
    }

    let mut last: Option<(Uuid, ControlResponse)> = None;
    for request in requests {
        let op_id = request.op_id;
        let submitted = tokio::time::timeout(WRITE_DEADLINE, node.submit(request)).await;
        let response = match submitted {
            Err(_) => {
                // Parked, so not lost: the replay loop retries it, and is woken
                // now rather than left to its ~30s sweep — a submit that timed
                // out reached a leader to time out against, so no leader
                // transition is coming to rouse it (#83). Tell the client which
                // op to poll.
                node.request_replay();
                let mut response = typed_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    ErrorKind::Timeout,
                    "write did not commit within the deadline; parked for replay",
                );
                set_header(&mut response, HEADER_OP_ID, &base.to_string());
                return Err(response);
            }
            Ok(Err(NodeError::Unavailable(detail))) => {
                // R4: refused only AFTER parking — the op is durable here and
                // the replay loop applies it once a quorum returns. The wake is
                // a no-op while there is no leader (the replayer skips a drain
                // without one) and costs nothing; it earns its keep when the
                // leader is present but was momentarily unreachable.
                node.request_replay();
                let mut response = typed_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    ErrorKind::Unavailable,
                    &format!("no quorum / leader unreachable (parked for replay): {detail}"),
                );
                response
                    .headers_mut()
                    .insert("retry-after", HeaderValue::from_static("1"));
                set_header(&mut response, HEADER_OP_ID, &base.to_string());
                return Err(response);
            }
            Ok(Err(e)) => {
                // Also parked-and-unapplied, so it gets the same wake.
                node.request_replay();
                let mut response = typed_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorKind::InternalError,
                    &e.to_string(),
                );
                set_header(&mut response, HEADER_OP_ID, &base.to_string());
                return Err(response);
            }
            Ok(Ok(response)) => response,
        };
        // Terminal either way (a Failed outcome replays to the identical
        // refusal), so the intent retires now.
        if let Err(e) = node.unpark_intent(&op_id) {
            tracing::error!(%op_id, error = %e, "op terminal but could not unpark");
        }
        if let ControlOutcome::Failed { reason } = &response.outcome {
            return Err(refusal_response(reason));
        }
        last = Some((op_id, response));
    }
    let Some((op_id, committed)) = last else {
        return Err(typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            "nothing to apply",
        ));
    };

    let unapplied = match state.barrier {
        WriteBarrier::None => {
            // `none` skips the *fleet* barrier, not local coherence. The render
            // below re-reads the resource just committed, so a node that has
            // not applied it yet would answer 404 for a write it durably holds
            // (#99). No peer is consulted here, so the level keeps its promise:
            // this waits for one apply, never for the fleet.
            if node
                .await_local_applied(committed.revision, state.barrier_timeout)
                .await
            {
                Vec::new()
            } else {
                tracing::warn!(
                    revision = committed.revision,
                    "local apply did not land in time; the render below reports \
                     what this node can actually show"
                );
                // Named in `unapplied` for the same reason `ready-nodes` names a
                // straggler: the client should learn which node is behind from
                // the response, not from someone reading our logs. Under `none`
                // the only node that can be behind is this one.
                vec![node.id()]
            }
        }
        WriteBarrier::ReadyNodes => {
            node.await_applied(committed.revision, state.barrier_timeout)
                .await
        }
    };

    let mut response = match mutation.render {
        Render::Captured {
            body,
            content_type,
            status,
        } => buffered_response(status, body, content_type)?,
        Render::FetchAfter { path, status } => {
            let (fetched, content_type, body) =
                fetch(state, &path, auth, host, Some(tenant)).await?;
            // The commit is real either way, but the render must not dress a
            // non-2xx re-read in the success code — a 201 wrapping a 404 body
            // would claim a state this node cannot show. Still load-bearing
            // after #99: both barrier levels now await the local apply first,
            // so *outrunning* it is no longer a way to get here, but a barrier
            // that timed out is, and so is an apply that landed while the
            // engine refused the op (a bind failure, §7.4.6) — the entry is
            // applied and the port still is not there to read. The cluster
            // headers below still carry the committed revision.
            let status = if fetched.is_success() {
                status
            } else {
                tracing::warn!(%path, status = %fetched, "post-commit render read did not confirm the write");
                fetched
            };
            buffered_response(status, body, content_type)?
        }
    };

    let revision = match mutation.port {
        Some(port) => format!("{}:{port}@{}", TenantId::default(), committed.revision),
        None => format!("{}@{}", TenantId::default(), committed.revision),
    };
    set_header(&mut response, HEADER_REVISION, &revision);
    set_header(&mut response, HEADER_OP_ID, &op_id.to_string());
    let mut warnings = Vec::new();
    if !unapplied.is_empty() {
        let nodes = unapplied
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        warnings.push(format!("unapplied={nodes}"));
    }
    // The commit is fleet truth, but THIS node's engine may still have failed
    // to realize it (a bind, a refused toggle): §7.4.6 — success with a named
    // warning, never a silent divergence the client cannot see.
    if let Some(port) = mutation.port
        && let Some(failure) = node.apply_failures().get(&port)
    {
        warnings.push(format!("local-engine={failure}"));
    }
    if !warnings.is_empty() {
        set_header(&mut response, HEADER_WARNINGS, &warnings.join(","));
    }
    Ok(response)
}

/// Serve one RFC-002 §5 tenancy route (issue #162): read from local applied
/// state, or commit one `ControlOp` and answer for it.
///
/// Deliberately *not* routed through `build_and_run`. That path is built around
/// a single imposter record — `If-Match` parsing against a port, `_rift.script`
/// resolution, a post-commit re-read of a loopback path — and none of it
/// applies here. Threading a "skip all of that" flag through it would make the
/// imposter path harder to read in order to reuse the ten lines these two
/// genuinely share.
///
/// One op per route, always, so there is no partial-commit case to reason
/// about: the only multi-record write on this surface (`PrincipalCreate`) is
/// atomic *inside* the state machine, which is why it is one op rather than two.
async fn terminate_tenancy(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    req: Request<Incoming>,
    route: tenancy::Route,
    principal_id: Option<String>,
    // `authorized_tenant` is the tenant the authorization decision was actually
    // made against — the route's own scope where it has one, else the caller's
    // `X-Rift-Tenant`. `GET /admin/audit` narrows its rows to exactly this, so
    // the rows a caller receives and the tenant they were authorized as can
    // never disagree.
    authorized_tenant: &TenantId,
    bindings: &[(TenantId, Role)],
) -> Response<FrontBody> {
    let idempotency = req
        .headers()
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    // Minting a credential cannot be made idempotent, and pretending otherwise
    // hands the client a key that does not work.
    //
    // `Idempotency-Key` derives a deterministic `op_id`, and a replayed `op_id`
    // is collapsed by `sm_op_dedup` to the *original* committed response with
    // nothing re-applied. But the key and the principal id are minted here, per
    // request, before any op id exists — so a retry (exactly what a client does
    // after the 504/503 paths below) would commit nothing and still answer
    // `201` carrying a freshly-minted key that was never stored, against a
    // principal id that does not exist. The first attempt's key, the only one
    // that ever worked, is unrecoverable by construction.
    //
    // Refused rather than silently ignored: a client that sent the header
    // believes its retry is safe, and quietly not honouring that is how it
    // would go on believing it. `op_id_for`'s doc already asks that
    // non-idempotent ops stay out of this path; this keeps that true.
    if idempotency.is_some() && matches!(route, tenancy::Route::PrincipalCreate(_)) {
        return typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            "Idempotency-Key is not supported when minting a principal: the key is generated \
             per request and shown once, so a replayed request cannot return the credential \
             the original one issued. Retry without the header and delete the surplus \
             principal if both attempts committed.",
        );
    }

    let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return typed_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorKind::RequestTooLarge,
                &format!("admin request body refused: {e}"),
            );
        }
    };

    let outcome = match tenancy::dispatch(
        node,
        route,
        &body,
        authorized_tenant,
        bindings,
        state.export_status.as_deref(),
    ) {
        Ok(outcome) => outcome,
        Err(tenancy::TenancyError::BadRequest(reason)) => {
            return typed_error(StatusCode::BAD_REQUEST, ErrorKind::BadData, &reason);
        }
        // Byte-identical to a cross-tenant refusal, by construction: both
        // render through `tenant_boundary_not_found`. A caller must not be
        // able to tell "no such tenant" from "not yours" (RFC-002 §8.4).
        Err(tenancy::TenancyError::NotFound) => return tenant_boundary_not_found(),
        Err(tenancy::TenancyError::Storage(reason)) => return internal(&reason),
    };

    let (op, status, rendered) = match outcome {
        tenancy::Outcome::Body { status, body } => {
            return buffered_response(status, Bytes::from(body), json_content_type())
                .unwrap_or_else(|response| response);
        }
        tenancy::Outcome::Commit { op, status, then } => (op, status, then),
    };

    // The same order every write on this front follows (R4): validate, park
    // durably, submit. Parking before submitting is what makes an accepted op
    // survive a crash — the replay loop finishes what this request cannot.
    if let Err(reason) = control::validate(&op) {
        return refusal_response(&reason);
    }
    let op_id = base_op_id(idempotency.as_deref());
    let request = tenancy::mint_request(op, principal_id, op_id);
    if let Err(e) = node.park_intent(&request) {
        return typed_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::InternalError,
            &format!("cannot durably accept the write: {e}"),
        );
    }

    let committed = match tokio::time::timeout(WRITE_DEADLINE, node.submit(request)).await {
        Err(_) => {
            node.request_replay();
            let mut response = typed_error(
                StatusCode::GATEWAY_TIMEOUT,
                ErrorKind::Timeout,
                "write did not commit within the deadline; parked for replay",
            );
            set_header(&mut response, HEADER_OP_ID, &op_id.to_string());
            return response;
        }
        Ok(Err(NodeError::Unavailable(detail))) => {
            node.request_replay();
            let mut response = typed_error(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorKind::Unavailable,
                &format!("no quorum / leader unreachable (parked for replay): {detail}"),
            );
            response
                .headers_mut()
                .insert("retry-after", HeaderValue::from_static("1"));
            set_header(&mut response, HEADER_OP_ID, &op_id.to_string());
            return response;
        }
        Ok(Err(e)) => {
            node.request_replay();
            let mut response = typed_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorKind::InternalError,
                &e.to_string(),
            );
            set_header(&mut response, HEADER_OP_ID, &op_id.to_string());
            return response;
        }
        Ok(Ok(response)) => response,
    };
    if let Err(e) = node.unpark_intent(&op_id) {
        tracing::error!(%op_id, error = %e, "op terminal but could not unpark");
    }
    if let ControlOutcome::Failed { reason } = &committed.outcome {
        return refusal_response(reason);
    }

    // Wait for *this* node to apply before answering, for the same reason the
    // imposter path does (#99): a `whoami` or a principal listing issued
    // immediately after this response must not read state older than the write
    // it just acknowledged.
    let unapplied = match state.barrier {
        WriteBarrier::None => {
            if node
                .await_local_applied(committed.revision, state.barrier_timeout)
                .await
            {
                Vec::new()
            } else {
                vec![node.id()]
            }
        }
        WriteBarrier::ReadyNodes => {
            node.await_applied(committed.revision, state.barrier_timeout)
                .await
        }
    };

    let body = rendered.map_or_else(Bytes::new, Bytes::from);
    // Content type follows the body, not the status. A `204` has no body, and
    // neither do the upsert/delete routes that render nothing — advertising
    // `application/json` over zero bytes makes every strict client's `.json()`
    // fail on a response that succeeded.
    let content_type = if body.is_empty() {
        None
    } else {
        json_content_type()
    };
    let mut response = match buffered_response(status, body, content_type) {
        Ok(response) => response,
        Err(response) => return response,
    };
    // The tenant the op actually addressed, not `default`. This header's
    // documented shape names a tenant, so reporting `default@N` for a write
    // against `acme` would be a plain falsehood in the one place a client
    // looks to correlate a write with the record it touched.
    set_header(
        &mut response,
        HEADER_REVISION,
        &format!("{authorized_tenant}@{}", committed.revision),
    );
    set_header(&mut response, HEADER_OP_ID, &op_id.to_string());
    if !unapplied.is_empty() {
        let nodes = unapplied
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        set_header(
            &mut response,
            HEADER_WARNINGS,
            &format!("unapplied={nodes}"),
        );
    }
    response
}

/// Translate one terminated route into ops + a render plan. Reads that inform
/// the mutation (current stubs for index-addressed edits, capture-before-delete
/// bodies) come from the local applied state / loopback admin.
async fn build_mutation(
    state: &FrontState,
    node: &Arc<RaftNode>,
    kind: Terminated,
    tenant: &TenantId,
    body: &[u8],
    auth: Option<&str>,
    host: Option<&HeaderValue>,
) -> Result<Mutation, Response<FrontBody>> {
    match kind {
        Terminated::Create => {
            let config: ImposterConfig = parse(body)?;
            let Some(port) = config.port else {
                return Err(typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    "a clustered imposter needs an explicit port: auto-assigned ports \
                     cannot replicate",
                ));
            };
            Ok(Mutation {
                // RFC-002 §8.1: `tenant` is the `authorize_action` decision's
                // own tenant, the same value the authority check just ran
                // against — never re-derived from the header a second time.
                // Checking against one binding and recording ownership from
                // another is exactly the confused-deputy bug the header
                // exists to close: an Editor of A must not be able to create
                // a resource owned by B.
                ops: vec![ControlOp::PutImposter {
                    tenant: tenant.clone(),
                    config: Box::new(config),
                }],
                port: Some(port),
                render: Render::FetchAfter {
                    path: format!("/imposters/{port}"),
                    status: StatusCode::CREATED,
                },
            })
        }
        Terminated::ReplaceAllImposters => {
            let replace: ReplaceAllBody = parse(body)?;
            // Upsert the new set first, then prune the leftovers — never a
            // DeleteAll up front. The ops commit as separate Raft entries, so a
            // mid-sequence loss of quorum tears the sequence; torn this way the
            // fleet keeps a superset (new configs plus stale leftovers) that a
            // retry heals, instead of an empty fleet that lost everything.
            let mut keep = std::collections::BTreeSet::new();
            let mut ops = Vec::new();
            for config in replace.imposters {
                let Some(port) = config.port else {
                    return Err(typed_error(
                        StatusCode::BAD_REQUEST,
                        ErrorKind::BadData,
                        "a clustered imposter needs an explicit port: auto-assigned ports \
                         cannot replicate",
                    ));
                };
                keep.insert(port);
                // Same §8.1 reasoning as `Create`: this is a whole-set
                // reconcile *of the authorized tenant*, so every imposter it
                // upserts is owned by `tenant`, not by whatever the caller's
                // default binding happens to be.
                ops.push(ControlOp::PutImposter {
                    tenant: tenant.clone(),
                    config: Box::new(config),
                });
            }
            if ops.is_empty() {
                ops.push(ControlOp::DeleteAll {
                    tenant: tenant.clone(),
                });
            } else {
                // The limitation this arm used to carry is gone (issue #182): `configured_ports`
                // no longer answers for `default` only, so the prune no longer under-discovers
                // leftovers for a non-default tenant. It is now fleet-wide and tenant-tagged, so
                // filter to the tenant this mutation authorized against — pruning another tenant's
                // ports from a wholesale replace of *this* tenant's set would be a cross-tenant
                // delete, which is precisely what the ownership gate exists to prevent elsewhere.
                let existing = node
                    .configured_ports()
                    .map_err(|e| internal(&e.to_string()))?;
                for (owner, port) in existing {
                    if owner == *tenant && !keep.contains(&port) {
                        ops.push(ControlOp::DeleteImposter {
                            tenant: tenant.clone(),
                            port,
                        });
                    }
                }
            }
            Ok(Mutation {
                ops,
                port: None,
                render: Render::FetchAfter {
                    path: "/imposters".to_owned(),
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::DeleteAllImposters => {
            let (_, content_type, captured) =
                fetch(state, "/imposters", auth, host, Some(tenant)).await?;
            Ok(Mutation {
                // RFC-002 §8.1: same "authorize and act on the same tenant"
                // rule as `Create` — this must delete the tenant that was
                // just authorized, not the fixed default, or an Editor
                // authorized against `acme` would destroy `default`'s
                // imposters (issue #161, B1).
                ops: vec![ControlOp::DeleteAll {
                    tenant: tenant.clone(),
                }],
                port: None,
                render: Render::Captured {
                    body: captured,
                    content_type,
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::DeleteImposter(port) => {
            let (status, content_type, captured) = fetch(
                state,
                &format!("/imposters/{port}"),
                auth,
                host,
                Some(tenant),
            )
            .await?;
            if status == StatusCode::NOT_FOUND {
                // Mirror upstream: deleting an absent imposter is a 404, and
                // committing nothing keeps the log free of no-ops.
                return Err(
                    match buffered_response(StatusCode::NOT_FOUND, captured, content_type) {
                        Ok(response) | Err(response) => response,
                    },
                );
            }
            Ok(Mutation {
                ops: vec![ControlOp::DeleteImposter {
                    tenant: tenant.clone(),
                    port,
                }],
                port: Some(port),
                render: Render::Captured {
                    body: captured,
                    content_type,
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::AddStub(port) => {
            let add: AddStubBody = parse(body)?;
            Ok(Mutation {
                ops: vec![ControlOp::PatchStubs {
                    tenant: tenant.clone(),
                    port,
                    edit: StubEditScript(vec![StubEdit::Add {
                        stub: add.stub,
                        index: add.index,
                    }]),
                }],
                port: Some(port),
                render: Render::FetchAfter {
                    path: format!("/imposters/{port}"),
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::ReplaceStubs(port) => {
            let replace: ReplaceStubsBody = parse(body)?;
            let mut config = stored_config(node, tenant, port)?;
            config.stubs = replace.stubs;
            Ok(put_config_mutation(tenant, port, config))
        }
        Terminated::ReplaceStubAt(port, index) => {
            let stub: Stub = parse(body)?;
            let mut config = stored_config(node, tenant, port)?;
            if index >= config.stubs.len() {
                return Err(stub_index_missing(index));
            }
            config.stubs[index] = stub;
            Ok(put_config_mutation(tenant, port, config))
        }
        Terminated::DeleteStubAt(port, index) => {
            let mut config = stored_config(node, tenant, port)?;
            if index >= config.stubs.len() {
                return Err(stub_index_missing(index));
            }
            config.stubs.remove(index);
            Ok(put_config_mutation(tenant, port, config))
        }
        Terminated::ReplaceStubById(port, id) => {
            let stub: Stub = parse(body)?;
            Ok(Mutation {
                ops: vec![ControlOp::PatchStubs {
                    tenant: tenant.clone(),
                    port,
                    edit: StubEditScript(vec![StubEdit::ReplaceById { id, stub }]),
                }],
                port: Some(port),
                render: Render::FetchAfter {
                    path: format!("/imposters/{port}"),
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::SetEnabled(port, enabled) => {
            let state = if enabled { "enabled" } else { "disabled" };
            Ok(Mutation {
                ops: vec![ControlOp::SetEnabled {
                    tenant: tenant.clone(),
                    port,
                    enabled,
                }],
                port: Some(port),
                // Upstream's own response shape, byte-identical — no re-read
                // needed for a message body.
                render: Render::Captured {
                    body: Bytes::from(
                        serde_json::json!({ "message": format!("Imposter {state}") }).to_string(),
                    ),
                    content_type: json_content_type(),
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::DeleteStubById(port, id) => Ok(Mutation {
            ops: vec![ControlOp::PatchStubs {
                tenant: tenant.clone(),
                port,
                edit: StubEditScript(vec![StubEdit::DeleteById { id }]),
            }],
            port: Some(port),
            render: Render::FetchAfter {
                path: format!("/imposters/{port}"),
                status: StatusCode::OK,
            },
        }),
        Terminated::PutRoutes => {
            let table: RouteTable = parse(body)?;
            // No loopback re-read: there is no upstream `/front-door/routes`
            // to fetch from (U-11's admin CRUD was deferred). A whole-table
            // replace is deterministic and pre-validated (`build_and_run`
            // runs `control::validate` before this ever commits), so the
            // table just parsed IS what gets stored — captured now rather
            // than re-read, the same shortcut `SetEnabled` takes for its
            // canned message.
            let body = serde_json::to_vec(&table).map_err(|e| internal(&e.to_string()))?;
            Ok(Mutation {
                ops: vec![ControlOp::PutRoutes {
                    tenant: tenant.clone(),
                    table,
                }],
                // No single stored record: a whole-table replace has no port to
                // label the revision header with, so it emits (and accepts) the
                // portless `default@<revision>` token instead — conditioned on
                // the tenant's route-table revision, not on any one route. See
                // `control::precondition_target`.
                port: None,
                render: Render::Captured {
                    body: Bytes::from(body),
                    content_type: json_content_type(),
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::DeleteRoute(id) => {
            // Mirrors `DeleteImposter`: idempotent at the state-machine level
            // (`mutate_tables`'s `DeleteRoute` arm never fails), but the admin
            // surface still answers 404 for a route that was never there —
            // captured *before* the delete commits, the same as
            // `DeleteImposter`'s pre-delete fetch, just read from the state
            // machine directly since there is no loopback endpoint to fetch
            // from.
            // `tenant`'s table, not the default one (issue #182): a delete must 404 against the
            // set the caller can actually see, or a route id that exists only in another tenant
            // would read as present here.
            let table = node
                .route_table(tenant.as_str())
                .map_err(|e| internal(&e.to_string()))?;
            let Some(route) = table.routes.iter().find(|r| r.id == id) else {
                return Err(typed_error(
                    StatusCode::NOT_FOUND,
                    ErrorKind::NoSuchResource,
                    &format!("no route with id {id:?}"),
                ));
            };
            let body = serde_json::to_vec(route).map_err(|e| internal(&e.to_string()))?;
            Ok(Mutation {
                ops: vec![ControlOp::DeleteRoute {
                    tenant: tenant.clone(),
                    id,
                }],
                port: None,
                render: Render::Captured {
                    body: Bytes::from(body),
                    content_type: json_content_type(),
                    status: StatusCode::OK,
                },
            })
        }
        // `terminate` diverts the tenancy surface to `terminate_tenancy` before
        // this is reached, so this arm is unreachable through the front's own
        // routing. Answered as an internal error rather than `unreachable!`:
        // if a future edit *does* route one here, a 500 names a bug in this
        // file, whereas a panic would take the whole admin listener down with
        // it — and an admin front that dies on a routing mistake is a worse
        // failure than the mistake.
        Terminated::Tenancy(_) => Err(internal(
            "tenancy routes are served by terminate_tenancy, not build_mutation",
        )),
    }
}

/// Index-addressed stub edits and whole-list replacement have no by-id spelling
/// in the op set, so they commit as a full `PutImposter` of the stored config
/// with the stub list edited — the engine's #316 diff still patches only the
/// touched stubs in place.
///
/// `tenant` is `authorize_action`'s decided tenant, threaded through by every
/// caller (RFC-002 §8.1) — never re-derived here, for the same confused-deputy
/// reason `Create` documents.
fn put_config_mutation(tenant: &TenantId, port: u16, config: ImposterConfig) -> Mutation {
    Mutation {
        ops: vec![ControlOp::PutImposter {
            tenant: tenant.clone(),
            config: Box::new(config),
        }],
        port: Some(port),
        render: Render::FetchAfter {
            path: format!("/imposters/{port}"),
            status: StatusCode::OK,
        },
    }
}

/// The committed config for `tenant`'s `port` from the local applied state, parsed.
///
/// Tenant-addressed as of issue #182. The ownership gate in `authorize_action` has already refused
/// a port owned by another tenant, so in practice this reads the caller's own record — but it takes
/// the tenant rather than trusting that, because a read that silently fell back to `default` is the
/// exact failure the gate exists to prevent, and defence in depth here costs one argument.
// The Err IS the client response (the early-return channel this module
// uses everywhere); boxing it would just move the bytes to every call site.
#[allow(clippy::result_large_err)]
fn stored_config(
    node: &Arc<RaftNode>,
    tenant: &TenantId,
    port: u16,
) -> Result<ImposterConfig, Response<FrontBody>> {
    let stored = node
        .get_imposter(tenant.as_str(), port)
        .map_err(|e| internal(&e.to_string()))?;
    let Some(stored) = stored else {
        return Err(typed_error(
            StatusCode::NOT_FOUND,
            ErrorKind::NoSuchResource,
            &format!("no imposter on port {port}"),
        ));
    };
    serde_json::from_str(&stored).map_err(|e| internal(&format!("stored config for {port}: {e}")))
}

fn mint(
    op_id: Uuid,
    op: ControlOp,
    expected_revision: Option<u64>,
    principal: Option<String>,
) -> ControlRequest {
    // Pre-epoch clocks mint 0: only this op's dedup TTL weakens, never its
    // response (same reasoning as the node's own mint site).
    let issued_at_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    ControlRequest {
        op_id,
        // U-10 attribution (issue #855, RFC-002 §6): the task-local
        // `with_principal_scope` seam does not survive the clustered write
        // path (the state-machine apply task is not the request task), so
        // this field is the one that does — populated here from
        // `authorize_action`'s resolved principal (issue #161); #163 reads
        // it back into the audit stream.
        principal,
        issued_at_secs,
        expected_revision,
        op,
    }
}

/// The port an `If-Match` on `mutation` must name, or `None` when the mutation
/// addresses a whole route table (issue #210) and so carries the portless
/// token. `Err` when the mutation has no conditionable target at all — a
/// collection-wide op such as `PUT`/`DELETE /imposters`, which stays a `400`.
///
/// The route-table case is decided by asking [`control::precondition_target`],
/// the state machine's own definition, rather than re-listing the ops here: the
/// front's `400` and apply's `409` must agree on what is conditionable, and two
/// independently maintained lists is precisely how they drift apart.
#[allow(clippy::result_large_err)]
fn precondition_port(mutation: &Mutation) -> Result<Option<u16>, Response<FrontBody>> {
    if let Some(port) = mutation.port {
        return Ok(Some(port));
    }
    let route_table = !mutation.ops.is_empty()
        && mutation.ops.iter().all(|op| {
            matches!(
                control::precondition_target(op),
                Some(PreconditionTarget::RouteTable(_))
            )
        });
    if route_table {
        Ok(None)
    } else {
        Err(typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            "If-Match applies to single-imposter and route-table operations only",
        ))
    }
}

/// Parse an `If-Match` header value against the [`HEADER_REVISION`] contract:
/// the token this front itself emits — `default:<port>@<revision>` for a single
/// imposter, `default@<revision>` for a route table (issue #210) — a bare
/// revision integer, or any of those wrapped in one pair of double quotes (a
/// normal ETag convention some HTTP clients apply automatically). Anything else
/// — a wildcard, a weak validator, a comma-separated list, a mismatched tenant
/// or port — is refused: a precondition this front cannot evaluate must never
/// silently pass as unconditional.
///
/// `expected_port` is the target's shape, from [`precondition_port`]. A ported
/// token on a route-table write (or a portless one on an imposter write) is
/// refused rather than coerced: the client conditioned on a *different* record
/// than the one it is writing, and quietly accepting that would hand back the
/// lost update the precondition exists to prevent.
#[allow(clippy::result_large_err)]
fn parse_if_match(raw: &str, expected_port: Option<u16>) -> Result<u64, Response<FrontBody>> {
    let bad = || {
        let form = match expected_port {
            Some(port) => format!("default:{port}@<revision>"),
            None => "default@<revision>".to_owned(),
        };
        typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            &format!(
                "If-Match must be the value from {HEADER_REVISION} ({form}) or a bare revision \
                 integer"
            ),
        )
    };

    let trimmed = raw.trim();
    let unquoted = trimmed
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(trimmed);

    if let Ok(revision) = unquoted.parse::<u64>() {
        return Ok(revision);
    }

    let (addressed, revision) = unquoted.split_once('@').ok_or_else(bad)?;
    match (addressed.split_once(':'), expected_port) {
        (Some((tenant, token_port)), Some(port)) => {
            if tenant != TenantId::default().as_str() {
                return Err(bad());
            }
            if token_port.parse::<u16>().map_err(|_| bad())? != port {
                return Err(bad());
            }
        }
        (None, None) => {
            if addressed != TenantId::default().as_str() {
                return Err(bad());
            }
        }
        // Token shape and target shape disagree.
        (Some(_), None) | (None, Some(_)) => return Err(bad()),
    }
    revision.parse::<u64>().map_err(|_| bad())
}

/// Fixed namespace for deriving op ids from `Idempotency-Key` values that are
/// not themselves UUIDs. Changing it would break every in-flight client key,
/// so: never.
const IDEMPOTENCY_NAMESPACE: Uuid = Uuid::from_u128(0x52_49_46_54_2d_45_45_2d_49_44_45_4d_50_4f_54);

/// The mutation's base op id: the client's `Idempotency-Key` verbatim when it
/// is a UUID, a v5 derivation of it otherwise, or a fresh v4 when absent.
fn base_op_id(idempotency: Option<&str>) -> Uuid {
    match idempotency.map(str::trim) {
        Some(key) if !key.is_empty() => key
            .parse()
            .unwrap_or_else(|_| Uuid::new_v5(&IDEMPOTENCY_NAMESPACE, key.as_bytes())),
        _ => Uuid::new_v4(),
    }
}

/// Per-op ids for a multi-op mutation, derived deterministically from the base
/// so a retried Idempotency-Key dedups every op in the sequence, not just the
/// first.
///
/// Stability caveat: ids shift if the same key later yields a different op
/// COUNT (a prune set that changed flips `base` ↔ `v5(base, 0)`). That cannot
/// double-apply today because every op that appears in a multi-op mutation
/// (Put/Delete/DeleteAll) is idempotent — the one non-idempotent op
/// (`PatchStubs` append) is always single-op. Keep it that way.
fn op_id_for(base: Uuid, index: usize, total: usize) -> Uuid {
    if total == 1 {
        base
    } else {
        Uuid::new_v5(&base, &index.to_be_bytes())
    }
}

fn json_content_type() -> Option<HeaderValue> {
    Some(HeaderValue::from_static("application/json"))
}

/// Whether a terminated op would introduce a scripting surface — the same
/// classifier the core admin gates on, applied to the incoming payload.
fn op_uses_script_surface(op: &ControlOp) -> bool {
    match op {
        ControlOp::PutImposter { config, .. } => config_uses_script_surface(config),
        ControlOp::PatchStubs { edit, .. } => {
            let stubs: Vec<Stub> = edit
                .0
                .iter()
                .filter_map(|step| match step {
                    StubEdit::Add { stub, .. } | StubEdit::ReplaceById { stub, .. } => {
                        Some(stub.clone())
                    }
                    StubEdit::DeleteById { .. } | StubEdit::Move { .. } => None,
                })
                .collect();
            if stubs.is_empty() {
                return false;
            }
            let scratch = ImposterConfig {
                stubs,
                ..ImposterConfig::default()
            };
            config_uses_script_surface(&scratch)
        }
        _ => false,
    }
}

/// Mirrors upstream's private `admin_script_base`: `--scripts-dir` when
/// configured, else every `file:` ref is refused.
fn front_script_base(scripts_dir: Option<&Path>) -> ScriptBaseDir {
    match scripts_dir {
        Some(dir) => ScriptBaseDir::ScriptsDir(dir.to_path_buf()),
        None => ScriptBaseDir::Unconfigured,
    }
}

/// The target imposter's already-resolved `_rift.scripts`, from applied
/// state; empty when the imposter is absent (resolve → then not-found
/// ordering: an unknown ref against an empty registry still fails with
/// `UnknownRef`, the same observable order upstream produces for a genuinely
/// missing imposter).
#[allow(clippy::result_large_err)]
fn stored_script_registry(
    node: &Arc<RaftNode>,
    tenant: &TenantId,
    port: u16,
) -> Result<HashMap<String, RiftScriptConfig>, Response<FrontBody>> {
    // An absent imposter is the domain-optional empty registry: an unknown ref
    // then fails as UnknownRef, upstream's resolve-then-not-found order. A
    // storage or parse failure is a real fault and must not masquerade as
    // "unknown script ref" — it propagates as 500, same as stored_config.
    let Some(stored) = node
        .get_imposter(tenant.as_str(), port)
        .map_err(|e| internal(&e.to_string()))?
    else {
        return Ok(HashMap::new());
    };
    let config: ImposterConfig = serde_json::from_str(&stored)
        .map_err(|e| internal(&format!("stored config for {port}: {e}")))?;
    Ok(config.rift.map(|rift| rift.scripts).unwrap_or_default())
}

/// Resolve one terminated op in place; `Err` is the client-shaped 400 with
/// upstream's exact message. `batch_index` is `Some` for `PUT /imposters`
/// ops.
#[allow(clippy::result_large_err)]
fn resolve_op_scripts(
    op: &mut ControlOp,
    node: &Arc<RaftNode>,
    base: &ScriptBaseDir,
    batch_index: Option<usize>,
) -> Result<(), Response<FrontBody>> {
    match op {
        ControlOp::PutImposter { config, .. } => resolve_scripts(config, base).map_err(|e| {
            let message = match batch_index {
                Some(idx) => format!(
                    "Script resolution failed in imposter[{idx}] (port {:?}): {e}",
                    config.port
                ),
                None => format!("Script resolution failed: {e}"),
            };
            typed_error(StatusCode::BAD_REQUEST, ErrorKind::BadData, &message)
        }),
        // The op already carries the tenant it was built for (#161 B1), so the script registry is
        // read from that tenant's imposter rather than re-deriving it or falling back to `default`.
        ControlOp::PatchStubs {
            tenant, port, edit, ..
        } => {
            let needs_registry = edit
                .0
                .iter()
                .any(|step| matches!(step, StubEdit::Add { .. } | StubEdit::ReplaceById { .. }));
            if !needs_registry {
                return Ok(());
            }
            let registry = stored_script_registry(node, tenant, *port)?;
            for step in &mut edit.0 {
                if let StubEdit::Add { stub, .. } | StubEdit::ReplaceById { stub, .. } = step {
                    resolve_stub_scripts(std::slice::from_mut(stub), &registry, base).map_err(
                        |e| {
                            typed_error(
                                StatusCode::BAD_REQUEST,
                                ErrorKind::BadData,
                                &format!("Script resolution failed: {e}"),
                            )
                        },
                    )?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Validate one *resolved* op's scripts; `Err` is the client-shaped 400 with
/// upstream's exact message. `batch_index` is `Some` for `PUT /imposters` ops.
///
/// Must run after [`resolve_op_scripts`]: upstream's validator only parses a
/// script it can see as inline `code`, so validating an unresolved `file:`/
/// `ref:` source silently checks nothing. Running it here — before the op is
/// minted or parked — is what keeps a syntactically broken script out of the
/// log entirely, rather than failing at bind time on every node (#57).
///
/// Two deliberate divergences from upstream. First, upstream mints a stub id
/// *before* validating an id-less added stub, so its error names a random UUID
/// where this names `stub[{index}]` — a label difference only.
///
/// Second, the stub edits that commit as a whole `PutImposter` (list replace,
/// and the index-addressed replace/delete) re-validate the full post-edit
/// config rather than just the incoming stub, which costs one script parse per
/// scripted response on every such edit. That only *rejects* differently when
/// stored state already holds a broken script — impossible for anything
/// written since this gate landed, because both op shapes now validate what
/// they commit. Legacy state with two broken sibling stubs cannot be repaired
/// by index-addressed deletes (each leaves the other behind); delete by id or
/// replace the whole stub list, neither of which validates the siblings.
#[allow(clippy::result_large_err)]
fn validate_op_scripts(
    op: &ControlOp,
    batch_index: Option<usize>,
) -> Result<(), Response<FrontBody>> {
    let refuse =
        |message: String| typed_error(StatusCode::BAD_REQUEST, ErrorKind::BadData, &message);
    match op {
        ControlOp::PutImposter { config, .. } => {
            let result = validate_stubs(&config.stubs);
            if result.is_valid() {
                return Ok(());
            }
            let detail = result.into_error_message().unwrap_or_default();
            Err(refuse(match batch_index {
                Some(idx) => format!(
                    "Script validation failed in imposter[{idx}] (port {:?}): {detail}",
                    config.port
                ),
                None => format!("Script validation failed: {detail}"),
            }))
        }
        ControlOp::PatchStubs { edit, .. } => {
            for step in &edit.0 {
                // Upstream labels an added stub by its insertion index and a
                // by-id replacement by 0; match both.
                let result = match step {
                    StubEdit::Add { stub, index } => validate_stub(stub, index.unwrap_or(0)),
                    StubEdit::ReplaceById { stub, .. } => validate_stub(stub, 0),
                    StubEdit::DeleteById { .. } | StubEdit::Move { .. } => continue,
                };
                if !result.is_valid() {
                    let detail = result.into_error_message().unwrap_or_default();
                    return Err(refuse(format!("Script validation failed: {detail}")));
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// `GET` a loopback admin path, forwarding the caller's authorization header.
/// Returns status, content type, and the collected body.
///
/// `scope` names the tenant this read is authorized as (see
/// `set_scope_header`'s doc) — every caller here is rendering the result of,
/// or capturing state ahead of, a mutation `authorize_action` already
/// decided, so it is always `Some` for a tenant-scoped op and `None` only for
/// the collection-wide `DeleteAllImposters`/`GET /imposters`-shaped capture,
/// which has no single tenant to name any more precisely than the mutation
/// itself already was authorized for.
async fn fetch(
    state: &FrontState,
    path: &str,
    auth: Option<&str>,
    host: Option<&HeaderValue>,
    scope: Option<&TenantId>,
) -> Result<(StatusCode, Option<HeaderValue>, Bytes), Response<FrontBody>> {
    let uri: Uri = format!("http://{}{path}", state.upstream_admin)
        .parse()
        .map_err(|e| internal(&format!("render path: {e}")))?;
    let mut request = Request::builder().method(Method::GET).uri(uri);
    if let Some(auth) = auth {
        request = request.header("authorization", auth);
    }
    if let Some(tenant) = scope {
        request = request.header(SCOPE_HEADER, tenant.as_str());
    }
    // The client's own Host, so the HATEOAS links upstream builds from it
    // carry the public authority rather than the loopback one.
    if let Some(host) = host {
        request = request.header("host", host);
    }
    let request = request
        .body(Full::new(Bytes::new()))
        .map_err(|e| internal(&e.to_string()))?;
    let response = state.fetch.request(request).await.map_err(|e| {
        typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            &format!("local admin backend unreachable: {e}"),
        )
    })?;
    let status = response.status();
    let content_type = response.headers().get("content-type").cloned();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|e| internal(&format!("render read: {e}")))?
        .to_bytes();

    // Narrow a collection re-read to the tenant this mutation was authorized as. `PUT /imposters`
    // and `DELETE /imposters` both answer with this body — a wholesale replace renders the set
    // afterwards, a delete-all captures it beforehand as "what was removed" — and upstream builds
    // it from an engine that now binds every tenant. Without this, either op hands an Editor of one
    // tenant the whole fleet's imposters, including (with `?replayable=true`) their stubs. The
    // `SCOPE_HEADER` sent above is an authorization signal for the loopback's own gate; it does not
    // filter a body, and `ImposterManager` has no tenant concept to filter by.
    //
    // Done here rather than at the two call sites so a third one cannot reintroduce the leak.
    if status.is_success()
        && is_imposter_listing(path)
        && let Some(tenant) = scope
    {
        let owned = tenant_owned_ports(state, tenant)?;
        let narrowed = narrow_imposter_listing(&body, &owned).map_err(|e| internal(&e))?;
        return Ok((status, content_type, Bytes::from(narrowed)));
    }
    Ok((status, content_type, body))
}

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

// The Err IS the client response (the early-return channel this module
// uses everywhere); boxing it would just move the bytes to every call site.
#[allow(clippy::result_large_err)]
fn parse<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, Response<FrontBody>> {
    serde_json::from_slice(body).map_err(|e| {
        typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            &format!("invalid request JSON: {e}"),
        )
    })
}

/// Assemble a buffered-body response with an optional upstream content type.
#[allow(clippy::result_large_err)]
fn buffered_response(
    status: StatusCode,
    body: Bytes,
    content_type: Option<HeaderValue>,
) -> Result<Response<FrontBody>, Response<FrontBody>> {
    let mut response = Response::builder()
        .status(status)
        .body(Full::new(body).map_err(|never| match never {}).boxed())
        .map_err(|e| internal(&e.to_string()))?;
    if let Some(content_type) = content_type {
        response.headers_mut().insert("content-type", content_type);
    }
    Ok(response)
}

/// Map a committed (or pre-validated) refusal reason to the client shape:
/// absent targets are 404s, everything else is bad data.
fn refusal_response(reason: &str) -> Response<FrontBody> {
    if reason.starts_with("revision conflict") {
        // Checked first: an absent-record precondition refusal also contains
        // "no imposter on port" and must stay a 409, not fall into the 404
        // branch below.
        typed_error(StatusCode::CONFLICT, ErrorKind::ResourceConflict, reason)
    } else if reason.contains("no imposter on port") || reason.contains("no stub with id") {
        typed_error(StatusCode::NOT_FOUND, ErrorKind::NoSuchResource, reason)
    } else {
        typed_error(StatusCode::BAD_REQUEST, ErrorKind::BadData, reason)
    }
}

fn stub_index_missing(index: usize) -> Response<FrontBody> {
    typed_error(
        StatusCode::NOT_FOUND,
        ErrorKind::NoSuchResource,
        &format!("no stub at index {index}"),
    )
}

fn typed_error(status: StatusCode, kind: ErrorKind, message: &str) -> Response<FrontBody> {
    error_response_typed(status, kind, message)
        .map(|body| body.map_err(|never| match never {}).boxed())
}

fn internal(message: &str) -> Response<FrontBody> {
    typed_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        ErrorKind::InternalError,
        message,
    )
}

/// The core admin's own 401 shape, byte-for-byte.
fn unauthorized() -> Response<FrontBody> {
    let body = r#"{"errors":[{"code":"unauthorized","type":"unauthorized","message":"Invalid authorization token"}]}"#;
    let mut response = Response::new(
        Full::new(Bytes::from_static(body.as_bytes()))
            .map_err(|never| match never {})
            .boxed(),
    );
    *response.status_mut() = StatusCode::UNAUTHORIZED;
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response
}

/// The injection-disallowed 400, mirroring upstream's shape so a client cannot
/// tell which listener refused it.
fn injection_disallowed() -> Response<FrontBody> {
    let body = serde_json::json!({
        "errors": [{
            "code": ErrorKind::InvalidInjection.slug(),
            "type": ErrorKind::InvalidInjection.slug(),
            "message": "inject requires --allowInjection to be set. See \
                        http://www.mbtest.org/docs/api/injection for more information.",
        }]
    });
    let mut response = Response::new(
        Full::new(Bytes::from(body.to_string()))
            .map_err(|never| match never {})
            .boxed(),
    );
    *response.status_mut() = StatusCode::BAD_REQUEST;
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response
}

/// Stamp upstream's own `x-rift-scope` header — distinct from the
/// cluster-facing `X-Rift-Tenant` a client sends, and never derived from
/// it directly — onto an outbound request to the loopback admin, naming
/// `tenant`: the value `authorize_action` already decided *this* request
/// against.
///
/// This is what lets `EeAuthorizer` (installed on the loopback as defence in
/// depth, `crate::authorizer`) re-derive the *same* tenant this front already
/// authorized against. Without it, `req.scope` is `None` at the loopback (the
/// client's `X-Rift-Tenant` never crosses into upstream's own header on its
/// own), which `EeAuthorizer` reads as `default` for lack of any other
/// signal — so every proxied request or internal re-read for a principal not
/// *also* bound to `default` would fail a second, spurious check for a
/// tenant nobody asked for.
fn set_scope_header(headers: &mut hyper::HeaderMap, tenant: &TenantId) {
    match HeaderValue::from_str(tenant.as_str()) {
        Ok(value) => {
            headers.insert(HeaderName::from_static(SCOPE_HEADER), value);
        }
        Err(e) => {
            // **Remove, never merely skip.** The client's own request headers
            // are forwarded to the loopback verbatim, so returning without
            // touching the map would leave a caller-supplied `x-rift-scope` in
            // place — and that header is what the loopback authorizer reads as
            // the requested tenant. Skipping would hand the caller's assertion
            // about itself to the very check meant to constrain it: the
            // confused deputy, in the one header whose whole contract is
            // "selects among existing bindings, never grants one".
            //
            // Unreachable in practice — tenant slugs are `[a-z0-9-]{1,64}` and
            // `"*"`, all spellable — which is exactly why the failure direction
            // has to be right: nothing will exercise it before it matters.
            headers.remove(HeaderName::from_static(SCOPE_HEADER));
            tracing::warn!(tenant = %tenant, error = %e, "dropping unspellable x-rift-scope header");
        }
    }
}

fn set_header(response: &mut Response<FrontBody>, name: &'static str, value: &str) {
    match HeaderValue::from_str(value) {
        Ok(value) => {
            response
                .headers_mut()
                .insert(HeaderName::from_static(name), value);
        }
        Err(e) => {
            // Purely informational headers; a value that cannot be spelled must
            // not fail the write it describes.
            tracing::warn!(header = name, error = %e, "dropping unspellable cluster header");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_terminates_exactly_the_config_surface() {
        let terminated = [
            (Method::POST, "/imposters"),
            (Method::PUT, "/imposters"),
            (Method::DELETE, "/imposters"),
            (Method::DELETE, "/imposters/4545"),
            (Method::POST, "/imposters/4545/stubs"),
            (Method::PUT, "/imposters/4545/stubs"),
            (Method::PUT, "/imposters/4545/stubs/0"),
            (Method::DELETE, "/imposters/4545/stubs/2"),
            (Method::PUT, "/imposters/4545/stubs/by-id/a"),
            (Method::DELETE, "/imposters/4545/stubs/by-id/a"),
            (Method::POST, "/imposters/4545/enable"),
            (Method::POST, "/imposters/4545/disable"),
        ];
        for (method, path) in terminated {
            assert!(
                classify(&method, path, None).is_some(),
                "{method} {path} must terminate"
            );
        }

        // Runtime-state mutations and every read stay proxied: replicating
        // them is #15/#16 territory, and reads must hit the live engine.
        let proxied = [
            (Method::GET, "/imposters"),
            (Method::GET, "/imposters/4545"),
            (Method::DELETE, "/imposters/4545/savedRequests"),
            (Method::DELETE, "/imposters/4545/requests"),
            (Method::POST, "/imposters/4545/verify"),
            (Method::PUT, "/imposters/4545/scenarios/checkout/state"),
            (Method::POST, "/imposters/4545/scenarios/reset"),
            (Method::DELETE, "/imposters/4545/spaces/flow-1"),
            (Method::GET, "/config"),
            (Method::GET, "/metrics"),
            (Method::POST, "/_reload"),
        ];
        for (method, path) in proxied {
            assert!(
                classify(&method, path, None).is_none(),
                "{method} {path} must proxy"
            );
        }

        // An unparseable port is not this surface's route at all.
        assert!(classify(&Method::DELETE, "/imposters/not-a-port", None).is_none());
    }

    /// The front-door route surface (issue #131): `PUT`/`DELETE` terminate,
    /// `GET` does not (it never reaches `classify` at all — `handle` answers
    /// it directly, since there is no upstream endpoint to proxy it to).
    #[test]
    fn classify_terminates_exactly_the_route_write_surface() {
        assert!(matches!(
            classify(&Method::PUT, "/front-door/routes", None),
            Some(Terminated::PutRoutes)
        ));
        assert!(matches!(
            classify(&Method::DELETE, "/front-door/routes/svc", None),
            Some(Terminated::DeleteRoute(id)) if id == "svc"
        ));

        for (method, path) in [
            (Method::GET, "/front-door/routes"),
            (Method::POST, "/front-door/routes"),
            (Method::DELETE, "/front-door/routes"),
            (Method::PUT, "/front-door/routes/svc"),
            (Method::DELETE, "/front-door/routes/"),
        ] {
            assert!(
                classify(&method, path, None).is_none(),
                "{method} {path} must not terminate as a route write"
            );
        }
    }

    #[test]
    fn op_ids_derive_deterministically_from_the_idempotency_key() {
        // A UUID key is used verbatim; a non-UUID key derives stably; absent
        // keys mint fresh (and therefore differ).
        let uuid_key = "0189dcf0-0454-4e0b-a10c-8a8f8dccce1f";
        assert_eq!(
            base_op_id(Some(uuid_key)),
            uuid_key.parse::<Uuid>().expect("uuid"),
        );
        assert_eq!(base_op_id(Some("my-key")), base_op_id(Some("  my-key  ")));
        assert_ne!(base_op_id(Some("my-key")), base_op_id(Some("other-key")));
        assert_ne!(base_op_id(None), base_op_id(None));
        assert_ne!(
            base_op_id(Some("")),
            base_op_id(Some("")),
            "an empty key is no key"
        );

        // Single-op mutations use the base verbatim (the pollable id); multi-op
        // sequences derive per-index ids that never collide with the base.
        let base = base_op_id(Some("my-key"));
        assert_eq!(op_id_for(base, 0, 1), base);
        assert_ne!(op_id_for(base, 0, 2), base);
        assert_ne!(op_id_for(base, 0, 2), op_id_for(base, 1, 2));
        assert_eq!(op_id_for(base, 1, 3), op_id_for(base, 1, 3));
    }

    #[test]
    fn front_script_base_maps_the_flag() {
        assert!(matches!(
            front_script_base(None),
            ScriptBaseDir::Unconfigured
        ));

        let dir = PathBuf::from("/tmp/rift-test-scripts");
        match front_script_base(Some(dir.as_path())) {
            ScriptBaseDir::ScriptsDir(got) => assert_eq!(got, dir),
            other => panic!("expected ScriptsDir, got {other:?}"),
        }
    }

    /// A `RaftNode` with empty applied state — real enough for `get_imposter`
    /// (which "does not require leadership", per its own doc comment) without
    /// paying for `cluster_init`/election. The `TempDir` must outlive the node.
    async fn test_node() -> (Arc<RaftNode>, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = rift_cluster::NodeConfig {
            node_id: 1,
            bind: "127.0.0.1:0".parse().expect("bind addr"),
            advertise: None,
            data_dir: dir.path().to_path_buf(),
            secret: Some("admin-front-test-secret".to_owned()),
            routes: rift_cluster::Router::new(),
            engine: None,
            audit_retention_secs: rift_cluster::DEFAULT_AUDIT_RETENTION_SECS,
            snapshot_log_entries: None,
        };
        let node = RaftNode::start(config).await.expect("node starts");
        (Arc::new(node), dir)
    }

    /// A bound front over a throwaway node, for the accept-loop observation
    /// tests. `upstream_admin` is never dialled — nothing sends a request.
    async fn test_front() -> (AdminFront, Arc<RaftNode>, tempfile::TempDir) {
        let (node, dir) = test_node().await;
        let front = bind(
            FrontConfig {
                public_addr: "127.0.0.1:0".to_owned(),
                upstream_admin: "127.0.0.1:1".parse().expect("addr"),
                api_key: None,
                legacy_key_is_fleet_admin: true,
                allow_injection: false,
                scripts_dir: None,
                barrier: crate::cli::WriteBarrier::None,
                barrier_timeout: Duration::from_secs(1),
                admin_async: false,
                export_status: None,
                readiness: Arc::new(crate::readiness::Readiness::awaiting([])),
            },
            &node,
        )
        .await
        .expect("front binds");
        (front, node, dir)
    }

    /// Issue #64: the accept loop dying is *observable*.
    ///
    /// Before this, the front held a bare `JoinHandle` that was only ever
    /// aborted. A panic in the loop left the node a zombie cluster member —
    /// public admin dead, still a Raft voter, `/readyz` still 200 — and nothing
    /// waiting on it, so `serve_until` never returned. An abort that `shutdown`
    /// did not request takes byte-for-byte the same path as a panic unwind:
    /// the drop guard runs and classifies it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_reports_unexpected_accept_loop_death() {
        let (front, node, _dir) = test_front().await;

        front.task.abort();

        let outcome = tokio::time::timeout(Duration::from_secs(5), front.wait())
            .await
            .expect("wait must resolve when the accept loop dies, not hang");
        let err = outcome.expect_err("an unrequested death is an error, not a clean stop");
        assert!(
            format!("{err}").contains("terminated unexpectedly"),
            "the error must name what happened: {err}"
        );

        // Take-once, asserted where there is genuinely something to lose: the
        // error above was moved out of the slot, so a second waiter must get
        // `Ok` rather than a clone that `anyhow::Error` cannot provide.
        assert!(
            tokio::time::timeout(Duration::from_secs(5), front.wait())
                .await
                .expect("a second wait must resolve")
                .is_ok(),
            "the error goes to the first caller only"
        );

        node.shutdown().await.expect("node shuts down");
    }

    /// Issue #64: an operator shutdown is not an error.
    ///
    /// The same abort that means "died" above means "asked to stop" here; the
    /// only difference is that `shutdown` records the intent first. A guard that
    /// could not tell them apart would make every clean shutdown exit nonzero.
    ///
    /// `wait` is called *without* pre-cancelling `done`, so it blocks until the
    /// guard has actually run and classified the ending. Cancelling `done` here
    /// would let `wait` read an empty slot before the aborted task was even
    /// dropped, and the assertion would hold no matter how the guard behaved —
    /// including with the classification deleted outright.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_after_shutdown_is_ok() {
        let (front, node, _dir) = test_front().await;

        // What `shutdown` does, minus consuming the front, so `wait` can still
        // be called: record the intent, then end the task.
        front.shutdown_requested.cancel();
        front.task.abort();

        assert!(
            tokio::time::timeout(Duration::from_secs(5), front.wait())
                .await
                .expect("wait must resolve once the guard runs")
                .is_ok(),
            "a requested shutdown must not publish an error"
        );

        node.shutdown().await.expect("node shuts down");
    }

    async fn body_text(response: Response<FrontBody>) -> String {
        let collected = response.into_body().collect().await.expect("collect body");
        String::from_utf8(collected.to_bytes().to_vec()).expect("utf8 body")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolve_op_scripts_leaves_non_script_ops_untouched() {
        let (node, _dir) = test_node().await;
        let mut op = ControlOp::DeleteImposter {
            tenant: TenantId::default(),
            port: 4545,
        };

        let result = resolve_op_scripts(&mut op, &node, &ScriptBaseDir::Unconfigured, None);
        assert!(result.is_ok());
        assert!(matches!(op, ControlOp::DeleteImposter { port: 4545, .. }));

        let mut op = ControlOp::SetEnabled {
            tenant: TenantId::default(),
            port: 4545,
            enabled: false,
        };
        assert!(resolve_op_scripts(&mut op, &node, &ScriptBaseDir::Unconfigured, None).is_ok());

        // Move/DeleteById steps carry no stub payload, so no registry read and
        // no resolution — even against a port that has no imposter at all.
        let mut op = ControlOp::PatchStubs {
            tenant: TenantId::default(),
            port: 4545,
            edit: StubEditScript(vec![
                StubEdit::Move { from: 1, to: 0 },
                StubEdit::DeleteById { id: "a".to_owned() },
            ]),
        };
        assert!(resolve_op_scripts(&mut op, &node, &ScriptBaseDir::Unconfigured, None).is_ok());

        node.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_resolution_error_names_the_imposter_index_and_port() {
        let (node, _dir) = test_node().await;
        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
            "stubs": [{
                "responses": [{ "_rift": { "script": { "file": "greet.rhai" } } }],
            }],
        }))
        .expect("config parses");
        let mut op = ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: Box::new(config),
        };

        let err = resolve_op_scripts(&mut op, &node, &ScriptBaseDir::Unconfigured, Some(1))
            .expect_err("an Unconfigured base must refuse a file: ref");
        let text = body_text(err).await;
        assert!(
            text.contains("Script resolution failed in imposter[1] (port Some(4545)):"),
            "{text}"
        );

        node.shutdown().await.expect("shutdown");
    }

    /// #57: a script that resolution left inline but the engine cannot parse is
    /// refused with upstream's message, and the batch variant names the index.
    #[tokio::test]
    async fn validation_refuses_unparseable_resolved_scripts() {
        let broken = |port: u16| -> ControlOp {
            ControlOp::PutImposter {
                tenant: TenantId::default(),
                config: Box::new(
                    serde_json::from_value(serde_json::json!({
                        "port": port,
                        "protocol": "http",
                        "stubs": [{
                            "id": "a",
                            "responses": [{
                                "_rift": {
                                    "script": { "code": "fn respond(ctx) { let x = ; }", "engine": "rhai" },
                                },
                            }],
                        }],
                    }))
                    .expect("config parses"),
                ),
            }
        };

        let text = body_text(
            validate_op_scripts(&broken(4545), None).expect_err("broken rhai must be refused"),
        )
        .await;
        assert!(text.contains("Script validation failed:"), "{text}");

        let text = body_text(
            validate_op_scripts(&broken(4545), Some(1)).expect_err("broken rhai must be refused"),
        )
        .await;
        assert!(
            text.contains("Script validation failed in imposter[1] (port Some(4545)):"),
            "{text}"
        );
    }

    /// #57: ops that carry no stub payload are never validated — including the
    /// stub-edit steps that only move or delete.
    #[test]
    fn validation_skips_ops_without_stub_payloads() {
        for op in [
            ControlOp::DeleteImposter {
                tenant: TenantId::default(),
                port: 4545,
            },
            ControlOp::SetEnabled {
                tenant: TenantId::default(),
                port: 4545,
                enabled: false,
            },
            ControlOp::PatchStubs {
                tenant: TenantId::default(),
                port: 4545,
                edit: StubEditScript(vec![
                    StubEdit::Move { from: 1, to: 0 },
                    StubEdit::DeleteById { id: "a".to_owned() },
                ]),
            },
        ] {
            assert!(validate_op_scripts(&op, None).is_ok());
        }
    }
    #[test]
    fn parse_if_match_accepts_the_emitted_token_and_bare_integers() {
        assert_eq!(
            parse_if_match("default:4545@17", Some(4545)).expect("token"),
            17
        );
        assert_eq!(
            parse_if_match("\"default:4545@17\"", Some(4545)).expect("etag-quoted token"),
            17
        );
        assert_eq!(parse_if_match("17", Some(4545)).expect("bare revision"), 17);
    }

    #[test]
    fn parse_if_match_rejects_wildcards_weak_validators_and_mismatches() {
        for bad in [
            "*",
            "W/\"default:4545@17\"",
            "default:9999@17",
            "other:4545@17",
            "default:4545@seventeen",
            "default:4545@17, default:4545@18",
            "",
        ] {
            let refused = parse_if_match(bad, Some(4545));
            let response = refused.expect_err(&format!("{bad:?} must be refused"));
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{bad:?}");
        }
    }

    /// Issue #210: a route-table write conditions on the portless token `GET
    /// /front-door/routes` answers.
    #[test]
    fn parse_if_match_accepts_the_portless_route_table_token() {
        assert_eq!(parse_if_match("default@17", None).expect("token"), 17);
        assert_eq!(
            parse_if_match("\"default@17\"", None).expect("etag-quoted token"),
            17
        );
        assert_eq!(
            parse_if_match(" default@0 ", None).expect("a never-written table is revision 0"),
            0
        );
        assert_eq!(parse_if_match("17", None).expect("bare revision"), 17);
    }

    /// The two token shapes are not interchangeable: a client that sends the
    /// one for the *other* kind of record conditioned on something it is not
    /// writing, and must be told so rather than have the token coerced.
    #[test]
    fn parse_if_match_refuses_a_token_whose_shape_does_not_match_the_target() {
        for (bad, port) in [
            // Ported token, route-table target.
            ("default:4545@17", None),
            // Portless token, single-imposter target.
            ("default@17", Some(4545)),
        ] {
            let response =
                parse_if_match(bad, port).expect_err(&format!("{bad:?} must be refused"));
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{bad:?}");
        }
    }

    #[test]
    fn parse_if_match_rejects_bad_portless_tokens() {
        for bad in [
            "*",
            "W/\"default@17\"",
            "other@17",
            "default@seventeen",
            "default@17, default@18",
            "@17",
            "",
        ] {
            let response =
                parse_if_match(bad, None).expect_err(&format!("{bad:?} must be refused"));
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{bad:?}");
        }
    }
}
