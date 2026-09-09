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
//! exact `Rift-Cluster-Revision` token (`<port>@<revision>`) or a bare
//! revision integer. A route-table write (`PUT /front-door/routes`, `DELETE
//! /front-door/routes/{id}`) may carry the *portless* form
//! (`routes@<revision>`), which `GET /front-door/routes` answers so a client
//! has something to condition on; a table that was never written reads as
//! revision `0`. The route-table revision is the table's, not any one route's: a
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
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
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
use rift_cluster::control::{
    self, ControlOp, ControlRequest, PreconditionTarget, StubEdit, StubEditScript,
};
use rift_cluster::decorate::{
    HEADER_BIND_FAILURES, HEADER_OP_ID, HEADER_PARTIAL, HEADER_REVISION, HEADER_WARNINGS,
};
use rift_cluster::stores::{ContextScope, FlowConfig, FlowNet, ResolvedKnobs};
use rift_cluster::{
    ControlOutcome, ControlResponse, KeyClass, NodeError, NodeId, OwnedKey, RaftNode,
    SESSION_KEY_BYTES, SessionKey,
};
use rift_cluster_base::seams::{
    ErrorKind, ImposterConfig, RiftScriptConfig, RouteTable, ScriptBaseDir, Stub,
    classify as classify_upstream, config_uses_script_surface, error_response_typed,
    not_a_stub_reason, resolve_scripts, resolve_stub_scripts, tcp_fault_carrier, validate_stub,
    validate_stubs,
};
// The compiler crate (RFC-004 §3.1–§3.3): `POST /specs/compile` runs it on the accepting node and
// hands the result straight back, storing nothing (D-72, #549). `serde_json::Value` stays
// fully-qualified below, matching this file's existing convention (no bare `use serde_json::Value`).
use rift_cluster_spec::{CompileOptions, MAX_SPEC_BYTES, compile};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::cli::WriteBarrier;
#[cfg(feature = "console")]
use crate::console;
use crate::fleet;
use crate::openapi;
use crate::readiness::Readiness;
use crate::session;

/// Largest admin request body the front accepts on a terminated route. The
/// proxied path streams and is not subject to this.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// How long a terminated write may take to commit (forwarding included) before
/// the client gets the `timeout` error shape. Distinct from the barrier
/// timeout, which begins after the commit and degrades to a warning.
const WRITE_DEADLINE: Duration = Duration::from_secs(10);

/// Total budget for a read that genuinely fans out to every other roster peer — since D-74 that
/// is the spaces listing and nothing else on this front. Bounded so one slow or unreachable peer
/// degrades the answer rather than hanging the client on it: the caller reports the shortfall in
/// the body as `partial: true` (beside `unavailable`), not through `Rift-Cluster-Partial`, which
/// is reserved for `/_fleet/members` and `/_fleet/health`.
const FLEET_PEER_BUDGET: Duration = Duration::from_secs(2);

type FrontBody = BoxBody<Bytes, hyper::Error>;

/// Everything the front needs besides the node itself.
pub struct FrontConfig {
    /// The public admin address to bind (what the operator pointed clients at);
    /// a `host:port` string because the core CLI accepts hostnames.
    pub public_addr: String,
    /// The loopback address the core admin actually bound.
    pub upstream_admin: SocketAddr,
    /// The fleet's one admin credential (`--api-key` / `MB_APIKEY`), or `None` for an open
    /// admin plane (D-73). Set ⇒ every admin route demands it, terminated or proxied.
    pub api_key: Option<String>,
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
    /// This node's startup-readiness latch, threaded through so `/_fleet/health` (RFC-006 §5.2,
    /// issue #185) can report the same state `/readyz` does without a second latch to keep in
    /// sync.
    pub readiness: Arc<Readiness>,
    /// The flow-state subsystem: the space listing's fleet-wide fan-out reaches it, and so does
    /// the owner lookup a space read is decorated with.
    pub flow_net: Arc<FlowNet>,
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
    /// The fleet's one admin credential (`--api-key` / `MB_APIKEY`), or `None` for an open
    /// admin plane (D-73). Set ⇒ every admin route below demands it — as a raw `Authorization`
    /// value, or as a session cookie minted from it by `POST /session`.
    api_key: Option<String>,
    allow_injection: bool,
    scripts_dir: Option<PathBuf>,
    barrier: WriteBarrier,
    barrier_timeout: Duration,
    admin_async: bool,
    readiness: Arc<Readiness>,
    /// See [`FrontConfig::flow_net`].
    flow_net: Arc<FlowNet>,
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
        allow_injection: config.allow_injection,
        scripts_dir: config.scripts_dir,
        barrier: config.barrier,
        barrier_timeout: config.barrier_timeout,
        admin_async: config.admin_async,
        readiness: config.readiness,
        flow_net: config.flow_net,
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

/// The routes the front terminates: the config-mutating surface, plus the
/// EE-only reads that have no upstream to proxy to (the tenancy surface and
/// the source inspection routes). Everything else proxies.
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
    /// `DELETE /imposters/{port}/savedProxyResponses` (issue #226): a Raft-committed
    /// `ControlOp::ProxyRecordedClear`. Pre-#226 this proxied to one node's in-process store,
    /// which cleared nothing the fleet's claim table holds. Its one-time sibling, the
    /// `savedRequests` clear, went the other way with D-74 (#552): the journal is per node again,
    /// so that DELETE is an ordinary proxied route and no longer terminates here.
    /// Recorded *stubs* stay, deliberately — they are imposter config, deleted through
    /// the stub-edit surfaces; this clears the exactly-once markers so signatures record
    /// afresh. GET on the same path stays proxied: the listing is upstream's own surface.
    ClearSavedProxyResponses(u16),
    /// `DELETE /imposters/{port}/spaces/{flow}` (issue #537, D-69): a space teardown has two
    /// independent halves. The *flow-state* half — already clustered via `ClusteredFlowStore` —
    /// is proxied to the local engine, and upstream's own `teardown_space` clears that space's
    /// recorded requests on the way through (`RequestJournal::clear_flow`), which since D-74 is
    /// the whole of the journal side: the journal is upstream's, per node, so there is nothing
    /// fleet-wide left to clear. The *stub* half is what #537 adds: space stubs are replicated
    /// config, so after a successful proxied teardown `terminate_space_teardown` commits
    /// `StubEdit::DeleteBySpace` through Raft, or the next `EngineAction::Sync` would resurrect
    /// them fleet-wide. Not routed through `build_mutation` — there is no loopback path to
    /// `FetchAfter`/`Captured` render from; the response is the proxy's own.
    SpaceTeardown(u16, String),
    /// `POST /imposters/{port}/spaces/{flow}/stubs` (issue #537, D-69): a stub scoped to one
    /// correlated-isolation space, committed as an ordinary `ControlOp::PatchStubs` instead of
    /// being proxied.
    ///
    /// Proxied, it reached only the receiving node's engine and never `sm_configs` — so it existed
    /// on one node, and the next `EngineAction::Sync` (any committed op, on any port) re-rendered
    /// that imposter from replicated state and deleted it as a stale stub. Both halves behind a
    /// `201`. A space stub is already an imposter-config stub distinguished only by `space`
    /// (upstream's `Stub::space`), so committing it needs no new replicated shape — the same
    /// `StubEdit::Add` the imposter-level route uses, with `space` set from the path.
    ///
    /// Reads on this shape stay proxied: they are upstream's own surface, and correct once the
    /// data replicates.
    AddSpaceStub(u16, String),
    /// `GET /imposters/{port}/spaces` (issue #374): every correlated-isolation space this imposter
    /// currently holds live flow-KV entries under, fleet-wide, with each row's live entry count
    /// and owning node plus the imposter's resolved `durability` on the envelope.
    ///
    /// Terminates — there is nothing to proxy to. Unlike [`Self::SpaceTeardown`], which proxies its
    /// flow-state half and adds a replicated stub delete alongside it, upstream's router has no
    /// bare `["spaces"]` shape at all: only the two-segment single-space read and the
    /// three-segment stubs write exist there. This is a fan-out over `FlowNet::fleet_spaces` —
    /// since D-74 the only *terminated* read on this front that reaches peers at all. It reports
    /// its incompleteness in the body (`partial`, beside `unavailable`) rather than through
    /// `Rift-Cluster-Partial`, because a listing refused by policy and one shortened by a slow
    /// peer are different facts and the header cannot tell them apart.
    SpacesList(u16),
    /// `POST /admin/imposters/{port}/try` (issue #335): send a sample request to this imposter and
    /// hand back what it answered, so an operator can tell whether a stub matches without leaving
    /// the console.
    ///
    /// Terminates here because there is nothing to proxy to — no upstream route serves this, and
    /// the imposter is reached as a *client* rather than as an admin API. Since issue #344 the
    /// exchange never opens a socket: it is dispatched **in-process**, over an in-memory hyper
    /// connection, straight to this node's own engine — so its containment is structural rather
    /// than configurable:
    ///
    /// - the route names a **port, never a URL or host** — there is no address at all to aim
    ///   elsewhere, because nothing is addressed;
    /// - the port must name an imposter this fleet has applied — checked against the state
    ///   machine before anything is dialled, so an unknown port is a `404` rather than an
    ///   attempt to reach whatever happens to be listening;
    /// - the imposter answering is *this node's own engine's*, resolved by `port` the same way
    ///   [`RaftNode::is_locally_bound`] resolves it — not whoever else might hold that port's
    ///   socket, which a loopback dial could not tell apart on BSD;
    /// - redirects are **never** followed ([`perform_try`]), because following one is the only way
    ///   the exchange could leave the imposter these rules pinned it to.
    TryImposter(u16),
    /// Whole-table replace of the front door's route table (issue #131).
    /// There is no upstream `/front-door/routes` to proxy to (U-11's admin
    /// CRUD was deferred), so this is provided here, not there.
    PutRoutes,
    DeleteRoute(String),
    /// `PUT /admin/fleet/name` (issue #373): set or rename the fleet's operator-facing name.
    /// Terminates — there is no upstream route to proxy to — and replicates through
    /// `ControlOp::FleetNamePut`, so every node and every console session agrees on one name.
    FleetNamePut,
    /// `POST /specs/compile?port=…[&name=…]` (D-72, #549): compile an OpenAPI 3.0 document
    /// into imposter JSON and hand it straight back. **Stateless** — nothing is stored, no
    /// `ControlOp` is minted, no table is read. The caller `PUT /imposters` the result, which
    /// is the one path a config takes into the log.
    ///
    SpecCompile,
}

/// Takes no query string, as of D-74. It used to: the merged requests read terminated or proxied
/// depending on whether `?since=`/`?match=` was present, which made the classifier's answer a
/// function of the query as well as the route. With the journal back in the engine no route on
/// this front is query-conditioned, and a classifier that cannot see the query cannot grow a
/// second, quieter routing rule inside one.
pub(crate) fn classify(method: &Method, path: &str) -> Option<Terminated> {
    // Fleet-wide state, matched before every port-addressed prefix below because it names no
    // port. Another method on this path falls through to the proxy and answers upstream's 404.
    if path == fleet::FLEET_NAME_PATH && *method == Method::PUT {
        return Some(Terminated::FleetNamePut);
    }
    // The one-shot OpenAPI import (D-72, #549): EE-only and terminating, because there is no
    // upstream `/specs` to proxy to and nothing here reads or writes replicated state. A
    // recognized path with another method falls through to `None`, exactly as the tenancy
    // surface does for its half-matches.
    if path == "/specs/compile" {
        return match *method {
            Method::POST => Some(Terminated::SpecCompile),
            _ => None,
        };
    }
    if path == "/front-door/routes" {
        // `GET` is a read with no `Terminated` variant — it predates the
        // tenancy/sources pattern of classifying EE reads, and terminates in
        // `handle` directly instead (see `HANDLE_DIRECT_ROUTES`).
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
    // The spaces **listing** (issue #374): `GET /imposters/{port}/spaces`, and its
    // `/admin/imposters/` alias — the same one the `requests`/`savedRequests` block just below
    // already treats interchangeably, for the identical reason: upstream's router has no bare
    // `["spaces"]` shape at all, so there is no "proxy, then decorate" fallback the way the
    // single-space read has one. Matched here, ahead of both prefix-specific blocks below, because
    // `spaces_list_target` already normalises both spellings and the trailing-slash form in one
    // pass; a second copy inside each block would be the alias drifting again, the exact failure
    // #223's own review found for `requests`.
    if *method == Method::GET
        && let Some(port) = spaces_list_target(path)
    {
        return Some(Terminated::SpacesList(port));
    }
    // The `/admin/imposters/{port}/requests|savedRequests` alias #223 invented for the merged
    // read is **gone** with the merge (D-74). It never existed upstream — upstream's own
    // `/admin/imposters/` prefix is reserved for flow-state inspection — so it was only ever a
    // spelling of a cluster-merged read, and with requests answered per node there is nothing
    // for it to be a second spelling *of*. The canonical `/imposters/{port}/requests` is the
    // route, and it proxies to this node's engine like any other read.
    if let Some(rest) = path.strip_prefix("/admin/imposters/") {
        let segments: Vec<&str> = rest.split('/').collect();
        // `POST /admin/imposters/{port}/try` (issue #335). Deliberately only under the `/admin/`
        // prefix and not the canonical `/imposters/` one: the canonical prefix is Mountebank's
        // published imposter surface, where `{port}/try` would read as a resource upstream might
        // one day define, while `/admin/imposters/` is already this front's own EE-only namespace
        // (flow-state, and the savedRequests alias above).
        if let [port_str, "try"] = segments.as_slice() {
            if *method != Method::POST {
                return None;
            }
            let port: u16 = port_str.parse().ok()?;
            return Some(Terminated::TryImposter(port));
        }
    }
    let rest = path.strip_prefix("/imposters/")?;
    let segments: Vec<&str> = rest.split('/').collect();
    let port: u16 = segments.first()?.parse().ok()?;
    match segments.as_slice() {
        [_] if *method == Method::DELETE => Some(Terminated::DeleteImposter(port)),
        [_, "enable"] if *method == Method::POST => Some(Terminated::SetEnabled(port, true)),
        [_, "disable"] if *method == Method::POST => Some(Terminated::SetEnabled(port, false)),
        // `requests`, `savedRequests` and `savedRequests/stream` are deliberately **absent**
        // (D-74): the journal is upstream's own, per node, so every verb on those paths — the
        // reads, the `?since=` cursor read, the SSE tail and the `DELETE` clear — proxies to this
        // node's engine and keeps upstream's Mountebank semantics verbatim, its own scalar
        // `x-rift-next-index`/`x-rift-truncated` included. Terminating them was what the merge
        // needed; nothing else did.
        //
        // Only the DELETE terminates (issue #226): the clear must purge the fleet's
        // replicated claim markers, which no proxied engine call can reach. GET stays
        // proxied — the recorded-responses listing is upstream's own surface.
        [_, "savedProxyResponses"] if *method == Method::DELETE => {
            Some(Terminated::ClearSavedProxyResponses(port))
        }
        // `DELETE /imposters/{port}/spaces/{flow}` (issue #537): exactly the two-segment shape
        // upstream's own router matches for `ImposterRoute::Space` (`["spaces", flow_id]`). Every
        // other method on this shape stays proxied exactly as before — only the delete gets a
        // replicated stub half to commit.
        [_, "spaces", flow] if *method == Method::DELETE && !flow.is_empty() => {
            Some(Terminated::SpaceTeardown(port, (*flow).to_owned()))
        }
        // `POST /imposters/{port}/spaces/{flow}/stubs` (issue #537): the three-segment shape,
        // which used to fall through to the proxy. It terminated nowhere, so the stub reached
        // only the receiving node's engine and the next config reconcile — any committed op, on
        // any port — deleted it as a stale stub `sm_configs` had never heard of. Committing it as
        // an ordinary `PatchStubs` is what makes it replicate and survive; a space stub is already
        // an imposter-config stub distinguished only by `space`, so nothing else has to change.
        //
        // Only the POST. The reads on this shape stay proxied — correct once the data replicates.
        [_, "spaces", flow, "stubs"] if *method == Method::POST && !flow.is_empty() => {
            Some(Terminated::AddSpaceStub(port, (*flow).to_owned()))
        }
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

/// The **raw** value of a query parameter, or `None` when it is absent. A bare `name` with no
/// `=value` yields `Some("")`, because presence and absence are different requests to every
/// caller here.
///
/// No percent-decoding, deliberately mirroring upstream's own `query_pairs` key semantics: this
/// and the proxy a request can fall through to must agree byte-for-byte about what the query
/// string says. Nothing this reads ever needs escaping — the one caller is `/specs/compile`'s
/// `?port=`/`?name=`, decimal digits and a bare imposter name.
fn query_param<'q>(query: Option<&'q str>, name: &str) -> Option<&'q str> {
    query
        .unwrap_or_default()
        .split('&')
        .find_map(|pair| match pair.split_once('=') {
            Some((key, value)) if key == name => Some(value),
            _ if pair == name => Some(""),
            _ => None,
        })
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
        return proxy(state, req, ProxyLeg::Gateway).await;
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
    // handled directly here, ahead of `classify`, the same way `/openapi.json` is.
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
    // does not also need a cluster-port credential to ask "is this node healthy". Behind the
    // same `authenticate` chokepoint as every other admin route, so it inherits the CSRF gate
    // and the open-plane handling for free.
    if let Some(route) = fleet::classify(req.method(), &path) {
        return match authenticate(&state, &req) {
            Ok(()) => {
                let Some(node) = state.node.upgrade() else {
                    return typed_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        ErrorKind::Unavailable,
                        "cluster node is shutting down",
                    );
                };
                match fleet::body(&route, &node, &state.readiness).await {
                    Ok(Some(body)) => match serde_json::to_vec(&body.value) {
                        Ok(bytes) => {
                            let mut response = buffered_response(
                                StatusCode::OK,
                                Bytes::from(bytes),
                                json_content_type(),
                            )
                            .unwrap_or_else(|response| response);
                            // The one production stamp of `Rift-Cluster-Partial` (D-74), serving
                            // both `/_fleet/members` and `/_fleet/health`: each body is folded
                            // across peers, and a voter that did not answer leaves a row (or an
                            // addend) this node could not fill.
                            if body.partial {
                                set_header(&mut response, HEADER_PARTIAL, "true");
                            }
                            response
                        }
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
        return match authenticate(&state, &req) {
            Ok(()) => read_routes(&state, &req).await,
            Err(response) => response,
        };
    }

    // `GET /openapi.json` publishes the hand-authored contract (RFC-006 §5.1, issue #184).
    //
    // Authenticated: the document describes the shape of the admin surface, so serving it to an
    // unauthenticated scanner on a keyed fleet would hand out a map of every route for free.
    if req.method() == Method::GET && path == "/openapi.json" {
        return match authenticate(&state, &req) {
            Ok(()) => match openapi::contract_json() {
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

    if let Some(kind) = classify(req.method(), &path) {
        return match authenticate(&state, &req) {
            Ok(()) => terminate(state, req, kind).await,
            Err(response) => response,
        };
    }

    // Proxied. Every remaining admin path is authenticated here — an unmatched path included,
    // because otherwise it would answer whatever the proxied backend gives an anonymous caller
    // instead of `401`, turning it into an unauthenticated route-existence oracle (the exact
    // leak upstream's own hook-ordering contract exists to close, reproduced here for the paths
    // upstream's classifier does not cover).
    if let Err(response) = authenticate(&state, &req) {
        return response;
    }
    let target = classify_upstream(req.method(), &path);
    if let Some(target) = target {
        // Read the marker's inputs before `state` and `req` move into `proxy`.
        let degraded = local_bind_failure(&state, &target.params);
        // Likewise the editor's token (C5, #188): the same applied state the write path's
        // precondition will check, read before the move for the same reason.
        let token = imposter_read_token(&state, req.method(), &path, &target.params);
        // `numberOfRequests` is **not** decorated any more (D-74): upstream's own answer — this
        // node's own count of what this node recorded — is the answer, because the journal is
        // upstream's own and per node. The fleet-sum rewrite that used to run here is gone with
        // the merge that produced the other nodes' halves.
        //
        // `owner` on a space read (issue #359), resolved before `proxy` moves `state` for the
        // same reason `degraded`/`token` are. A flow is the only thing the ring owns, so this is
        // the one read that can name one.
        let space_flow_owner = (req.method() == Method::GET)
            .then(|| space_read_target(&path))
            .flatten()
            .map(|(port, flow)| space_owner(&state, port, &flow));
        // The resolved `_rift` knobs (issue #370), read from the applied config before `proxy`
        // moves `state` for the same reason as everything above. Single-imposter read only: the
        // listing carries no knobs panel, and resolving them per entry would be a stored-config
        // read per imposter on the list screen.
        //
        // This read and the proxied body are two reads of the same imposter, so a write landing
        // between them makes the response describe two revisions at once. The same window every
        // decoration here has; harmless for a knobs panel, which reports configuration rather
        // than acting on it, and the response's `Rift-Cluster-Revision` names the record the
        // *write* path will condition on.
        let flow_knobs = (req.method() == Method::GET && is_single_imposter_read(&path))
            .then(|| port_param(&target.params))
            .flatten()
            .and_then(|port| flow_state_resolved(&state, port));
        let mut response = proxy(state, req, ProxyLeg::Admin).await;
        if let Some(owner) = space_flow_owner {
            response = decorate_space_owner(response, owner).await;
        }
        if let Some(knobs) = flow_knobs {
            response = decorate_flow_state_resolved(response, knobs).await;
        }
        if let Some(reason) = degraded {
            set_header(&mut response, HEADER_BIND_FAILURES, &reason);
        }
        if let Some(token) = token {
            set_header(&mut response, HEADER_REVISION, &token);
        }
        return response;
    }
    proxy(state, req, ProxyLeg::Admin).await
}

/// The token a portless (route-table) write and read both stamp into
/// [`HEADER_REVISION`]: `routes@<revision>`.
///
/// `routes`, not the old `default` tenant segment (#550, D-73): the segment names *what* the
/// revision belongs to, and with tenancy gone the only portless record on this front is the
/// route table. A bare revision would have been the smaller change and is deliberately not it —
/// the token has to stay self-describing so a client cannot feed an imposter's revision back as
/// an `If-Match` on the table.
const ROUTES_REVISION_SUBJECT: &str = "routes";

/// `GET /front-door/routes`: the fleet's current route table, read straight from the state
/// machine. This is the front door's *only* read path (issue #131) — upstream never shipped a
/// `GET` to proxy to, so unlike every other read in this module there is no loopback re-read to
/// fall back on.
async fn read_routes(state: &Arc<FrontState>, _req: &Request<Incoming>) -> Response<FrontBody> {
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
    let (table, revision) = match node.route_table_with_revision() {
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
    // The same token shape the write path emits for a portless mutation — a read whose token
    // the write path would refuse is worse than no token at all.
    set_header(
        &mut response,
        HEADER_REVISION,
        &format!("{ROUTES_REVISION_SUBJECT}@{revision}"),
    );
    response
}

/// Authenticate the request and authorize `action` against it (RFC-002 §4.3,
/// §8.1, §8.4) — the single gate every admin request passes through, whether
/// it will be terminated, proxied, or is the front door's own read.
///
/// Fail closed throughout: a state-machine read that errors becomes a `500`,
/// never a fallthrough to allow.
/// The `Cookie` name a session token rides in (RFC-006 §5.3, issue #185).
const SESSION_COOKIE_NAME: &str = "rift_session";

/// The CSRF header [`csrf_gate`] requires on a state-changing, cookie-authenticated request. Any
/// value counts — its presence is what matters (it proves the caller is same-origin JavaScript
/// that could read a response header or set a custom one, which a cross-site form submission or
/// `<img>`/`<script>` tag cannot do), not its content.
const CSRF_HEADER: &str = "x-rift-csrf";

/// Authenticate a request against the fleet's one credential (D-73).
///
/// `Ok(())` means the request may proceed. There is nothing for it to carry: with tenancy and
/// principals gone (#550) every authenticated caller is *the* administrator, so an identity
/// channel here would be a second source of truth for a decision the key already settles.
///
/// Two ways in, and no third:
///
/// - **The key itself**, as the raw `Authorization` value — byte-identical to open-source
///   Rift's own gate (`admin_api/server.rs`'s `api_key_matches`), constant-time, no scheme
///   prefix to strip. Deliberately the same spelling: this front sits in front of upstream's
///   listener, and a credential shape that worked against one and not the other is the kind of
///   split that surfaces as a mystery 401 on the loopback leg.
/// - **A session cookie** minted from that key by `POST /session` (RFC-006 §5.3), so the browser
///   does not keep the key after login. The CSRF gate runs on this branch only — a bearer
///   cannot be attached by a victim's browser, which is the whole attack.
///
/// **No key configured ⇒ no gate**, exactly as upstream behaves and exactly as this fleet
/// behaved before any credential existed (D-73 supersedes D-44's principal-counting rule). An
/// operator closes the plane by setting `--api-key`; there is no second switch.
///
/// `Err` is a rendered `401` (or `403` from the CSRF gate, or `503` while the node is shutting
/// down). Never a fall-through to allow.
#[allow(clippy::result_large_err)]
fn authenticate(state: &FrontState, req: &Request<Incoming>) -> Result<(), Response<FrontBody>> {
    let Some(expected) = state.api_key.as_deref() else {
        return Ok(());
    };
    let Some(node) = state.node.upgrade() else {
        return Err(typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            "cluster node is shutting down",
        ));
    };

    let provided = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if api_key_matches(provided, expected) {
        return Ok(());
    }
    // Only when the caller presented no bearer at all: a *wrong* `Authorization` is a refusal,
    // never an invitation to look for a second credential on the same request.
    if provided.is_empty() {
        match resolve_cookie(&node, req) {
            Ok(true) => {
                csrf_gate(req)?;
                return Ok(());
            }
            Ok(false) => {}
            // `resolve_cookie`'s `Err` is already the rendered response (a `500` from a
            // state-machine read failure) — propagate it as-is rather than re-wrapping it.
            Err(response) => return Err(response),
        }
    }
    Err(unauthorized())
}

/// Constant-time equality for the admin API key, matching open-source Rift's own
/// `api_key_matches` (`rift-http-proxy/src/admin_api/server.rs`) byte for byte — including its
/// fail-closed arm on a blank configured key, which is what stops a whitespace-only `MB_APIKEY`
/// from authenticating a request that carried no header at all (it reaches here as `""`).
///
/// A plain `!=` short-circuits at the first differing byte, letting a network attacker recover
/// the key from response-timing differences. The length check inside `ct_eq` is not secret.
fn api_key_matches(provided: &str, expected: &str) -> bool {
    if expected.trim().is_empty() {
        return false;
    }
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

/// Whether the request carries a valid `rift_session` cookie (RFC-006 §5.3, issue #185).
///
/// `Ok(false)` flattens every "not authenticated by cookie" case alike — no cookie present, no
/// signing key committed yet, a token that fails [`session::verify`] for any reason — for the
/// same reason upstream's key compare does: the caller cannot act on *why*, only on whether the
/// cookie held up.
///
/// Read fresh from applied state on every call, never cached: rotating the signing key
/// (`ControlOp::SessionKeyPut`) is the only revocation this fleet has, and it must cut a live
/// session on its very next request. Do not add a cache here or in the caller.
#[allow(clippy::result_large_err)]
fn resolve_cookie(node: &RaftNode, req: &Request<Incoming>) -> Result<bool, Response<FrontBody>> {
    let Some(token) = session_cookie(req) else {
        return Ok(false);
    };
    let Some(key) = node.session_key().map_err(|e| internal(&e.to_string()))? else {
        // No console login has ever minted a signing key on this fleet, so no cookie this node
        // issued could exist — but a client can still present garbage, and that is `false`, not
        // an error.
        return Ok(false);
    };
    Ok(session::verify(&key, &token, now_secs()).is_ok())
}

/// The `rift_session` cookie's raw value, if the request carries one. No percent-decoding: the
/// token alphabet (base64url plus `.`) never needs it.
fn session_cookie(req: &Request<Incoming>) -> Option<String> {
    let raw = req.headers().get(hyper::header::COOKIE)?.to_str().ok()?;
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
/// every caller of `authenticate` — every terminated and proxied route, the `/_fleet/*` routes,
/// `/front-door/routes` and `/openapi.json` — gets this for free. There is no second call site to
/// add and no route that reaches a cookie-authenticated request without passing through it: a
/// route that "terminates early" still had to call `authenticate` to know the request was
/// authenticated at all.
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
    // header — one comparison, one place. D-73 supersedes D-46: the `--api-key` **is** the
    // credential this exchange accepts, because it is the only one the fleet has.
    let Some(expected) = state.api_key.as_deref() else {
        // An open plane has no key to exchange, and minting a cookie anyway would hand the
        // console a credential that proves nothing and revokes nothing. The console reads this
        // as "no login needed" rather than as a failure.
        return typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            "this fleet runs with no --api-key, so the admin plane is open and there is no \
             credential to exchange for a session",
        );
    };
    if !api_key_matches(&login.api_key, expected) {
        return unauthorized();
    }

    let key = match ensure_session_key(state, &node).await {
        Ok(key) => key,
        Err(response) => return response,
    };
    let token = session::mint(&key, now_secs(), session::SESSION_TTL_SECS);

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
// The error channel here *is* a rendered HTTP response, which is how a handler returns a
// refusal with `?` from anywhere in its body. `Response<FrontBody>` is hyper's type and
// its size is not ours to shrink; boxing it would only move the unboxing to every caller.
#[allow(clippy::result_large_err)]
async fn ensure_session_key(
    state: &FrontState,
    node: &Arc<RaftNode>,
) -> Result<SessionKey, Response<FrontBody>> {
    if let Some(key) = node.session_key().map_err(|e| internal(&e.to_string()))? {
        return Ok(key);
    }

    let mut bytes = [0u8; SESSION_KEY_BYTES];
    rand::thread_rng().fill_bytes(&mut bytes);
    let op = ControlOp::SessionKeyPut {
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
    let request = mint(op_id, op, None);
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

// ---------------------------------------------------------------------------
// Proxy path
// ---------------------------------------------------------------------------

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

/// The `(port, flowId)` of a space read — `GET /imposters/{port}/spaces/{flowId}` — or `None`.
///
/// Exactly the two-segment shape upstream's router matches for `ImposterRoute::Space`; the
/// three-segment `["spaces", flow, "stubs"]` is a different route and is deliberately excluded,
/// the same distinction the `SpaceTeardown` delete draws.
fn space_read_target(path: &str) -> Option<(u16, String)> {
    let path = path.split('?').next().unwrap_or(path);
    let rest = path
        .strip_prefix("/admin/imposters/")
        .or_else(|| path.strip_prefix("/imposters/"))?;
    match rest.split('/').collect::<Vec<_>>().as_slice() {
        [port, "spaces", flow] if !flow.is_empty() => {
            Some((port.parse::<u16>().ok()?, (*flow).to_owned()))
        }
        _ => None,
    }
}

/// The `port` of a spaces **listing** — `GET /imposters/{port}/spaces` — or `None` (issue #374).
///
/// Exactly the one-segment shape [`space_read_target`] rejects (its `[port, "spaces", flow]` arm
/// requires a third, non-empty segment): the two parsers partition every `.../spaces...` shape
/// between them rather than overlapping, so neither can shadow the other. A trailing slash
/// (`/spaces/`) is the same resource, not a space whose id is empty — matched here as `flow == ""`
/// on the three-segment shape, the same way `space_read_target`'s `!flow.is_empty()` guard rejects
/// it from the other side.
fn spaces_list_target(path: &str) -> Option<u16> {
    let path = path.split('?').next().unwrap_or(path);
    let rest = path
        .strip_prefix("/admin/imposters/")
        .or_else(|| path.strip_prefix("/imposters/"))?;
    match rest.split('/').collect::<Vec<_>>().as_slice() {
        [port, "spaces"] | [port, "spaces", ""] => port.parse::<u16>().ok(),
        _ => None,
    }
}

/// This imposter's `ContextScope`, resolved from applied config the way every scope-dependent EE
/// route needs it — shared by [`space_owner`] and the spaces listing (#374) so the two cannot
/// answer with two different scopes for the same imposter.
///
/// `None` when the config could not be read or no longer parses — **the caller decides what that
/// means**, because the same failure is survivable at one call site and not at the other.
///
/// [`space_owner`] folds `None` to `ContextScope::default()` (`Imposter`, the isolating choice) and
/// serves the read anyway: there it costs one advisory `owner` field on a single flow, and the read
/// itself is the engine's to answer. The spaces listing cannot do that. There the scope *selects
/// which set of flows is enumerated at all*, so a wrong guess does not degrade one field — it
/// returns a confident, complete-looking list of the wrong namespace, or of nothing. A
/// fleet-scoped imposter whose config read hiccups would answer `{"spaces":[],"partial":false}`,
/// which the console renders as "this imposter holds no spaces" while it holds several.
///
/// That is why this returns `Option` rather than keeping the `unwrap_or_default()` it was extracted
/// from: a default that is *harmless* as a fallback for one field is a data-path swallow when it
/// picks the query.
fn imposter_scope(node: &RaftNode, port: u16) -> Option<ContextScope> {
    node.imposter_config(port)
        .inspect_err(|e| {
            tracing::warn!(port, error = %e, "the imposter's context scope could not be resolved");
        })
        .ok()
        .flatten()
        .and_then(|json| serde_json::from_str::<ImposterConfig>(&json).ok())
        .and_then(|config| FlowConfig::from_imposter(&config).ok())
        .map(|flow| flow.scope)
}

/// The ring member holding this space's flow state (issue #359).
///
/// A *space* is a flow, and a flow is the only thing this cluster assigns an owner to: imposters,
/// stubs and config are replicated to every node, so every node serves them and none owns them. One
/// port with several flows therefore has several owners, one per flow.
///
/// The key is **not** the flow id from the URL. It is that id under the imposter's
/// `flowState.contextScope` — `i{port}:` per imposter (the default), `f:` fleet-wide — which is why
/// this reads the imposter's own config to find the scope. Under `Fleet` two imposters' same-named
/// spaces are one flow with one owner, and hashing the bare id would name the wrong node for every
/// imposter-scoped flow, which is the default case.
///
/// [`ContextScope::scoped_flow_id`] is shared with the store that writes under this key, so the two
/// cannot drift apart.
///
/// Computed here rather than in the browser deliberately: HRW is reproducible in principle, but a
/// console that re-implemented it would assert an answer the server never gave, and the first
/// disagreement would send an operator to the wrong node.
///
/// Absence over error, for the reason [`imposter_read_token`] gives — the space read is served by
/// the engine and must not start failing because an ownership lookup could not run. `None` when the
/// node handle is gone, no membership is applied, or the config cannot be read or parsed.
///
fn space_owner(state: &FrontState, port: u16, flow_id: &str) -> Option<NodeId> {
    let node = state.node.upgrade()?;
    let ring = node.ring();
    if ring.is_empty() {
        return None;
    }
    // `Imposter` is both the documented default and the isolating one, and #359's contract is that
    // an owner lookup never fails a read it decorates. Unchanged from before `imposter_scope` was
    // extracted — see that function's doc for why the listing must NOT make the same fold.
    let scope = imposter_scope(&node, port).unwrap_or_default();
    ring.owner(OwnedKey::new(
        KeyClass::FlowKv,
        &scope.scoped_flow_id(Some(port), flow_id),
    ))
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
    params: &[(&'static str, String)],
) -> Option<String> {
    if *method != Method::GET || !is_single_imposter_read(path) {
        return None;
    }
    let port = port_param(params)?;
    let node = state.node.upgrade()?;
    let revision = node
        .imposter_revision(port)
        // Absence-over-error is right (the read itself still serves; the console just disables
        // conditional saves), but a storage failure must not vanish on the way to it — the corrupt
        // -record case inside `imposter_revision` already logs at error level, and a redb read
        // failure deserves the same trail rather than a silent None.
        .inspect_err(
            |e| tracing::warn!(port, error = %e, "imposter read serves without a revision token"),
        )
        .ok()
        .flatten()?;
    // The same token the write path emits for this record, so a read's token is one the write
    // path will accept back as `If-Match` — see `RiftClusterRevision`'s contract note.
    Some(format!("{port}@{revision}"))
}

/// Whether `path` is exactly the single-imposter read, `/imposters/{port}` — the one proxied read
/// whose response carries a conditionable record's revision.
///
/// Also matches the `/admin/imposters/{port}` alias (issue #223 review, Important): upstream
/// answers the imposter read identically under both spellings, and leaving this one unaware of
/// the alias is exactly how the two ended up disagreeing about the very same imposter — one
/// carrying the revision token and the resolved-knobs block, the other silently not.
fn is_single_imposter_read(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path);
    let mut segments = path.trim_start_matches('/').split('/');
    match segments.next() {
        Some("imposters") => {}
        Some("admin") if segments.next() == Some("imposters") => {}
        _ => return false,
    }
    segments.next().is_some_and(|p| p.parse::<u16>().is_ok()) && segments.next().is_none()
}

/// Add `owner` to a proxied space read (issue #359).
///
/// Purely additive, which is what makes its failure handling differ from a decoration that
/// *corrects* a value upstream already answered: this one adds a field that is optional by
/// construction — the console renders its absence as "not known", so a body arriving without
/// `owner` is honest, while a 500 would break a space read that upstream answered perfectly well.
///
/// So an unparseable body passes through **unchanged and logged**, never silently defaulted: the
/// caller then sees exactly what upstream sent rather than a laundered version of it, and the log
/// is what stops the pass-through from being a swallow.
async fn decorate_space_owner(
    response: Response<FrontBody>,
    owner: Option<NodeId>,
) -> Response<FrontBody> {
    let (parts, body) = response.into_parts();
    if !parts.status.is_success() {
        return Response::from_parts(parts, body);
    }
    let Some(owner) = owner else {
        return Response::from_parts(parts, body);
    };
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return internal(&format!("reading the space body to decorate owner: {e}"));
        }
    };
    match rewrite_space_owner(&bytes, owner) {
        Ok(rewritten) => {
            buffered_response(parts.status, Bytes::from(rewritten), json_content_type())
                .unwrap_or_else(|response| response)
        }
        Err(e) => {
            tracing::error!(error = %e, "the space body could not be decorated with its owner");
            buffered_response(parts.status, bytes, json_content_type())
                .unwrap_or_else(|response| response)
        }
    }
}

/// Add `_rift.flowStateResolved` to a proxied single-imposter read (issue #370).
///
/// Additive, and so it takes [`decorate_space_owner`]'s failure polarity: an unparseable body
/// passes through **unchanged and logged**, never silently defaulted — never the fail-closed
/// polarity a decoration that *corrects* an upstream value would need. That is safe here in a way
/// it would not be if this rendered the stored
/// config — the block is built from the already-parsed knobs ([`ResolvedKnobs`]), so upstream's
/// redaction of the credentialed `flowState.redis` block cannot be undone by any path through here,
/// including the failure path.
async fn decorate_flow_state_resolved(
    response: Response<FrontBody>,
    knobs: ResolvedKnobs,
) -> Response<FrontBody> {
    let (parts, body) = response.into_parts();
    if !parts.status.is_success() {
        return Response::from_parts(parts, body);
    }
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return internal(&format!(
                "reading the imposter body to decorate flowStateResolved: {e}"
            ));
        }
    };
    let (body, failed) = match rewrite_flow_state_resolved(&bytes, &knobs) {
        Ok(rewritten) => (Bytes::from(rewritten), None),
        Err(e) => (bytes, Some(e)),
    };
    if let Some(e) = failed {
        tracing::error!(error = %e, "the imposter body could not be decorated with its resolved flow-state knobs");
    }
    let mut response = buffered_response(parts.status, body, json_content_type())
        .unwrap_or_else(|response| response);
    carry_over_headers(&mut response, &parts.headers);
    response
}

/// Move the proxied response's own headers onto a rebuilt response.
///
/// [`buffered_response`] starts from an **empty** header map. That is right for a response this
/// front composes from nothing and wrong for one it *rewrites*: the knobs decoration rebuilds the
/// body of an answer the embedded engine already produced, and every header that answer carried
/// — the revision token, a bind-failure marker, upstream's own cache and vary headers — would be
/// dropped on the floor by a rebuild that started empty, leaving an answer that has silently lost
/// what it was saying about itself. (Before D-74 the same helper also carried headers across a
/// chain of front decorations; the single-imposter read now has exactly this one.)
///
/// `content-type` is left as the rebuild set it, and `content-length` is deliberately not carried:
/// the body it described is not the body being sent.
fn carry_over_headers(response: &mut Response<FrontBody>, previous: &hyper::HeaderMap) {
    let headers = response.headers_mut();
    // `iter()` rather than `into_iter()`: the owning iterator yields `None` for the name of a
    // repeated header's second and later values, so a name-keyed loop over it silently drops them.
    // `append` keeps every value of a multi-valued header.
    for (name, value) in previous {
        if name == hyper::header::CONTENT_TYPE || name == hyper::header::CONTENT_LENGTH {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }
}

/// Insert `_rift.flowStateResolved` into an imposter body, creating `_rift` if upstream sent none.
///
/// Upstream's own `_rift.flowState` keeps every key and value it arrived with — `flowIdSource`
/// included, and still as the flat string upstream renders. That is a compatibility contract, not
/// tidiness: rift-verify reads it there to drive correlated isolation, so rewriting it in EE would
/// break rift-verify against an EE cluster, which is what this repo's `parity` job exists to catch.
///
/// Not *byte*-identical: the document round-trips through `serde_json::Value` without
/// `preserve_order`, so key order comes out normalised. This is the only rewrite the
/// single-imposter read goes through since D-74 (#552) removed the `numberOfRequests` fleet-sum
/// decoration, and no consumer depends on key order.
fn rewrite_flow_state_resolved(bytes: &[u8], knobs: &ResolvedKnobs) -> Result<Vec<u8>, String> {
    let mut doc: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| format!("the imposter body was not JSON: {e}"))?;
    let map = doc
        .as_object_mut()
        .ok_or_else(|| "the imposter body was not a JSON object".to_owned())?;
    let rift = map
        .entry("_rift")
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    let rift = rift
        .as_object_mut()
        .ok_or_else(|| "the imposter body's `_rift` was not a JSON object".to_owned())?;
    rift.insert("flowStateResolved".to_owned(), knobs.to_json());
    serde_json::to_vec(&doc).map_err(|e| e.to_string())
}

/// The three published `_rift` knobs for one imposter, read from the applied config (issue #370).
///
/// Resolved from the *stored* document rather than from upstream's response because that is the
/// only place the inherited-vs-set distinction survives: parsing resolves an absent key to its
/// default, and upstream's allowlist never emits two of the three knobs at all.
fn flow_state_resolved(state: &FrontState, port: u16) -> Option<ResolvedKnobs> {
    let node = state.node.upgrade()?;
    let config = node
        .imposter_config(port)
        .inspect_err(|e| {
            tracing::warn!(port, error = %e, "imposter read serves without its resolved flow-state knobs");
        })
        .ok()
        // `Ok(None)` — no committed record on this node's applied state — is deliberately silent,
        // and is the one branch here that is not a fault. Upstream answered the read from its own
        // engine, so this node is serving an imposter whose config it has not applied yet: an
        // ordinary lag window on a node still catching up. The console renders the knobs as unknown
        // for as long as it lasts, which is the honest answer, and logging every such read would
        // make a routine catch-up look like an error.
        .flatten()?;
    // `error`, not `warn`, for the two below: admission validates both before a record commits, so
    // either one failing means an applied record that should not exist — an integrity signal worth
    // alerting on, not just something to find by grepping afterwards. The `warn` above is the
    // different, benign case of a read that simply could not be served.
    let config: ImposterConfig = serde_json::from_str(&config)
        .inspect_err(|e| {
            tracing::error!(port, error = %e, "the stored imposter config did not parse; serving without resolved flow-state knobs");
        })
        .ok()?;
    // A stored value the knobs cannot interpret is left off the response rather than published as
    // a default: admission refuses those, so reaching here means a record written out of band, and
    // "async, inherited" over a document that says otherwise is the wrong-but-quiet answer.
    ResolvedKnobs::from_imposter(&config)
        .inspect_err(|e| {
            tracing::error!(port, error = %e, "the stored flow-state knobs did not resolve; serving without them");
        })
        .ok()
}

/// Insert `owner` into a space body. Errors rather than guessing if it is not a JSON object.
fn rewrite_space_owner(bytes: &[u8], owner: NodeId) -> Result<Vec<u8>, String> {
    let mut doc: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| format!("the space body was not JSON: {e}"))?;
    let map = doc
        .as_object_mut()
        .ok_or_else(|| "the space body was not a JSON object".to_owned())?;
    // A **string**, for the reason `cluster_api::node_id` gives and this route originally missed
    // (issue #359 shipped it as a bare number): a raft id is a `u64`, and every id above
    // 2^53 - 1 silently rounds when a JavaScript reader parses it — the console would render a
    // neighbouring node's id and send an operator to the wrong node, quietly. The listing added by
    // issue #374 renders the same field the same way; two spellings of one field across two
    // adjacent routes is a contract a client has to special-case.
    map.insert("owner".to_owned(), serde_json::json!(owner.to_string()));
    serde_json::to_vec(&doc).map_err(|e| e.to_string())
}

/// `<port>=<reason>` when this node could not realize the addressed imposter's port, else `None`
/// (issue #143). See [`HEADER_BIND_FAILURES`].
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

/// Which leg of the local proxy a request is on — and therefore whether the admin credential
/// is injected onto it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProxyLeg {
    /// An admin request this front has already authenticated.
    Admin,
    /// `/__rift/{port}/*` data-plane gateway traffic (RFC-002 §7's open plane). Upstream exempts
    /// this prefix from its own key gate for the reason the credential must not be injected here
    /// either: the request is forwarded to the imposter, where an `Authorization` header would
    /// land in its predicates and its recorded request log.
    Gateway,
}

/// Forward `req` to the loopback admin and stream the response back. `leg` decides whether the
/// configured admin key is injected — see [`ProxyLeg`] and the comment on the injection itself.
async fn proxy(
    state: Arc<FrontState>,
    req: Request<Incoming>,
    leg: ProxyLeg,
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
    // **The loopback listener runs upstream's own `--api-key` gate** (`compose` no longer clears
    // `cli.oss.api_key`, D-73), and that gate is a raw constant-time compare against the
    // configured key — it knows nothing of cookies. A cookie-authenticated request arrives here
    // with no `Authorization` at all, and forwarding the *session token* as one (what this did
    // before #550) would now be rejected on the loopback leg: the console would log in, get its
    // cookie, and then 401 on `GET /imposters`.
    //
    // So the front injects the configured key, replacing whatever the client sent. Replace, not
    // fill-in: this front has already authenticated the request, and re-presenting the caller's
    // own header would make the loopback re-decide a decision that was already made — with a
    // value the caller controls.
    //
    // Never on the gateway leg: `/__rift/*` is forwarded to the imposter, and an admin
    // credential landing in an app-under-test's predicates and request log is precisely why
    // upstream exempts that prefix from its own key gate.
    if leg == ProxyLeg::Admin {
        match state.api_key.as_deref().map(HeaderValue::from_str) {
            Some(Ok(value)) => {
                parts.headers.insert("authorization", value);
            }
            // An unspellable key cannot be presented, so the loopback would refuse the request
            // with a 401 the caller cannot act on. Remove rather than forward the client's own
            // header — a fleet whose key cannot be sent is misconfigured, and answering as
            // though it were open would be the silent-fallback shape.
            Some(Err(e)) => {
                tracing::error!(error = %e, "configured --api-key is not a spellable header value");
                parts.headers.remove("authorization");
            }
            // Open plane: upstream's gate is off too, so there is nothing to present. The
            // client's own header is dropped rather than forwarded, so a caller cannot reach
            // the loopback with a credential this front never examined.
            None => {
                parts.headers.remove("authorization");
            }
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

/// `POST /specs/compile?port=<u16>[&name=<text>]` — the one-shot OpenAPI import (D-72, #549).
///
/// The body is an OpenAPI 3.0 document, JSON or YAML, at most
/// [`rift_cluster_spec::MAX_SPEC_BYTES`]. The answer is the compiled imposter JSON — byte for
/// byte what a client would `PUT /imposters` — plus the operation index the compiler built it
/// from, so a caller can show what it is about to deploy.
///
/// **Stateless, and that is the whole point.** Nothing is stored, no `ControlOp` is minted, no
/// applied-state table is read: the cluster retains nothing about the spec, and the imposter
/// reaches the log through the one path every other config takes. That is what replaced the
/// `/specs` store, its blob table, its drift diff and its edit-time validation (RFC-004
/// §3.4–§3.6, retired by D-71).
///
/// **`port` is required**, unlike the store-backed compile it replaces. That one could fall back
/// to the spec's single bound port, because a stored spec had bindings; this one has no record to
/// infer from, and a compiled imposter with no port cannot be `PUT` under `--cluster` at all
/// (`validate_replicable_config`: an auto-assigned port cannot replicate). Answering with a
/// portless document would hand the caller something the very next call refuses.
///
/// The compiler's refusals — an unsupported version, an external `$ref`, a parse failure, its own
/// self-check — become this route's `400` verbatim. There is no separate warning channel: a
/// document the compiler would warn about is a document it refuses, so a `200` here means the
/// output passed the contract it just emitted.
async fn terminate_spec_compile(req: Request<Incoming>) -> Response<FrontBody> {
    let query = req.uri().query().map(str::to_owned);
    let port = match query_param(query.as_deref(), "port") {
        Some(raw) => match raw.parse::<u16>() {
            Ok(port) if port != 0 => port,
            _ => {
                return typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    &format!("port {raw:?} is not a usable port number"),
                );
            }
        },
        None => {
            return typed_error(
                StatusCode::BAD_REQUEST,
                ErrorKind::BadData,
                "compile needs ?port=<number>: a compiled imposter with no port cannot be \
                 replicated, so there is nothing useful to answer without one",
            );
        }
    };
    // Raw, with no percent-decoding, for [`query_param`]'s own reason — and an empty `?name=`
    // reads as absent rather than as a name of zero characters, because the compiler omits the
    // field entirely for `None` and an empty `name` is not a thing a caller means to ask for.
    let name = query_param(query.as_deref(), "name")
        .filter(|value| !value.is_empty())
        .map(str::to_owned);

    // Bounded before it is parsed, for the reason `rift-cluster-spec`'s own cap exists: this is
    // attacker-influenceable input. `Limited` caps the stream, so a body with no declared length
    // is bounded too — but its own refusal is a transport error, and the cap that matters to the
    // caller is the spec cap, so the length is re-checked below and answered by name.
    let body = match Limited::new(req.into_body(), MAX_SPEC_BYTES + 1)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return typed_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorKind::RequestTooLarge,
                &format!("spec exceeds {MAX_SPEC_BYTES} bytes"),
            );
        }
    };
    if body.len() > MAX_SPEC_BYTES {
        return typed_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorKind::RequestTooLarge,
            &format!("spec exceeds {MAX_SPEC_BYTES} bytes"),
        );
    }

    let compiled = match compile(
        &body,
        &CompileOptions {
            port: Some(port),
            name,
            max_bytes: MAX_SPEC_BYTES,
        },
    ) {
        Ok(compiled) => compiled,
        Err(e) => {
            return typed_error(
                StatusCode::BAD_REQUEST,
                ErrorKind::BadData,
                &format!("spec does not compile: {e}"),
            );
        }
    };
    let operations: Vec<serde_json::Value> = compiled
        .operations
        .iter()
        .map(|op| {
            serde_json::json!({
                "id": op.id.as_str(),
                "method": op.method,
                "pathTemplate": op.path_template,
                "stubIds": op.stub_ids,
            })
        })
        .collect();
    json_ok(
        StatusCode::OK,
        &serde_json::json!({
            "imposter": compiled.imposter,
            "operations": operations,
        }),
    )
}

/// A JSON success body. `buffered_response`'s `Err` is itself a client response (an oversize
/// body), so either arm is the answer.
fn json_ok(status: StatusCode, body: &serde_json::Value) -> Response<FrontBody> {
    match buffered_response(status, Bytes::from(body.to_string()), json_content_type()) {
        Ok(response) | Err(response) => response,
    }
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
) -> Response<FrontBody> {
    let Some(node) = state.node.upgrade() else {
        return typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            "cluster node is shutting down",
        );
    };

    // So does the one-shot OpenAPI compile (D-72, #549): it holds no record, mints no op and
    // reads no table, so none of the imposter write machinery below applies. It takes `req`
    // directly because its whole input is the request body.
    match kind {
        Terminated::SpecCompile => {
            return terminate_spec_compile(req).await;
        }
        // A try commits nothing — its whole result is what the imposter answered — so it returns
        // here for the same reason the source surface does.
        Terminated::TryImposter(port) => {
            return terminate_try_imposter(&node, req, port).await;
        }
        // Two independent halves (issue #537, D-69): the flow-state teardown proxies exactly as
        // it always has — and clears that space's recorded requests on this node's engine on the
        // way through, which since D-74 is all the journal side there is — and the replicated
        // stub delete is committed alongside it inside `terminate_space_teardown` itself.
        // Diverted here for the same reason the try and the compile are — neither half fits
        // `build_mutation`'s single-op, loopback-rendered shape, and there is nothing to
        // `FetchAfter`/`Captured` from for a route with no state-machine record of its own.
        Terminated::SpaceTeardown(port, flow) => {
            return terminate_space_teardown(&state, &node, req, port, flow).await;
        }
        // A fan-out read has nothing to commit, so it returns here for the same reason the try
        // and the compile do above — none of the `If-Match`/`_rift.script`/loopback-render
        // machinery below applies to a read.
        Terminated::SpacesList(port) => {
            return terminate_spaces_list(&state, &node, port).await;
        }
        _ => {}
    }

    // Authentication already ran in `handle`, once, for every admin request —
    // terminated, proxied, or the front door's own read. Nothing here re-checks it, and the
    // caller's own credential is deliberately not carried forward: the render re-read below
    // presents the fleet's configured `--api-key` instead (see `fetch`), for the same reason
    // `proxy` does.
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
                    "If-Match is not readable ASCII; expected <port>@<revision> or a bare revision",
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

    let is_batch = matches!(kind, Terminated::ReplaceAllImposters);
    let mutation = match build_mutation(&state, &node, kind, &body, host.as_ref()).await {
        Ok(mutation) => mutation,
        Err(response) => return response,
    };
    match run_mutation(
        &state,
        &node,
        mutation,
        is_batch,
        host.as_ref(),
        idempotency.as_deref(),
        if_match.as_deref(),
    )
    .await
    {
        Ok(response) => response,
        Err(response) => response,
    }
}

/// `DELETE /imposters/{port}/spaces/{flow}`: proxy the flow-state teardown exactly as this route
/// always has — it is already clustered via `ClusteredFlowStore` — then, only if that teardown
/// actually happened, commit the space's stub delete through Raft (issue #537, D-69).
///
/// **The journal half is upstream's now (D-74).** The proxied teardown lands in upstream's
/// `teardown_space`, which already calls `RequestJournal::clear_flow(port, space)` on the way
/// through — so the space's recorded requests go on the node that took the teardown, which is the
/// node whose journal they were in. The `ControlOp::JournalClearGen { space: Some(flow), .. }`
/// this function used to commit alongside existed only to raise a *replicated* clear generation
/// the merge-on-read consulted; with no merge and no fleet-wide journal, there is nothing for it
/// to be replicated for.
///
/// The stub half stays committed, and must: #537 made space stubs *replicated* config, so a
/// teardown that only tore down the local engine would be undone by the next
/// `EngineAction::Sync`, which re-renders every imposter from `sm_configs` and would resurrect the
/// stubs fleet-wide.
///
/// Ordering is "only act on a teardown that really happened": a proxy failure means the
/// flow-state store was never touched, so there is nothing for the stub half to record either.
/// The reverse failure — proxy succeeds but the commit does not — is answered as an error rather
/// than swallowed (this file's production rule: a failed commit must surface, never a silent
/// 200), even though the flow-state half has by then already torn down; there is no atomic way to
/// straddle a proxied side effect and a Raft write, and reporting the honest partial failure is
/// better than hiding it behind the proxy's own 200.
async fn terminate_space_teardown(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    req: Request<Incoming>,
    port: u16,
    flow: String,
) -> Response<FrontBody> {
    let response = proxy(Arc::clone(state), req, ProxyLeg::Admin).await;
    if !response.status().is_success() {
        return response;
    }
    let op = ControlOp::PatchStubs {
        port,
        edit: StubEditScript(vec![StubEdit::DeleteBySpace { space: flow }]),
    };
    match commit_teardown_half(node, op, "space-stub delete").await {
        Ok(()) => response,
        Err(error) => error,
    }
}

/// Commit the replicated half of a space teardown, after its proxied flow-state half already
/// succeeded.
///
/// `what` names the half in every failure message rather than being folded into a constant
/// string: "the teardown failed" would not say which side of the proxy boundary broke, and the
/// two sides are recovered differently.
///
/// The reverse failure — proxy succeeded, this did not — is answered as an error rather than
/// swallowed (this file's production rule: a failed commit must surface, never a silent 200), even
/// though the flow-state half has by then already torn down. There is no atomic way to straddle a
/// proxied side effect and a Raft write, and reporting the honest partial is better than hiding it.
// A rendered refusal in the error channel, as everywhere else on this front.
#[allow(clippy::result_large_err)]
async fn commit_teardown_half(
    node: &Arc<RaftNode>,
    op: ControlOp,
    what: &str,
) -> Result<(), Response<FrontBody>> {
    // `validate` first, like every other write on this front — a refusal here would be this
    // function's own bug (the op is built from an already-authorized, already-proxied request),
    // but the R4 order (validate, park durably, submit) is kept uniform rather than special-cased
    // away for the one write that "shouldn't" need it.
    if let Err(reason) = control::validate(&op) {
        return Err(refusal_response(&reason));
    }
    let op_id = Uuid::new_v4();
    let request = mint(op_id, op, None);
    if let Err(e) = node.park_intent(&request) {
        return Err(typed_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::InternalError,
            &format!(
                "flow-state teardown succeeded but the {what} could not be durably accepted: {e}"
            ),
        ));
    }
    let committed = match tokio::time::timeout(WRITE_DEADLINE, node.submit(request)).await {
        Err(_) => {
            node.request_replay();
            let mut error = typed_error(
                StatusCode::GATEWAY_TIMEOUT,
                ErrorKind::Timeout,
                &format!(
                    "flow-state teardown succeeded but the {what} did not commit within the \
                     deadline; parked for replay"
                ),
            );
            set_header(&mut error, HEADER_OP_ID, &op_id.to_string());
            return Err(error);
        }
        Ok(Err(NodeError::Unavailable(detail))) => {
            node.request_replay();
            let mut error = typed_error(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorKind::Unavailable,
                &format!(
                    "flow-state teardown succeeded but the {what} found no quorum/leader \
                     (parked for replay): {detail}"
                ),
            );
            set_header(&mut error, HEADER_OP_ID, &op_id.to_string());
            return Err(error);
        }
        Ok(Err(e)) => {
            node.request_replay();
            let mut error = typed_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorKind::InternalError,
                &format!("flow-state teardown succeeded but the {what} failed: {e}"),
            );
            set_header(&mut error, HEADER_OP_ID, &op_id.to_string());
            return Err(error);
        }
        Ok(Ok(response)) => response,
    };
    if let Err(e) = node.unpark_intent(&op_id) {
        tracing::error!(%op_id, error = %e, "op terminal but could not unpark");
    }
    if let ControlOutcome::Failed { reason } = &committed.outcome {
        return Err(refusal_response(reason));
    }
    Ok(())
}

/// `GET /imposters/{port}/spaces` (issue #374): the fleet-wide list of correlated-isolation spaces
/// this imposter currently holds live flow-KV entries under.
///
/// Unlike [`terminate_space_teardown`], there is no upstream body to proxy and then decorate: the
/// whole response is built from `FlowNet::fleet_spaces`'s fan-out plus the imposter's resolved
/// `durability` knob, both read fresh from this node's applied state.
///
/// Refused for exactly one reason: **the scope could not be resolved.** Guessing would enumerate
/// the wrong namespace and report it as complete (see [`imposter_scope`]).
///
/// A `Fleet`-scoped listing used to be refused as well, because `f:` carries no tenant component
/// and one `FlowNet` shard served every tenant's imposters — so enumerating it handed one tenant
/// another tenant's flow ids. #550 removed tenancy: there is one administrator, the fleet
/// namespace is theirs, and there is no boundary left for the listing to cross. It is served.
///
/// The remaining refusal folds into `partial` rather than into a wrong list, because for an
/// enumeration the scope is not a detail of the answer — it *is* the query.
async fn terminate_spaces_list(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    port: u16,
) -> Response<FrontBody> {
    let scope = imposter_scope(node, port);
    let (prefix, unavailable): (Option<String>, Option<&str>) = match scope {
        None => (None, Some("scope-unresolved")),
        Some(scope) => (Some(scope.prefix_for(Some(port))), None),
    };

    // Nothing is enumerated at all when the scope is unusable — not even under the `Imposter`
    // default. A `f:` imposter scanned as `i{port}:` finds nothing and would look definitively
    // empty; scanning `f:` for real would leak. Refusing is the only answer that is neither.
    let (rows, partial) = match &prefix {
        None => (Vec::new(), true),
        Some(prefix) => {
            state
                .flow_net
                .fleet_spaces(port, prefix, FLEET_PEER_BUDGET)
                .await
        }
    };

    let spaces: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "space": row.space,
                "entryCount": row.entry_count,
                // A decimal STRING, not a JSON number — `cluster_api::node_id`'s reasoning
                // verbatim: a `NodeId` is a `u64`, JSON numbers are IEEE-754 doubles wherever the
                // reader is JavaScript, and an id above 2^53-1 would round silently on the way in.
                // Unlike `last_applied`/`m_idx`, a node id is an identifier, never a magnitude, so
                // a string costs nothing and is the only encoding that survives the round trip.
                "owner": row.owner.to_string(),
            })
        })
        .collect();
    let mut body = serde_json::json!({
        "spaces": spaces,
        "partial": partial,
    });
    // Machine-readable, and distinct from `partial` alone: both say "this is not the whole list",
    // but only this says the list was never attempted and why. Without it the console can only
    // render "some node was slow", which for a fleet-scoped imposter would be a plain lie about a
    // listing that is refused by policy and will not improve on a retry.
    if let Some(reason) = unavailable {
        body["unavailable"] = serde_json::json!(reason);
    }
    // Only ever inserted, never defaulted: `flow_state_resolved` already folds an unreadable or
    // unparseable config to `None` rather than a guess (see its own doc), and publishing a default
    // `durability` here would be indistinguishable from a real one — the wrong-but-quiet answer
    // the error rules exist to prevent. `spaces`/`partial` are still served either way; a knobs
    // read failing must not take the listing down with it.
    if let Some(knobs) = flow_state_resolved(state, port) {
        body["durability"] = knobs.durability_json();
    }

    match serde_json::to_vec(&body) {
        Ok(bytes) => buffered_response(StatusCode::OK, Bytes::from(bytes), json_content_type())
            .unwrap_or_else(|response| response),
        // `serde_json::Value` built entirely from strings/numbers/bools/vecs of the same never
        // fails to serialize; this arm exists so a future field that *can* fail (arbitrary map
        // keys, NaN floats) does not silently drop the body instead of answering `500`.
        Err(e) => internal(&format!("rendering the spaces listing: {e}")),
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

/// Commit a already-built [`Mutation`] op by op, run the barrier, and render the response. Errors
/// are already client-shaped.
///
/// Split out of the original `build_and_run` (issue #278): building a `Mutation` from a
/// [`Terminated`] route stayed in [`build_mutation`], everything after that — validate, the
/// precondition, the injection gate, script resolve/validate, mint, park, submit, the barrier, the
/// render, the cluster headers — moved here, so that the three spec writes (`terminate_spec_put`,
/// `terminate_spec_delete`, `terminate_spec_deploy`), which build their own `Mutation` for reasons
/// `build_mutation`'s own arms for them explain, can inherit this tail without going through a
/// `Terminated` classification a second time.
#[allow(clippy::too_many_arguments)]
// A rendered refusal in the error channel, as in `ensure_session_key` above.
#[allow(clippy::result_large_err)]
async fn run_mutation(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    mut mutation: Mutation,
    // Whether this is `PUT /imposters`'s whole-array replace — the one route whose script errors
    // are labelled `imposters[{idx}]` (see `batch_indices` below). Passed by the caller that still
    // holds the `Terminated` kind; the spec writes, which build their own `Mutation`, are never
    // batches.
    is_batch: bool,
    host: Option<&HeaderValue>,
    idempotency: Option<&str>,
    if_match: Option<&str>,
) -> Result<Response<FrontBody>, Response<FrontBody>> {
    // Pre-validate every op before committing any: a multi-op mutation (PUT
    // /imposters) must not tear half the fleet's config down and then refuse
    // the other half. The state machine re-runs the same checks on apply.
    for op in &mutation.ops {
        if let Err(reason) = control::validate(op) {
            return Err(refusal_response(&reason));
        }
    }

    // A precondition can only ever address a revision the state machine
    // actually stores: a single imposter, or (issue #210) the route
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
        .map(|(index, op)| mint(op_id_for(base, index, total), op, expected_revision))
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
            let (fetched, content_type, body) = fetch(state, &path, host).await?;
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
        Some(port) => format!("{port}@{}", committed.revision),
        None => format!("{ROUTES_REVISION_SUBJECT}@{}", committed.revision),
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

/// Total budget for one try exchange (issue #335) — the in-process handshake, the send, and
/// reading the response.
///
/// A fixed constant rather than a knob. A stub whose `wait` behaviour deliberately exceeds this is
/// curl's job: making the budget configurable would turn a diagnosis affordance into a way to pin
/// an admin worker open for as long as the caller likes.
const TRY_BUDGET: Duration = Duration::from_secs(10);

/// Most of an imposter's response body a try reads back (issue #335).
///
/// This is a diagnosis surface, not a transfer surface — past a megabyte nobody is reading the
/// body to find out whether a stub matched. Exceeding it sets `truncated` rather than failing:
/// a cut answer still answers the question that was asked.
const TRY_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// One header of a try request or response.
///
/// A list of these rather than a map, in both directions: HTTP permits a repeated header name, a
/// mock exists to reproduce exactly what a system under test sends and receives, and a map would
/// silently drop one of a repeated pair.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct TryHeader {
    name: String,
    value: String,
}

/// The sample request a caller wants sent (issue #335).
///
/// **Carries no host, scheme or port.** That is the containment, not an omission: the only
/// addressing input is the `{port}` in the route, which the handler proves names an applied
/// imposter before dispatching. Since issue #344 the exchange is dispatched in-process, not
/// dialled, so there is no scheme to choose at all — an `https` imposter is answered identically
/// to an `http` one, with no TLS handshake. There is deliberately no field here through which a
/// caller could aim the server somewhere else.
///
/// `deny_unknown_fields` because a misspelt `header`/`headers` would otherwise silently send a
/// request without them and leave the operator reading a mismatch they did not cause.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TryRequest {
    method: String,
    path: String,
    #[serde(default)]
    headers: Vec<TryHeader>,
    #[serde(default)]
    body: Option<String>,
}

/// What the imposter answered (issue #335).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TryResponse {
    status: u16,
    headers: Vec<TryHeader>,
    body: String,
    /// `true` when the body was not valid UTF-8 and replacement characters were substituted.
    /// Skipped when false so a client can tell "decoded cleanly" from "decoded with loss" without
    /// having the bytes to compare.
    #[serde(skip_serializing_if = "is_false")]
    body_lossy: bool,
    /// The same, for header *values*.
    ///
    /// Its own flag rather than folding into `body_lossy`, and not omitted on the grounds that
    /// non-UTF-8 header values are rare: this is a mock server with fault injection, so serving
    /// deliberately malformed header bytes is a thing an operator does **on purpose** — and the
    /// header they garbled is exactly the one they are then staring at in the console. Silence
    /// here would be the same defect `body_lossy` exists to prevent, in the place it is most
    /// likely to be encountered deliberately.
    #[serde(skip_serializing_if = "is_false")]
    headers_lossy: bool,
    /// `true` when the body hit [`TRY_MAX_RESPONSE_BYTES`] and what is reported is a prefix.
    #[serde(skip_serializing_if = "is_false")]
    truncated: bool,
    elapsed_ms: u64,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's `skip_serializing_if` hands us a reference.
fn is_false(value: &bool) -> bool {
    !*value
}

/// Why a try produced no exchange at all.
///
/// Kept separate from the imposter's own answer on purpose: the imposter replying `502` and the
/// endpoint being unable to reach it are different facts, and collapsing them would leave a
/// console unable to say which of the two an operator is looking at.
#[derive(Debug)]
enum TryFailure {
    /// [`TRY_BUDGET`] expired. Renders `504`.
    Timeout,
    /// The in-process exchange failed to complete — including the imposter having left this node
    /// between the ownership gate and the exchange. Renders `502`.
    Unreachable(String),
    /// The caller's own envelope was unusable — an invalid method token, a path that will not
    /// form a request target, or a header the wire cannot carry. Renders `400`.
    BadRequest(String),
    /// The stub injected a connection-level TCP fault (issue #344): on the wire the serve loop
    /// would abort the connection rather than send a response, so there is nothing to present as
    /// the imposter's answer. The `String` is the fault name exactly as the stub spelled it.
    /// Renders `502`.
    Fault(String),
}

/// The in-process try server's service error: "this exchange produced no response" — the
/// dispatch was already taken, or the imposter left this node ([`perform_try`]'s `gone` path).
///
/// A concrete error type, not an inline `Box<dyn Error>`, so the in-process server's `service_fn`
/// has a fixed `Service::Error` to coerce to `Box<dyn Error + Send + Sync>` at the one boundary
/// (`hyper::server::conn::http1::Builder::serve_connection`) that needs it — an error boxed
/// per-call inside the closure instead runs into a `for<'a>` lifetime hyper's bound cannot unify
/// with a `'static` one.
#[derive(Debug)]
struct TryExchangeEnded;

impl std::fmt::Display for TryExchangeEnded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "try exchange ended without a response")
    }
}

impl std::error::Error for TryExchangeEnded {}

/// Aborts the task it holds when dropped, unless it was taken first — both halves of a try's
/// in-memory connection end with the exchange, *whichever way the exchange ends*: a completed
/// read, a budget expiry, or `perform_try` itself being dropped because the admin caller went away
/// mid-try (hyper drops the in-flight service future the moment its client disconnects). Without
/// the guard on the server half, that last case would leave the imposter's dispatch running as a
/// detached task — one orphan per abandoned try against a slow stub, which an Operator can farm.
struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);

impl AbortOnDrop {
    fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self(Some(handle))
    }

    /// Hand the handle back for an explicit abort-and-observe; the guard then does nothing on drop.
    fn take(mut self) -> tokio::task::JoinHandle<()> {
        self.0.take().expect("taken at most once")
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}

/// Send `spec` to the imposter's own engine and read back what came out — **in-process, over an
/// in-memory HTTP/1 connection, never a socket** (issue #344).
///
/// `dispatch` is the resolved engine call: production passes a closure over
/// [`RaftNode::dispatch_to_imposter`] (`None` meaning the imposter left this node between the
/// ownership gate and this exchange); tests pass a canned service instead, so the exchange's own
/// rules — the budget, the body cap, lossy decoding, that a `3xx` comes back as data rather than
/// being followed, that a connection fault renders explicitly — are provable in milliseconds
/// without standing up a cluster.
///
/// The mechanism: `dispatch` becomes the sole handler on one end of a [`tokio::io::duplex`],
/// served as a real HTTP/1 connection (`hyper::server::conn::http1`); the other end is a real
/// HTTP/1 client (`hyper::client::conn::http1`) this function drives directly. Nothing is
/// addressed by host or port — the "connection" is two in-memory pipes — so there is no value
/// through which the exchange could reach anything other than `dispatch`. Real HTTP/1 framing is
/// preserved end to end, which is worth more than a bare function call would be: it is exactly the
/// wire format a stub's own predicates and recorded requests are built to see. It also means the
/// request's framing headers are the transport's, not the caller's (`Content-Length` and
/// `Transfer-Encoding` in `spec.headers` are dropped and recomputed from the body actually sent).
///
/// `budget` and `cap` are parameters only so the tests above can use short ones; production passes
/// [`TRY_BUDGET`] and [`TRY_MAX_RESPONSE_BYTES`].
async fn perform_try<D, Fut>(
    dispatch: D,
    port: u16,
    spec: &TryRequest,
    budget: Duration,
    cap: usize,
) -> Result<TryResponse, TryFailure>
where
    D: FnOnce(Request<Incoming>) -> Fut + Send + 'static,
    Fut: Future<Output = Option<Response<Full<Bytes>>>> + Send + 'static,
{
    // Every validation happens before anything is dispatched — the in-process connection below is
    // never stood up for a request that could not have been sent in the first place.
    let method = Method::from_bytes(spec.method.as_bytes()).map_err(|e| {
        TryFailure::BadRequest(format!("{:?} is not an HTTP method: {e}", spec.method))
    })?;

    // Origin-form only: a `path_and_query` component can never carry an authority, which is the
    // whole containment argument here — there is no grammar position left for a caller to smuggle
    // a different host into. The leading `/` is required *here*, not only in the handler, so the
    // guarantee lives with the function that documents it: `PathAndQuery` would also accept
    // `*`, `?x` and `#x`, which serialize to request lines the peer rejects — a `502` blaming
    // the imposter for the caller's envelope.
    if !spec.path.starts_with('/') {
        return Err(TryFailure::BadRequest(format!(
            "{:?} is not a usable path: it must start with '/'",
            spec.path
        )));
    }
    let uri = Uri::builder()
        .path_and_query(spec.path.as_str())
        .build()
        .map_err(|e| {
            TryFailure::BadRequest(format!("{:?} is not a usable path: {e}", spec.path))
        })?;

    let mut headers = Vec::with_capacity(spec.headers.len());
    let mut caller_set_host = false;
    for header in &spec.headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|e| {
            TryFailure::BadRequest(format!(
                "{:?} is not a usable header name: {e}",
                header.name
            ))
        })?;
        let value = HeaderValue::from_str(&header.value).map_err(|e| {
            TryFailure::BadRequest(format!(
                "{:?} is not a usable value for header {:?}: {e}",
                header.value, header.name
            ))
        })?;
        // Framing belongs to the transport: the body sent is exactly `spec.body`, and hyper
        // frames it from that. A caller-supplied `Content-Length`/`Transfer-Encoding` would be
        // honoured over the real length and stall the exchange into a `504` that reads as the
        // imposter's fault, so both are dropped rather than sent — the same thing a browser or
        // curl does with a hand-written length that disagrees with the body it sends.
        if name == hyper::header::CONTENT_LENGTH || name == hyper::header::TRANSFER_ENCODING {
            continue;
        }
        caller_set_host |= name == hyper::header::HOST;
        headers.push((name, value));
    }

    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    if !caller_set_host {
        // HTTP/1.1 needs a `Host`; the caller sent none, so the imposter's own loopback name is
        // supplied — a stub matching on `host` sees what a real loopback dial would have carried.
        builder = builder.header(hyper::header::HOST, format!("127.0.0.1:{port}"));
    }
    let body = spec.body.clone().map(Bytes::from).unwrap_or_default();
    let request = builder
        .body(Full::new(body))
        .map_err(|e| TryFailure::BadRequest(format!("not a usable request: {e}")))?;

    let started = std::time::Instant::now();

    // The in-memory "connection": no socket, no address, so there is nothing to misroute to.
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);

    // `dispatch` is `FnOnce`; the service `hyper` drives needs to be callable through `&self`,
    // so the closure it takes is called at most once — the `Option` is taken on that one call,
    // and a stray second one (never expected for a single try exchange) answers loudly rather
    // than silently reusing state that is gone.
    let dispatch = Arc::new(Mutex::new(Some(dispatch)));
    let gone = Arc::new(AtomicBool::new(false));

    // Set inside the service below, and for the same reason `gone` is: it is a fact about the
    // dispatch that is only knowable *there*. `tcp_fault_carrier` reads the `TcpFaultKind`
    // response extension — the authoritative signal, the one the engine's own `FaultIo` acts on —
    // and an extension is process memory, not bytes. The exchange under this function is a real
    // HTTP/1.1 round trip over `tokio::io::duplex`: the service's response is serialized by
    // `serve_connection` and re-parsed by the client, which builds a *fresh* response whose
    // extension map is empty. So the classification cannot be moved out to where the response is
    // read — it has to happen while the dispatch's own response is still in hand.
    let fault: Arc<Mutex<Option<&'static str>>> = Arc::new(Mutex::new(None));

    // The server half is spawned, and its handle is **kept**: the budget below cancels only the
    // future it wraps, and the dispatch — the imposter's own handler, `wait` behaviour and all —
    // runs inside this task. Left detached, a stub slower than the budget would outlive the
    // request that started it, one orphaned task per try, unbounded and unlogged; the admin
    // front's own accept loop refuses to orphan a task for the same reason. Aborted once the
    // exchange has ended, whichever way it ended.
    let server = {
        let dispatch = Arc::clone(&dispatch);
        let gone = Arc::clone(&gone);
        let fault = Arc::clone(&fault);
        AbortOnDrop::new(tokio::spawn(async move {
            let service = service_fn(move |req: Request<Incoming>| {
                let dispatch = Arc::clone(&dispatch);
                let gone = Arc::clone(&gone);
                let fault = Arc::clone(&fault);
                async move {
                    let taken = dispatch.lock().expect("dispatch lock poisoned").take();
                    let Some(dispatch) = taken else {
                        return Err(TryExchangeEnded);
                    };
                    match dispatch(req).await {
                        Some(response) => {
                            // Classify here, where the extension still exists. The carrier is
                            // still handed to hyper afterwards: the client half needs *a*
                            // response to complete the exchange, and the caller below discards
                            // it in favour of the fault.
                            if let Some(name) = tcp_fault_carrier(&response) {
                                *fault.lock().expect("fault lock poisoned") = Some(name);
                            }
                            Ok(response)
                        }
                        // The imposter left this node between the ownership gate and this
                        // exchange. Answering with an invented status would be worse than no
                        // answer at all, so the connection is dropped instead — hyper reports
                        // that to the client as a send error, which `gone` turns into the
                        // right message rather than a bare transport string.
                        None => {
                            gone.store(true, Ordering::SeqCst);
                            Err(TryExchangeEnded)
                        }
                    }
                }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(server_io), service)
                .await
            {
                // Routine when the client half is dropped mid-exchange (a budget expiry, the
                // `gone` path above) — the caller already has the answer that matters — so
                // debug, exactly as the admin front's own connection loop logs its ends.
                tracing::debug!(port, error = %e, "try: in-process serve_connection ended with an error");
            }
        }))
    };

    // One budget over the whole exchange — the handshake, the send, *and* the body read below. A
    // per-call timeout on the client alone would not cover a dispatch that answers headers
    // promptly and then dribbles the body, which is exactly the shape a `wait` behaviour produces.
    let exchange = tokio::time::timeout(budget, async move {
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(client_io))
            .await
            .map_err(|e| TryFailure::Unreachable(e.to_string()))?;
        // The client connection is driven on its own task so `send_request` below can await the
        // response; its errors reach this function through `send_request`/`frame` already, so a
        // failure here is logged at debug rather than lost. Its handle is kept so the exchange
        // ends both halves the same way.
        let client_conn = tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(port, error = %e, "try: in-process client connection ended with an error");
            }
        });
        let _guard = AbortOnDrop::new(client_conn);

        let response = match sender.send_request(request).await {
            Ok(response) => response,
            Err(e) => {
                return Err(if gone.load(Ordering::SeqCst) {
                    TryFailure::Unreachable(format!(
                        "imposter {port} is no longer served by this node: it left between the \
                         gate and the exchange"
                    ))
                } else {
                    TryFailure::Unreachable(e.to_string())
                });
            }
        };

        // The fault carrier is checked before anything else about the response: a fault is not an
        // answer, whatever its (fabricated) status or body say. The verdict was reached in the
        // service above; by the time `send_request` has resolved, the service has necessarily run
        // (the response head cannot exist before the future that produced it), so the cell is
        // settled. The name is the engine's canonical one for the kind — `CONNECTION_RESET_BY_PEER`
        // rather than whichever alias the config author typed.
        if let Some(name) = *fault.lock().expect("fault lock poisoned") {
            return Err(TryFailure::Fault(name.to_owned()));
        }

        let status = response.status().as_u16();
        let mut headers_lossy = false;
        let headers: Vec<TryHeader> = response
            .headers()
            .iter()
            .map(|(name, value)| {
                // A header a mock chose to send in non-UTF-8 bytes is still worth reporting — the
                // lossy rendering is the diagnosis and dropping the header would hide it — but the
                // substitution is recorded so the console can say the bytes were not what it shows.
                let value = String::from_utf8_lossy(value.as_bytes());
                headers_lossy |= matches!(value, std::borrow::Cow::Owned(_));
                TryHeader {
                    name: name.as_str().to_owned(),
                    value: value.into_owned(),
                }
            })
            .collect();

        // Frame by frame, stopping at the cap, for the same reason the pre-#344 chunked read did:
        // buffering the whole body before slicing it would hold a multi-gigabyte response in the
        // admin process before the cap ever applied.
        let mut body = response.into_body();
        let mut collected: Vec<u8> = Vec::new();
        let mut truncated = false;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| TryFailure::Unreachable(e.to_string()))?;
            let Some(chunk) = frame.data_ref() else {
                continue; // trailers carry no body bytes
            };
            let room = cap.saturating_sub(collected.len());
            // `>`, not `>=`. A chunk that exactly fills the remaining room dropped nothing, so it
            // must not raise `truncated` — a body of exactly `cap` bytes is complete, and
            // reporting it as cut would send an operator looking for content that was never
            // missing. If more does follow, the next iteration sees `room == 0` and flags it then,
            // which is the moment loss actually happens.
            if chunk.len() > room {
                collected.extend_from_slice(&chunk[..room]);
                truncated = true;
                break;
            }
            collected.extend_from_slice(chunk);
        }

        // The imposter's own timing: measured here, before the teardown below, so what the
        // operator judges a stub by does not carry the abort of the in-memory connection.
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok((status, headers, headers_lossy, collected, truncated, elapsed_ms))
    })
    .await
    .map_err(|_| TryFailure::Timeout);

    // The exchange has ended, one way or another: stop the server half now rather than letting a
    // slow dispatch run on. Awaiting the aborted handle is what surfaces a panic inside the
    // dispatch — the imposter's own handler runs there — which would otherwise vanish with the
    // handle; a cancellation is the expected outcome and says nothing. Bounded, because the
    // budget above no longer covers this: an abort lands at the task's next yield, and a
    // dispatch stuck in a non-yielding section must not extend the caller's wait past it.
    let server = server.take();
    server.abort();
    if let Ok(Err(e)) = tokio::time::timeout(Duration::from_millis(50), server).await
        && e.is_panic()
    {
        tracing::error!(port, error = %e, "try: the in-process dispatch panicked");
    }

    let (status, headers, headers_lossy, collected, truncated, elapsed_ms) = exchange??;
    let body = String::from_utf8_lossy(&collected);
    let body_lossy = matches!(body, std::borrow::Cow::Owned(_));
    Ok(TryResponse {
        status,
        headers,
        headers_lossy,
        body: body.into_owned(),
        body_lossy,
        truncated,
        elapsed_ms,
    })
}

/// `POST /admin/imposters/{port}/try` (issue #335).
///
/// The port must name an imposter this fleet has applied — checked from the state machine below,
/// before anything is dispatched, so a try against an unconfigured port is a `404` rather than an
/// attempt to reach whatever the engine happens to be holding.
async fn terminate_try_imposter(
    node: &Arc<RaftNode>,
    req: Request<Incoming>,
    port: u16,
) -> Response<FrontBody> {
    let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        // `Limited`'s error covers a genuine I/O failure (a reset connection, a malformed chunked
        // stream) as well as the size cap, so the underlying error is carried through rather than
        // collapsed into a hardcoded "too large" that would be untrue for the other half — the
        // same shape every other body-collect site on this front uses.
        Err(e) => {
            return typed_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorKind::RequestTooLarge,
                &format!("admin request body refused: {e}"),
            );
        }
    };
    let spec: TryRequest = match parse(&body) {
        Ok(spec) => spec,
        Err(response) => return response,
    };
    if !spec.path.starts_with('/') {
        return typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            "path must start with '/': this endpoint addresses an imposter by port, and an \
             absolute-form target would be a way to name a different host",
        );
    }

    // The record must exist in applied state before this node will dial the port at all: a try
    // against a port nothing has configured is a 404, not an attempt to connect to whatever
    // happens to be listening there.
    match node.get_imposter(port) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return typed_error(
                StatusCode::NOT_FOUND,
                ErrorKind::NoSuchResource,
                &format!("no imposter on port {port}"),
            );
        }
        Err(e) => return internal(&e.to_string()),
    }

    // **The engine-holds-this-port check, and it is load-bearing.** The read above proves an
    // imposter *record* exists on this port. It does not by itself prove that
    // this node's engine is the one that will answer — and those two facts are decoupled on
    // purpose: a `PutImposter` whose bind fails still commits and still reads back
    // (`bind_failure_does_not_fail_apply`), because a bind failure must not wedge the replicated
    // log.
    //
    // Without this gate that gap is an escalation, not an edge case. An Editor holds both
    // `ImposterWrite` and `ImposterTry`, so they could create an imposter on a port already held
    // by something else on this box — the metrics listener, the probe listener, the cluster RPC
    // port — and then use the try to send a request of their choosing to it and read the reply.
    // Every containment property this endpoint claims is downstream of "the thing that answers is
    // the imposter you own"; this is where that is actually established.
    //
    // The positive form (`is_locally_bound`) is required: `bind_failure(..).is_none()` is also
    // true for a port this node serves nothing on, which is precisely the dangerous case.
    //
    // **The residual this used to carry is gone, not hidden.** Before issue #344 this gate proved
    // only that the engine *held* the port, not that a loopback dial would reach it — an imposter
    // binds `0.0.0.0` by default, and BSD accepts that alongside a foreign `127.0.0.1:{port}`
    // socket, so the more-specific socket still won the connection while this check reported true.
    // Since #344 there is no dial to misroute: the exchange is dispatched in-process, straight to
    // the `Arc<Imposter>` this same `is_locally_bound` call resolved, over an in-memory connection
    // that never touches a socket. The BSD wildcard/REUSEPORT case and the `localhost`-vs-`::1`
    // variants a loopback dial could not tell apart are all closed at once, by construction —
    // `a_try_answers_from_this_nodes_engine_not_from_whoever_holds_loopback` pins it.
    if !node.is_locally_bound(port) {
        return typed_error(
            StatusCode::BAD_GATEWAY,
            ErrorKind::BackendUnavailable,
            &format!(
                "imposter {port} is not bound by this node's engine, so nothing was dispatched: \
                 a try only ever answers from an imposter this node is actually serving"
            ),
        );
    }

    let dispatch = {
        let node = Arc::clone(node);
        move |req: Request<Incoming>| async move {
            match node.dispatch_to_imposter(port, req) {
                Some(answer) => Some(answer.await),
                None => None,
            }
        }
    };

    render_try_outcome(
        perform_try(dispatch, port, &spec, TRY_BUDGET, TRY_MAX_RESPONSE_BYTES).await,
        port,
    )
}

/// Turn a try's outcome into the response the caller sees.
///
/// Split from [`terminate_try_imposter`] so the **status mapping itself** is testable without a
/// cluster. That mapping is the part of this endpoint most able to break silently: swapping
/// `GATEWAY_TIMEOUT` and `BAD_GATEWAY` here compiles, and every test below the `TryFailure` level
/// still passes, while a console starts telling operators their mock is unreachable when it was
/// merely slow.
///
/// The split it encodes is the design's central one: the *imposter's* answer — including its own
/// `4xx`/`5xx` — is a **successful try** and rides inside a `200`, while a failure of the endpoint
/// to reach or complete the exchange is a `502`/`504`. Conflating the two would leave a client
/// unable to tell "the mock said 502" from "the mock could not be reached".
fn render_try_outcome(outcome: Result<TryResponse, TryFailure>, port: u16) -> Response<FrontBody> {
    match outcome {
        Ok(outcome) => match serde_json::to_vec(&outcome) {
            Ok(rendered) => {
                buffered_response(StatusCode::OK, Bytes::from(rendered), json_content_type())
                    .unwrap_or_else(|response| response)
            }
            Err(e) => internal(&format!("rendering the try result: {e}")),
        },
        Err(TryFailure::Timeout) => typed_error(
            StatusCode::GATEWAY_TIMEOUT,
            ErrorKind::Timeout,
            &format!(
                "the imposter did not answer within {}s",
                TRY_BUDGET.as_secs()
            ),
        ),
        Err(TryFailure::Unreachable(why)) => typed_error(
            StatusCode::BAD_GATEWAY,
            ErrorKind::BackendUnavailable,
            &format!("could not reach the imposter on port {port}: {why}"),
        ),
        Err(TryFailure::BadRequest(why)) => {
            typed_error(StatusCode::BAD_REQUEST, ErrorKind::BadData, &why)
        }
        Err(TryFailure::Fault(name)) => typed_error(
            StatusCode::BAD_GATEWAY,
            ErrorKind::BackendUnavailable,
            &format!(
                "the imposter's stub on port {port} injects a connection fault ({name}): on the \
                 wire this connection would be aborted with no response, so there is nothing to \
                 show"
            ),
        ),
    }
}

/// Translate one terminated route into ops + a render plan. Reads that inform
/// the mutation (current stubs for index-addressed edits, capture-before-delete
/// bodies) come from the local applied state / loopback admin.
#[allow(clippy::too_many_arguments)]
// A rendered refusal in the error channel, as in `ensure_session_key` above.
#[allow(clippy::result_large_err)]
async fn build_mutation(
    state: &FrontState,
    node: &Arc<RaftNode>,
    kind: Terminated,
    body: &[u8],
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
                ops: vec![ControlOp::PutImposter {
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
                ops.push(ControlOp::PutImposter {
                    config: Box::new(config),
                });
            }
            if ops.is_empty() {
                ops.push(ControlOp::DeleteAll);
            } else {
                // Fleet-wide, because the set this reconciles is fleet-wide (#550): every port
                // with an applied config that the body left out is pruned.
                let existing = node
                    .configured_ports()
                    .map_err(|e| internal(&e.to_string()))?;
                for port in existing {
                    if !keep.contains(&port) {
                        ops.push(ControlOp::DeleteImposter { port });
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
        Terminated::FleetNamePut => {
            let parsed: fleet::FleetNameBody = parse(body)?;
            Ok(Mutation {
                ops: vec![ControlOp::FleetNamePut { name: parsed.name }],
                // Fleet-wide state, not an imposter record: no port to label the revision
                // header with, and `precondition_target` answers `None` for this op, so an
                // `If-Match` against it is refused rather than silently ignored.
                port: None,
                // Nothing to re-read: the name is not a resource with a representation, and a
                // `FetchAfter` would have to invent a loopback route that does not exist.
                render: Render::Captured {
                    body: Bytes::new(),
                    content_type: None,
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::DeleteAllImposters => {
            let (_, content_type, captured) = fetch(state, "/imposters", host).await?;
            Ok(Mutation {
                ops: vec![ControlOp::DeleteAll],
                port: None,
                render: Render::Captured {
                    body: captured,
                    content_type,
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::DeleteImposter(port) => {
            let (status, content_type, captured) =
                fetch(state, &format!("/imposters/{port}"), host).await?;
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
                ops: vec![ControlOp::DeleteImposter { port }],
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
        Terminated::AddSpaceStub(port, flow) => {
            // The shape guard upstream's handler ran (#336) — through the seam, so there is one
            // `STUB_FIELD_NAMES` and not a copy here that goes stale when upstream adds a field.
            // It must run *before* deserialization: `Stub` comes from `StubRaw`, where every field
            // is `#[serde(default)]` and unknown keys are discarded, so any object parses — an
            // object of only unrecognised keys becomes the vacuous stub that matches everything in
            // its space. The two sibling routes disagree about their envelope (`POST
            // .../stubs` takes `{"stub": …}`, this one the bare stub), which is how the mistake is
            // actually reached.
            let payload: serde_json::Value = parse(body)?;
            if let Some(reason) = not_a_stub_reason(&payload) {
                return Err(typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    &reason,
                ));
            }
            let mut stub: Stub = parse(body)?;
            // The path names the space, not the body — exactly as upstream's handler does it, so a
            // caller cannot file a stub into a space the URL never mentioned.
            stub.space = Some(flow.clone());
            Ok(Mutation {
                ops: vec![ControlOp::PatchStubs {
                    port,
                    edit: StubEditScript(vec![StubEdit::Add { stub, index: None }]),
                }],
                port: Some(port),
                // Upstream's own GET, looped back to, so the body is byte-for-byte the
                // `{"space", "stubs"}` shape this route has always answered.
                render: Render::FetchAfter {
                    path: format!("/imposters/{port}/spaces/{flow}/stubs"),
                    status: StatusCode::CREATED,
                },
            })
        }
        Terminated::ReplaceStubs(port) => {
            let replace: ReplaceStubsBody = parse(body)?;
            let mut config = stored_config(node, port)?;
            config.stubs = replace.stubs;
            Ok(put_config_mutation(port, config))
        }
        Terminated::ReplaceStubAt(port, index) => {
            let stub: Stub = parse(body)?;
            let mut config = stored_config(node, port)?;
            if index >= config.stubs.len() {
                return Err(stub_index_missing(index));
            }
            config.stubs[index] = stub;
            Ok(put_config_mutation(port, config))
        }
        Terminated::DeleteStubAt(port, index) => {
            let mut config = stored_config(node, port)?;
            if index >= config.stubs.len() {
                return Err(stub_index_missing(index));
            }
            config.stubs.remove(index);
            Ok(put_config_mutation(port, config))
        }
        Terminated::ReplaceStubById(port, id) => {
            let stub: Stub = parse(body)?;
            Ok(Mutation {
                ops: vec![ControlOp::PatchStubs {
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
                ops: vec![ControlOp::SetEnabled { port, enabled }],
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
            //
            let body = serde_json::to_vec(&table).map_err(|e| internal(&e.to_string()))?;
            Ok(Mutation {
                ops: vec![ControlOp::PutRoutes { table }],
                // No single stored record: a whole-table replace has no port to
                // label the revision header with, so it emits (and accepts) the
                // portless `routes@<revision>` token instead — conditioned on
                // the route table's revision, not on any one route. See
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
            let table = node.route_table().map_err(|e| internal(&e.to_string()))?;
            let Some(route) = table.routes.iter().find(|r| r.id == id) else {
                return Err(typed_error(
                    StatusCode::NOT_FOUND,
                    ErrorKind::NoSuchResource,
                    &format!("no route with id {id:?}"),
                ));
            };
            let body = serde_json::to_vec(route).map_err(|e| internal(&e.to_string()))?;
            Ok(Mutation {
                ops: vec![ControlOp::DeleteRoute { id }],
                port: None,
                render: Render::Captured {
                    body: Bytes::from(body),
                    content_type: json_content_type(),
                    status: StatusCode::OK,
                },
            })
        }
        // `DELETE /imposters/{port}/savedProxyResponses` (issue #226): one committed op deletes
        // the port's exactly-once markers fleet-wide, and every node's stale completion-cache
        // entries retire against the applied state (`completed_lookup`'s revision check) — no
        // fan-out, nothing a partitioned peer can miss forever. This is the one clear on this
        // front that still commits; the `savedRequests` clear is the local engine's own again
        // (D-74), proxied like every other verb on that path.
        Terminated::ClearSavedProxyResponses(port) => Ok(Mutation {
            ops: vec![ControlOp::ProxyRecordedClear { port }],
            port: Some(port),
            // Byte-identical to what upstream's own clear answers with (`handle_get(port, ...)`,
            // the imposter's own `GET` representation) — a re-render, not a canned message, is
            // what "re-render the imposter as upstream does" means here.
            render: Render::FetchAfter {
                path: format!("/imposters/{port}"),
                status: StatusCode::OK,
            },
        }),
        // A space teardown is a proxy plus a commit, and `build_mutation` renders neither shape:
        // the flow-state half is a proxy, not a state-machine record with a loopback path to
        // `FetchAfter` from, and the response the client gets is the proxy's own body, not a
        // re-read. Diverts to `terminate_space_teardown` in `terminate`.
        Terminated::SpaceTeardown(_, _) => Err(internal(
            "space teardown is served by terminate_space_teardown, not build_mutation",
        )),
        // A fan-out read, not a `ControlOp`. Diverts to `terminate_spaces_list` in `terminate`
        // before this is ever reached.
        Terminated::SpacesList(_) => Err(internal(
            "the spaces listing is served by terminate_spaces_list, not build_mutation",
        )),
        // A try commits nothing — it is an outbound exchange whose whole result is the response
        // body. Diverts to `terminate_try_imposter` in `terminate`, same shape as the space
        // teardown above.
        Terminated::TryImposter(_) => Err(internal(
            "a try is served by terminate_try_imposter, not build_mutation",
        )),
        // Diverted to `terminate_spec_compile` in `terminate` before this is ever reached —
        // and it is not a `ControlOp` at all: a compile commits nothing.
        Terminated::SpecCompile => Err(internal(
            "the OpenAPI compile is served by terminate_spec_compile, not build_mutation",
        )),
    }
}

/// Index-addressed stub edits and whole-list replacement have no by-id spelling
/// in the op set, so they commit as a full `PutImposter` of the stored config
/// with the stub list edited — the engine's #316 diff still patches only the
/// touched stubs in place.
///
fn put_config_mutation(port: u16, config: ImposterConfig) -> Mutation {
    Mutation {
        ops: vec![ControlOp::PutImposter {
            config: Box::new(config),
        }],
        port: Some(port),
        render: Render::FetchAfter {
            path: format!("/imposters/{port}"),
            status: StatusCode::OK,
        },
    }
}

/// The committed config for `port` from the local applied state, parsed.
// The Err IS the client response (the early-return channel this module
// uses everywhere); boxing it would just move the bytes to every call site.
#[allow(clippy::result_large_err)]
fn stored_config(node: &Arc<RaftNode>, port: u16) -> Result<ImposterConfig, Response<FrontBody>> {
    let stored = node
        .get_imposter(port)
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

fn mint(op_id: Uuid, op: ControlOp, expected_revision: Option<u64>) -> ControlRequest {
    // Pre-epoch clocks mint 0: only this op's dedup TTL weakens, never its
    // response (same reasoning as the node's own mint site).
    let issued_at_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    ControlRequest {
        op_id,
        // U-10 attribution (issue #855): the task-local `with_principal_scope` seam does not
        // survive the clustered write path (the state-machine apply task is not the request
        // task), so this field is the one that does. `None` since #550 — a keyed fleet has one
        // administrator, so naming them on every op would be a constant, not attribution. The
        // field stays on the envelope because the *log* still carries it and a future second
        // identity is a value change rather than a format change.
        principal: None,
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
                Some(PreconditionTarget::RouteTable)
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
/// the token this front itself emits — `<port>@<revision>` for a single
/// imposter, `routes@<revision>` for the route table (issue #210) — a bare
/// revision integer, or any of those wrapped in one pair of double quotes (a
/// normal ETag convention some HTTP clients apply automatically). Anything else
/// — a wildcard, a weak validator, a comma-separated list, or a token naming
/// the wrong subject — is refused: a precondition this front cannot evaluate
/// must never silently pass as unconditional.
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
            Some(port) => format!("{port}@<revision>"),
            None => format!("{ROUTES_REVISION_SUBJECT}@<revision>"),
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
    match expected_port {
        // An imposter token names its port. A token for a different port — or the route table's
        // — is refused rather than accepted for its number: the subject is what stops a
        // revision read from one record conditioning a write to another.
        Some(port) => {
            if addressed.parse::<u16>().map_err(|_| bad())? != port {
                return Err(bad());
            }
        }
        None => {
            if addressed != ROUTES_REVISION_SUBJECT {
                return Err(bad());
            }
        }
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
                    StubEdit::DeleteById { .. }
                    | StubEdit::DeleteBySpace { .. }
                    | StubEdit::Move { .. } => None,
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
    port: u16,
) -> Result<HashMap<String, RiftScriptConfig>, Response<FrontBody>> {
    // An absent imposter is the domain-optional empty registry: an unknown ref
    // then fails as UnknownRef, upstream's resolve-then-not-found order. A
    // storage or parse failure is a real fault and must not masquerade as
    // "unknown script ref" — it propagates as 500, same as stored_config.
    let Some(stored) = node
        .get_imposter(port)
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
        ControlOp::PatchStubs { port, edit, .. } => {
            let needs_registry = edit
                .0
                .iter()
                .any(|step| matches!(step, StubEdit::Add { .. } | StubEdit::ReplaceById { .. }));
            if !needs_registry {
                return Ok(());
            }
            let registry = stored_script_registry(node, *port)?;
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
                    StubEdit::DeleteById { .. }
                    | StubEdit::DeleteBySpace { .. }
                    | StubEdit::Move { .. } => continue,
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

/// `GET` a loopback admin path to render a committed write's response. Returns status, content
/// type, and the collected body.
///
/// **Presents the fleet's configured `--api-key`, never the caller's own credential** — the same
/// rule `proxy` follows, for the same reason: the loopback listener runs upstream's raw key
/// compare, and a cookie-authenticated request has no `Authorization` to forward. Getting this
/// wrong is not a quiet failure but it is a confusing one: the write *commits* and only the
/// render is refused, so the client is told `401` about a change that actually landed.
// A rendered refusal in the error channel, as in `ensure_session_key` above.
#[allow(clippy::result_large_err)]
async fn fetch(
    state: &FrontState,
    path: &str,
    host: Option<&HeaderValue>,
) -> Result<(StatusCode, Option<HeaderValue>, Bytes), Response<FrontBody>> {
    let uri: Uri = format!("http://{}{path}", state.upstream_admin)
        .parse()
        .map_err(|e| internal(&format!("render path: {e}")))?;
    let mut request = Request::builder().method(Method::GET).uri(uri);
    if let Some(key) = state.api_key.as_deref() {
        request = request.header("authorization", key);
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

    /// Issue #359. The two-segment space read is the only shape that carries an owner.
    #[test]
    fn space_read_target_matches_only_the_space_read() {
        assert_eq!(
            space_read_target("/imposters/4545/spaces/qa-flow"),
            Some((4545, "qa-flow".to_owned()))
        );
        assert_eq!(
            space_read_target("/admin/imposters/4545/spaces/qa-flow"),
            Some((4545, "qa-flow".to_owned()))
        );
        assert_eq!(
            space_read_target("/imposters/4545/spaces/qa-flow?x=1"),
            Some((4545, "qa-flow".to_owned()))
        );

        // The three-segment stubs route is a different route — the same distinction the
        // `SpaceTeardown` delete draws, and the one a `starts_with` would get wrong.
        assert_eq!(
            space_read_target("/imposters/4545/spaces/qa-flow/stubs"),
            None
        );
        assert_eq!(space_read_target("/imposters/4545/spaces/"), None);
        assert_eq!(space_read_target("/imposters/4545/spaces"), None);
        assert_eq!(space_read_target("/imposters/4545"), None);
        assert_eq!(space_read_target("/imposters"), None);
        assert_eq!(
            space_read_target("/imposters/notaport/spaces/qa-flow"),
            None
        );
    }

    /// Issue #374. The listing is the *one*-segment shape, and is exactly the shape
    /// `space_read_target` rejects — the two parsers partition the `spaces` routes between them
    /// rather than overlapping, so neither can shadow the other.
    #[test]
    fn spaces_list_target_matches_only_the_bare_listing() {
        assert_eq!(spaces_list_target("/imposters/4545/spaces"), Some(4545));
        assert_eq!(
            spaces_list_target("/admin/imposters/4545/spaces"),
            Some(4545)
        );
        assert_eq!(spaces_list_target("/imposters/4545/spaces?x=1"), Some(4545));

        // A trailing slash is the same resource, not a space whose id is empty.
        assert_eq!(spaces_list_target("/imposters/4545/spaces/"), Some(4545));

        // Every deeper shape belongs to another route.
        assert_eq!(spaces_list_target("/imposters/4545/spaces/qa-flow"), None);
        assert_eq!(
            spaces_list_target("/imposters/4545/spaces/qa-flow/stubs"),
            None
        );
        assert_eq!(spaces_list_target("/imposters/4545"), None);
        assert_eq!(spaces_list_target("/imposters"), None);
        assert_eq!(spaces_list_target("/imposters/notaport/spaces"), None);
    }

    /// Issue #359, and the assertion this feature actually turns on.
    ///
    /// The owner is decided by the flow id **under its context scope**, not by the bare id from the
    /// URL. An earlier draft hashed the bare id: every path and JSON test still passed, and it named
    /// the wrong node for every imposter-scoped flow — which is the default. This pins the key.
    #[test]
    fn the_owner_key_is_scoped_so_the_same_flow_id_differs_by_scope() {
        let ring = rift_cluster::Ring::new([1, 2, 3, 4, 5, 6, 7], 1);

        // Same caller-chosen id, two scopes, two different keys — and so, in general, two owners.
        let imposter_key = ContextScope::Imposter.scoped_flow_id(Some(4545), "cart");
        let fleet_key = ContextScope::Fleet.scoped_flow_id(Some(4545), "cart");
        assert_eq!(imposter_key, "i4545:cart");
        assert_eq!(fleet_key, "f:cart");

        // Two imposters, same flow id, imposter scope: different keys, so isolated (#152).
        assert_ne!(
            ContextScope::Imposter.scoped_flow_id(Some(4545), "cart"),
            ContextScope::Imposter.scoped_flow_id(Some(4646), "cart")
        );
        // Under fleet scope the port is irrelevant: one flow, one owner, shared by both imposters.
        assert_eq!(
            ContextScope::Fleet.scoped_flow_id(Some(4545), "cart"),
            ContextScope::Fleet.scoped_flow_id(Some(4646), "cart")
        );

        // Hashing the bare id is the bug this test exists for: it is a third key, equal to neither.
        let bare = ring.owner(OwnedKey::new(KeyClass::FlowKv, "cart"));
        let scoped = ring.owner(OwnedKey::new(KeyClass::FlowKv, &imposter_key));
        assert!(bare.is_some() && scoped.is_some());
        assert_ne!(
            "cart", imposter_key,
            "the URL's flow id is not the key the store owns it under"
        );
    }

    /// Issue #359. `owner` is added without disturbing what upstream already answered.
    #[test]
    fn rewrite_space_owner_adds_the_owner_and_preserves_the_body() {
        let body = br#"{"space":"qa-flow","stubs":[],"scenarios":[],"numberOfRequests":7}"#;
        let out = rewrite_space_owner(body, 3).expect("a JSON object decorates");
        let doc: serde_json::Value = serde_json::from_slice(&out).expect("still JSON");

        assert_eq!(doc["owner"], serde_json::json!("3"));
        // Every field upstream answered survives — the decoration adds, it does not rewrite.
        assert_eq!(doc["space"], "qa-flow");
        assert_eq!(doc["numberOfRequests"], 7);
        assert!(doc["stubs"].is_array());
        assert!(doc["scenarios"].is_array());
    }

    /// Issues #359 and #374. A raft id is a `u64`; JSON numbers are read back by JavaScript as
    /// IEEE-754 doubles, so every id above 2^53 - 1 rounds. `9_007_199_254_740_993` is
    /// 2^53 + 1 — the smallest id that survives a round trip only as a string, and the value
    /// `cluster_api`'s own node-id tests pin for the same reason.
    ///
    /// This is a regression test in the literal sense: the single-space read shipped this field
    /// as a bare number in #359, so a fleet whose ids ran that high would have sent an operator
    /// to a *neighbouring* node with no error anywhere.
    #[test]
    fn a_space_owner_above_the_js_safe_integer_survives_the_round_trip() {
        let body = br#"{"space":"qa-flow","stubs":[],"scenarios":[],"numberOfRequests":0}"#;
        let out = rewrite_space_owner(body, 9_007_199_254_740_993).expect("decorates");
        let doc: serde_json::Value = serde_json::from_slice(&out).expect("still JSON");

        assert_eq!(doc["owner"], serde_json::json!("9007199254740993"));
    }

    /// Issue #359. A body that is not a JSON object errors rather than inventing a shape; the
    /// caller logs and passes the original through, so nothing is silently defaulted.
    #[test]
    fn rewrite_space_owner_refuses_a_body_it_cannot_decorate() {
        assert!(rewrite_space_owner(b"not json at all", 1).is_err());
        assert!(rewrite_space_owner(b"[1,2,3]", 1).is_err());
    }

    /// The knobs an imposter document carries when nothing was configured (#370).
    fn default_knobs() -> ResolvedKnobs {
        let config = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
        }))
        .expect("parses");
        ResolvedKnobs::from_imposter(&config).expect("valid")
    }

    /// Issue #370. `_rift.flowStateResolved` is added beside upstream's `_rift.flowState`, and
    /// every key and value in upstream's block survives unchanged.
    ///
    /// This is a compatibility contract, not tidiness: upstream renders
    /// `_rift.flowState.flowIdSource` as a flat string and rift-verify reads it there to drive
    /// correlated isolation. Rewriting it in EE would break rift-verify against an EE cluster and
    /// diverge EE from a field upstream owns — which the `parity` CI job exists to catch.
    #[test]
    fn upstream_flow_state_is_left_untouched_by_the_decoration() {
        let body = br#"{"port":4545,"protocol":"http","_rift":{"flowState":{"backend":"inmemory","ttlSeconds":300,"flowIdSource":"header:X-Mock-Space"},"warnings":[]}}"#;

        let out = rewrite_flow_state_resolved(body, &default_knobs()).expect("decorates");
        let doc: serde_json::Value = serde_json::from_slice(&out).expect("still JSON");

        // Upstream's block, exactly as it arrived — including the flat-string flowIdSource.
        assert_eq!(
            doc["_rift"]["flowState"]["flowIdSource"],
            "header:X-Mock-Space"
        );
        assert!(doc["_rift"]["flowState"]["flowIdSource"].is_string());
        assert_eq!(doc["_rift"]["flowState"]["backend"], "inmemory");
        assert_eq!(doc["_rift"]["flowState"]["ttlSeconds"], 300);
        assert!(doc["_rift"]["warnings"].is_array());
        // And the rest of the document survives — the decoration adds, it does not rewrite.
        assert_eq!(doc["port"], 4545);
        assert_eq!(doc["protocol"], "http");
        // The new sibling block is present.
        assert_eq!(
            doc["_rift"]["flowStateResolved"]["durability"]["value"],
            "async"
        );
    }

    /// Issue #370 — **security regression test**.
    ///
    /// Upstream's `expose_flow_state` is an allowlist precisely because `flowState.redis.url` can
    /// carry a credentialed connection string (`redis://user:secret@host`), and it has its own test
    /// asserting the credential survives nowhere in the exposed value. A decoration that rendered
    /// the *stored* config rather than the parsed knobs would undo that redaction from the EE side —
    /// so this asserts the boundary holds through EE's addition, both when upstream has already
    /// stripped `redis` and in the belt-and-braces case where a `redis` block is present in the body
    /// being decorated.
    #[test]
    fn the_redis_block_is_never_exposed_by_the_decoration() {
        let config = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
            "_rift": { "flowState": {
                "backend": "redis",
                "redis": { "url": "redis://user:secret@host:6379", "keyPrefix": "rift:" },
                "durability": "sync",
            }},
        }))
        .expect("parses");
        let knobs = ResolvedKnobs::from_imposter(&config).expect("valid");

        // Upstream's allowlist has already stripped `redis` from what it answered.
        let body = br#"{"port":4545,"_rift":{"flowState":{"backend":"redis","ttlSeconds":300}}}"#;
        let out = rewrite_flow_state_resolved(body, &knobs).expect("decorates");
        let text = String::from_utf8(out).expect("utf-8");

        assert!(
            !text.contains("secret"),
            "the credential must not survive: {text}"
        );
        assert!(
            !text.contains("redis://"),
            "the connection URL must not survive: {text}"
        );
        let doc: serde_json::Value = serde_json::from_slice(text.as_bytes()).expect("still JSON");
        assert!(doc["_rift"]["flowStateResolved"].get("redis").is_none());
        assert!(doc["_rift"]["flowState"].get("redis").is_none());
        // The knob that *is* published still came through.
        assert_eq!(
            doc["_rift"]["flowStateResolved"]["durability"]["value"],
            "sync"
        );
        assert_eq!(
            doc["_rift"]["flowStateResolved"]["durability"]["source"],
            "set"
        );
    }

    /// Issue #370. An imposter whose body carries no `_rift` at all still gets the resolved block —
    /// the console renders one panel for every imposter, so "absent" must mean "inherited", not
    /// "no panel".
    #[test]
    fn the_resolved_block_is_added_when_the_body_has_no_rift_at_all() {
        let body = br#"{"port":4545,"protocol":"http"}"#;
        let out = rewrite_flow_state_resolved(body, &default_knobs()).expect("decorates");
        let doc: serde_json::Value = serde_json::from_slice(&out).expect("still JSON");

        assert_eq!(
            doc["_rift"]["flowStateResolved"]["readConsistency"]["value"],
            "strong"
        );
        assert_eq!(
            doc["_rift"]["flowStateResolved"]["readConsistency"]["source"],
            "default"
        );
        assert_eq!(
            doc["_rift"]["flowStateResolved"]["flowIdSource"]["value"],
            "imposter_port"
        );
    }

    /// Issue #370 — regression. The knobs decoration rebuilds the response through
    /// `buffered_response`, whose header map starts empty, so it is the stage that can destroy
    /// what the proxied response already carried.
    ///
    /// When this was written, `decorate_number_of_requests` ran first and stamped
    /// `Rift-Cluster-Partial` on the single-imposter read; D-74 (#552) removed that decoration,
    /// and the header no longer rides this read at all. The two headers below are now stand-ins
    /// for whatever the proxied response carried — `Rift-Cluster-Revision` is the one that
    /// matters in production — and the claim is unchanged: nothing set on the response before the
    /// rebuild may be lost by it.
    ///
    /// Asserted on both branches, because the rebuild happens on the failure path too.
    #[tokio::test]
    async fn the_knobs_decoration_keeps_headers_an_earlier_decoration_set() {
        for body in [
            &br#"{"port":4545,"_rift":{"flowState":{}}}"#[..],
            // The un-decoratable body: the pass-through branch rebuilds the response as well.
            &b"not json at all"[..],
        ] {
            let mut response =
                buffered_response(StatusCode::OK, Bytes::from(body), json_content_type())
                    .expect("a response");
            set_header(&mut response, HEADER_PARTIAL, "true");
            set_header(&mut response, HEADER_REVISION, "4545@7.1");

            let out = decorate_flow_state_resolved(response, default_knobs()).await;

            assert_eq!(
                out.headers()
                    .get(HEADER_PARTIAL)
                    .map(|v| v.to_str().expect("ascii")),
                Some("true"),
                "the partial stamp must survive the knobs decoration"
            );
            assert_eq!(
                out.headers()
                    .get(HEADER_REVISION)
                    .map(|v| v.to_str().expect("ascii")),
                Some("4545@7.1"),
            );
            // The rebuild's own content-type is kept, not the carried one.
            assert_eq!(
                out.headers()
                    .get("content-type")
                    .map(|v| v.to_str().expect("ascii")),
                Some("application/json"),
            );
        }
    }

    /// Issue #370. Same polarity as [`rewrite_space_owner`]: a body that is not a JSON object
    /// errors, and the caller logs and passes the original through rather than inventing a shape.
    #[test]
    fn rewrite_flow_state_resolved_refuses_a_body_it_cannot_decorate() {
        assert!(rewrite_flow_state_resolved(b"not json at all", &default_knobs()).is_err());
        assert!(rewrite_flow_state_resolved(b"[1,2,3]", &default_knobs()).is_err());
        // `_rift` present but not an object is equally undecoratable.
        assert!(rewrite_flow_state_resolved(br#"{"_rift":"nope"}"#, &default_knobs()).is_err());
    }

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
            // Issue #537: the replicated space-stub delete rides alongside the flow-state proxy,
            // so this route terminates too — the flow-state half is still proxied *inside*
            // `terminate_space_teardown`, but `classify` itself recognizes the route rather than
            // falling through entirely.
            (Method::DELETE, "/imposters/4545/spaces/flow-1"),
            // Issue #374: the spaces **listing** terminates too — there is no upstream
            // `["spaces"]` route to proxy to at all (see `spaces_list_target`'s doc).
            (Method::GET, "/imposters/4545/spaces"),
        ];
        for (method, path) in terminated {
            assert!(
                classify(&method, path).is_some(),
                "{method} {path} must terminate"
            );
        }

        // The listing's two-segment shape must not swallow the three-segment single-space read:
        // `spaces_list_target` requires an absent or empty third segment, so a real flow id keeps
        // this proxied to the engine exactly as it always has been.
        assert!(
            classify(&Method::GET, "/imposters/4545/spaces/qa-flow").is_none(),
            "GET .../spaces/{{flowId}} must stay proxied, not be swept into the listing route"
        );

        // Runtime-state mutations and every read stay proxied: replicating
        // them is #15/#16 territory, and reads must hit the live engine.
        let proxied = [
            (Method::GET, "/imposters"),
            (Method::GET, "/imposters/4545"),
            (Method::POST, "/imposters/4545/verify"),
            (Method::PUT, "/imposters/4545/scenarios/checkout/state"),
            (Method::POST, "/imposters/4545/scenarios/reset"),
            (Method::GET, "/config"),
            (Method::GET, "/metrics"),
            (Method::POST, "/_reload"),
        ];
        for (method, path) in proxied {
            assert!(
                classify(&method, path).is_none(),
                "{method} {path} must proxy"
            );
        }

        // D-74: the whole recorded-request surface proxies again, under either spelling and
        // whatever the query says. The journal is upstream's own and per node, so there is
        // nothing for this front to terminate — the read, its `?since=` cursor form, the SSE
        // tail and the clear all reach this node's engine and answer with upstream's own
        // semantics. Pinned as an exhaustive row rather than a comment because #223/#224/#225
        // terminated exactly these and a half-reverted classifier would leave one of them
        // answering out of a subsystem that no longer exists.
        for (method, path) in [
            (Method::GET, "/imposters/4545/requests"),
            (Method::GET, "/imposters/4545/savedRequests"),
            (Method::DELETE, "/imposters/4545/requests"),
            (Method::DELETE, "/imposters/4545/savedRequests"),
            (Method::GET, "/imposters/4545/savedRequests/stream"),
            // The `/admin/imposters/` alias #223 invented for the merged read goes with it: it
            // never existed upstream, so there is nothing left for it to be a second spelling of.
            (Method::GET, "/admin/imposters/4545/requests"),
            (Method::GET, "/admin/imposters/4545/savedRequests"),
            (Method::DELETE, "/admin/imposters/4545/requests"),
            (Method::DELETE, "/admin/imposters/4545/savedRequests"),
            // The fleet-wide pair, which had no upstream to proxy to at all.
            (Method::GET, "/admin/requests"),
            (Method::GET, "/admin/requests/stream"),
        ] {
            assert!(
                classify(&method, path).is_none(),
                "{method} {path} must not terminate: the request journal is upstream's own \
                 (D-74)"
            );
        }

        // Issue #537: a space teardown classifies with the port and flow id extracted, exactly
        // the shape `terminate_space_teardown` needs.
        assert!(
            matches!(
                classify(&Method::DELETE, "/imposters/4545/spaces/flow-1"),
                Some(Terminated::SpaceTeardown(4545, flow)) if flow == "flow-1"
            ),
            "DELETE .../spaces/{{flow}} must terminate with the port and flow extracted"
        );
        // Every other method on the same two-segment shape stays proxied — only the delete
        // gains a replicated half; a write there is `SpaceStubs`' three-segment sibling, a
        // different route entirely.
        for method in [Method::GET, Method::PUT, Method::POST] {
            assert!(
                classify(&method, "/imposters/4545/spaces/flow-1").is_none(),
                "{method} .../spaces/{{flow}} must still proxy"
            );
        }
        assert!(
            classify(&Method::DELETE, "/imposters/4545/spaces/").is_none(),
            "an empty flow id names no space to tear down"
        );

        // Flow-state inspection under the `/admin/imposters/` prefix stays this front's
        // non-concern and falls through to the proxy.
        assert!(
            classify(&Method::DELETE, "/admin/imposters/4545/flow-state/flow-9").is_none(),
            "DELETE .../flow-state/... must not be captured by this front"
        );

        // An unparseable port is not this surface's route at all.
        assert!(classify(&Method::DELETE, "/imposters/not-a-port").is_none());
    }

    // ---- Spaces listing (issue #374) ---------------------------------------------------------
    //
    // `GET /imposters/{port}/spaces` had zero HTTP-level coverage: everything above pins
    // `classify`, but nothing drove a real request through `terminate` into
    // `terminate_spaces_list` and inspected the body it renders. These do, over `test_front_over`
    // — a bound front plus `reqwest`, since GET reads terminate exactly like writes do (see
    // `classify`'s own routing) and nothing here needs `upstream_admin` dialled.
    //
    // Most of these run over `test_front_over` as-is, whose `FlowNet` is deliberately never bound
    // to `node`'s ring (see its own doc) — exactly right for pinning the envelope (field names,
    // `unavailable`'s two refusal states, `durability`'s presence/absence), since an unbound net
    // answers `fleet_spaces` via its own cluster-view-unavailable arm regardless of scope. The one
    // test below that needs a real row builds its own front over a bound one-voter ring instead
    // (`test_front_with_bound_flow`) rather than stretching `test_front_over` to cover every case.

    /// `GET /imposters/{port}/spaces` against the bound front, returned as `(status, body)` —
    /// no headers needed here, since this route carries no cursor.
    async fn read_spaces(front: &AdminFront, port: u16) -> (u16, String) {
        let addr = front.local_addr();
        let response = reqwest::get(format!("http://{addr}/imposters/{port}/spaces"))
            .await
            .expect("the front answers");
        let status = response.status().as_u16();
        let body = response.text().await.expect("a body");
        (status, body)
    }

    /// Commit `port`'s config through Raft so `terminate_spaces_list` has something to resolve a
    /// scope from. A one-voter `cluster_init` is enough to commit locally — nothing here depends
    /// on quorum size, only on the config being *applied*, which is what `imposter_scope` and
    /// `flow_state_resolved` both read from.
    async fn seed_imposter(node: &Arc<RaftNode>, port: u16, flow_state: serde_json::Value) {
        node.put_imposter(
            serde_json::from_value(serde_json::json!({
                "port": port,
                "protocol": "http",
                "_rift": { "flowState": flow_state },
            }))
            .expect("imposter config parses"),
        )
        .await
        .expect("commit the imposter");
    }

    /// A port with no applied config at all resolves no scope (`imposter_config` reads
    /// `Ok(None)`, which `imposter_scope` flattens the identical way a read error would — see
    /// that function's own doc on why the two are not worth telling apart to the caller). This
    /// pins two things at once because they share the one cause: `unavailable` names it, and
    /// `durability` is omitted rather than defaulted (`flow_state_resolved` hits the same
    /// `Ok(None)` and returns early).
    #[tokio::test]
    async fn spaces_list_with_no_imposter_is_scope_unresolved_and_omits_durability() {
        let (front, _node, _dir) = test_front_over().await;

        let (status, body) = read_spaces(&front, 9999).await;

        assert_eq!(status, 200, "body: {body}");
        let doc: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(doc["unavailable"], "scope-unresolved", "{body}");
        assert_eq!(doc["spaces"], serde_json::json!([]), "{body}");
        assert_eq!(doc["partial"], true, "{body}");
        assert!(
            doc.get("durability").is_none(),
            "an unresolvable scope means the knobs could not be read either; durability must be \
             omitted, never defaulted to \"async\": {body}"
        );
    }

    /// A `fleet`-scoped listing is **served** since #550. It used to be refused because the `f:`
    /// namespace carries no tenant component and one shard served every tenant's imposters, so
    /// enumerating it handed one tenant another's flow ids. With one administrator there is no
    /// boundary left to cross, and `scope-unresolved` is the only refusal that remains.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spaces_list_for_a_fleet_scoped_imposter_is_served() {
        let (front, node, _dir) = test_front_over().await;
        node.cluster_init()
            .await
            .expect("single-voter cluster init");
        seed_imposter(&node, 4545, serde_json::json!({ "contextScope": "fleet" })).await;

        let (status, body) = read_spaces(&front, 4545).await;

        assert_eq!(status, 200, "body: {body}");
        let doc: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert!(
            doc.get("unavailable").is_none(),
            "a fleet-scoped listing is servable since #550: {body}"
        );
        assert_eq!(doc["spaces"], serde_json::json!([]), "{body}");
    }

    /// The ordinary path: a resolvable, imposter-scoped config carries no `unavailable` key at
    /// all — its absence is itself the signal the console keys the generic partial banner off,
    /// so a regression that started stamping it unconditionally would silently break that gate.
    /// Also pins the envelope's field names (`spaces`/`partial`/`durability`); a row's own field
    /// names (`space`/`entryCount`/`owner`) are pinned by
    /// `spaces_list_row_shape_and_content_reflect_a_real_write` below, which is the one test in
    /// this group that actually has a row to inspect.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spaces_list_for_an_ordinary_imposter_carries_no_unavailable_key() {
        let (front, node, _dir) = test_front_over().await;
        node.cluster_init()
            .await
            .expect("single-voter cluster init");
        // Default scope: `contextScope` absent entirely, exactly like an imposter nobody has
        // touched `_rift.flowState` on.
        seed_imposter(&node, 4545, serde_json::json!({})).await;

        let (status, body) = read_spaces(&front, 4545).await;

        assert_eq!(status, 200, "body: {body}");
        let doc: serde_json::Value = serde_json::from_str(&body).expect("json body");
        let obj = doc.as_object().expect("an object");
        assert!(
            !obj.contains_key("unavailable"),
            "a resolvable imposter scope must not carry the refusal field: {body}"
        );
        assert!(obj.contains_key("spaces"), "{body}");
        assert!(obj.contains_key("partial"), "{body}");
        assert_eq!(
            doc["durability"],
            serde_json::json!({ "value": "async", "source": "default" }),
            "the knobs are readable here, so durability must publish the resolved value, not be \
             omitted: {body}"
        );
        // No entries were ever written into this harness's (deliberately unbound) `FlowNet`, so
        // the list itself is empty here — `spaces_list_row_shape_and_content_reflect_a_real_write`
        // below is the one test in this group with a bound ring and a real row to inspect.
        assert_eq!(doc["spaces"], serde_json::json!([]), "{body}");
    }

    /// [`test_front_over`], but with the flow subsystem actually bound to `node`'s own (one-voter)
    /// ring rather than left detached — the one thing that harness's own doc says it does not do.
    /// Needed here, and only here, because a row in the spaces listing requires `fleet_spaces` to
    /// resolve a real owner rather than answer through its cluster-view-unavailable arm.
    async fn test_front_with_bound_flow()
    -> (AdminFront, Arc<RaftNode>, Arc<FlowNet>, tempfile::TempDir) {
        let (node, dir) = test_node().await;
        node.cluster_init()
            .await
            .expect("single-voter cluster init");
        let net = FlowNet::new(rift_cluster::stores::FlowShard::in_memory(
            rift_cluster::stores::ShardConfig::default(),
        ));
        net.bind(
            &node,
            rift_cluster::stores::FlowBindConfig {
                bridge: rift_cluster::BridgeConfig::for_workers(1),
                // Effectively off: this test writes directly through the owner and reads back
                // immediately, so the anti-entropy loop has nothing to do and no reason to run
                // mid-test.
                anti_entropy_interval: Duration::from_secs(3600),
            },
        )
        .expect("bind flow net");
        let front = bind(
            FrontConfig {
                public_addr: "127.0.0.1:0".to_owned(),
                upstream_admin: "127.0.0.1:1".parse().expect("addr"),
                api_key: None,
                allow_injection: false,
                scripts_dir: None,
                barrier: crate::cli::WriteBarrier::None,
                barrier_timeout: Duration::from_secs(1),
                admin_async: false,
                readiness: Arc::new(crate::readiness::Readiness::awaiting([])),
                flow_net: Arc::clone(&net),
            },
            &node,
        )
        .await
        .expect("front binds");
        (front, node, net, dir)
    }

    /// The row shape `terminate_spaces_list` renders for a real space: `space`/`entryCount`/
    /// `owner` field names, `owner` as the decimal-string encoding the doc on that mapping
    /// explains (never a JSON number — a `NodeId` above 2^53-1 would round on the wire), and
    /// `partial: false` because the one-voter ring has no peer to time out on. The filtering
    /// logic that decides *which* entries are "this node's own" is `owned_spaces`'s pure unit
    /// coverage in `stores/flow.rs`; this test only owns what the HTTP envelope does with the row
    /// that logic hands back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spaces_list_row_shape_and_content_reflect_a_real_write() {
        use rift_cluster_base::seams::FlowStoreProvider as _;

        let (front, node, net, _dir) = test_front_with_bound_flow().await;
        seed_imposter(&node, 4545, serde_json::json!({})).await;

        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
        }))
        .expect("config parses");
        let store = rift_cluster::stores::ClusteredFlowStoreProvider::new(Arc::clone(&net))
            .provide(&config)
            .expect("the clustered provider always provides");
        tokio::task::spawn_blocking(move || {
            store.set("checkout", "step", serde_json::json!("paid"))
        })
        .await
        .expect("blocking op")
        .expect("write through the owner");

        let (status, body) = read_spaces(&front, 4545).await;

        assert_eq!(status, 200, "body: {body}");
        let doc: serde_json::Value = serde_json::from_str(&body).expect("json body");
        let rows = doc["spaces"].as_array().expect("a spaces array");
        assert_eq!(rows.len(), 1, "exactly the one space written: {body}");
        assert_eq!(rows[0]["space"], "checkout", "{body}");
        assert_eq!(rows[0]["entryCount"], 1, "{body}");
        assert_eq!(
            rows[0]["owner"],
            serde_json::json!(node.id().to_string()),
            "the owner must be the decimal-string NodeId, not a bare number: {body}"
        );
        assert_eq!(
            doc["partial"], false,
            "a one-voter ring has no peer to fail; this answer is complete: {body}"
        );
    }

    #[test]
    fn the_query_value_reader_finds_exactly_the_named_parameter() {
        assert_eq!(query_param(Some("port=4545"), "port"), Some("4545"));
        assert_eq!(
            query_param(Some("name=petstore&port=4545&x=5"), "port"),
            Some("4545"),
            "position in the query string must not matter"
        );
        assert_eq!(
            query_param(Some("portly=no&port=yes"), "port"),
            Some("yes"),
            "a parameter whose name merely starts with `port` is a different parameter"
        );
        assert_eq!(
            query_param(Some("port"), "port"),
            Some(""),
            "a valueless `port` is an empty value, not an absent one — the caller 400s on it"
        );
        assert_eq!(query_param(Some("name=x"), "port"), None);
        assert_eq!(query_param(None, "port"), None);
        // Raw and undecoded: the value must arrive byte-identical, or the compile names an
        // imposter the caller never asked for.
        assert_eq!(
            query_param(Some("name=pet-store_v2"), "name"),
            Some("pet-store_v2"),
            "the submitted value must survive the query string unchanged"
        );
    }

    #[test]
    fn classify_terminates_exactly_the_try_surface() {
        assert!(matches!(
            classify(&Method::POST, "/admin/imposters/4545/try"),
            Some(Terminated::TryImposter(4545))
        ));

        for (method, path) in [
            // Read verbs do not try. A try mutates (scenario state, the request log, proxy
            // recordings), so it is POST-only; anything else falls through.
            (Method::GET, "/admin/imposters/4545/try"),
            (Method::PUT, "/admin/imposters/4545/try"),
            (Method::DELETE, "/admin/imposters/4545/try"),
            // Not on the canonical Mountebank-published prefix — see `classify`'s own comment.
            (Method::POST, "/imposters/4545/try"),
            // A port that is not a port.
            (Method::POST, "/admin/imposters/notaport/try"),
            (Method::POST, "/admin/imposters/70000/try"),
            (Method::POST, "/admin/imposters//try"),
            // Neighbouring and deeper shapes.
            (Method::POST, "/admin/imposters/4545/try/again"),
            (Method::POST, "/admin/imposters/4545/tryout"),
            (Method::POST, "/admin/imposters/4545"),
        ] {
            assert!(
                !matches!(classify(&method, path), Some(Terminated::TryImposter(_))),
                "{method} {path} must not classify as a try"
            );
        }
    }

    #[test]
    fn classify_terminates_only_the_stateless_compile() {
        assert!(matches!(
            classify(&Method::POST, "/specs/compile"),
            Some(Terminated::SpecCompile)
        ));
        // The query is not part of the match — `classify` cannot see one (D-74) — so a missing
        // `port` is the handler's `400`, not a route that does not exist. The difference matters,
        // because 404 would send a caller looking for a route they typed correctly.

        for (method, path) in [
            (Method::GET, "/specs"),
            (Method::POST, "/specs"),
            (Method::PUT, "/specs"),
            (Method::DELETE, "/specs"),
            (Method::GET, "/specs/compile"),
            (Method::PUT, "/specs/compile"),
            (Method::DELETE, "/specs/compile"),
            (Method::GET, "/specs/petstore"),
            (Method::PUT, "/specs/petstore"),
            (Method::DELETE, "/specs/petstore"),
            (Method::POST, "/specs/petstore/compile"),
            (Method::POST, "/specs/petstore/deploy"),
        ] {
            assert!(
                classify(&method, path).is_none(),
                "{method} {path} must not terminate"
            );
        }
    }

    /// The front's cap is the compiler's own — the front bounds the body before parsing it, and
    /// a body it accepted must not then be refused by `compile` for a different number.
    #[test]
    fn the_compile_cap_is_the_compilers_own() {
        assert_eq!(MAX_SPEC_BYTES, rift_cluster_spec::MAX_SPEC_BYTES);
        assert_eq!(MAX_SPEC_BYTES, 4 * 1024 * 1024);
    }

    /// The front-door route surface (issue #131): `PUT`/`DELETE` terminate,
    /// `GET` does not (it never reaches `classify` at all — `handle` answers
    /// it directly, since there is no upstream endpoint to proxy it to).
    #[test]
    fn classify_terminates_exactly_the_route_write_surface() {
        assert!(matches!(
            classify(&Method::PUT, "/front-door/routes"),
            Some(Terminated::PutRoutes)
        ));
        assert!(matches!(
            classify(&Method::DELETE, "/front-door/routes/svc"),
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
                classify(&method, path).is_none(),
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
            snapshot_log_entries: None,
        };
        let node = RaftNode::start(config).await.expect("node starts");
        (Arc::new(node), dir)
    }

    /// A bound front over a throwaway node, for the accept-loop observation
    /// tests. `upstream_admin` is never dialled — nothing sends a request.
    async fn test_front() -> (AdminFront, Arc<RaftNode>, tempfile::TempDir) {
        let (front, node, dir) = test_front_over().await;
        (front, node, dir)
    }

    /// A bound front over a throwaway node whose `FlowNet` is deliberately never bound to the
    /// node's ring — enough to drive the terminated reads that answer from this node alone.
    async fn test_front_over() -> (AdminFront, Arc<RaftNode>, tempfile::TempDir) {
        let (node, dir) = test_node().await;
        let front = bind(
            FrontConfig {
                public_addr: "127.0.0.1:0".to_owned(),
                upstream_admin: "127.0.0.1:1".parse().expect("addr"),
                api_key: None,
                allow_injection: false,
                scripts_dir: None,
                barrier: crate::cli::WriteBarrier::None,
                barrier_timeout: Duration::from_secs(1),
                admin_async: false,
                readiness: Arc::new(crate::readiness::Readiness::awaiting([])),
                // In-memory and never bound to `node`'s ring: this only needs to satisfy
                // `FrontConfig`'s required field.
                flow_net: FlowNet::new(rift_cluster::stores::FlowShard::in_memory(
                    rift_cluster::stores::ShardConfig::default(),
                )),
            },
            &node,
        )
        .await
        .expect("front binds");
        (front, node, dir)
    }
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
        let mut op = ControlOp::DeleteImposter { port: 4545 };

        let result = resolve_op_scripts(&mut op, &node, &ScriptBaseDir::Unconfigured, None);
        assert!(result.is_ok());
        assert!(matches!(op, ControlOp::DeleteImposter { port: 4545, .. }));

        let mut op = ControlOp::SetEnabled {
            port: 4545,
            enabled: false,
        };
        assert!(resolve_op_scripts(&mut op, &node, &ScriptBaseDir::Unconfigured, None).is_ok());

        // Move/DeleteById steps carry no stub payload, so no registry read and
        // no resolution — even against a port that has no imposter at all.
        let mut op = ControlOp::PatchStubs {
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
            ControlOp::DeleteImposter { port: 4545 },
            ControlOp::SetEnabled {
                port: 4545,
                enabled: false,
            },
            ControlOp::PatchStubs {
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
        assert_eq!(parse_if_match("4545@17", Some(4545)).expect("token"), 17);
        assert_eq!(
            parse_if_match("\"4545@17\"", Some(4545)).expect("etag-quoted token"),
            17
        );
        assert_eq!(parse_if_match("17", Some(4545)).expect("bare revision"), 17);
    }

    #[test]
    fn parse_if_match_rejects_wildcards_weak_validators_and_mismatches() {
        for bad in [
            "*",
            "W/\"4545@17\"",
            "9999@17",
            "routes@17",
            "4545@seventeen",
            "4545@17, 4545@18",
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
        assert_eq!(parse_if_match("routes@17", None).expect("token"), 17);
        assert_eq!(
            parse_if_match("\"routes@17\"", None).expect("etag-quoted token"),
            17
        );
        assert_eq!(
            parse_if_match(" routes@0 ", None).expect("a never-written table is revision 0"),
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
            ("4545@17", None),
            // Portless token, single-imposter target.
            ("routes@17", Some(4545)),
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
            "W/\"routes@17\"",
            "other@17",
            "routes@seventeen",
            "routes@17, routes@18",
            "@17",
            "",
        ] {
            let response =
                parse_if_match(bad, None).expect_err(&format!("{bad:?} must be refused"));
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{bad:?}");
        }
    }

    /// Issue #335's exchange rules — and, since issue #344, the property that makes them all
    /// hold: **the exchange never opens a socket.** `perform_try` is handed a *dispatch* — in
    /// production, this node's own engine (`RaftNode::dispatch_to_imposter`) — and speaks HTTP/1
    /// to it over an in-memory connection. So these tests hand it canned in-process services, not
    /// canned TCP servers: the budget, the body cap, the lossy flags, that a redirect is *returned*
    /// rather than chased, and that a connection-fault stub is an explicit outcome are all
    /// properties of `perform_try` alone, provable in milliseconds without a cluster.
    mod try_exchange {
        use super::*;
        use std::pin::Pin;
        use std::sync::Mutex;

        // Only the tests name the kinds; production code asks `tcp_fault_carrier` for the name
        // and never branches on the variant, so this import stays out of the module header.
        use rift_cluster_base::seams::TcpFaultKind;

        /// The record a canned service keeps of the one request it served.
        #[derive(Debug, Clone)]
        struct Arrived {
            method: String,
            /// `Uri::to_string()` — for an origin-form request this is the path and query.
            target: String,
            /// `Uri::authority()` — must always be `None`: the caller's path can never carry one.
            authority: Option<String>,
            headers: Vec<(String, String)>,
            body: Vec<u8>,
        }

        type Answer = Pin<Box<dyn Future<Output = Option<Response<Full<Bytes>>>> + Send>>;

        /// A canned in-process service: records what arrived, then answers `response`. Returns
        /// the dispatch `perform_try` takes and the shared record.
        #[allow(clippy::type_complexity)] // the pair is two test-scaffolding handles, not a domain type worth naming.
        fn canned(
            response: Response<Full<Bytes>>,
        ) -> (
            impl FnOnce(Request<Incoming>) -> Answer + Send + 'static,
            Arc<Mutex<Option<Arrived>>>,
        ) {
            let seen = Arc::new(Mutex::new(None));
            let record = Arc::clone(&seen);
            let dispatch = move |req: Request<Incoming>| -> Answer {
                Box::pin(async move {
                    let (parts, body) = req.into_parts();
                    let body = body
                        .collect()
                        .await
                        .expect("the test body reads")
                        .to_bytes()
                        .to_vec();
                    *record.lock().expect("record") = Some(Arrived {
                        method: parts.method.to_string(),
                        target: parts.uri.to_string(),
                        authority: parts.uri.authority().map(ToString::to_string),
                        headers: parts
                            .headers
                            .iter()
                            .map(|(name, value)| {
                                (
                                    name.as_str().to_owned(),
                                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                                )
                            })
                            .collect(),
                        body,
                    });
                    Some(response)
                })
            };
            (dispatch, seen)
        }

        /// A dispatch that must never be reached: an envelope refused up front is refused
        /// *before* anything is dispatched, and this is what proves it.
        fn never() -> impl FnOnce(Request<Incoming>) -> Answer + Send + 'static {
            |_req: Request<Incoming>| -> Answer {
                Box::pin(async { panic!("the exchange must not reach the imposter") })
            }
        }

        fn answer(
            status: u16,
            headers: &[(&str, &[u8])],
            body: impl Into<Bytes>,
        ) -> Response<Full<Bytes>> {
            let mut response = Response::builder().status(status);
            for (name, value) in headers {
                response = response.header(
                    HeaderName::from_bytes(name.as_bytes()).expect("header name"),
                    HeaderValue::from_bytes(value).expect("header value"),
                );
            }
            response
                .body(Full::new(body.into()))
                .expect("response builds")
        }

        fn spec(method: &str, path: &str) -> TryRequest {
            TryRequest {
                method: method.to_owned(),
                path: path.to_owned(),
                headers: Vec::new(),
                body: None,
            }
        }

        const PORT: u16 = 4545;
        const CAP: usize = TRY_MAX_RESPONSE_BYTES;
        const BUDGET: Duration = Duration::from_secs(5);

        /// The containment property with the most weight behind it: a `3xx` comes back as data.
        ///
        /// Following it is the *only* way an exchange could end up talking to something other than
        /// the imposter — a mock is free to answer `Location: http://169.254.169.254/`, and a
        /// client that chased it would have turned a "send a request to a mock you can already
        /// see" endpoint into a general-purpose SSRF. In-process there is nothing to chase *with*
        /// (no client, no socket), and this pins that the `302` is what the caller sees.
        #[tokio::test]
        async fn a_redirect_is_returned_not_followed() {
            let elsewhere = "http://169.254.169.254/latest/meta-data/";
            let (dispatch, seen) = canned(answer(302, &[("location", elsewhere.as_bytes())], ""));

            let outcome = perform_try(dispatch, PORT, &spec("GET", "/anything"), BUDGET, CAP)
                .await
                .expect("a redirect is an answer, not a failure");

            assert_eq!(outcome.status, 302, "the redirect itself is the result");
            let location = outcome
                .headers
                .iter()
                .find(|h| h.name.eq_ignore_ascii_case("location"))
                .expect("the Location header rides back to the caller");
            assert_eq!(
                location.value, elsewhere,
                "the caller sees where it pointed; the server does not go there"
            );
            let seen = seen.lock().expect("record").clone().expect("one exchange");
            assert_eq!(
                seen.target, "/anything",
                "exactly the one request was dispatched"
            );
        }

        /// The imposter's own failure status is a *successful* try. Conflating it with the
        /// endpoint's own `502` would leave a console unable to tell "the mock answered 502" from
        /// "the mock could not be reached".
        #[tokio::test]
        async fn an_imposter_error_status_is_a_successful_try() {
            let body = "{\"error\":\"deliberate\"}";
            let (dispatch, _seen) = canned(answer(503, &[], body));

            let outcome = perform_try(dispatch, PORT, &spec("GET", "/boom"), BUDGET, CAP)
                .await
                .expect("the exchange happened, so it succeeded");

            assert_eq!(outcome.status, 503);
            assert_eq!(outcome.body, body);
            assert!(!outcome.truncated);
            assert!(!outcome.body_lossy);
        }

        /// The cap cuts the body and says so. Silence here would be the worst outcome: an
        /// operator comparing a truncated body against what they expected would read the cut as a
        /// mismatch in the mock.
        #[tokio::test]
        async fn an_oversized_response_body_is_truncated_and_flagged() {
            let cap = 1024;
            let body = "x".repeat(cap * 3);
            let (dispatch, _seen) = canned(answer(200, &[], body));

            let outcome = perform_try(dispatch, PORT, &spec("GET", "/big"), BUDGET, cap)
                .await
                .expect("a big body is still an answer");

            assert!(outcome.truncated, "the cut must be declared");
            assert_eq!(
                outcome.body.len(),
                cap,
                "exactly the cap is kept, not the whole body"
            );
        }

        /// A mock may serve bytes that are not text. Reporting them lossily is right — a base64
        /// side-channel would complicate every client for a rare case — but doing so *silently*
        /// would show an operator replacement characters the mock never sent.
        #[tokio::test]
        async fn a_non_utf8_body_is_flagged_lossy() {
            let (dispatch, _seen) = canned(answer(200, &[], vec![0xffu8, 0xfe, 0xfd]));

            let outcome = perform_try(dispatch, PORT, &spec("GET", "/binary"), BUDGET, CAP)
                .await
                .expect("non-text is still an answer");

            assert!(
                outcome.body_lossy,
                "replacement happened and the caller must be told"
            );
            assert!(outcome.body.contains('\u{fffd}'));
        }

        /// A clean body must *not* be flagged, or the flag means nothing.
        #[tokio::test]
        async fn a_clean_body_is_not_flagged_lossy_or_truncated() {
            let body = "plain text";
            let (dispatch, _seen) = canned(answer(200, &[], body));

            let outcome = perform_try(dispatch, PORT, &spec("GET", "/plain"), BUDGET, CAP)
                .await
                .expect("answered");

            assert!(!outcome.body_lossy);
            assert!(!outcome.truncated);
            assert_eq!(outcome.body, body);
        }

        /// The budget bounds the whole exchange. In-process the shape a `wait` behaviour produces
        /// is a dispatch that simply does not come back in time — and that is a timeout (`504`),
        /// never an unreachable imposter (`502`).
        #[tokio::test]
        async fn a_slow_stub_hits_the_budget() {
            let dispatch = |_req: Request<Incoming>| -> Answer {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Some(answer(200, &[], "too late"))
                })
            };

            let failure = perform_try(
                dispatch,
                PORT,
                &spec("GET", "/slow"),
                Duration::from_millis(150),
                CAP,
            )
            .await
            .expect_err("the budget must expire");

            assert!(
                matches!(failure, TryFailure::Timeout),
                "a stall is a timeout (→504), not an unreachable peer (→502): {failure:?}"
            );
        }

        /// The budget does not merely stop *waiting* — it stops the dispatch. The server half runs
        /// the imposter's handler on its own task, and a timeout that only abandoned it would leave
        /// one orphaned task per slow try, holding the engine, unbounded and unlogged.
        #[tokio::test]
        async fn a_timed_out_dispatch_is_stopped_not_orphaned() {
            let finished = Arc::new(AtomicBool::new(false));
            let dispatch = {
                let finished = Arc::clone(&finished);
                move |_req: Request<Incoming>| -> Answer {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        finished.store(true, Ordering::SeqCst);
                        Some(answer(200, &[], "too late"))
                    })
                }
            };

            let failure = perform_try(
                dispatch,
                PORT,
                &spec("GET", "/slow"),
                Duration::from_millis(100),
                CAP,
            )
            .await
            .expect_err("the budget must expire");
            assert!(matches!(failure, TryFailure::Timeout), "{failure:?}");

            // Well past the dispatch's own sleep: had it been left running it would have finished.
            tokio::time::sleep(Duration::from_millis(600)).await;
            assert!(
                !finished.load(Ordering::SeqCst),
                "the dispatch outlived the try that started it"
            );
        }

        /// A dispatch that panics — the imposter's own handler runs there — is the endpoint's
        /// failure, reported and bounded, never a hang: the server half dies, the client half sees
        /// the connection close, and the try answers `Unreachable`.
        #[tokio::test]
        async fn a_panicking_dispatch_is_a_bounded_failure_not_a_hang() {
            let dispatch = |_req: Request<Incoming>| -> Answer {
                Box::pin(async { panic!("the imposter's handler blew up") })
            };

            let failure = tokio::time::timeout(
                Duration::from_secs(2),
                perform_try(dispatch, PORT, &spec("GET", "/boom"), BUDGET, CAP),
            )
            .await
            .expect("answers well inside the budget")
            .expect_err("a panic is not an answer");
            assert!(
                matches!(failure, TryFailure::Unreachable(_)),
                "the exchange ended without a response: {failure:?}"
            );
        }

        /// An imposter this node no longer serves — deleted between the gate and the exchange —
        /// is the endpoint's own failure, not the imposter's: it must not arrive as a `200`
        /// carrying some invented status, nor as the engine's own "no imposter on port" body
        /// dressed up as the mock's answer.
        #[tokio::test]
        async fn an_imposter_that_left_this_node_is_a_failure_not_a_result() {
            let dispatch = |_req: Request<Incoming>| -> Answer { Box::pin(async { None }) };

            let failure = perform_try(dispatch, PORT, &spec("GET", "/nobody-home"), BUDGET, CAP)
                .await
                .expect_err("nothing answered");

            let TryFailure::Unreachable(why) = &failure else {
                panic!("a vanished imposter is unreachable (→502), not {failure:?}");
            };
            assert!(
                why.contains("no longer") || why.contains("not served"),
                "the reason names the imposter leaving, not a transport error: {why}"
            );
        }

        /// The method, path, headers and body reach the imposter as the caller wrote them — the
        /// endpoint is a conduit, and a stub that matches on any of them must see the real thing.
        /// And what arrives is origin-form: a path, never an authority.
        #[tokio::test]
        async fn the_caller_s_own_request_is_what_arrives() {
            let (dispatch, seen) = canned(answer(200, &[], ""));

            let outcome = perform_try(
                dispatch,
                PORT,
                &TryRequest {
                    method: "PATCH".to_owned(),
                    path: "/orders/7?status=open".to_owned(),
                    headers: vec![
                        TryHeader {
                            name: "X-Trace".to_owned(),
                            value: "abc".to_owned(),
                        },
                        // A repeated name is why headers are a list, not a map.
                        TryHeader {
                            name: "X-Trace".to_owned(),
                            value: "def".to_owned(),
                        },
                    ],
                    body: Some("{\"qty\":2}".to_owned()),
                },
                BUDGET,
                CAP,
            )
            .await
            .expect("answered");
            assert_eq!(outcome.status, 200);

            let seen = seen
                .lock()
                .expect("record")
                .clone()
                .expect("the service recorded it");
            assert_eq!(seen.method, "PATCH");
            assert_eq!(
                seen.target, "/orders/7?status=open",
                "target arrives verbatim, query included"
            );
            assert_eq!(
                seen.authority, None,
                "origin-form: no authority can be expressed"
            );
            let traces: Vec<&str> = seen
                .headers
                .iter()
                .filter(|(name, _)| name == "x-trace")
                .map(|(_, value)| value.as_str())
                .collect();
            assert_eq!(
                traces,
                ["abc", "def"],
                "both values of a repeated header survive, in order"
            );
            // HTTP/1.1 needs a Host; the caller sent none, so the imposter's own loopback name is
            // supplied — a stub matching on `host` sees what a real loopback dial would carry.
            let host = seen
                .headers
                .iter()
                .find(|(name, _)| name == "host")
                .map(|(_, value)| value.as_str());
            assert_eq!(host, Some("127.0.0.1:4545"));
            assert_eq!(seen.body, b"{\"qty\":2}");
        }

        /// A caller who sets `Host` themselves keeps it: a stub that matches on a virtual host
        /// must be reachable, and the endpoint has no reason to overrule an explicit header.
        #[tokio::test]
        async fn a_caller_supplied_host_header_is_kept() {
            let (dispatch, seen) = canned(answer(200, &[], ""));

            perform_try(
                dispatch,
                PORT,
                &TryRequest {
                    method: "GET".to_owned(),
                    path: "/".to_owned(),
                    headers: vec![TryHeader {
                        name: "Host".to_owned(),
                        value: "shop.example".to_owned(),
                    }],
                    body: None,
                },
                BUDGET,
                CAP,
            )
            .await
            .expect("answered");

            let seen = seen.lock().expect("record").clone().expect("recorded");
            let hosts: Vec<&str> = seen
                .headers
                .iter()
                .filter(|(name, _)| name == "host")
                .map(|(_, value)| value.as_str())
                .collect();
            assert_eq!(hosts, ["shop.example"], "one Host, the caller's");
        }

        /// Framing is the transport's. A caller-supplied `Content-Length` that disagrees with the
        /// body would be honoured by hyper over the real length and stall the exchange into a
        /// `504` that reads as the imposter's fault; so it, and `Transfer-Encoding`, are dropped
        /// and the body arrives framed from what was actually sent.
        #[tokio::test]
        async fn caller_supplied_framing_headers_are_replaced_by_the_real_ones() {
            let (dispatch, seen) = canned(answer(200, &[], ""));

            perform_try(
                dispatch,
                PORT,
                &TryRequest {
                    method: "POST".to_owned(),
                    path: "/orders".to_owned(),
                    headers: vec![
                        TryHeader {
                            name: "Content-Length".to_owned(),
                            value: "999".to_owned(),
                        },
                        TryHeader {
                            name: "Transfer-Encoding".to_owned(),
                            value: "chunked".to_owned(),
                        },
                    ],
                    body: Some("ab".to_owned()),
                },
                BUDGET,
                CAP,
            )
            .await
            .expect("answered, not stalled");

            let seen = seen.lock().expect("record").clone().expect("recorded");
            assert_eq!(seen.body, b"ab", "the body is what was sent");
            let lengths: Vec<&str> = seen
                .headers
                .iter()
                .filter(|(name, _)| name == "content-length")
                .map(|(_, value)| value.as_str())
                .collect();
            assert_eq!(
                lengths,
                ["2"],
                "framed from the real body, not the caller's claim"
            );
            assert!(
                !seen
                    .headers
                    .iter()
                    .any(|(name, _)| name == "transfer-encoding"),
                "no caller-chosen transfer coding: {:?}",
                seen.headers
            );
        }

        /// An unusable method token is the caller's mistake (`400`), not the imposter's failure
        /// (`502`) — and it must be caught before anything is dispatched.
        #[tokio::test]
        async fn an_invalid_method_token_is_a_bad_request() {
            let failure = perform_try(never(), PORT, &spec("GET SPACE", "/x"), BUDGET, CAP)
                .await
                .expect_err("not a method");

            assert!(matches!(failure, TryFailure::BadRequest(_)), "{failure:?}");
        }

        /// The same for a header the wire cannot carry: a name with a space, a value with a
        /// newline. Refused as the caller's own error, before dispatch — never passed to hyper to
        /// fail as something that reads like the imposter's fault.
        #[tokio::test]
        async fn an_unusable_header_is_a_bad_request() {
            for (name, value) in [("X Bad", "ok"), ("X-Ok", "line\r\nbreak")] {
                let failure = perform_try(
                    never(),
                    PORT,
                    &TryRequest {
                        method: "GET".to_owned(),
                        path: "/".to_owned(),
                        headers: vec![TryHeader {
                            name: name.to_owned(),
                            value: value.to_owned(),
                        }],
                        body: None,
                    },
                    BUDGET,
                    CAP,
                )
                .await
                .expect_err("not a header");
                assert!(
                    matches!(failure, TryFailure::BadRequest(_)),
                    "{name:?}: {value:?} → {failure:?}"
                );
            }
        }

        /// **No path can steer the exchange off the imposter.**
        ///
        /// Since #344 there is no URL to assemble and no host to reach — the dispatch *is* the
        /// imposter — but the path is still the one caller-controlled input that becomes the
        /// request target, and a target carrying an authority would be exactly what a future
        /// change back to a client would need. So every hostile spelling must arrive as a bare
        /// origin-form target with no authority, or be refused; never anything else.
        ///
        /// Each of these is a real technique against naive URL assembly, not a hypothetical:
        /// `//host` is protocol-relative; `@` re-reads what precedes it as userinfo when it lands
        /// before the first slash; `\` is folded to `/` by some parsers; a control character can
        /// attempt request splitting.
        #[tokio::test]
        async fn no_path_can_move_the_exchange_off_the_imposter() {
            for hostile in [
                "//evil.com/",
                "//evil.com:80/x",
                "/\\/evil.com/",
                "/@evil.com/",
                "/..//evil.com",
                "/x?next=http://evil.com",
                "/x#//evil.com",
                "/\r\nHost: evil.com",
                "/\u{0000}",
                "/%2f%2fevil.com",
                "/x\tHTTP/1.1",
                // Absolute-form and the other `PathAndQuery` starts: a request line the peer
                // would reject must be the caller's 400, never a 502 blaming the imposter.
                "http://evil.com/",
                "*",
                "?x",
                "#x",
                "",
            ] {
                let (dispatch, seen) = canned(answer(200, &[], ""));
                match perform_try(dispatch, PORT, &spec("GET", hostile), BUDGET, CAP).await {
                    Ok(_) => {
                        let seen = seen.lock().expect("record").clone().expect("dispatched");
                        assert_eq!(
                            seen.authority, None,
                            "{hostile:?} was accepted but arrived with an authority"
                        );
                        assert!(
                            seen.target.starts_with('/'),
                            "{hostile:?} arrived as {:?}, not origin-form",
                            seen.target
                        );
                        let hosts: Vec<&str> = seen
                            .headers
                            .iter()
                            .filter(|(name, _)| name == "host")
                            .map(|(_, value)| value.as_str())
                            .collect();
                        assert_eq!(
                            hosts,
                            ["127.0.0.1:4545"],
                            "{hostile:?} must not smuggle a second Host"
                        );
                    }
                    // Refusing is equally correct — what must not happen is a *different* target
                    // being reached, or a transport-shaped failure blaming the imposter.
                    Err(TryFailure::BadRequest(_)) => {}
                    Err(other) => panic!("{hostile:?} failed for the wrong reason: {other:?}"),
                }
            }
        }

        /// The ordinary paths an operator actually sends still work — otherwise the check above
        /// could be satisfied by refusing everything.
        #[tokio::test]
        async fn an_ordinary_path_survives_the_origin_check() {
            for good in ["/", "/orders", "/orders/7?status=open&q=a+b", "/a%20b"] {
                let (dispatch, seen) = canned(answer(200, &[], ""));
                perform_try(dispatch, PORT, &spec("GET", good), BUDGET, CAP)
                    .await
                    .unwrap_or_else(|e| panic!("{good:?} must be usable: {e:?}"));
                let seen = seen.lock().expect("record").clone().expect("dispatched");
                assert_eq!(seen.target, good, "arrives verbatim");
            }
        }

        /// Attach the `TcpFaultKind` extension the engine stamps on a carrier response. This is
        /// what `handle_imposter_request` does at all three carrier sites (`_rift.fault.tcp`, a
        /// top-level `fault`, and a v2 script's `reset()`), and it is the signal
        /// `tcp_fault_carrier` reads.
        fn carrier(kind: TcpFaultKind, headers: &[(&str, &[u8])]) -> Response<Full<Bytes>> {
            let mut response = answer(502, headers, "");
            response.extensions_mut().insert(kind);
            response
        }

        /// A stub that injects a connection-level fault answers, in-process, with the engine's
        /// carrier response — a `502` that on the wire is never sent, because the serve loop
        /// aborts the socket instead. Presenting that carrier as "the imposter said 502" would
        /// be a fabricated answer; the try says what the wire would have done. And the `error`
        /// fault — a real `5xx` the wire does send — stays a successful try, so the two must not
        /// be conflated.
        ///
        /// The name reported is the engine's *canonical* one for the kind, not whichever alias
        /// the config author typed: one kind, one name, whether the stub said `garbage` or
        /// `RANDOM_DATA_THEN_CLOSE`.
        #[tokio::test]
        async fn a_connection_fault_carrier_is_an_explicit_transport_outcome() {
            for (kind, canonical) in [
                (TcpFaultKind::Reset, "CONNECTION_RESET_BY_PEER"),
                (TcpFaultKind::Empty, "EMPTY_RESPONSE"),
                (TcpFaultKind::RandomData, "RANDOM_DATA_THEN_CLOSE"),
                (TcpFaultKind::MalformedChunk, "MALFORMED_RESPONSE_CHUNK"),
            ] {
                let (dispatch, _seen) = canned(carrier(kind, &[]));
                let failure = perform_try(dispatch, PORT, &spec("GET", "/reset"), BUDGET, CAP)
                    .await
                    .expect_err("a connection fault is not an answer");
                let TryFailure::Fault(reported) = &failure else {
                    panic!("{canonical}: expected an explicit fault outcome, got {failure:?}");
                };
                assert_eq!(
                    reported, canonical,
                    "the fault is named canonically, not by the stub's alias"
                );
            }

            let (dispatch, _seen) = canned(answer(500, &[("x-rift-fault", b"error")], "injected"));
            let outcome = perform_try(dispatch, PORT, &spec("GET", "/error"), BUDGET, CAP)
                .await
                .expect("an `error` fault is a real response the wire sends");
            assert_eq!(outcome.status, 500);
            assert_eq!(outcome.body, "injected");
        }

        /// The extension is the whole signal, and the `x-rift-fault` header is not consulted at
        /// all. Both halves matter, and each was a real defect under the header-based check this
        /// replaced (upstream #965 / #984):
        ///
        /// - **Extension, no header.** A v2 script's `reset()` built its carrier without the
        ///   header, so the old check missed it and rendered the carrier's fabricated `502` as
        ///   though the imposter had answered it. Now it is the fault it is.
        /// - **Header, no extension.** A stub is free to set `x-rift-fault: reset` on an ordinary
        ///   response the wire really does send. The old check reported that as an aborted
        ///   connection; it is a response, and the try must show it.
        #[tokio::test]
        async fn the_extension_classifies_the_carrier_and_the_header_does_not() {
            let (dispatch, _seen) = canned(carrier(TcpFaultKind::Reset, &[]));
            let failure = perform_try(dispatch, PORT, &spec("GET", "/script-reset"), BUDGET, CAP)
                .await
                .expect_err("a carrier with no header is still a fault");
            assert!(
                matches!(&failure, TryFailure::Fault(name) if name == "CONNECTION_RESET_BY_PEER"),
                "expected a fault named for the extension, got {failure:?}"
            );

            let (dispatch, _seen) = canned(answer(
                200,
                &[("x-rift-fault", b"reset")],
                "a real body the wire sends",
            ));
            let outcome = perform_try(dispatch, PORT, &spec("GET", "/self-inflicted"), BUDGET, CAP)
                .await
                .expect("a header a stub set on a real response does not make it a fault");
            assert_eq!(outcome.status, 200);
            assert_eq!(outcome.body, "a real body the wire sends");
        }

        /// Why the classification lives inside the service rather than next to the response the
        /// exchange returns — the trap a later simplification would fall into.
        ///
        /// `perform_try` runs a genuine HTTP/1.1 round trip over `tokio::io::duplex`: the
        /// dispatch's response is serialized by `serve_connection` and re-parsed by the client
        /// half, which builds a fresh response. `http::Extensions` is process memory and does not
        /// cross a byte stream, so the extension is gone by then — reading it there would
        /// silently classify *every* carrier as an ordinary answer. This pins that the carrier is
        /// still caught even though the response the caller would see no longer carries the mark.
        #[tokio::test]
        async fn the_carrier_is_classified_before_the_exchange_erases_the_extension() {
            // The same carrier, but with a body and status that would be plainly visible if the
            // classification had been missed and the carrier rendered as an answer.
            let mut response = answer(502, &[], "carrier body that must never be shown");
            response.extensions_mut().insert(TcpFaultKind::Empty);
            let (dispatch, _seen) = canned(response);

            let failure = perform_try(dispatch, PORT, &spec("GET", "/empty"), BUDGET, CAP)
                .await
                .expect_err("the carrier must not survive as an answer");
            assert!(
                matches!(&failure, TryFailure::Fault(name) if name == "EMPTY_RESPONSE"),
                "expected EMPTY_RESPONSE, got {failure:?}"
            );
        }

        /// A caller cannot smuggle in addressing of its own. The envelope is closed, so a `host`,
        /// `scheme` or `url` field is a `400` rather than something silently ignored — the
        /// silent version would let a client believe it had aimed the request somewhere it had
        /// not.
        #[test]
        fn the_envelope_refuses_any_addressing_field() {
            for smuggled in [
                r#"{"method":"GET","path":"/x","scheme":"https"}"#,
                r#"{"method":"GET","path":"/x","host":"example.com"}"#,
                r#"{"method":"GET","path":"/x","url":"http://example.com/"}"#,
                r#"{"method":"GET","path":"/x","port":9999}"#,
            ] {
                assert!(
                    serde_json::from_str::<TryRequest>(smuggled).is_err(),
                    "{smuggled} must be refused, not quietly ignored"
                );
            }
            assert!(
                serde_json::from_str::<TryRequest>(r#"{"method":"GET","path":"/x"}"#).is_ok(),
                "the legal minimum still parses"
            );
        }

        /// The optional flags are absent when false, so `bodyLossy`/`headersLossy`/`truncated`
        /// mean something when a client sees them at all.
        #[test]
        fn the_optional_flags_are_omitted_when_false() {
            let clean = TryResponse {
                status: 200,
                headers: Vec::new(),
                headers_lossy: false,
                body: "ok".to_owned(),
                body_lossy: false,
                truncated: false,
                elapsed_ms: 3,
            };
            let rendered = serde_json::to_string(&clean).expect("renders");
            assert!(!rendered.contains("bodyLossy"), "{rendered}");
            assert!(!rendered.contains("headersLossy"), "{rendered}");
            assert!(!rendered.contains("truncated"), "{rendered}");
            assert!(rendered.contains("\"elapsedMs\":3"), "{rendered}");

            let flagged = TryResponse {
                body_lossy: true,
                headers_lossy: true,
                truncated: true,
                ..clean
            };
            let rendered = serde_json::to_string(&flagged).expect("renders");
            assert!(rendered.contains("\"bodyLossy\":true"), "{rendered}");
            assert!(rendered.contains("\"headersLossy\":true"), "{rendered}");
            assert!(rendered.contains("\"truncated\":true"), "{rendered}");
        }

        /// **The status mapping**, which nothing else in this file gates.
        ///
        /// Every other try test stops at the `TryFailure` variant. That leaves the translation
        /// from variant to HTTP status — the last step before an operator sees anything — proven
        /// only by reading it. Swapping `GATEWAY_TIMEOUT` and `BAD_GATEWAY` compiles and keeps all
        /// of them green, while the console starts reporting a slow mock as an unreachable one.
        ///
        /// The `200` arm is the design's central claim and is asserted here rather than assumed:
        /// the imposter's own `503` is a *successful* try, so the endpoint answers `200` and the
        /// `503` rides in the payload.
        #[test]
        fn every_outcome_maps_to_the_status_the_contract_publishes() {
            let answered = TryResponse {
                status: 503,
                headers: Vec::new(),
                headers_lossy: false,
                body: "mock said no".to_owned(),
                body_lossy: false,
                truncated: false,
                elapsed_ms: 4,
            };
            let ok = render_try_outcome(Ok(answered), 4545);
            assert_eq!(
                ok.status(),
                StatusCode::OK,
                "the imposter's own 5xx is a successful try — the endpoint answers 200"
            );

            assert_eq!(
                render_try_outcome(Err(TryFailure::Timeout), 4545).status(),
                StatusCode::GATEWAY_TIMEOUT,
                "a budget expiry is 504, never 502"
            );
            assert_eq!(
                render_try_outcome(Err(TryFailure::Unreachable("refused".into())), 4545).status(),
                StatusCode::BAD_GATEWAY,
                "a failed exchange is 502, never 504"
            );
            assert_eq!(
                render_try_outcome(Err(TryFailure::BadRequest("nope".into())), 4545).status(),
                StatusCode::BAD_REQUEST,
                "the caller's own malformed envelope is 400 — not the imposter's fault"
            );
            let fault = render_try_outcome(
                Err(TryFailure::Fault("CONNECTION_RESET_BY_PEER".into())),
                4545,
            );
            assert_eq!(
                fault.status(),
                StatusCode::BAD_GATEWAY,
                "a connection fault the stub injects is 502: on the wire there is no response"
            );
        }

        /// A body that is *exactly* the cap dropped nothing, so it must not be flagged.
        ///
        /// This was a real off-by-one (`>=` where `>` belonged): a complete body reported as cut
        /// is the same class of harm as a cut one reported as complete — it sends an operator
        /// hunting for content that was never missing.
        #[tokio::test]
        async fn a_body_exactly_at_the_cap_is_complete_not_truncated() {
            let cap = 512;
            let body = "y".repeat(cap);
            let (dispatch, _seen) = canned(answer(200, &[], body));

            let outcome = perform_try(dispatch, PORT, &spec("GET", "/exact"), BUDGET, cap)
                .await
                .expect("answered");

            assert_eq!(outcome.body.len(), cap);
            assert!(
                !outcome.truncated,
                "nothing was dropped, so nothing may be declared dropped"
            );
        }

        /// Header values get the same honesty the body does.
        ///
        /// A mock that injects a malformed header is doing so deliberately — that is what fault
        /// injection is for — and the console renders these values verbatim, so an unflagged
        /// substitution shows the operator characters the mock never sent.
        #[tokio::test]
        async fn a_non_utf8_header_value_is_flagged_lossy() {
            let (dispatch, _seen) = canned(answer(200, &[("x-sig", &[0xff, 0xfe])], ""));

            let outcome = perform_try(dispatch, PORT, &spec("GET", "/hdr"), BUDGET, CAP)
                .await
                .expect("answered");

            assert!(outcome.headers_lossy, "the substitution must be declared");
            let sig = outcome
                .headers
                .iter()
                .find(|h| h.name == "x-sig")
                .expect("the header is reported, not dropped");
            assert!(sig.value.contains('\u{fffd}'));
            assert!(
                !outcome.body_lossy,
                "the body was clean — the two flags must not be conflated"
            );
        }

        /// And clean headers are not flagged, or the flag says nothing.
        #[tokio::test]
        async fn clean_headers_are_not_flagged_lossy() {
            let (dispatch, _seen) = canned(answer(200, &[("x-sig", b"abc")], ""));

            let outcome = perform_try(dispatch, PORT, &spec("GET", "/hdr"), BUDGET, CAP)
                .await
                .expect("answered");

            assert!(!outcome.headers_lossy);
        }

        /// The published budget and cap are the ones the handler actually uses. Both are quoted
        /// verbatim in the OpenAPI description a client reads, so drift here is drift in a
        /// contract, not a constant.
        #[test]
        fn the_published_limits_are_the_enforced_ones() {
            assert_eq!(TRY_BUDGET, Duration::from_secs(10));
            assert_eq!(TRY_MAX_RESPONSE_BYTES, 1024 * 1024);
        }
    }
}
