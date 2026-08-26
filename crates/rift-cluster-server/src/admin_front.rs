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
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use http_body_util::{BodyExt, Full, Limited, channel::Channel, combinators::BoxBody};
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
    HEADER_BIND_FAILURES, HEADER_NEXT_INDEX, HEADER_OP_ID, HEADER_PARTIAL, HEADER_REVISION,
    HEADER_TRUNCATED, HEADER_WARNINGS,
};
use rift_cluster::stores::{
    ContextScope, Coverage, FleetCursor, FleetTailEvent, FlowConfig, FlowNet, IdPolicy, JoinMode,
    JournalCursor, JournalNet, ResolvedKnobs,
};
use rift_cluster::{
    ControlOutcome, ControlResponse, FLEET_SCOPE, KeyClass, NodeError, NodeId, OwnedKey, PullError,
    RaftNode, SESSION_KEY_BYTES, SessionKey, SourcePuller, TenantId, routes_installed_for,
};
use rift_cluster_base::seams::{
    ErrorKind, ImposterConfig, RecordedRequest, RiftScriptConfig, RouteTable, SCOPE_HEADER,
    ScriptBaseDir, Stub, classify as classify_upstream, config_uses_script_surface,
    error_response_typed, resolve_scripts, resolve_stub_scripts, validate_stub, validate_stubs,
};
// The compiler crate (RFC-004 S2, issue #278): the front compiles specs on the accepting node
// (PUT, deploy, edit-time warnings). `serde_json::Value` stays fully-qualified below, matching
// this file's existing convention (no bare `use serde_json::Value`).
use rift_cluster_spec::{
    CompileOptions, MAX_SPEC_BYTES, STUB_ID_PREFIX, SpecDigest, compile, validate_stub_response,
};
use serde::{Deserialize, Serialize};
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
use crate::route_hits::{self, RouteHitCounter, RouteHits};
use crate::session;
use crate::tenancy;

/// Largest admin request body the front accepts on a terminated route. The
/// proxied path streams and is not subject to this.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// How long a terminated write may take to commit (forwarding included) before
/// the client gets the `timeout` error shape. Distinct from the barrier
/// timeout, which begins after the commit and degrades to a warning.
const WRITE_DEADLINE: Duration = Duration::from_secs(10);

/// Edit-time spec validation, RFC-004 §3.2 / issue #278 — see [`spec_warnings`]. Its own header
/// rather than a ride on [`HEADER_WARNINGS`]: that one already means "the fleet write itself is
/// degraded" (an unreached node, a local engine failure), and this means something unrelated — a
/// *successful* write whose static body disagrees with the spec that deployed it.
const HEADER_SPEC_WARNINGS: &str = "rift-spec-warnings";

/// Total budget for a merge-on-read fan-out to every other roster peer (issue #223): the merged
/// journal read, its `numberOfRequests` decoration, and the transitional `DELETE savedRequests`
/// fan-out all share it. Bounded so one slow or unreachable peer degrades an answer to
/// `Rift-Cluster-Partial: true` rather than hanging the client on it.
const JOURNAL_PEER_BUDGET: Duration = Duration::from_secs(2);

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
    /// This node's source puller, for the `nodeLocal` half of
    /// `GET /admin/sources` (issue #239) — the last poll error per source is
    /// deliberately node-local state, reachable only through it.
    pub puller: Arc<SourcePuller>,
    /// This node's per-route dispatch counts (issue #368). Read directly rather than over the
    /// loopback cluster port: the same reason `/_fleet/members` reads its own bind fields off the
    /// body it already built — a node asking itself over the wire can observe a different moment
    /// than the one it is reporting.
    pub route_hits: Arc<RouteHitCounter>,
    /// Whether this node binds a front-door listener (issue #403), resolved once in `compose` and
    /// shared with the cluster port so the two cannot disagree. Counts alone cannot tell "no
    /// request reached this route" from "nothing in this fleet could have dispatched one".
    pub front_door_bound: bool,
    /// This node's view of the fleet request journal (issue #223): the merged read, the
    /// `numberOfRequests` decoration, and the transitional `DELETE savedRequests` fan-out all
    /// reach the fleet through it.
    pub journal_net: Arc<JournalNet>,
    /// The flow-state subsystem (issue #372): `GET /admin/tenants` and
    /// `GET /admin/tenants/:id` reach it through `tenancy::dispatch` to fan out
    /// `numberOfFlowEntries`.
    pub flow_net: Arc<FlowNet>,
    /// How many imposters one fleet journal answer may cover (issue #362).
    ///
    /// A cap exists because the resumption token carries a row per covered port and rides a
    /// `Last-Event-ID` header, and because the walk's cost is linear in the ports it touches. What
    /// it must never be is *silent*: whatever this leaves out is named in the answer's `coverage`
    /// block, which is the whole of the issue's third acceptance criterion.
    pub fleet_journal_port_cap: usize,
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
    /// See [`FrontConfig::puller`].
    puller: Arc<SourcePuller>,
    /// See [`FrontConfig::route_hits`].
    route_hits: Arc<RouteHitCounter>,
    /// See [`FrontConfig::front_door_bound`].
    front_door_bound: bool,
    /// See [`FrontConfig::journal_net`].
    journal_net: Arc<JournalNet>,
    /// See [`FrontConfig::flow_net`].
    flow_net: Arc<FlowNet>,
    /// See [`FrontConfig::fleet_journal_port_cap`].
    fleet_journal_port_cap: usize,
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
        puller: config.puller,
        route_hits: config.route_hits,
        front_door_bound: config.front_door_bound,
        journal_net: config.journal_net,
        flow_net: config.flow_net,
        fleet_journal_port_cap: config.fleet_journal_port_cap,
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
    /// `GET /imposters/{port}/requests|savedRequests` with no `?match=` (issues #223, #225): a
    /// fleet-wide merge-on-read rather than a proxy, **including** the `?since=` form as of #225 —
    /// the vector cursor is what made a merged cursor read expressible at all, so the front door
    /// now terminates every requests-read that is not predicate-scoped.
    ReadSavedRequests(u16),
    /// `GET /imposters/{port}/savedRequests/stream` with no `?match=` (issue #348): the live
    /// sibling of [`Self::ReadSavedRequests`] — a merged, fleet-wide SSE tail instead of the
    /// per-node proxy this path used to get.
    ///
    /// Only the canonical spelling, mirroring upstream's `stream_target`, which recognises
    /// exactly this one. The `/admin/imposters/` alias above exists because *upstream itself*
    /// serves the requests read under both spellings; it does not serve the stream under both, so
    /// terminating a second spelling here would invent a route that answers 404 when proxied.
    ///
    /// `?match=` still proxies, for [`terminated_saved_requests`]'s reason verbatim (issue #223
    /// review, B1): the merge path evaluates no predicates, so terminating a predicate-scoped
    /// stream would answer with the whole fleet's requests instead of the caller's subset.
    ///
    /// `GET /events` is deliberately **not** here. It stays proxied per-node and FleetAdmin-gated
    /// because its payload is tenant-unfiltered (`principal.rs`, issue #163 owns the filtering) —
    /// the asymmetry is documented in Ch.7 and `docs/rift-cluster-server.md`.
    StreamSavedRequests(u16),
    /// `GET /admin/requests` (issue #362): the tenant's whole request journal in one merged,
    /// cursor-exact read — [`Self::ReadSavedRequests`] across every imposter the caller's tenant
    /// owns instead of one.
    ///
    /// Terminates because there is nothing to proxy to: upstream has no fleet surface, and a
    /// per-node one could not answer for the fleet anyway. EE-only, same posture as
    /// [`Self::SourceList`] and the tenancy surface.
    ///
    /// **Scope is the tenant, not the fleet**, which is why this needs no FleetAdmin gate the way
    /// `GET /events` does: the port set comes from `tenant_owned_ports` on this node's applied
    /// state, so a caller can only ever address imposters their own tenant owns. `/events` stays
    /// gated precisely because its payload is *not* tenant-filtered; this one is, by construction.
    ///
    /// No `?match=`: the merge path evaluates no predicates, so a predicate-scoped fleet read would
    /// answer with the whole tenant's requests instead of the caller's subset — issue #223 B1's
    /// reason, unchanged. Predicate-scoped reads stay per-imposter and proxied.
    ReadFleetRequests,
    /// `GET /admin/requests/stream` (issue #362): the live sibling of [`Self::ReadFleetRequests`],
    /// and [`Self::StreamSavedRequests`]'s fleet-wide counterpart.
    StreamFleetRequests,
    /// `DELETE` on the same two paths. Two different designs live under this one variant now,
    /// selected in `terminate` by whether `?match=` narrowed the request (issue #223 item 4's
    /// original design decision, B3, still holds for the scoped form):
    ///
    /// - **Unscoped** (no `?match=`, issue #224): a Raft-committed `ControlOp::JournalClearGen`
    ///   with `space: None` — see `build_mutation`. This replaced the pre-#224 fan-out over the
    ///   cluster RPC port entirely; that mechanism (`JournalNet::clear_peers`) no longer exists.
    /// - **Scoped** (`?match=` present): unchanged from #223 — a local-only proxy to the local
    ///   engine, always stamped `Rift-Cluster-Partial: true`, never a Raft write. See
    ///   `terminate_clear_saved_requests`'s own doc for why this half was deliberately left alone.
    ClearSavedRequests(u16),
    /// `DELETE /imposters/{port}/savedProxyResponses` (issue #226): a Raft-committed
    /// `ControlOp::ProxyRecordedClear` — the proxy-recording sibling of the unscoped
    /// `ClearSavedRequests` commit above, for the same reason: pre-#226 this proxied to
    /// one node's in-process store, which cleared nothing the fleet's claim table holds.
    /// Recorded *stubs* stay, deliberately — they are imposter config, deleted through
    /// the stub-edit surfaces; this clears the exactly-once markers so signatures record
    /// afresh. GET on the same path stays proxied: the listing is upstream's own surface.
    ClearSavedProxyResponses(u16),
    /// `DELETE /imposters/{port}/spaces/{flow}` (issue #224): a space teardown has two
    /// independent halves. The *flow-state* half — already clustered via `ClusteredFlowStore` —
    /// stays exactly what it was: proxied to the local engine, untouched by this issue. The
    /// *journal* half is what #224 adds: after a successful proxied teardown,
    /// `terminate_space_teardown` additionally commits `ControlOp::JournalClearGen { space:
    /// Some(flow), .. }` through Raft, the space-scoped sibling of the unscoped
    /// `ClearSavedRequests` commit above. Not routed through `build_mutation` — there is no
    /// loopback path to `FetchAfter`/`Captured` render from; the response is the proxy's own.
    SpaceTeardown(u16, String),
    /// `GET /imposters/{port}/spaces` (issue #374): every correlated-isolation space this imposter
    /// currently holds live flow-KV entries under, fleet-wide, with each row's live entry count
    /// and owning node plus the imposter's resolved `durability` on the envelope.
    ///
    /// Terminates — there is nothing to proxy to. Unlike [`Self::SpaceTeardown`], which proxies its
    /// flow-state half and adds a journal commit alongside it, upstream's router has no bare
    /// `["spaces"]` shape at all: only the two-segment single-space read and the three-segment
    /// stubs write exist there. This is a merge-on-read fan-out over `FlowNet::fleet_spaces`, the
    /// same shape [`Self::ReadFleetRequests`] and the `numberOfFlowEntries` decoration already use
    /// for a fleet-scoped read that has no single node's answer to trust.
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
    /// - the port must be one of the caller's own tenant's imposters, which
    ///   [`addressed_port`] delegates to the ownership gate in [`authorize_action`] — so an
    ///   unknown port and another tenant's port answer the identical RFC-002 §8.4 `404` and this
    ///   cannot be used to map which ports exist;
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
    /// RFC-002 §5's tenancy admin surface (issue #162). Every one of these
    /// **terminates**, reads included — there is no upstream `/admin/tenants`
    /// to proxy to, exactly as with `GET /front-door/routes`.
    Tenancy(tenancy::Route),
    /// `GET /admin/sources` (issue #239): the fleet's source declarations for
    /// the tenant in view. Terminates — the upstream admin has no source
    /// surface; the cluster-port `/admin/sources` (issue #134) is a different
    /// listener with a different trust model.
    SourceList,
    /// `GET /admin/sources/{id}` — one source record.
    SourceRead(String),
    /// `POST /admin/sources` (issue #253): declare (upsert by id) a source
    /// under the caller's tenant. The cluster port already serves this verb,
    /// default-tenant and under the cluster credential (issue #134); this
    /// promotes it to the RBAC'd front, tenant-resolved from `X-Rift-Tenant`,
    /// through the very same [`rift_cluster::SourcePuller::put`] the cluster
    /// port now delegates to — so the two fronts cannot answer differently
    /// for the same declaration.
    SourcePut,
    /// `DELETE /admin/sources/{id}` (issue #253). "Stop tracking this URI":
    /// the source row is removed, but the imposters it created stay bound —
    /// only their provenance is cleared. Orphaned, never torn down; see
    /// [`rift_cluster::SourcePuller::delete`]'s doc.
    SourceDelete(String),
    /// `POST /admin/sources/{id}/pull` (issue #253): fetch the source now and
    /// apply what it produced, under the caller's tenant rather than the
    /// cluster port's fixed default.
    SourcePull(String),
    /// `GET /specs` (RFC-004 §4.3, issue #278): the tenant's imported specs, id-ascending, from
    /// local applied state. Terminates for the same reason the source surface does — there is no
    /// upstream `/specs` to proxy to, this is an EE-only projection.
    SpecList,
    /// `GET /specs/{id}` — one spec's record plus the document exactly as imported.
    SpecRead(String),
    /// `PUT /specs/{id}` — import (or re-import) an OpenAPI 3.0 document under this id. Compiled on
    /// the accepting node before anything commits; an unchanged re-import writes no log entry.
    SpecPut(String),
    /// `DELETE /specs/{id}`. Refuses while bound to a port unless `force` — see `classify`'s
    /// `?force` handling, the same shape as `SourceDelete`'s tenant-scoped pattern.
    SpecDelete {
        id: String,
        force: bool,
    },
    /// `POST /specs/{id}/compile` — a dry run: compile the stored document and diff it against
    /// what is deployed. Commits nothing, so it is authorized as a read (`Action::SpecRead`)
    /// despite the verb.
    SpecCompile(String),
    /// `POST /specs/{id}/deploy` — compile the stored document for a port and commit it as that
    /// port's imposter, stamping the spec's provenance in the same barrier. Requires
    /// `Action::SpecWrite` **and** `Action::ImposterWrite` on the target port (the latter checked
    /// in `terminate_spec_deploy`, not here — see `Action::SpecWrite`'s doc).
    SpecDeploy(String),
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
        | Terminated::SetEnabled(port, _)
        | Terminated::ReadSavedRequests(port)
        | Terminated::StreamSavedRequests(port)
        | Terminated::ClearSavedRequests(port)
        | Terminated::ClearSavedProxyResponses(port)
        | Terminated::SpaceTeardown(port, _)
        | Terminated::SpacesList(port)
        // Issue #335: this is not merely *a* tenant check for the try endpoint, it is the **only**
        // one. Returning the port here is what makes an unknown port and another tenant's port
        // answer the same §8.4 404 — and what stops the endpoint dialling a port the caller does
        // not own. A handler-local re-check would be a second copy of this rule, free to drift.
        | Terminated::TryImposter(port) => Some(*port),
        Terminated::Create
        | Terminated::ReplaceAllImposters
        | Terminated::DeleteAllImposters
        | Terminated::PutRoutes
        | Terminated::DeleteRoute(_)
        | Terminated::Tenancy(_)
        | Terminated::SourceList
        | Terminated::SourceRead(_)
        // A source names no imposter port of its own — the ports it owns are
        // a *consequence* of a pull, not the address a write is made to. A
        // cross-tenant source id is refused by tenant scoping alone (the
        // source table is keyed `(tenant, id)`), the same way a cross-tenant
        // tenancy-surface record is: there is no separate port to check
        // ownership of the way an imposter write has one.
        | Terminated::SourcePut
        | Terminated::SourceDelete(_)
        // The fleet journal addresses *every* port the tenant owns, so there is no single port to
        // check ownership of (issue #362). Ownership is not skipped, it is inherent: the handler
        // derives its port set from `tenant_owned_ports`, so a port the caller's tenant does not
        // own is never in the walk to begin with — the same guarantee this gate gives a
        // single-port route, established by construction instead of by check.
        | Terminated::ReadFleetRequests
        | Terminated::StreamFleetRequests
        | Terminated::SourcePull(_)
        // A spec names no imposter port of its own, for the same reason a source does not: the
        // ports it addresses (`SpecRecord::ports`) are a *consequence* of deploying it, not the
        // address a write is made to. `SpecDeploy`'s port lives in the body and is checked against
        // `Action::ImposterWrite` inside `terminate_spec_deploy` itself, not through this gate.
        | Terminated::SpecList
        | Terminated::SpecRead(_)
        | Terminated::SpecPut(_)
        | Terminated::SpecDelete { .. }
        | Terminated::SpecCompile(_)
        | Terminated::SpecDeploy(_) => None,
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
        | Terminated::ReadSavedRequests(_)
        | Terminated::StreamSavedRequests(_)
        | Terminated::ClearSavedRequests(_)
        | Terminated::ClearSavedProxyResponses(_)
        | Terminated::SpaceTeardown(_, _)
        | Terminated::SpacesList(_)
        | Terminated::TryImposter(_)
        | Terminated::PutRoutes
        | Terminated::DeleteRoute(_)
        // Resource routes: the id names a record *within* the caller's
        // tenant, so `X-Rift-Tenant` is the subject — unlike the tenancy
        // surface, where the tenant is the path segment being administered.
        | Terminated::SourceList
        | Terminated::SourceRead(_)
        | Terminated::SourcePut
        | Terminated::SourceDelete(_)
        | Terminated::ReadFleetRequests
        | Terminated::StreamFleetRequests
        | Terminated::SourcePull(_)
        // A spec's id names a record within the caller's tenant, exactly like a source's — the
        // header is the subject, not a path segment.
        | Terminated::SpecList
        | Terminated::SpecRead(_)
        | Terminated::SpecPut(_)
        | Terminated::SpecDelete { .. }
        | Terminated::SpecCompile(_)
        | Terminated::SpecDeploy(_) => None,
    }
}

/// `GET|DELETE .../requests|savedRequests`, once `port` is already known — shared by the
/// canonical `/imposters/{port}/...` match arm and the `/admin/imposters/{port}/...` alias in
/// [`classify`], so the two spellings cannot silently drift apart the way they did before issue
/// #223's review (Important: the alias proxied local-only while the canonical path terminated).
fn terminated_saved_requests(
    method: &Method,
    query: Option<&str>,
    port: u16,
) -> Option<Terminated> {
    match *method {
        // `?since=` now terminates too (issue #225): the vector cursor replaced the scalar one
        // #223 could not honour, so a `since` read is a merged read like any other and the
        // engine's `parse_since` must never see a vector token. That carve-out is deleted here.
        //
        // `?match=` still falls through to the proxy (issue #223 review, B1), and for a reason
        // #225 does not touch: it is a predicate the merge-on-read path never evaluates at all,
        // so terminating a `?match=`-scoped request would silently answer with the *whole*
        // fleet's requests instead of the caller's scoped subset, and would turn a malformed
        // filter's upstream `400` into a `200` with everything. Proxying leaves upstream's own
        // clause parser and its existing error handling in charge of it.
        Method::GET if !has_query_param(query, "match") => {
            Some(Terminated::ReadSavedRequests(port))
        }
        Method::DELETE => Some(Terminated::ClearSavedRequests(port)),
        _ => None,
    }
}

pub(crate) fn classify(method: &Method, path: &str, query: Option<&str>) -> Option<Terminated> {
    // The tenancy surface first: it is EE-only and terminates in full, so it
    // must never fall through to the imposter classifiers or the proxy.
    if let Some(route) = tenancy::classify(method, path, query) {
        return Some(Terminated::Tenancy(route));
    }
    // The source inspection surface (issue #239): EE-only and terminating for
    // the same reason as tenancy. Reads only — a recognized path with another
    // method falls through to the proxy and answers upstream's own 404/405,
    // exactly as tenancy::classify does for its half-matches.
    if path == "/admin/sources" {
        return match *method {
            Method::GET => Some(Terminated::SourceList),
            // `POST /admin/sources` (issue #253): declare (upsert by id) a
            // source. There is no separate "create vs replace" distinction
            // the way imposters have one — `SourcePut` is an upsert either
            // way — so one variant covers both.
            Method::POST => Some(Terminated::SourcePut),
            _ => None,
        };
    }
    // The fleet request journal (issue #362), EE-only and terminating for tenancy's reason. Matched
    // before the `/admin/imposters/` and `/imposters/` prefixes below because it is not
    // port-addressed at all — it is the tenant's whole journal, and there is no port segment to
    // parse. A recognized path with another method falls through to the proxy and answers
    // upstream's own 404/405, exactly as `/admin/sources` does for its half-matches.
    if path == "/admin/requests" {
        return match *method {
            // `?match=` deliberately does not terminate here: see `Terminated::ReadFleetRequests`.
            // Unlike the per-imposter path there is no sensible proxy fallback for a fleet-scoped
            // predicate read, so it is simply not offered — a `?match=` read stays a per-imposter
            // request, which is the surface that can actually honour it.
            Method::GET if !has_query_param(query, "match") => Some(Terminated::ReadFleetRequests),
            _ => None,
        };
    }
    if path == "/admin/requests/stream" {
        return match *method {
            Method::GET if !has_query_param(query, "match") => {
                Some(Terminated::StreamFleetRequests)
            }
            _ => None,
        };
    }
    if let Some(id) = path.strip_prefix("/admin/sources/") {
        // Percent-decoding is deliberately not done, for tenancy::classify's
        // reason: a source id is validated printable-ASCII without `/`, so
        // nothing legal needs escaping and `%2f` must not smuggle a segment.
        return match *method {
            Method::GET if !id.is_empty() && !id.contains('/') => {
                Some(Terminated::SourceRead(id.to_owned()))
            }
            Method::DELETE if !id.is_empty() && !id.contains('/') => {
                Some(Terminated::SourceDelete(id.to_owned()))
            }
            // `POST /admin/sources/{id}/pull`: `/pull` is a suffix on the id
            // path rather than its own path segment, so an id that itself
            // ends in `/pull` cannot be confused with the verb — stripping
            // the suffix and then re-running the same empty/`/`-free check
            // the other two arms use closes exactly that. Mirrors the
            // cluster port's own `pull_source` (`sources/mod.rs`), which
            // parses the identical shape for the identical reason.
            Method::POST => match id.strip_suffix("/pull") {
                Some(inner) if !inner.is_empty() && !inner.contains('/') => {
                    Some(Terminated::SourcePull(inner.to_owned()))
                }
                _ => None,
            },
            _ => None,
        };
    }
    // The specs surface (RFC-004 §4.3, issue #278): EE-only and terminating for the same reason
    // sources and tenancy are. `/specs` itself only recognises `GET` — every other method falls
    // through to `None` (and, for an unrecognized path shape, eventually the proxy) rather than
    // being force-fit onto a variant here.
    if path == "/specs" {
        return match *method {
            Method::GET => Some(Terminated::SpecList),
            _ => None,
        };
    }
    if let Some(id) = path.strip_prefix("/specs/") {
        // Undecoded, no `/`, non-empty — `is_source_name`'s shape rule (`control::validate`
        // enforces the charset/length at commit time), and the same reasoning as the source id
        // parse just above: nothing legal needs percent-decoding, and `%2f` must not smuggle a
        // segment.
        return match *method {
            Method::GET if !id.is_empty() && !id.contains('/') => {
                Some(Terminated::SpecRead(id.to_owned()))
            }
            Method::PUT if !id.is_empty() && !id.contains('/') => {
                Some(Terminated::SpecPut(id.to_owned()))
            }
            Method::DELETE if !id.is_empty() && !id.contains('/') => Some(Terminated::SpecDelete {
                id: id.to_owned(),
                force: query_flag(query, "force"),
            }),
            // `/compile` and `/deploy` are suffixes on the id path, exactly like `/pull` on a
            // source id above — an id that itself ends in one of these cannot be confused with the
            // verb, because stripping the suffix and re-running the same empty/`/`-free check
            // closes exactly that.
            Method::POST => match id.strip_suffix("/compile") {
                Some(inner) if !inner.is_empty() && !inner.contains('/') => {
                    Some(Terminated::SpecCompile(inner.to_owned()))
                }
                _ => match id.strip_suffix("/deploy") {
                    Some(inner) if !inner.is_empty() && !inner.contains('/') => {
                        Some(Terminated::SpecDeploy(inner.to_owned()))
                    }
                    _ => None,
                },
            },
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
    // The savedRequests alias under `/admin/imposters/` (issue #223 review, Important):
    // upstream's own `/admin/imposters/` prefix is otherwise reserved for flow-state inspection
    // (`classify_admin_flow_state` in the vendored authorizer), but this front's own
    // merge-on-read read and clear fan-out terminate the identical two verbs the canonical
    // `/imposters/{port}/...` spelling does. `is_imposter_listing` already treats both spellings
    // of the *listing* alike; without this, the two spellings disagreed about the very same
    // imposter's requests — the canonical path terminated (fleet-merged, honestly partial), the
    // alias silently proxied local-only with yesterday's `x-rift-next-index` and no partial
    // header at all.
    if let Some(rest) = path.strip_prefix("/admin/imposters/") {
        let segments: Vec<&str> = rest.split('/').collect();
        if let [port_str, "requests" | "savedRequests"] = segments.as_slice() {
            let port: u16 = port_str.parse().ok()?;
            return terminated_saved_requests(method, query, port);
        }
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
        // Both spellings are one handler upstream (`router.rs` maps `["requests"]` and
        // `["savedRequests"]` identically), so they classify identically here too (issue #223).
        [_, "requests" | "savedRequests"] => terminated_saved_requests(method, query, port),
        // The live tail (issue #348). Exactly upstream's `stream_target` shape — `savedRequests`
        // only, never `requests`, because that is the one spelling upstream's own classifier
        // recognises. Anything else on this path (a non-GET, or a `?match=`-scoped tail) falls
        // through to the proxy and keeps behaving precisely as it does today.
        [_, "savedRequests", "stream"]
            if *method == Method::GET && !has_query_param(query, "match") =>
        {
            Some(Terminated::StreamSavedRequests(port))
        }
        // Only the DELETE terminates (issue #226): the clear must purge the fleet's
        // replicated claim markers, which no proxied engine call can reach. GET stays
        // proxied — the recorded-responses listing is upstream's own surface.
        [_, "savedProxyResponses"] if *method == Method::DELETE => {
            Some(Terminated::ClearSavedProxyResponses(port))
        }
        // `DELETE /imposters/{port}/spaces/{flow}` (issue #224): exactly the two-segment shape
        // upstream's own router matches for `ImposterRoute::Space` (`["spaces", flow_id]` —
        // `SpaceStubs`'s three-segment `["spaces", flow_id, "stubs"]` is a different route and a
        // write, not a delete, so it falls through here untouched). Every other method on this
        // shape stays proxied exactly as before — only the delete gets a journal half to commit.
        [_, "spaces", flow] if *method == Method::DELETE && !flow.is_empty() => {
            Some(Terminated::SpaceTeardown(port, (*flow).to_owned()))
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

/// Whether `query` names `name` at all — a bare `name` with no `=value` still counts, because it
/// is the *presence* of the parameter that matters at this classifier (not what it says): with
/// either `since` or `match` present, the merge-on-read route falls through to the proxy instead
/// of terminating (issue #223 review, B1 — generalised from the `since`-only `has_since_param`
/// this replaces).
///
/// Raw and case-sensitive, with **no** percent-decoding — deliberately mirroring upstream's own
/// `query_pairs` key semantics, so this classifier and the proxy target it falls through to agree
/// on what counts as "the parameter is present" for the identical query string.
fn has_query_param(query: Option<&str>, name: &str) -> bool {
    query
        .unwrap_or_default()
        .split('&')
        .any(|pair| pair == name || pair.split_once('=').is_some_and(|(key, _)| key == name))
}

/// The **raw** value of a query parameter, or `None` when it is absent — [`has_query_param`]'s
/// matching rules, with the value handed back instead of discarded.
///
/// No percent-decoding, for the same reason that function does none: this and the proxy it can
/// fall through to must agree byte-for-byte about what the query string says. Nothing this reads
/// ever needs escaping — a cursor token is unpadded base64url (`[A-Za-z0-9_-]`) and the legacy
/// form it also accepts is decimal digits, neither of which a conforming client encodes. A token
/// that arrives encoded anyway is, correctly, not a token this node issued, and is refused as one
/// rather than silently repaired into a position nobody asked for.
///
/// A valueless `?since` (no `=`) yields `Some("")`, not `None`: for a cursor those are different
/// requests — "this token is empty" is a client bug worth a 400, while absence means "start from
/// the beginning" — and collapsing them would turn the first into the second.
/// A boolean query flag: `?force`, `?force=` , `?force=true` and `?force=1` are `true`;
/// `?force=false` / `?force=0` are `false`, as is absence. Unlike [`has_query_param`] (presence
/// only — right for `?match=`, whose value is the predicate), a flag the contract declares
/// `type: boolean` must honour an explicit `false`: a client that spells "do not force" as
/// `force=false` would otherwise get the destructive path it just declined.
fn query_flag(query: Option<&str>, name: &str) -> bool {
    match query_param(query, name) {
        None => false,
        Some(value) => !matches!(value.to_ascii_lowercase().as_str(), "false" | "0" | "no"),
    }
}

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
        // Exactly upstream's own mapping for these two paths (`imposter_action` in the vendored
        // `authz.rs`, via `principal::map_action`): GET reads fold onto `ImposterRead` regardless
        // of route, and DELETE has its own action shared with `savedProxyResponses`. Reusing them
        // rather than minting new ones keeps the audit action name identical to what the same
        // route was authorized under before it terminated here.
        Terminated::ReadSavedRequests(_) => Action::ImposterRead,
        // The same action the proxied stream already resolves to: upstream classifies the
        // per-port alias as a port-scoped `imposter.read` (`admin_api/authz.rs`), so
        // terminating it changes no authorization posture at all (issue #348).
        Terminated::StreamSavedRequests(_) => Action::ImposterRead,
        // The same action the per-imposter read carries (issue #362): this is that read across the
        // caller's own imposters, so a principal who may read one may read the set. A new action
        // would let a role be granted the fleet view without the per-imposter one, or the reverse,
        // and there is no coherent policy that wants either.
        Terminated::ReadFleetRequests | Terminated::StreamFleetRequests => Action::ImposterRead,
        Terminated::ClearSavedRequests(_) => Action::SavedRequestsClear,
        // The same action upstream authorizes the identical route under (see the comment
        // above): the proxied path already landed on `SavedRequestsClear`, and terminating
        // must not rename what the same call is gated and audited as.
        Terminated::ClearSavedProxyResponses(_) => Action::SavedRequestsClear,
        // Exactly upstream's own mapping for this shape (`principal::map_action`'s
        // `has_space && !is_flow_state` arm, the proxied path's identical route used before
        // this terminated): a space teardown is the Operator-tier "disturb" sibling of
        // `FlowStateClear`, distinguished by the canonical (non-`/admin/imposters/`) prefix.
        Terminated::SpaceTeardown(_, _) => Action::SpaceTeardown,
        // Exactly upstream's own mapping for a space *read* (`principal::map_action`'s
        // `IMPOSTER_READ` arm folds every route onto `ImposterRead` regardless of `has_space`):
        // the single-space `GET .../spaces/{flowId}` already carries this action via the proxied
        // path, and the listing is the same read at a coarser grain — a fleet-wide merge instead of
        // one flow — so it takes the identical action rather than a new one a role table would need
        // to learn separately.
        Terminated::SpacesList(_) => Action::ImposterRead,
        // Issue #335. Not `ImposterRead`, despite the caller only wanting to look: a try is a
        // write in *effect* — it advances scenario state, appends to the request log and can
        // trigger proxyOnce recording — which is the Operator-tier "disturb" shape. See
        // `Action::ImposterTry`'s own doc for why it is a distinct variant rather than a ride on
        // an existing one.
        Terminated::TryImposter(_) => Action::ImposterTry,
        // The front-door route table (issue #131) predates RFC-002 and has no
        // action of its own in its closed §4.1 list. Treated as an ordinary
        // imposter-tier config write pending a dedicated action.
        Terminated::PutRoutes => Action::ImposterWrite,
        Terminated::DeleteRoute(_) => Action::ImposterWrite,
        Terminated::Tenancy(route) => route.action(),
        Terminated::SourceList | Terminated::SourceRead(_) => Action::SourceRead,
        // Deliberately NOT `Action::SourceRead` and deliberately NOT a new
        // `Action::SourceWrite` (issue #253's explicit design decision).
        // `control.rs`'s own audit mapping already names these ops:
        // `SourcePut`/`SourcePullResult` emit `imposter.write`, `SourceDelete`
        // emits `imposter.delete` (`control::action_for`, ~line 919-920) — a
        // pull commits as `SourcePullResult`, the same op a declare's
        // `SourcePut` does not, but both land on the write action. Minting a
        // `SourceWrite` action here would make the audit stream and this
        // enforcement gate disagree about what the *same event* was called,
        // which is worse than the asymmetry with the read side looks: a
        // security reviewer correlating "who wrote this imposter" from the
        // audit log by action name would find writes attributed to an action
        // nothing ever authorized. The read/write split itself is
        // deliberate too — reading a source is its own, lighter power (a
        // Viewer may see what an imposter was built from, `role_allows`'s own
        // comment on `Action::SourceRead`), while writing one is exactly as
        // consequential as `PUT /imposters`, because that is what a pull
        // ultimately produces.
        Terminated::SourcePut | Terminated::SourcePull(_) => Action::ImposterWrite,
        Terminated::SourceDelete(_) => Action::ImposterDelete,
        // RFC-004 §4.3 (issue #278): listing, reading and dry-run compiling a spec are all reads —
        // a compile commits nothing, it only shows what a deploy *would* commit.
        Terminated::SpecList | Terminated::SpecRead(_) | Terminated::SpecCompile(_) => {
            Action::SpecRead
        }
        // Importing redefines what a future deploy *would* answer; deploying redefines what an
        // imposter answers *now* — both are `spec.write`. Deploy additionally requires
        // `imposter.write` on the target port, checked in `terminate_spec_deploy` because the
        // port lives in the body, not the route (see `Action::SpecWrite`'s doc).
        Terminated::SpecPut(_) | Terminated::SpecDeploy(_) => Action::SpecWrite,
        Terminated::SpecDelete { .. } => Action::SpecDelete,
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
                match fleet::body(&route, &node, &state.readiness).await {
                    Ok(Some(body)) => match serde_json::to_vec(&body.value) {
                        Ok(bytes) => {
                            let mut response = buffered_response(
                                StatusCode::OK,
                                Bytes::from(bytes),
                                json_content_type(),
                            )
                            .unwrap_or_else(|response| response);
                            // Same header, same meaning, as the journal merge stamps (#361): the
                            // members list is folded across peers, and a voter that did not answer
                            // leaves a row this node could not fill.
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
        // No port: the route table is read per tenant, not per imposter.
        return match authorize_action(&state, &req, Action::ImposterRead, None, None) {
            Ok((tenant, ..)) => read_routes(&state, &req, &tenant).await,
            Err(response) => response,
        };
    }

    // `GET /front-door/route-hits` (issue #368): the same read, same tenant addressing and same
    // action as the route table above — it reports on exactly the rows that read returns.
    //
    // A separate path rather than a field on the routes body, because that body is a config
    // document a client `PUT`s back verbatim, and because this one pays a fleet fan-out that a
    // config read should not. Spelled `route-hits` and not `routes/hits`: a route id is a
    // user-chosen string, `hits` is a legal one, and `/front-door/routes/{id}` is already a route.
    if req.method() == Method::GET && path == "/front-door/route-hits" {
        return match authorize_action(&state, &req, Action::ImposterRead, None, None) {
            Ok((tenant, ..)) => read_route_hits(&state, &tenant).await,
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
                    // `numberOfRequests` decoration (issue #223): the single-imposter read and the
                    // listing both carry it, and upstream's own answer is this node's local
                    // G-counter slot only — cloned ahead of `proxy`'s move of `state`, same as
                    // `degraded`/`token` above, since the fleet total is asked for only after the
                    // loopback has already answered.
                    let number_of_requests = (list_read
                        || (req.method() == Method::GET && is_single_imposter_read(&path)))
                    .then(|| Arc::clone(&state.journal_net));
                    // `owner` on a space read (issue #359), resolved before `proxy` moves `state`
                    // for the same reason `degraded`/`token` are. A flow is the only thing the
                    // ring owns, so this is the one read that can name one.
                    let space_flow_owner = (req.method() == Method::GET)
                        .then(|| space_read_target(&path))
                        .flatten()
                        .map(|(port, flow)| space_owner(&state, &tenant, port, &flow));
                    // The node, for undoing D2's apply-time dataset compile-down on the way out
                    // (issue #286). Both the single read and the listing: the engine holds a
                    // compiled `lookup` whose CSV path is **node-local**, so leaving it in a
                    // rendered config would make the same imposter read differently depending on
                    // which node answered — and a `GET`-edit-`PUT` through the console would then
                    // store that node's path as a hand-written lookup, wrong everywhere else.
                    let dataset_render_node = (list_read
                        || (req.method() == Method::GET && is_single_imposter_read(&path)))
                    .then(|| state.node.upgrade())
                    .flatten();
                    // The resolved `_rift` knobs (issue #370), read from the applied config before
                    // `proxy` moves `state` for the same reason as everything above. Single-imposter
                    // read only: the listing carries no knobs panel, and resolving them per entry
                    // would be a stored-config read per imposter on the list screen.
                    //
                    // This read and the proxied body are two reads of the same imposter, so a write
                    // landing between them makes the response describe two revisions at once. The
                    // same window every decoration here has; harmless for a knobs panel, which
                    // reports configuration rather than acting on it, and the response's
                    // `Rift-Cluster-Revision` names the record the *write* path will condition on.
                    let flow_knobs = (req.method() == Method::GET
                        && is_single_imposter_read(&path))
                    .then(|| port_param(&target.params))
                    .flatten()
                    .and_then(|port| flow_state_resolved(&state, &tenant, port));
                    let mut response = proxy(state, req, Some(&tenant)).await;
                    if let Some(owned) = owned {
                        response = filter_imposter_list(response, &owned).await;
                    }
                    if let Some(net) = number_of_requests {
                        response =
                            decorate_number_of_requests(response, &net, JOURNAL_PEER_BUDGET).await;
                    }
                    if let Some(owner) = space_flow_owner {
                        response = decorate_space_owner(response, owner).await;
                    }
                    if let Some(knobs) = flow_knobs {
                        response = decorate_flow_state_resolved(response, knobs).await;
                    }
                    if let Some(node) = dataset_render_node {
                        response = strip_compiled_dataset_lookups(response, &node).await;
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

/// `GET /front-door/route-hits`: how many requests each of `tenant`'s routes has claimed, summed
/// across the fleet (issue #368).
///
/// Four answers, kept apart on purpose:
///
/// - `{"installed": true, "hits": {...}}` — one entry per id in this tenant's stored table, `0`
///   included. The zero is the point: a route that has never taken a request is wrong or dead.
/// - `{"installed": false, "hits": null}` — this tenant's routes are stored but never compiled
///   into the shared front door, so none of them *can* take a dispatch. A map of zeros here would
///   assert they took no traffic, which is a claim about traffic where the truth is about
///   installation (the same bound-vs-unknown error #369 fixed for bind status).
/// - either of the above with `Rift-Cluster-Partial: true` — a voter could not be reached, so the
///   sums are floors.
async fn read_route_hits(state: &Arc<FrontState>, tenant: &TenantId) -> Response<FrontBody> {
    let Some(node) = state.node.upgrade() else {
        return typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            "cluster node is shutting down",
        );
    };

    // Not installed is decided before anything else: there is nothing to sum, and no reason to
    // spend the fan-out budget discovering that.
    if !routes_installed_for(tenant.as_str()) {
        return route_hits_response(&RouteHits::NotInstalled, false);
    }

    // The same read `GET /front-door/routes` answers from, so the ids reported here are exactly
    // the rows the console is rendering. A counter for an id this table no longer names is
    // stranded and deliberately not reported.
    let table = match node.route_table(tenant.as_str()) {
        Ok(table) => table,
        Err(e) => return internal(&e.to_string()),
    };
    let ids: Vec<String> = table.routes.into_iter().map(|route| route.id).collect();

    let local = state.route_hits.snapshot();
    let status = node.status();
    let peers = route_hits::peer_hits(&node).await;
    let (hits, partial) = route_hits::merge_route_hits(&ids, &local, &peers);
    // Folded from the same peer replies the counts came from, so the presence answer and the sums
    // describe the same moment and the same set of nodes (issue #403).
    //
    // `peer_hits` enumerates *voters*, but a learner replicates and serves the data plane in full
    // and can bind a front door — and past the auto-voter ceiling learners are a permanent steady
    // state, not a transient. They are therefore counted as members the fan-out never asked, which
    // blocks a claim of proven absence without pretending they answered. (Their counts are missing
    // from the sums too; that is the older gap this does not widen — see the follow-up filed with
    // this change.)
    let unasked_members = status
        .learners
        .iter()
        .filter(|&&id| id != status.node_id)
        .count();
    let front_door = route_hits::merge_front_door(state.front_door_bound, &peers, unasked_members);
    route_hits_response(&RouteHits::Installed { hits, front_door }, partial)
}

fn route_hits_response(hits: &RouteHits, partial: bool) -> Response<FrontBody> {
    let body = match serde_json::to_vec(&hits.body()) {
        Ok(body) => body,
        Err(e) => return internal(&e.to_string()),
    };
    let mut response =
        match buffered_response(StatusCode::OK, Bytes::from(body), json_content_type()) {
            Ok(response) | Err(response) => response,
        };
    if partial {
        set_header(&mut response, HEADER_PARTIAL, "true");
    }
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
            // The refusal is §8.4's indistinguishable 404, identical to `NotBoundToTenant` below
            // (D-45: "owned by someone else" and "owned by nobody" are one answer).
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
        // RFC-002 §8.4 / D-45: must render byte-identical to the tenant-serving
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
    // credential is the silent-fallback shape this codebase treats as a defect (D-46).
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
// The error channel here *is* a rendered HTTP response, which is how a handler returns a
// refusal with `?` from anywhere in its body. `Response<FrontBody>` is hyper's type and
// its size is not ours to shrink; boxing it would only move the unboxing to every caller.
#[allow(clippy::result_large_err)]
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

/// The blob an op carries, if it carries one (#438).
///
/// Both ops still carry their bytes on the log until D-23 (#439) lands; this
/// is the copy the fan-out sideloads to a quorum ahead of propose.
///
/// Only two ops do. Both already hold a digest that was derived from these
/// exact bytes at mint time, so this re-uses it rather than re-hashing: a
/// digest computed twice is two chances to disagree, and `control::validate`
/// re-proves the record against its bytes on every replica anyway.
fn op_blob(op: &ControlOp) -> Option<(&str, &[u8])> {
    match op {
        ControlOp::DatasetPut {
            record,
            csv: Some(csv),
            ..
        } => Some((record.digest.as_str(), csv.as_bytes())),
        ControlOp::SpecPut {
            meta,
            document: Some(document),
            ..
        } => Some((meta.digest.as_str(), document.as_bytes())),
        // Not a blob op — or a blob op already stripped by `fan_out_then_submit`. The second
        // cannot reach here from a replay: parking happens before stripping at every site,
        // so a parked intent always still carries its bytes.
        _ => None,
    }
}

/// Take the bytes off the op (#439, D-23): what is proposed carries the digest, the declared
/// size and the accepting node, never the payload. Called only once a joint quorum holds the
/// bytes — see [`fan_out_then_submit`], the sole caller.
fn strip_blob(op: &mut ControlOp, accepted_by: u64) {
    match op {
        ControlOp::DatasetPut { csv, origin, .. } => {
            *csv = None;
            *origin = accepted_by;
        }
        ControlOp::SpecPut {
            document, origin, ..
        } => {
            *document = None;
            *origin = accepted_by;
        }
        _ => {}
    }
}

/// Fan this op's blob out to a joint-consensus quorum, take the bytes off the op, and submit
/// it — in one place, so nothing between "a quorum holds it" and "the op that references it
/// committed" can be dropped by a caller (#438, #439; D-18, D-19, D-23, D-48).
///
/// Runs **after** the intent is parked and **before** submit, which is what makes a shortfall
/// recoverable rather than lost: the parked intent still carries its bytes, so a replay
/// (`compose::drain_parked_intents`) re-enters here and re-runs the fan-out, which is
/// idempotent because `BlobTransfer::put` probes what the peer already staged and sends only
/// the remainder.
///
/// Only once a majority of both the committed and the effective configuration holds the
/// bytes — the joint rule D-19 fixes, read in one `with_raft_state` closure by
/// `network::joint_voters` and decided by `QuorumTargets::satisfied_by` — are they
/// **stripped** from the op and `origin` stamped with this node. What reaches the log is
/// metadata-sized (D-23); a member the fan-out missed fetches on apply, asking this node
/// first. The stripped copy is what `submit` sees; the parked copy is untouched.
///
/// `submit` is the caller's own submit expression, with or without a deadline, because the
/// three write paths differ there and nowhere else. It runs while the blob is pinned and the
/// pin is released on return. There is no guard handed back and therefore none a caller can
/// `let _ =` away — which is how the pin-hold gap #438 disclosed is closed structurally
/// rather than by a test asserting nobody dropped it.
///
/// `Err` carries an operator-facing reason the blob did not reach a quorum; the caller must
/// then leave the intent **parked** and ask for a replay. `Ok` carries whatever `submit`
/// returned, untouched.
pub(crate) async fn fan_out_then_submit<F, Fut>(
    node: &RaftNode,
    mut request: ControlRequest,
    submit: F,
) -> Result<Fut::Output, String>
where
    F: FnOnce(ControlRequest) -> Fut,
    Fut: std::future::Future,
{
    let Some((digest, bytes)) = op_blob(&request.op) else {
        return Ok(submit(request).await);
    };
    let digest = rift_cluster::blobs::BlobDigest::parse(digest)
        .map_err(|e| format!("the op carries an unusable blob digest: {e}"))?;

    let (outcome, pin) = node
        .fan_out_blob(&digest, bytes)
        .await
        .map_err(|e| format!("blob fan-out failed: {e}"))?;

    if !outcome.quorum {
        // Naming the skewed peers separately matters for diagnosis: "3 nodes, 2
        // acks, 1 cannot serve blobs" is an upgrade, while "3 nodes, 2 acks" with
        // no skew is a partition. Collapsing them would make a half-upgraded fleet
        // look like a network fault.
        return Err(format!(
            "the blob reached {} of the current voters, which is not a majority of both \
             the committed and the effective configuration{}",
            outcome.acks.len(),
            if outcome.skewed.is_empty() {
                String::new()
            } else {
                format!(
                    " ({} member(s) run a build that cannot serve blobs)",
                    outcome.skewed.len()
                )
            }
        ));
    }
    tracing::debug!(
        digest = digest.as_str(),
        acks = outcome.acks.len(),
        bytes_sent = outcome.bytes_sent,
        joint = outcome.joint,
        "blob fanned out to a quorum before propose"
    );

    strip_blob(&mut request.op, node.id());
    let submitted = submit(request).await;
    drop(pin);
    Ok(submitted)
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
fn imposter_scope(node: &RaftNode, tenant: &TenantId, port: u16) -> Option<ContextScope> {
    node.imposter_config(tenant.as_str(), port)
        .inspect_err(|e| {
            tracing::warn!(tenant = tenant.as_str(), port, error = %e, "the imposter's context scope could not be resolved");
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
/// `flowState.contextScope` — `i{port}:` per imposter (the default), `t<tenant>:` per tenant (#288), `f:` fleet-wide — which is why
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
/// The tenant it renders under `Tenant` scope is the *request* tenant, which is safe only
/// because `imposter_scope` reads `imposter_config(tenant, port)` — it resolves a scope at all
/// only when the request tenant owns the row, so the tenant that owns the port and the tenant
/// named here are the same tenant.
fn space_owner(state: &FrontState, tenant: &TenantId, port: u16, flow_id: &str) -> Option<NodeId> {
    let node = state.node.upgrade()?;
    let ring = node.ring();
    if ring.is_empty() {
        return None;
    }
    // `Imposter` is both the documented default and the isolating one, and #359's contract is that
    // an owner lookup never fails a read it decorates. Unchanged from before `imposter_scope` was
    // extracted — see that function's doc for why the listing must NOT make the same fold.
    let scope = imposter_scope(&node, tenant, port).unwrap_or_default();
    ring.owner(OwnedKey::new(
        KeyClass::FlowKv,
        &scope.scoped_flow_id(Some(port), Some(tenant.as_str()), flow_id),
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
///
/// Also matches the `/admin/imposters/{port}` alias (issue #223 review, Important): the
/// collection listing already answers identically under both spellings (see
/// [`is_imposter_listing`]), and leaving this one unaware of the alias is exactly how it and the
/// listing ended up disagreeing about the very same imposter — the listing's `numberOfRequests`
/// fleet-decorated, the single-imposter alias silently answering this node's local count alone.
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

/// Add `owner` to a proxied space read (issue #359).
///
/// Purely additive, which is what makes its failure handling differ from
/// [`decorate_number_of_requests`] below. That one **fails closed** because it *corrects* a value
/// upstream already answered wrongly — a `numberOfRequests` this node cannot vouch for is worse
/// than a 500. This one adds a field that is optional by construction: the console renders its
/// absence as "not known", so a body arriving without `owner` is honest, while a 500 would break a
/// space read that upstream answered perfectly well.
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

/// Remove the compiled dataset `lookup` from a rendered imposter (issue #286).
///
/// The engine executes a compiled `_behaviors.lookup` whose CSV path is node-local, so it must not
/// appear in a rendered config: the same imposter would read differently per node, and an operator
/// who edited a rendered document and `PUT` it back would store that node's filesystem path as a
/// hand-written lookup. What is left is the operator's own `_rift.dataset` block, carrying the pin.
///
/// Additive-in-reverse, and takes the same failure polarity as [`decorate_flow_state_resolved`]: a
/// body that will not parse passes through **unchanged and logged**. That is the safe direction
/// here — the untouched body is the engine's own answer, which is complete and merely more verbose
/// than intended, where a fabricated one would not be.
async fn strip_compiled_dataset_lookups(
    response: Response<FrontBody>,
    node: &Arc<RaftNode>,
) -> Response<FrontBody> {
    let (parts, body) = response.into_parts();
    if !parts.status.is_success() {
        return Response::from_parts(parts, body);
    }
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return internal(&format!(
                "reading the imposter body to strip compiled dataset lookups: {e}"
            ));
        }
    };
    let body = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(mut document) => {
            rift_cluster::datasets::strip_compiled(&mut document, |digest| node.spool_path(digest));
            match serde_json::to_vec(&document) {
                Ok(stripped) => Bytes::from(stripped),
                Err(e) => {
                    // The original bytes are valid and the status is untouched, so this is a safe
                    // last resort — but it reverts to the *unstripped* body, which is the leak this
                    // function exists to prevent. Logged for parity with the parse arm above, so it
                    // cannot happen invisibly if `document` ever stops being trivially serializable.
                    tracing::error!(
                        error = %e,
                        "the imposter body could not be re-serialized after stripping its \
                         compiled dataset lookups; serving it unstripped"
                    );
                    bytes
                }
            }
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                "the imposter body could not be parsed to strip its compiled dataset lookups"
            );
            bytes
        }
    };
    let mut response = buffered_response(parts.status, body, json_content_type())
        .unwrap_or_else(|response| response);
    carry_over_headers(&mut response, &parts.headers);
    response
}

/// Add `_rift.flowStateResolved` to a proxied single-imposter read (issue #370).
///
/// Additive, and so it takes [`decorate_space_owner`]'s failure polarity rather than
/// [`decorate_number_of_requests`]'s: an unparseable body passes through **unchanged and logged**,
/// never silently defaulted. That is safe here in a way it would not be if this rendered the stored
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

/// Move the headers an earlier decoration set onto a rebuilt response.
///
/// [`buffered_response`] starts from an **empty** header map. That is right for the first
/// decoration in a chain and wrong for any later one, and this is the first decoration that runs
/// after another: on the single-imposter read [`decorate_number_of_requests`] has already stamped
/// `Rift-Cluster-Partial` when the fleet fan-out could not reach a peer, and dropping it would make
/// a count that is knowingly missing a node's slot arrive looking authoritative — the wrong-but-
/// quiet failure that decoration fails closed to avoid in the first place.
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
/// `preserve_order`, so key order comes out normalised. Already true of this path — the
/// `numberOfRequests` decoration re-serialises the same way — and no consumer depends on it.
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
fn flow_state_resolved(state: &FrontState, tenant: &TenantId, port: u16) -> Option<ResolvedKnobs> {
    let node = state.node.upgrade()?;
    let config = node
        .imposter_config(tenant.as_str(), port)
        .inspect_err(|e| {
            tracing::warn!(tenant = tenant.as_str(), port, error = %e, "imposter read serves without its resolved flow-state knobs");
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
            tracing::error!(tenant = tenant.as_str(), port, error = %e, "the stored imposter config did not parse; serving without resolved flow-state knobs");
        })
        .ok()?;
    // A stored value the knobs cannot interpret is left off the response rather than published as
    // a default: admission refuses those, so reaching here means a record written out of band, and
    // "async, inherited" over a document that says otherwise is the wrong-but-quiet answer.
    ResolvedKnobs::from_imposter(&config)
        .inspect_err(|e| {
            tracing::error!(tenant = tenant.as_str(), port, error = %e, "the stored flow-state knobs did not resolve; serving without them");
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

/// Rewrite `numberOfRequests` to the fleet sum on a proxied imposter read (issue #223): after #222
/// bound the engine to `ClusterJournal`, upstream's own answer is this node's local G-counter slot
/// only, real but partial — the fleet total is `fleet_count`'s job, fetched over
/// `/_cluster/journal/counts` under `budget` via [`JournalNet::fleet_counts`]. Runs on both shapes
/// upstream answers: the listing's array (already narrowed to `owned` by
/// [`filter_imposter_list`], so only ports the caller can see are ever asked about) and the
/// single-imposter object.
///
/// **Fails closed**, exactly like [`narrow_imposter_listing`]: a body that is not what the
/// tenancy filter already required refuses rather than passing the un-decorated original through —
/// a `numberOfRequests` this node cannot vouch for is worse than a 500 that says so.
async fn decorate_number_of_requests(
    response: Response<FrontBody>,
    net: &JournalNet,
    budget: Duration,
) -> Response<FrontBody> {
    let (parts, body) = response.into_parts();
    if !parts.status.is_success() {
        return Response::from_parts(parts, body);
    }
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return internal(&format!(
                "reading the imposter body to decorate numberOfRequests: {e}"
            ));
        }
    };
    match rewrite_number_of_requests(&bytes, net, budget).await {
        Ok((rewritten, partial)) => {
            let mut response =
                buffered_response(parts.status, Bytes::from(rewritten), json_content_type())
                    .unwrap_or_else(|response| response);
            if partial {
                set_header(&mut response, HEADER_PARTIAL, "true");
            }
            response
        }
        Err(e) => internal(&e),
    }
}

/// Collect every `port` the body names, ask the fleet for each one's slot in one round trip per
/// peer, and rewrite `numberOfRequests` in place — `imposters[].numberOfRequests` for the listing,
/// the one object's field for the single-imposter read.
async fn rewrite_number_of_requests(
    bytes: &[u8],
    net: &JournalNet,
    budget: Duration,
) -> Result<(Vec<u8>, bool), String> {
    let mut doc: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| {
        format!("the imposter body was not JSON, so numberOfRequests could not be decorated: {e}")
    })?;
    let is_listing = doc.get("imposters").and_then(|v| v.as_array()).is_some();

    let ports: Vec<u16> = if is_listing {
        doc["imposters"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.get("port").and_then(serde_json::Value::as_u64))
            .filter_map(|p| u16::try_from(p).ok())
            .collect()
    } else {
        doc.get("port")
            .and_then(serde_json::Value::as_u64)
            .and_then(|p| u16::try_from(p).ok())
            .into_iter()
            .collect()
    };
    // Nothing to decorate — an empty listing, or a body this shouldn't have run against at all —
    // is not a failure; it just re-encodes unchanged.
    if ports.is_empty() {
        return serde_json::to_vec(&doc)
            .map(|body| (body, false))
            .map_err(|e| e.to_string());
    }

    let (totals, partial) = net.fleet_counts(&ports, budget).await;
    let rewrite_entry = |entry: &mut serde_json::Value| {
        let Some(port) = entry
            .get("port")
            .and_then(serde_json::Value::as_u64)
            .and_then(|p| u16::try_from(p).ok())
        else {
            return;
        };
        if let Some(total) = totals.get(&port)
            && let Some(map) = entry.as_object_mut()
        {
            map.insert("numberOfRequests".to_owned(), serde_json::json!(total));
        }
    };
    if is_listing {
        if let Some(array) = doc.get_mut("imposters").and_then(|v| v.as_array_mut()) {
            for entry in array {
                rewrite_entry(entry);
            }
        }
    } else {
        rewrite_entry(&mut doc);
    }

    serde_json::to_vec(&doc)
        .map(|body| (body, partial))
        .map_err(|e| format!("re-encoding the decorated imposter body: {e}"))
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

    // So does the whole source surface (issue #239 for the reads, #253 for
    // the three writes below): same shared gate, but none of the imposter
    // write machinery below applies — a source is not an imposter record,
    // has no `If-Match` surface, and needs no `_rift.script` resolution.
    match kind {
        Terminated::SourceRead(id) => {
            return terminate_sources(&state, &node, Some(id.as_str()), &tenant);
        }
        Terminated::SourceList => {
            return terminate_sources(&state, &node, None, &tenant);
        }
        Terminated::SourcePut => {
            return terminate_source_write(&state, req, SourceWrite::Put, principal_id, &tenant)
                .await;
        }
        Terminated::SourceDelete(id) => {
            return terminate_source_write(
                &state,
                req,
                SourceWrite::Delete(id),
                principal_id,
                &tenant,
            )
            .await;
        }
        Terminated::SourcePull(id) => {
            return terminate_source_write(
                &state,
                req,
                SourceWrite::Pull(id),
                principal_id,
                &tenant,
            )
            .await;
        }
        // The spec reads (RFC-004 §4.3, issue #278) return here for the same reason the source
        // reads do: they answer from local applied state and have nothing to commit.
        Terminated::SpecList => {
            return terminate_spec_read(&node, &tenant, None);
        }
        Terminated::SpecRead(id) => {
            return terminate_spec_read(&node, &tenant, Some(id.as_str()));
        }
        // A dry-run compile needs the request body (the optional `{"port": ...}`), so it takes
        // `req` directly and does its own collection, exactly as the source writes below do.
        Terminated::SpecCompile(id) => {
            return terminate_spec_compile(&node, req, id, &tenant).await;
        }
        // A try commits nothing — its whole result is what the imposter answered — so it returns
        // here for the same reason the source surface does.
        Terminated::TryImposter(port) => {
            return terminate_try_imposter(&node, req, port, &tenant).await;
        }
        // A merge-on-read has nothing to commit, so it returns here for the same reason the
        // source surface does — none of the `If-Match`/`_rift.script`/loopback-render machinery
        // below applies.
        Terminated::ReadSavedRequests(port) => {
            return terminate_read_saved_requests(&state, port, req.uri().query()).await;
        }
        // The live sibling of the read above, and it returns here for the same reason: nothing to
        // commit, and none of the `If-Match`/`_rift.script`/loopback-render machinery applies to a
        // response whose body is still being written when the handler returns (issue #348).
        Terminated::StreamSavedRequests(port) => {
            return terminate_stream_saved_requests(&state, port, req.headers());
        }
        // The fleet pair (issue #362) returns here for the identical reasons — nothing to commit,
        // and a streamed body outlives the handler.
        Terminated::ReadFleetRequests => {
            return terminate_read_fleet_requests(&state, &tenant, req.uri().query());
        }
        Terminated::StreamFleetRequests => {
            return terminate_stream_fleet_requests(&state, &tenant, req.headers());
        }
        // Only the `?match=`-narrowed form diverts here (issue #223 item 4's original design,
        // B3 — #224 left it alone deliberately, see `terminate_clear_saved_requests`'s doc): a
        // scoped clear has no fleet-wide meaning, so it stays a local-only proxy stamped
        // `Rift-Cluster-Partial`, never a Raft write. The **unscoped** form falls through this
        // match (to `_ => {}` below) into the ordinary terminated-write pipeline instead — as of
        // #224 it commits `ControlOp::JournalClearGen` through Raft like any other write, so it
        // needs exactly the machinery this early return exists to skip.
        Terminated::ClearSavedRequests(_) if has_query_param(req.uri().query(), "match") => {
            return terminate_clear_saved_requests(&state, req, &tenant).await;
        }
        // Two independent halves (issue #224): the flow-state teardown proxies exactly as
        // before this issue, and the journal-generation commit happens alongside it inside
        // `terminate_space_teardown` itself. Diverted here for the same reason the source
        // writes and the tenancy surface are — neither half fits `build_mutation`'s single-op,
        // loopback-rendered shape, and there is nothing to `FetchAfter`/`Captured` from for a
        // route with no state-machine record of its own.
        Terminated::SpaceTeardown(port, flow) => {
            return terminate_space_teardown(&state, &node, req, port, flow, &tenant, principal_id)
                .await;
        }
        // A merge-on-read fan-out has nothing to commit, so it returns here for the same reason
        // `ReadSavedRequests`/`ReadFleetRequests` do above — none of the `If-Match`/`_rift.script`/
        // loopback-render machinery below applies to a read.
        Terminated::SpacesList(port) => {
            return terminate_spaces_list(&state, &node, port, &tenant, &bindings).await;
        }
        _ => {}
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

    // The three spec writes (RFC-004 §4.3, issue #278) each need to inspect the stored record (and,
    // for delete, decide 409-vs-force) before there is a `Mutation` to build at all, so they build
    // their own and call `run_mutation` directly rather than routing through `build_mutation`'s
    // single generic shape — same reasoning as the tenancy surface and the source writes above,
    // just deferred to here because these three DO share the rest of the write machinery
    // (`If-Match`, park/replay, the barrier, the cluster headers) that the early-return surfaces
    // above do not.
    match kind {
        Terminated::SpecPut(id) => {
            return terminate_spec_put(
                &state,
                &node,
                &tenant,
                id,
                &body,
                auth.as_deref(),
                host.as_ref(),
                idempotency.as_deref(),
                if_match.as_deref(),
                principal_id,
            )
            .await;
        }
        Terminated::SpecDelete { id, force } => {
            return terminate_spec_delete(
                &state,
                &node,
                &tenant,
                id,
                force,
                auth.as_deref(),
                host.as_ref(),
                idempotency.as_deref(),
                if_match.as_deref(),
                principal_id,
            )
            .await;
        }
        Terminated::SpecDeploy(id) => {
            return terminate_spec_deploy(
                &state,
                &node,
                &tenant,
                id,
                &body,
                auth.as_deref(),
                host.as_ref(),
                idempotency.as_deref(),
                if_match.as_deref(),
                principal_id,
                &bindings,
            )
            .await;
        }
        _ => {}
    }

    let is_batch = matches!(kind, Terminated::ReplaceAllImposters);
    let mutation = match build_mutation(
        &state,
        &node,
        kind,
        &tenant,
        &body,
        auth.as_deref(),
        host.as_ref(),
        principal_id.as_deref(),
        &bindings,
    )
    .await
    {
        Ok(mutation) => mutation,
        Err(response) => return response,
    };
    match run_mutation(
        &state,
        &node,
        &tenant,
        mutation,
        is_batch,
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

/// `GET /imposters/{port}/requests|savedRequests` with no `?match=` (issues #223, #225): the
/// fleet-wide merge-on-read `classify` terminated this route for, in both its uncursored and its
/// `?since=` form.
///
/// `merge_shards_since`'s gate tests already prove the walk's properties (every entry exactly
/// once, per-shard gaplessness across membership changes, a token that never regresses, an honest
/// truncation bit); this is purely turning that outcome into the bytes and headers the openapi
/// contract pins:
///
/// - the body stays a **bare JSON array** of `RecordedRequest`, cursored or not — the historical
///   shape is a compatibility contract, and `matchOutcome` rides inside each entry untouched so
///   #220's diagnostics survive a merged read for free;
/// - `x-rift-next-index` carries the **opaque vector token**, replacing #223's withheld-header
///   convention. #223 withheld it because a scalar index means nothing across shards; #225's
///   whole point is that there is now a value that does mean something, so withholding it would
///   leave the client no way to page at all;
/// - `x-rift-truncated: true` only when the walk really lost entries to eviction, matching the
///   meaning upstream gives the header.
///
/// A malformed token is a **typed 400**, never a defaulted position: silently reading it as 0
/// would replay the whole journal, and reading it as "current" would skip everything recorded
/// since — both are wrong-but-quiet, and the wrongness would surface in the client's decoder with
/// nothing server-side to correlate. Upstream refuses an unparseable `since` the same way.
async fn terminate_read_saved_requests(
    state: &Arc<FrontState>,
    port: u16,
    query: Option<&str>,
) -> Response<FrontBody> {
    let this_node = state.journal_net.node_id();
    let cursor = match query_param(query, "since") {
        // Legacy acceptance is deliberate and narrow: a bare `u64` a client holds can only have
        // come from a per-node proxied read of THIS node, because a merged read has issued no
        // cursor at all until this issue. Reading it as `{this_node: seq}` is the honest upgrade
        // -window interpretation; every other shard starts at 0 because the client provably has
        // seen none of them.
        Some(raw) => match JournalCursor::decode_or_legacy(raw, this_node) {
            Ok(cursor) => Some(cursor),
            Err(e) => {
                return typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    &format!("since is not a usable cursor: {e}"),
                );
            }
        },
        // Absent `since` is a **baseline** read, and stays `None` all the way down rather than
        // becoming a cursor at position zero. The two differ in exactly one observable: a
        // baseline read is a snapshot and can never be truncated, while a reader who claims a
        // position of zero has provably missed whatever eviction removed. Upstream and this
        // crate's single-node path both draw that line, so collapsing it here would make every
        // ordinary uncursored read of an evicting port answer `x-rift-truncated: true`.
        None => None,
    };

    let page = state
        .journal_net
        .merge_read_since(port, cursor.as_ref(), JOURNAL_PEER_BUDGET)
        .await;
    let requests: Vec<&RecordedRequest> = page.entries.iter().map(|entry| &entry.request).collect();
    match serde_json::to_vec(&requests) {
        Ok(bytes) => {
            // Headers are set only on a response that carries the body they describe: a 500
            // from `buffered_response` must not advertise a cursor for a page nobody received,
            // which is why upstream splits its own cursor-response builder the same way.
            match buffered_response(StatusCode::OK, Bytes::from(bytes), json_content_type()) {
                Ok(mut response) => {
                    set_header(&mut response, HEADER_NEXT_INDEX, &page.next.encode());
                    if page.truncated {
                        set_header(&mut response, HEADER_TRUNCATED, "true");
                    }
                    if page.partial {
                        set_header(&mut response, HEADER_PARTIAL, "true");
                    }
                    response
                }
                Err(response) => response,
            }
        }
        // Fails closed like `narrow_imposter_listing`: a body this node cannot encode must not
        // become a 200 with nothing in it, which would read as "no requests ever recorded" to
        // whatever is asserting against this.
        Err(e) => internal(&format!("encoding the merged journal read: {e}")),
    }
}

/// The ports one fleet journal answer covers, and what it left out — resolved from the caller's
/// tenant (issue #362).
///
/// Ownership is established here, once, for both the read and the stream: the port set is
/// `tenant_owned_ports` on this node's applied state, so a port another tenant owns is never in the
/// walk at all. That is why neither route needs the per-port ownership gate `addressed_port` gives
/// a single-imposter route — see `Terminated::ReadFleetRequests`.
///
/// Carries `tenant_owned_ports`' own `result_large_err` allow, and for its reason: the error *is* a
/// built `Response`, which is the whole point — a refusal here is already the answer to send, not a
/// code some caller has to re-render.
#[allow(clippy::result_large_err)]
fn fleet_ports(
    state: &Arc<FrontState>,
    tenant: &TenantId,
) -> Result<Vec<u16>, Response<FrontBody>> {
    Ok(tenant_owned_ports(state, tenant)?.into_iter().collect())
}

/// The `coverage` block every fleet journal answer carries — the stated cap (issue #362, AC3).
///
/// D-32: `coverage: {covered, total, omitted, capped}` is on every fleet read and re-announced
/// on the stream whenever the covered set moves, so a capped view is never mistaken for the whole.
///
/// `omitted` names the ports rather than counting them: "3 imposters were left out" tells an
/// operator their view is short, while naming them tells them whose traffic they are not seeing.
fn coverage_json(coverage: &Coverage) -> serde_json::Value {
    serde_json::json!({
        "covered": coverage.covered,
        "total": coverage.total(),
        "omitted": coverage.omitted,
        "capped": coverage.is_capped(),
    })
}

/// One row of a fleet journal answer: the recorded request, plus which imposter it was recorded on.
///
/// The port is what makes a merged fleet row readable at all — without it the answer is a pile of
/// requests with no way to tell which mock served them.
fn fleet_row(event: &FleetTailEvent) -> serde_json::Value {
    serde_json::json!({
        "port": event.port,
        "flowId": event.entry.flow_id,
        "request": event.entry.request,
    })
}

/// `GET /admin/requests` (issue #362) — the tenant's whole request journal, merged server-side and
/// resumable through one cursor.
///
/// This is `terminate_read_saved_requests` across every imposter the tenant owns, and it exists
/// because assembling the same view in the console cost N requests per poll, an ordering that was
/// an artifact of which response arrived first, N independent cursors that could drop or replay
/// entries at a poll boundary, and a **silent** truncation at the first 25 ports. Each of those is
/// answered here: one request, one order by recorded timestamp, one exact cursor, and a cap that
/// states what it left out.
///
/// A missing `?since=` is a baseline read, exactly as it is per imposter: every covered port serves
/// its retained history. Because a baseline names no port, every covered port is a join — so the
/// answer declares them in `joined`, which is what tells a *resuming* client which ports may repeat
/// entries it has already seen.
fn terminate_read_fleet_requests(
    state: &Arc<FrontState>,
    tenant: &TenantId,
    query: Option<&str>,
) -> Response<FrontBody> {
    let cursor = match query_param(query, "since") {
        Some(raw) => match FleetCursor::decode(raw) {
            Ok(cursor) => Some(cursor),
            // Refused, never defaulted, for `JournalCursor`'s reason: defaulting to the beginning
            // replays the whole journal and defaulting to "now" silently skips everything since the
            // token went stale. `CursorError::WrongScope` is what lets this message tell a caller
            // who pasted a per-imposter token which endpoint takes it.
            Err(e) => {
                return typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    &format!("since is not a usable fleet cursor: {e}"),
                );
            }
        },
        None => None,
    };

    let ports = match fleet_ports(state, tenant) {
        Ok(ports) => ports,
        Err(response) => return response,
    };

    let page = state.journal_net.fleet_page(
        &ports,
        state.fleet_journal_port_cap,
        cursor.as_ref(),
        // A read serves history: a joining port's retained entries are exactly what the caller
        // asked for, and `joined` declares where duplicates are possible.
        JoinMode::Replay,
        // No `id:` lines to emit, so no per-event token is folded — see `IdPolicy`.
        IdPolicy::PageOnly,
    );

    let body = serde_json::json!({
        "requests": page.events.iter().map(fleet_row).collect::<Vec<_>>(),
        "cursor": page.next.encode(),
        "coverage": coverage_json(&page.coverage),
        "joined": page.joined,
    });

    match serde_json::to_vec(&body) {
        Ok(bytes) => {
            match buffered_response(StatusCode::OK, Bytes::from(bytes), json_content_type()) {
                Ok(mut response) => {
                    // The same three headers the per-imposter read sets, meaning the same three things
                    // — so a client that already understands one read understands this one.
                    set_header(&mut response, HEADER_NEXT_INDEX, &page.next.encode());
                    if page.truncated {
                        set_header(&mut response, HEADER_TRUNCATED, "true");
                    }
                    if page.partial {
                        set_header(&mut response, HEADER_PARTIAL, "true");
                    }
                    response
                }
                Err(response) => response,
            }
        }
        // Fails closed like the per-imposter read: a body this node cannot encode must not become a
        // 200 with nothing in it, which reads as "no requests ever recorded".
        Err(e) => internal(&format!("encoding the fleet journal read: {e}")),
    }
}

/// How often an idle stream emits `: ping`, matching upstream's `HEARTBEAT` exactly so a load
/// balancer's idle timeout behaves the same clustered as it does single-node.
const STREAM_HEARTBEAT: Duration = Duration::from_secs(15);

/// Smallest gap between two drains of one stream. The append signal is journal-wide (one channel
/// per journal, not per port), so recording on ANY imposter wakes every attached tail; without a
/// floor here a busy node would have each tail re-merging back-to-back, overwhelmingly to produce
/// empty pages for ports its client never asked about. Far below the anti-entropy cadence a tail
/// declares, so it costs no visible latency.
const STREAM_DRAIN_DEBOUNCE: Duration = Duration::from_millis(25);

/// Write-side buffer for the SSE channel, upstream's `CHANNEL_BUFFER` verbatim. A client this far
/// behind on the socket blocks the forwarder on `send_data`, which is what turns a slow reader
/// into *its own* cursor stalling rather than into unbounded memory here.
const STREAM_CHANNEL_BUFFER: usize = 16;

/// One SSE frame. `id` is a cursor token rather than upstream's scalar bus seq — the one
/// deliberate divergence in the framing, and the reason the tail and the `?since=` read are the
/// same contract.
fn sse_frame(event: &str, id: Option<&str>, data: &serde_json::Value) -> Bytes {
    let mut frame = format!("event: {event}\n");
    if let Some(id) = id {
        frame.push_str(&format!("id: {id}\n"));
    }
    frame.push_str(&format!("data: {data}\n\n"));
    Bytes::from(frame)
}

/// `GET /imposters/{port}/savedRequests/stream` with no `?match=` (issue #348) — the merged,
/// fleet-wide live tail.
///
/// **Shape:** a cursor walk that never ends. Every wake re-runs `merge_cached_since` — the same
/// `merge_shards_since` the `?since=` read runs — from the cursor this connection holds, emits
/// whatever is new, and folds the cursor forward. Live tailing, a `Last-Event-ID` reconnect and a
/// polled cursor read are therefore one code path, which is what makes "the `id:` after an event
/// is the token a simultaneous cursor read would return" true by construction rather than by
/// test.
///
/// **Latency is declared, not pretended away.** Local entries surface as soon as the journal's
/// append signal fires. *Peer* entries cannot: they arrive in the replica cache on the
/// anti-entropy cadence, and asking every peer per wake would multiply inter-node traffic by the
/// number of attached clients. So the tail rides that cadence and says so — `hello` carries
/// `clusterTailLatencyMs`, and the number is the interval the loop was actually started with.
///
/// **What is deliberately different from upstream's stream**, all additive per Ch.12's standing
/// gate: `hello` carries `clusterTailLatencyMs` and `cursor` and omits upstream's scalar `seq`
/// (a merged stream has no single bus position); `id:` is the vector cursor token; and `index` is
/// emitted only for entries this node wrote — a peer's bare seq means nothing in this node's
/// numbering, and offering it would invite a client to present another shard's position as a
/// legacy scalar `?since=`.
fn terminate_stream_saved_requests(
    state: &Arc<FrontState>,
    port: u16,
    headers: &hyper::HeaderMap,
) -> Response<FrontBody> {
    let this_node = state.journal_net.node_id();
    // A reconnect presents the id of the last event it actually received. Same acceptance as the
    // read path's `?since=`, legacy scalar included, so a client can move between the two modes
    // with one token — and the same typed 400 rather than a defaulted position, because
    // defaulting either replays everything or silently skips it.
    let resume = match headers.get("last-event-id") {
        None => None,
        Some(value) => {
            let Ok(raw) = value.to_str() else {
                return typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    "Last-Event-ID is not readable ASCII; expected a cursor token",
                );
            };
            match JournalCursor::decode_or_legacy(raw, this_node) {
                Ok(cursor) => Some(cursor),
                Err(e) => {
                    return typed_error(
                        StatusCode::BAD_REQUEST,
                        ErrorKind::BadData,
                        &format!("Last-Event-ID is not a usable cursor: {e}"),
                    );
                }
            }
        }
    };

    let journal_net = Arc::clone(&state.journal_net);
    let tail_latency = journal_net.tail_latency();

    // Subscribed BEFORE the baseline position is read, and the order is load-bearing. `subscribe`
    // marks a receiver as having already seen the channel's current value, so a recording that
    // lands between the read and the subscribe would be above the baseline — owed to this client —
    // yet leave no bump for it to wake on. The entry would then sit unsent until some later
    // append, tick, or the cadence timer, which is exactly the "local entries surface immediately"
    // promise below broken. Subscribing first can only cost a spurious first wake, which drains
    // nothing and is free.
    let mut appends = journal_net.journal_changes();
    let mut ticks = journal_net.tick_changes();

    // A plain connect is live-only: take the position a baseline cursor read would answer with and
    // emit nothing for it. That is upstream's contract too — v1 never replays on connect, and a
    // client that wants history polls the read. A reconnect instead starts from the presented
    // token, so its first drain IS the catch-up, which is where zero-loss comes from.
    let (mut cursor, mut drain_now) = match resume {
        Some(cursor) => (cursor, true),
        None => (journal_net.merge_cached_since(port, None).next, false),
    };

    let hello = serde_json::json!({
        "engineVersion": rift_cluster_base::version(),
        "types": ["requests"],
        "port": port,
        "clusterTailLatencyMs": u64::try_from(tail_latency.as_millis()).unwrap_or(u64::MAX),
        "cursor": cursor.encode(),
    });

    let (mut tx, body) = Channel::<Bytes, hyper::Error>::new(STREAM_CHANNEL_BUFFER);

    tokio::spawn(async move {
        if tx
            .send_data(sse_frame("hello", None, &hello))
            .await
            .is_err()
        {
            return;
        }
        let mut heartbeat = tokio::time::interval(STREAM_HEARTBEAT);
        heartbeat.tick().await; // the immediate first tick, consumed so no ping fires now
        // A floor under how often peer entries are looked for, even if no tick ever reports having
        // merged anything new — the declared latency has to hold whether or not the cache moved.
        let mut cadence = tokio::time::interval(tail_latency);
        cadence.tick().await;
        // Only transitions are announced, so a healthy stream stays silent and a degraded one says
        // so exactly once until it recovers.
        let mut declared_partial = false;

        loop {
            if drain_now {
                drain_now = false;
                let page = journal_net.tail_page(port, &cursor);

                if page.truncated {
                    // Upstream's `lagged` means "there is a gap; reconcile by polling", which is
                    // precisely what truncation means here — entries this reader had not reached
                    // were dropped by retention. Reusing the event name keeps one vocabulary.
                    let frame = sse_frame(
                        "lagged",
                        None,
                        &serde_json::json!({ "truncated": true, "cursor": page.next.encode() }),
                    );
                    if tx.send_data(frame).await.is_err() {
                        return;
                    }
                }
                if page.partial != declared_partial {
                    declared_partial = page.partial;
                    let frame = sse_frame(
                        "partial",
                        None,
                        &serde_json::json!({ "partial": declared_partial }),
                    );
                    if tx.send_data(frame).await.is_err() {
                        return;
                    }
                }

                // Emission order and the per-event token both come from `tail_page`: they are
                // one rule (a token is only sound over a per-shard seq-ascending sequence) and it
                // lives with the walk, not here.
                for event in &page.events {
                    let entry = &event.entry;
                    let id = event.id.encode();

                    let mut data = serde_json::json!({
                        "port": port,
                        "flowId": entry.flow_id,
                        "request": entry.request,
                    });
                    if entry.node_id == this_node {
                        // Parity with the proxied single-node stream, which carries the local
                        // journal index. Withheld for peer entries on purpose: that seq is a
                        // position in *another* shard, and this field is what a client would hand
                        // back as a legacy scalar `?since=`, where it would be read as ours.
                        data["index"] = serde_json::json!(entry.seq);
                    }
                    if tx
                        .send_data(sse_frame("request", Some(&id), &data))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                // Adopt the page token even when the drain emitted nothing: it additionally covers
                // ranges the shards no longer hold (evicted, or dropped by a clear generation), so
                // a cursor left at the running fold would re-examine and re-reject them on every
                // wake for the life of the connection.
                cursor = page.next;
                tokio::time::sleep(STREAM_DRAIN_DEBOUNCE).await;
                continue;
            }

            tokio::select! {
                // A local append is visible immediately; this is what keeps single-node latency
                // indistinguishable from upstream's.
                result = appends.changed() => {
                    if result.is_err() {
                        return; // the journal is gone: the node is shutting down
                    }
                    drain_now = true;
                }
                // A tick that merged something new — how peer entries arrive.
                result = ticks.changed() => {
                    if result.is_err() {
                        return;
                    }
                    drain_now = true;
                }
                _ = cadence.tick() => drain_now = true,
                _ = heartbeat.tick() => {
                    if tx.send_data(Bytes::from_static(b": ping\n\n")).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    match Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        // Defeat proxy/response buffering so events reach the client immediately — upstream sets
        // the same header for the same reason.
        .header("X-Accel-Buffering", "no")
        .body(body.boxed())
    {
        Ok(response) => response,
        // Unreachable in practice (a static status and three static headers), but this file's rule
        // is that a failure surfaces rather than becoming a wrong-status 200.
        Err(e) => internal(&format!("building the savedRequests stream response: {e}")),
    }
}

/// `GET /admin/requests/stream` (issue #362) — the live fleet journal.
///
/// Structurally `terminate_stream_saved_requests` with a fleet cursor and a port set instead of one
/// port, deliberately: the two are the same walk, and keeping the shapes identical is what makes
/// "the `id:` after an event is the token a simultaneous fleet read would return" true by
/// construction. Everything load-bearing there is load-bearing here for the same reasons — the
/// subscribe-before-baseline ordering, the debounce, the declared `clusterTailLatencyMs`, and the
/// `partial`/`lagged` vocabulary.
///
/// One event is new. `coverage` is emitted in `hello` and again whenever the covered set changes,
/// because the cap is dynamic: a port that goes quiet can be displaced by one that wakes up, and a
/// client whose view silently narrowed would have no way to know. Announced on change only, so a
/// steady fleet stays quiet.
fn terminate_stream_fleet_requests(
    state: &Arc<FrontState>,
    tenant: &TenantId,
    headers: &hyper::HeaderMap,
) -> Response<FrontBody> {
    let resume = match headers.get("last-event-id") {
        None => None,
        Some(value) => {
            let Ok(raw) = value.to_str() else {
                return typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    "Last-Event-ID is not readable ASCII; expected a fleet cursor token",
                );
            };
            match FleetCursor::decode(raw) {
                Ok(cursor) => Some(cursor),
                Err(e) => {
                    return typed_error(
                        StatusCode::BAD_REQUEST,
                        ErrorKind::BadData,
                        &format!("Last-Event-ID is not a usable fleet cursor: {e}"),
                    );
                }
            }
        }
    };

    // Resolved once here only to fail fast — a caller whose tenant cannot be read should get a
    // status code, not an SSE stream that dies on its first drain. The value is deliberately NOT
    // carried into the task: see the re-resolution inside the loop.
    if let Err(response) = fleet_ports(state, tenant) {
        return response;
    }

    let journal_net = Arc::clone(&state.journal_net);
    let cap = state.fleet_journal_port_cap;
    let owner = Arc::clone(state);
    let subject = tenant.clone();
    let tail_latency = journal_net.tail_latency();
    let this_node = journal_net.node_id();

    // Subscribed BEFORE the baseline is read, and the order is load-bearing for the reason issue
    // #348 records: `subscribe` marks a receiver as having seen the channel's current value, so a
    // recording landing between the read and the subscribe would be owed to this client yet leave
    // no bump to wake on. Subscribing first can only cost a spurious first wake, which is free.
    let mut appends = journal_net.journal_changes();
    let mut ticks = journal_net.tick_changes();

    // A plain connect is live-only — every covered port adopts its baseline and replays nothing,
    // which is `JoinMode::Live` applied across the set. A reconnect starts from the presented
    // token, so its first drain IS the catch-up, and that is where zero-loss comes from.
    // The set as it stands at connect, for `hello` only. Every drain re-derives its own.
    let ports = fleet_ports(state, tenant).unwrap_or_default();
    let (mut cursor, mut drain_now) = match resume {
        Some(cursor) => (cursor, true),
        None => (
            journal_net
                .fleet_page(&ports, cap, None, JoinMode::Live, IdPolicy::PageOnly)
                .next,
            false,
        ),
    };

    let baseline = journal_net.coverage_for(&ports, cap);
    let hello = serde_json::json!({
        "engineVersion": rift_cluster_base::version(),
        "types": ["requests"],
        "scope": "fleet",
        "clusterTailLatencyMs": u64::try_from(tail_latency.as_millis()).unwrap_or(u64::MAX),
        "cursor": cursor.encode(),
        "coverage": coverage_json(&baseline),
    });
    let mut declared_coverage = baseline;

    let (mut tx, body) = Channel::<Bytes, hyper::Error>::new(STREAM_CHANNEL_BUFFER);

    tokio::spawn(async move {
        if tx
            .send_data(sse_frame("hello", None, &hello))
            .await
            .is_err()
        {
            return;
        }
        let mut heartbeat = tokio::time::interval(STREAM_HEARTBEAT);
        heartbeat.tick().await;
        let mut cadence = tokio::time::interval(tail_latency);
        cadence.tick().await;
        let mut declared_partial = false;

        loop {
            if drain_now {
                drain_now = false;
                // **Re-resolved every drain, never captured.** A stream lives indefinitely and the
                // tenant's imposter set does not: a port can be deleted and the number reissued to
                // another tenant, and an imposter created after the connect belongs in the walk. A
                // set frozen at connect would keep reading a shard this tenant no longer owns —
                // emitting another tenant's recorded requests to a connection that was authorized
                // before the handover — and would never show a new imposter at all. The read path
                // re-resolves per request for the same reason; this is what makes the two agree
                // about what "the tenant's fleet" means at any instant.
                let Ok(ports) = fleet_ports(&owner, &subject) else {
                    // Only reachable when the node is shutting down. Ending the stream is the
                    // honest answer — the client reconnects and gets a status code.
                    return;
                };
                let page = journal_net.fleet_page(
                    &ports,
                    cap,
                    Some(&cursor),
                    // A port that (re-)enters coverage mid-stream starts live: replaying its
                    // history into an open tail would look like a burst of new traffic that never
                    // happened. `coverage` below is what tells the client its view widened.
                    JoinMode::Live,
                    IdPolicy::PerEvent,
                );

                if page.coverage != declared_coverage {
                    declared_coverage = page.coverage.clone();
                    let frame = sse_frame("coverage", None, &coverage_json(&declared_coverage));
                    if tx.send_data(frame).await.is_err() {
                        return;
                    }
                }
                if page.truncated {
                    let frame = sse_frame(
                        "lagged",
                        None,
                        &serde_json::json!({ "truncated": true, "cursor": page.next.encode() }),
                    );
                    if tx.send_data(frame).await.is_err() {
                        return;
                    }
                }
                if page.partial != declared_partial {
                    declared_partial = page.partial;
                    let frame = sse_frame(
                        "partial",
                        None,
                        &serde_json::json!({ "partial": declared_partial }),
                    );
                    if tx.send_data(frame).await.is_err() {
                        return;
                    }
                }

                for event in &page.events {
                    let mut data = fleet_row(event);
                    if event.entry.node_id == this_node {
                        // Parity with the per-imposter stream, and withheld for peer entries for
                        // its reason: that seq is a position in another node's shard, and this is
                        // the field a client would hand back as a legacy scalar `?since=`.
                        data["index"] = serde_json::json!(event.entry.seq);
                    }
                    // An absent id omits the line rather than substituting the page token — see
                    // `FleetTailEvent::id`. Under `PerEvent` it is always present; the fallback
                    // costs a duplicate on reconnect and can never skip an entry.
                    let id = event.id.as_ref().map(FleetCursor::encode);
                    if tx
                        .send_data(sse_frame("request", id.as_deref(), &data))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }

                cursor = page.next;
                tokio::time::sleep(STREAM_DRAIN_DEBOUNCE).await;
                continue;
            }

            tokio::select! {
                result = appends.changed() => {
                    if result.is_err() {
                        return; // the journal is gone: the node is shutting down
                    }
                    drain_now = true;
                }
                result = ticks.changed() => {
                    if result.is_err() {
                        return;
                    }
                    drain_now = true;
                }
                _ = cadence.tick() => drain_now = true,
                _ = heartbeat.tick() => {
                    if tx.send_data(Bytes::from_static(b": ping\n\n")).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    match Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(body.boxed())
    {
        Ok(response) => response,
        Err(e) => internal(&format!(
            "building the fleet savedRequests stream response: {e}"
        )),
    }
}

/// `DELETE /imposters/{port}/requests|savedRequests` **with `?match=`** (issue #223 item 4's
/// original design, B3 — reachable only for the scoped form as of issue #224; see
/// `Terminated::ClearSavedRequests`'s own doc for where the unscoped form went).
///
/// Design decision (issue #223 review, B3), unchanged by #224: a `?match=` clear stays
/// **local-only**, proxied to the local engine exactly as any other proxied write is, and always
/// stamped `Rift-Cluster-Partial: true`. #224 gave the *unscoped* clear a wire format that can
/// carry real fleet-wide meaning (a Raft-committed generation bump) — but the wire a scoped clear
/// would need is a different, harder problem this issue does not solve: `ControlOp::JournalClearGen`
/// carries a `space`, not an arbitrary match predicate, so a `?match=` filter (an SDK's own
/// `flowId`/method/path clause) has nowhere to travel that isn't either dropped or misrepresented
/// as a full-space clear. Committing the wrong, wider thing would be worse than staying local: a
/// client that asked to clear ten of a space's two hundred entries would find all two hundred
/// gone fleet-wide instead. So this stays exactly what B3 shipped it as — a local, honestly
/// partial clear — until a predicate-carrying op is designed for it.
async fn terminate_clear_saved_requests(
    state: &Arc<FrontState>,
    req: Request<Incoming>,
    tenant: &TenantId,
) -> Response<FrontBody> {
    let mut response = proxy(Arc::clone(state), req, Some(tenant)).await;
    // Stamped only for a clear the local engine actually performed: a 404/409/etc. means nothing
    // was cleared on this node, and claiming a scoped-but-honest partial regardless would attach
    // the header to a request that never actually cleared anything.
    if response.status().is_success() {
        set_header(&mut response, HEADER_PARTIAL, "true");
    }
    response
}

/// `DELETE /imposters/{port}/spaces/{flow}` (issue #224): proxy the flow-state teardown exactly
/// as this route always has — it is already clustered via `ClusteredFlowStore`, and #224 does not
/// touch that half — then, only if that teardown actually happened, additionally commit
/// `ControlOp::JournalClearGen { space: Some(flow), .. }` through Raft so every node's own
/// journal starts dropping that space's pre-clear entries too, the space-scoped sibling of what
/// the unscoped `savedRequests` clear now commits for a whole port.
///
/// Ordering mirrors `terminate_clear_saved_requests`'s own "only act on a clear that really
/// happened" rule: a proxy failure means the flow-state store was never touched, so there is
/// nothing for the journal half to record either. The reverse failure — proxy succeeds but the
/// commit does not — is answered as an error rather than swallowed (this file's production rule:
/// a failed commit must surface, never a silent 200), even though the flow-state half has by then
/// already torn down; there is no atomic way to straddle a proxied side effect and a Raft write,
/// and reporting the honest partial failure is better than hiding it behind the proxy's own 200.
async fn terminate_space_teardown(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    req: Request<Incoming>,
    port: u16,
    flow: String,
    tenant: &TenantId,
    principal_id: Option<String>,
) -> Response<FrontBody> {
    let response = proxy(Arc::clone(state), req, Some(tenant)).await;
    if !response.status().is_success() {
        return response;
    }
    let op = ControlOp::JournalClearGen {
        tenant: tenant.clone(),
        port,
        space: Some(flow),
    };
    // `validate` first, like every other write on this front — a refusal here would be this
    // function's own bug (the op is built from an already-authorized, already-proxied request),
    // but the R4 order (validate, park durably, submit) is kept uniform rather than special-cased
    // away for the one write that "shouldn't" need it.
    if let Err(reason) = control::validate(&op) {
        return refusal_response(&reason);
    }
    let op_id = Uuid::new_v4();
    let request = mint(op_id, op, None, principal_id);
    if let Err(e) = node.park_intent(&request) {
        return typed_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::InternalError,
            &format!(
                "flow-state teardown succeeded but the journal clear could not be durably accepted: {e}"
            ),
        );
    }
    let committed = match tokio::time::timeout(WRITE_DEADLINE, node.submit(request)).await {
        Err(_) => {
            node.request_replay();
            let mut error = typed_error(
                StatusCode::GATEWAY_TIMEOUT,
                ErrorKind::Timeout,
                "flow-state teardown succeeded but the journal clear did not commit within the \
                 deadline; parked for replay",
            );
            set_header(&mut error, HEADER_OP_ID, &op_id.to_string());
            return error;
        }
        Ok(Err(NodeError::Unavailable(detail))) => {
            node.request_replay();
            let mut error = typed_error(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorKind::Unavailable,
                &format!(
                    "flow-state teardown succeeded but the journal clear found no quorum/leader \
                     (parked for replay): {detail}"
                ),
            );
            set_header(&mut error, HEADER_OP_ID, &op_id.to_string());
            return error;
        }
        Ok(Err(e)) => {
            node.request_replay();
            let mut error = typed_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorKind::InternalError,
                &format!("flow-state teardown succeeded but the journal clear failed: {e}"),
            );
            set_header(&mut error, HEADER_OP_ID, &op_id.to_string());
            return error;
        }
        Ok(Ok(response)) => response,
    };
    if let Err(e) = node.unpark_intent(&op_id) {
        tracing::error!(%op_id, error = %e, "op terminal but could not unpark");
    }
    if let ControlOutcome::Failed { reason } = &committed.outcome {
        return refusal_response(reason);
    }
    response
}

/// Whether `bindings` carries a `FleetAdmin` grant (issue #288) — the one predicate both the fleet
/// admission gate ([`run_mutation`]) and the fleet-scoped spaces listing ([`terminate_spaces_list`])
/// ask, so the two cannot answer it differently. Mirrors [`authz::decide`]'s own inline fleet check
/// (a `FleetAdmin` binding always names [`FLEET_SCOPE`]) without reaching into that module, which
/// answers a different question — "does this binding set grant `action` in `requested`" — than the
/// narrower "does it hold FleetAdmin at all" these two callers need.
fn fleet_admin(bindings: &[(TenantId, Role)]) -> bool {
    bindings
        .iter()
        .any(|(tenant, role)| *role == Role::FleetAdmin && tenant.as_str() == FLEET_SCOPE)
}

/// The fleet admission gate (RFC-005 §S1, issue #288): a **client-supplied** config that sets
/// `contextScope: "fleet"` crosses every tenant's boundary — one namespace shared by the whole
/// fleet — so only a `FleetAdmin` may admit it. Refused with a `400` naming the requirement,
/// before anything is validated, minted or parked; a batch (`PUT /imposters`) carrying one
/// fleet-scoped config among several is refused whole.
///
/// Called from exactly the places a config comes off the wire — `build_mutation`'s `Create` and
/// `ReplaceAllImposters` arms and `terminate_spec_deploy` — and deliberately **not** from
/// `run_mutation`, which also commits `PutImposter` ops that `put_config_mutation` rebuilt from
/// the *stored* config for an index-addressed stub edit. An Editor replacing a stub on an
/// imposter a `FleetAdmin` admitted is not admitting the knob (their body carries no `flowState`
/// at all), so gating that would refuse them for a key they never sent — while the by-id edits
/// next to it (`PatchStubs`) went through. The gate is about who sets the scope, not who touches
/// the imposter afterwards.
///
/// Skipped under the no-principal bypass: with no principal there is no role to hold, so nothing
/// is gated — the same open-admin-plane reasoning `authorize_action`'s own bypass arm documents.
/// A source pull is the other admission path and is gated in `SourcePuller::pull` itself, where
/// no principal is ever present to hold the role.
// The Err IS the client response — this module's early-return channel.
#[allow(clippy::result_large_err)]
fn refuse_fleet_scope_without_fleet_admin<'a>(
    configs: impl IntoIterator<Item = &'a ImposterConfig>,
    principal_id: Option<&str>,
    bindings: &[(TenantId, Role)],
) -> Result<(), Response<FrontBody>> {
    if principal_id.is_none() || fleet_admin(bindings) {
        return Ok(());
    }
    let sets_fleet = configs
        .into_iter()
        .any(|config| FlowConfig::declared_scope(config) == Some(ContextScope::Fleet));
    if sets_fleet {
        return Err(typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            "flowState.contextScope \"fleet\" requires FleetAdmin: a fleet-scoped context crosses \
             every tenant's boundary",
        ));
    }
    Ok(())
}

/// `GET /imposters/{port}/spaces` (issue #374): the fleet-wide list of correlated-isolation spaces
/// this imposter currently holds live flow-KV entries under.
///
/// Tenant scoping already ran in `authorize_action`'s ownership gate before `terminate` ever
/// dispatched here (the same §8.4 404 every other port-addressed route gets — see
/// `Terminated::SpacesList`'s entry in `addressed_port`), so by the time this runs `port` is known
/// to belong to `tenant`; there is nothing left here to check for existence.
///
/// Unlike [`terminate_space_teardown`], there is no upstream body to proxy and then decorate: the
/// whole response is built from `FlowNet::fleet_spaces`'s fan-out plus the imposter's resolved
/// `durability` knob, both read fresh from this node's applied state.
///
/// A `Tenant`-scoped imposter is served under its own `t<tenant>:` prefix — bounded to the
/// caller's tenant by construction, so there is nothing to refuse. `Imposter` scope is likewise
/// always served. Only `Fleet` scope can still be refused, and one resolution failure:
///
/// - **The scope could not be resolved.** Guessing would enumerate the wrong namespace and report
///   it as complete (see [`imposter_scope`]).
/// - **The scope is `Fleet` and the caller is not `FleetAdmin`.** `ContextScope::Fleet` renders the
///   prefix `f:`, which carries **no tenant component**, and one `FlowNet` shard serves every
///   tenant's imposters on this node. So a `f:` scan matches every fleet-scoped flow in the
///   cluster, whoever created it, and this route would hand one tenant another tenant's flow ids,
///   entry counts and owning nodes. #359's single-space read does not have this problem: it
///   answers about an id the caller already named, whereas this route is precisely what turns
///   "know the id" into "enumerate them". Filtering by tenant is not available — the `f:` key
///   records none — so this fails closed and says why, which is the standing rule for a
///   classifier that cannot establish its boundary. RFC-005 §S1's `FleetAdmin` gate (issue #288)
///   is exactly what narrows this: a `FleetAdmin` binding — whose whole role is to cross every
///   tenant's boundary — is served the real fleet-wide list instead ([`fleet_admin`]).
///
/// Both refusals fold into `partial` rather than into a wrong list, because for an enumeration the
/// scope is not a detail of the answer — it *is* the query.
async fn terminate_spaces_list(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    port: u16,
    tenant: &TenantId,
    bindings: &[(TenantId, Role)],
) -> Response<FrontBody> {
    let scope = imposter_scope(node, tenant, port);
    let (prefix, unavailable): (Option<String>, Option<&str>) = match scope {
        None => (None, Some("scope-unresolved")),
        Some(ContextScope::Imposter) => (
            Some(ContextScope::Imposter.prefix_for(Some(port), None)),
            None,
        ),
        Some(ContextScope::Tenant) => (
            Some(ContextScope::Tenant.prefix_for(Some(port), Some(tenant.as_str()))),
            None,
        ),
        Some(ContextScope::Fleet) if fleet_admin(bindings) => {
            (Some(ContextScope::Fleet.prefix_for(Some(port), None)), None)
        }
        Some(ContextScope::Fleet) => (None, Some("fleet-scope")),
    };

    // Nothing is enumerated at all when the scope is unusable — not even under the `Imposter`
    // default. A `f:` imposter scanned as `i{port}:` finds nothing and would look definitively
    // empty; scanning `f:` for real would leak. Refusing is the only answer that is neither.
    let (rows, partial) = match &prefix {
        None => (Vec::new(), true),
        Some(prefix) => {
            state
                .flow_net
                .fleet_spaces(port, prefix, JOURNAL_PEER_BUDGET)
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
    if let Some(knobs) = flow_state_resolved(state, tenant, port) {
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
    tenant: &TenantId,
    mut mutation: Mutation,
    // Whether this is `PUT /imposters`'s whole-array replace — the one route whose script errors
    // are labelled `imposters[{idx}]` (see `batch_indices` below). Passed by the caller that still
    // holds the `Terminated` kind; the spec writes, which build their own `Mutation`, are never
    // batches.
    is_batch: bool,
    auth: Option<&str>,
    host: Option<&HeaderValue>,
    idempotency: Option<&str>,
    if_match: Option<&str>,
    principal_id: Option<String>,
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
    // Which ports the edit-time spec-warnings read below (issue #278) must check — computed here
    // because `mutation.ops` is moved into `requests` two lines down.
    let spec_warning_ports = spec_warning_ports(&mutation.ops);
    let requests: Vec<ControlRequest> = mutation
        .ops
        .into_iter()
        .enumerate()
        .map(|(index, op)| {
            // A deploy commits `[PutImposter, SpecBind]` under one `If-Match`; the precondition is
            // checked once, on the imposter write in front, and `SpecBind` has no target by design
            // (`control::precondition_target` — see its own doc). Minting `expected_revision` for
            // it would refuse the deploy inside `apply` for a precondition the client never named
            // against that record. Deliberately narrowed to `SpecBind` and not to "every op without
            // a target": the journal clears (`JournalClearGen`, `ProxyRecordedClear`) also have no
            // target but *do* reach here with an `If-Match`, and their committed refusal (a `409`
            // saying preconditions do not apply) is the contract a client conditioning a
            // destructive clear relies on — dropping the header for them would apply the clear
            // unconditionally and silently.
            let op_revision =
                expected_revision.filter(|_| !matches!(op, ControlOp::SpecBind { .. }));
            mint(
                op_id_for(base, index, total),
                op,
                op_revision,
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
                // #438, inside the spawned task rather than before it: the
                // caller already holds its 202, so the fan-out must not be
                // awaited on the request path or the async contract changes.
                // A shortfall leaves the intent parked for the replay loop,
                // exactly as a failed submit does below.
                let submitted = match fan_out_then_submit(&node, request, |r| node.submit(r)).await
                {
                    Ok(submitted) => submitted,
                    Err(reason) => {
                        tracing::warn!(%op_id, %reason, "async blob fan-out short of quorum; intent stays parked");
                        node.request_replay();
                        return;
                    }
                };
                match submitted {
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
        // #438, synchronous branch: same rule as the async one above and as the
        // tenancy seam — fan out inside the parked window, and on a shortfall
        // refuse with the op id rather than unparking.
        let submitted = match fan_out_then_submit(node, request, |r| {
            tokio::time::timeout(WRITE_DEADLINE, node.submit(r))
        })
        .await
        {
            Ok(submitted) => submitted,
            Err(reason) => {
                node.request_replay();
                let mut response = typed_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    ErrorKind::Unavailable,
                    &format!("blob not durable enough to commit (parked for replay): {reason}"),
                );
                response
                    .headers_mut()
                    .insert("retry-after", HeaderValue::from_static("1"));
                set_header(&mut response, HEADER_OP_ID, &base.to_string());
                return Err(response);
            }
        };
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

    // Edit-time spec validation (RFC-004 §3.2, issue #278): read *after* the barrier, from this
    // node's own applied state — the barrier already waited for the local apply, so what
    // `spec_warnings` reads back here is guaranteed to be what this write just committed, not
    // whatever was there before it.
    //
    // Compiling an OpenAPI document is CPU work — up to 4 MiB of YAML per bound port — so it runs
    // off the request task rather than stalling this worker's other connections behind a write
    // that has already committed. A join failure (the blocking pool refusing or a panic inside the
    // compiler) is logged and yields no warnings: this pass may never turn a committed write into
    // an error, and "no header" is the only response shape it is allowed to degrade to.
    if !spec_warning_ports.is_empty() {
        let (node, tenant) = (Arc::clone(node), tenant.clone());
        let spec_violations = match tokio::task::spawn_blocking(move || {
            spec_warnings(&node, &tenant, &spec_warning_ports)
        })
        .await
        {
            Ok(violations) => violations,
            Err(e) => {
                tracing::error!(error = %e, "edit-time spec validation did not run");
                Vec::new()
            }
        };
        if let Some(value) = spec_warnings_header(&spec_violations) {
            set_header(&mut response, HEADER_SPEC_WARNINGS, &value);
        }
    }
    Ok(response)
}

/// The most entries a `Rift-Spec-Warnings` value carries before it says `+N more`, and the most
/// bytes it may occupy: intermediaries commonly cap a response's header block at 8 KiB, and a
/// warning that turned a successful write into a proxy's `502` would defeat its own purpose.
const SPEC_WARNINGS_MAX_ENTRIES: usize = 10;
const SPEC_WARNINGS_MAX_BYTES: usize = 2048;

/// Render `violations` as the `Rift-Spec-Warnings` value, or `None` when there is nothing to say.
///
/// Visible ASCII only — a violation's detail echoes JSON values back, so anything outside the
/// printable range is replaced rather than rejected (losing a byte of fidelity in a warning beats
/// refusing a write that already committed) — capped at [`SPEC_WARNINGS_MAX_ENTRIES`] entries and
/// [`SPEC_WARNINGS_MAX_BYTES`] bytes, with an explicit `; +N more` / `…` so a truncated value
/// never reads as the whole story.
fn spec_warnings_header(violations: &[String]) -> Option<String> {
    if violations.is_empty() {
        return None;
    }
    let mut value = violations
        .iter()
        .take(SPEC_WARNINGS_MAX_ENTRIES)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join("; ");
    if violations.len() > SPEC_WARNINGS_MAX_ENTRIES {
        value.push_str(&format!(
            "; +{} more",
            violations.len() - SPEC_WARNINGS_MAX_ENTRIES
        ));
    }
    let mut value: String = value
        .chars()
        .map(|c| if (' '..='~').contains(&c) { c } else { '?' })
        .collect();
    if value.len() > SPEC_WARNINGS_MAX_BYTES {
        // ASCII by now, so a byte index is a char index and the cut cannot split a character.
        value.truncate(SPEC_WARNINGS_MAX_BYTES - 3);
        value.push_str("...");
    }
    Some(value)
}

/// `GET /admin/sources` / `GET /admin/sources/{id}` (issue #239).
///
/// The response deliberately keeps two kinds of fact in two shapes (D-31):
///
/// - **`sources` / `source`** is the replicated projection, verbatim. Every
///   converged node answers it byte-identically, and `RedbStateMachine::
///   sources` documents that diffing two nodes' answers is how an operator
///   checks a `SourcePut` has converged — a property that survives only if
///   nothing node-local is ever mixed into this half.
/// - **`nodeLocal`** carries what is true of *this node only*: which node
///   answered, and the last poll error per source. A poll failure is
///   deliberately not replicated (`scheduler.rs::PollStatus`), so it can
///   differ across the fleet — flattening it into the record would tell an
///   operator the fleet is failing when one node is, or that it is healthy
///   when only the node they happened to hit is.
///
/// The cluster-port `read_source` (sources/mod.rs) *does* flat-merge
/// `lastPollError`, and that is not a contradiction: there the caller
/// addressed one node explicitly, so "this node's view" is the question being
/// asked. This surface sits behind one fleet address, where it is not.
fn terminate_sources(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    id: Option<&str>,
    tenant: &TenantId,
) -> Response<FrontBody> {
    let view = match id {
        Some(id) => {
            match node.source(tenant.as_str(), id) {
                Ok(Some(record)) => SourcesView::One(record),
                // Byte-identical to the cross-tenant refusal (RFC-002 §8.4):
                // a source that exists in another tenant must be
                // indistinguishable from one that never existed.
                Ok(None) => return tenant_boundary_not_found(),
                Err(e) => return internal(&e.to_string()),
            }
        }
        None => match node.sources(tenant.as_str()) {
            Ok(records) => SourcesView::List(records),
            Err(e) => return internal(&e.to_string()),
        },
    };
    // The poll-error lookup carries the authorized tenant: source ids are only
    // unique within a tenant, and a bare-id lookup would answer with another
    // tenant's failure string — see `PollStatus::last_error`'s key doc.
    let body = match render_sources(node.id(), &view, |id| {
        state.puller.last_poll_error(tenant.as_str(), id)
    }) {
        Ok(body) => body,
        Err(e) => return internal(&format!("rendering sources: {e}")),
    };
    buffered_response(StatusCode::OK, Bytes::from(body), json_content_type())
        .unwrap_or_else(|response| response)
}

/// Which of the two response shapes a sources read renders.
enum SourcesView {
    List(Vec<rift_cluster::SourceRecord>),
    One(rift_cluster::SourceRecord),
}

impl SourcesView {
    fn records(&self) -> &[rift_cluster::SourceRecord] {
        match self {
            SourcesView::List(records) => records,
            SourcesView::One(record) => std::slice::from_ref(record),
        }
    }
}

/// Render the two-part sources body — see [`terminate_sources`] for why the
/// parts must stay apart. The poll-error lookup is injected so the separation
/// is unit-testable without a bound front or a live puller.
fn render_sources(
    node_id: rift_cluster::NodeId,
    view: &SourcesView,
    last_poll_error: impl Fn(&str) -> Option<String>,
) -> serde_json::Result<Vec<u8>> {
    let poll_errors: serde_json::Map<String, serde_json::Value> = view
        .records()
        .iter()
        .filter_map(|record| {
            last_poll_error(&record.id).map(|error| (record.id.clone(), error.into()))
        })
        .collect();
    let node_local = serde_json::json!({
        // A STRING, for the reason `cluster_api::node_id` documents at length: a `NodeId` is a
        // `u64`, JSON numbers are IEEE-754 doubles wherever the reader is JavaScript, and every id
        // above 2^53-1 arrives silently rounded. #332 fixed `/_fleet/*` and `/_cluster/*`; this
        // endpoint carries the same id and was missed, so it went on reporting a node that does
        // not exist.
        "nodeId": node_id.to_string(),
        "pollErrors": serde_json::Value::Object(poll_errors),
    });
    let body = match view {
        SourcesView::List(records) => {
            serde_json::json!({ "sources": records, "nodeLocal": node_local })
        }
        SourcesView::One(record) => {
            serde_json::json!({ "source": record, "nodeLocal": node_local })
        }
    };
    serde_json::to_vec(&body)
}

/// Which of the three source-write routes (issue #253) `terminate_source_write` is serving.
///
/// Carved out of `Terminated` at the `terminate` call site rather than matched there directly, so
/// the match inside `terminate_source_write` is exhaustive with no wildcard arm — the same reason
/// `action_for` has none. A route this function was never meant to receive is a compile error
/// here, not a `_ => unreachable!()` waiting to fire in production.
enum SourceWrite {
    Put,
    Delete(String),
    Pull(String),
}

/// Serve one of the three RBAC'd source write routes (issue #253): `POST /admin/sources`,
/// `DELETE /admin/sources/{id}`, `POST /admin/sources/{id}/pull`.
///
/// Follows `terminate_tenancy`'s shape — a direct control-plane submit with an `Rift-Cluster-Op-Id`
/// header on the response — rather than `build_and_run`'s: that path is built around a single
/// *imposter* record (`If-Match` against a port, `_rift.script` resolution, a post-commit re-read
/// of the loopback admin), and none of it applies to a source, which upstream has no concept of at
/// all. There is deliberately no `If-Match` here: a source row carries no revision surface of its
/// own to condition on (`SourceRecord.revision` is *when it last wrote*, not something a client
/// hands back as a precondition), and `SourcePut` is an idempotent upsert by id rather than a
/// read-modify-write, so there is no lost update for a precondition to guard against.
///
/// All three commit through [`SourcePuller`], which does the actual parse -> validate -> submit ->
/// read-back work identically for the cluster port's default-tenant twin of each route (see its own
/// doc) — this function only supplies the tenant `authorize_action` already resolved and renders
/// the result as an HTTP response.
async fn terminate_source_write(
    state: &Arc<FrontState>,
    req: Request<Incoming>,
    kind: SourceWrite,
    principal_id: Option<String>,
    tenant: &TenantId,
) -> Response<FrontBody> {
    // Collected unconditionally, like every other terminated write in this file (`terminate`'s own
    // body read above `build_and_run`, `terminate_tenancy`'s) — `Delete` and `Pull` carry no body of
    // their own, but draining it here rather than leaving it unread is what keeps a client that sent
    // one anyway from stalling the connection's keep-alive.
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

    match kind {
        SourceWrite::Put => {
            let (record, op_id) = match state.puller.put(tenant.clone(), &body, principal_id).await
            {
                Ok(ok) => ok,
                Err(e) => return source_write_error(e),
            };
            let payload = match serde_json::to_vec(&record) {
                Ok(payload) => payload,
                Err(e) => return internal(&e.to_string()),
            };
            let mut response = match buffered_response(
                StatusCode::OK,
                Bytes::from(payload),
                json_content_type(),
            ) {
                Ok(response) | Err(response) => response,
            };
            set_header(&mut response, HEADER_OP_ID, &op_id.to_string());
            response
        }
        SourceWrite::Delete(id) => {
            let (revision, op_id) =
                match state.puller.delete(tenant.clone(), &id, principal_id).await {
                    Ok(ok) => ok,
                    Err(e) => return source_write_error(e),
                };
            let payload = serde_json::json!({ "revision": revision }).to_string();
            let mut response = match buffered_response(
                StatusCode::OK,
                Bytes::from(payload),
                json_content_type(),
            ) {
                Ok(response) | Err(response) => response,
            };
            set_header(&mut response, HEADER_OP_ID, &op_id.to_string());
            response
        }
        SourceWrite::Pull(id) => {
            let report = match state.puller.pull(tenant.as_str(), &id, principal_id).await {
                Ok(report) => report,
                Err(e) => return source_write_error(e),
            };
            let payload = match serde_json::to_vec(&report) {
                Ok(payload) => payload,
                Err(e) => return internal(&e.to_string()),
            };
            // No `Rift-Cluster-Op-Id` here, unlike the two arms above: `pull` is the
            // pre-existing cluster-port method (kept byte-for-byte so its behaviour towards its
            // existing caller is unchanged — issue #253's explicit constraint), and it does not
            // surface the op id it minted internally for the write it committed. A client that
            // needs to correlate a pull has the report's own `revision`; the `Unavailable` error
            // path below still names an op id to poll, from `PullError` itself.
            match buffered_response(StatusCode::OK, Bytes::from(payload), json_content_type()) {
                Ok(response) | Err(response) => response,
            }
        }
    }
}

/// Map a [`PullError`] from one of the three source-write routes onto the front's status classes —
/// the cluster port's own `pull_error` (`rift-cluster`'s `sources` module) draws the identical
/// distinction, reproduced here because this front renders `Response<FrontBody>`, not `RpcError`,
/// and putting a dependency on the front's HTTP types into `rift-cluster` (a crate with no `hyper`
/// dependency at all) to share one function would be the wrong direction for it.
fn source_write_error(e: PullError) -> Response<FrontBody> {
    match e {
        // Byte-identical to `terminate_sources`'s own cross-tenant/absent 404 (RFC-002 §8.4): a
        // pull of an id that does not exist in the caller's tenant must not be distinguishable from
        // one that belongs to someone else. `put`/`delete` never produce this variant — an upsert
        // has no "unknown" case, and a delete is idempotent when absent (`SourcePuller::delete`'s
        // own doc) — only `pull` reads the record first and can find nothing there.
        PullError::UnknownSource(_) => tenant_boundary_not_found(),
        PullError::BadRequest(detail) => refusal_response(&detail),
        // The fetch itself failed against the source's own host — the upstream's fault, not this
        // cluster's, and worth retrying. Mirrors `sources::pull_error`'s identical reasoning for the
        // cluster port.
        PullError::Fetch { id, detail } => typed_error(
            StatusCode::BAD_GATEWAY,
            ErrorKind::UpstreamFailure,
            &format!("fetching source {id:?}: {detail}"),
        ),
        PullError::Unavailable { detail, op_id } => {
            let mut response = typed_error(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorKind::Unavailable,
                &detail,
            );
            set_header(&mut response, HEADER_OP_ID, &op_id.to_string());
            response
        }
        PullError::Internal(detail) => internal(&detail),
    }
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
/// addressing input is the `{port}` in the route, which [`addressed_port`] has already proven
/// belongs to the caller's tenant. Since issue #344 the exchange is dispatched in-process, not
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

/// The TCP-fault names/aliases the engine's carrier response names via its `x-rift-fault` header,
/// when a stub's `_rift.fault.tcp` would have aborted the connection instead of sending a real
/// response (issue #344).
///
/// Mirrors `rift_mock_core::imposter::fault_io::TcpFaultKind::parse` — which is `pub(crate)`
/// upstream, so this header is the only observable of that classification available outside the
/// crate, and the alias list is duplicated here rather than imported. A name this list has not
/// caught up with (a future upstream alias) degrades gracefully rather than breaking: the carrier
/// response then renders as the imposter's own answer, which is still truthful — that response
/// really did come back from the engine — just less explicit than naming the fault.
///
/// Two known edges of this header-based detection, stated rather than hidden. **A v2 script's
/// `reset()`** (upstream #357) builds its carrier without the `x-rift-fault` header — only the
/// `TcpFaultKind` extension, which is not visible from here — so that one shape renders as the
/// carrier itself: `status: 502`, empty body, `x-rift-script` header. The fix is upstream —
/// achird-labs/rift#965: either stamp the header on that third carrier, or make the
/// classification a seam so `RaftNode::dispatch_to_imposter` can turn the extension into the
/// header itself *before* the response crosses the in-memory connection (extensions do not
/// survive serialization; only there is it still attached). Until it lands the OpenAPI names the
/// gap. And the reverse: a stub is free to
/// set `x-rift-fault: reset` on an ordinary response the wire would send — self-inflicted, and
/// reported here as the fault it claims to be.
const TCP_FAULT_NAMES: &[&str] = &[
    "reset",
    "CONNECTION_RESET_BY_PEER",
    "empty",
    "EMPTY_RESPONSE",
    "garbage",
    "random",
    "RANDOM_DATA_THEN_CLOSE",
    "malformed",
    "MALFORMED_RESPONSE_CHUNK",
];

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

    // The server half is spawned, and its handle is **kept**: the budget below cancels only the
    // future it wraps, and the dispatch — the imposter's own handler, `wait` behaviour and all —
    // runs inside this task. Left detached, a stub slower than the budget would outlive the
    // request that started it, one orphaned task per try, unbounded and unlogged; the admin
    // front's own accept loop refuses to orphan a task for the same reason. Aborted once the
    // exchange has ended, whichever way it ended.
    let server = {
        let dispatch = Arc::clone(&dispatch);
        let gone = Arc::clone(&gone);
        AbortOnDrop::new(tokio::spawn(async move {
            let service = service_fn(move |req: Request<Incoming>| {
                let dispatch = Arc::clone(&dispatch);
                let gone = Arc::clone(&gone);
                async move {
                    let taken = dispatch.lock().expect("dispatch lock poisoned").take();
                    let Some(dispatch) = taken else {
                        return Err(TryExchangeEnded);
                    };
                    match dispatch(req).await {
                        Some(response) => Ok(response),
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
        // answer, whatever its (fabricated) status or body say.
        if let Some(fault) = response
            .headers()
            .get("x-rift-fault")
            .and_then(|value| value.to_str().ok())
            .filter(|name| TCP_FAULT_NAMES.contains(name))
        {
            return Err(TryFailure::Fault(fault.to_owned()));
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
/// The tenant check is **not** here, and must not be added here: `addressed_port` routes this
/// variant through the ownership gate in [`authorize_action`], so by the time this runs the port
/// is known to be one of `tenant`'s imposters and an unknown or other-tenant port has already
/// answered the fixed §8.4 `404`. A second copy of that rule in this function is a copy free to
/// drift from the one every other port-addressed route is held to.
async fn terminate_try_imposter(
    node: &Arc<RaftNode>,
    req: Request<Incoming>,
    port: u16,
    tenant: &TenantId,
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

    // `get_imposter` is also what makes this safe to read at all: it is keyed by the authorized
    // tenant, so it cannot reach a config the ownership gate would have refused. The read proves
    // the record is this tenant's; it does not by itself prove anything about who answers the
    // port — that is what the gate below is for.
    match node.get_imposter(tenant.as_str(), port) {
        Ok(Some(_)) => {}
        // The ownership gate already passed, so the config disappearing between there and here
        // means it was deleted in between. That is a genuine not-found, and it renders through the
        // same §8.4 body every other refusal on this port does.
        Ok(None) => return tenant_boundary_not_found(),
        Err(e) => return internal(&e.to_string()),
    }

    // **The engine-holds-this-port check, and it is load-bearing.** Everything above proves the
    // caller's tenant owns the imposter *record* on this port. It does not by itself prove that
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

    // An audited export must never be deduplicated (issue #287). `base_op_id` derives the op id
    // from `Idempotency-Key`, and a replayed op id short-circuits in the state machine *above* the
    // audit write — so a second keyed request would serve the bytes again and record nothing.
    // Dedup keys on the op id alone, never on the op's content, so the replay need not even name
    // the same dataset: one recorded read would buy `DEDUP_TTL_SECS` (24h) of unrecorded exports
    // of everything the caller can reach.
    //
    // A fresh id per request instead of refusing the header: a read has no state to make
    // idempotent, so there is no retry semantics worth preserving, and each export genuinely is a
    // separate event that has to leave its own trace. Recording twice is harmless; recording once
    // for two exports is the failure this op exists to prevent.
    let is_audited_export = matches!(route, tenancy::Route::DatasetContent(..));

    // Parsed here, where both the headers and the query are in hand — `classify` sees only the
    // path and query, and `dispatch` sees neither.
    let upload = if matches!(route, tenancy::Route::DatasetUpload(_)) {
        match tenancy::DatasetUploadMeta::parse(
            |name| {
                req.headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned)
            },
            req.uri().query(),
        ) {
            Ok(meta) => Some(meta),
            Err(reason) => {
                return typed_error(StatusCode::BAD_REQUEST, ErrorKind::BadData, &reason);
            }
        }
    } else {
        None
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

    let outcome = match tenancy::dispatch(
        node,
        route,
        &body,
        authorized_tenant,
        bindings,
        upload,
        state.export_status.as_deref(),
        &state.flow_net,
    )
    .await
    {
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

    let (op, status, rendered, rendered_content_type) = match outcome {
        tenancy::Outcome::Body {
            status,
            body,
            partial,
        } => {
            let mut response = buffered_response(status, Bytes::from(body), json_content_type())
                .unwrap_or_else(|response| response);
            if partial {
                set_header(&mut response, HEADER_PARTIAL, "true");
            }
            return response;
        }
        tenancy::Outcome::Commit {
            op,
            status,
            then,
            then_content_type,
        } => (op, status, then, then_content_type),
    };

    // The same order every write on this front follows (R4): validate, park
    // durably, submit. Parking before submitting is what makes an accepted op
    // survive a crash — the replay loop finishes what this request cannot.
    if let Err(reason) = control::validate(&op) {
        return refusal_response(&reason);
    }
    let op_id = if is_audited_export {
        uuid::Uuid::new_v4()
    } else {
        base_op_id(idempotency.as_deref())
    };
    let request = tenancy::mint_request(op, principal_id, op_id);
    if let Err(e) = node.park_intent(&request) {
        return typed_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::InternalError,
            &format!("cannot durably accept the write: {e}"),
        );
    }

    // #438: the bytes reach a quorum before the op that references them is
    // proposed, so a commit implies the blob is quorum-durable — the guarantee
    // the log itself used to provide. Deliberately inside the parked window and
    // deliberately *not* unparked on failure: the replay loop owns the intent
    // from here and re-runs the fan-out.
    // The pin lives inside `fan_out_then_submit`, which owns the submit — nothing
    // references the blob until the op it carries commits, and no caller can drop
    // a guard it was never handed (#439).
    let submitted = match fan_out_then_submit(node, request, |r| {
        tokio::time::timeout(WRITE_DEADLINE, node.submit(r))
    })
    .await
    {
        Ok(submitted) => submitted,
        Err(reason) => {
            node.request_replay();
            let mut response = typed_error(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorKind::Unavailable,
                &format!("blob not durable enough to commit (parked for replay): {reason}"),
            );
            response
                .headers_mut()
                .insert("retry-after", HeaderValue::from_static("1"));
            set_header(&mut response, HEADER_OP_ID, &op_id.to_string());
            return response;
        }
    };

    let committed = match submitted {
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
    } else if let Some(declared) = rendered_content_type {
        HeaderValue::from_static(declared).into()
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

/// The tenant's spec `id` from local applied state, or the client response that says why not:
/// the §8.4-shaped `404` when there is no such record (a cross-tenant id is simply absent —
/// `node.spec` is keyed `(tenant, id)`), `500` on a storage read failure. The one lookup every
/// spec route that names an id starts with.
#[allow(clippy::result_large_err)]
fn spec_record_or_404(
    node: &Arc<RaftNode>,
    tenant: &TenantId,
    id: &str,
) -> Result<rift_cluster::SpecRecord, Response<FrontBody>> {
    match node.spec(tenant.as_str(), id) {
        Ok(Some(record)) => Ok(record),
        Ok(None) => Err(typed_error(
            StatusCode::NOT_FOUND,
            ErrorKind::NoSuchResource,
            &format!("no spec {id:?}"),
        )),
        Err(e) => Err(internal(&e.to_string())),
    }
}

/// The document a spec record's digest names. The record and its blob are two tables
/// (`raft::store`'s `sm_specs` / `sm_spec_blobs`); a record whose digest names no blob would mean
/// apply upserted one without the other, which its `SpecPut` arm makes impossible — answered as a
/// `500`, never silently, so a bug there is visible instead of serving a spec with no text.
#[allow(clippy::result_large_err)]
fn spec_document_or_500(
    node: &Arc<RaftNode>,
    record: &rift_cluster::SpecRecord,
) -> Result<String, Response<FrontBody>> {
    match node.spec_document(&record.digest) {
        Ok(Some(document)) => Ok(document),
        Ok(None) => Err(internal(&format!(
            "spec {:?} has no stored document for digest {:?}",
            record.id, record.digest
        ))),
        Err(e) => Err(internal(&e.to_string())),
    }
}

/// A JSON success body. `buffered_response`'s `Err` is itself a client response (an oversize
/// body), so either arm is the answer.
fn json_ok(status: StatusCode, body: &serde_json::Value) -> Response<FrontBody> {
    match buffered_response(status, Bytes::from(body.to_string()), json_content_type()) {
        Ok(response) | Err(response) => response,
    }
}

/// `GET /specs` (`id: None`) / `GET /specs/{id}` (`id: Some`) — RFC-004 §4.3, issue #278.
///
/// Both answer from local applied state, exactly like [`terminate_sources`]: a spec is a
/// replicated projection, so every converged node answers identically and there is no leadership
/// requirement. A single-spec read that finds no record — whether the id never existed or names
/// another tenant's spec — renders the identical §8.4 404: `node.spec` is already keyed
/// `(tenant, id)`, so a cross-tenant id is simply absent, with nothing extra to check.
fn terminate_spec_read(
    node: &Arc<RaftNode>,
    tenant: &TenantId,
    id: Option<&str>,
) -> Response<FrontBody> {
    let Some(id) = id else {
        let specs = match node.specs(tenant.as_str()) {
            Ok(specs) => specs,
            Err(e) => return internal(&e.to_string()),
        };
        return json_ok(StatusCode::OK, &serde_json::json!({ "specs": specs }));
    };
    let record = match spec_record_or_404(node, tenant, id) {
        Ok(record) => record,
        Err(response) => return response,
    };
    let document = match spec_document_or_500(node, &record) {
        Ok(document) => document,
        Err(response) => return response,
    };
    let mut body = match serde_json::to_value(&record) {
        Ok(body) => body,
        Err(e) => return internal(&e.to_string()),
    };
    if let serde_json::Value::Object(map) = &mut body {
        map.insert("document".to_owned(), serde_json::Value::String(document));
    }
    json_ok(StatusCode::OK, &body)
}

/// `POST /specs/{id}/compile` (RFC-004 §4.3, issue #278): a dry run. Compiles the stored document
/// — for the port the body names, else the spec's one bound port, else no port at all — and
/// answers with exactly what `deploySpec` would commit, plus a stub-id diff against whatever is
/// currently deployed on the target port. Commits nothing: there is no `Mutation` here at all.
async fn terminate_spec_compile(
    node: &Arc<RaftNode>,
    req: Request<Incoming>,
    id: String,
    tenant: &TenantId,
) -> Response<FrontBody> {
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
    #[derive(Deserialize)]
    struct CompileBody {
        port: Option<u16>,
    }
    // Absent/empty body is fine (RFC-004 §4.3's `requestBody: required: false`) — it just means
    // "no explicit target port".
    let requested_port = if body.is_empty() {
        None
    } else {
        match serde_json::from_slice::<CompileBody>(&body) {
            Ok(parsed) => parsed.port,
            Err(e) => {
                return typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    &format!("compile body: {e}"),
                );
            }
        }
    };
    let record = match spec_record_or_404(node, tenant, &id) {
        Ok(record) => record,
        Err(response) => return response,
    };
    let document = match spec_document_or_500(node, &record) {
        Ok(document) => document,
        Err(response) => return response,
    };
    // Falls back to the spec's one bound port, precisely because that is the only port a diff
    // could mean anything for without the caller having to say so twice; two-or-more bound ports
    // have no single implied target, so the caller must name one.
    let target_port = requested_port.or(match record.ports.as_slice() {
        [port] => Some(*port),
        _ => None,
    });
    let compiled = match compile(
        document.as_bytes(),
        &CompileOptions {
            port: target_port,
            name: Some(id.clone()),
            max_bytes: MAX_SPEC_BYTES,
        },
    ) {
        Ok(compiled) => compiled,
        Err(e) => {
            return typed_error(
                StatusCode::BAD_REQUEST,
                ErrorKind::BadData,
                &format!("spec {id:?} does not compile: {e}"),
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
    let diff = match target_port {
        None => serde_json::Value::Null,
        Some(port) => {
            let deployed = match node.get_imposter(tenant.as_str(), port) {
                Ok(deployed) => deployed,
                Err(e) => return internal(&e.to_string()),
            };
            // The deployed side was stored through `ImposterConfig`'s own (de)serialization — the
            // same round trip `terminate_spec_deploy` sends every compiled imposter through before
            // it is committed (see `normalize_for_diff`'s own doc for why that is not optional
            // here). Comparing the compiler's raw canonical JSON against it directly would report
            // spurious drift on a spec that has not changed at all.
            spec_compile_diff(
                port,
                deployed.as_deref(),
                &normalize_for_diff(&compiled.imposter),
            )
        }
    };
    json_ok(
        StatusCode::OK,
        &serde_json::json!({
            "imposter": compiled.imposter,
            "operations": operations,
            "diff": diff,
        }),
    )
}

/// Round `imposter` through `ImposterConfig`'s own (de)serialization — deserialize, then
/// re-serialize — the same transform every committed `PutImposter` goes through before it is
/// stored (`terminate_spec_deploy` builds its op from exactly this). Without it, a diff comparing
/// the compiler's raw canonical JSON against a *stored* config would see spurious drift on every
/// single stub of an unchanged spec: the engine's own `IsResponseOut` renders `statusCode` as a
/// **string** (`"200"`) for Mountebank wire compatibility, while `rift-cluster-spec` emits a JSON
/// **number** — a real, pre-existing asymmetry in the vendored engine's own serialization, not a
/// bug in either crate. Applying the same round trip to both sides of the comparison neutralizes
/// it, leaving only genuine content differences visible. Falls back to `imposter` unchanged if it
/// somehow does not parse as an `ImposterConfig` (it always should — it is the compiler's own
/// output) — a diff that cannot normalize one side is more honest reporting some noise than none
/// at all.
fn normalize_for_diff(imposter: &serde_json::Value) -> serde_json::Value {
    match serde_json::from_value::<ImposterConfig>(imposter.clone()) {
        Ok(config) => match serde_json::to_value(&config) {
            Ok(normalized) => normalized,
            Err(e) => {
                tracing::error!(error = %e, "compiled imposter did not re-serialize; diff is unnormalized");
                imposter.clone()
            }
        },
        Err(e) => {
            // Loud, because the consequence is not "no diff" but a *wrong* one — every stub reads
            // as `changed` — and nothing else would tell an operator why.
            tracing::error!(error = %e, "compiled imposter is not an ImposterConfig; diff is unnormalized");
            imposter.clone()
        }
    }
}

/// The stub-id-level half of `compileSpec`'s diff (RFC-004 §4.3): `added` — compiled ids the
/// deployed config lacks; `removed` — deployed ids the compiler no longer emits, counting only
/// generated (`spec:`-prefixed) ids, because a hand-added stub was never the compiler's to track;
/// `changed` — ids on both sides whose stub JSON differs. `deployed` is `None` (nothing on the
/// port) when there is nothing to diff against at all.
fn spec_compile_diff(
    port: u16,
    deployed: Option<&str>,
    compiled: &serde_json::Value,
) -> serde_json::Value {
    fn stubs_by_id(imposter: &serde_json::Value) -> HashMap<&str, &serde_json::Value> {
        imposter
            .get("stubs")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|stub| Some((stub.get("id")?.as_str()?, stub)))
            .collect()
    }

    let compiled_stubs = stubs_by_id(compiled);
    let Some(deployed) = deployed else {
        let mut added: Vec<&str> = compiled_stubs.keys().copied().collect();
        added.sort_unstable();
        return serde_json::json!({
            "port": port,
            "deployed": false,
            "added": added,
            "removed": Vec::<&str>::new(),
            "changed": Vec::<&str>::new(),
        });
    };
    let deployed_value: serde_json::Value = match serde_json::from_str(deployed) {
        Ok(value) => value,
        Err(e) => {
            // The stored config always round-trips (it was admitted as JSON in the first place);
            // this is a defensive fallback for a state this crate cannot reach honestly, not an
            // expected path — logged rather than silently treated as "nothing deployed".
            tracing::error!(port, error = %e, "stored imposter config did not parse for a spec diff");
            serde_json::Value::Null
        }
    };
    let deployed_stubs = stubs_by_id(&deployed_value);

    let prefix = format!("{STUB_ID_PREFIX}:");
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    for (id, stub) in &compiled_stubs {
        match deployed_stubs.get(id) {
            None => added.push(*id),
            Some(deployed_stub) => {
                if stub != deployed_stub {
                    changed.push(*id);
                }
            }
        }
    }
    for id in deployed_stubs.keys() {
        if id.starts_with(&prefix) && !compiled_stubs.contains_key(id) {
            removed.push(*id);
        }
    }
    added.sort_unstable();
    removed.sort_unstable();
    changed.sort_unstable();
    serde_json::json!({
        "port": port,
        "deployed": true,
        "added": added,
        "removed": removed,
        "changed": changed,
    })
}

/// `PUT /specs/{id}` (RFC-004 §4.3, issue #278): import or re-import an OpenAPI 3.0 document.
///
/// Compiled on this node **before** anything commits — `compile`'s own refusals (unsupported
/// version, external `$ref`, a parse failure, a self-check failure) become this route's `400`
/// verbatim, so a document that reaches the log has already been shown to compile. An unchanged
/// re-import (same digest already stored under `id`) answers without minting a `Mutation` at all:
/// zero log growth for a retry or a poll.
#[allow(clippy::too_many_arguments)]
async fn terminate_spec_put(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    tenant: &TenantId,
    id: String,
    body: &[u8],
    auth: Option<&str>,
    host: Option<&HeaderValue>,
    idempotency: Option<&str>,
    if_match: Option<&str>,
    principal_id: Option<String>,
) -> Response<FrontBody> {
    let text = match String::from_utf8(body.to_vec()) {
        Ok(text) => text,
        Err(_) => {
            return typed_error(
                StatusCode::BAD_REQUEST,
                ErrorKind::BadData,
                "spec must be UTF-8 text (JSON or YAML)",
            );
        }
    };
    if text.len() > MAX_SPEC_BYTES {
        return typed_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorKind::RequestTooLarge,
            &format!("spec exceeds {MAX_SPEC_BYTES} bytes"),
        );
    }
    if let Err(e) = compile(text.as_bytes(), &CompileOptions::default()) {
        return typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            &format!("spec does not compile: {e}"),
        );
    }
    let digest = SpecDigest::of(text.as_bytes()).to_hex();
    // YAML is a strict superset of JSON, so "parses as JSON" is the sniff: anything that does not
    // is presumed YAML. `compile` above already proved the document parses as one of the two.
    let format = if serde_json::from_str::<serde_json::Value>(&text).is_ok() {
        control::SpecFormat::Json
    } else {
        control::SpecFormat::Yaml
    };
    let existing = match node.spec(tenant.as_str(), &id) {
        Ok(existing) => existing,
        Err(e) => return internal(&e.to_string()),
    };
    if let Some(existing) = &existing
        && existing.digest == digest
    {
        return json_ok(
            StatusCode::OK,
            &serde_json::json!({
                "id": id,
                "digest": digest,
                "format": format,
                "revision": existing.revision,
                "unchanged": true,
            }),
        );
    }
    let status = if existing.is_none() {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let response_body = serde_json::json!({
        "id": id.clone(),
        "digest": digest.clone(),
        "format": format,
        "unchanged": false,
    });
    let mutation = Mutation {
        ops: vec![ControlOp::SpecPut {
            tenant: tenant.clone(),
            id,
            meta: control::SpecMeta {
                format,
                digest: control::Digest::new(digest),
                source: control::SpecSource::Inline,
                size: text.len() as u64,
            },
            document: Some(text),
            origin: 0,
        }],
        port: None,
        render: Render::Captured {
            body: Bytes::from(response_body.to_string()),
            content_type: json_content_type(),
            status,
        },
    };
    match run_mutation(
        state,
        node,
        tenant,
        mutation,
        false,
        auth,
        host,
        idempotency,
        if_match,
        principal_id,
    )
    .await
    {
        Ok(response) | Err(response) => response,
    }
}

/// `DELETE /specs/{id}` (RFC-004 §4.3, issue #278). Refuses with `409` while any port is still
/// bound to the spec, unless `?force` — then every bound port is unbound first (`SpecUnbind`) and
/// the record removed (`SpecDelete`) in the same mutation. Unbinding never tears an imposter down:
/// it keeps serving, it just stops carrying provenance.
#[allow(clippy::too_many_arguments)]
async fn terminate_spec_delete(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    tenant: &TenantId,
    id: String,
    force: bool,
    auth: Option<&str>,
    host: Option<&HeaderValue>,
    idempotency: Option<&str>,
    if_match: Option<&str>,
    principal_id: Option<String>,
) -> Response<FrontBody> {
    let record = match spec_record_or_404(node, tenant, &id) {
        Ok(record) => record,
        Err(response) => return response,
    };
    if !force && !record.ports.is_empty() {
        let ports = record
            .ports
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(",");
        return typed_error(
            StatusCode::CONFLICT,
            ErrorKind::ResourceConflict,
            &format!("spec {id:?} is bound to port(s) {ports}; DELETE with ?force to unbind first"),
        );
    }
    let unbound_ports: Vec<u16> = if force {
        record.ports.clone()
    } else {
        Vec::new()
    };
    let mut ops: Vec<ControlOp> = unbound_ports
        .iter()
        .map(|&port| ControlOp::SpecUnbind {
            tenant: tenant.clone(),
            port,
        })
        .collect();
    ops.push(ControlOp::SpecDelete {
        tenant: tenant.clone(),
        id: id.clone(),
    });
    let response_body = serde_json::json!({
        "id": id,
        "digest": record.digest,
        "unboundPorts": unbound_ports,
    });
    let mutation = Mutation {
        ops,
        port: None,
        render: Render::Captured {
            body: Bytes::from(response_body.to_string()),
            content_type: json_content_type(),
            status: StatusCode::OK,
        },
    };
    match run_mutation(
        state,
        node,
        tenant,
        mutation,
        false,
        auth,
        host,
        idempotency,
        if_match,
        principal_id,
    )
    .await
    {
        Ok(response) | Err(response) => response,
    }
}

/// `POST /specs/{id}/deploy` (RFC-004 §4.3, issue #278): compile the stored document for `port`
/// and commit it as that port's imposter, stamping the spec's provenance in the same barrier —
/// `[PutImposter, SpecBind]`, so park/replay, `Idempotency-Key` dedup, `If-Match` against the
/// imposter's own revision and the write barrier are all inherited from `run_mutation` rather than
/// reimplemented.
///
/// `Action::SpecWrite` alone must not be a back door into imposter mutation (RFC-004 §4.3): a
/// principal also needs `Action::ImposterWrite` on the target port, checked here rather than in
/// `action_for` because the port lives in the body, not the route. Skipped entirely under the
/// no-principal open-admin-plane bypass, for `authorize_action`'s own reason: with no principal at
/// all there is no second role to check.
#[allow(clippy::too_many_arguments)]
async fn terminate_spec_deploy(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    tenant: &TenantId,
    id: String,
    body: &[u8],
    auth: Option<&str>,
    host: Option<&HeaderValue>,
    idempotency: Option<&str>,
    if_match: Option<&str>,
    principal_id: Option<String>,
    bindings: &[(TenantId, Role)],
) -> Response<FrontBody> {
    #[derive(Deserialize)]
    struct DeployBody {
        port: Option<u16>,
        policy: Option<String>,
    }
    let parsed: DeployBody = if body.is_empty() {
        DeployBody {
            port: None,
            policy: None,
        }
    } else {
        match serde_json::from_slice(body) {
            Ok(parsed) => parsed,
            Err(e) => {
                return typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    &format!("deploy body: {e}"),
                );
            }
        }
    };
    if parsed.policy.is_some() {
        return typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            "drift policy (overwrite|skip|fail) arrives with S3 (#279); omit it",
        );
    }
    let Some(port) = parsed.port else {
        return typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            "deploy needs a port",
        );
    };
    let record = match spec_record_or_404(node, tenant, &id) {
        Ok(record) => record,
        Err(response) => return response,
    };
    if principal_id.is_some() {
        match authz::decide(bindings, Action::ImposterWrite, tenant) {
            Decision::Allow { .. } => {}
            Decision::Deny(Denial::InsufficientRole { role, .. }) => {
                return typed_error(
                    StatusCode::FORBIDDEN,
                    ErrorKind::InsufficientAccess,
                    &format!("role {role:?} does not grant imposter.write"),
                );
            }
            Decision::Deny(Denial::NotBoundToTenant) => return tenant_boundary_not_found(),
            Decision::Deny(Denial::Unauthenticated) => return unauthorized(),
        }
    }
    let document = match spec_document_or_500(node, &record) {
        Ok(document) => document,
        Err(response) => return response,
    };
    let compiled = match compile(
        document.as_bytes(),
        &CompileOptions {
            port: Some(port),
            name: Some(id.clone()),
            max_bytes: MAX_SPEC_BYTES,
        },
    ) {
        Ok(compiled) => compiled,
        Err(e) => {
            return typed_error(
                StatusCode::BAD_REQUEST,
                ErrorKind::BadData,
                &format!("spec {id:?} does not compile for port {port}: {e}"),
            );
        }
    };
    let config: ImposterConfig = match serde_json::from_value(compiled.imposter) {
        Ok(config) => config,
        Err(e) => {
            // The compiler's own contract is that its output deserializes as an `ImposterConfig` —
            // it is exactly what a client would `PUT /imposters`. Failing here names a bug in
            // `rift-cluster-spec`, not a client mistake, so it is a `500`, not a `400`.
            return internal(&format!(
                "compiled spec is not a valid imposter config: {e}"
            ));
        }
    };
    // The compiled config is what a client would `PUT /imposters`, so it passes the same fleet
    // admission gate (#288) — the compiler emits no `flowState` today, but the gate is about
    // what reaches the log, not about who is expected to write it.
    if let Err(response) =
        refuse_fleet_scope_without_fleet_admin([&config], principal_id.as_deref(), bindings)
    {
        return response;
    }
    let existed = match node.get_imposter(tenant.as_str(), port) {
        Ok(existed) => existed.is_some(),
        Err(e) => return internal(&e.to_string()),
    };
    let mutation = Mutation {
        ops: vec![
            ControlOp::PutImposter {
                tenant: tenant.clone(),
                config: Box::new(config),
            },
            ControlOp::SpecBind {
                tenant: tenant.clone(),
                id,
                port,
            },
        ],
        port: Some(port),
        render: Render::FetchAfter {
            path: format!("/imposters/{port}"),
            status: if existed {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            },
        },
    };
    match run_mutation(
        state,
        node,
        tenant,
        mutation,
        false,
        auth,
        host,
        idempotency,
        if_match,
        principal_id,
    )
    .await
    {
        Ok(response) | Err(response) => response,
    }
}

/// The distinct ports a mutation's config-mutating ops (`PutImposter`/`PatchStubs`) touch, in
/// order — what edit-time spec validation checks after the commit (RFC-004 §3.2, issue #278).
///
/// Empty when `ops` contains a `SpecBind`: that means this mutation IS a deploy, and a deploy's
/// compiled config already self-checked at compile time (`rift-cluster-spec`'s own
/// `CompileError::SelfCheck`) — re-validating it would be redundant.
fn spec_warning_ports(ops: &[ControlOp]) -> Vec<u16> {
    if ops
        .iter()
        .any(|op| matches!(op, ControlOp::SpecBind { .. }))
    {
        return Vec::new();
    }
    let mut ports = Vec::new();
    for op in ops {
        let port = match op {
            ControlOp::PutImposter { config, .. } => config.port,
            ControlOp::PatchStubs { port, .. } => Some(*port),
            _ => None,
        };
        if let Some(port) = port
            && !ports.contains(&port)
        {
            ports.push(port);
        }
    }
    ports
}

/// Edit-time spec validation (RFC-004 §3.2, issue #278): after a config-mutating write commits,
/// check every port it touched (from [`spec_warning_ports`]) against the spec that deployed it,
/// when it is bound to one. **Warn, never refuse** — a deliberately divergent stub is a legitimate
/// fixture; this is what tells an operator about it instead of leaving it silent. Read failures are
/// logged and skipped, never surfaced as a failure of the write that already committed.
fn spec_warnings(node: &Arc<RaftNode>, tenant: &TenantId, ports: &[u16]) -> Vec<String> {
    let mut violations = Vec::new();
    // An internal failure on a port that IS spec-bound is reported as an entry of its own rather
    // than silently contributing nothing: "no header" must keep meaning "checked and clean", never
    // "could not check" — the write already committed either way, so this is the only channel
    // left to say the difference out loud to the client (the server log has the detail).
    let unavailable =
        |port: u16, why: &str| format!("port {port}: spec validation unavailable ({why})");
    for &port in ports {
        let binding = match node.spec_binding(tenant.as_str(), port) {
            Ok(Some(binding)) => binding,
            Ok(None) => continue,
            Err(e) => {
                tracing::error!(port, error = %e, "reading spec binding for edit-time warnings");
                violations.push(unavailable(port, "binding unreadable"));
                continue;
            }
        };
        let document = match node.spec_document(&binding.digest) {
            Ok(Some(document)) => document,
            // Should be impossible (the blob and the binding that names its digest are written
            // together) — never fail a write that already committed over it, but it is a bug, so
            // it is loud rather than swallowed.
            Ok(None) => {
                tracing::error!(
                    port,
                    digest = %binding.digest,
                    "a bound spec's document is missing its blob; the write already committed"
                );
                violations.push(unavailable(port, "spec document missing"));
                continue;
            }
            Err(e) => {
                tracing::error!(port, error = %e, "reading the bound spec's document");
                violations.push(unavailable(port, "spec document unreadable"));
                continue;
            }
        };
        let compiled = match compile(
            document.as_bytes(),
            &CompileOptions {
                port: Some(port),
                name: None,
                max_bytes: MAX_SPEC_BYTES,
            },
        ) {
            Ok(compiled) => compiled,
            Err(e) => {
                tracing::warn!(port, error = %e, "bound spec no longer compiles; skipping edit-time warnings");
                violations.push(unavailable(port, "bound spec does not compile"));
                continue;
            }
        };
        let mut by_stub = HashMap::new();
        for op in &compiled.operations {
            for response in &op.responses {
                by_stub.insert(response.stub_id.as_str(), (op, &response.status));
            }
        }

        let config = match node.get_imposter(tenant.as_str(), port) {
            Ok(Some(config)) => config,
            Ok(None) => continue,
            Err(e) => {
                tracing::error!(port, error = %e, "reading the deployed config for edit-time warnings");
                violations.push(unavailable(port, "deployed config unreadable"));
                continue;
            }
        };
        let config: serde_json::Value = match serde_json::from_str(&config) {
            Ok(config) => config,
            Err(e) => {
                tracing::error!(port, error = %e, "deployed config is not valid JSON");
                violations.push(unavailable(port, "deployed config unreadable"));
                continue;
            }
        };
        for stub in config
            .get("stubs")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(id) = stub.get("id").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Some(&(op, status)) = by_stub.get(id) else {
                continue;
            };
            for body in static_is_bodies(stub) {
                for violation in validate_stub_response(op, status, &body) {
                    violations.push(format!("{id} {violation}"));
                }
            }
        }
    }
    violations
}

/// The static `is` bodies in `stub` worth schema-checking against the spec that generated it.
///
/// Skips a response carrying anything but a bare `is` — `_behaviors`, `proxy`, `inject`, `fault`
/// and `_rift` are all runtime concerns (S4/S6's job, not this compile-time check) — and skips a
/// templated string body (`{{…}}`), whose rendered value is not known until request time. Pure
/// over `serde_json::Value` so it is unit-testable without a node.
fn static_is_bodies(stub: &serde_json::Value) -> Vec<serde_json::Value> {
    let Some(responses) = stub.get("responses").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };
    responses
        .iter()
        .filter_map(|response| {
            let object = response.as_object()?;
            if object.len() != 1 {
                return None;
            }
            let is = object.get("is")?;
            match is.get("body") {
                None => None,
                Some(serde_json::Value::String(text)) if text.contains("{{") => None,
                Some(serde_json::Value::String(text)) => Some(
                    serde_json::from_str(text)
                        .unwrap_or_else(|_| serde_json::Value::String(text.clone())),
                ),
                Some(other) => Some(other.clone()),
            }
        })
        .collect()
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
    tenant: &TenantId,
    body: &[u8],
    auth: Option<&str>,
    host: Option<&HeaderValue>,
    // Who is writing, for the fleet admission gate on the two arms that take a config off the
    // wire (#288) — see `refuse_fleet_scope_without_fleet_admin`.
    principal_id: Option<&str>,
    bindings: &[(TenantId, Role)],
) -> Result<Mutation, Response<FrontBody>> {
    match kind {
        Terminated::Create => {
            let config: ImposterConfig = parse_pinned(body, node, tenant)?;
            refuse_fleet_scope_without_fleet_admin([&config], principal_id, bindings)?;
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
            let replace: ReplaceAllBody = parse_pinned(body, node, tenant)?;
            refuse_fleet_scope_without_fleet_admin(&replace.imposters, principal_id, bindings)?;
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
            let add: AddStubBody = parse_pinned(body, node, tenant)?;
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
            let replace: ReplaceStubsBody = parse_pinned(body, node, tenant)?;
            let mut config = stored_config(node, tenant, port)?;
            config.stubs = replace.stubs;
            Ok(put_config_mutation(tenant, port, config))
        }
        Terminated::ReplaceStubAt(port, index) => {
            let stub: Stub = parse_pinned(body, node, tenant)?;
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
            let stub: Stub = parse_pinned(body, node, tenant)?;
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
        // Same shape, same reasoning: source reads divert to
        // `terminate_sources` before this is reached.
        Terminated::SourceList | Terminated::SourceRead(_) => Err(internal(
            "source reads are served by terminate_sources, not build_mutation",
        )),
        // Same shape again: the three source writes divert to
        // `terminate_source_write` before this is reached — none of them is a
        // `ControlOp` sequence `build_mutation` knows how to build (a source
        // write is not an imposter record at all).
        Terminated::SourcePut | Terminated::SourceDelete(_) | Terminated::SourcePull(_) => Err(
            internal("source writes are served by terminate_source_write, not build_mutation"),
        ),
        // Not a `ControlOp` at all — the merged read has nothing to commit — and diverts to its
        // own `terminate_*` handler before this is ever reached.
        Terminated::ReadSavedRequests(_) => Err(internal(
            "the merged journal read is served by terminate_read_saved_requests, not build_mutation",
        )),
        // Same shape as the read above, and diverted in `terminate` for the same reason.
        Terminated::StreamSavedRequests(_) => Err(internal(
            "the merged journal tail is served by terminate_stream_saved_requests, not build_mutation",
        )),
        // The fleet pair (issue #362), diverted in `terminate` exactly as the per-imposter pair is.
        Terminated::ReadFleetRequests => Err(internal(
            "the fleet journal read is served by terminate_read_fleet_requests, not build_mutation",
        )),
        Terminated::StreamFleetRequests => Err(internal(
            "the fleet journal tail is served by terminate_stream_fleet_requests, not build_mutation",
        )),
        // Only the **unscoped** form of the clear reaches here (issue #224): the `?match=`
        // narrowed form diverts to `terminate_clear_saved_requests` in `terminate` before
        // `build_mutation` is ever called (see that match arm's own comment), so `kind` is
        // always the port-wide clear here, never the scoped one.
        Terminated::ClearSavedRequests(port) => Ok(Mutation {
            ops: vec![ControlOp::JournalClearGen {
                tenant: tenant.clone(),
                port,
                space: None,
            }],
            port: Some(port),
            // Byte-identical to what upstream's own `handle_clear_requests` answers with
            // (`handle_get(port, ...)`, the imposter's own `GET` representation) — a re-render,
            // not a canned message, is what "re-render the imposter as upstream does" means here.
            render: Render::FetchAfter {
                path: format!("/imposters/{port}"),
                status: StatusCode::OK,
            },
        }),
        // The proxy-recording sibling of the clear above (issue #226): one committed op
        // deletes the port's exactly-once markers fleet-wide, and every node's stale
        // completion-cache entries retire against the applied state (`completed_lookup`'s
        // revision check) — no fan-out, nothing a partitioned peer can miss forever.
        Terminated::ClearSavedProxyResponses(port) => Ok(Mutation {
            ops: vec![ControlOp::ProxyRecordedClear {
                tenant: tenant.clone(),
                port,
            }],
            port: Some(port),
            // Same re-render upstream's own clear answers with, for the same reason as
            // `ClearSavedRequests` above.
            render: Render::FetchAfter {
                path: format!("/imposters/{port}"),
                status: StatusCode::OK,
            },
        }),
        // Neither half of a space teardown is a single `ControlOp` `build_mutation` can render
        // the normal way: the flow-state half is a proxy, not a state-machine record with a
        // loopback path to `FetchAfter` from, and the journal half's response is the proxy's own
        // body, not a re-read. Diverts to `terminate_space_teardown` in `terminate`, same shape
        // as the source writes and the tenancy surface above.
        Terminated::SpaceTeardown(_, _) => Err(internal(
            "space teardown is served by terminate_space_teardown, not build_mutation",
        )),
        // A merge-on-read fan-out, not a `ControlOp` — same shape as the saved-requests reads
        // above. Diverts to `terminate_spaces_list` in `terminate` before this is ever reached.
        Terminated::SpacesList(_) => Err(internal(
            "the spaces listing is served by terminate_spaces_list, not build_mutation",
        )),
        // A try commits nothing — it is an outbound exchange whose whole result is the response
        // body. Diverts to `terminate_try_imposter` in `terminate`, same shape as the source
        // writes, the tenancy surface and the space teardown above.
        Terminated::TryImposter(_) => Err(internal(
            "a try is served by terminate_try_imposter, not build_mutation",
        )),
        // The three spec reads divert to `terminate_spec_read`/`terminate_spec_compile` in
        // `terminate` before this is ever reached — same shape as the source reads above.
        Terminated::SpecList | Terminated::SpecRead(_) | Terminated::SpecCompile(_) => {
            Err(internal(
                "spec reads are served by terminate_spec_read/terminate_spec_compile, not build_mutation",
            ))
        }
        // The three spec writes each build their own `Mutation` (a re-import short-circuits before
        // minting anything at all) and call `run_mutation` directly — diverted in `terminate`
        // before this is ever reached, same shape as the source writes above.
        Terminated::SpecPut(_) => Err(internal(
            "a spec import is served by terminate_spec_put, not build_mutation",
        )),
        Terminated::SpecDelete { .. } => Err(internal(
            "a spec delete is served by terminate_spec_delete, not build_mutation",
        )),
        Terminated::SpecDeploy(_) => Err(internal(
            "a spec deploy is served by terminate_spec_deploy, not build_mutation",
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
// A rendered refusal in the error channel, as in `ensure_session_key` above.
#[allow(clippy::result_large_err)]
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

/// [`parse`], with every `_rift.dataset` block resolved and pinned first (RFC-005 D2, #286).
///
/// Admission happens **here, on the leader, before the op is built**, so the committed entry names
/// an exact digest rather than "latest" — which is time-dependent and would let two nodes applying
/// the same entry reach different rows. Every body that can carry a stub response goes through
/// this, not just a whole `ImposterConfig`.
///
/// The state machine is read only when the body actually binds something: this runs on every
/// config write, and the overwhelming majority carry no dataset block at all.
#[allow(clippy::result_large_err)]
fn parse_pinned<T: serde::de::DeserializeOwned>(
    body: &[u8],
    node: &Arc<RaftNode>,
    tenant: &TenantId,
) -> Result<T, Response<FrontBody>> {
    let mut document: serde_json::Value = parse(body)?;

    // A failed *read* must not be reported as "no such dataset" — that would refuse a valid
    // binding with a message naming the wrong cause. The closure can only answer `Option`, so the
    // error is carried out here and takes precedence over whatever the refusal said.
    let mut read_failure: Option<String> = None;
    let mut live: Option<Vec<rift_cluster::DatasetSummary>> = None;

    let pinned = rift_cluster::datasets::pin_bindings(&mut document, |name, version| {
        if read_failure.is_some() {
            return None;
        }
        let live = match live {
            Some(ref cached) => cached,
            None => match node.datasets(tenant.as_str()) {
                Ok(rows) => live.insert(rows),
                Err(e) => {
                    read_failure = Some(e.to_string());
                    return None;
                }
            },
        };
        // No version asked for means the latest *live* one — `datasets` already drops
        // tombstoned rows, so a deleted version cannot be picked here.
        let found = match version {
            Some(want) => live.iter().find(|d| d.name == name && d.version == want),
            None => live
                .iter()
                .filter(|d| d.name == name)
                .max_by_key(|d| d.version),
        }?;
        Some(rift_cluster::datasets::ResolvedDataset {
            version: found.version,
            digest: found.digest.clone(),
            delimiter: found.delimiter,
            key_columns: found.key_columns.clone(),
        })
    });

    if let Some(error) = read_failure {
        return Err(internal(&format!("could not read datasets: {error}")));
    }
    pinned.map_err(|e| typed_error(StatusCode::BAD_REQUEST, ErrorKind::BadData, &e))?;

    serde_json::from_value(document).map_err(|e| {
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
    } else if reason.contains("no imposter on port")
        || reason.contains("no stub with id")
        || reason.starts_with("no spec ")
    {
        typed_error(StatusCode::NOT_FOUND, ErrorKind::NoSuchResource, reason)
    } else if reason.contains("cannot tell whether dataset") {
        // `ports_binding_dataset` fails closed when a stored config will not parse (RFC-005 §5).
        // That is this node's storage being corrupt, not the caller's request being wrong, and
        // `400 bad data` would send an operator to inspect a payload that is fine.
        typed_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::InternalError,
            reason,
        )
    } else if reason.contains("is bound to port(s)") {
        // The state machine's own `SpecDelete` refusal (issue #278) — the front pre-checks the
        // same condition and answers `409`, so a delete that loses the race against a deploy
        // must render the same status, not a `400` that names the identical fact differently.
        //
        // `DatasetDelete` (RFC-005 §5, issue #287) refuses in the same words deliberately, so
        // this one rule covers both. It has no front-side pre-check at all — the binding can be
        // committed concurrently with the delete, so only the apply-time refusal is authoritative
        // and this is the only place its status is decided.
        typed_error(StatusCode::CONFLICT, ErrorKind::ResourceConflict, reason)
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
        let imposter_key = ContextScope::Imposter.scoped_flow_id(Some(4545), None, "cart");
        let fleet_key = ContextScope::Fleet.scoped_flow_id(Some(4545), None, "cart");
        assert_eq!(imposter_key, "i4545:cart");
        assert_eq!(fleet_key, "f:cart");

        // Two imposters, same flow id, imposter scope: different keys, so isolated (#152).
        assert_ne!(
            ContextScope::Imposter.scoped_flow_id(Some(4545), None, "cart"),
            ContextScope::Imposter.scoped_flow_id(Some(4646), None, "cart")
        );
        // Under fleet scope the port is irrelevant: one flow, one owner, shared by both imposters.
        assert_eq!(
            ContextScope::Fleet.scoped_flow_id(Some(4545), None, "cart"),
            ContextScope::Fleet.scoped_flow_id(Some(4646), None, "cart")
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

    /// Issue #370 — regression. The knobs decoration is the first that runs *after* another one, so
    /// it is the first that can destroy what an earlier one set.
    ///
    /// `decorate_number_of_requests` stamps `Rift-Cluster-Partial` on the single-imposter read when
    /// the fleet fan-out could not reach a peer, and the knobs decoration rebuilds the response
    /// through `buffered_response`, whose header map starts empty. Losing the stamp would leave a
    /// count that is knowingly missing a node's slot looking authoritative.
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
            set_header(&mut response, HEADER_REVISION, "default:4545@7.1");

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
                Some("default:4545@7.1"),
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
            // Issue #223: the fleet merge-on-read (no `?since=`) and the clear — unscoped as of
            // #224 a Raft-committed generation bump, `?match=`-scoped still a local proxy (see
            // `Terminated::ClearSavedRequests`'s doc) — but both forms terminate either way, so
            // `classify` draws no distinction here. Both spellings, since they are one handler
            // behind two paths.
            (Method::GET, "/imposters/4545/requests"),
            (Method::GET, "/imposters/4545/savedRequests"),
            (Method::DELETE, "/imposters/4545/savedRequests"),
            (Method::DELETE, "/imposters/4545/requests"),
            // Issue #224: the journal half of a space teardown now rides alongside the
            // flow-state proxy, so this route terminates too — the flow-state half is still
            // proxied *inside* `terminate_space_teardown`, but `classify` itself now recognizes
            // the route rather than falling through entirely.
            (Method::DELETE, "/imposters/4545/spaces/flow-1"),
            // Issue #374: the spaces **listing** terminates too — there is no upstream
            // `["spaces"]` route to proxy to at all (see `spaces_list_target`'s doc).
            (Method::GET, "/imposters/4545/spaces"),
        ];
        for (method, path) in terminated {
            assert!(
                classify(&method, path, None).is_some(),
                "{method} {path} must terminate"
            );
        }

        // The listing's two-segment shape must not swallow the three-segment single-space read:
        // `spaces_list_target` requires an absent or empty third segment, so a real flow id keeps
        // this proxied to the engine exactly as it always has been.
        assert!(
            classify(&Method::GET, "/imposters/4545/spaces/qa-flow", None).is_none(),
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
                classify(&method, path, None).is_none(),
                "{method} {path} must proxy"
            );
        }

        // Issue #225 flips this row. `?since=` used to fall through to the proxy because a
        // scalar cursor cannot address a multi-writer merge; the vector cursor is exactly the
        // value that can, so the cursored read now terminates like its uncursored sibling and
        // the engine's own `parse_since` never sees a vector token. Both spellings, and the
        // legacy scalar form a pre-#225 client still holds.
        for query in ["since=3", "since=eyJ2IjoxfQ", "since"] {
            assert!(
                classify(&Method::GET, "/imposters/4545/requests", Some(query)).is_some(),
                "GET .../requests?{query} must terminate as of #225"
            );
            assert!(
                classify(&Method::GET, "/imposters/4545/savedRequests", Some(query)).is_some(),
                "GET .../savedRequests?{query} must terminate as of #225"
            );
        }

        // Issue #223 review, B1, and **not** widened by #225: `?match=` is a scoped predicate the
        // merge-on-read path never evaluates, so terminating on it would silently widen the answer
        // to the whole fleet's requests and turn a malformed filter's upstream 400 into a 200 with
        // everything. Both spellings, both query forms (`match=...` and a bare `match`), still
        // fall through to the proxy.
        for query in ["match=flow_id%3Drun-7", "match"] {
            assert!(
                classify(&Method::GET, "/imposters/4545/requests", Some(query)).is_none(),
                "GET .../requests?{query} must still proxy"
            );
            assert!(
                classify(&Method::GET, "/imposters/4545/savedRequests", Some(query)).is_none(),
                "GET .../savedRequests?{query} must still proxy"
            );
        }
        // `match` still wins when both are present: `since` no longer excuses a read from
        // terminating, but `match` alone is still enough to send the whole thing to the proxy.
        assert!(
            classify(
                &Method::GET,
                "/imposters/4545/savedRequests",
                Some("since=3&match=flow_id%3Drun-7")
            )
            .is_none(),
            "GET .../savedRequests?since=3&match=... must still proxy — `match` is the exception"
        );
        // `?match=` on the DELETE, unlike the GET, still terminates — B3's design decision is
        // that a scoped clear stays local-only (and stamps partial), not that it stops
        // terminating; `classify` has no query-based exception on the DELETE arm.
        assert!(
            classify(
                &Method::DELETE,
                "/imposters/4545/savedRequests",
                Some("match=flow_id%3Drun-7")
            )
            .is_some(),
            "DELETE .../savedRequests?match=... must still terminate"
        );

        // Issue #224: a space teardown classifies with the port and flow id extracted, exactly
        // the shape `terminate_space_teardown` needs.
        assert!(
            matches!(
                classify(&Method::DELETE, "/imposters/4545/spaces/flow-1", None),
                Some(Terminated::SpaceTeardown(4545, flow)) if flow == "flow-1"
            ),
            "DELETE .../spaces/{{flow}} must terminate with the port and flow extracted"
        );
        // Every other method on the same two-segment shape stays proxied — only the delete
        // gains a journal half; a write there is `SpaceStubs`' three-segment sibling, a
        // different route entirely, untouched by this issue.
        for method in [Method::GET, Method::PUT, Method::POST] {
            assert!(
                classify(&method, "/imposters/4545/spaces/flow-1", None).is_none(),
                "{method} .../spaces/{{flow}} must still proxy"
            );
        }
        assert!(
            classify(&Method::DELETE, "/imposters/4545/spaces/", None).is_none(),
            "an empty flow id names no space to tear down"
        );

        // Issue #223 review, Important: the `/admin/imposters/{port}/...` alias must terminate
        // identically to the canonical `/imposters/{port}/...` spelling for both verbs — the same
        // "both spellings, one handler" treatment `is_imposter_listing` already gives the
        // collection listing, extended to the two-verb savedRequests route.
        for (method, path) in [
            (Method::GET, "/admin/imposters/4545/requests"),
            (Method::GET, "/admin/imposters/4545/savedRequests"),
            (Method::DELETE, "/admin/imposters/4545/requests"),
            (Method::DELETE, "/admin/imposters/4545/savedRequests"),
        ] {
            assert!(
                classify(&method, path, None).is_some(),
                "{method} {path} (admin alias) must terminate"
            );
        }
        // The alias must flip with the canonical path (issue #225), not lag behind it — the two
        // spellings drifting apart is exactly the defect #223's review caught on this route.
        assert!(
            classify(
                &Method::GET,
                "/admin/imposters/4545/savedRequests",
                Some("since=3")
            )
            .is_some(),
            "GET admin-alias .../savedRequests?since=3 must terminate as of #225"
        );
        // The admin alias is scoped to the savedRequests/requests shape — flow-state inspection
        // and any other unrecognized suffix under this prefix stay this front's non-concern and
        // fall through exactly as before this fix.
        assert!(
            classify(
                &Method::DELETE,
                "/admin/imposters/4545/flow-state/flow-9",
                None
            )
            .is_none(),
            "DELETE .../flow-state/... must not be captured by the new alias arm"
        );

        // An unparseable port is not this surface's route at all.
        assert!(classify(&Method::DELETE, "/imposters/not-a-port", None).is_none());
    }

    // ---- Spaces listing (issue #374) ---------------------------------------------------------
    //
    // `GET /imposters/{port}/spaces` had zero HTTP-level coverage: everything above pins
    // `classify`, but nothing drove a real request through `terminate` into
    // `terminate_spaces_list` and inspected the body it renders. These do, over `test_front_over`
    // — the same bound-front-plus-reqwest harness `read_fleet_requests`/`read_requests` already
    // use for the other merge-on-read routes, since GET reads terminate exactly like writes do
    // (see `classify`'s own routing) and nothing here needs `upstream_admin` dialled.
    //
    // Most of these run over `test_front_over` as-is, whose `FlowNet` is deliberately never bound
    // to `node`'s ring (see its own doc) — exactly right for pinning the envelope (field names,
    // `unavailable`'s two refusal states, `durability`'s presence/absence), since an unbound net
    // answers `fleet_spaces` via its own cluster-view-unavailable arm regardless of scope. The one
    // test below that needs a real row builds its own front over a bound one-voter ring instead
    // (`test_front_with_bound_flow`), the same way the cursor tests above build their own front
    // over a caller-supplied journal rather than stretching `test_front_over` to cover every case.

    /// `GET /imposters/{port}/spaces` against the bound front, returned as `(status, body)` —
    /// no headers needed here, unlike `read_requests`/`read_fleet_requests`, since this route
    /// carries no cursor.
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
        let (front, _node, _journal, _dir) =
            test_front_over(rift_cluster::stores::ClusterJournal::new(1)).await;

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

    /// `contextScope: "fleet"` is refused for the policy reason `terminate_spaces_list`'s doc
    /// gives (the `f:` namespace carries no tenant component) — distinct from the unresolved
    /// case above, and the body must say which.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spaces_list_for_a_fleet_scoped_imposter_is_unavailable_fleet_scope() {
        let (front, node, _journal, _dir) =
            test_front_over(rift_cluster::stores::ClusterJournal::new(1)).await;
        node.cluster_init()
            .await
            .expect("single-voter cluster init");
        seed_imposter(&node, 4545, serde_json::json!({ "contextScope": "fleet" })).await;

        let (status, body) = read_spaces(&front, 4545).await;

        assert_eq!(status, 200, "body: {body}");
        let doc: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(doc["unavailable"], "fleet-scope", "{body}");
        assert_eq!(doc["spaces"], serde_json::json!([]), "{body}");
        assert_eq!(doc["partial"], true, "{body}");
    }

    /// RFC-005 S1 (#288): a `tenant`-scoped imposter's listing is bounded by the caller's own
    /// tenant prefix (`t<tenant>:`), so unlike `fleet` it is served. Bound harness, because the
    /// point is that a write made through the *provider* — which resolves the owning tenant at
    /// `provide` time and keys the entry `tdefault:checkout` — is what the *listing* then finds
    /// under the prefix it derives from the request tenant. If the two ever computed the prefix
    /// differently (say the provider fell back to its defensive `t??:`), the row would vanish
    /// from this list without any other error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spaces_list_for_a_tenant_scoped_imposter_is_served_under_the_tenant_prefix() {
        use rift_cluster_base::seams::FlowStoreProvider as _;

        let (front, node, net, _dir) = test_front_with_bound_flow().await;
        seed_imposter(&node, 4545, serde_json::json!({ "contextScope": "tenant" })).await;

        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
            "_rift": { "flowState": { "contextScope": "tenant" } },
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
        assert!(
            doc.get("unavailable").is_none(),
            "a tenant-scoped listing is servable: {body}"
        );
        let rows = doc["spaces"].as_array().expect("a spaces array");
        assert_eq!(
            rows.len(),
            1,
            "the one space written under `tdefault:`: {body}"
        );
        assert_eq!(rows[0]["space"], "checkout", "{body}");
        assert_eq!(rows[0]["entryCount"], 1, "{body}");
        assert_eq!(doc["partial"], false, "{body}");
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
        let (front, node, _journal, _dir) =
            test_front_over(rift_cluster::stores::ClusterJournal::new(1)).await;
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
                legacy_key_is_fleet_admin: true,
                allow_injection: false,
                scripts_dir: None,
                barrier: crate::cli::WriteBarrier::None,
                barrier_timeout: Duration::from_secs(1),
                admin_async: false,
                export_status: None,
                readiness: Arc::new(crate::readiness::Readiness::awaiting([])),
                puller: Arc::new(SourcePuller::new(
                    rift_cluster_base::seams::SourceRegistry::default(),
                )),
                route_hits: Arc::new(RouteHitCounter::default()),
                front_door_bound: false,
                journal_net: JournalNet::new(rift_cluster::stores::ClusterJournal::new(1)),
                flow_net: Arc::clone(&net),
                fleet_journal_port_cap: rift_cluster::stores::DEFAULT_FLEET_JOURNAL_PORT_CAP,
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

    /// Issue #224: `?match=` must keep `terminate`'s scoped-vs-unscoped dispatch on the
    /// pure-proxy path (`terminate_clear_saved_requests`), never `build_and_run`/
    /// `build_mutation`'s Raft-committing one — so a scoped clear never becomes a `ControlOp`.
    /// `classify` alone cannot prove this: it answers `Some(Terminated::ClearSavedRequests(port))`
    /// either way, by design (see that variant's own doc) — the dispatch guard in `terminate` is
    /// `has_query_param(query, "match")`, which is what actually decides, and which this tests
    /// directly against the same query strings `classify_terminates_exactly_the_config_surface`
    /// already proves still terminate.
    #[test]
    fn a_match_narrowed_clear_still_proxies() {
        assert!(
            has_query_param(Some("match=flow_id%3Drun-7"), "match"),
            "a ?match=... clear must be recognized as scoped, so `terminate` routes it to the \
             proxy path instead of committing a JournalClearGen"
        );
        assert!(
            has_query_param(Some("match"), "match"),
            "a bare ?match with no value is still scoped — presence is what matters, not the \
             value"
        );
        assert!(
            !has_query_param(None, "match"),
            "no query at all is the unscoped form, which commits through Raft"
        );
        assert!(
            !has_query_param(Some("since=3"), "match"),
            "an unrelated query parameter must not be mistaken for `match`"
        );
    }

    /// `query_param` is what hands the cursor token to the decoder, so the ways it can quietly
    /// return the wrong string are the ways a walk silently restarts or skips (issue #225).
    #[test]
    fn the_query_value_reader_finds_exactly_the_named_parameter() {
        assert_eq!(query_param(Some("since=abc"), "since"), Some("abc"));
        assert_eq!(
            query_param(Some("match=x&since=abc&limit=5"), "since"),
            Some("abc"),
            "position in the query string must not matter"
        );
        assert_eq!(
            query_param(Some("sincerely=no&since=yes"), "since"),
            Some("yes"),
            "a parameter whose name merely starts with `since` is a different parameter"
        );
        assert_eq!(
            query_param(Some("since"), "since"),
            Some(""),
            "a valueless `since` is an empty token, not an absent one — the caller 400s on it"
        );
        assert_eq!(query_param(Some("match=x"), "since"), None);
        assert_eq!(query_param(None, "since"), None);
        // A base64url token contains `-` and `_` and nothing that needs escaping; it must arrive
        // byte-identical or the walk resumes somewhere the client never asked for.
        let token = JournalCursor {
            generation: 3,
            pos: [(1u64, 9u64), (2, 4)].into_iter().collect(),
        }
        .encode();
        assert_eq!(
            query_param(Some(&format!("since={token}")), "since"),
            Some(token.as_str()),
            "the issued token must survive the query string unchanged"
        );
    }

    /// The three shapes `?since=` can take, and what each must mean (issue #225, AC5). The
    /// malformed case is the one that matters most: defaulting it would either replay the whole
    /// journal or skip everything since, and both are silent.
    #[test]
    fn a_since_value_is_a_token_a_legacy_scalar_or_a_refusal() {
        let this_node = 7;

        let token = JournalCursor {
            generation: 2,
            pos: [(1u64, 5u64)].into_iter().collect(),
        }
        .encode();
        assert_eq!(
            JournalCursor::decode_or_legacy(&token, this_node).expect("a vector token is accepted"),
            JournalCursor {
                generation: 2,
                pos: [(1u64, 5u64)].into_iter().collect(),
            }
        );

        let legacy =
            JournalCursor::decode_or_legacy("42", this_node).expect("a legacy scalar is accepted");
        assert_eq!(
            legacy.pos.get(&this_node).copied(),
            Some(42),
            "a bare u64 can only have come from a proxied read of this node"
        );

        for bad in ["", "not base64!!", "-1", "Zm9v"] {
            assert!(
                JournalCursor::decode_or_legacy(bad, this_node).is_err(),
                "{bad:?} must be refused, not defaulted to a position"
            );
        }
    }

    /// The try surface (issue #335): exactly `POST /admin/imposters/{port}/try`, and nothing
    /// adjacent to it.
    ///
    /// The negative half carries the weight. This is the one route that dispatches to the
    /// imposter's own engine in-process, so every shape that is *not* it must fall through to the
    /// proxy rather than be generously accepted — a classifier that also matched, say, the
    /// canonical `/imposters/`
    /// prefix or a `GET` would widen a deliberately narrow capability by accident.
    #[test]
    fn classify_terminates_exactly_the_try_surface() {
        assert!(matches!(
            classify(&Method::POST, "/admin/imposters/4545/try", None),
            Some(Terminated::TryImposter(4545))
        ));
        // A query string is not part of the match — the sample's own query rides in the body's
        // `path`, so anything here is ignored rather than making the route miss.
        assert!(matches!(
            classify(&Method::POST, "/admin/imposters/4545/try", Some("x=1")),
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
                !matches!(
                    classify(&method, path, None),
                    Some(Terminated::TryImposter(_))
                ),
                "{method} {path} must not classify as a try"
            );
        }
    }

    /// The fleet journal (issue #362) terminates on exactly two paths and one method each.
    ///
    /// The negative half matters as much as the positive: these paths sit above the
    /// `/admin/imposters/` and `/imposters/` prefixes in `classify`, so a mistake here would
    /// shadow the per-imposter routes rather than merely fail to match.
    #[test]
    fn classify_terminates_exactly_the_fleet_journal_routes() {
        assert!(matches!(
            classify(&Method::GET, "/admin/requests", None),
            Some(Terminated::ReadFleetRequests)
        ));
        assert!(matches!(
            classify(&Method::GET, "/admin/requests", Some("since=abc")),
            Some(Terminated::ReadFleetRequests)
        ));
        assert!(matches!(
            classify(&Method::GET, "/admin/requests/stream", None),
            Some(Terminated::StreamFleetRequests)
        ));

        for (method, path, query) in [
            // `?match=` is a predicate the merge path never evaluates. Unlike the per-imposter
            // route there is no proxy fallback to fall through to, so it simply does not
            // terminate — a fleet-scoped predicate read is not on offer.
            (Method::GET, "/admin/requests", Some("match=method:GET")),
            (
                Method::GET,
                "/admin/requests/stream",
                Some("match=method:GET"),
            ),
            // Reads only: there is no fleet-wide clear, and inventing one here would silently
            // widen a per-imposter destructive verb to the whole tenant.
            (Method::DELETE, "/admin/requests", None),
            (Method::POST, "/admin/requests", None),
            (Method::PUT, "/admin/requests", None),
            (Method::DELETE, "/admin/requests/stream", None),
            // Neighbours that must keep their own meaning.
            (Method::GET, "/admin/requests/", None),
            (Method::GET, "/admin/requests/4545", None),
            (Method::GET, "/admin/requestsfoo", None),
        ] {
            let classified = classify(&method, path, query);
            assert!(
                !matches!(
                    classified,
                    Some(Terminated::ReadFleetRequests | Terminated::StreamFleetRequests)
                ),
                "{method} {path} (query {query:?}) must not classify as a fleet journal route"
            );
        }
    }

    /// The per-imposter routes must still classify exactly as they did — the fleet paths are
    /// matched before them, so this is the shadowing check.
    #[test]
    fn the_fleet_routes_do_not_shadow_the_per_imposter_ones() {
        assert!(matches!(
            classify(&Method::GET, "/imposters/4545/savedRequests", None),
            Some(Terminated::ReadSavedRequests(4545))
        ));
        assert!(matches!(
            classify(&Method::GET, "/admin/imposters/4545/savedRequests", None),
            Some(Terminated::ReadSavedRequests(4545))
        ));
        assert!(matches!(
            classify(&Method::GET, "/imposters/4545/savedRequests/stream", None),
            Some(Terminated::StreamSavedRequests(4545))
        ));
    }

    /// A fleet read is the per-imposter read over the caller's own set, so it carries the same
    /// action. A distinct action would let a role hold one without the other, and no coherent
    /// policy wants that.
    #[test]
    fn the_fleet_journal_is_an_ordinary_imposter_read() {
        assert_eq!(
            action_for(&Terminated::ReadFleetRequests),
            Action::ImposterRead
        );
        assert_eq!(
            action_for(&Terminated::StreamFleetRequests),
            Action::ImposterRead
        );
        // And it names no single port, so the per-port ownership gate has nothing to check —
        // ownership comes from the tenant's own port set instead.
        assert_eq!(addressed_port(&Terminated::ReadFleetRequests), None);
        assert_eq!(addressed_port(&Terminated::StreamFleetRequests), None);
    }

    /// The live tail (issue #348) terminates on exactly one path, one method, and only without a
    /// predicate — everything else on or near it keeps proxying exactly as it does today.
    #[test]
    fn classify_terminates_exactly_the_saved_requests_stream() {
        assert!(matches!(
            classify(&Method::GET, "/imposters/4545/savedRequests/stream", None),
            Some(Terminated::StreamSavedRequests(4545))
        ));
        assert!(
            matches!(
                classify(
                    &Method::GET,
                    "/imposters/4545/savedRequests/stream",
                    Some("types=requests")
                ),
                Some(Terminated::StreamSavedRequests(4545))
            ),
            "an unrelated query parameter must not push the tail back onto the proxy"
        );

        for (method, path, query) in [
            // `?match=` is a predicate the merge path never evaluates, so it keeps proxying —
            // issue #223 review B1, verbatim. Terminating it would answer with the whole fleet's
            // requests instead of the caller's scoped subset.
            (
                Method::GET,
                "/imposters/4545/savedRequests/stream",
                Some("match=method:GET"),
            ),
            // `GET /events` is the firehose: tenant-unfiltered, FleetAdmin-gated, and issue #163's
            // to widen. It must never reach this front's terminator.
            (Method::GET, "/events", None),
            (Method::GET, "/events", Some("port=4545")),
            // Only `savedRequests` has a stream upstream — `requests` does not, so terminating it
            // would invent a route that 404s when proxied.
            (Method::GET, "/imposters/4545/requests/stream", None),
            // The `/admin/` alias exists for the read because upstream serves the read under both
            // spellings. It does not serve the stream under both.
            (
                Method::GET,
                "/admin/imposters/4545/savedRequests/stream",
                None,
            ),
            // A stream is a read.
            (Method::POST, "/imposters/4545/savedRequests/stream", None),
            (Method::DELETE, "/imposters/4545/savedRequests/stream", None),
            // Ports that are not ports, and neighbouring shapes.
            (
                Method::GET,
                "/imposters/notaport/savedRequests/stream",
                None,
            ),
            (Method::GET, "/imposters/70000/savedRequests/stream", None),
            (
                Method::GET,
                "/imposters/4545/savedRequests/stream/more",
                None,
            ),
            (Method::GET, "/imposters/4545/savedRequestsstream", None),
        ] {
            assert!(
                !matches!(
                    classify(&method, path, query),
                    Some(Terminated::StreamSavedRequests(_))
                ),
                "{method} {path} (query {query:?}) must not classify as the merged tail"
            );
        }
    }

    /// A floor is stamped as one (issue #368).
    ///
    /// `merge_route_hits` deciding `partial` correctly is worth nothing if the flag never reaches
    /// the wire, and no wire test can catch that: every one of them runs a solo node, where there
    /// is no peer to be unknown and `partial` is always false. This is #369's own bug shape — a
    /// merge computed right and plumbed wrong — so the plumbing is asserted directly.
    #[test]
    fn a_partial_route_hits_answer_is_stamped_and_a_complete_one_is_not() {
        let hits = RouteHits::Installed {
            hits: std::collections::BTreeMap::from([("svc".to_owned(), 3)]),
            front_door: route_hits::FrontDoorPresence::Bound,
        };

        let floor = route_hits_response(&hits, true);
        assert_eq!(
            floor
                .headers()
                .get(HEADER_PARTIAL)
                .and_then(|value| value.to_str().ok()),
            Some("true"),
            "a sum missing an unreachable node's contribution must say so"
        );

        let complete = route_hits_response(&hits, false);
        assert!(
            complete.headers().get(HEADER_PARTIAL).is_none(),
            "a complete answer must not claim degraded coverage"
        );
        assert_eq!(complete.status(), StatusCode::OK);
    }

    /// The source inspection surface (issue #239): the two reads terminate,
    /// everything else on the path falls through to the proxy and answers
    /// upstream's own 404/405.
    #[test]
    fn classify_terminates_exactly_the_source_surface() {
        assert!(matches!(
            classify(&Method::GET, "/admin/sources", None),
            Some(Terminated::SourceList)
        ));
        assert!(matches!(
            classify(&Method::GET, "/admin/sources/payments", None),
            Some(Terminated::SourceRead(id)) if id == "payments"
        ));
        // `%2f` must not smuggle a segment: the id is matched literally,
        // undecoded, exactly as the classifier's comment promises.
        assert!(matches!(
            classify(&Method::GET, "/admin/sources/pay%2Fments", None),
            Some(Terminated::SourceRead(id)) if id == "pay%2Fments"
        ));

        // The write half, promoted from the cluster port in #253.
        assert!(matches!(
            classify(&Method::POST, "/admin/sources", None),
            Some(Terminated::SourcePut)
        ));
        assert!(matches!(
            classify(&Method::DELETE, "/admin/sources/payments", None),
            Some(Terminated::SourceDelete(id)) if id == "payments"
        ));
        assert!(matches!(
            classify(&Method::POST, "/admin/sources/payments/pull", None),
            Some(Terminated::SourcePull(id)) if id == "payments"
        ));
        // The `/pull` verb is a suffix on the id path, not its own segment, so
        // an id containing a slash still cannot smuggle one past the check.
        assert!(matches!(
            classify(&Method::DELETE, "/admin/sources/pay%2Fments", None),
            Some(Terminated::SourceDelete(id)) if id == "pay%2Fments"
        ));

        for (method, path) in [
            // `PUT` is not the upsert verb here — `POST` is, mirroring the
            // cluster port — so it must not terminate as one.
            (Method::PUT, "/admin/sources/payments"),
            (Method::GET, "/admin/sources/"),
            (Method::GET, "/admin/sources/a/b"),
            (Method::DELETE, "/admin/sources/"),
            (Method::DELETE, "/admin/sources/a/b"),
            // An empty id in front of the verb, and a nested one behind it.
            (Method::POST, "/admin/sources//pull"),
            (Method::POST, "/admin/sources/a/b/pull"),
            // `POST` on an id with no verb is not a route.
            (Method::POST, "/admin/sources/payments"),
        ] {
            assert!(
                classify(&method, path, None).is_none(),
                "{method} {path} must not terminate"
            );
        }
    }

    /// The `/specs` surface (RFC-004 §5, issue #278) terminates in full — every verb it
    /// serves is EE-only with no upstream counterpart to proxy to.
    #[test]
    fn classify_terminates_exactly_the_spec_surface() {
        assert!(matches!(
            classify(&Method::GET, "/specs", None),
            Some(Terminated::SpecList)
        ));
        assert!(matches!(
            classify(&Method::GET, "/specs/petstore", None),
            Some(Terminated::SpecRead(id)) if id == "petstore"
        ));
        assert!(matches!(
            classify(&Method::PUT, "/specs/petstore", None),
            Some(Terminated::SpecPut(id)) if id == "petstore"
        ));
        assert!(matches!(
            classify(&Method::DELETE, "/specs/petstore", None),
            Some(Terminated::SpecDelete { id, force: false }) if id == "petstore"
        ));
        assert!(matches!(
            classify(&Method::DELETE, "/specs/petstore", Some("force")),
            Some(Terminated::SpecDelete { id, force: true }) if id == "petstore"
        ));
        assert!(matches!(
            classify(&Method::DELETE, "/specs/petstore", Some("force=true")),
            Some(Terminated::SpecDelete { id, force: true }) if id == "petstore"
        ));
        assert!(matches!(
            classify(&Method::POST, "/specs/petstore/compile", None),
            Some(Terminated::SpecCompile(id)) if id == "petstore"
        ));
        assert!(matches!(
            classify(&Method::POST, "/specs/petstore/deploy", None),
            Some(Terminated::SpecDeploy(id)) if id == "petstore"
        ));
        // Undecoded, like every other id on this front: `%2f` cannot smuggle a segment.
        assert!(matches!(
            classify(&Method::GET, "/specs/pet%2Fstore", None),
            Some(Terminated::SpecRead(id)) if id == "pet%2Fstore"
        ));

        for (method, path) in [
            (Method::POST, "/specs"),
            (Method::PUT, "/specs"),
            (Method::DELETE, "/specs"),
            (Method::GET, "/specs/"),
            (Method::PUT, "/specs/"),
            (Method::GET, "/specs/a/b"),
            (Method::PUT, "/specs/a/b"),
            (Method::POST, "/specs/petstore"),
            (Method::POST, "/specs//compile"),
            (Method::POST, "/specs/a/b/deploy"),
            (Method::GET, "/specs/petstore/compile"),
            (Method::GET, "/specs/petstore/deploy"),
            (Method::POST, "/specs/petstore/drift"),
        ] {
            assert!(
                classify(&method, path, None).is_none(),
                "{method} {path} must not terminate"
            );
        }
    }

    /// RFC-004 §4.3: reads and the dry-run compile at `spec.read`, import and deploy at
    /// `spec.write`, delete at `spec.delete`. None of them is port-addressed — deploy names its
    /// port in the body and checks `ImposterWrite` itself — and none names a tenant in its path.
    #[test]
    fn spec_routes_map_to_the_spec_actions_and_address_no_port() {
        let cases = [
            (Terminated::SpecList, Action::SpecRead),
            (
                Terminated::SpecRead("petstore".to_owned()),
                Action::SpecRead,
            ),
            (
                Terminated::SpecCompile("petstore".to_owned()),
                Action::SpecRead,
            ),
            (
                Terminated::SpecPut("petstore".to_owned()),
                Action::SpecWrite,
            ),
            (
                Terminated::SpecDeploy("petstore".to_owned()),
                Action::SpecWrite,
            ),
            (
                Terminated::SpecDelete {
                    id: "petstore".to_owned(),
                    force: false,
                },
                Action::SpecDelete,
            ),
            (
                Terminated::SpecDelete {
                    id: "petstore".to_owned(),
                    force: true,
                },
                Action::SpecDelete,
            ),
        ];
        for (kind, action) in cases {
            assert_eq!(action_for(&kind), action, "{kind:?}");
            assert_eq!(addressed_port(&kind), None, "{kind:?}");
            assert_eq!(scope_for(&kind), None, "{kind:?}");
        }
    }

    /// The front's cap and the compiler's cap are the same number, kept in two crates on
    /// purpose (`rift-cluster` must not depend on the compiler — apply never runs spec code).
    /// This is the tripwire that keeps them from drifting apart.
    #[test]
    fn the_spec_size_cap_matches_the_compiler() {
        assert_eq!(
            rift_cluster::control::MAX_SPEC_BYTES,
            rift_cluster_spec::MAX_SPEC_BYTES
        );
    }

    /// `static_is_bodies` skips exactly what edit-time validation must never flag: a stub
    /// The header caps itself (issue #278): ten entries then `+N more`, visible ASCII only, and a
    /// hard byte ceiling so a fat body echoed back can never push the response past a proxy's
    /// header limit and turn a committed write into a `502`.
    #[test]
    fn spec_warnings_header_caps_entries_bytes_and_charset() {
        assert_eq!(spec_warnings_header(&[]), None, "nothing to say, no header");

        let few = vec!["a /x: one".to_owned(), "b /y: two".to_owned()];
        assert_eq!(
            spec_warnings_header(&few).as_deref(),
            Some("a /x: one; b /y: two")
        );

        let many: Vec<String> = (0..13).map(|i| format!("s{i} /f: v")).collect();
        let value = spec_warnings_header(&many).expect("header");
        assert_eq!(
            value.matches("; ").count(),
            10,
            "ten entries plus the tail: {value}"
        );
        assert!(value.ends_with("; +3 more"), "{value}");
        assert!(
            !value.contains("s10 "),
            "the eleventh entry is folded into the count: {value}"
        );

        let curly = vec!["s /name: expected \u{201c}string\u{201d}, got number\n".to_owned()];
        assert_eq!(
            spec_warnings_header(&curly).as_deref(),
            Some("s /name: expected ?string?, got number?"),
            "non-visible-ASCII is replaced, never rejected"
        );

        let fat = vec!["x".repeat(SPEC_WARNINGS_MAX_BYTES * 2)];
        let value = spec_warnings_header(&fat).expect("header");
        assert_eq!(value.len(), SPEC_WARNINGS_MAX_BYTES);
        assert!(value.ends_with("..."), "{}", &value[value.len() - 8..]);
    }

    /// `compileSpec`'s diff (issue #278): compiled ids the deployed config lacks are `added`,
    /// deployed `spec:` ids the compiler no longer emits are `removed` (a hand-added stub is never
    /// the compiler's loss), and ids on both sides whose JSON differs are `changed`.
    #[test]
    fn spec_compile_diff_classifies_added_removed_and_changed_by_stub_id() {
        let compiled = serde_json::json!({ "stubs": [
            { "id": "spec:listPets:200", "responses": [{ "is": { "statusCode": 200 } }] },
            { "id": "spec:createPets:201", "responses": [{ "is": { "statusCode": 201 } }] },
        ]});
        let deployed = serde_json::json!({ "stubs": [
            { "id": "spec:listPets:200", "responses": [{ "is": { "statusCode": 200, "body": "edited" } }] },
            { "id": "spec:showPetById:200", "responses": [{ "is": { "statusCode": 200 } }] },
            { "id": "hand-added", "responses": [{ "is": { "statusCode": 418 } }] },
        ]})
        .to_string();

        assert_eq!(
            spec_compile_diff(4545, Some(&deployed), &compiled),
            serde_json::json!({
                "port": 4545,
                "deployed": true,
                "added": ["spec:createPets:201"],
                "removed": ["spec:showPetById:200"],
                "changed": ["spec:listPets:200"],
            })
        );
        assert_eq!(
            spec_compile_diff(4545, None, &compiled),
            serde_json::json!({
                "port": 4545,
                "deployed": false,
                "added": ["spec:createPets:201", "spec:listPets:200"],
                "removed": [],
                "changed": [],
            }),
            "nothing on the port: everything is an addition, in id order"
        );
    }

    /// `?force` is a boolean flag, not a presence check (issue #278): the contract declares it
    /// `type: boolean`, so a client that spells "do not force" as `force=false` must not get the
    /// destructive path it declined.
    #[test]
    fn query_flag_honours_an_explicit_false() {
        for (query, want) in [
            (None, false),
            (Some("force"), true),
            (Some("force="), true),
            (Some("force=true"), true),
            (Some("force=1"), true),
            (Some("force=TRUE"), true),
            (Some("force=false"), false),
            (Some("force=0"), false),
            (Some("other=1"), false),
            (Some("a=b&force=false"), false),
            (Some("a=b&force"), true),
        ] {
            assert_eq!(query_flag(query, "force"), want, "{query:?}");
        }
        assert!(matches!(
            classify(&Method::DELETE, "/specs/petstore", Some("force=false")),
            Some(Terminated::SpecDelete { force: false, .. })
        ));
    }

    /// carrying `_behaviors` (or any key beside `is`) is a runtime concern, and a templated body
    /// is not known until request time. A bare `is` with a static body is kept.
    #[test]
    fn static_is_bodies_skips_behaviors_and_templated_strings_but_keeps_a_static_is() {
        let stub = serde_json::json!({
            "id": "spec:listPets:200",
            "responses": [
                { "is": { "statusCode": 200, "body": { "id": 1 } } },
                { "is": { "statusCode": 200, "body": "{{templated}}" } },
                {
                    "is": { "statusCode": 200, "body": { "id": 2 } },
                    "_behaviors": { "wait": 10 }
                },
                { "proxy": { "to": "http://example.com" } },
            ]
        });
        assert_eq!(
            static_is_bodies(&stub),
            vec![serde_json::json!({ "id": 1 })]
        );
    }

    /// A string body that itself parses as JSON is checked as the value it encodes, not as a bare
    /// string — otherwise a schema declaring `type: object` would flag every stub whose body
    /// happened to arrive JSON-encoded-as-text rather than as a native JSON value.
    #[test]
    fn static_is_bodies_parses_a_json_encoded_string_body() {
        let stub = serde_json::json!({
            "id": "spec:listPets:200",
            "responses": [{ "is": { "statusCode": 200, "body": "{\"id\":1}" } }]
        });
        assert_eq!(
            static_is_bodies(&stub),
            vec![serde_json::json!({ "id": 1 })]
        );
    }

    /// A non-JSON string body is checked as the literal string it is — a plain-text response is a
    /// legitimate declared shape, not a parse failure to be silently dropped.
    #[test]
    fn static_is_bodies_keeps_a_plain_string_body_as_a_string() {
        let stub = serde_json::json!({
            "id": "spec:listPets:200",
            "responses": [{ "is": { "statusCode": 200, "body": "hello" } }]
        });
        assert_eq!(
            static_is_bodies(&stub),
            vec![serde_json::Value::String("hello".to_owned())]
        );
    }

    /// A response with no `body` at all has nothing to check, and a stub with no `responses`
    /// array is the same "nothing to check" answer, not a panic.
    #[test]
    fn static_is_bodies_skips_a_response_with_no_body_and_a_stub_with_no_responses() {
        let stub = serde_json::json!({
            "id": "spec:listPets:200",
            "responses": [{ "is": { "statusCode": 204 } }]
        });
        assert_eq!(static_is_bodies(&stub), Vec::<serde_json::Value>::new());
        assert_eq!(
            static_is_bodies(&serde_json::json!({ "id": "spec:listPets:200" })),
            Vec::<serde_json::Value>::new()
        );
    }

    /// Issue #239's design decision, asserted at the render seam: a poll error
    /// is this node's observation, so it travels under `nodeLocal` and never
    /// lands on the replicated record — which stays byte-comparable across
    /// nodes for convergence checks.
    #[test]
    fn render_sources_keeps_node_local_facts_off_the_replicated_record() {
        let record = rift_cluster::SourceRecord {
            id: "payments".to_owned(),
            uri: "scripted://cfg/payments.json".to_owned(),
            mode: rift_cluster::control::SourceMode::Tracking,
            auth_ref: None,
            on_drift: rift_cluster::control::OnDrift::Overwrite,
            poll_secs: Some(60),
            drifted: false,
            last_version: Some("v1".to_owned()),
            last_digest: None,
            last_pulled_at_secs: None,
            last_outcome: None,
            ports: vec![9301],
            revision: 12,
        };

        let body = render_sources(
            3_342_140_982_834_931_156,
            &SourcesView::One(record.clone()),
            |id| (id == "payments").then(|| "connect timeout".to_owned()),
        )
        .expect("renders");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("valid JSON");
        assert!(
            body["source"].get("lastPollError").is_none(),
            "the record must stay exactly the replicated projection: {body}"
        );
        // A realistic id, as a string. `7` passed happily while the endpoint rounded every id a
        // real fleet actually issues — the third time in this codebase that a single-digit fixture
        // hid a `u64` round-trip defect (see #332).
        assert_eq!(body["nodeLocal"]["nodeId"], "3342140982834931156");
        assert_eq!(
            body["nodeLocal"]["pollErrors"]["payments"],
            "connect timeout"
        );

        let body = render_sources(7, &SourcesView::List(vec![record]), |_| None).expect("renders");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("valid JSON");
        assert_eq!(body["sources"][0]["id"], "payments");
        assert_eq!(
            body["nodeLocal"]["pollErrors"],
            serde_json::json!({}),
            "no error is an empty map, not an absent field"
        );
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
        let (front, node, _journal, dir) =
            test_front_over(rift_cluster::stores::ClusterJournal::new(1)).await;
        (front, node, dir)
    }

    /// [`test_front`] over a caller-supplied journal, handed back so a test can record into the
    /// very shard the front will read (issue #225).
    ///
    /// The cursor tests need this because `?since=` **terminates** now: nothing they exercise
    /// dials `upstream_admin`, so a front over a journal they control is a complete, honest
    /// harness for the whole handler — query parsing, token decode, the merge walk, and the
    /// response headers — rather than a mock of it.
    async fn test_front_over(
        journal: Arc<rift_cluster::stores::ClusterJournal>,
    ) -> (
        AdminFront,
        Arc<RaftNode>,
        Arc<rift_cluster::stores::ClusterJournal>,
        tempfile::TempDir,
    ) {
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
                puller: Arc::new(SourcePuller::new(
                    rift_cluster_base::seams::SourceRegistry::default(),
                )),
                route_hits: Arc::new(RouteHitCounter::default()),
                front_door_bound: false,
                journal_net: JournalNet::new(Arc::clone(&journal)),
                // In-memory and never bound to `node`'s ring: nothing in this file's
                // tests exercises `/admin/tenants`'s flow-entry fan-out, so this only
                // needs to satisfy `FrontConfig`'s required field.
                flow_net: FlowNet::new(rift_cluster::stores::FlowShard::in_memory(
                    rift_cluster::stores::ShardConfig::default(),
                )),
                fleet_journal_port_cap: rift_cluster::stores::DEFAULT_FLEET_JOURNAL_PORT_CAP,
            },
            &node,
        )
        .await
        .expect("front binds");
        (front, node, journal, dir)
    }

    // ---- Cursor wiring on the merged read (issue #225) ---------------------------------
    //
    // The token codec and the merge walk each have their own gate tests. These cover the seam
    // between them — query parsing, decode, and the response headers — which is the part a
    // mutation could break while every other test in the tree stayed green.

    use rift_cluster_base::seams::RequestJournal;

    const CURSOR_TEST_PORT: u16 = 4545;

    fn recorded(path: &str) -> rift_cluster_base::seams::RecordedRequest {
        rift_cluster_base::seams::RecordedRequest {
            mode: rift_cluster_base::seams::ResponseMode::Text,
            request_from: "t".into(),
            method: "GET".into(),
            path: path.into(),
            query: Default::default(),
            headers: Default::default(),
            body: None,
            timestamp: format!("2026-01-01T00:00:{:02}Z", path.len()),
            match_outcome: None,
            status: None,
            latency_ms: None,
            node: None,
        }
    }

    /// `GET /admin/requests` against the bound front (issue #362).
    async fn read_fleet_requests(
        front: &AdminFront,
        query: Option<&str>,
    ) -> (u16, reqwest::header::HeaderMap, String) {
        let addr = front.local_addr();
        let url = match query {
            Some(q) => format!("http://{addr}/admin/requests?{q}"),
            None => format!("http://{addr}/admin/requests"),
        };
        let response = reqwest::get(url).await.expect("the front answers");
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.text().await.expect("a body");
        (status, headers, body)
    }

    /// A tenant owning no imposters still gets a well-formed answer — an empty page that says it
    /// covers nothing, rather than a 404 or a bare `[]` a client has to guess the shape of.
    #[tokio::test]
    async fn the_fleet_read_answers_a_stated_empty_coverage() {
        let (front, _node, _journal, _dir) =
            test_front_over(rift_cluster::stores::ClusterJournal::new(1)).await;

        let (status, headers, body) = read_fleet_requests(&front, None).await;

        assert_eq!(status, 200, "body: {body}");
        let page: serde_json::Value = serde_json::from_str(&body).expect("a JSON page");
        assert_eq!(
            page["requests"].as_array().map(Vec::len),
            Some(0),
            "no imposters, no rows"
        );
        assert_eq!(page["coverage"]["total"], 0);
        assert_eq!(page["coverage"]["capped"], false);
        assert!(
            page["cursor"]
                .as_str()
                .is_some_and(|token| !token.is_empty()),
            "even an empty page hands back a resumable position: {body}"
        );
        assert!(
            headers.contains_key(HEADER_NEXT_INDEX),
            "the cursor is also a header, for parity with the per-imposter read"
        );
    }

    /// A per-imposter token pasted into the fleet endpoint is a good cursor at the wrong door.
    /// It must be refused — never read as a fleet position — and the message must be specific
    /// enough to act on.
    #[tokio::test]
    async fn the_fleet_read_refuses_a_per_imposter_cursor() {
        let (front, _node, _journal, _dir) =
            test_front_over(rift_cluster::stores::ClusterJournal::new(1)).await;
        let per_port = JournalCursor::start().encode();

        let (status, _headers, body) =
            read_fleet_requests(&front, Some(&format!("since={per_port}"))).await;

        assert_eq!(status, 400, "body: {body}");
        assert!(
            body.contains("per-imposter"),
            "the refusal must name the scope mismatch, not just say 'bad cursor': {body}"
        );
    }

    /// The legacy bare scalar names a position in one shard of one port. It has no fleet meaning,
    /// so it is refused here rather than quietly read as one.
    #[tokio::test]
    async fn the_fleet_read_refuses_the_legacy_scalar_cursor() {
        let (front, _node, _journal, _dir) =
            test_front_over(rift_cluster::stores::ClusterJournal::new(1)).await;

        let (status, _headers, body) = read_fleet_requests(&front, Some("since=42")).await;

        assert_eq!(status, 400, "body: {body}");
    }

    /// Garbage is refused too — and, critically, never defaulted to the beginning (which would
    /// replay the whole journal) or to now (which would skip everything since).
    #[tokio::test]
    async fn the_fleet_read_refuses_a_malformed_cursor() {
        let (front, _node, _journal, _dir) =
            test_front_over(rift_cluster::stores::ClusterJournal::new(1)).await;

        let (status, _headers, body) =
            read_fleet_requests(&front, Some("since=not%20base64%21%21")).await;

        assert_eq!(status, 400, "body: {body}");
    }

    /// `GET /imposters/{port}/savedRequests` against the bound front, with an optional raw
    /// query string — returned as `(status, headers, body)` so a test can assert on all three.
    async fn read_requests(
        front: &AdminFront,
        query: Option<&str>,
    ) -> (u16, reqwest::header::HeaderMap, String) {
        let addr = front.local_addr();
        let url = match query {
            Some(q) => format!("http://{addr}/imposters/{CURSOR_TEST_PORT}/savedRequests?{q}"),
            None => format!("http://{addr}/imposters/{CURSOR_TEST_PORT}/savedRequests"),
        };
        let response = reqwest::get(url).await.expect("the front answers");
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.text().await.expect("a body");
        (status, headers, body)
    }

    /// Open the merged tail and return whatever frames arrive in a short window. Raw socket
    /// rather than an HTTP client because the body never ends — a client that waits for
    /// completion would wait forever.
    async fn stream_frames(front: &AdminFront, last_event_id: Option<&str>) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let addr = front.local_addr();
        let resume = last_event_id
            .map(|id| format!("Last-Event-ID: {id}\r\n"))
            .unwrap_or_default();
        let mut socket = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to the front");
        socket
            .write_all(
                format!(
                    "GET /imposters/{CURSOR_TEST_PORT}/savedRequests/stream HTTP/1.1\r\n\
                     Host: {addr}\r\nAccept: text/event-stream\r\n{resume}\r\n"
                )
                .as_bytes(),
            )
            .await
            .expect("write the stream request");

        let mut seen = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        // Read until the first drain has plainly happened (a `request` or `lagged` frame), or the
        // window closes — an idle stream legitimately sends nothing more than `hello`.
        while std::time::Instant::now() < deadline {
            let mut buf = [0_u8; 8192];
            match tokio::time::timeout(Duration::from_millis(300), socket.read(&mut buf)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(read)) => seen.extend_from_slice(&buf[..read]),
                Ok(Err(e)) => panic!("the stream connection failed mid-read: {e}"),
                Err(_) => {}
            }
            let text = String::from_utf8_lossy(&seen);
            if text.contains("event: lagged") || text.contains("event: request") {
                break;
            }
        }
        String::from_utf8_lossy(&seen).into_owned()
    }

    fn header_of(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    }

    /// The happy path end to end: a merged read issues a token, and presenting that token walks
    /// forward rather than replaying. Nothing else in the tree proves `x-rift-next-index` is
    /// actually *set* by the handler, nor that a token survives the query string and decodes on
    /// the way back in.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_merged_read_issues_a_cursor_that_pages_forward() {
        let journal = rift_cluster::stores::ClusterJournal::new(1);
        let (front, node, journal, _dir) = test_front_over(journal).await;

        RequestJournal::record(&*journal, CURSOR_TEST_PORT, "", recorded("/a"));
        RequestJournal::record(&*journal, CURSOR_TEST_PORT, "", recorded("/bb"));

        let (status, headers, body) = read_requests(&front, None).await;
        assert_eq!(status, 200, "uncursored merged read: {body}");
        assert!(body.contains("/a") && body.contains("/bb"), "body: {body}");
        assert!(
            header_of(&headers, HEADER_TRUNCATED).is_none(),
            "nothing was evicted, so truncation must not be claimed"
        );
        let token = header_of(&headers, HEADER_NEXT_INDEX)
            .expect("a merged read must issue a cursor (issue #225)");
        assert!(
            JournalCursor::decode(&token).is_ok(),
            "the issued token must decode, not merely look opaque: {token}"
        );

        // Presenting it walks forward: everything recorded so far is consumed.
        let (status, _, body) = read_requests(&front, Some(&format!("since={token}"))).await;
        assert_eq!(status, 200);
        assert_eq!(body, "[]", "the token must not replay the page it followed");

        // And a genuinely new entry surfaces on the next page, so "empty" was exhaustion
        // rather than the cursor swallowing everything.
        RequestJournal::record(&*journal, CURSOR_TEST_PORT, "", recorded("/ccc"));
        let (status, _, body) = read_requests(&front, Some(&format!("since={token}"))).await;
        assert_eq!(status, 200);
        assert!(
            body.contains("/ccc") && !body.contains("/bb"),
            "only the entry recorded after the token may appear: {body}"
        );

        node.shutdown().await.expect("node shuts down");
    }

    /// A cursor the front cannot read is a **typed 400**, never a defaulted position. This is
    /// the arm with the most expensive silent failure in the change: defaulting to 0 replays
    /// the whole journal as new traffic, and defaulting to "current" hides everything recorded
    /// since — both surface in the client's decoder with nothing server-side to correlate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_malformed_since_is_a_typed_400_not_a_defaulted_position() {
        let journal = rift_cluster::stores::ClusterJournal::new(1);
        let (front, node, journal, _dir) = test_front_over(journal).await;
        RequestJournal::record(&*journal, CURSOR_TEST_PORT, "", recorded("/a"));

        for bad in ["not%20base64%21", "Zm9v", "since-was-empty"] {
            let (status, _, body) = read_requests(&front, Some(&format!("since={bad}"))).await;
            assert_eq!(status, 400, "since={bad} must be refused: {body}");
            assert!(
                !body.contains("/a"),
                "a refused cursor must not answer with journal entries: {body}"
            );
        }
        // A bare `?since` with no value is an empty token, not an absent one.
        let (status, _, _) = read_requests(&front, Some("since")).await;
        assert_eq!(
            status, 400,
            "a valueless `since` is a broken token, not a baseline read"
        );

        node.shutdown().await.expect("node shuts down");
    }

    /// The upgrade window: a bare `u64` predates the vector cursor and can only have come from
    /// a proxied per-node read of this node, so it is honoured as this node's own position.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_legacy_scalar_since_is_read_as_this_nodes_position() {
        let journal = rift_cluster::stores::ClusterJournal::new(1);
        let (front, node, journal, _dir) = test_front_over(journal).await;
        RequestJournal::record(&*journal, CURSOR_TEST_PORT, "", recorded("/a"));
        RequestJournal::record(&*journal, CURSOR_TEST_PORT, "", recorded("/bb"));

        // Seq 1 is `/a`, so a scalar cursor of 1 has consumed it and must not see it again.
        let (status, headers, body) = read_requests(&front, Some("since=1")).await;
        assert_eq!(status, 200, "a legacy scalar must be accepted: {body}");
        assert!(
            body.contains("/bb") && !body.contains("\"/a\""),
            "a scalar cursor must resume this node's shard, not restart it: {body}"
        );
        assert!(
            header_of(&headers, HEADER_NEXT_INDEX).is_some(),
            "a legacy read still upgrades the caller to a vector token"
        );

        node.shutdown().await.expect("node shuts down");
    }

    /// AC3's wiring half: `x-rift-truncated` is stamped when — and only when — retention ate
    /// entries the presented position had not reached. The walk's own gate tests pin the
    /// boundary arithmetic; this pins that the bit actually reaches the response.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eviction_stamps_the_truncation_header_on_a_stale_cursor() {
        // A cap this small makes retention pressure immediate and deterministic, instead of
        // recording ten thousand entries to reach the default.
        let journal = rift_cluster::stores::ClusterJournal::with_parts(
            1,
            rift_cluster::stores::JournalConfig {
                fleet_capacity: 2,
                min_shard_cap: 2,
                ..Default::default()
            },
            Arc::new(rift_cluster::stores::journal::MonotonicClock::default()),
        );
        let (front, node, journal, _dir) = test_front_over(journal).await;

        RequestJournal::record(&*journal, CURSOR_TEST_PORT, "", recorded("/a"));
        let (_, headers, _) = read_requests(&front, None).await;
        let early = header_of(&headers, HEADER_NEXT_INDEX).expect("a cursor");
        assert!(
            header_of(&headers, HEADER_TRUNCATED).is_none(),
            "nothing has been evicted yet"
        );

        // Push past the cap so the shard's watermark climbs above where `early` points.
        for path in ["/bb", "/ccc", "/dddd", "/eeeee"] {
            RequestJournal::record(&*journal, CURSOR_TEST_PORT, "", recorded(path));
        }

        let (status, headers, body) = read_requests(&front, Some(&format!("since={early}"))).await;
        assert_eq!(
            status, 200,
            "a truncated read still serves what survives: {body}"
        );
        assert_eq!(
            header_of(&headers, HEADER_TRUNCATED).as_deref(),
            Some("true"),
            "the reader's position predates the eviction watermark and must be told so: {body}"
        );

        // But a **baseline** read of the very same evicting shard must NOT claim truncation: it
        // is a snapshot of what is retained, so it has no hole. Collapsing absence into a
        // position of zero would make this the loudest false alarm in the API — every ordinary
        // uncursored read of a busy port, forever. Upstream and the single-node path both draw
        // this line, so the merged read has to as well.
        let (_, headers, _) = read_requests(&front, None).await;
        assert!(
            header_of(&headers, HEADER_TRUNCATED).is_none(),
            "a baseline read is a snapshot and can never be truncated"
        );

        node.shutdown().await.expect("node shuts down");
    }

    /// Issue #64: the accept loop dying is *observable*.
    ///
    /// Before this, the front held a bare `JoinHandle` that was only ever
    /// aborted. A panic in the loop left the node a zombie cluster member —
    /// public admin dead, still a Raft voter, `/readyz` still 200 — and nothing
    /// waiting on it, so `serve_until` never returned. An abort that `shutdown`
    /// The `lagged` half of issue #348's honesty rule: retention overtaking a reader is
    /// **announced**, not quietly skipped over.
    ///
    /// Worth an end-to-end test rather than trusting the flag: `truncated` is computed by the
    /// merge and the stream just forwards it, so the only thing that can break here is the
    /// forwarding — which no unit test of the merge would ever notice. Uses the same tiny
    /// retention cap `eviction_stamps_the_truncation_header_on_a_stale_cursor` does, so eviction
    /// is immediate and deterministic instead of ten thousand recordings away.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eviction_past_a_resuming_reader_is_announced_as_lagged() {
        let journal = rift_cluster::stores::ClusterJournal::with_parts(
            1,
            rift_cluster::stores::JournalConfig {
                fleet_capacity: 2,
                min_shard_cap: 2,
                ..Default::default()
            },
            Arc::new(rift_cluster::stores::journal::MonotonicClock::default()),
        );
        let (front, _node, journal, _dir) = test_front_over(journal).await;

        RequestJournal::record(&*journal, CURSOR_TEST_PORT, "", recorded("/a"));
        let (_, headers, _) = read_requests(&front, None).await;
        let early = header_of(&headers, HEADER_NEXT_INDEX).expect("a cursor");

        // Push past the cap, so the shard's watermark climbs above where `early` points and the
        // entries that reader had not reached are gone for good.
        for path in ["/bb", "/ccc", "/dddd", "/eeeee"] {
            RequestJournal::record(&*journal, CURSOR_TEST_PORT, "", recorded(path));
        }

        let body = stream_frames(&front, Some(&early)).await;
        assert!(
            body.contains("event: lagged"),
            "a reader whose position predates the eviction watermark must be told, not silently \
             served the remainder as though nothing were missing: {body}"
        );
        assert!(
            body.contains("\"truncated\": true") || body.contains("\"truncated\":true"),
            "the lagged frame says what was lost: {body}"
        );

        // The vacuity guard: a reader that has missed nothing must NOT be told it lagged, or the
        // assertion above would pass on a stream that cries wolf on every connection.
        let (_, headers, _) = read_requests(&front, None).await;
        let current = header_of(&headers, HEADER_NEXT_INDEX).expect("a cursor");
        let body = stream_frames(&front, Some(&current)).await;
        assert!(
            !body.contains("event: lagged"),
            "an up-to-date reader has no hole and must not be told it has one: {body}"
        );
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

        /// A stub that injects a connection-level fault answers, in-process, with the engine's
        /// carrier response — a `502` that on the wire is never sent, because the serve loop
        /// aborts the socket instead. Presenting that carrier as "the imposter said 502" would
        /// be a fabricated answer; the try says what the wire would have done. And the `error`
        /// fault — a real `5xx` the wire does send — stays a successful try, so the two must not
        /// be conflated by the header alone.
        #[tokio::test]
        async fn a_connection_fault_carrier_is_an_explicit_transport_outcome() {
            for name in [
                "CONNECTION_RESET_BY_PEER",
                "reset",
                "EMPTY_RESPONSE",
                "RANDOM_DATA_THEN_CLOSE",
                "malformed",
            ] {
                let (dispatch, _seen) =
                    canned(answer(502, &[("x-rift-fault", name.as_bytes())], ""));
                let failure = perform_try(dispatch, PORT, &spec("GET", "/reset"), BUDGET, CAP)
                    .await
                    .expect_err("a connection fault is not an answer");
                let TryFailure::Fault(reported) = &failure else {
                    panic!("{name}: expected an explicit fault outcome, got {failure:?}");
                };
                assert_eq!(reported, name, "the fault is named as the stub spelled it");
            }

            let (dispatch, _seen) = canned(answer(500, &[("x-rift-fault", b"error")], "injected"));
            let outcome = perform_try(dispatch, PORT, &spec("GET", "/error"), BUDGET, CAP)
                .await
                .expect("an `error` fault is a real response the wire sends");
            assert_eq!(outcome.status, 500);
            assert_eq!(outcome.body, "injected");
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

    // ---- #439: the bytes leave the op at submit, after the quorum (D-49) ----

    fn carried_dataset_put(csv: &str) -> ControlOp {
        ControlOp::DatasetPut {
            tenant: TenantId::default(),
            record: rift_cluster::control::DatasetRecord {
                name: "customers".to_owned(),
                digest: rift_cluster::control::Digest::new(
                    rift_cluster::blobs::digest_of_bytes(csv.as_bytes())
                        .as_str()
                        .to_owned(),
                ),
                key_columns: vec!["id".to_owned()],
                delimiter: ',',
                columns: vec!["id".to_owned(), "name".to_owned()],
                rows: 1,
                bytes: csv.len() as u64,
            },
            csv: Some(csv.to_owned()),
            origin: 0,
        }
    }

    #[test]
    fn strip_blob_drops_the_payload_and_stamps_the_origin() {
        let mut op = carried_dataset_put("id,name\n1,ada\n");
        assert!(
            op_blob(&op).is_some(),
            "carried: there is a blob to fan out"
        );

        strip_blob(&mut op, 42);

        let ControlOp::DatasetPut { csv, origin, .. } = &op else {
            panic!("expected DatasetPut")
        };
        assert_eq!(csv, &None);
        assert_eq!(*origin, 42);
        assert!(
            op_blob(&op).is_none(),
            "stripped: nothing left to fan out, so a second pass is a no-op"
        );
    }

    /// Pins D-49: what `submit` receives is metadata-sized and names this node as origin, and
    /// the blob is already in this node's own store — the fan-out ran before the strip, while
    /// the op still carried its bytes.
    #[tokio::test]
    async fn fan_out_then_submit_hands_submit_a_stripped_op_with_this_node_as_origin() {
        let (node, _dir) = test_node().await;
        // A quorum needs a membership: an empty one is refused by design (D-19).
        node.cluster_init().await.expect("solo cluster");
        let csv = "id,name\n1,ada\n";
        let request = ControlRequest {
            op_id: uuid::Uuid::new_v4(),
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op: carried_dataset_put(csv),
        };

        let handed = fan_out_then_submit(&node, request, |r| async move { r })
            .await
            .expect("a solo cluster is its own joint quorum");

        let ControlOp::DatasetPut {
            csv: carried,
            origin,
            ..
        } = &handed.op
        else {
            panic!("expected DatasetPut")
        };
        assert_eq!(carried, &None, "the bytes must not reach the log");
        assert_eq!(*origin, node.id());
        assert!(
            serde_json::to_vec(&handed.op).expect("serialise").len() < 4096,
            "what is proposed is metadata-sized"
        );
        let digest = rift_cluster::blobs::digest_of_bytes(csv.as_bytes());
        assert!(
            node.blobs().stat(&digest).expect("stat").have,
            "fanned out to this node's own store before the strip"
        );
    }
}
