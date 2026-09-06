//! Rift cluster facade.
//!
//! This crate is the entry point for the clustering layer that builds on top of
//! the Rift core (vendored under `vendor/rift`). The core crates are re-exported
//! here so cluster code depends on a single facade rather than reaching into the
//! submodule directly — the other cluster crates take `rift-cluster-base` as their only
//! path into the core, and Cargo (not a lint) is what enforces that: they carry
//! no direct `rift-mock-core` / `rift-types` / `rift-http-proxy` dependency.
//!
//! Licensed under Apache-2.0, the same as the core. The facade exists for
//! dependency hygiene, not for a licence boundary — there no longer is one.

pub use rift_http_proxy;
/// The imposter linter. Re-exported here for the same reason as everything else in this crate:
/// the MCP server's `lint` tool runs it in-process, and reaching `vendor/rift` directly from
/// `rift-cluster-server` is exactly what the facade exists to prevent.
pub use rift_lint;
pub use rift_mock_core;
pub use rift_types;

/// The upstream extension seams the cluster backends plug into.
///
/// Each of these is a generic, `Local`-by-default trait upstream (Apache-2.0);
/// nothing here is cluster-aware. Grouping them in one module keeps the
/// cluster/core boundary legible: anything a cluster crate implements
/// against should be reachable from here, and anything that is not is a gap to
/// close upstream rather than to fork around.
pub mod seams {
    /// Flow / scenario state: keyed KV with compare-and-set, plus the provider
    /// hook that lets an embedder supply the store per imposter.
    ///
    /// U-1's compare-and-set is implemented by upstream's own
    /// `rift-store-redis::RedisFlowStore` as well as by the cluster's stores:
    /// by decision D-6 the existing Redis backend, CAS included, stays upstream
    /// and nothing Redis-shaped is withheld on this side of the facade. The
    /// cluster's implementations of these traits are its own (redb-backed,
    /// D-16) — none of them is Redis.
    pub use rift_mock_core::extensions::flow_state::{
        CasOutcome, FlowStore, FlowStoreProvider, NoOpFlowStore,
    };

    /// The cursor arithmetic itself, so a clustered sequencer advances by the
    /// same packing as the built-in one rather than reimplementing repeats.
    pub use rift_mock_core::behaviors::RuleCycler;
    /// Response cursors (cycling), keyed by port + slot + stub identity + scope.
    pub use rift_mock_core::behaviors::sequencer::{
        LocalSequencer, ResponseSequencer, SequenceKey,
    };

    /// Recorded-request storage backing `savedRequests` and `numberOfRequests`,
    /// including the cursor read used by the `since=` API and the SSE streams.
    pub use rift_mock_core::imposter::journal::{
        JournalEntry, JournalRead, JournalReadSince, LocalJournal, MAX_RECORDED_REQUESTS,
        RequestJournal,
    };

    /// The recorded request itself, its match diagnosis, and the response mode it
    /// carries. These appear in [`RequestJournal`]'s own signatures, so a cluster
    /// crate cannot implement the seam without naming them — and this facade is its
    /// only path into the core.
    pub use rift_mock_core::imposter::{MatchOutcome, RecordedRequest, ResponseMode};

    /// Proxy recordings and the `proxyOnce` record-once claim gate. The
    /// publication half (`StubPlacement`, `StubPublication`, the recorded
    /// types) is what a *publishing* store (#226, upstream #910/#911) consumes:
    /// `complete()` hands it the built stub, and `publishes_stubs()` makes the
    /// store the sole publisher.
    pub use rift_mock_core::recording::{
        ClaimOutcome, ClaimToken, LocalProxyStore, ProxyRecordingStore, ProxyStoreError,
        RecordedResponse, RequestSignature, StubPlacement, StubPublication,
    };

    /// The last-chance hook consulted when a request matched no stub, before
    /// the defaultForward / defaultResponse / empty-200 fallthrough. What the
    /// cluster's pull-on-miss safety net hangs on.
    pub use rift_mock_core::extensions::no_match::{
        NoMatchContext, NoMatchDirective, NoMatchInterceptor,
    };

    /// The exchange inspector (U-13, upstream #966; RFC-004 §6): a synchronous
    /// per-imposter hook pair that sees a request after journaling and before
    /// matching, and the response before it is written, and may replace either.
    /// Installed per imposter through `ImposterManager::with_exchange_inspector_provider`,
    /// the `FlowStoreProvider` shape. What spec enforcement (RFC-004 §3.6, S6)
    /// and full-fidelity traffic observation hang on; nothing here consumes it
    /// yet.
    pub use rift_mock_core::extensions::exchange_inspector::{
        ExchangeInspector, ExchangeInspectorProvider, InspectRequest, InspectResponse,
        InspectVerdict,
    };

    /// Incremental config reconciliation (U-6): the per-port / per-stub apply
    /// path that replaces reset-the-world reload, plus the change-event hook,
    /// the attribution that rides with it, and the stable stub identity both
    /// sides key on. This is the whole-config level of D-5's order-aware
    /// reconcile — the diff runs upstream, keyed by `stub_key`.
    ///
    /// [`EventContext`] is U-10 (upstream #855): it answers *who* caused a
    /// change, which is what turns an event stream into an audit trail. It is
    /// `#[non_exhaustive]`, so an embedder builds one from [`Default`] and
    /// assigns — a struct literal will not compile outside upstream.
    pub use rift_mock_core::imposter::{
        ApplyReport, EventContext, ImposterEvent, ImposterEventListener, ImposterManager,
        TlsDefaults, stub_key,
    };

    /// Pluggable admin-API authorization (U-9, upstream #854) and the
    /// attribution channel that carries its verdict to the event hook (U-10).
    ///
    /// The built-in gate is one global api key, so success yields *access*, not
    /// an identity — every caller is equivalent. [`AdminAuthorizer`] is
    /// consulted **after** the route is parsed, so a decision can see the
    /// action, the port, the space and the parsed path params. Two parts of the
    /// upstream contract the cluster RBAC layer (#161) is built on:
    ///
    /// - **Ordering.** The api-key check runs before the route is parsed, so
    ///   `Deny` renders `403` and a bad key renders `401`. Two limits worth
    ///   knowing before relying on that: keyless is upstream's *default* (the
    ///   gate is `if let Some(key) = api_key`), so `credential` may legitimately
    ///   be `None`; and an authorizer can only `Allow`/`Deny`, with `Deny`
    ///   mapping unconditionally to `403` — an EE authorizer cannot answer
    ///   `401`. When route classification returns `None` the hook is not
    ///   consulted at all and the request 404s, so the hook bounds what a
    ///   principal can *do*, not what it can learn about which routes exist.
    /// - **[`AuthzRequest::scope`] is caller-asserted.** It arrives in the
    ///   `x-rift-scope` request header, so any caller can set it to anything. It
    ///   says which target a create is *claimed* for; it is never the
    ///   authorization subject.
    ///
    /// [`with_principal_scope`] is how an allowed principal reaches
    /// [`EventContext`]: upstream sets a task-local around the request rather
    /// than threading a principal parameter through every mutating manager
    /// method.
    ///
    /// **It does not survive the clustered write path.** A task-local follows
    /// the task across `.await` but not across a task boundary, and a clustered
    /// mutation is applied by openraft's state-machine task, not by the admin
    /// request task that opened the scope — so `current_principal()` is `None`
    /// at every replicated emit. Clustered attribution rides
    /// `ControlRequest.principal` in the log instead (#161 populates it, #163
    /// reads it); this seam is the single-node/embedded path. See
    /// `docs/architecture/08-tenancy-security.md`.
    pub use rift_mock_core::extensions::authz::{
        AdminAuthorizer, AllowAll, AuthzDecision, AuthzRequest, SharedAdminAuthorizer, actions,
        current_principal, with_principal_scope,
    };

    /// Upstream's own admin route classification (rift#889, this repo's finding
    /// against U-9).
    ///
    /// The authorizer hook alone is only usable when upstream parses your
    /// routes. The clustered admin front terminates part of the admin surface
    /// itself and proxies the rest, so to consult an authorizer it must produce
    /// the action / port / space / params tuple — and producing it by hand
    /// means a **second route parser**, which upstream has already shipped a
    /// bug from: a classifier that filtered empty path segments while the
    /// router did not, so `PUT /imposters/:port/scenarios//state` dispatched a
    /// mutation it had never classified. Calling [`classify`] is what keeps one
    /// parser authoritative.
    ///
    /// [`SCOPE_HEADER`] is exported for the same reason: an embedder that
    /// hardcodes `"x-rift-scope"` silently stops seeing scopes the day upstream
    /// renames it.
    ///
    /// [`AuthzTarget`] is `#[non_exhaustive]` — read its fields, and match with
    /// `..` rather than destructuring exhaustively.
    pub use rift_http_proxy::admin_api::authz::{AuthzTarget, SCOPE_HEADER, classify};

    /// The config types a replicated control op carries (ADR-001 §4.1): the
    /// imposter config itself, the stub type its edit scripts address, and the
    /// error the engine reports when an apply side-effect fails.
    pub use rift_mock_core::imposter::{ImposterConfig, ImposterError, Stub, StubResponse};

    /// The per-imposter flow-state block, and the passthrough map a
    /// provider-supplied store reads its own options out of (upstream #845).
    ///
    /// `FlowStoreProvider::provide` is handed an [`ImposterConfig`], so this is
    /// the path a clustered store takes to per-imposter settings that upstream
    /// has no opinion about.
    pub use rift_mock_core::imposter::RiftFlowStateConfig;

    /// The operator-configured outbound TLS trust policy for `proxy` stubs, and
    /// the client that realises it (upstream #974/#976).
    ///
    /// Injecting a manager replaces upstream's construction wholesale, so the
    /// clustered path has to build this itself or a clustered node silently
    /// ignores `--upstream-ca-file` / `--upstream-tls-skip-verify` that the
    /// single-node binary honours — the exact divergence #976 fixed upstream.
    /// `manager_parity`'s builder-call guard is what catches it.
    pub use rift_mock_core::imposter::build_upstream_client;
    pub use rift_mock_core::proxy::OutboundTls;

    /// Config-time `_rift.script` `file:`/`ref:` resolution (upstream #356):
    /// the clustered admin front resolves before replicating, so nothing
    /// unresolved is ever committed to the log.
    pub use rift_mock_core::imposter::{
        RiftScriptConfig, ScriptBaseDir, ScriptResolveError, resolve_scripts, resolve_stub_scripts,
    };

    /// Post-resolution script validation (upstream's admin-time 400 gate): the
    /// clustered admin front validates resolved ops before parking, so nothing
    /// syntactically broken is ever committed to the log. Only meaningful
    /// *after* resolution — an unresolved `file:`/`ref:` source carries no
    /// `code` to parse.
    pub use rift_mock_core::scripting::{validate_stub, validate_stubs};

    /// The typed admin error envelope (upstream #797): a stable `type` slug
    /// plus the frozen legacy `code`. Client-visible cluster failures emit
    /// this, never a hand-rolled shape.
    pub use rift_mock_core::response::{ErrorKind, error_response_typed};

    /// The `--allowInjection` classifier: whether a config carries a scripting
    /// surface. The clustered admin front gates terminated writes on this, the
    /// same check the core admin applies before storing.
    pub use rift_http_proxy::injection_gate::config_uses_script_surface;

    /// The space-stub shape guard (upstream #336): why a body is not shaped like a stub, or
    /// `None` if it is. `Stub` deserializes through `StubRaw`, where every field is
    /// `#[serde(default)]` and unknown keys are discarded, so **any** JSON object deserializes —
    /// an object of only unrecognised keys becomes the vacuous stub, which matches everything in
    /// its space and serves a response nobody authored.
    ///
    /// The clustered admin front terminates `POST /imposters/:port/spaces/:flowId/stubs` as a
    /// replicated write (issue #537), so the request never reaches the upstream handler that
    /// used to apply this. Reading the rule through the seam rather than restating the field list
    /// here is the point: a second copy goes stale the moment upstream adds a stub field, and it
    /// fails the wrong way — a legitimate stub answered `400`.
    pub use rift_http_proxy::admin_api::not_a_stub_reason;

    /// TCP-fault carrier classification (upstream #965): whether a response is
    /// the placeholder a fault stub produces — the one the engine's own serve
    /// loop throws away in favour of aborting the socket, so a client over TCP
    /// never sees it. An embedder answering in-process (the `try` endpoint,
    /// #344) *does* receive it, and needs this to say "the connection would
    /// have been aborted" instead of presenting the carrier's `502` as the
    /// imposter's answer.
    ///
    /// Authoritative because it reads the `TcpFaultKind` response extension —
    /// the same signal `FaultIo` acts on — rather than the `x-rift-fault`
    /// header, which is neither necessary (a v2 script's `reset()` carrier set
    /// no header before #984) nor sufficient (`_rift.fault.error` stamps one on
    /// a response the client genuinely receives). The extension is only
    /// readable while the response is still in memory; see `perform_try`.
    pub use rift_mock_core::{TcpFaultKind, tcp_fault_carrier};

    /// Backend-outage reporting and response decoration: how a backend surfaces
    /// as a structured 503, and how per-request annotations become response
    /// headers without the core handlers knowing what they mean.
    pub use rift_mock_core::extensions::decorate::{
        BackendUnavailable, ResponseDecorator, ResponsePhase, annotate, backend_error_response,
        with_annotation_scope,
    };

    /// Server composition: the bootstrap builder, the metrics listener, and the
    /// single-port gateway dispatch a cluster binary composes rather than
    /// forking.
    ///
    /// The plain gateway listener — the `/__rift/:port` promotion of rift#212 —
    /// is upstream by decision D-11 (U-7). Only what needs cluster state stays
    /// on this side: the replicated front-door route table (U-11's admin CRUD
    /// lives in the admin front), the admin front's in-process dispatch
    /// (#344, [`handle_imposter_request`]) and the `Rift-Cluster-*` decoration.
    pub use rift_http_proxy::gateway::dispatch_to_port;
    pub use rift_http_proxy::server::{
        Cli, Commands, RunningServer, ServerBuilder, run_metrics_server,
    };
    /// The per-imposter half of [`dispatch_to_port`], for a caller that already holds the
    /// `Arc<Imposter>` and must not re-resolve it (issue #344): the admin front's in-process
    /// try resolves the imposter once, behind its own gate, and answers from exactly that one —
    /// a second lookup inside the dispatch could find it gone and answer with the engine's own
    /// "no imposter on port" 404 as if the imposter had said so.
    pub use rift_mock_core::imposter::handle_imposter_request;

    /// Imposter sources (U-12): the scheme-dispatched provider SPI upstream
    /// ships `file:` and `https:` implementations of, plus the registry an
    /// embedder registers its own providers into.
    ///
    /// The clustered control plane (issue #134) fetches *through* this and then
    /// submits the result as a control op, rather than letting each node fetch
    /// independently — two nodes fetching the same URI can get different bytes,
    /// and the Raft apply path must be deterministic. `MAX_BODY_BYTES` is the
    /// provider-side fetch cap the log-entry bound is matched to.
    pub use rift_http_proxy::sources::{
        FetchedImposters, FileSource, HttpSource, ImposterSource, MAX_BODY_BYTES, SourceMeta,
        SourceRef, SourceRegistry, parse_uri_list,
    };

    /// The shared parse path every provider must route its bytes through, so a
    /// document behaves identically whichever source delivered it.
    ///
    /// The cluster `git:`/`s3:`/`registry:` providers (#136) are exactly the
    /// third-party sources [`parse_remote_document`]'s own doc anticipates:
    /// they hand back bytes, and format sniffing, the Mountebank document
    /// shapes and the `intercept`/`routes` block rules all run here. Its two
    /// fail-closed differences from the `--configfile` path — EJS
    /// `include`/`stringify` refused, `_rift.script` `file:` references refused
    /// — are the reason a provider must not parse for itself: a document
    /// fetched from a git host has no author who already holds local access.
    pub use rift_http_proxy::config_loader::{LoadedConfig, parse_remote_document};

    /// The front door's route table (issue #19 / U-11): content-based routing
    /// from one listener to many imposters. Upstream ships the listener, the
    /// matcher and the config-file surface; its admin CRUD was deferred, which
    /// is why the clustered admin front provides it as a replicated
    /// control-plane object (issue #131) rather than proxying to an upstream
    /// endpoint that does not exist.
    ///
    /// [`RouteObserver`] and [`bind_front_door_with_observer`] are the counting seam (issue #368):
    /// upstream calls the observer once per request a route claims, which is what backs the admin
    /// plane's per-route HITS figure. Single-node Rift installs none and pays nothing for it.
    pub use rift_http_proxy::front_door::{
        CompiledRoutes, HeaderMatch, Route, RouteMatch, RouteObserver, RouteTable, RouteTableError,
        RouteTarget, RunningFrontDoor, bind_front_door, bind_front_door_with_observer,
    };
}

/// Build edition marker, surfaced in banners and `--version` output.
pub const EDITION: &str = "cluster";

/// The open-source Rift this build embeds, as the vendored submodule's
/// `git describe` (e.g. `v0.15.0`), or `unknown` when the pin could not be
/// determined at build time.
///
/// This is deliberately not any vendored crate's `CARGO_PKG_VERSION`: every
/// crate under `vendor/rift` inherits `0.1.0` from that workspace, so those
/// numbers identify nothing. See this crate's `build.rs`.
pub const UPSTREAM_VERSION: &str = env!("RIFT_UPSTREAM_VERSION");

/// Returns the semantic version of this cluster build.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// The one-line identity a cluster binary reports from `--version` and at
/// startup: its own version, its edition, and which open-source Rift is inside
/// it. All three matter on a bug report — the edition says which code paths
/// exist at all, and the pin says which engine produced the behaviour.
#[must_use]
pub fn version_banner() -> String {
    format!(
        "{} ({EDITION}, rift {UPSTREAM_VERSION})",
        version(),
        EDITION = EDITION,
        UPSTREAM_VERSION = UPSTREAM_VERSION,
    )
}

#[cfg(test)]
mod tests {
    /// Names every seam the cluster crates build against.
    ///
    /// The point is the compile, not the assertion: an upstream rename, move,
    /// or visibility change breaks this test rather than surfacing later as a
    /// confusing error inside `rift-cluster`. When upstream does move a seam,
    /// fix the re-export in `seams` — do not delete the line here.
    ///
    /// Pins D-11: `dispatch_to_port` and `ServerBuilder` resolve from upstream,
    /// so the plain gateway listener is an upstream seam, not a cluster fork.
    /// Pins D-6: U-1's `FlowStore::compare_and_set` / `CasOutcome` resolve from
    /// upstream too — the CAS was not withheld from the open-source store.
    #[test]
    fn seams_resolve() {
        use crate::seams::*;

        fn assert_object_safe<T: ?Sized>() {}
        assert_object_safe::<dyn FlowStore>();
        assert_object_safe::<dyn FlowStoreProvider>();
        assert_object_safe::<dyn ResponseSequencer>();
        assert_object_safe::<dyn RequestJournal>();
        assert_object_safe::<dyn ProxyRecordingStore>();
        assert_object_safe::<dyn ImposterEventListener>();
        assert_object_safe::<dyn AdminAuthorizer>();
        assert_object_safe::<dyn ResponseDecorator>();
        assert_object_safe::<dyn NoMatchInterceptor>();
        assert_object_safe::<dyn ExchangeInspector>();
        assert_object_safe::<dyn ExchangeInspectorProvider>();
        _named::<InspectRequest<'_>>();
        _named::<InspectResponse<'_>>();
        let _ = InspectVerdict::Proceed;

        // Built-in implementations the `--cluster`-off path keeps using.
        let _: fn() -> LocalSequencer = LocalSequencer::default;
        let _: fn() -> LocalJournal = LocalJournal::default;
        let _: fn() -> ImposterManager = ImposterManager::new;
        let _: &dyn FlowStore = &NoOpFlowStore;

        // Value types that cross the seam boundary.
        let _: fn() -> RiftFlowStateConfig = RiftFlowStateConfig::default;
        let _: fn(&_, usize) -> String = stub_key;
        let _ = CasOutcome::Applied;
        let _ = ResponsePhase::DataPlane;
        let _ = ClaimOutcome::InFlight;
        let _ = MAX_RECORDED_REQUESTS;
        fn _named<T>() {}
        _named::<SequenceKey<'_>>();
        _named::<JournalEntry>();
        _named::<JournalRead>();
        _named::<JournalReadSince>();
        _named::<MatchOutcome>();
        _named::<RecordedRequest>();
        _named::<ResponseMode>();
        _named::<ClaimToken>();
        _named::<ProxyStoreError>();
        _named::<LocalProxyStore>();
        _named::<ApplyReport>();
        _named::<ImposterEvent>();
        _named::<ImposterConfig>();
        _named::<ImposterError>();
        _named::<Stub>();
        _named::<RiftScriptConfig>();
        _named::<ScriptBaseDir>();
        _named::<ScriptResolveError>();
        _named::<NoMatchContext<'_>>();
        let _ = NoMatchDirective::Proceed;
        let _ = (resolve_scripts, resolve_stub_scripts);
        let _ = (validate_stub, validate_stubs);
        let _ = ErrorKind::Unavailable;
        let _ = (error_response_typed, config_uses_script_surface);
        let _ = not_a_stub_reason;
        _named::<BackendUnavailable>();
        _named::<TlsDefaults>();
        _named::<ServerBuilder>();
        _named::<RunningServer>();
        _named::<Cli>();
        _named::<Commands>();
        let _ = (
            annotate,
            backend_error_response,
            dispatch_to_port,
            handle_imposter_request,
        );
        let _ = (
            run_metrics_server,
            with_annotation_scope::<std::future::Ready<()>>,
        );

        // Imposter sources (U-12 / issue #134).
        assert_object_safe::<dyn ImposterSource>();
        _named::<SourceRef>();
        _named::<SourceRegistry>();
        _named::<SourceMeta>();
        _named::<FetchedImposters>();
        _named::<FileSource>();
        _named::<HttpSource>();
        let _ = (MAX_BODY_BYTES, parse_uri_list);

        // Admin authorization + event attribution (U-9 / U-10, issue #160).
        _named::<AuthzRequest<'_>>();
        _named::<AuthzDecision>();
        _named::<EventContext>();
        _named::<AllowAll>();
        _named::<SharedAdminAuthorizer>();
        let _ = AuthzDecision::allow();
        // All nine, not just the two the EE code happens to name today: the
        // point of the constants is that an upstream rename is a compile error
        // here rather than a match arm that silently stops matching, and that
        // only holds for the ones something actually names.
        let _ = [
            actions::SYSTEM_READ,
            actions::SYSTEM_WRITE,
            actions::IMPOSTER_READ,
            actions::IMPOSTER_WRITE,
            actions::IMPOSTER_DELETE,
            actions::IMPOSTER_VERIFY,
            actions::EVENTS_READ,
            actions::INTERCEPT_READ,
            actions::INTERCEPT_WRITE,
        ];
        let _ = (
            current_principal,
            with_principal_scope::<std::future::Ready<()>>,
        );
        // The registration point: a seam that cannot be installed is not a seam.
        let _: fn(ServerBuilder, std::sync::Arc<dyn AdminAuthorizer>) -> ServerBuilder =
            ServerBuilder::admin_authorizer;

        // Route classification (rift#889): the half that makes the authorizer
        // usable from an admin front upstream does not parse.
        _named::<AuthzTarget>();
        let _ = SCOPE_HEADER;

        // Front-door route table (issue #131).
        _named::<RouteTable>();
        _named::<Route>();
        _named::<RouteMatch>();
        _named::<RouteTarget>();
        _named::<HeaderMatch>();
        _named::<RouteTableError>();
        _named::<CompiledRoutes>();
        _named::<RunningFrontDoor>();
        let _ = bind_front_door;
        // Issue #368's counting seam.
        _named::<&dyn RouteObserver>();
        let _ = bind_front_door_with_observer;
    }

    /// A provider-supplied store can read its own options out of `flowState`.
    ///
    /// This is the whole point of the upstream `extra` passthrough (#845, this
    /// repo's #118): without it there is nowhere to put a per-imposter setting
    /// that upstream has no opinion about, and the clustered flow store (#120)
    /// has two. Asserting the behaviour rather than merely naming the type,
    /// because a rename would break the seam test above, but a *retraction* of
    /// the flatten attribute would leave both compiling while silently dropping
    /// every key.
    #[test]
    fn flow_state_config_carries_provider_options_through() {
        use crate::seams::RiftFlowStateConfig;

        let config: RiftFlowStateConfig = serde_json::from_value(serde_json::json!({
            "backend": "inmemory",
            "readConsistency": "strong",
            "durability": "async",
        }))
        .expect("flowState with provider options parses");

        assert_eq!(config.backend, "inmemory", "typed fields still bind");
        assert_eq!(
            config.extra.get("readConsistency").and_then(|v| v.as_str()),
            Some("strong"),
            "an unknown key must reach the provider, not be dropped: {:?}",
            config.extra
        );
        assert_eq!(
            config.extra.get("durability").and_then(|v| v.as_str()),
            Some("async")
        );
    }

    /// An embedder can actually *build* an [`EventContext`].
    ///
    /// This guards the `Default` derive specifically. `EventContext` is
    /// `#[non_exhaustive]`, so a struct literal will not compile outside
    /// upstream and `Default` + assignment is the only construction path an
    /// cluster crate has. Drop that derive upstream and the seam test above
    /// still passes while every #163 audit test loses its ability to build a
    /// context at all.
    #[test]
    fn an_embedder_can_construct_an_event_context_and_carry_a_principal() {
        use crate::seams::{AuthzDecision, EventContext};

        let unattributed = EventContext::default();
        assert_eq!(
            unattributed.principal, None,
            "absent attribution must be reported as absent, never guessed"
        );

        let mut ctx = EventContext::default();
        ctx.principal = Some("tenant-a/alice".to_owned());
        assert_eq!(ctx.principal.as_deref(), Some("tenant-a/alice"));

        // The handoff, as a compile check rather than a behavioural claim:
        // `Allow` must stay a distinct variant carrying an owned principal, so
        // #161 can move it into an `EventContext` (single-node) or into
        // `ControlRequest.principal` (clustered). Collapsing `Allow` to a unit
        // variant, or narrowing `principal` to a borrow, breaks this line.
        let AuthzDecision::Allow { principal } = (AuthzDecision::Allow {
            principal: Some("tenant-a/alice".to_owned()),
        }) else {
            panic!("Allow must stay a distinct variant carrying its principal");
        };
        assert_eq!(principal, ctx.principal);
    }

    /// The clustered admin front can classify a route through the facade —
    /// which is the whole point of the export (rift#889).
    ///
    /// Asserted behaviourally, not by naming the symbols: what #161 needs is the
    /// action, the port and the **parsed params**, and a `classify` that
    /// resolved but returned an empty `params` would satisfy a compile check
    /// while still forcing a hand-rolled parser. The alternative to this call is
    /// a second route parser, which upstream has already shipped a bug from.
    #[test]
    fn the_admin_front_can_classify_a_route_without_a_second_parser() {
        use crate::seams::{SCOPE_HEADER, actions, classify};
        use hyper::Method;

        let target = classify(&Method::PUT, "/imposters/4545/stubs/by-id/abc123")
            .expect("a dispatchable admin route classifies");
        assert_eq!(target.action, actions::IMPOSTER_WRITE);
        assert_eq!(target.port, Some(4545));
        assert!(
            target.params.contains(&("stubId", "abc123".to_owned())),
            "the parsed params are the point — without them an embedder must \
             re-parse the path: {:?}",
            target.params
        );

        // `None` means "not an authorizable admin route": the hook is not
        // consulted and the request 404s. #161's deny-by-default must not read
        // this as a denial.
        assert!(classify(&Method::GET, "/definitely-not-a-route").is_none());

        // The header name comes from upstream, so a rename cannot silently
        // strand an embedder that hardcoded it.
        assert_eq!(SCOPE_HEADER, "x-rift-scope");
    }

    /// [`AuthzRequest`]'s `Debug` must redact the credential.
    ///
    /// Upstream hand-writes this rather than deriving it, and the cluster
    /// RBAC layer (#161) logs authz decisions — so a derived `Debug` upstream
    /// would put the verbatim admin token into cluster logs, and nothing on
    /// this side would notice. Asserted at the boundary we depend on it at.
    #[test]
    fn the_authz_request_debug_never_leaks_the_credential() {
        use crate::seams::{AuthzRequest, actions};

        let req = AuthzRequest {
            credential: Some("super-secret-admin-token"),
            action: actions::IMPOSTER_WRITE,
            port: Some(4545),
            space: None,
            scope: Some("tenant-a"),
            params: &[("port", "4545")],
        };
        let rendered = format!("{req:?}");
        assert!(
            !rendered.contains("super-secret-admin-token"),
            "the admin credential leaked into Debug: {rendered}"
        );
        assert!(rendered.contains("<redacted>"), "got: {rendered}");
        // A caller-asserted scope is not a secret and must stay visible — it is
        // exactly what an audit record needs to show was *claimed*.
        assert!(rendered.contains("tenant-a"), "got: {rendered}");
    }

    /// The cluster side can implement the trait and refuse.
    ///
    /// Honest about what this buys: the *assertion* is trivial — upstream
    /// already pins `Deny.is_allowed() == false`. The value is the **compile**,
    /// which is what proves `AdminAuthorizer` is implementable from outside
    /// upstream (object-safe, no sealed supertrait, `AuthzRequest` constructible
    /// with a caller-owned `params` slice). That is the shape #161's
    /// deny-by-default RBAC is built on, and an upstream change that broke it
    /// would surface here rather than inside `rift-cluster`.
    #[test]
    fn an_cluster_authorizer_can_deny() {
        use crate::seams::{AdminAuthorizer, AuthzDecision, AuthzRequest, actions};

        struct DenyEverything;
        impl AdminAuthorizer for DenyEverything {
            fn authorize(&self, _req: AuthzRequest<'_>) -> AuthzDecision {
                AuthzDecision::Deny {
                    reason: "no binding grants this action",
                }
            }
        }

        let authorizer: std::sync::Arc<dyn AdminAuthorizer> = std::sync::Arc::new(DenyEverything);
        let decision = authorizer.authorize(AuthzRequest {
            credential: Some("token"),
            action: actions::IMPOSTER_DELETE,
            port: None,
            space: None,
            scope: None,
            params: &[],
        });
        assert!(!decision.is_allowed());
    }

    /// `with_principal_scope` actually makes `current_principal` observable —
    /// the mechanism upstream chose instead of threading a principal parameter
    /// through every mutating manager method.
    #[tokio::test]
    async fn the_principal_scope_is_observable_inside_it_and_absent_outside() {
        use crate::seams::{current_principal, with_principal_scope};

        assert_eq!(
            current_principal(),
            None,
            "outside any request scope there is nobody to name"
        );

        let seen = with_principal_scope(Some("tenant-a/alice".to_owned()), async {
            current_principal()
        })
        .await;
        assert_eq!(seen.as_deref(), Some("tenant-a/alice"));

        assert_eq!(
            current_principal(),
            None,
            "the scope must not leak past the request it was opened for"
        );
    }

    #[test]
    fn version_is_the_crate_version() {
        assert_eq!(super::version(), env!("CARGO_PKG_VERSION"));
        assert_eq!(super::EDITION, "cluster");
    }

    #[test]
    fn the_banner_names_the_edition_and_the_embedded_upstream() {
        let banner = super::version_banner();
        assert!(banner.starts_with(super::version()), "{banner}");
        assert!(banner.contains("cluster"), "{banner}");
        assert!(banner.contains(super::UPSTREAM_VERSION), "{banner}");
    }

    #[test]
    fn the_upstream_pin_is_never_an_empty_string() {
        // An empty pin would render as `rift ` and read as a formatting bug
        // rather than as missing information; build.rs substitutes a marker.
        assert!(!super::UPSTREAM_VERSION.is_empty());
    }
}
