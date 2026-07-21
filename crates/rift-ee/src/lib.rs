//! Rift Enterprise Edition.
//!
//! This crate is the entry point for proprietary functionality that builds on
//! top of the open-source Rift (vendored under `vendor/rift`). The open-source
//! crates are re-exported here so enterprise code depends on a single facade
//! rather than reaching into the submodule directly — the other enterprise
//! crates take `rift-ee` as their only path into the core, and Cargo (not a
//! lint) is what enforces that: they carry no direct `rift-mock-core` /
//! `rift-types` / `rift-http-proxy` dependency.

pub use rift_http_proxy;
pub use rift_mock_core;
pub use rift_types;

/// The upstream extension seams the enterprise backends plug into.
///
/// Each of these is a generic, `Local`-by-default trait upstream (Apache-2.0);
/// nothing here is cluster-aware. Grouping them in one module keeps the
/// enterprise/OSS boundary legible: anything an enterprise crate implements
/// against should be reachable from here, and anything that is not is a gap to
/// close upstream rather than to fork around.
pub mod seams {
    /// Flow / scenario state: keyed KV with compare-and-set, plus the provider
    /// hook that lets an embedder supply the store per imposter.
    pub use rift_mock_core::extensions::flow_state::{
        CasOutcome, FlowStore, FlowStoreProvider, NoOpFlowStore,
    };

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

    /// Proxy recordings and the `proxyOnce` record-once claim gate.
    pub use rift_mock_core::recording::{
        ClaimOutcome, ClaimToken, LocalProxyStore, ProxyRecordingStore, ProxyStoreError,
    };

    /// Incremental config reconciliation: the per-port / per-stub apply path
    /// that replaces reset-the-world reload, plus the change-event hook and the
    /// stable stub identity both sides key on.
    pub use rift_mock_core::imposter::{
        ApplyReport, ImposterEvent, ImposterEventListener, ImposterManager, TlsDefaults, stub_key,
    };

    /// Backend-outage reporting and response decoration: how a backend surfaces
    /// as a structured 503, and how per-request annotations become response
    /// headers without the OSS handlers knowing what they mean.
    pub use rift_mock_core::extensions::decorate::{
        BackendUnavailable, ResponseDecorator, ResponsePhase, annotate, backend_error_response,
        with_annotation_scope,
    };

    /// Server composition: the bootstrap builder, the metrics listener, and the
    /// single-port gateway dispatch an enterprise binary composes rather than
    /// forking.
    pub use rift_http_proxy::gateway::dispatch_to_port;
    pub use rift_http_proxy::server::{
        Cli, Commands, RunningServer, ServerBuilder, run_metrics_server,
    };
}

/// Build edition marker, surfaced in banners and `--version` output.
pub const EDITION: &str = "enterprise";

/// Returns the semantic version of this enterprise build.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    /// Names every seam the enterprise crates build against.
    ///
    /// The point is the compile, not the assertion: an upstream rename, move,
    /// or visibility change breaks this test rather than surfacing later as a
    /// confusing error inside `rift-cluster`. When upstream does move a seam,
    /// fix the re-export in `seams` — do not delete the line here.
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
        assert_object_safe::<dyn ResponseDecorator>();

        // Built-in implementations the `--cluster`-off path keeps using.
        let _: fn() -> LocalSequencer = LocalSequencer::default;
        let _: fn() -> LocalJournal = LocalJournal::default;
        let _: fn() -> ImposterManager = ImposterManager::new;
        let _: &dyn FlowStore = &NoOpFlowStore;

        // Value types that cross the seam boundary.
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
        _named::<ClaimToken>();
        _named::<ProxyStoreError>();
        _named::<LocalProxyStore>();
        _named::<ApplyReport>();
        _named::<ImposterEvent>();
        _named::<BackendUnavailable>();
        _named::<TlsDefaults>();
        _named::<ServerBuilder>();
        _named::<RunningServer>();
        _named::<Cli>();
        _named::<Commands>();
        let _ = (annotate, backend_error_response, dispatch_to_port);
        let _ = (
            run_metrics_server,
            with_annotation_scope::<std::future::Ready<()>>,
        );
    }

    #[test]
    fn version_is_the_crate_version() {
        assert_eq!(super::version(), env!("CARGO_PKG_VERSION"));
        assert_eq!(super::EDITION, "enterprise");
    }
}
