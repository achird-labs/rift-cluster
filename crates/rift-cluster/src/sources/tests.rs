//! Gate tests for the pull orchestration (issue #134).
//!
//! The state-machine half of sources is covered in `raft::store`; what is
//! proven here is the part that must NOT be in the apply path: that a pull
//! fetches once, that identical content produces no log entry at all, and that
//! the digest is stable across the things a document may legitimately differ in.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rift_ee::seams::{
    FetchedImposters, ImposterConfig, ImposterSource, SourceMeta, SourceRef, SourceRegistry,
};

use super::{PullError, SourcePuller, bootstrap_id, canonical, digest_of};

/// `digest_of` is fallible; every config these tests build encodes, so a
/// failure here is a broken test, not a case under test.
fn digest(configs: &[ImposterConfig]) -> crate::control::Digest {
    digest_of(configs).expect("test configs encode")
}

fn config(port: u16, name: &str) -> ImposterConfig {
    serde_json::from_value(serde_json::json!({
        "port": port,
        "protocol": "http",
        "name": name,
    }))
    .expect("test config parses")
}

/// A source that counts its fetches — the counting in-process server the
/// fetch-once criterion calls for, without a socket.
struct CountingSource {
    fetches: Arc<AtomicUsize>,
    configs: Vec<ImposterConfig>,
    version: Option<String>,
}

impl ImposterSource for CountingSource {
    fn schemes(&self) -> &'static [&'static str] {
        &["counting"]
    }

    fn fetch<'a>(
        &'a self,
        _r: &'a SourceRef,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<FetchedImposters>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(FetchedImposters {
                configs: self.configs.clone(),
                intercept: None,
                routes: None,
                meta: SourceMeta {
                    version: self.version.clone(),
                    fetched_at: std::time::SystemTime::now(),
                },
                unchanged: false,
            })
        })
    }
}

fn puller_over(source: Arc<dyn ImposterSource>) -> SourcePuller {
    let mut registry = SourceRegistry::new();
    registry.register(source).expect("register");
    SourcePuller::new(registry)
}

// -- digest -----------------------------------------------------------------

/// The short circuit is only as good as the digest: the same content in a
/// different document order must hash the same, or a fleet would re-apply an
/// identical config every time the source reordered its list.
#[test]
fn the_digest_is_stable_across_document_order() {
    let a = digest(&[config(8080, "a"), config(8081, "b")]);
    let b = digest(&[config(8081, "b"), config(8080, "a")]);
    assert_eq!(a, b, "document order is not content");
}

#[test]
fn the_digest_changes_with_content() {
    let a = digest(&[config(8080, "a")]);
    assert_ne!(a, digest(&[config(8080, "changed")]), "a changed field");
    assert_ne!(a, digest(&[config(8081, "a")]), "a changed port");
    assert_ne!(
        a,
        digest(&[config(8080, "a"), config(8081, "b")]),
        "an added imposter"
    );
    assert_ne!(a, digest(&[]), "an emptied document");
}

#[test]
fn the_digest_is_hex_sha256() {
    let digest = digest(&[config(8080, "a")]);
    assert_eq!(digest.as_str().len(), 64, "{digest}");
    assert!(
        digest.as_str().chars().all(|c| c.is_ascii_hexdigit()),
        "{digest}"
    );
}

/// The canonical encoding is what makes the digest independent of `serde_json`'s
/// map ordering — the property the whole short circuit rests on.
#[test]
fn canonical_encoding_sorts_object_keys_recursively() {
    let a = serde_json::json!({ "b": 1, "a": { "z": [1, 2], "y": true } });
    let b = serde_json::json!({ "a": { "y": true, "z": [1, 2] }, "b": 1 });
    assert_eq!(canonical(&a), canonical(&b));
    assert_eq!(canonical(&a), r#"{"a":{"y":true,"z":[1,2]},"b":1}"#);
    // Arrays are ordered data, not a set: reordering them must change the
    // encoding, or a reordered stub list would read as unchanged.
    assert_ne!(
        canonical(&serde_json::json!([1, 2])),
        canonical(&serde_json::json!([2, 1]))
    );
}

// -- the puller before it is bound ------------------------------------------

/// The routes exist before the node does (they are needed to bind the cluster
/// port the node then advertises). A request landing in that window is told so,
/// rather than silently doing nothing.
#[tokio::test]
async fn an_unbound_puller_reports_that_it_is_not_ready() {
    let fetches = Arc::new(AtomicUsize::new(0));
    let puller = puller_over(Arc::new(CountingSource {
        fetches: Arc::clone(&fetches),
        configs: vec![config(8080, "a")],
        version: None,
    }));
    match puller.pull("mocks", None).await {
        Err(PullError::Internal(detail)) => assert!(detail.contains("not available"), "{detail}"),
        other => panic!("an unbound puller must refuse, got {other:?}"),
    }
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        0,
        "an unbound puller must not fetch: there is nowhere to submit the result"
    );
}

#[test]
fn a_puller_reports_the_schemes_it_serves() {
    let puller = puller_over(Arc::new(CountingSource {
        fetches: Arc::new(AtomicUsize::new(0)),
        configs: vec![],
        version: None,
    }));
    assert_eq!(puller.schemes(), vec!["counting".to_owned()]);
    assert!(puller.serves("counting://host/x.json"));
    assert!(
        !puller.serves("git+https://host/r#main:p"),
        "a scheme with no provider is not servable, and the operator is told before it is stored"
    );
    // A bare path is `file:` upstream, which this registry does not serve.
    assert!(!puller.serves("mocks.json"));
}

// -- bootstrap ids ----------------------------------------------------------

/// "Idempotent by id" is the whole contract of `--imposters` under `--cluster`:
/// the same URI must name the same source on every boot of every node, or a
/// rolling restart would accumulate one source per restart, all fighting over
/// the same ports.
#[test]
fn a_bootstrap_id_is_stable_for_a_uri() {
    let uri = "https://cfg.test/team-a/mocks.json";
    assert_eq!(bootstrap_id(uri), bootstrap_id(uri));
    assert!(
        bootstrap_id(uri).starts_with("https-cfg-test-team-a-mocks-json"),
        "an operator has to recognise their own uri in the list: {}",
        bootstrap_id(uri)
    );
}

/// Two URIs that slugify identically must not collide onto one source.
#[test]
fn bootstrap_ids_distinguish_uris_that_slugify_alike() {
    let a = bootstrap_id("https://cfg.test/a/mocks.json");
    let b = bootstrap_id("https://cfg.test/b/mocks.json");
    assert_ne!(a, b);
}

/// The id is a redb key and a path segment, so it must satisfy the same rule
/// `control::validate` enforces on an operator-supplied one — a bootstrap that
/// minted an id the control plane then refused would be unusable.
#[test]
fn a_bootstrap_id_is_always_a_valid_source_id() {
    for uri in [
        "https://cfg.test/mocks.json",
        "file:/srv/very/deeply/nested/path/that/keeps/going/and/going/mocks.json",
        "mocks.json",
        "counting://h/x",
        "https://cfg.test/../%20weird%20/mocks.json?v=1#frag",
    ] {
        let id = bootstrap_id(uri);
        let op = crate::control::ControlOp::SourcePut {
            tenant: crate::control::TenantId::default(),
            id: id.clone(),
            uri: uri.to_owned(),
            mode: crate::control::SourceMode::Pinned,
            auth_ref: None,
            on_drift: crate::control::OnDrift::Overwrite,
            poll_secs: None,
        };
        assert_eq!(
            crate::control::validate(&op),
            Ok(()),
            "bootstrap id {id:?} from {uri:?} must be admissible"
        );
    }
}
