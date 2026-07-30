//! Gate tests for the pull orchestration (issue #134).
//!
//! The state-machine half of sources is covered in `raft::store`; what is
//! proven here is the part that must NOT be in the apply path: that a pull
//! fetches once, that identical content produces no log entry at all, and that
//! the digest is stable across the things a document may legitimately differ in.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rift_cluster_base::seams::{
    FetchedImposters, ImposterConfig, ImposterSource, SourceMeta, SourceRef, SourceRegistry,
};

use super::{PullError, SourcePuller, bootstrap_id, canonical, check_credential_use, digest_of};

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

// -- issue #136 review, B4: authRef refused for a scheme that consumes none -

/// A [`SourceProviders`] wired the way production is: one upstream,
/// non-credentialed scheme (`counting`, standing in for `file:`/a bespoke
/// embedder provider), and the two real HTTP-based cluster providers this
/// crate ships (`s3:`, `registry:`) registered as credentialed. Real
/// `S3Source`/`RegistrySource` rather than a hand-rolled `CredentialedSource`
/// stub, so `check_credential_use`'s refusal is proven against the actual
/// production providers `authRef` is meant to reach — not just against
/// whatever a test fixture happens to claim. (`git+https:`/`git+file:` is not
/// included here: `GitSource::new` probes a `git` binary at construction,
/// which is an environment dependency this otherwise pure-logic test does not
/// need — the same `SourceProviders`-level check applies to it identically,
/// since `check_credential_use` never looks past the credentialed map.)
fn providers_with_the_real_credentialed_schemes() -> super::SourceProviders {
    let mut upstream = SourceRegistry::new();
    upstream
        .register(Arc::new(CountingSource {
            fetches: Arc::new(AtomicUsize::new(0)),
            configs: vec![],
            version: None,
        }))
        .expect("register the non-credentialed upstream scheme");
    let mut providers = super::SourceProviders::new(upstream);

    let resolver: Arc<dyn super::auth::CredentialResolver> =
        Arc::new(super::auth::StandardResolver::new(None));
    providers
        .register_credentialed(Arc::new(
            super::s3::S3Source::new(
                Arc::clone(&resolver),
                super::s3::S3Config {
                    endpoint: None,
                    region: "us-east-1".to_owned(),
                },
            )
            .expect("build s3 source"),
        ))
        .expect("register s3 as credentialed");
    providers
        .register_credentialed(Arc::new(
            super::registry::RegistrySource::new(
                resolver,
                super::registry::RegistryConfig {
                    endpoint: "http://registry.invalid".to_owned(),
                    imposters_pointer: "/data/imposters".to_owned(),
                },
            )
            .expect("build registry source"),
        ))
        .expect("register registry as credentialed");
    providers
}

/// The refusal this check exists for: before it, `POST /admin/sources` with
/// `{ uri: "counting://…", authRef: "tok" }` would be accepted and then
/// fetched anonymously forever, silently, because the upstream
/// `ImposterSource` path has no seam to receive `auth_ref` at all.
#[test]
fn check_credential_use_refuses_an_auth_ref_on_a_scheme_that_consumes_none() {
    let puller = SourcePuller::new(providers_with_the_real_credentialed_schemes());
    let err = check_credential_use(&puller, Some("tok"), "counting://host/x.json")
        .expect_err("`counting` never consumes a credential");
    let rendered = err.to_string();
    assert!(
        rendered.contains("counting"),
        "the refusal must name the offending scheme: {rendered}"
    );
    assert!(
        rendered.contains("s3") && rendered.contains("registry"),
        "the refusal must say which schemes do take a credential: {rendered}"
    );
}

/// The other half: `authRef` on a scheme that genuinely consumes one — the
/// two real HTTP providers this build ships — must still be accepted.
#[test]
fn check_credential_use_accepts_an_auth_ref_on_the_real_credentialed_schemes() {
    let puller = SourcePuller::new(providers_with_the_real_credentialed_schemes());
    for uri in ["s3://bucket/key", "registry://svc-a"] {
        assert!(
            check_credential_use(&puller, Some("tok"), uri).is_ok(),
            "uri {uri} takes a credential and must accept one"
        );
    }
}

/// No `authRef` at all is always fine, on any scheme — the ordinary anonymous
/// path this check must never disturb.
#[test]
fn check_credential_use_accepts_no_auth_ref_on_any_scheme() {
    let puller = SourcePuller::new(providers_with_the_real_credentialed_schemes());
    for uri in [
        "counting://host/x.json",
        "s3://bucket/key",
        "registry://svc-a",
    ] {
        assert!(
            check_credential_use(&puller, None, uri).is_ok(),
            "uri {uri}: no authRef is always fine"
        );
    }
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
