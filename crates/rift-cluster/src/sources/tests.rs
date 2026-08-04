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

use crate::control::DEFAULT_TENANT;

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
    match puller.pull(DEFAULT_TENANT, "mocks", None).await {
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

// -- unavailable schemes (#270) ---------------------------------------------
//
// The `-static` image flavor ships no `git` binary, so it cannot back `git+`.
// The requirement is not merely that it boots: it is that the scheme stays
// *nameable*. A provider that simply went unregistered would make an image
// decision indistinguishable from an operator's typo — the silent failure the
// source rules forbid.

fn counting_upstream() -> SourceRegistry {
    let mut upstream = SourceRegistry::new();
    upstream
        .register(Arc::new(CountingSource {
            fetches: Arc::new(AtomicUsize::new(0)),
            configs: vec![],
            version: None,
        }))
        .expect("register the upstream scheme");
    upstream
}

/// Must stay byte-identical to `compose.rs`'s private `NO_GIT_REASON`, which
/// this crate cannot reach. The assertions below match on substrings, so a
/// reword over there does not fail here loudly — it quietly makes these tests
/// assert less than they claim. Change both.
const NO_GIT: &str = "no `git` binary on PATH; install git, or use the default (non-static) image if this is `-static`";

fn providers_with_git_unavailable() -> super::SourceProviders {
    let mut providers = super::SourceProviders::new(counting_upstream());
    providers
        .register_unavailable(super::git::GIT_SCHEMES, NO_GIT)
        .expect("git+ is unserved here, so it may be marked unavailable");
    providers
}

#[test]
fn an_unavailable_scheme_is_named_rather_than_missing() {
    let providers = providers_with_git_unavailable();
    assert!(
        !providers.schemes().contains(&"git+https".to_owned()),
        "an unavailable scheme is not a served one"
    );
    assert_eq!(
        providers.unavailable_schemes(),
        vec!["git+file".to_owned(), "git+https".to_owned()],
        "both git schemes stay enumerable so a listing can say why they are off"
    );
}

#[test]
fn declaring_an_unavailable_scheme_names_the_cause_and_the_fix() {
    let providers = providers_with_git_unavailable();
    let refusal = providers
        .scheme_refusal("git+https://example.com/repo#main:mocks.json")
        .expect("a scheme this build cannot fetch is refused");

    assert!(
        refusal.contains("`git+https:` sources are unavailable"),
        "the refusal must name the scheme: {refusal}"
    );
    assert!(
        refusal.contains("no `git` binary on PATH"),
        "the refusal must name the cause: {refusal}"
    );
    assert!(
        refusal.contains("use the default (non-static) image"),
        "the refusal must name the fix: {refusal}"
    );
    // The whole point of the unavailable map: this must NOT read as though the
    // operator invented a scheme that does not exist.
    assert!(
        !refusal.contains("no imposter source is registered"),
        "an unavailable scheme must never be reported as an unknown one: {refusal}"
    );
}

/// `git+file:` reaches [`super::git::GitSource`] only in its `git+file://<path>`
/// form, and the refusal funnel inherits exactly that.
///
/// Pinned here because **two scheme parsers disagree** about the single-colon
/// spelling, which is a trap for anyone reading either one alone:
/// upstream's `SourceRef::scheme` splits on `"://"` and calls
/// `git+file:/srv/x` a `file:` URI, while this crate's
/// `control::require_well_formed_uri` splits on `':'` and calls the same string
/// `git+file` — so it is *validated* as git and then *routed* to `FileSource`.
///
/// That disagreement predates this issue and is identical on both image
/// flavors (a git-present node routes it to `FileSource` too, and fails on a
/// literal path). #270 neither causes it nor fixes it — filed separately — but
/// the parse is asserted here so that a change to either parser fails a test
/// instead of silently moving which provider a URI lands on.
#[test]
fn only_the_double_slash_git_file_spelling_routes_to_git() {
    let providers = providers_with_git_unavailable();

    let refusal = providers
        .scheme_refusal("git+file:///srv/mocks.git#main:m.json")
        .expect("the `//` form parses as git+file and is refused");
    assert!(
        refusal.contains("`git+file:` sources are unavailable"),
        "{refusal}"
    );

    assert_eq!(
        super::SourceRef::new("git+file:///srv/mocks.git#main:m.json").scheme(),
        "git+file",
        "the documented `//` spelling is what routes to the git provider"
    );
    assert_eq!(
        super::SourceRef::new("git+file:/srv/mocks.git#main:m.json").scheme(),
        "file",
        "the single-colon spelling is a `file:` URI to the fetch path, whatever \
         control-plane validation calls it"
    );

    // #301: the two parsers can no longer *silently* disagree, because the
    // spelling they disagree about is refused before it can be committed. This
    // assertion is what keeps the disagreement above from being re-opened by a
    // change to either parser without a test failing.
    let err = crate::control::validate(&source_put_op("git+file:/srv/mocks.git#main:m.json"))
        .expect_err("control-plane validation must refuse the single-colon spelling");
    assert!(err.contains("git+file://"), "{err}");
}

/// A `SourcePut` carrying `uri`, for asserting what control-plane validation
/// does with a spelling this module has just pinned the *routing* of.
fn source_put_op(uri: &str) -> crate::control::ControlOp {
    crate::control::ControlOp::SourcePut {
        tenant: crate::control::TenantId::default(),
        id: "mocks".to_owned(),
        uri: uri.to_owned(),
        mode: crate::control::SourceMode::Pinned,
        auth_ref: None,
        on_drift: crate::control::OnDrift::Overwrite,
        poll_secs: None,
    }
}

/// The two `check_remote` guards that the `//` spelling leaves vacuous.
///
/// A `git+file:` remote is whatever follows `git+file:` — so for the only
/// spelling #301 now admits it always begins `//`, which can neither start with
/// `-` nor be relative. Those two guards therefore stop being reachable through
/// admission, and this is where they keep being proven. They still matter:
/// `parse_git_uri` calls `check_remote` at fetch time on whatever a stored op
/// holds, and the guards are the reason "a URI may name a corpus" does not
/// become "a URI may run a command as the rift process, fleet-wide".
#[test]
fn check_remote_refuses_option_shaped_and_relative_file_remotes() {
    let err = super::git::check_remote("--upload-pack=/tmp/pwn.sh", true)
        .expect_err("a remote git would read as an option must be refused");
    assert!(err.to_string().contains("option"), "{err}");

    let err = super::git::check_remote("relative/path", true)
        .expect_err("a relative git+file: remote must be refused");
    assert!(err.to_string().contains("absolute"), "{err}");

    let err = super::git::check_remote("ext::sh -c whoami", true)
        .expect_err("a transport helper remote must be refused");
    assert!(err.to_string().contains("transport"), "{err}");

    super::git::check_remote("///srv/mocks.git", true)
        .expect("the `//` spelling's remote is what a legal git+file: uri yields");
}

#[test]
fn an_unknown_scheme_refusal_still_names_what_is_unavailable() {
    let providers = providers_with_git_unavailable();
    let refusal = providers
        .scheme_refusal("ftp://example.com/mocks.json")
        .expect("ftp is served by nothing");

    assert!(
        refusal.contains("no imposter source is registered for the `ftp:` scheme"),
        "{refusal}"
    );
    // An operator who typed `git+http:` on a static image lands here, and a
    // list that quietly omitted `git+https:` would send them hunting for a
    // spelling mistake instead of at the flavor.
    assert!(
        refusal.contains("unavailable in this build: git+file, git+https"),
        "an unknown-scheme refusal must still disclose the disabled schemes: {refusal}"
    );
}

#[test]
fn a_served_scheme_earns_no_refusal() {
    let providers = providers_with_git_unavailable();
    assert!(
        providers.scheme_refusal("counting://host/x.json").is_none(),
        "marking git+ unavailable must not disturb the schemes this build does serve"
    );
}

#[test]
fn a_scheme_is_either_served_or_unavailable_never_both() {
    let mut providers = super::SourceProviders::new(counting_upstream());
    let err = providers
        .register_unavailable(&["counting"], NO_GIT)
        .expect_err("a scheme with a live provider may not also be marked unavailable");
    assert!(
        err.to_string()
            .contains("both served and marked unavailable"),
        "{err}"
    );
}

#[test]
fn a_scheme_may_not_be_explained_twice() {
    let mut providers = providers_with_git_unavailable();
    let err = providers
        .register_unavailable(&["git+https"], "some other reason")
        .expect_err("a second registration would silently replace the first one's reason");
    assert!(
        err.to_string().contains("already marked unavailable"),
        "{err}"
    );
    assert!(
        providers
            .scheme_refusal("git+https://h/r#main:m.json")
            .expect("still refused")
            .contains("no `git` binary on PATH"),
        "the original reason must survive a refused re-registration"
    );
}

#[test]
fn a_puller_refuses_an_unavailable_scheme_through_the_same_funnel() {
    // Both declaration paths and `pull` route through `scheme_refusal`; this is
    // the pass-through that carries it from the provider set to the puller the
    // fronts actually hold.
    let puller = SourcePuller::new(providers_with_git_unavailable());
    let refusal = puller
        .scheme_refusal("git+file:///srv/mocks#main:m.json")
        .expect("refused");
    assert!(
        refusal.contains("`git+file:` sources are unavailable"),
        "{refusal}"
    );
    assert_eq!(
        puller.unavailable_schemes(),
        vec!["git+file".to_owned(), "git+https".to_owned()]
    );
}

/// The probe's own classification, exercised directly rather than through a
/// hand-built error value.
///
/// This is the decision the whole `-static` flavor rests on: `NotFound` boots a
/// node with `git+` disabled, and *every other* failure refuses the boot. A
/// non-executable file is the cheap, sound way to produce the second case —
/// POSIX answers `EACCES`, not `ENOENT` — with no `PATH` mutation and so no
/// interference between parallel tests. Without this, nothing proves
/// `probe_program` distinguishes them; the compose-arm tests are handed the
/// answer.
#[test]
fn the_probe_calls_a_non_executable_file_unusable_not_absent() {
    // `Cargo.toml` is present in the crate root at test time and is not
    // executable — exactly the "a git exists but cannot be run" shape.
    let err = super::git::GitSource::probe_program("./Cargo.toml")
        .expect_err("a non-executable file is not a usable git");

    assert!(
        matches!(err, super::git::GitProbeError::SpawnFailed(_)),
        "a present-but-unrunnable binary must NOT be classified as absent — that \
         would boot a node with git+ silently disabled instead of refusing: {err:?}"
    );
}

#[test]
fn the_probe_calls_a_missing_binary_absent() {
    let err = super::git::GitSource::probe_program("rift-no-such-binary-anywhere")
        .expect_err("a missing binary is not a usable git");

    assert!(
        matches!(err, super::git::GitProbeError::NotFound(_)),
        "absence is the one arm allowed to degrade, so it must be recognised: {err:?}"
    );
}

/// The disjointness invariant is enforced from *both* registration functions;
/// `a_scheme_is_either_served_or_unavailable_never_both` covers one direction,
/// this covers the other. Worth testing separately because `scheme_refusal`
/// consults `serves()` first, so a scheme that wrongly landed in both maps
/// would produce **no symptom at all** there — it would simply read as served.
#[test]
fn a_scheme_already_unavailable_may_not_then_be_served() {
    let mut providers = super::SourceProviders::new(counting_upstream());
    providers
        .register_unavailable(&["counting-2"], NO_GIT)
        .expect("unserved, so it may be marked unavailable");

    let err = providers
        .register_credentialed(Arc::new(ClaimingSource {
            schemes: &["counting-2"],
        }))
        .expect_err("a scheme marked unavailable may not also acquire a provider");
    assert!(
        err.to_string()
            .contains("both served and marked unavailable"),
        "{err}"
    );
}

/// Claims a fixed scheme list and nothing else — enough to drive the
/// registration guards without standing up a real provider.
#[derive(Debug)]
struct ClaimingSource {
    schemes: &'static [&'static str],
}

impl super::CredentialedSource for ClaimingSource {
    fn schemes(&self) -> &'static [&'static str] {
        self.schemes
    }

    fn fetch_with_auth<'a>(
        &'a self,
        _r: &'a SourceRef,
        _auth_ref: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<FetchedImposters>> + Send + 'a>,
    > {
        Box::pin(async { anyhow::bail!("ClaimingSource never fetches") })
    }
}
