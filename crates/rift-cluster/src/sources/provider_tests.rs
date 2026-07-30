//! Gate tests for the cluster source providers (issue #136).
//!
//! Every provider is exercised against a **local fixture** — a temp git repo, a
//! stub HTTP server standing in for S3, a stub registry — so the suite makes no
//! network egress. What each test is really pinning down:
//!
//! * the URI shape the provider claims, and what it refuses;
//! * where `version` comes from, and that identical content re-fetches to the
//!   *same* version (which is what makes #134's digest short circuit fire);
//! * that a named credential is used when it resolves, and that an unresolvable
//!   one is an **error rather than an anonymous fetch** — the fail-closed rule;
//! * that no secret material reaches an error string, a `Debug`, or a request
//!   the provider did not intend to authenticate.

use std::collections::HashMap;
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rift_cluster_base::seams::SourceRef;

use super::auth::{AuthError, Credential, CredentialResolver};
use super::git::GitSource;
use super::registry::{RegistryConfig, RegistrySource};
use super::s3::{S3Config, S3Source};
use super::{CredentialedSource, SourceProviders};

// -- fixtures ---------------------------------------------------------------

/// A resolver over a fixed map. The production resolver's *order* is proven in
/// `auth::tests`; what the provider tests need is only "this ref resolves to
/// this secret, that one does not".
#[derive(Debug)]
struct MapResolver(HashMap<String, String>);

impl MapResolver {
    fn new(entries: &[(&str, &str)]) -> Arc<Self> {
        Arc::new(Self(
            entries
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        ))
    }

    fn empty() -> Arc<Self> {
        Arc::new(Self(HashMap::new()))
    }
}

impl CredentialResolver for MapResolver {
    fn resolve(&self, auth_ref: &str) -> Result<Credential, AuthError> {
        self.0
            .get(auth_ref)
            .map(Credential::new)
            .ok_or_else(|| AuthError::Unresolved(auth_ref.to_owned(), auth_ref.to_uppercase()))
    }
}

/// One request the stub server saw.
#[derive(Debug, Clone)]
struct Recorded {
    target: String,
    headers: Vec<(String, String)>,
}

impl Recorded {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// A minimal HTTP/1.1 server that answers every request from a script and
/// records what it was asked.
///
/// Hand-rolled rather than pulled from a crate: the providers under test are
/// the only clients, the responses are fixed strings, and a fixture that is one
/// screen of code is easier to trust than a dependency when the thing being
/// proven is "exactly these bytes went over the wire".
struct StubServer {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<Recorded>>>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

impl StubServer {
    /// `respond` maps a request target to `(status, headers, body)`.
    async fn start(
        respond: impl Fn(&Recorded) -> (u16, Vec<(String, String)>, String) + Send + Sync + 'static,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub server");
        let addr = listener.local_addr().expect("stub addr");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (tx, mut rx) = tokio::sync::oneshot::channel();

        let recorder = Arc::clone(&seen);
        tokio::spawn(async move {
            let respond = Arc::new(respond);
            loop {
                let accepted = tokio::select! {
                    biased;
                    _ = &mut rx => return,
                    accepted = listener.accept() => accepted,
                };
                let Ok((stream, _)) = accepted else { return };
                let recorder = Arc::clone(&recorder);
                let respond = Arc::clone(&respond);
                tokio::spawn(async move {
                    let _ = serve_one(stream, &recorder, respond.as_ref()).await;
                });
            }
        });

        Self {
            addr,
            seen,
            _shutdown: tx,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn requests(&self) -> Vec<Recorded> {
        self.seen.lock().expect("stub lock").clone()
    }
}

async fn serve_one(
    mut stream: tokio::net::TcpStream,
    seen: &Mutex<Vec<Recorded>>,
    respond: &(dyn Fn(&Recorded) -> (u16, Vec<(String, String)>, String) + Send + Sync),
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let mut raw = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
        if raw.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    let text = String::from_utf8_lossy(&raw).into_owned();
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or_default().to_owned();
    let target = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned();
    let headers: Vec<(String, String)> = lines
        .take_while(|line| !line.is_empty())
        .filter_map(|line| {
            line.split_once(':')
                .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
        })
        .collect();

    let recorded = Recorded { target, headers };
    seen.lock().expect("stub lock").push(recorded.clone());

    let (status, extra, body) = respond(&recorded);
    let mut response = format!(
        "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n",
        body.len()
    );
    for (k, v) in extra {
        response.push_str(&format!("{k}: {v}\r\n"));
    }
    response.push_str("\r\n");
    response.push_str(&body);
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

const DOCUMENT: &str = r#"{"imposters":[{"port":8080,"protocol":"http","name":"from-source"}]}"#;
const OTHER_DOCUMENT: &str = r#"{"imposters":[{"port":8081,"protocol":"http","name":"changed"}]}"#;

fn ok_json(
    body: &'static str,
    etag: Option<&'static str>,
) -> impl Fn(&Recorded) -> (u16, Vec<(String, String)>, String) + Send + Sync + 'static {
    move |_| {
        let headers = etag
            .map(|e| vec![("etag".to_owned(), e.to_owned())])
            .unwrap_or_default();
        (200, headers, body.to_owned())
    }
}

/// A git repo with one commit, returning `(dir, commit_sha)`.
fn temp_git_repo(files: &[(&str, &str)]) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path();

    let run = |args: &[&str]| {
        let out = Command::new("git")
            .args(args)
            .current_dir(path)
            .output()
            .unwrap_or_else(|e| panic!("git {args:?}: {e}"));
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    };

    run(&["-c", "init.defaultBranch=main", "init", "--quiet"]);
    run(&["config", "user.email", "fixture@example.invalid"]);
    run(&["config", "user.name", "fixture"]);
    for (name, content) in files {
        let file = path.join(name);
        if let Some(parent) = file.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        let mut f = std::fs::File::create(&file).expect("create fixture file");
        f.write_all(content.as_bytes()).expect("write fixture file");
    }
    run(&["add", "."]);
    run(&["commit", "--quiet", "-m", "fixture"]);
    let sha = run(&["rev-parse", "HEAD"]);
    (dir, sha)
}

fn git_uri(repo: &Path, git_ref: &str, path: &str) -> String {
    format!("git+file:{}#{git_ref}:{path}", repo.display())
}

/// Skip a git test when no `git` binary is available, rather than failing for a
/// reason that has nothing to do with the code under test.
fn git_available() -> bool {
    Command::new("git")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

async fn fetch(
    provider: &dyn CredentialedSource,
    uri: &str,
    auth_ref: Option<&str>,
) -> anyhow::Result<rift_cluster_base::seams::FetchedImposters> {
    let source_ref = SourceRef::new(uri);
    provider.fetch_with_auth(&source_ref, auth_ref).await
}

// -- AC1: git ---------------------------------------------------------------

/// The core git claim: a single file at a named ref, with `version` = the
/// commit sha. If `version` were anything else (a timestamp, the file hash),
/// #134's provenance would stop naming a commit an operator can check out.
#[tokio::test]
async fn git_source_fetches_a_file_at_a_ref() {
    if !git_available() {
        return;
    }
    let (repo, sha) = temp_git_repo(&[("imposters.json", DOCUMENT)]);
    let source = GitSource::new(MapResolver::empty()).expect("git available");

    let fetched = fetch(
        &source,
        &git_uri(repo.path(), "main", "imposters.json"),
        None,
    )
    .await
    .expect("fetches");

    assert_eq!(fetched.configs.len(), 1, "one imposter in the document");
    assert_eq!(fetched.configs[0].port, Some(8080));
    assert_eq!(
        fetched.meta.version.as_deref(),
        Some(sha.as_str()),
        "version must be the commit sha, so provenance names something checkoutable"
    );
}

/// A directory path merges every document under it — the multi-document rule
/// U-12 applies across sources, applied here across files.
#[tokio::test]
async fn git_source_fetches_a_directory() {
    if !git_available() {
        return;
    }
    let (repo, _) = temp_git_repo(&[("mocks/a.json", DOCUMENT), ("mocks/b.json", OTHER_DOCUMENT)]);
    let source = GitSource::new(MapResolver::empty()).expect("git available");

    let fetched = fetch(&source, &git_uri(repo.path(), "main", "mocks"), None)
        .await
        .expect("fetches");

    let mut ports: Vec<u16> = fetched.configs.iter().filter_map(|c| c.port).collect();
    ports.sort_unstable();
    assert_eq!(
        ports,
        vec![8080, 8081],
        "both documents in the directory must be merged"
    );
}

/// Two documents claiming one port is an error naming both, not a silent
/// last-one-wins — the operator wrote both expecting both to serve.
#[tokio::test]
async fn git_source_refuses_a_port_declared_twice_in_a_directory() {
    if !git_available() {
        return;
    }
    let (repo, _) = temp_git_repo(&[("mocks/a.json", DOCUMENT), ("mocks/b.json", DOCUMENT)]);
    let source = GitSource::new(MapResolver::empty()).expect("git available");

    let err = fetch(&source, &git_uri(repo.path(), "main", "mocks"), None)
        .await
        .expect_err("a duplicated port is refused");
    let rendered = err.to_string();
    assert!(
        rendered.contains("8080"),
        "the error must name the port: {rendered}"
    );
    assert!(
        rendered.contains("a.json") && rendered.contains("b.json"),
        "the error must name both documents: {rendered}"
    );
}

/// AC4 for git: the same commit re-fetches to the same version, which is what
/// lets #134 skip the write entirely.
#[tokio::test]
async fn git_re_fetch_of_an_unchanged_ref_reports_the_same_version() {
    if !git_available() {
        return;
    }
    let (repo, sha) = temp_git_repo(&[("imposters.json", DOCUMENT)]);
    let source = GitSource::new(MapResolver::empty()).expect("git available");
    let uri = git_uri(repo.path(), "main", "imposters.json");

    let first = fetch(&source, &uri, None).await.expect("first fetch");
    let second = fetch(&source, &uri, None).await.expect("second fetch");

    assert_eq!(first.meta.version, second.meta.version);
    assert_eq!(second.meta.version.as_deref(), Some(sha.as_str()));
}

#[tokio::test]
async fn git_source_reports_a_missing_path_rather_than_serving_nothing() {
    if !git_available() {
        return;
    }
    let (repo, _) = temp_git_repo(&[("imposters.json", DOCUMENT)]);
    let source = GitSource::new(MapResolver::empty()).expect("git available");

    let err = fetch(&source, &git_uri(repo.path(), "main", "absent.json"), None)
        .await
        .expect_err("a missing path is an error");
    assert!(
        err.to_string().contains("absent.json"),
        "the error must name what was missing: {err}"
    );
}

/// Fail-closed, git edition: a source that names a credential and cannot get
/// one must NOT fall back to an anonymous clone. A public-repo fallback would
/// silently serve the wrong thing for a repo that later became private.
#[tokio::test]
async fn git_an_unresolvable_credential_is_an_error_not_an_anonymous_fetch() {
    if !git_available() {
        return;
    }
    let (repo, _) = temp_git_repo(&[("imposters.json", DOCUMENT)]);
    let source = GitSource::new(MapResolver::empty()).expect("git available");

    let err = fetch(
        &source,
        &git_uri(repo.path(), "main", "imposters.json"),
        Some("gh-token"),
    )
    .await
    .expect_err("an unresolvable auth_ref must fail the fetch");

    assert!(
        err.to_string().contains("gh-token"),
        "the error must name the ref an operator has to fix: {err}"
    );
}

/// AC7, git edition: whatever `git` says on failure, the token must not be in
/// it. A token spliced into the remote URL is the classic way this leaks.
#[tokio::test]
async fn git_errors_never_echo_the_token() {
    if !git_available() {
        return;
    }
    let source =
        GitSource::new(MapResolver::new(&[("gh-token", "ghp_supersecrettoken")])).expect("git");

    // A remote that cannot exist, so the failure path renders whatever git and
    // the provider have to say about it.
    let err = fetch(
        &source,
        "git+file:/nonexistent-repo-fixture#main:imposters.json",
        Some("gh-token"),
    )
    .await
    .expect_err("a missing remote fails");

    let rendered = format!("{err} {err:?} {:?}", err.chain().collect::<Vec<_>>());
    assert!(
        !rendered.contains("ghp_supersecrettoken"),
        "the token leaked into a git error: {rendered}"
    );
}

// -- issue #136 review, B1: git argument injection ---------------------------

/// A shell script that touches `marker` if it is ever actually executed —
/// the proof that a refused remote/ref was never handed to a real `git`
/// invocation, rather than merely producing an error message for some other
/// reason.
fn write_marker_script(dir: &Path) -> (PathBuf, PathBuf) {
    let marker = dir.join("pwned");
    let script = dir.join("pwn.sh");
    std::fs::write(&script, format!("#!/bin/sh\ntouch {}\n", marker.display()))
        .expect("write marker script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("chmod marker script");
    }
    (script, marker)
}

/// `git fetch --upload-pack=<cmd> <remote> <ref>` runs `<cmd>` — so a remote
/// beginning with `-` must be refused before it ever reaches `git`, not just
/// produce *an* error. The marker file is the proof: if `check_remote` were
/// deleted, this would silently execute the script instead of failing.
#[tokio::test]
async fn git_refuses_a_remote_that_would_be_read_as_an_option() {
    if !git_available() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let (script, marker) = write_marker_script(dir.path());
    let source = GitSource::new(MapResolver::empty()).expect("git available");

    let uri = format!("git+file:--upload-pack={}#main:x", script.display());
    let err = fetch(&source, &uri, None)
        .await
        .expect_err("an option-shaped remote must be refused");
    assert!(
        err.to_string().contains("option"),
        "the refusal must name why: {err}"
    );
    assert!(
        !marker.exists(),
        "the remote was executed instead of refused: {}",
        marker.display()
    );
}

/// Same bug, ref position: `git fetch <remote> --upload-pack=<cmd>` is just
/// as effective as putting it in the remote.
#[tokio::test]
async fn git_refuses_a_ref_that_would_be_read_as_an_option() {
    if !git_available() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let (script, marker) = write_marker_script(dir.path());
    let remote = dir.path().join("does-not-need-to-exist");
    let source = GitSource::new(MapResolver::empty()).expect("git available");

    let uri = format!(
        "git+file:{}#--upload-pack={}:x",
        remote.display(),
        script.display()
    );
    let err = fetch(&source, &uri, None)
        .await
        .expect_err("an option-shaped ref must be refused");
    assert!(
        err.to_string().contains("option"),
        "the refusal must name why: {err}"
    );
    assert!(
        !marker.exists(),
        "the ref was executed instead of refused: {}",
        marker.display()
    );
}

/// `ext::<command>` is a transport helper: its whole purpose is running a
/// command as the transport. This must be refused before it ever reaches
/// `git`.
#[tokio::test]
async fn git_refuses_a_transport_helper_remote() {
    if !git_available() {
        return;
    }
    let source = GitSource::new(MapResolver::empty()).expect("git available");
    let err = fetch(&source, "git+file:ext::sh -c whoami#main:x", None)
        .await
        .expect_err("an ext:: transport helper must be refused");
    assert!(
        err.to_string().contains("transport"),
        "the refusal must name why: {err}"
    );
}

/// The positive shape rules `check_remote` enforces beyond the deny-listed
/// characters: a `git+file:` remote must be an absolute path, and a
/// `git+https:`-shaped remote must actually resolve to the `https` scheme
/// once the `git+` prefix is stripped.
#[tokio::test]
async fn git_refuses_a_relative_file_remote_and_a_non_https_remote() {
    if !git_available() {
        return;
    }
    let source = GitSource::new(MapResolver::empty()).expect("git available");

    let err = fetch(&source, "git+file:relative/path.git#main:x", None)
        .await
        .expect_err("a relative git+file: remote must be refused");
    assert!(err.to_string().contains("absolute"), "{err}");

    // `git+ssh://` strips down to a plain `ssh://` remote, which is not
    // `https` — `SourceProviders` would never route this scheme to
    // `GitSource` in production (its `schemes()` is exactly `git+https`/
    // `git+file`), but calling the provider directly, as this whole file
    // does, exercises `check_remote`'s shape rule on its own terms.
    let err = fetch(&source, "git+ssh://github.com/org/repo#main:x", None)
        .await
        .expect_err("a non-https remote must be refused");
    assert!(err.to_string().contains("https"), "{err}");
}

// -- issue #136 review, B2: the git subprocess has no timeout ---------------

/// A `git+https:` remote that accepts a TCP connection and then never
/// responds — the stalled-remote case `run_git_raw`'s deadline exists for.
/// Without a timeout, `git fetch`'s TLS handshake would block the
/// `spawn_blocking` thread forever; with one, the fetch fails promptly.
#[tokio::test]
async fn git_a_stalled_remote_times_out_instead_of_hanging_forever() {
    if !git_available() {
        return;
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stalled stub");
    let addr = listener.local_addr().expect("stub addr");
    std::thread::spawn(move || {
        // Accept every connection and then do nothing: no TLS ServerHello
        // ever arrives, so a real `git` client's read blocks indefinitely —
        // exactly what a stalled remote looks like from the fetching side.
        for stream in listener.incoming().flatten() {
            std::mem::forget(stream);
        }
    });

    let source = GitSource::with_timeout(MapResolver::empty(), Duration::from_millis(300))
        .expect("git available");
    let uri = format!("git+https://127.0.0.1:{}/o/r#main:x", addr.port());

    let started = std::time::Instant::now();
    let err = fetch(&source, &uri, None)
        .await
        .expect_err("a stalled remote must fail rather than hang forever");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the injected timeout was not respected: took {:?}",
        started.elapsed()
    );
    assert!(
        err.to_string().contains("did not complete") || err.to_string().contains("killed"),
        "the error should say the invocation was killed on a timeout: {err}"
    );
}

// -- issue #136 review, B6.2: a future CI image cannot silently drop `git` --

/// Every other git test in this file quietly returns without asserting
/// anything when `git` is absent from `PATH` (see `git_available`), so a CI
/// image change that drops `git` would make every one of them pass by doing
/// nothing. This one test is deliberately *not* guarded, so that regression
/// fails loudly and names what went untested.
#[test]
fn git_must_be_available_in_this_test_environment() {
    assert!(
        git_available(),
        "no `git` binary on PATH: every git+https:/git+file: provider test in this file — \
         argument-injection refusals, the timeout, directory merges, credential handling — \
         silently skips without one, so none of it was exercised by this test run"
    );
}

// -- AC2: s3 ----------------------------------------------------------------

fn s3_over(stub: &StubServer) -> S3Config {
    S3Config {
        endpoint: Some(stub.base_url()),
        region: "us-east-1".to_owned(),
    }
}

/// `version` = ETag. The ETag is what makes a re-pull of an unchanged object
/// free, so it is asserted exactly — including that the quotes S3 wraps it in
/// are stripped, since a quoted and unquoted form of one ETag would read as two
/// versions and defeat the short circuit.
#[tokio::test]
async fn s3_source_fetches_and_uses_the_etag_as_version() {
    let stub = StubServer::start(ok_json(DOCUMENT, Some("\"abc123\""))).await;
    let source = S3Source::new(MapResolver::empty(), s3_over(&stub)).expect("build");

    let fetched = fetch(&source, "s3://bucket/mocks/imposters.json", None)
        .await
        .expect("fetches");

    assert_eq!(fetched.configs.len(), 1);
    assert_eq!(
        fetched.meta.version.as_deref(),
        Some("abc123"),
        "the ETag's quotes must be stripped, or one object reads as two versions"
    );

    let seen = stub.requests();
    assert_eq!(seen.len(), 1, "one object, one GET");
    assert_eq!(
        seen[0].target, "/bucket/mocks/imposters.json",
        "path-style addressing against the configured endpoint"
    );
}

#[tokio::test]
async fn s3_re_fetch_of_unchanged_content_reports_the_same_version() {
    let stub = StubServer::start(ok_json(DOCUMENT, Some("\"abc123\""))).await;
    let source = S3Source::new(MapResolver::empty(), s3_over(&stub)).expect("build");

    let first = fetch(&source, "s3://bucket/k", None).await.expect("first");
    let second = fetch(&source, "s3://bucket/k", None).await.expect("second");
    assert_eq!(first.meta.version, second.meta.version);
}

/// A resolved credential must actually sign the request, and the signature must
/// carry the key *id* while the secret key never appears on the wire.
#[tokio::test]
async fn s3_signs_with_a_resolved_credential_without_sending_the_secret() {
    let stub = StubServer::start(ok_json(DOCUMENT, Some("\"abc123\""))).await;
    let source = S3Source::new(
        MapResolver::new(&[("bucket-key", "AKIAEXAMPLE:wJalrXUtnFEMIsupersecret")]),
        s3_over(&stub),
    )
    .expect("build");

    fetch(&source, "s3://bucket/k", Some("bucket-key"))
        .await
        .expect("fetches");

    let seen = stub.requests();
    let auth = seen[0]
        .header("authorization")
        .expect("a credentialed fetch must be signed");
    assert!(
        auth.starts_with("AWS4-HMAC-SHA256 "),
        "expected SigV4, got {auth}"
    );
    assert!(
        auth.contains("Credential=AKIAEXAMPLE/"),
        "the credential scope must name the key id: {auth}"
    );
    assert!(
        auth.contains("SignedHeaders=") && auth.contains("Signature="),
        "a SigV4 header has both parts: {auth}"
    );
    assert!(
        seen[0].header("x-amz-content-sha256").is_some(),
        "S3 requires the payload hash header on a signed request"
    );

    let whole_request = format!("{seen:?}");
    assert!(
        !whole_request.contains("wJalrXUtnFEMIsupersecret"),
        "the secret access key went over the wire: {whole_request}"
    );
}

/// Ambient-role default: no `auth_ref` and no static keys means an unsigned
/// request, which is what reaches a public object. This is the *only* path to
/// an anonymous fetch — contrast with the test below.
#[tokio::test]
async fn s3_without_an_auth_ref_fetches_anonymously() {
    let stub = StubServer::start(ok_json(DOCUMENT, Some("\"abc123\""))).await;
    let source = S3Source::new(MapResolver::empty(), s3_over(&stub)).expect("build");

    fetch(&source, "s3://bucket/k", None)
        .await
        .expect("fetches");

    assert!(
        stub.requests()[0].header("authorization").is_none(),
        "no credential was configured, so nothing should have been signed"
    );
}

/// AC6, the central security assertion: a source that *names* a credential and
/// cannot resolve it must fail. Falling through to the anonymous path here
/// would turn a secrets-mount outage into "quietly serving whatever a public
/// bucket has" — a security classifier failing open.
#[tokio::test]
async fn s3_an_unresolvable_credential_is_an_error_not_an_anonymous_fetch() {
    let stub = StubServer::start(ok_json(DOCUMENT, Some("\"abc123\""))).await;
    let source = S3Source::new(MapResolver::empty(), s3_over(&stub)).expect("build");

    let err = fetch(&source, "s3://bucket/k", Some("bucket-key"))
        .await
        .expect_err("an unresolvable auth_ref must fail the fetch");

    assert!(
        err.to_string().contains("bucket-key"),
        "the error must name the ref: {err}"
    );
    assert!(
        stub.requests().is_empty(),
        "nothing may go over the wire once the credential failed to resolve"
    );
}

#[tokio::test]
async fn s3_reports_a_non_success_status() {
    let stub = StubServer::start(|_| (403, Vec::new(), "<Error/>".to_owned())).await;
    let source = S3Source::new(MapResolver::empty(), s3_over(&stub)).expect("build");

    let err = fetch(&source, "s3://bucket/k", None)
        .await
        .expect_err("403 is an error");
    assert!(err.to_string().contains("403"), "{err}");
}

/// Issue #136 review, B3: a raw `#`/`?`/space/non-ASCII byte in the key must
/// never be read as a URL fragment/query delimiter. Before the fix,
/// `s3://bucket/a#b` GETed `/bucket/a` — a different, legitimate-looking
/// object — and a `?` put an unsigned query on the wire that desynced the
/// request from what SigV4 actually signed. Each key here must reach the
/// stub at its *encoded* target, and still carry a valid signature: since
/// `fetch_with_auth` builds the request URL and signs `canonical_uri` from
/// the exact same encoded string, the stub only ever seeing the right target
/// is what proves the two never diverged.
#[tokio::test]
async fn s3_percent_encodes_special_bytes_in_the_key_and_still_signs_correctly() {
    let stub = StubServer::start(ok_json(DOCUMENT, Some("\"abc123\""))).await;
    let source = S3Source::new(
        MapResolver::new(&[("bucket-key", "AKIAEXAMPLE:wJalrXUtnFEMIsupersecret")]),
        s3_over(&stub),
    )
    .expect("build");

    for (key, expected_target) in [
        ("a#b.json", "/bucket/a%23b.json"),
        ("a?b.json", "/bucket/a%3Fb.json"),
        ("a b/héllo.json", "/bucket/a%20b/h%C3%A9llo.json"),
    ] {
        fetch(&source, &format!("s3://bucket/{key}"), Some("bucket-key"))
            .await
            .unwrap_or_else(|e| panic!("fetching key {key:?} must succeed against the stub: {e}"));

        let seen = stub.requests();
        let last = seen.last().expect("a request was recorded");
        assert_eq!(
            last.target, expected_target,
            "key {key:?} must reach the stub at its percent-encoded target, not a truncated or \
             query-bearing one"
        );
        let auth = last
            .header("authorization")
            .expect("a credentialed fetch must still be signed");
        assert!(
            auth.starts_with("AWS4-HMAC-SHA256 ") && auth.contains("Credential=AKIAEXAMPLE/"),
            "key {key:?}: expected a valid SigV4 header, got {auth}"
        );
    }
}

// -- AC3: registry ----------------------------------------------------------

fn registry_over(stub: &StubServer) -> RegistryConfig {
    RegistryConfig {
        endpoint: stub.base_url(),
        imposters_pointer: "/data/imposters".to_owned(),
    }
}

/// The registry adapter is deliberately small: a base URL and a pointer into
/// the response, both from server config. The URI carries only service ids —
/// so this asserts the ids reach the path and the pointer picks the imposters
/// out of a wrapper shape the provider knows nothing about.
#[tokio::test]
async fn registry_source_maps_a_configured_response() {
    let stub = StubServer::start(|req| {
        let name = req.target.rsplit('/').next().unwrap_or_default();
        let port = if name == "svc-a" { 8080 } else { 8081 };
        (
            200,
            vec![("etag".to_owned(), format!("\"v-{name}\""))],
            format!(
                r#"{{"data":{{"imposters":[{{"port":{port},"protocol":"http","name":"{name}"}}]}}}}"#
            ),
        )
    })
    .await;
    let source = RegistrySource::new(MapResolver::empty(), registry_over(&stub)).expect("build");

    let fetched = fetch(&source, "registry://svc-a,svc-b", None)
        .await
        .expect("fetches");

    let mut ports: Vec<u16> = fetched.configs.iter().filter_map(|c| c.port).collect();
    ports.sort_unstable();
    assert_eq!(ports, vec![8080, 8081], "both services must be merged");

    let targets: Vec<String> = stub.requests().iter().map(|r| r.target.clone()).collect();
    assert_eq!(
        targets,
        vec!["/svc-a".to_owned(), "/svc-b".to_owned()],
        "one request per service id, in URI order"
    );
}

#[tokio::test]
async fn registry_re_fetch_of_unchanged_content_reports_the_same_version() {
    let stub = StubServer::start(ok_json(
        r#"{"data":{"imposters":[{"port":8080,"protocol":"http","name":"a"}]}}"#,
        Some("\"v1\""),
    ))
    .await;
    let source = RegistrySource::new(MapResolver::empty(), registry_over(&stub)).expect("build");

    let first = fetch(&source, "registry://svc-a", None)
        .await
        .expect("first");
    let second = fetch(&source, "registry://svc-a", None)
        .await
        .expect("second");

    assert!(
        first.meta.version.is_some(),
        "a registry fetch has a version"
    );
    assert_eq!(
        first.meta.version, second.meta.version,
        "unchanged content must re-fetch to the same version"
    );
}

/// A pointer that does not match the response is a configuration error the
/// operator must see — not an empty imposter list, which would silently *delete*
/// every imposter the source owns on the next pull.
#[tokio::test]
async fn registry_reports_a_response_the_pointer_does_not_match() {
    let stub = StubServer::start(ok_json(r#"{"something":"else"}"#, None)).await;
    let source = RegistrySource::new(MapResolver::empty(), registry_over(&stub)).expect("build");

    let err = fetch(&source, "registry://svc-a", None)
        .await
        .expect_err("an unmatched pointer is an error");
    assert!(
        err.to_string().contains("/data/imposters"),
        "the error must name the configured pointer: {err}"
    );
}

/// The other half of the pointer contract, and the one a "does the pointer
/// resolve?" check alone misses: the pointer resolves, but to something that is
/// not an imposter array. A registry that changed its envelope — `imposters`
/// becoming an object, or an error string — must be refused by name here rather
/// than handed onward as a document, where it becomes a confusing parse error
/// at best and an empty imposter set at worst.
#[tokio::test]
async fn registry_reports_a_pointer_that_resolves_to_a_non_array() {
    for body in [
        r#"{"data":{"imposters":"service unavailable"}}"#,
        r#"{"data":{"imposters":{"port":8080}}}"#,
        r#"{"data":{"imposters":null}}"#,
    ] {
        let stub = StubServer::start(move |_| (200, Vec::new(), body.to_owned())).await;
        let source =
            RegistrySource::new(MapResolver::empty(), registry_over(&stub)).expect("build");

        let err = match fetch(&source, "registry://svc-a", None).await {
            Err(e) => e,
            Ok(fetched) => panic!(
                "{body} must be refused; instead it yielded {} imposter(s)",
                fetched.configs.len()
            ),
        };
        assert!(
            err.to_string().contains("/data/imposters"),
            "refusing {body}: the error must name the configured pointer, got {err}"
        );
    }
}

#[tokio::test]
async fn registry_sends_the_resolved_credential_as_a_bearer_token() {
    let stub = StubServer::start(ok_json(
        r#"{"data":{"imposters":[{"port":8080,"protocol":"http","name":"a"}]}}"#,
        Some("\"v1\""),
    ))
    .await;
    let source = RegistrySource::new(
        MapResolver::new(&[("registry-token", "supersecrettoken")]),
        registry_over(&stub),
    )
    .expect("build");

    fetch(&source, "registry://svc-a", Some("registry-token"))
        .await
        .expect("fetches");

    assert_eq!(
        stub.requests()[0].header("authorization"),
        Some("Bearer supersecrettoken")
    );
}

#[tokio::test]
async fn registry_an_unresolvable_credential_is_an_error_not_an_anonymous_fetch() {
    let stub = StubServer::start(ok_json(DOCUMENT, None)).await;
    let source = RegistrySource::new(MapResolver::empty(), registry_over(&stub)).expect("build");

    let err = fetch(&source, "registry://svc-a", Some("registry-token"))
        .await
        .expect_err("an unresolvable auth_ref must fail the fetch");

    assert!(err.to_string().contains("registry-token"), "{err}");
    assert!(
        stub.requests().is_empty(),
        "nothing may go over the wire once the credential failed to resolve"
    );
}

/// AC7 for the HTTP providers: the token must not survive into an error string
/// even when the *server* fails in a way that echoes the request.
#[tokio::test]
async fn registry_errors_never_echo_the_token() {
    let stub = StubServer::start(|req| {
        // A hostile-ish registry that reflects what it was sent.
        (500, Vec::new(), format!("{:?}", req.headers))
    })
    .await;
    let source = RegistrySource::new(
        MapResolver::new(&[("registry-token", "supersecrettoken")]),
        registry_over(&stub),
    )
    .expect("build");

    let err = fetch(&source, "registry://svc-a", Some("registry-token"))
        .await
        .expect_err("500 is an error");

    let rendered = format!("{err} {err:?}");
    assert!(
        !rendered.contains("supersecrettoken"),
        "an echoing server's body reached the error string with the token in it: {rendered}"
    );
}

// -- AC4: the short circuit actually fires ----------------------------------
//
// The real assertion — that a re-pull of unchanged content produces *no log
// entry* — lives in `crates/rift-cluster/tests/cluster.rs` as
// `a_credentialed_source_short_circuits_on_unchanged_content` (issue #136
// review, B6.1). What used to be here compared two `digest_of(...)` calls
// directly: a near-tautology that would still pass if the short circuit in
// `sources::mod::SourcePuller::pull` were deleted outright, since nothing
// here ever drove a real pull through it. The replacement exercises the
// actual shipped path — a credentialed provider registered into
// `SourceProviders`, pulled twice through a real `SourcePuller`/`RaftNode` —
// and asserts on `PullReport.unchanged` and the applied log index, not on a
// digest computed independently of the code under test.

// -- the seam ---------------------------------------------------------------

/// A credentialed provider must not be reachable through the plain
/// `ImposterSource` path — that path has no `auth_ref` to give it, so a
/// provider registered there would fetch anonymously forever.
#[test]
fn a_scheme_may_not_be_claimed_by_both_registries() {
    let mut providers = SourceProviders::new(rift_cluster_base::seams::SourceRegistry::new());
    providers
        .register_credentialed(Arc::new(
            S3Source::new(
                MapResolver::empty(),
                S3Config {
                    endpoint: None,
                    region: "us-east-1".to_owned(),
                },
            )
            .expect("build"),
        ))
        .expect("first registration");

    let clash = providers.register_credentialed(Arc::new(
        S3Source::new(
            MapResolver::empty(),
            S3Config {
                endpoint: None,
                region: "us-east-1".to_owned(),
            },
        )
        .expect("build"),
    ));
    assert!(clash.is_err(), "a doubly-claimed scheme must be refused");
}

#[test]
fn schemes_lists_both_registries() {
    let mut upstream = rift_cluster_base::seams::SourceRegistry::new();
    upstream
        .register(Arc::new(rift_cluster_base::seams::FileSource::new(false)))
        .expect("register file");
    let mut providers = SourceProviders::new(upstream);
    providers
        .register_credentialed(Arc::new(
            S3Source::new(
                MapResolver::empty(),
                S3Config {
                    endpoint: None,
                    region: "us-east-1".to_owned(),
                },
            )
            .expect("build"),
        ))
        .expect("register s3");

    let schemes = providers.schemes();
    assert!(schemes.contains(&"file".to_owned()), "{schemes:?}");
    assert!(schemes.contains(&"s3".to_owned()), "{schemes:?}");
}

/// Belt and braces on the fixture itself: `temp_git_repo` must actually produce
/// a repo, or every skipped-looking git test above would be vacuous.
#[test]
fn the_git_fixture_builds_a_real_repo() {
    if !git_available() {
        return;
    }
    let (repo, sha) = temp_git_repo(&[("imposters.json", DOCUMENT)]);
    assert_eq!(sha.len(), 40, "a full commit sha");
    assert!(PathBuf::from(repo.path()).join(".git").is_dir());
}
