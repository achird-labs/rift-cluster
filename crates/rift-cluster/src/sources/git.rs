//! `git+https:`/`git+file:` — imposters from a path inside a git ref (issue #136).
//!
//! ## URI shape
//!
//! ```text
//! git+https://host/org/repo#<ref>:<path>
//! git+file:<local-path>#<ref>:<path>
//! ```
//!
//! `<ref>` is anything `git fetch` accepts as a refspec (a branch, a tag, a raw
//! sha); `<path>` is a path inside that ref's tree, either a single document or
//! a directory of them. For `git+https` the remote is the URI with the `git+`
//! prefix stripped and the `#fragment` removed — `https://host/org/repo`. For
//! `git+file` the remote is everything between `git+file:` and the `#` — a
//! plain local path. `git+file` exists for two reasons: it makes the fetch path
//! testable against a temp repo with no network egress (see
//! `provider_tests.rs`), and it is genuinely useful on its own for a mounted
//! bare repo (a sidecar-synced mirror, a shared NFS clone) that never touches a
//! remote host at all.
//!
//! ## Version
//!
//! `version` is the full 40-character commit sha `FETCH_HEAD` resolves to,
//! never a timestamp or a content hash — that is what lets #134's provenance
//! name something an operator can `git checkout` to reproduce exactly what the
//! fleet applied.
//!
//! ## Directory merge
//!
//! A directory path parses *every* file under it as its own remote document
//! (through the same [`parse_remote_document`] every provider routes through)
//! and merges them in sorted path order, mirroring upstream
//! `SourceSet::fetch_all`'s merge rule applied here across files instead of
//! across sources: a port declared twice is an error naming the port and both
//! paths, never a silent last-one-wins.
//!
//! ## Subprocess, not a library
//!
//! This fetches by shelling out to the `git` binary rather than linking `gix`
//! or `git2`. Both libraries are heavy for what one shallow, single-ref fetch
//! on the leader's fetch path needs — `gix` pulls in a large pure-Rust tree,
//! `git2` pulls in libgit2 via C bindings — and the subprocess's one real
//! downside, "the binary might not be installed", is turned into a clean
//! **construction-time** error: [`GitSource::new`] probes `git --version` so a
//! missing binary fails composition, not a first pull deep into a running
//! fleet.
//!
//! ## Auth
//!
//! A resolved credential is a token (a GitHub App / PAT-style token). It is
//! passed to `git` through the `GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_0`/
//! `GIT_CONFIG_VALUE_0` environment variables, setting `http.extraHeader` to
//! `Authorization: Basic base64("x-access-token:<token>")` for the lifetime of
//! the subprocess only. The token is never spliced into the remote URL: `git`
//! echoes remote URLs in its own error output (a failed fetch, a redirect
//! warning), and a URL-embedded token is exactly how that kind of credential
//! leaks into a log line an operator later pastes into a bug report.
//!
//! `http.followRedirects=false` is set on every invocation for the same
//! reason: git does **not** strip `http.extraHeader` when it follows a
//! redirect, so a `git+https:` remote that 302s to a different host would
//! otherwise hand that host the `Authorization` header too. Refusing the
//! redirect outright, rather than trying to strip the header before following
//! it, is the only way to guarantee the token never reaches a host the
//! operator did not name.
//!
//! ## Hardening the subprocess
//!
//! Every invocation additionally carries:
//!
//! * `protocol.ext.allow=never` — defence in depth against the `ext::`
//!   transport helper beyond [`check_remote`]'s own refusal of any remote
//!   containing `::`; two independent gates rather than one.
//! * `GIT_CONFIG_GLOBAL=/dev/null` / `GIT_CONFIG_SYSTEM=/dev/null` — a host
//!   `~/.gitconfig` or system config is never consulted, so an
//!   `insteadOf` rewrite, a `credential.helper`, or an `http.proxy` set
//!   outside this process cannot silently redirect or re-credential a fetch
//!   that a replicated, operator-supplied URI is supposed to control in full.
//! * `GIT_TERMINAL_PROMPT=0` — an auth failure against a remote that wants
//!   interactive credentials fails immediately instead of blocking the
//!   subprocess (and the blocking-pool thread running it) on a prompt nothing
//!   will ever answer.
//!
//! ## Timeout
//!
//! Every `git` invocation runs under [`GIT_TIMEOUT`] (mirroring the 30s
//! `REQUEST_TIMEOUT` the HTTP providers use), enforced by spawning the child
//! and polling for exit with a deadline rather than `Command::output()`, which
//! blocks forever on a stalled remote. A stalled `git+https:`/`git+file:`
//! source would otherwise leak one blocking-pool thread per tracking poll
//! against it until the pool's thread cap is exhausted and all blocking work
//! on the node stops. [`GitSource::with_timeout`] makes the budget an
//! injectable constructor parameter so a stalled-remote test does not have to
//! wait out the real 30s.

use std::future::Future;
use std::io::Read as _;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Context as _;
use base64::prelude::{BASE64_STANDARD, Engine as _};
use rift_cluster_base::seams::{
    FetchedImposters, LoadedConfig, SourceMeta, SourceRef, parse_remote_document,
};

use super::CredentialedSource;
use super::auth::{self, Credential, CredentialResolver};

/// Whole-invocation budget for a single `git` subprocess: connect, transfer
/// and exit, mirroring the 30s `REQUEST_TIMEOUT` the HTTP providers enforce.
/// See the module doc's "Timeout" section for why this exists at all.
pub const GIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Global `-c` flags applied to *every* `git` invocation this module makes —
/// see the module doc's "Hardening the subprocess" and "Auth" sections for
/// why each one is here.
const GIT_SAFETY_FLAGS: &[&str] = &[
    "-c",
    "protocol.ext.allow=never",
    "-c",
    "http.followRedirects=false",
];

/// `git+https:` / `git+file:` imposter source.
///
/// Carries no secret material: the resolver is consulted fresh on every fetch,
/// and the credential it returns lives only on the stack of the blocking task
/// that shells out to `git`.
pub struct GitSource {
    resolver: Arc<dyn CredentialResolver>,
    timeout: Duration,
}

impl std::fmt::Debug for GitSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitSource")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl GitSource {
    /// Build a source over `resolver` with the standard [`GIT_TIMEOUT`]
    /// budget, refusing to start if this host has no usable `git` binary.
    ///
    /// Probing at construction rather than at first fetch is the whole point
    /// of choosing a subprocess over a linked library: an absent binary is a
    /// composition-time error an operator sees at startup, not a pull that
    /// fails mysteriously the first time a `git+https:` source is actually
    /// used.
    ///
    /// # Errors
    /// If `git --version` cannot be run, or exits unsuccessfully.
    pub fn new(resolver: Arc<dyn CredentialResolver>) -> anyhow::Result<Self> {
        Self::with_timeout(resolver, GIT_TIMEOUT)
    }

    /// As [`Self::new`], but with an explicit per-invocation timeout — the
    /// seam a test uses to prove a stalled remote fails promptly rather than
    /// waiting out the real 30s budget.
    ///
    /// # Errors
    /// If `git --version` cannot be run, or exits unsuccessfully.
    pub fn with_timeout(
        resolver: Arc<dyn CredentialResolver>,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let output = std::process::Command::new("git")
            .arg("--version")
            .output()
            .context(
                "git+https:/git+file: sources require a `git` binary on PATH; none was found",
            )?;
        if !output.status.success() {
            anyhow::bail!(
                "`git --version` exited with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(Self { resolver, timeout })
    }
}

impl CredentialedSource for GitSource {
    fn schemes(&self) -> &'static [&'static str] {
        &["git+https", "git+file"]
    }

    fn fetch_with_auth<'a>(
        &'a self,
        r: &'a SourceRef,
        auth_ref: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<FetchedImposters>> + Send + 'a>> {
        Box::pin(async move {
            // Fail closed, before any subprocess runs: a named credential that
            // does not resolve must never fall through to an anonymous clone.
            // Resolved off the async worker thread: `resolve` can do blocking
            // file I/O under the secrets-directory step.
            let credential = auth::resolve_off_thread(&self.resolver, auth_ref).await?;

            let uri = r.uri.clone();
            let timeout = self.timeout;
            let (sha, loaded) = tokio::task::spawn_blocking(move || {
                fetch_blocking(&uri, credential.as_ref(), timeout)
            })
            .await
            .context("git fetch task panicked")??;

            Ok(FetchedImposters {
                configs: loaded.imposters,
                intercept: loaded.intercept,
                routes: loaded.routes,
                meta: SourceMeta {
                    version: Some(sha),
                    fetched_at: SystemTime::now(),
                },
                unchanged: false,
            })
        })
    }
}

/// The parts of a `git+https:`/`git+file:` URI: where to fetch from, which ref,
/// and which path inside its tree.
struct GitLocation {
    remote: String,
    git_ref: String,
    path: String,
}

fn parse_git_uri(uri: &str) -> anyhow::Result<GitLocation> {
    // `git+file:` is checked first: it would also match a bare `git+` strip,
    // which would leave the literal `file:` prefix stuck to the remote.
    let (is_file, remote, fragment) = if let Some(rest) = uri.strip_prefix("git+file:") {
        rest.split_once('#').map(|(a, b)| (true, a, b))
    } else if let Some(rest) = uri.strip_prefix("git+") {
        rest.split_once('#').map(|(a, b)| (false, a, b))
    } else {
        None
    }
    .ok_or_else(|| {
        anyhow::anyhow!(
            "source uri {uri:?} is not a git+https:/git+file: uri with a #<ref>:<path> fragment"
        )
    })?;

    let (git_ref, path) = fragment.split_once(':').ok_or_else(|| {
        anyhow::anyhow!("source uri {uri:?} has fragment {fragment:?}, which names no path")
    })?;

    check_remote(remote, is_file)?;
    check_ref(git_ref)?;

    Ok(GitLocation {
        remote: remote.to_owned(),
        git_ref: git_ref.to_owned(),
        path: path.to_owned(),
    })
}

/// Refuse a remote that `git fetch` would read as an **option** rather than as
/// a place to fetch from.
///
/// This is the sharpest edge in the whole provider. `git`'s option parser
/// permutes, so a positional argument beginning with `-` is still parsed as an
/// option — and `git fetch --upload-pack=<cmd>` runs `<cmd>`. A source URI is
/// operator-supplied *data* that reaches every node through the replicated log,
/// so without this check "may name a mock corpus" silently becomes "may execute
/// a command as the rift process, fleet-wide". Same reasoning for `ext::`,
/// whose whole purpose is running a helper command as the transport.
///
/// Positive shape rules rather than a deny-list: a `git+file:` remote must be
/// an absolute path, and a `git+https:` remote must parse as an `https` URL
/// with a host. Anything a deny-list would have missed fails these.
pub(crate) fn check_remote(remote: &str, is_file: bool) -> anyhow::Result<()> {
    if remote.starts_with('-') {
        anyhow::bail!(
            "source uri names a git remote beginning with `-`, which git would read as an option \
             rather than as a repository"
        );
    }
    if remote.contains("::") {
        anyhow::bail!(
            "source uri names a git transport helper (`<helper>::<target>`), which runs a command; \
             use a plain https or file remote"
        );
    }
    if is_file {
        if !remote.starts_with('/') {
            anyhow::bail!("a git+file: remote must be an absolute path");
        }
    } else {
        let url = reqwest::Url::parse(remote)
            .map_err(|_| anyhow::anyhow!("a git+https: remote must be a valid https url"))?;
        if url.scheme() != "https" {
            anyhow::bail!("a git+https: remote must use the https scheme");
        }
        if !url.has_host() {
            anyhow::bail!("a git+https: remote names no host");
        }
    }
    Ok(())
}

/// Refuse a ref that git would read as an option, or that is not a ref at all.
///
/// Same class of bug as [`check_remote`] — `git fetch <remote> --upload-pack=…`
/// is just as effective as putting it in the remote position — plus the
/// ordinary ref-name rules, so a typo fails here with a clear message instead
/// of somewhere inside git.
pub(crate) fn check_ref(git_ref: &str) -> anyhow::Result<()> {
    if git_ref.is_empty() {
        anyhow::bail!("source uri names an empty git ref");
    }
    if git_ref.starts_with('-') {
        anyhow::bail!(
            "source uri names a git ref beginning with `-`, which git would read as an option"
        );
    }
    if git_ref.contains("..") || git_ref.contains("::") || git_ref.starts_with('/') {
        anyhow::bail!("source uri names an invalid git ref");
    }
    if !git_ref
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/' | '+'))
    {
        anyhow::bail!(
            "source uri names a git ref with characters outside [A-Za-z0-9._/+-]; a ref is a \
             branch, a tag or a commit sha"
        );
    }
    Ok(())
}

/// The `git` environment: baseline hardening applied to every invocation, plus
/// a resolved credential (when there is one) carried as an `http.extraHeader`,
/// never as part of a command-line argument or the remote URL — both of which
/// `git` is liable to echo back on failure.
fn git_env(credential: Option<&Credential>) -> Vec<(String, String)> {
    let mut env = vec![
        // Never consult a host config: an `insteadOf` rewrite, a
        // `credential.helper`, or an `http.proxy` set outside this process
        // must not be able to silently redirect or re-credential a fetch this
        // URI is supposed to control in full.
        ("GIT_CONFIG_GLOBAL".to_owned(), "/dev/null".to_owned()),
        ("GIT_CONFIG_SYSTEM".to_owned(), "/dev/null".to_owned()),
        // A remote that wants interactive credentials fails immediately
        // instead of blocking this subprocess — and the blocking-pool thread
        // running it — on a prompt nothing will ever answer.
        ("GIT_TERMINAL_PROMPT".to_owned(), "0".to_owned()),
    ];
    let Some(credential) = credential else {
        return env;
    };
    let basic = BASE64_STANDARD.encode(format!("x-access-token:{}", credential.expose()));
    env.extend([
        ("GIT_CONFIG_COUNT".to_owned(), "1".to_owned()),
        ("GIT_CONFIG_KEY_0".to_owned(), "http.extraHeader".to_owned()),
        (
            "GIT_CONFIG_VALUE_0".to_owned(),
            format!("Authorization: Basic {basic}"),
        ),
    ]);
    env
}

/// The working directory, environment and timeout budget shared by every
/// `git` invocation of one fetch — bundled so the helper functions below take
/// one argument instead of three repeated at every call site.
struct GitCtx<'a> {
    dir: &'a Path,
    env: &'a [(String, String)],
    timeout: Duration,
}

/// Everything that happens in a `spawn_blocking` task: the actual `git`
/// subprocess work. Returns the fetched commit sha and the merged document(s).
fn fetch_blocking(
    uri: &str,
    credential: Option<&Credential>,
    timeout: Duration,
) -> anyhow::Result<(String, LoadedConfig)> {
    let location = parse_git_uri(uri)?;
    let env = git_env(credential);

    let workdir = tempfile::tempdir().context("creating a git working directory")?;
    let ctx = GitCtx {
        dir: workdir.path(),
        env: &env,
        timeout,
    };

    run_git(&ctx, &["-c", "init.defaultBranch=main", "init", "--quiet"])?;
    run_git(
        &ctx,
        &["fetch", "--depth", "1", &location.remote, &location.git_ref],
    )?;
    let sha = run_git(&ctx, &["rev-parse", "FETCH_HEAD"])?;

    let loaded = match object_type(&ctx, &location.path)? {
        Some(ObjectType::Blob) => {
            let content = show(&ctx, &location.path)?;
            parse_remote_document(&content, uri)
                .with_context(|| format!("parsing {}", location.path))?
        }
        Some(ObjectType::Tree) => {
            // `-z` (NUL-separated, `--name-only`) rather than the default:
            // without it, git C-quotes any path with a non-ASCII, quote or
            // backslash byte (`"m\303\266cks.json"`), and that literal quoted
            // form is then fed straight into `git show`, which fails to find
            // it — breaking the whole directory source on one such file.
            let listing = run_git_bytes(
                &ctx,
                &[
                    "ls-tree",
                    "-r",
                    "-z",
                    "--name-only",
                    "FETCH_HEAD",
                    "--",
                    &location.path,
                ],
            )?;
            let mut entries: Vec<String> = listing
                .split(|&b| b == 0)
                .filter(|entry| !entry.is_empty())
                .map(|entry| String::from_utf8_lossy(entry).into_owned())
                .collect();
            entries.sort_unstable();
            // A directory that matches nothing is a configuration error, not
            // an empty document: under `on_drift: overwrite` an empty
            // `LoadedConfig` would delete every port this source owns on the
            // very next pull. `registry.rs` already treats a non-matching
            // pointer this way; this keeps the two providers consistent.
            if entries.is_empty() {
                anyhow::bail!(
                    "source uri {uri:?} names path {:?}, a directory with no files at commit \
                     {sha}; refusing to treat that as an empty imposter set",
                    location.path
                );
            }

            let mut documents = Vec::with_capacity(entries.len());
            for entry in entries {
                let content = show(&ctx, &entry)?;
                let doc = parse_remote_document(&content, &entry)
                    .with_context(|| format!("parsing {entry}"))?;
                documents.push((entry, doc));
            }
            super::common::merge_documents(documents, "document")?
        }
        None => anyhow::bail!(
            "source uri {uri:?} names path {:?}, which does not exist at commit {sha}",
            location.path
        ),
    };

    Ok((sha, loaded))
}

enum ObjectType {
    Blob,
    Tree,
}

/// Whether `path` is a file or a directory in the fetched tree, or `None` when
/// it is neither — the single check that turns a typo'd path into a named
/// error instead of a silently empty document.
///
/// A non-zero exit is `None` (the path is absent) only when git's stderr
/// actually says so; anything else — a corrupt object database, an I/O error
/// — is surfaced as a real failure. Collapsing every non-zero exit into "path
/// does not exist" would report the former as the latter, sending an operator
/// looking for a typo that is not there.
fn object_type(ctx: &GitCtx<'_>, path: &str) -> anyhow::Result<Option<ObjectType>> {
    let output = run_git_raw(ctx, &["cat-file", "-t", &format!("FETCH_HEAD:{path}")])?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return if stderr.contains("does not exist") || stderr.contains("bad revision") {
            Ok(None)
        } else {
            anyhow::bail!("git cat-file -t {path:?} failed: {}", stderr.trim())
        };
    }
    match String::from_utf8_lossy(&output.stdout).trim() {
        "blob" => Ok(Some(ObjectType::Blob)),
        "tree" => Ok(Some(ObjectType::Tree)),
        _ => Ok(None),
    }
}

/// `git show FETCH_HEAD:<path>`, decoded as UTF-8 — a config document is text,
/// so anything else is refused with the path named rather than lossily
/// mangled.
fn show(ctx: &GitCtx<'_>, path: &str) -> anyhow::Result<String> {
    let bytes = run_git_bytes(ctx, &["show", &format!("FETCH_HEAD:{path}")])?;
    String::from_utf8(bytes).map_err(|e| anyhow::anyhow!("{path} is not valid UTF-8: {e}"))
}

/// Run `git` with `args`, returning trimmed stdout as text. Used for the small
/// metadata commands (`rev-parse`, `init`, `fetch`) whose output is always
/// ASCII.
fn run_git(ctx: &GitCtx<'_>, args: &[&str]) -> anyhow::Result<String> {
    let bytes = run_git_bytes(ctx, args)?;
    Ok(String::from_utf8_lossy(&bytes).trim().to_owned())
}

/// Run `git` with `args`, returning raw stdout bytes, and failing if `git`
/// exits unsuccessfully.
///
/// A failure's message is built from `git`'s stderr, which never contains the
/// credential: the only place the credential is ever written is the process
/// environment, which `git` does not echo back on any of the commands this
/// module runs.
fn run_git_bytes(ctx: &GitCtx<'_>, args: &[&str]) -> anyhow::Result<Vec<u8>> {
    let output = run_git_raw(ctx, args)?;
    if !output.status.success() {
        anyhow::bail!(
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

/// Kill the timed-out `git` and every process it spawned, then reap it.
///
/// On unix the child leads its own process group (see `run_git_raw`), so
/// `kill(-pgid)` reaches the transport helper too — the one that actually holds
/// the stalled socket and the inherited pipe write ends. Elsewhere there is no
/// portable group kill available here, so the direct child is killed and a
/// helper, if any, is left to exit on its own; the timeout still fires, it just
/// cannot promise the pipes close immediately.
fn kill_group(child: &mut std::process::Child) {
    #[cfg(unix)]
    {
        // Negative pid = "the group whose id is this pid". Safe: the argument
        // is a pid this process owns, and `kill` has no memory effects.
        let pid = child.id();
        if let Ok(pid) = i32::try_from(pid) {
            unsafe {
                libc::kill(-pid, libc::SIGKILL);
            }
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// The raw result of a `git` invocation: whatever exit status, stdout and
/// stderr it produced. Callers decide what a non-zero exit means (a real
/// failure for most commands; a meaningful "not found" for `cat-file -t`), so
/// this layer never itself turns a non-zero exit into an `Err`.
struct GitOutput {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// Spawn `git` under `ctx.timeout`'s budget, killing it if the deadline
/// passes rather than the open-ended block of `Command::output()` (issue #136
/// review, B2) — see the module doc's "Timeout" section.
///
/// stdout and stderr are drained by dedicated reader threads running
/// concurrently with the wait loop, not read after the fact: a response too
/// large for the OS pipe buffer would otherwise deadlock the poll loop itself
/// (the child blocks writing to a full pipe that nothing is reading, and
/// polling `try_wait` never reads it). Only the wait is polled with a
/// deadline; the readers just run to completion (or to the child's death).
fn run_git_raw(ctx: &GitCtx<'_>, args: &[&str]) -> anyhow::Result<GitOutput> {
    let mut cmd = std::process::Command::new("git");
    cmd.args(GIT_SAFETY_FLAGS)
        .args(args)
        .current_dir(ctx.dir)
        .envs(ctx.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Its own process group, so the timeout path can kill the whole tree.
    //
    // Killing only the direct child is not enough and the difference is not
    // subtle: `git fetch` over https execs a helper (`git-remote-https`), and
    // that grandchild inherits these very pipes. Kill `git` alone and the
    // helper survives, still holding the write ends open and still blocked on
    // the stalled socket — so `read_to_end` below never sees EOF and the
    // deadline buys nothing at all. This is not theoretical: the first
    // implementation did exactly that and the stalled-remote test sat for 300s
    // against a 300ms budget.
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut cmd, 0);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawning git {args:?}"))?;
    let mut stdout_pipe = child.stdout.take().expect("stdout was piped above");
    let mut stderr_pipe = child.stderr.take().expect("stderr was piped above");

    let stdout_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout_pipe.read_to_end(&mut buf);
        buf
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr_pipe.read_to_end(&mut buf);
        buf
    });

    // The exit status is captured directly from whichever branch reaps the
    // child — `try_wait`'s `Some` on the ordinary path, `wait` after `kill` on
    // the timeout path — because a `Child` cannot be waited on twice: once
    // reaped, a second `wait`/`try_wait` call errors (the OS process table
    // entry is already gone).
    let deadline = Instant::now() + ctx.timeout;
    let status = loop {
        if let Some(status) = child.try_wait().context("polling git subprocess")? {
            break Some(status);
        }
        if Instant::now() >= deadline {
            // Kill the whole group before joining the readers, so every
            // inherited write end of these pipes is closed and the reader
            // threads below see EOF at once. Killing just `child` leaves the
            // transport helper alive holding them open — see the
            // `process_group` comment above.
            kill_group(&mut child);
            break None;
        }
        std::thread::sleep(Duration::from_millis(25));
    };

    // `unwrap_or_default` here is the terminal last resort on a reader
    // thread's own join, not on git's output: a panicked reader thread (which
    // cannot happen barring a bug in the two closures above) degrades to an
    // empty buffer rather than poisoning this call, and the exit status is
    // what actually decides success or failure.
    let stdout = stdout_reader.join().unwrap_or_default();
    let stderr = stderr_reader.join().unwrap_or_default();

    let Some(status) = status else {
        anyhow::bail!(
            "git {args:?} did not complete within {}s and was killed",
            ctx.timeout.as_secs()
        );
    };
    Ok(GitOutput {
        status,
        stdout,
        stderr,
    })
}
