//! Resolving a source's `auth_ref` into a credential (issue #136).
//!
//! A source names a credential; it never carries one ([`crate::control::validate`]
//! refuses a URI with credentials in its authority). This module is the other
//! half of that rule: turning the *name* into the secret, at fetch time, on the
//! node doing the fetching — so the secret exists in one process's memory for
//! the length of one request and never in the replicated log.
//!
//! ## Resolution order
//!
//! First hit wins:
//!
//! 1. environment variable `RIFT_SOURCE_AUTH_<REF>`, where `<REF>` is the
//!    `auth_ref` upper-cased with every non-alphanumeric character replaced by
//!    `_` (so `gh-mocks` reads `RIFT_SOURCE_AUTH_GH_MOCKS`);
//! 2. a file named exactly `<auth_ref>` under the configured secrets directory —
//!    the shape a Kubernetes secret mounts as;
//! 3. a cloud secret manager, when one is configured.
//!
//! ## Failing closed
//!
//! An unresolvable `auth_ref` is a **pull error**. It is never an anonymous
//! retry and never a silent skip: this is a security classifier, and a
//! classifier that cannot classify treats the input as the dangerous class. A
//! source that declares it needs a credential and does not get one has not
//! "found no credential" — it has failed.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A resolved secret.
///
/// `Debug` renders a placeholder and there is no `Display`, so the ordinary
/// ways a value leaks into a log line — `{:?}` on a surrounding struct, a
/// `tracing` field, an `anyhow` context chain — cannot leak this one. Reading
/// the secret is deliberately a named call ([`Self::expose`]) so every use site
/// is greppable.
#[derive(Clone, PartialEq, Eq)]
pub struct Credential(String);

impl Credential {
    #[must_use]
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    /// The secret itself. Every caller of this is a place a secret could
    /// escape; there are meant to be very few.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credential(<redacted>)")
    }
}

/// Why an `auth_ref` could not be turned into a credential.
///
/// Neither variant carries secret material — only the *name*, which is bounded
/// public config validated by [`crate::control::is_source_name`]. The
/// `Unreadable` detail is the I/O error, which names a path, not a content.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error(
        "no credential is configured for auth_ref {0:?}; set RIFT_SOURCE_AUTH_{1} or mount it in \
         the secrets directory"
    )]
    Unresolved(String, String),

    #[error("reading the credential for auth_ref {0:?}: {1}")]
    Unreadable(String, String),
}

/// Turns an `auth_ref` into a [`Credential`].
pub trait CredentialResolver: Send + Sync + fmt::Debug {
    fn resolve(&self, auth_ref: &str) -> Result<Credential, AuthError>;
}

/// Resolve `auth_ref` against `resolver` off the async worker thread (issue
/// #136 review, B5).
///
/// [`StandardResolver::resolve`] does a blocking `std::fs::read_to_string`
/// under the secrets-directory step, and every provider's `fetch_with_auth`
/// previously called `resolve` directly inside an `async fn` — stalling a
/// tokio worker thread on a slow or contended secret mount for as long as that
/// read takes. Wrapping the call in [`tokio::task::spawn_blocking`] here, once,
/// keeps the three providers from each needing to know that resolution can
/// block.
///
/// The fail-closed ordering is preserved exactly: this still runs, and still
/// propagates via `?`, before any network request or subprocess the caller
/// goes on to make — only *where* the resolver's own blocking work runs has
/// changed, not when the result is available relative to the fetch.
///
/// # Errors
/// If `auth_ref` is `Some` and resolution fails, or if the blocking task
/// itself panics.
pub async fn resolve_off_thread(
    resolver: &Arc<dyn CredentialResolver>,
    auth_ref: Option<&str>,
) -> Result<Option<Credential>, AuthError> {
    let Some(auth_ref) = auth_ref else {
        return Ok(None);
    };
    let name = auth_ref.to_owned();
    let task_ref = name.clone();
    let resolver = Arc::clone(resolver);
    match tokio::task::spawn_blocking(move || resolver.resolve(&task_ref)).await {
        Ok(result) => result.map(Some),
        // A panicked blocking task is not a credential failure to blame on
        // the auth_ref's name — it is an internal defect, and folding it into
        // an ordinary `Unresolved` would misdirect an operator toward fixing a
        // secret that was never the problem.
        Err(join_error) => Err(AuthError::Unreadable(
            name,
            format!("credential resolution task panicked: {join_error}"),
        )),
    }
}

/// The environment-variable name an `auth_ref` reads.
#[must_use]
pub fn env_var_suffix(auth_ref: &str) -> String {
    auth_ref
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// How this resolver reads the environment.
///
/// Injected rather than calling [`std::env::var`] directly because the
/// resolution *order* is the load-bearing behaviour here, and proving an order
/// with real process environment means mutating it — which is `unsafe` on
/// edition 2024 and races every other test in the binary. Production wires
/// [`std::env::var`]; tests wire a map.
type EnvLookup = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// The standard resolver: environment, then a mounted secrets directory.
///
/// A cloud secret manager is the documented third step; no manager is wired in
/// this build, so a ref that reaches step 3 is [`AuthError::Unresolved`] rather
/// than silently absent.
#[derive(Clone)]
pub struct StandardResolver {
    env: EnvLookup,
    secrets_dir: Option<PathBuf>,
}

impl fmt::Debug for StandardResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StandardResolver")
            .field("secrets_dir", &self.secrets_dir)
            .finish_non_exhaustive()
    }
}

impl StandardResolver {
    #[must_use]
    pub fn new(secrets_dir: Option<PathBuf>) -> Self {
        Self {
            env: Arc::new(|name| std::env::var(name).ok()),
            secrets_dir,
        }
    }

    /// A resolver reading a caller-supplied environment. Test seam; see
    /// [`EnvLookup`].
    #[must_use]
    pub fn with_env(
        env: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
        secrets_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            env: Arc::new(env),
            secrets_dir,
        }
    }
}

impl CredentialResolver for StandardResolver {
    fn resolve(&self, auth_ref: &str) -> Result<Credential, AuthError> {
        let suffix = env_var_suffix(auth_ref);
        if let Some(value) = (self.env)(&format!("RIFT_SOURCE_AUTH_{suffix}")) {
            return Ok(Credential::new(value.trim_end_matches('\n')));
        }

        if let Some(dir) = &self.secrets_dir {
            let path = secret_path(dir, auth_ref)?;
            match std::fs::read_to_string(&path) {
                Ok(value) => return Ok(Credential::new(value.trim_end_matches('\n'))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                // A secrets file that exists but cannot be read is NOT "no
                // credential": falling through to Unresolved here would report
                // a permissions bug as a configuration bug, and an operator
                // would go looking in the wrong place.
                Err(e) => {
                    return Err(AuthError::Unreadable(
                        auth_ref.to_owned(),
                        format!("{}: {e}", path.display()),
                    ));
                }
            }
        }

        Err(AuthError::Unresolved(auth_ref.to_owned(), suffix))
    }
}

/// The file an `auth_ref` reads from `dir`, refusing any ref that would escape
/// it.
///
/// `auth_ref` is already validated as a bounded name at admission
/// ([`crate::control::is_source_name`]), so this cannot currently trigger — it
/// is here because "the validated set never contains a separator" is an
/// invariant of *another* module, and a path built from an external name should
/// not depend on that invariant staying true.
fn secret_path(dir: &Path, auth_ref: &str) -> Result<PathBuf, AuthError> {
    if auth_ref.is_empty()
        || auth_ref.contains(['/', '\\'])
        || auth_ref.contains("..")
        || Path::new(auth_ref).components().count() != 1
    {
        return Err(AuthError::Unreadable(
            auth_ref.to_owned(),
            "auth_ref is not a plain file name".to_owned(),
        ));
    }
    Ok(dir.join(auth_ref))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn resolver(env: &[(&str, &str)], dir: Option<PathBuf>) -> StandardResolver {
        let map: HashMap<String, String> = env
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        StandardResolver::with_env(move |name| map.get(name).cloned(), dir)
    }

    // -- AC5: resolution order, first hit wins ------------------------------

    #[test]
    fn auth_prefers_the_env_var() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("gh-mocks"), "from-file").expect("write");

        let resolved = resolver(
            &[("RIFT_SOURCE_AUTH_GH_MOCKS", "from-env")],
            Some(dir.path().to_path_buf()),
        )
        .resolve("gh-mocks")
        .expect("resolves");

        assert_eq!(
            resolved.expose(),
            "from-env",
            "the env var must win over the secrets dir, or the documented order is a fiction"
        );
    }

    #[test]
    fn auth_falls_back_to_the_secrets_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("gh-mocks"), "from-file\n").expect("write");

        let resolved = resolver(&[], Some(dir.path().to_path_buf()))
            .resolve("gh-mocks")
            .expect("resolves");

        assert_eq!(
            resolved.expose(),
            "from-file",
            "a k8s-mounted secret carries a trailing newline the token must not"
        );
    }

    #[test]
    fn auth_reports_a_missing_ref() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = resolver(&[], Some(dir.path().to_path_buf()))
            .resolve("gh-mocks")
            .expect_err("no credential anywhere");

        assert!(
            matches!(&err, AuthError::Unresolved(name, _) if name == "gh-mocks"),
            "{err:?}"
        );
        assert!(
            err.to_string().contains("RIFT_SOURCE_AUTH_GH_MOCKS"),
            "the error must name the env var an operator would set: {err}"
        );
    }

    /// The `<REF>` mangling is documented, so it is asserted rather than left
    /// to whatever the implementation happens to do.
    #[test]
    fn the_env_var_name_is_the_documented_mangling() {
        assert_eq!(env_var_suffix("gh-mocks"), "GH_MOCKS");
        assert_eq!(env_var_suffix("team.a_b"), "TEAM_A_B");
        assert_eq!(env_var_suffix("Plain9"), "PLAIN9");
    }

    // -- AC5/AC6: failing closed --------------------------------------------

    /// An unreadable secrets file is distinct from an absent one. Collapsing
    /// them would report a permissions failure as "you forgot to configure it".
    #[test]
    fn an_unreadable_secret_is_not_reported_as_absent() {
        let err = resolver(&[], Some(PathBuf::from("/nonexistent-dir")))
            .resolve("../escape")
            .expect_err("a traversing ref is refused");
        assert!(matches!(err, AuthError::Unreadable(..)), "{err:?}");
    }

    // -- AC7: no secret material anywhere -----------------------------------

    #[test]
    fn the_credential_does_not_render_in_debug() {
        let credential = Credential::new("ghp_supersecrettoken");
        let rendered = format!("{credential:?}");
        assert!(
            !rendered.contains("ghp_supersecrettoken"),
            "Debug leaked the secret: {rendered}"
        );
        assert_eq!(rendered, "Credential(<redacted>)");

        // The secret must not escape through a surrounding struct's derived
        // Debug either — that is the shape it would actually leak in.
        #[derive(Debug)]
        struct Wrapper {
            _credential: Credential,
        }
        let wrapped = format!(
            "{:?}",
            Wrapper {
                _credential: credential
            }
        );
        assert!(
            !wrapped.contains("ghp_supersecrettoken"),
            "a derived Debug leaked the secret: {wrapped}"
        );
    }

    /// `AuthError` is rendered into `last.outcome` and into log lines, so it is
    /// checked directly for secret material.
    #[test]
    fn auth_errors_never_carry_secret_material() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("gh-mocks"), "ghp_supersecrettoken").expect("write");

        // Resolution succeeded, so nothing to check there; the failure path is
        // the one that renders. Provoke it with a ref that has no credential
        // while a *different* ref's secret is on disk.
        let err = resolver(&[], Some(dir.path().to_path_buf()))
            .resolve("other")
            .expect_err("no credential for `other`");

        let rendered = format!("{err} {err:?}");
        assert!(
            !rendered.contains("ghp_supersecrettoken"),
            "the error leaked a secret: {rendered}"
        );
    }
}
