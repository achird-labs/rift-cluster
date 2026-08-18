//! CLI arguments and credential loading for the `mcp` subcommand.

use std::fmt;
use std::path::{Path, PathBuf};

// `reqwest`'s own re-export rather than a direct `url` dependency: the crate
// already pins `reqwest` (Cargo.toml:49), and taking `url` directly would add a
// second place for the version to drift from the one actually parsing requests.
use reqwest::Url;

/// Everything `rift-cluster-server mcp` needs to talk to a remote admin front.
///
/// `--api-key-file` rather than an env var is deliberate and is the documented
/// spelling everywhere (RFC-006 §9.4): env vars leak into crash dumps, `/proc`,
/// and every child process the agent host spawns.
#[derive(clap::Args, Debug, Clone)]
pub struct McpArgs {
    /// Base URL of the cluster's admin API, e.g. `https://fleet.example:2525`.
    #[arg(long, value_name = "URL")]
    pub url: Url,

    /// File holding the API key of the principal this server acts as.
    ///
    /// Bind a dedicated `agent` principal in only the tenants the agent should
    /// touch — never a `FleetAdmin` key (RFC-006 §8.3).
    #[arg(long, value_name = "PATH")]
    pub api_key_file: PathBuf,

    /// Per-request timeout against the admin API, in seconds.
    // Range-checked: `Duration::from_secs(0)` as a reqwest timeout makes every
    // request elapse immediately, so a `0` here is a server that answers nothing
    // and blames the fleet.
    #[arg(
        long,
        value_name = "SECONDS",
        default_value_t = 30,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub timeout_secs: u64,
}

/// An admin API key.
///
/// The inner value is never rendered. `Debug` is hand-written and there is no
/// `Display`, because the guarantee "the MCP server never logs the key"
/// (RFC-006 §9.4) is otherwise one `#[derive(Debug)]` and one `tracing` call
/// away from being false — and it would fail silently, in a log nobody reads
/// until it is already shipped.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey(String);

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(<redacted>)")
    }
}

impl ApiKey {
    /// Read a key from disk.
    ///
    /// Trailing whitespace is trimmed: a key file written by `echo` ends in a
    /// newline, and sending that byte makes every request a `401` that looks
    /// like a wrong key rather than a stray newline.
    pub fn load(path: &Path) -> Result<Self, StartupError> {
        let raw =
            std::fs::read_to_string(path).map_err(|source| StartupError::KeyFileUnreadable {
                path: path.to_path_buf(),
                source,
            })?;
        Self::from_contents(&raw, path)
    }

    fn from_contents(raw: &str, path: &Path) -> Result<Self, StartupError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(StartupError::KeyFileEmpty {
                path: path.to_path_buf(),
            });
        }
        Ok(Self(trimmed.to_owned()))
    }

    /// The exact `Authorization` header value.
    ///
    /// The raw key, with **no** `Bearer ` prefix: the admin front hashes the
    /// header value verbatim and strips nothing, so a `Bearer ` prefix is a
    /// different credential and is answered `401`
    /// (`docs/api/openapi-ee.yaml`, `securitySchemes.apiKeyAuth`).
    #[must_use]
    pub fn header_value(&self) -> &str {
        &self.0
    }
}

/// Failures that stop the MCP server before it serves anything.
///
/// Every one of these is reported and exits non-zero. None of them panics —
/// an agent host launching this over stdio gets a diagnosable message, not a
/// backtrace on a closed pipe.
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("could not read the API key file {path}: {source}")]
    KeyFileUnreadable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("the API key file {path} is empty")]
    KeyFileEmpty { path: PathBuf },

    #[error("could not build the HTTP client: {0}")]
    Client(#[from] reqwest::Error),

    /// A `--url` carrying `user:password@`.
    ///
    /// The message deliberately does not echo the URL: rendering it is exactly how
    /// the password would reach a log.
    #[error(
        "--url must not contain a username or password; the credential is the API key \
         in --api-key-file, and a URL's userinfo is rendered into error messages"
    )]
    UrlHasUserinfo,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str, contents: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("rift-mcp-test-{name}"));
        std::fs::write(&path, contents).expect("write temp key file");
        path
    }

    /// E1 — a key file written by `echo` ends in a newline. Sending it is a 401.
    #[test]
    fn key_file_trailing_newline_is_trimmed() {
        let path = tmp("trailing-newline", "s3cret-key\n");
        let key = ApiKey::load(&path).expect("must load");
        assert_eq!(key.header_value(), "s3cret-key");
        let _ = std::fs::remove_file(&path);
    }

    /// E1 — surrounding whitespace of any shape, not just the newline.
    #[test]
    fn key_file_surrounding_whitespace_is_trimmed() {
        let path = tmp("whitespace", "  s3cret-key \r\n");
        let key = ApiKey::load(&path).expect("must load");
        assert_eq!(key.header_value(), "s3cret-key");
        let _ = std::fs::remove_file(&path);
    }

    /// E2 — an empty key file is refused by name, not accepted as an empty credential.
    #[test]
    fn empty_key_file_is_refused() {
        let path = tmp("empty", "   \n\t ");
        let err = ApiKey::load(&path).expect_err("an empty key file must be refused");
        assert!(
            matches!(err, StartupError::KeyFileEmpty { .. }),
            "expected KeyFileEmpty, got {err:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// E3 / AC2 — a missing key file is a clean typed error, never a panic.
    #[test]
    fn missing_key_file_is_a_clean_error() {
        let path = std::env::temp_dir().join("rift-mcp-test-does-not-exist-4f2a");
        let _ = std::fs::remove_file(&path);
        let err = ApiKey::load(&path).expect_err("a missing key file must be an error");
        assert!(
            matches!(err, StartupError::KeyFileUnreadable { .. }),
            "expected KeyFileUnreadable, got {err:?}"
        );
    }

    /// E5 — the key must not survive a `Debug` render. This is the promise in
    /// RFC-006 §9.4, and `{:?}` on a config struct is exactly how it gets broken.
    #[test]
    fn api_key_debug_is_redacted() {
        let key = ApiKey("super-secret-value".to_owned());
        let rendered = format!("{key:?}");
        assert_eq!(rendered, "ApiKey(<redacted>)");
        assert!(
            !rendered.contains("super-secret-value"),
            "the key leaked into Debug output: {rendered}"
        );
    }

    /// E5 — and it must not leak through a struct that merely *holds* it, which is
    /// the realistic leak: nobody debugs the key directly, they debug the config.
    #[test]
    fn api_key_is_redacted_inside_a_containing_struct() {
        #[derive(Debug)]
        struct Config {
            #[allow(dead_code)]
            key: ApiKey,
        }
        let rendered = format!(
            "{:?}",
            Config {
                key: ApiKey("super-secret-value".to_owned())
            }
        );
        assert!(
            !rendered.contains("super-secret-value"),
            "the key leaked through a containing struct: {rendered}"
        );
    }

    /// E4 — the header value is the raw key. If this ever gains a `Bearer `
    /// prefix the whole tool set silently 401s.
    #[test]
    fn header_value_has_no_bearer_prefix() {
        let key = ApiKey("s3cret-key".to_owned());
        assert_eq!(key.header_value(), "s3cret-key");
        assert!(!key.header_value().starts_with("Bearer "));
    }
}
