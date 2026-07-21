//! The peer-facing RPC client: pooled connections, signed requests, fast-fail.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper::body::Bytes;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rand::Rng;

use super::AuthError;
use super::auth::{AUTH_HEADER, SignedRequest, Signer};
use super::routes::{PROTO_HEADER, PROTO_VERSION};
use super::{DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT, RpcError};
use crate::metrics;

/// Locally observed peer liveness.
///
/// Trait-shaped so membership can supply real health once it exists: the point
/// is that a peer already known to be down costs zero wall-clock, instead of
/// burning the full connect+request deadline on every request during an outage.
pub trait PeerHealth: Send + Sync {
    fn is_healthy(&self, peer: SocketAddr) -> bool;
}

/// Health source for single-node and test use: never fast-fails.
pub struct AlwaysHealthy;

impl PeerHealth for AlwaysHealthy {
    fn is_healthy(&self, _peer: SocketAddr) -> bool {
        true
    }
}

/// Timeouts and retry budget for peer calls.
#[derive(Debug, Clone, Copy)]
pub struct RpcClientConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    /// Attempts *after* the first. Only transient failures are retried.
    pub max_retries: u32,
}

impl Default for RpcClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_retries: 3,
        }
    }
}

/// Signed, pooled client for the cluster port.
#[derive(Clone)]
pub struct RpcClient {
    http: Client<HttpConnector, Full<Bytes>>,
    signer: Option<Signer>,
    health: Arc<dyn PeerHealth>,
    config: RpcClientConfig,
}

impl RpcClient {
    /// Build a client. `signer` is `None` only for an explicitly insecure
    /// cluster (see [`crate::config`]).
    #[must_use]
    pub fn new(
        signer: Option<Signer>,
        health: Arc<dyn PeerHealth>,
        config: RpcClientConfig,
    ) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(config.connect_timeout));
        connector.set_nodelay(true);
        // The legacy client is the pooling one: connections are kept per peer
        // so a steady owner-forwarding load does not re-handshake per request.
        let http = Client::builder(TokioExecutor::new()).build(connector);
        Self {
            http,
            signer,
            health,
            config,
        }
    }

    /// Call `method path` on `peer` with `body`, retrying transient failures.
    pub async fn call(
        &self,
        peer: SocketAddr,
        method: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, RpcError> {
        if !self.health.is_healthy(peer) {
            // Fast-fail: resolve now rather than parking the caller for the
            // full deadline against a peer the local view already knows is gone.
            let err = RpcError::Transport(format!("peer {peer} is not healthy"));
            metrics::rpc_failure(err.reason());
            return Err(err);
        }

        let mut attempt = 0;
        loop {
            let result = self.attempt(peer, method, path, body.clone()).await;
            match result {
                Ok(response) => return Ok(response),
                Err(e) if e.is_retryable() && attempt < self.config.max_retries => {
                    attempt += 1;
                    tokio::time::sleep(backoff(attempt)).await;
                }
                Err(e) => {
                    metrics::rpc_failure(e.reason());
                    return Err(e);
                }
            }
        }
    }

    async fn attempt(
        &self,
        peer: SocketAddr,
        method: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, RpcError> {
        let uri = format!("http://{peer}{path}");
        let mut builder = Request::builder()
            .method(method)
            .uri(&uri)
            .header("content-type", "application/json")
            .header(PROTO_HEADER, PROTO_VERSION.to_string());

        if let Some(signer) = &self.signer {
            builder = builder.header(
                AUTH_HEADER,
                signer.header(SignedRequest {
                    method,
                    path,
                    body: &body,
                }),
            );
        }

        let request = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| RpcError::Transport(e.to_string()))?;

        let response =
            tokio::time::timeout(self.config.request_timeout, self.http.request(request))
                .await
                .map_err(|_| RpcError::Timeout)?
                .map_err(|e| RpcError::Transport(e.to_string()))?;

        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|e| RpcError::Transport(e.to_string()))?
            .to_bytes();

        if status.is_success() {
            return Ok(bytes.to_vec());
        }
        Err(status_to_error(status.as_u16(), &bytes, method, path))
    }
}

/// Map a peer's error response back onto the typed error, so a remote refusal
/// is indistinguishable from a local one at the call site.
fn status_to_error(status: u16, body: &[u8], method: &str, path: &str) -> RpcError {
    let envelope = serde_json::from_slice::<serde_json::Value>(body).ok();
    let field = |name: &str| -> Option<String> {
        envelope
            .as_ref()
            .and_then(|v| v.get(name))
            .and_then(|v| v.as_str())
            .map(str::to_owned)
    };
    let detail = field("message").unwrap_or_else(|| format!("peer returned status {status}"));

    match status {
        // The peer names which credential check failed. Collapsing every 401
        // to `BadMac` would report a fleet-wide clock problem or a nonce-cache
        // overflow as a forged MAC, sending an operator hunting a secret
        // mismatch that isn't there.
        401 => RpcError::Unauthorized(auth_error_from_reason(field("error").as_deref())),
        404 => RpcError::UnknownRoute {
            method: method.to_owned(),
            path: path.to_owned(),
        },
        413 => RpcError::BodyTooLarge { limit: 0 },
        426 => RpcError::VersionSkew {
            peer: None,
            ours: PROTO_VERSION,
        },
        503 => RpcError::Shed,
        504 => RpcError::Timeout,
        _ => RpcError::Handler(detail),
    }
}

/// Recover the peer's specific auth failure from the error envelope. An absent
/// or unrecognized reason falls back to `BadMac` — the class that says "this
/// credential was not acceptable" without inventing a more specific claim.
fn auth_error_from_reason(reason: Option<&str>) -> AuthError {
    match reason {
        Some("malformed") => AuthError::Malformed,
        Some("stale_timestamp") => AuthError::StaleTimestamp,
        Some("replayed_nonce") => AuthError::ReplayedNonce,
        Some("nonce_cache_full") => AuthError::NonceCacheFull,
        _ => AuthError::BadMac,
    }
}

/// Exponential backoff with jitter: 50/100/200 ms ± 25%.
fn backoff(attempt: u32) -> Duration {
    let base = 50_u64.saturating_mul(1 << attempt.min(6).saturating_sub(1));
    let jitter = rand::thread_rng().gen_range(0..=base / 2);
    Duration::from_millis(base.saturating_sub(base / 4).saturating_add(jitter))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NeverHealthy;
    impl PeerHealth for NeverHealthy {
        fn is_healthy(&self, _peer: SocketAddr) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn client_fast_fails_unhealthy_peer_without_burning_the_deadline() {
        let client = RpcClient::new(
            Some(Signer::new("s")),
            Arc::new(NeverHealthy),
            RpcClientConfig {
                request_timeout: Duration::from_secs(30),
                ..Default::default()
            },
        );
        // 203.0.113.1 is TEST-NET-3: guaranteed unroutable, so a non-fast-fail
        // path would hang until the (30 s) deadline rather than returning.
        let peer: SocketAddr = "203.0.113.1:4790".parse().expect("valid test address");
        let started = std::time::Instant::now();
        let err = client
            .call(peer, "POST", "/internal/v1/ping", vec![])
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::Transport(_)), "{err:?}");
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "did not fast-fail"
        );
    }

    #[test]
    fn backoff_grows_and_stays_bounded() {
        for attempt in 1..=4 {
            let d = backoff(attempt);
            assert!(d >= Duration::from_millis(20), "attempt {attempt}: {d:?}");
            assert!(d <= Duration::from_millis(1000), "attempt {attempt}: {d:?}");
        }
        assert!(backoff(3) > backoff(1));
    }

    fn mapped(status: u16, body: &[u8]) -> RpcError {
        status_to_error(status, body, "POST", "/internal/v1/echo")
    }

    #[test]
    fn peer_status_maps_back_to_typed_errors() {
        assert!(matches!(mapped(401, b"{}"), RpcError::Unauthorized(_)));
        assert!(matches!(mapped(426, b"{}"), RpcError::VersionSkew { .. }));
        assert!(matches!(mapped(413, b"{}"), RpcError::BodyTooLarge { .. }));
        assert!(matches!(mapped(503, b"{}"), RpcError::Shed));
        assert!(matches!(mapped(504, b"{}"), RpcError::Timeout));
        assert!(
            matches!(mapped(500, br#"{"message":"boom"}"#), RpcError::Handler(m) if m == "boom")
        );
    }

    #[test]
    fn peer_auth_failures_keep_their_specific_reason() {
        // A clock-skew incident and a nonce-cache overflow must not both look
        // like a forged MAC at the caller.
        let cases = [
            (r#"{"error":"stale_timestamp"}"#, AuthError::StaleTimestamp),
            (r#"{"error":"replayed_nonce"}"#, AuthError::ReplayedNonce),
            (r#"{"error":"nonce_cache_full"}"#, AuthError::NonceCacheFull),
            (r#"{"error":"malformed"}"#, AuthError::Malformed),
            (r#"{"error":"bad_mac"}"#, AuthError::BadMac),
            // Absent or unrecognized reason: no more specific claim than
            // "this credential was refused".
            ("{}", AuthError::BadMac),
            (r#"{"error":"something-new"}"#, AuthError::BadMac),
        ];
        for (body, expected) in cases {
            assert_eq!(
                mapped(401, body.as_bytes()),
                RpcError::Unauthorized(expected),
                "body {body}"
            );
        }
    }

    #[test]
    fn unknown_route_reports_what_was_actually_called() {
        let err = mapped(
            404,
            br#"{"message":"unknown route: POST /internal/v1/echo"}"#,
        );
        assert_eq!(
            err,
            RpcError::UnknownRoute {
                method: "POST".into(),
                path: "/internal/v1/echo".into()
            }
        );
    }
}
