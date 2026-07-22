//! The peer-facing RPC client: pooled connections, signed requests, fast-fail.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

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

    /// Record a call to `peer` that completed successfully. Default no-op:
    /// most health sources (tests, [`AlwaysHealthy`]) don't track outcomes:
    /// only a tracking implementation needs to act on this.
    fn record_success(&self, _peer: SocketAddr) {}

    /// Record a call to `peer` that failed. Default no-op; see
    /// [`Self::record_success`].
    fn record_failure(&self, _peer: SocketAddr) {}
}

/// Health source for single-node and test use: never fast-fails.
pub struct AlwaysHealthy;

impl PeerHealth for AlwaysHealthy {
    fn is_healthy(&self, _peer: SocketAddr) -> bool {
        true
    }
}

/// Number of consecutive failures [`TrackedPeerHealth`] tolerates before
/// marking a peer unhealthy.
const DEFAULT_FAILURE_THRESHOLD: u32 = 3;

/// How long [`TrackedPeerHealth`] keeps a tripped peer marked unhealthy.
const DEFAULT_COOLDOWN: Duration = Duration::from_secs(5);

#[derive(Default)]
struct PeerState {
    consecutive_failures: u32,
    unhealthy_until: Option<Instant>,
}

/// A real, locally observed [`PeerHealth`]: after
/// [`DEFAULT_FAILURE_THRESHOLD`] consecutive failed calls to a peer, it reports
/// unhealthy for a bounded cooldown so [`RpcClient::call`] fast-fails instead
/// of burning the full connect+request timeout on a peer already known to be
/// down. A success clears the streak immediately; a cooldown that elapses
/// without one also clears it (the peer gets a fresh run at the threshold
/// rather than staying flagged forever on stale failures).
pub struct TrackedPeerHealth {
    state: Mutex<HashMap<SocketAddr, PeerState>>,
    threshold: u32,
    cooldown: Duration,
}

impl Default for TrackedPeerHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackedPeerHealth {
    /// A tracker using the default threshold (3) and cooldown (5s).
    #[must_use]
    pub fn new() -> Self {
        Self::with_params(DEFAULT_FAILURE_THRESHOLD, DEFAULT_COOLDOWN)
    }

    /// A tracker with an explicit threshold and cooldown, for callers (and
    /// tests) that need to tune the sensitivity.
    #[must_use]
    pub fn with_params(threshold: u32, cooldown: Duration) -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
            threshold,
            cooldown,
        }
    }
}

impl PeerHealth for TrackedPeerHealth {
    fn is_healthy(&self, peer: SocketAddr) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match state.get(&peer) {
            Some(entry) => match entry.unhealthy_until {
                Some(until) if Instant::now() < until => false,
                Some(_) => {
                    // Cooldown elapsed on its own: treat it as recovered, same
                    // as an explicit success, so a later failure needs a fresh
                    // run at the threshold rather than tripping on the very
                    // next attempt.
                    state.remove(&peer);
                    true
                }
                None => true,
            },
            None => true,
        }
    }

    fn record_success(&self, peer: SocketAddr) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.remove(&peer);
    }

    fn record_failure(&self, peer: SocketAddr) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = state.entry(peer).or_default();
        entry.consecutive_failures += 1;
        if entry.consecutive_failures >= self.threshold {
            entry.unhealthy_until = Some(Instant::now() + self.cooldown);
        }
    }
}

/// Resolves a peer's advertised authority (`host:port`) to a dialable
/// [`SocketAddr`], fresh on every call — no caching, so a changed pod IP (a
/// StatefulSet rollout, a service mesh reassigning an address) is picked up on
/// the very next attempt rather than baked in once at connection time.
/// Injectable so tests can substitute a mock without doing real DNS.
pub trait PeerResolver: Send + Sync {
    fn resolve(&self, authority: &str) -> std::io::Result<SocketAddr>;
}

/// The production resolver: standard OS/DNS resolution via
/// [`std::net::ToSocketAddrs`], which — unlike a bare [`str::parse`] — accepts
/// hostnames as well as literal addresses.
pub struct DnsResolver;

impl PeerResolver for DnsResolver {
    fn resolve(&self, authority: &str) -> std::io::Result<SocketAddr> {
        use std::net::ToSocketAddrs;
        authority
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| std::io::Error::other(format!("no address found for {authority}")))
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
                Ok(response) => {
                    self.health.record_success(peer);
                    return Ok(response);
                }
                Err(e) if e.is_retryable() && attempt < self.config.max_retries => {
                    attempt += 1;
                    tokio::time::sleep(backoff(attempt)).await;
                }
                Err(e) => {
                    // Only liveness failures count against a peer's health. A
                    // `Handler` 500 proves the opposite — the peer answered — and
                    // counting it would fast-fail a live node for refusing a
                    // request it was right to refuse: a still-booting seed
                    // ("raft not yet initialized"), or a leader legitimately
                    // rejecting an eviction while a membership change is in
                    // flight. Three of those in a row must not blind us to the
                    // one node we actually need.
                    if e.is_liveness_failure() {
                        self.health.record_failure(peer);
                    }
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
    fn tracked_health_trips_after_threshold_consecutive_failures() {
        let health = TrackedPeerHealth::with_params(3, Duration::from_secs(5));
        let peer: SocketAddr = "127.0.0.1:4001".parse().expect("valid addr");
        assert!(health.is_healthy(peer), "unknown peer starts healthy");
        health.record_failure(peer);
        health.record_failure(peer);
        assert!(
            health.is_healthy(peer),
            "under the threshold, the peer must stay healthy"
        );
        health.record_failure(peer);
        assert!(
            !health.is_healthy(peer),
            "the Nth consecutive failure must trip it unhealthy"
        );
    }

    #[test]
    fn tracked_health_short_circuits_during_cooldown_and_recovers_after() {
        let health = TrackedPeerHealth::with_params(3, Duration::from_millis(50));
        let peer: SocketAddr = "127.0.0.1:4002".parse().expect("valid addr");
        for _ in 0..3 {
            health.record_failure(peer);
        }
        assert!(
            !health.is_healthy(peer),
            "must short-circuit immediately after tripping"
        );
        std::thread::sleep(Duration::from_millis(80));
        assert!(
            health.is_healthy(peer),
            "must recover once the cooldown elapses, even without a success"
        );
    }

    #[test]
    fn tracked_health_success_clears_the_failure_streak() {
        let health = TrackedPeerHealth::with_params(3, Duration::from_secs(5));
        let peer: SocketAddr = "127.0.0.1:4003".parse().expect("valid addr");
        health.record_failure(peer);
        health.record_failure(peer);
        health.record_success(peer);
        health.record_failure(peer);
        health.record_failure(peer);
        assert!(
            health.is_healthy(peer),
            "a success must reset the streak, not merely pause it \
             (2 + success + 2 must never reach the threshold of 3)"
        );
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
