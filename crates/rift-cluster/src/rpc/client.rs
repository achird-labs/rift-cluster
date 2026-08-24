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

/// Resolves a peer's advertised authority (`host:port`) to the dialable
/// [`SocketAddr`]s it names, fresh on every call — no caching, so a changed pod
/// IP (a StatefulSet rollout, a service mesh reassigning an address) is picked
/// up on the very next attempt rather than baked in once at connection time.
/// Injectable so tests can substitute a mock without doing real DNS.
///
/// **Every** address is returned, in the resolver's own order, and callers try
/// them in turn (#79). Returning only the first made a dual-stack name whose
/// leading address nobody listens on permanently unreachable, even with a live
/// address sitting second in the same answer.
///
/// An implementation must never answer `Ok` with an empty vec: no addresses is
/// a resolution failure, and returning it as success would hand callers a list
/// they silently loop zero times over.
pub trait PeerResolver: Send + Sync {
    fn resolve(&self, authority: &str) -> std::io::Result<Vec<SocketAddr>>;
}

/// The production resolver: standard OS/DNS resolution via
/// [`std::net::ToSocketAddrs`], which — unlike a bare [`str::parse`] — accepts
/// hostnames as well as literal addresses.
pub struct DnsResolver;

impl PeerResolver for DnsResolver {
    fn resolve(&self, authority: &str) -> std::io::Result<Vec<SocketAddr>> {
        use std::net::ToSocketAddrs;
        // The OS resolver's order is preserved exactly. It implements RFC 6724
        // destination-address selection and is the only component that knows
        // this host's actual connectivity; re-sorting it here — "prefer IPv4",
        // say — would pick a guaranteed-unreachable address on an IPv6-only
        // host whose name still carries a stale A record. That is this bug
        // mirrored, not fixed.
        let addrs: Vec<SocketAddr> = authority.to_socket_addrs()?.collect();
        if addrs.is_empty() {
            return Err(std::io::Error::other(format!(
                "no address found for {authority}"
            )));
        }
        Ok(addrs)
    }
}

/// A validated `host:port` authority — anything a peer can be dialed at:
/// hostname, IPv4 literal, or bracketed IPv6 literal (issue #68). Stored and
/// displayed byte-for-byte as given, never normalised: membership persists
/// this string verbatim, and [`PeerResolver::resolve`] must receive exactly
/// what an operator wrote so a hostname is re-resolved, not pinned to
/// whichever address it happened to mean at parse time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authority(String);

impl Authority {
    /// The authority exactly as parsed or built.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<SocketAddr> for Authority {
    fn from(addr: SocketAddr) -> Self {
        // `SocketAddr::to_string` already brackets IPv6, so this round-trips
        // losslessly through `FromStr`'s literal fast path.
        Self(addr.to_string())
    }
}

impl std::str::FromStr for Authority {
    type Err = AuthorityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // A literal `SocketAddr` — IPv4, or IPv6 in the required `[..]:port`
        // form — is always a valid authority; accept it verbatim rather than
        // re-deriving it from the host/port split below.
        if s.parse::<SocketAddr>().is_ok() {
            return Ok(Self(s.to_owned()));
        }

        let Some((host, port)) = s.rsplit_once(':') else {
            return Err(AuthorityError::MissingPort(s.to_owned()));
        };
        if host.is_empty() {
            return Err(AuthorityError::EmptyHost(s.to_owned()));
        }
        // Only a host that is *neither* bracketed nor scheme-prefixed can be a
        // mis-typed IPv6 literal. Testing for a colon alone claimed everything
        // from `[::1]:` to `http://rift-0:4790` was an unbracketed IPv6 address
        // — telling an operator to bracket something they never wrote.
        if host.contains(':') && !host.starts_with('[') && !host.contains("//") {
            // `::1:4790` is ambiguous: it could equally be the unbracketed
            // IPv6 literal `::1:4790` with no port at all. Refuse it rather
            // than guess — the operator must bracket it, exactly as
            // `SocketAddr`'s own parser already requires.
            return Err(AuthorityError::UnbracketedIpv6(s.to_owned()));
        }
        // A hostname or bracketed literal, and nothing else. Without this a
        // pasted URL or a typo parses, reaches the Raft log, and becomes a
        // membership entry that can never resolve — removable only by an admin
        // membership change. Rejecting it at the boundary is the whole reason
        // this is a validated type rather than a `String`.
        if !host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '[' | ']' | ':'))
        {
            return Err(AuthorityError::InvalidHost(s.to_owned()));
        }
        port.parse::<u16>()
            .map_err(|_| AuthorityError::InvalidPort(s.to_owned()))?;

        Ok(Self(s.to_owned()))
    }
}

/// Why a `host:port` authority was refused, so clap's error names the
/// specific problem rather than a generic parse failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthorityError {
    /// No `:` at all, so there is no port to read.
    #[error("{0:?} has no port; expected host:port")]
    MissingPort(String),
    /// The host half was empty (`:4790`).
    #[error("{0:?} has an empty host; expected host:port")]
    EmptyHost(String),
    /// The host half contains a character no hostname or IP literal has —
    /// whitespace, a URL scheme or path, or any other non-ASCII-hostname byte.
    #[error("{0:?} has an invalid host; expected host:port")]
    InvalidHost(String),
    /// The port half did not parse as a `u16`.
    #[error("{0:?} has an invalid port; expected host:port")]
    InvalidPort(String),
    /// An unbracketed IPv6 literal — ambiguous with a bare host containing a
    /// colon. Write it as `[addr]:port`.
    #[error("{0:?} looks like an unbracketed IPv6 address; write it as [addr]:port")]
    UnbracketedIpv6(String),
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

/// Floor on the link speed a bulk replication transfer is granted before it is declared failed:
/// an 8 MiB body at this rate is 8 s on top of the ordinary request timeout.
///
/// Deliberately not a tuning knob and deliberately generous. It exists so a transfer whose size
/// the cluster has *already accepted* (a snapshot chunk, a log entry carrying a dataset) is not
/// cut off by a timeout sized for a small control RPC. A link genuinely slower than this fails
/// the transfer, which is the honest outcome — the write parks and replays.
pub(crate) const MIN_REPLICATION_BYTES_PER_SEC: u64 = 1024 * 1024;

impl RpcClientConfig {
    /// The deadline one attempt at `body_len` bytes of bulk replication traffic gets:
    /// [`Self::request_timeout`] plus the time [`MIN_REPLICATION_BYTES_PER_SEC`] needs to carry
    /// the body. `body_len` of 0 is exactly `request_timeout`.
    #[must_use]
    pub(crate) fn replication_deadline(&self, body_len: usize) -> Duration {
        // Infallible on every platform this crate targets (`usize` is never wider than `u64`).
        // The fallback is here only to keep a panicking conversion off a path that cannot fail,
        // and it errs toward a longer deadline rather than a shorter one.
        let bytes = u64::try_from(body_len).unwrap_or(u64::MAX);
        let millis = bytes.saturating_mul(1_000) / MIN_REPLICATION_BYTES_PER_SEC;
        self.request_timeout
            .saturating_add(Duration::from_millis(millis))
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

    /// The deadline one attempt at `body_len` bytes of bulk replication traffic gets. See
    /// [`RpcClientConfig::replication_deadline`].
    #[must_use]
    pub(crate) fn replication_deadline(&self, body_len: usize) -> Duration {
        self.config.replication_deadline(body_len)
    }

    /// Fast-fail against a peer the local view already knows is gone, rather than parking the
    /// caller for the full deadline.
    fn refuse_if_unhealthy(&self, peer: SocketAddr) -> Result<(), RpcError> {
        if self.health.is_healthy(peer) {
            return Ok(());
        }
        let err = RpcError::Transport(format!("peer {peer} is not healthy"));
        metrics::rpc_failure(err.reason());
        Err(err)
    }

    /// Charge a completed attempt to the peer's health.
    ///
    /// Only liveness failures count against a peer. A `Handler` 500 proves the opposite — the
    /// peer answered — and counting it would fast-fail a live node for refusing a request it was
    /// right to refuse: a still-booting seed ("raft not yet initialized"), or a leader
    /// legitimately rejecting an eviction while a membership change is in flight. Three of those
    /// in a row must not blind us to the one node we actually need.
    fn record_outcome(&self, peer: SocketAddr, outcome: &Result<Vec<u8>, RpcError>) {
        match outcome {
            Ok(_) => self.health.record_success(peer),
            Err(e) => {
                if e.is_liveness_failure() {
                    self.health.record_failure(peer);
                }
                metrics::rpc_failure(e.reason());
            }
        }
    }

    /// Call `method path` on `peer` with `body`, retrying transient failures. Each attempt is
    /// bounded by [`RpcClientConfig::request_timeout`].
    pub async fn call(
        &self,
        peer: SocketAddr,
        method: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, RpcError> {
        self.refuse_if_unhealthy(peer)?;

        let mut attempt = 0;
        loop {
            let result = self
                .attempt(
                    peer,
                    method,
                    path,
                    body.clone(),
                    self.config.request_timeout,
                )
                .await;
            let retryable = matches!(&result, Err(e) if e.is_retryable());
            if retryable && attempt < self.config.max_retries {
                attempt += 1;
                tokio::time::sleep(backoff(attempt)).await;
                continue;
            }
            self.record_outcome(peer, &result);
            return result;
        }
    }

    /// Call `method path` on `peer` exactly once, bounded by `deadline`.
    ///
    /// (An inherent method — unrelated to [`FnOnce::call_once`], which it shadows in name only.)
    ///
    /// For a body too large to be governed by the flat [`RpcClientConfig::request_timeout`] — a
    /// Raft snapshot chunk, a log entry carrying a dataset — where the caller is *already* a retry
    /// loop. Retrying here would re-send the whole body under a deadline meant for a small control
    /// RPC, which is how a transfer that simply needs more time becomes a transfer that never
    /// completes (#428, #411). Health accounting is identical to [`Self::call`].
    ///
    /// Pair it with [`Self::replication_deadline`] rather than a flat constant, so the deadline
    /// scales with what is actually being sent.
    pub async fn call_once(
        &self,
        peer: SocketAddr,
        method: &str,
        path: &str,
        body: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcError> {
        self.refuse_if_unhealthy(peer)?;
        let result = self.attempt(peer, method, path, body, deadline).await;
        // A caller-supplied deadline expiring says the payload did not fit the link in the time
        // this caller allowed — not that the peer is unreachable, which is what `PeerHealth`
        // exists to track. Charging it would let three slow chunks mark the peer unhealthy and
        // fast-fail *every* RPC to it for the cooldown, heartbeats included, so the fix's own
        // failure mode would stop us talking to a node that is merely on a slow link. Liveness is
        // still observed, and far more often, by the ordinary small RPCs going through `call`.
        if matches!(result, Err(RpcError::Timeout)) {
            metrics::rpc_failure(RpcError::Timeout.reason());
        } else {
            self.record_outcome(peer, &result);
        }
        result
    }

    /// One HTTP round trip, bounded by `deadline`.
    ///
    /// The deadline is a parameter rather than read from `self.config` because the bound has to
    /// govern from *inside*: a caller wrapping this in its own `timeout` would leave whichever
    /// bound is shorter binding, so a long deadline around a short `request_timeout` would be
    /// silently ignored and every bulk transfer would still be cut off at `request_timeout`.
    async fn attempt(
        &self,
        peer: SocketAddr,
        method: &str,
        path: &str,
        body: Vec<u8>,
        deadline: Duration,
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

        // The deadline covers response-body collection too, not just the head. Timing out only the
        // head would leave a peer that sends headers and then stalls mid-body hanging the caller
        // for ever — and on the bulk path there is no retry above this inside `RpcClient` to
        // notice.
        let (status, bytes) = tokio::time::timeout(deadline, async {
            let response = self
                .http
                .request(request)
                .await
                .map_err(|e| RpcError::Transport(e.to_string()))?;
            let status = response.status();
            let bytes = response
                .into_body()
                .collect()
                .await
                .map_err(|e| RpcError::Transport(e.to_string()))?
                .to_bytes();
            Ok::<_, RpcError>((status, bytes))
        })
        .await
        .map_err(|_| RpcError::Timeout)??;

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
        // Preserved as its own class rather than folded into `Handler`: the
        // caller's next move differs entirely. A `BadRequest` will fail
        // identically on every retry and names something the operator can fix;
        // a `Handler` error is the peer failing at something it should have
        // managed, and is worth escalating rather than rewriting the request.
        400 => RpcError::BadRequest(detail),
        404 => RpcError::UnknownRoute {
            method: method.to_owned(),
            path: path.to_owned(),
        },
        413 => RpcError::BodyTooLarge { limit: 0 },
        // The peer is a follower and named the leader (or an election is in
        // flight and it could not). Recovered as a field rather than parsed out
        // of `message`, so the caller can re-issue to the named node (#391).
        421 => RpcError::NotLeader {
            leader: field("leader"),
        },
        426 => RpcError::VersionSkew {
            peer: None,
            ours: PROTO_VERSION,
        },
        // Two different 503s share this status: local shedding (no bridge
        // capacity) and a write the cluster could not commit. They are told
        // apart by the envelope's reason label, the same way the 401 arm
        // recovers which credential check failed — collapsing them would lose
        // the op id a client needs to poll for the write's real outcome.
        503 => match field("error").as_deref() {
            Some("unavailable") => RpcError::Unavailable {
                detail,
                op_id: field("opId"),
            },
            _ => RpcError::Shed,
        },
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Issue #68: what an advertise address is allowed to be.
    ///
    /// The whole point is that a *name* is now allowed, so the accepting half
    /// matters as much as the rejecting half.
    #[test]
    fn authority_accepts_hostname_ipv4_and_bracketed_ipv6() {
        for good in [
            "rift-0.rift-headless.ns.svc.cluster.local:4790",
            "localhost:4790",
            "127.0.0.1:4790",
            "[::1]:4790",
            "[2001:db8::1]:4790",
        ] {
            let authority: Authority = good.parse().unwrap_or_else(|e| panic!("{good}: {e}"));
            assert_eq!(
                authority.as_str(),
                good,
                "an authority must round-trip verbatim — membership stores this string"
            );
        }
    }

    #[test]
    fn authority_rejects_missing_port_empty_host_and_unbracketed_ipv6() {
        for bad in [
            "rift-0",
            "rift-0:",
            ":4790",
            "rift-0:notaport",
            "host:70000",
        ] {
            assert!(
                bad.parse::<Authority>().is_err(),
                "{bad:?} must not parse as an authority"
            );
        }

        // Ambiguous rather than merely malformed: `::1:4790` could be read as
        // the IPv6 address `::1:4790` with no port, so it is refused with the
        // bracketing spelled out instead of guessed at.
        let err = "::1:4790"
            .parse::<Authority>()
            .expect_err("an unbracketed IPv6 literal is ambiguous");
        assert!(
            format!("{err}").contains('['),
            "the error must tell the operator to bracket it: {err}"
        );
    }

    /// A value that parses becomes a durable membership entry, so anything the
    /// resolver could never answer must be refused at the boundary rather than
    /// written to the Raft log and left for an admin membership change.
    #[test]
    fn authority_rejects_url_shaped_and_structurally_invalid_hosts() {
        for bad in [
            "http://rift-0:4790",
            "user@rift-0:4790",
            "//rift-0:4790",
            "rift-0/foo:4790",
            "*:4790",
            "münchen:4790",
            "rift 0:4790",
        ] {
            assert!(
                bad.parse::<Authority>().is_err(),
                "{bad:?} can never resolve, so it must not reach membership"
            );
        }
    }

    /// The bracketing advice must be reserved for values it actually applies
    /// to — telling an operator to bracket an IPv6 address they never wrote
    /// sends them chasing the wrong problem.
    #[test]
    fn authority_blames_the_real_problem_not_ipv6_bracketing() {
        for (input, expect_brackets) in [
            ("::1:4790", true),
            ("http://rift-0:4790", false),
            ("[::1]:", false),
            ("[::1]:99999", false),
        ] {
            let err = input
                .parse::<Authority>()
                .expect_err("all of these are invalid");
            assert_eq!(
                format!("{err}").contains("unbracketed IPv6"),
                expect_brackets,
                "{input:?} was blamed on the wrong thing: {err}"
            );
        }
    }

    #[test]
    fn authority_from_socket_addr_round_trips() {
        for addr in ["127.0.0.1:4790", "[::1]:4790"] {
            let socket: SocketAddr = addr.parse().expect("socket addr");
            let authority = Authority::from(socket);
            assert_eq!(
                authority.as_str().parse::<SocketAddr>().ok(),
                Some(socket),
                "a literal must survive the newtype so the fast path still fires"
            );
            assert!(
                authority.as_str().parse::<Authority>().is_ok(),
                "and must re-parse as an authority — this is the default-advertise path"
            );
        }
    }

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
        assert!(
            matches!(mapped(400, br#"{"message":"nope"}"#), RpcError::BadRequest(m) if m == "nope"),
            "a peer's refusal of the request must not read as the peer failing"
        );
    }

    /// Both 503 classes share a status, so the reason label is what tells them
    /// apart — and an `Unavailable` must carry its op id across the wire, or a
    /// client cannot poll for the write's real outcome.
    #[test]
    fn the_two_503_classes_are_told_apart_by_their_reason() {
        assert!(matches!(
            mapped(503, br#"{"error":"shed"}"#),
            RpcError::Shed
        ));
        assert!(
            matches!(mapped(503, b"{}"), RpcError::Shed),
            "an unlabelled 503 keeps the pre-existing meaning"
        );
        let mapped = mapped(
            503,
            br#"{"error":"unavailable","message":"no quorum","opId":"1a2b"}"#,
        );
        match mapped {
            RpcError::Unavailable { detail, op_id } => {
                assert_eq!(detail, "no quorum");
                assert_eq!(op_id.as_deref(), Some("1a2b"));
            }
            other => panic!("expected Unavailable, got {other:?}"),
        }
    }

    /// #391: the leader hint has to survive the wire as *data*. Before the fix
    /// it existed only inside a 500's rendered message, which no caller could
    /// act on without parsing prose.
    #[test]
    fn a_421_carries_the_leader_hint_back_as_typed_data() {
        match mapped(
            421,
            br#"{"error":"not_leader","message":"not the leader; leader is 10.0.0.7:7000","leader":"10.0.0.7:7000"}"#,
        ) {
            RpcError::NotLeader { leader } => {
                assert_eq!(leader.as_deref(), Some("10.0.0.7:7000"));
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }

        // An election in flight names nobody. The variant must still come back
        // typed — "no leader yet" and "not this node" are the same class of
        // answer and the caller distinguishes them by the absent hint.
        match mapped(421, br#"{"error":"not_leader","message":"not the leader"}"#) {
            RpcError::NotLeader { leader } => assert_eq!(leader, None),
            other => panic!("expected a hintless NotLeader, got {other:?}"),
        }
    }

    /// #391 skew guard: the join reply must stay a non-2xx. A structured
    /// "forward" carried in a 200 would read as success to any deployed joiner,
    /// which ignores the join reply body — it would record itself joined
    /// without being a member.
    #[test]
    fn a_redirect_is_never_a_success_status() {
        let err = mapped(421, br#"{"error":"not_leader"}"#);
        assert_eq!(err.status(), 421);
        assert!(!(200..300).contains(&err.status()));
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
        // Polled, not slept: a fixed 80 ms against a 50 ms cooldown left a
        // 30 ms margin, which a loaded CI box eats. Waiting longer than needed
        // costs nothing here, but failing on scheduler jitter costs a rerun and
        // teaches people to ignore the suite.
        let deadline = Instant::now() + Duration::from_secs(2);
        while !health.is_healthy(peer) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
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

    /// A listener that answers `status` after `delay`, counting the requests it received.
    ///
    /// Deliberately hand-rolled rather than an `RpcServer`: these tests are about *when* the
    /// client gives up, so the responder has to be slow on demand, and the connection count is
    /// the only way to prove a retry did or did not happen.
    async fn slow_responder(
        delay: Duration,
        status: u16,
        body: &'static str,
    ) -> (SocketAddr, Arc<AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("bound address");
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    // Drain what the client sent so it is never blocked writing while we sleep.
                    let mut scratch = vec![0u8; 64 * 1024];
                    let _ = stream.read(&mut scratch).await;
                    tokio::time::sleep(delay).await;
                    let response = format!(
                        "HTTP/1.1 {status} STATUS\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        (addr, hits)
    }

    fn test_client(health: Arc<dyn PeerHealth>, config: RpcClientConfig) -> RpcClient {
        RpcClient::new(None, health, config)
    }

    /// #428: the caller's deadline governs even when it is **longer** than `request_timeout`.
    ///
    /// This is the whole point of the helper and the one property an obvious implementation
    /// (`timeout(deadline, self.attempt(..))`) silently fails: `attempt` applies `request_timeout`
    /// internally, so wrapping from outside leaves the shorter inner bound binding and a bulk
    /// transfer is cut off at 2 s no matter what the caller asked for.
    #[tokio::test]
    async fn call_once_deadline_outlives_a_shorter_request_timeout() {
        let (addr, _hits) = slow_responder(Duration::from_millis(600), 200, "ok").await;
        let client = test_client(
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                request_timeout: Duration::from_millis(200),
                ..Default::default()
            },
        );

        let response = client
            .call_once(
                addr,
                "POST",
                "/internal/v1/echo",
                vec![],
                Duration::from_secs(5),
            )
            .await
            .expect("the caller's 5 s deadline must govern, not the 200 ms request_timeout");

        assert_eq!(response, b"ok");
    }

    /// The same knob in the other direction: a deadline **shorter** than `request_timeout` must
    /// also bind, so a caller can ask for less than the configured budget.
    #[tokio::test]
    async fn call_once_stops_at_a_deadline_shorter_than_the_request_timeout() {
        let (addr, _hits) = slow_responder(Duration::from_secs(30), 200, "ok").await;
        let client = test_client(
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                request_timeout: Duration::from_secs(30),
                ..Default::default()
            },
        );

        let started = std::time::Instant::now();
        let err = client
            .call_once(
                addr,
                "POST",
                "/internal/v1/echo",
                vec![],
                Duration::from_millis(300),
            )
            .await
            .expect_err("the 300 ms deadline must bind");

        assert_eq!(err, RpcError::Timeout);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "gave up after {:?}, so the deadline did not bind",
            started.elapsed()
        );
    }

    /// #428: openraft is already the retry loop for a bulk transfer, and a retry re-sends the
    /// whole payload — so this helper attempts exactly once even for a retryable failure.
    #[tokio::test]
    async fn call_once_does_not_retry_a_retryable_failure() {
        let (addr, hits) = slow_responder(Duration::from_secs(30), 200, "ok").await;
        let client = test_client(
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                request_timeout: Duration::from_secs(30),
                max_retries: 3,
                ..Default::default()
            },
        );

        let err = client
            .call_once(
                addr,
                "POST",
                "/internal/v1/echo",
                vec![],
                Duration::from_millis(300),
            )
            .await
            .expect_err("times out");

        assert_eq!(err, RpcError::Timeout);
        assert!(err.is_retryable(), "the failure is one `call` would retry");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "call_once must attempt exactly once, whatever max_retries says"
        );
    }

    /// Threading a deadline through `attempt` must not change what `call` does with its own
    /// configured budget.
    #[tokio::test]
    async fn call_still_honours_the_configured_request_timeout() {
        let (addr, hits) = slow_responder(Duration::from_secs(30), 200, "ok").await;
        let client = test_client(
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                request_timeout: Duration::from_millis(300),
                max_retries: 0,
                ..Default::default()
            },
        );

        let started = std::time::Instant::now();
        let err = client
            .call(addr, "POST", "/internal/v1/echo", vec![])
            .await
            .expect_err("the configured request timeout still applies");

        assert_eq!(err, RpcError::Timeout);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "call waited {:?}, so request_timeout stopped governing",
            started.elapsed()
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "max_retries: 0 means one try"
        );
    }

    /// A long deadline must not become a long hang against a peer already known to be gone.
    #[tokio::test]
    async fn call_once_fast_fails_an_unhealthy_peer() {
        let client = test_client(Arc::new(NeverHealthy), RpcClientConfig::default());
        // TEST-NET-3: guaranteed unroutable, so anything but a fast-fail parks for the deadline.
        let peer: SocketAddr = "203.0.113.1:4790".parse().expect("valid test address");

        let started = std::time::Instant::now();
        let err = client
            .call_once(
                peer,
                "POST",
                "/internal/v1/ping",
                vec![],
                Duration::from_secs(30),
            )
            .await
            .expect_err("an unhealthy peer is refused up front");

        assert!(matches!(err, RpcError::Transport(_)), "{err:?}");
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "did not fast-fail: {:?}",
            started.elapsed()
        );
    }

    /// The bulk path keeps `call`'s health accounting exactly: a peer that *answers* — even to
    /// refuse — is alive, and must not be cooled down for it.
    #[tokio::test]
    async fn call_once_does_not_count_a_handler_refusal_against_peer_health() {
        let (addr, _hits) = slow_responder(Duration::from_millis(0), 500, "{}").await;
        let health = Arc::new(TrackedPeerHealth::with_params(1, Duration::from_secs(30)));
        let client = test_client(
            Arc::clone(&health) as Arc<dyn PeerHealth>,
            RpcClientConfig::default(),
        );

        let err = client
            .call_once(
                addr,
                "POST",
                "/internal/v1/echo",
                vec![],
                Duration::from_secs(5),
            )
            .await
            .expect_err("a 500 is a refusal");

        assert!(matches!(err, RpcError::Handler(_)), "{err:?}");
        assert!(
            health.is_healthy(addr),
            "a peer that answered must stay healthy, even at threshold 1"
        );
    }

    /// #428: the bulk deadline is `request_timeout` plus a floor on link speed, so an 8 MiB
    /// snapshot chunk is granted seconds rather than being cut off at the flat 2 s budget.
    ///
    /// Literal expectations against an explicit 2 s `request_timeout` — the arithmetic is the
    /// contract, so it is spelled out rather than recomputed from the constant.
    #[test]
    fn replication_deadline_scales_with_body_size() {
        let config = RpcClientConfig {
            request_timeout: Duration::from_secs(2),
            ..Default::default()
        };

        assert_eq!(
            config.replication_deadline(0),
            Duration::from_millis(2_000),
            "an empty body gets exactly the configured budget"
        );
        assert_eq!(
            config.replication_deadline(512 * 1024),
            Duration::from_millis(2_500)
        );
        assert_eq!(
            config.replication_deadline(1024 * 1024),
            Duration::from_millis(3_000)
        );
        assert_eq!(
            config.replication_deadline(4 * 1024 * 1024),
            Duration::from_millis(6_000)
        );
        // The cluster port's own body cap, which is the largest body that can ever reach this.
        assert_eq!(
            config.replication_deadline(32 * 1024 * 1024),
            Duration::from_millis(34_000)
        );
    }

    /// A length no link could carry must saturate rather than overflow the deadline.
    #[test]
    fn replication_deadline_saturates_instead_of_overflowing() {
        let config = RpcClientConfig::default();
        let deadline = config.replication_deadline(usize::MAX);
        assert!(deadline >= config.request_timeout);
    }
}
