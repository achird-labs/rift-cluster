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

/// Locally observed peer liveness.
///
/// Trait-shaped so membership can supply real health once it exists: the point
/// is that a peer already known to be down costs zero wall-clock, instead of
/// burning the full connect+request deadline on every request during an outage.
pub trait PeerHealth: Send + Sync {
    /// Decide whether a call to `peer` may proceed.
    ///
    /// Returns an [`Admission`] rather than a `bool` because the answer is not
    /// only yes or no: a call may be let through as the one half-open **trial**
    /// against a peer marked down (D-78). The caller hands the token back to
    /// [`Self::record_success`], [`Self::record_failure`] or [`Self::release`], so
    /// that only the trial's *own* outcome can close the trial. Without it, a call
    /// admitted before the peer tripped, failing after a trial was admitted, would
    /// close that trial's window — and against a peer that never answers, one such
    /// misattribution lets trials pile up at one per interval.
    fn admit(&self, peer: SocketAddr) -> Admission;

    /// Record a call to `peer` that got an answer — a success, or a refusal that
    /// proves the peer is reachable. Default no-op: most health sources (tests,
    /// [`AlwaysHealthy`]) don't track outcomes.
    fn record_success(&self, _peer: SocketAddr, _admission: Admission) {}

    /// Record a call to `peer` that failed for a reason about the peer's
    /// reachability. Default no-op; see [`Self::record_success`].
    fn record_failure(&self, _peer: SocketAddr, _admission: Admission) {}

    /// A call ended with no evidence either way — a deadline the *caller* chose
    /// ran out (#442). Default no-op. For a trial this reopens the window rather
    /// than re-arming the mark.
    fn release(&self, _peer: SocketAddr, _admission: Admission) {}
}

/// What [`PeerHealth::admit`] decided about one call (D-78).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "the admission must be handed back when the call ends, or a trial is never resolved"]
pub enum Admission {
    /// The peer is believed reachable; the call proceeds normally.
    Healthy,
    /// The peer is marked down, and this call is the one half-open trial —
    /// *this* trial, which is what the id is for.
    Trial(TrialId),
    /// The peer is marked down and no trial is due; the call must not be made.
    Refused,
}

/// Identifies one half-open trial, so that its outcome can resolve only itself.
///
/// A flag alone cannot tell "my trial is open" from "a later trial is open". A
/// trial can outlive its own episode — the entry is cleared by a success or by
/// the cooldown, the peer trips again, and a new trial goes out — and when the
/// first one finally reports, a bare flag would let it close the second's window,
/// putting two trials in flight at once (#599's second review). Ids come from one
/// tracker-wide counter rather than a per-entry one, because entries are removed
/// and recreated and a per-entry counter would restart and collide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrialId(u64);

/// Health source for single-node and test use: never fast-fails.
pub struct AlwaysHealthy;

impl PeerHealth for AlwaysHealthy {
    fn admit(&self, _peer: SocketAddr) -> Admission {
        Admission::Healthy
    }
}

/// Number of consecutive failures [`TrackedPeerHealth`] tolerates before
/// marking a peer unhealthy.
const DEFAULT_FAILURE_THRESHOLD: u32 = 3;

/// How long [`TrackedPeerHealth`] keeps a tripped peer marked unhealthy *if no
/// trial ever reaches it* — see [`DEFAULT_HALF_OPEN_INTERVAL`], which is what
/// governs recovery in the ordinary case.
const DEFAULT_COOLDOWN: Duration = Duration::from_secs(5);

/// How often a peer marked unhealthy admits one **trial** call (D-78).
///
/// The cost of the half-open state is bounded by "one trial in flight", not by
/// this interval: a second caller is refused while a trial is outstanding
/// whatever the interval says. So this only paces *resolved* trials — it stops a
/// peer that fast-fails (`ECONNREFUSED` returns immediately) from admitting a
/// tight loop of them — which is why it can be short, and short is what makes
/// recovery fast.
///
/// 250ms is five Raft liveness ticks (`raft::network::LIVENESS_TICK`, 50ms) and
/// five heartbeat intervals, so a peer that is back is found within the window
/// the Raft layer needs to notice the same thing.
const DEFAULT_HALF_OPEN_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Default)]
struct PeerState {
    consecutive_failures: u32,
    unhealthy_until: Option<Instant>,
    /// When the current pacing window opened — set when the mark is armed, and
    /// again whenever a trial is admitted or fails. A trial is admitted once
    /// [`TrackedPeerHealth::half_open_interval`] has passed since then.
    ///
    /// Armed at the **trip**, deliberately: the peer has just failed, so trying
    /// it again in the same breath tells us nothing and costs a caller. It also
    /// makes an interval longer than the test mean "no trials", which is how a
    /// test asks for the plain open-circuit behaviour.
    window_from: Option<Instant>,
    /// The trial currently outstanding, if any. One at a time: this, not
    /// [`PeerState::window_from`], is what stops an outage costing more than one
    /// stalled trial caller per peer — and it holds only because a trial is
    /// closed **only by its own outcome**, matched by [`TrialId`].
    trial: Option<TrialId>,
}

/// A real, locally observed [`PeerHealth`]: after
/// [`DEFAULT_FAILURE_THRESHOLD`] consecutive failed calls to a peer, it reports
/// unhealthy so [`RpcClient::call`] fast-fails instead of burning the full
/// connect+request timeout on a peer already known to be down. A success clears
/// the streak immediately.
///
/// **Half-open, not open-then-closed (D-78).** While a peer is marked unhealthy
/// this admits **one trial call per [`DEFAULT_HALF_OPEN_INTERVAL`]**, one at a
/// time: a success clears the mark, a liveness failure re-arms the cooldown and
/// shuts the window until the next interval.
///
/// Without it the only early clear was [`RpcClient::probe`], whose sole
/// production caller is the Raft liveness ticker — and that is gated on
/// `!leading()`, so on a **follower** nothing ever tested whether a tripped peer
/// was back and the mark stood for the full cooldown. Every owner-routed store
/// (the sequencer, the flow store and shard, proxyOnce claims) calls through
/// this gate, and under D-10 CAS and proxyOnce *reject* rather than degrade — so
/// one peer restarting made each follower refuse every op homed on it for five
/// seconds, on an otherwise healthy fleet (issue #597).
///
/// The gate exists to stop *many* callers burning full deadlines against a peer
/// known to be down. It was never meant to stop *anyone* discovering the peer is
/// back: that is the difference between an open circuit and a half-open one, and
/// it is the whole of this decision.
///
/// A trial that reports back neither way — its future dropped by an outer
/// deadline — leaves its trial set, and the peer then behaves exactly as it
/// did before this change: the cooldown expires on its own and clears the entry.
/// The worst case is the old behaviour, never worse, which is why there is no
/// separate abandon timer to get wrong.
pub struct TrackedPeerHealth {
    state: Mutex<HashMap<SocketAddr, PeerState>>,
    threshold: u32,
    cooldown: Duration,
    half_open_interval: Duration,
    /// Source of [`TrialId`]s. Tracker-wide — see [`TrialId`] for why not per peer.
    next_trial: std::sync::atomic::AtomicU64,
}

impl Default for TrackedPeerHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackedPeerHealth {
    /// A tracker using the default threshold (3), cooldown (5s) and half-open
    /// interval (250ms).
    #[must_use]
    pub fn new() -> Self {
        Self::with_params(DEFAULT_FAILURE_THRESHOLD, DEFAULT_COOLDOWN)
    }

    /// A tracker with an explicit threshold and cooldown, for callers (and
    /// tests) that need to tune the sensitivity. The half-open interval keeps
    /// its default; see [`Self::with_half_open_interval`].
    #[must_use]
    pub fn with_params(threshold: u32, cooldown: Duration) -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
            threshold,
            cooldown,
            half_open_interval: DEFAULT_HALF_OPEN_INTERVAL,
            next_trial: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Override how often a tripped peer admits a trial call (D-78).
    ///
    /// Separate from [`Self::with_params`] so its existing callers — and the
    /// tests pinning threshold and cooldown behaviour — keep meaning what they
    /// meant. A test that wants the pre-D-78 open circuit sets this longer than
    /// the test itself runs.
    #[must_use]
    pub fn with_half_open_interval(mut self, interval: Duration) -> Self {
        self.half_open_interval = interval;
        self
    }
}

impl PeerHealth for TrackedPeerHealth {
    fn admit(&self, peer: SocketAddr) -> Admission {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = state.get_mut(&peer) else {
            return Admission::Healthy;
        };
        match entry.unhealthy_until {
            None => Admission::Healthy,
            Some(until) if now >= until => {
                // Cooldown elapsed on its own: treat it as recovered, same as an
                // explicit success, so a later failure needs a fresh run at the
                // threshold rather than tripping on the very next attempt. This
                // arm is also the backstop for a trial that never reported back.
                state.remove(&peer);
                Admission::Healthy
            }
            // Marked unhealthy, and the cooldown has not run out. This is where
            // the circuit is half-open rather than open (D-78): admit one trial,
            // so somebody can find out the peer is back.
            Some(_) => {
                if entry.trial.is_some() {
                    return Admission::Refused;
                }
                let due = entry
                    .window_from
                    .is_none_or(|from| now.duration_since(from) >= self.half_open_interval);
                if !due {
                    return Admission::Refused;
                }
                let id = TrialId(
                    self.next_trial
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                entry.window_from = Some(now);
                entry.trial = Some(id);
                tracing::debug!(%peer, trial = id.0, "peer health: admitting a half-open trial call");
                Admission::Trial(id)
            }
        }
    }

    fn record_success(&self, peer: SocketAddr, _admission: Admission) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        // Removing the whole entry clears the streak, the mark and any
        // outstanding trial in one step: the peer answered, so nothing this
        // tracker remembered about it is still true.
        state.remove(&peer);
    }

    fn record_failure(&self, peer: SocketAddr, admission: Admission) {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = state.entry(peer).or_default();
        match admission {
            // The trial's own failure: the peer is still down. Close the window
            // and re-arm, restarting the pacing from *now* rather than from when
            // the trial was admitted — a trial that took two seconds to fail must
            // not make the next one due the instant it returns.
            //
            // The streak is deliberately *not* incremented. It has already done
            // its job — it is what tripped the mark — and folding trials into it
            // would grow it without bound for as long as the peer is down.
            //
            // Matched by id, not assumed: if the entry was cleared while this trial
            // ran (a success elsewhere, or the cooldown expiring) — and perhaps
            // re-tripped with a newer trial out — this failure is a fresh
            // observation and counts like any other, and the newer trial stays
            // outstanding.
            Admission::Trial(id) if entry.trial == Some(id) => {
                entry.trial = None;
                entry.window_from = Some(now);
                entry.unhealthy_until = Some(now + self.cooldown);
            }
            // An ordinary failure. It must never close an outstanding trial: a call
            // admitted before the peer tripped can fail after a trial was admitted,
            // and nothing about *its* failure says the trial is over.
            Admission::Healthy | Admission::Trial(_) => {
                entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                if entry.consecutive_failures >= self.threshold {
                    entry.unhealthy_until = Some(now + self.cooldown);
                    entry.window_from = Some(now);
                }
            }
            // A refused call was never made, so it observed nothing.
            Admission::Refused => {}
        }
    }

    fn release(&self, peer: SocketAddr, admission: Admission) {
        let Admission::Trial(id) = admission else {
            return;
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        // No re-arm and no clear: the trial learned nothing about the peer. The
        // window reopens at `window_from`'s pacing, so the next trial follows one
        // interval after this one was admitted. Only this trial is released — a
        // newer one, from a later episode, is left alone.
        if let Some(entry) = state.get_mut(&peer)
            && entry.trial == Some(id)
        {
            entry.trial = None;
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
/// them in turn (#79, decision D-28). Returning only the first made a dual-stack
/// name whose leading address nobody listens on permanently unreachable, even
/// with a live address sitting second in the same answer.
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
        // mirrored, not fixed. A prefer-IPv4 knob was rejected for exactly this
        // reason: decision D-28.
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

/// Signed, pooled client for the cluster port.
#[derive(Clone)]
pub struct RpcClient {
    http: Client<HttpConnector, Full<Bytes>>,
    signer: Option<Signer>,
    health: Arc<dyn PeerHealth>,
    config: RpcClientConfig,
}

impl RpcClient {
    /// The per-attempt deadline this client applies by default.
    ///
    /// Exposed so a caller that needs a *size-aware* deadline can build one on
    /// top of it rather than re-deriving the base from a constant — Raft
    /// replication does exactly that for large log entries (#411).
    #[must_use]
    pub(crate) fn request_timeout(&self) -> Duration {
        self.config.request_timeout
    }

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
        let admission = self.health.admit(peer);
        if admission == Admission::Refused {
            // Fast-fail: resolve now rather than parking the caller for the
            // full deadline against a peer the local view already knows is gone.
            return Err(RpcError::Transport(format!("peer {peer} is not healthy")));
        }

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
            match result {
                Ok(response) => {
                    self.health.record_success(peer, admission);
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
                        self.health.record_failure(peer, admission);
                    } else {
                        // Reachability is what this tracker holds, and a refusal
                        // settles it in the affirmative — so the answer is
                        // recorded, not merely declined as a failure. Under D-78
                        // that also matters mechanically: this call may be the
                        // half-open trial, and a trial that reports back neither
                        // way holds the window shut until the cooldown.
                        //
                        // Every variant that reaches this arm comes from
                        // `status_to_error`, i.e. after an HTTP response arrived;
                        // `attempt`'s own local errors are all `Transport` or
                        // `Timeout`, which take the arm above.
                        self.health.record_success(peer, admission);
                    }
                    return Err(e);
                }
            }
        }
    }

    /// A liveness probe: one attempt, bounded by `deadline`, that **ignores** the
    /// peer's health mark and clears it on success.
    ///
    /// Decision D-22: every liveness mechanism — the per-peer ticker, a
    /// keepalive during a transfer — goes through this and never through
    /// [`Self::call`] or [`Self::call_once`], both of which sit behind the gate.
    ///
    /// [`TrackedPeerHealth`] fast-fails calls to a peer that recently failed,
    /// which is right for ordinary traffic and self-defeating for the one call
    /// whose purpose is to discover that the peer is back. Measured (#431): the
    /// leader's first ~20 heartbeats to a restarted voter all failed on the
    /// leader with `peer … is not healthy`, and by the time the cooldown let
    /// one through the voter had campaigned — its term had moved and it
    /// rejected everything the leader sent from then on. A probe therefore
    /// skips the gate; a probe failure is not charged either, because the
    /// tracker already knows.
    pub async fn probe(
        &self,
        peer: SocketAddr,
        method: &str,
        path: &str,
        body: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcError> {
        match tokio::time::timeout(deadline, self.attempt(peer, method, path, body, deadline)).await
        {
            // A probe is not admitted — it bypasses the gate (D-22) — so it
            // carries no trial. Its success still clears the entry outright,
            // any outstanding trial included: the peer answered.
            Ok(Ok(response)) => {
                self.health.record_success(peer, Admission::Healthy);
                Ok(response)
            }
            Ok(Err(e)) => Err(e),
            Err(_elapsed) => Err(RpcError::Timeout),
        }
    }

    /// Call `method path` on `peer` exactly once, bounded by `deadline`.
    ///
    /// [`Self::call`] is wrong for a Raft replication transfer twice over:
    /// openraft is already the retry loop, so a second attempt here re-sends the
    /// whole body (several MiB, for a config document at the front's cap), and the fixed
    /// `request_timeout` is far too short for an entry that size once the
    /// transfer is allowed to outlive openraft's 50 ms RPC deadline (#411).
    ///
    /// Health accounting matches `call`'s — a liveness failure counts against the
    /// peer, and any answer, a refusal included, counts for it — with one
    /// deliberate difference: a deadline expiry is **released**, neither charged
    /// nor credited, because the deadline was this caller's choice (#442). See
    /// [`Self::settle_single_attempt`].
    pub async fn call_once(
        &self,
        peer: SocketAddr,
        method: &str,
        path: &str,
        body: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<u8>, RpcError> {
        let admission = self.health.admit(peer);
        if admission == Admission::Refused {
            return Err(RpcError::Transport(format!("peer {peer} is not healthy")));
        }

        // `deadline` twice, deliberately: `attempt` applies it to the request
        // itself, and the outer bound also covers collecting the response body,
        // which `attempt` does not time out. Passing it inward is the load-
        // bearing half — an outer-only bound leaves `request_timeout` binding.
        match tokio::time::timeout(deadline, self.attempt(peer, method, path, body, deadline)).await
        {
            Ok(Ok(response)) => {
                self.health.record_success(peer, admission);
                Ok(response)
            }
            Ok(Err(e)) => {
                self.settle_single_attempt(peer, &e, admission);
                Err(e)
            }
            Err(_elapsed) => {
                self.settle_single_attempt(peer, &RpcError::Timeout, admission);
                Err(RpcError::Timeout)
            }
        }
    }

    /// Settle a failed single-attempt call against `peer`'s health: charge a
    /// liveness failure, credit an answer, and **release** a deadline expiry.
    ///
    /// A caller-supplied deadline running out says the payload did not cross the link in the
    /// budget *this caller* chose. That is a statement about the transfer, not about whether the
    /// peer is reachable, which is what [`PeerHealth`] tracks. Charging it makes the tracker
    /// defeat itself: three slow transfers trip the threshold, [`Self::call`]'s
    /// `admit` gate then fast-fails **every** RPC to that peer for the cooldown — heartbeats
    /// included — and the node stops talking to a peer that was only ever on a slow link.
    ///
    /// Measured, not hypothetical: while diagnosing #431 the leader's first ~20 heartbeats to a
    /// restarted node failed with "peer … is not healthy", its own liveness tracker having
    /// suppressed the liveness check. D-22 records the rule: a caller's own deadline expiring is
    /// not evidence the peer is down (#442).
    ///
    /// Liveness is still observed, and far more often, by the small RPCs going through
    /// [`Self::call`] — this only declines to add a signal that can be a false positive.
    /// Connect and transport failures are charged as before: those *are* about the peer.
    ///
    /// **Not for [`Self::call`], and do not unify the two.** The exemption is specific to a
    /// single attempt on a deadline the *caller* chose for a payload it chose. On the ordinary
    /// retried path the timeout is the configured `request_timeout`, nobody picked it per-call,
    /// and its expiry after the full retry budget is exactly the signal that marks a dead peer —
    /// routing `call` through here would silently disable the health tracker.
    ///
    /// **Release, not silence (D-78).** A deadline expiry used to record nothing,
    /// which was harmless while the gate was open-or-closed. Once a call can be
    /// the half-open trial, recording nothing leaves that trial outstanding until
    /// the cooldown, so the expiry releases it: the window reopens, and nothing is
    /// learned about the peer either way — which is exactly what #442 says a
    /// caller's own deadline is worth.
    fn settle_single_attempt(&self, peer: SocketAddr, err: &RpcError, admission: Admission) {
        match err {
            RpcError::Timeout => self.health.release(peer, admission),
            e if e.is_liveness_failure() => self.health.record_failure(peer, admission),
            // An answer arrived — see the matching arm in `call`.
            _ => self.health.record_success(peer, admission),
        }
    }

    /// One request/response exchange, bounded by `deadline`.
    ///
    /// The deadline is a parameter rather than always `config.request_timeout`
    /// because a Raft replication transfer needs a size-aware one (#411).
    /// Wrapping this call from outside would not work: the inner bound is what
    /// actually cancels the HTTP request, so a nested-but-larger outer deadline
    /// leaves the shorter one binding — which is the second ceiling behind the
    /// heartbeat one, and it silently caps transfers at whatever fits in
    /// `request_timeout`.
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

        let response = tokio::time::timeout(deadline, self.http.request(request))
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
        // Preserved as its own class rather than folded into `Handler`: the
        // caller's next move differs entirely. A `BadRequest` will fail
        // identically on every retry and names something the operator can fix;
        // a `Handler` error is the peer failing at something it should have
        // managed, and is worth escalating rather than rewriting the request.
        400 => RpcError::BadRequest(detail),
        // Two different 404s share this status, told apart by the envelope's
        // reason label exactly like the 503 pair below: "no such route"
        // (`UnknownRoute`) and "this route exists, but not this resource"
        // (`NotFound`, #437) are different questions a caller must be able to
        // tell apart — #439's fetch-on-apply needs "this peer lacks the
        // blob, ask another" to read differently from "this build has no
        // blob route at all".
        404 => match field("error").as_deref() {
            Some("not_found") => RpcError::NotFound { what: detail },
            _ => RpcError::UnknownRoute {
                method: method.to_owned(),
                path: path.to_owned(),
            },
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
        fn admit(&self, _peer: SocketAddr) -> Admission {
            Admission::Refused
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
        // About the threshold, not the half-open window: an interval longer than
        // the test asks for the plain open circuit (D-78).
        let health = TrackedPeerHealth::with_params(3, Duration::from_secs(5))
            .with_half_open_interval(Duration::from_secs(600));
        let peer: SocketAddr = "127.0.0.1:4001".parse().expect("valid addr");
        assert_eq!(
            health.admit(peer),
            Admission::Healthy,
            "unknown peer starts healthy"
        );
        health.record_failure(peer, Admission::Healthy);
        health.record_failure(peer, Admission::Healthy);
        assert_eq!(
            health.admit(peer),
            Admission::Healthy,
            "under the threshold, the peer must stay healthy"
        );
        health.record_failure(peer, Admission::Healthy);
        assert_eq!(
            health.admit(peer),
            Admission::Refused,
            "the Nth consecutive failure must trip it unhealthy"
        );
    }

    #[test]
    fn tracked_health_short_circuits_during_cooldown_and_recovers_after() {
        // As above: no trials, so this stays a test of the cooldown alone.
        let health = TrackedPeerHealth::with_params(3, Duration::from_millis(50))
            .with_half_open_interval(Duration::from_secs(600));
        let peer: SocketAddr = "127.0.0.1:4002".parse().expect("valid addr");
        for _ in 0..3 {
            health.record_failure(peer, Admission::Healthy);
        }
        assert_eq!(
            health.admit(peer),
            Admission::Refused,
            "must short-circuit immediately after tripping"
        );
        // Polled, not slept: a fixed 80 ms against a 50 ms cooldown left a
        // 30 ms margin, which a loaded CI box eats. Waiting longer than needed
        // costs nothing here, but failing on scheduler jitter costs a rerun and
        // teaches people to ignore the suite.
        let deadline = Instant::now() + Duration::from_secs(2);
        while health.admit(peer) != Admission::Healthy && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            health.admit(peer),
            Admission::Healthy,
            "must recover once the cooldown elapses, even without a success"
        );
    }

    fn is_trial(admission: Admission) -> bool {
        matches!(admission, Admission::Trial(_))
    }

    /// Poll `admit` until it hands out a trial, bounded, returning the trial's
    /// admission so the caller can resolve that very trial.
    fn wait_for_trial(health: &TrackedPeerHealth, peer: SocketAddr) -> Option<Admission> {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let admission = health.admit(peer);
            if is_trial(admission) {
                return Some(admission);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        None
    }

    /// Pins D-78, and the defect it closes (#597).
    ///
    /// Before it, the *only* early clear was `RpcClient::probe`, whose one
    /// production caller is the Raft liveness ticker — gated on `!leading()`. So
    /// on a follower nothing ever tested a tripped peer, the mark stood for the
    /// whole cooldown, and every owner-routed op to a restarted peer failed or
    /// degraded for five seconds on a healthy fleet.
    ///
    /// The cooldown is 60s so nothing here can pass by waiting it out.
    #[test]
    fn a_tripped_peer_admits_a_trial_and_a_successful_one_clears_the_mark() {
        let health = TrackedPeerHealth::with_params(1, Duration::from_secs(60))
            .with_half_open_interval(Duration::ZERO);
        let peer: SocketAddr = "127.0.0.1:4790".parse().expect("valid address");

        health.record_failure(peer, Admission::Healthy);
        let trial = health.admit(peer);
        assert!(
            is_trial(trial),
            "with a zero interval the next caller is the trial, and must be let through"
        );

        // The peer is back: the trial succeeds.
        health.record_success(peer, trial);
        assert_eq!(
            health.admit(peer),
            Admission::Healthy,
            "a successful trial clears the mark outright — `Healthy`, not merely \
             another `Trial`, which is all a surviving mark would hand out here"
        );
    }

    /// The cost bound: one trial at a time, so an outage still costs one stalled
    /// trial caller per peer rather than a stampede. This is the half of D-78
    /// that keeps the gate worth having.
    #[test]
    fn only_one_trial_is_outstanding_at_a_time() {
        let health = TrackedPeerHealth::with_params(1, Duration::from_secs(60))
            .with_half_open_interval(Duration::ZERO);
        let peer: SocketAddr = "127.0.0.1:4791".parse().expect("valid address");
        health.record_failure(peer, Admission::Healthy);

        assert!(
            is_trial(health.admit(peer)),
            "the first caller is the trial"
        );
        for i in 0..5 {
            assert_eq!(
                health.admit(peer),
                Admission::Refused,
                "caller {i} must be refused while the trial is outstanding, \
                 even with a zero pacing interval"
            );
        }
    }

    /// The bound above holds only if a failure closes the trial **only when it is
    /// the trial's own**. A call admitted while the peer still looked healthy can
    /// fail after the peer tripped and after a trial went out; if that failure
    /// closed the trial's window, a second trial would be admitted beside the
    /// first — and against a peer that never answers, trials would pile up at one
    /// per interval (#599's review). The admission token is what tells them apart.
    #[test]
    fn a_failure_admitted_before_the_trip_does_not_close_the_trial() {
        let health = TrackedPeerHealth::with_params(1, Duration::from_secs(60))
            .with_half_open_interval(Duration::ZERO);
        let peer: SocketAddr = "127.0.0.1:4794".parse().expect("valid address");

        let early = health.admit(peer);
        assert_eq!(
            early,
            Admission::Healthy,
            "admitted while the peer looked fine"
        );
        health.record_failure(peer, Admission::Healthy); // another call trips it
        assert!(is_trial(health.admit(peer)), "the trial goes out");

        health.record_failure(peer, early); // the early call fails *now*
        assert_eq!(
            health.admit(peer),
            Admission::Refused,
            "the trial is still outstanding; an earlier call's failure must not \
             open a second one"
        );
    }

    /// A trial can outlive its own episode: the entry is cleared (here by a
    /// success, as a probe getting through would), the peer trips again, and a
    /// newer trial goes out. When the first trial finally reports — as a failure
    /// or as a released deadline — it must resolve only itself, or two trials
    /// would be in flight at once (#599's second review).
    #[test]
    fn a_trial_from_an_earlier_episode_cannot_close_a_later_one() {
        let health = TrackedPeerHealth::with_params(1, Duration::from_secs(60))
            .with_half_open_interval(Duration::ZERO);
        let peer: SocketAddr = "127.0.0.1:4797".parse().expect("valid address");

        health.record_failure(peer, Admission::Healthy);
        let stale = health.admit(peer);
        assert!(is_trial(stale), "{stale:?}");

        // The episode ends without that trial reporting, and a new one begins.
        health.record_success(peer, Admission::Healthy);
        health.record_failure(peer, Admission::Healthy);
        let current = health.admit(peer);
        assert!(is_trial(current), "{current:?}");
        assert_ne!(stale, current, "two trials must be told apart");

        health.release(peer, stale);
        assert_eq!(
            health.admit(peer),
            Admission::Refused,
            "a stale trial's release must not free the current trial's window"
        );
        health.record_failure(peer, stale);
        assert_eq!(
            health.admit(peer),
            Admission::Refused,
            "a stale trial's failure must not close the current trial either"
        );

        // The current trial resolving itself is what reopens the window.
        health.record_failure(peer, current);
        assert!(
            is_trial(health.admit(peer)),
            "the next trial may now go out"
        );
    }

    /// A failed trial re-arms the cooldown rather than clearing the mark.
    ///
    /// Timed relative to the trial's failure, not to the trip, so the margins
    /// survive a loaded box: the check runs after the *original* cooldown has
    /// expired (so an un-re-armed entry would be gone, answering `Healthy`) and
    /// well inside the re-armed one (so a re-armed entry answers `Trial`).
    #[test]
    fn a_failed_trial_re_arms_the_cooldown() {
        let health = TrackedPeerHealth::with_params(1, Duration::from_secs(1))
            .with_half_open_interval(Duration::ZERO);
        let peer: SocketAddr = "127.0.0.1:4792".parse().expect("valid address");
        health.record_failure(peer, Admission::Healthy); // trip at t0; mark until t0 + 1s

        let trial = health.admit(peer);
        assert!(is_trial(trial), "{trial:?}");
        std::thread::sleep(Duration::from_millis(600));
        health.record_failure(peer, trial); // at f >= t0 + 600ms; re-armed until f + 1s

        // Now >= f + 500ms >= t0 + 1.1s: the original mark has expired.
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            is_trial(health.admit(peer)),
            "the failed trial must have re-armed the mark; `Healthy` here means the \
             original cooldown lapsed with nothing extending it"
        );
    }

    /// The window opens one interval after the trip, and again one interval after
    /// a failed trial — otherwise a peer that refuses connections instantly would
    /// admit a tight loop of trials. The immediate checks have the whole 300ms
    /// interval as margin.
    #[test]
    fn the_next_trial_waits_out_the_interval() {
        let health = TrackedPeerHealth::with_params(1, Duration::from_secs(60))
            .with_half_open_interval(Duration::from_millis(300));
        let peer: SocketAddr = "127.0.0.1:4795".parse().expect("valid address");
        health.record_failure(peer, Admission::Healthy);

        assert_eq!(
            health.admit(peer),
            Admission::Refused,
            "a peer that just failed must not be trialled in the same breath"
        );
        let first = wait_for_trial(&health, peer)
            .expect("a trial must be admitted once the interval elapses");

        health.record_failure(peer, first);
        assert_eq!(
            health.admit(peer),
            Admission::Refused,
            "a failed trial must not be followed by another in the same breath"
        );
        assert!(
            wait_for_trial(&health, peer).is_some(),
            "a second trial must follow, one interval after the first one failed"
        );
    }

    /// A trial that reports back neither way — its future dropped by an outer
    /// deadline — must degrade to the pre-D-78 behaviour, with the cooldown
    /// clearing it, and must never wedge the peer shut for longer than that.
    #[test]
    fn an_unresolved_trial_still_clears_on_the_cooldown() {
        let health = TrackedPeerHealth::with_params(1, Duration::from_millis(400))
            .with_half_open_interval(Duration::ZERO);
        let peer: SocketAddr = "127.0.0.1:4793".parse().expect("valid address");
        health.record_failure(peer, Admission::Healthy);

        assert!(is_trial(health.admit(peer)), "the trial is admitted");
        // ...and it never reports back: no `record_*`, no `release`.
        assert_eq!(
            health.admit(peer),
            Admission::Refused,
            "the window stays shut while a trial is outstanding"
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while health.admit(peer) != Admission::Healthy && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            Instant::now() < deadline,
            "an abandoned trial must not outlive the cooldown — the worst case is \
             the pre-D-78 behaviour, never worse"
        );
    }

    /// `release` reopens the window for a trial that learned nothing, without
    /// clearing or re-arming the mark — and is a no-op for anything but a trial.
    #[test]
    fn a_released_trial_reopens_the_window_and_nothing_else() {
        let health = TrackedPeerHealth::with_params(1, Duration::from_secs(60))
            .with_half_open_interval(Duration::ZERO);
        let peer: SocketAddr = "127.0.0.1:4796".parse().expect("valid address");
        health.record_failure(peer, Admission::Healthy);

        let first = health.admit(peer);
        assert!(is_trial(first), "{first:?}");
        health.release(peer, first);
        assert!(
            is_trial(health.admit(peer)),
            "a released trial frees the window, and the mark is still there — \
             `Healthy` would mean release cleared it"
        );

        // That second trial is outstanding now; releasing an ordinary admission
        // must not close it.
        health.release(peer, Admission::Healthy);
        assert_eq!(health.admit(peer), Admission::Refused);
    }

    #[test]
    fn tracked_health_success_clears_the_failure_streak() {
        let health = TrackedPeerHealth::with_params(3, Duration::from_secs(5));
        let peer: SocketAddr = "127.0.0.1:4003".parse().expect("valid addr");
        health.record_failure(peer, Admission::Healthy);
        health.record_failure(peer, Admission::Healthy);
        health.record_success(peer, Admission::Healthy);
        health.record_failure(peer, Admission::Healthy);
        health.record_failure(peer, Admission::Healthy);
        assert_eq!(
            health.admit(peer),
            Admission::Healthy,
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

    /// A responder that answers `200 {}` after `delay`.
    async fn spawn_slow_responder(delay: Duration) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind slow responder");
        let addr = listener.local_addr().expect("responder addr");
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 8192];
                    let _ = socket.read(&mut buf).await;
                    tokio::time::sleep(delay).await;
                    let body = b"{}";
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                    let _ = socket.flush().await;
                });
            }
        });
        (addr, handle)
    }

    /// #431: a liveness probe must reach a peer the tracker has written off, and
    /// its success must clear the mark — otherwise the tracker suppresses the one
    /// call that could tell it the peer is back.
    ///
    /// Pins D-22: `probe` bypasses `is_healthy` and clears the mark on success,
    /// while an ordinary `call` to the same tripped peer still fast-fails.
    #[tokio::test]
    async fn probe_reaches_a_peer_the_tracker_marks_unhealthy_and_clears_the_mark() {
        let (addr, _guard) = spawn_slow_responder(Duration::from_millis(0)).await;
        // No half-open trials: this test is about `probe` bypassing a *closed*
        // gate, so the gate must stay closed for the gated call below (D-78).
        let health = Arc::new(
            TrackedPeerHealth::with_params(1, Duration::from_secs(60))
                .with_half_open_interval(Duration::from_secs(600)),
        );
        health.record_failure(addr, Admission::Healthy);
        let client = RpcClient::new(
            None,
            health.clone(),
            RpcClientConfig {
                connect_timeout: Duration::from_millis(200),
                request_timeout: Duration::from_millis(500),
                max_retries: 0,
            },
        );

        let gated = client
            .call(addr, "POST", "/internal/v1/raft/append", b"{}".to_vec())
            .await;
        assert!(
            matches!(gated, Err(RpcError::Transport(ref m)) if m.contains("not healthy")),
            "an ordinary call to a tripped peer must still fast-fail, got {gated:?}"
        );

        let probed = client
            .probe(
                addr,
                "POST",
                "/internal/v1/raft/append",
                b"{}".to_vec(),
                Duration::from_millis(500),
            )
            .await;
        assert!(
            probed.is_ok(),
            "the probe must bypass the health gate: {probed:?}"
        );
        assert_eq!(
            health.admit(addr),
            Admission::Healthy,
            "a successful probe must clear the mark"
        );

        let after = client
            .call(addr, "POST", "/internal/v1/raft/append", b"{}".to_vec())
            .await;
        assert!(
            after.is_ok(),
            "ordinary calls must work again once the probe cleared it"
        );
    }

    /// #411's *second* ceiling: the transfer deadline must **replace** the
    /// per-attempt `request_timeout`, not nest outside it.
    ///
    /// This is the case the sibling test below cannot see, because there the
    /// deadline is the shorter of the two and binds either way. Wrapping
    /// `attempt` in a larger timeout leaves the smaller inner bound governing,
    /// so transfers silently cap at whatever fits in `request_timeout` — which
    /// is exactly what shipped in the first cut of this fix: on a 3-node
    /// cluster a 4 MiB entry committed in 1.1 s while 8 MiB never committed
    /// at all, because a 2 s inner bound cut every attempt that ran longer.
    #[tokio::test]
    async fn call_once_deadline_outlives_a_shorter_request_timeout() {
        // Answers later than `request_timeout` but well inside the deadline.
        let (addr, _guard) = spawn_slow_responder(Duration::from_millis(600)).await;

        let client = RpcClient::new(
            None,
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                connect_timeout: Duration::from_millis(200),
                // Deliberately shorter than the answer takes: if this still
                // binds, a large replication transfer can never complete.
                request_timeout: Duration::from_millis(200),
                max_retries: 0,
            },
        );

        let result = client
            .call_once(
                addr,
                "POST",
                "/internal/v1/raft/append",
                b"body".to_vec(),
                Duration::from_secs(5),
            )
            .await;

        assert!(
            result.is_ok(),
            "the caller's 5 s deadline must govern, not the 200 ms request_timeout: {result:?}"
        );
    }

    /// `call` must keep its configured per-attempt timeout — threading a
    /// deadline through `attempt` for #411 must not change the ordinary path.
    #[tokio::test]
    async fn call_still_honours_the_configured_request_timeout() {
        let (addr, _guard) = spawn_slow_responder(Duration::from_millis(800)).await;

        let client = RpcClient::new(
            None,
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                connect_timeout: Duration::from_millis(200),
                request_timeout: Duration::from_millis(150),
                max_retries: 0,
            },
        );

        let result = client
            .call(addr, "POST", "/internal/v1/ping", b"body".to_vec())
            .await;

        assert!(
            matches!(result, Err(RpcError::Timeout)),
            "an ordinary call must still be cut at its configured 150 ms, got {result:?}"
        );
    }

    /// A listener that accepts and then answers `status` after `delay`.
    ///
    /// Hand-rolled rather than an `RpcServer` because these tests turn on *when* the peer answers
    /// and on it answering at all — the point is what the client records, not what it parses.
    async fn responder(delay: Duration, status: u16) -> SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("bound address");
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut scratch = vec![0u8; 16 * 1024];
                    let _ = socket.read(&mut scratch).await;
                    tokio::time::sleep(delay).await;
                    let response = format!(
                        "HTTP/1.1 {status} STATUS\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{{}}"
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        addr
    }

    /// A deadline expiring must NOT mark the peer unhealthy.
    ///
    /// The regression this guards is self-defeating rather than merely wasteful: the health gate
    /// covers `call` too, so a peer cooled down by slow transfers stops receiving heartbeats —
    /// which is how a node on a slow link becomes a node nobody can reach (#431).
    ///
    /// Pins D-22: a caller's own deadline expiring is not evidence the peer is down (#442) — the
    /// tracker is never charged for it.
    #[tokio::test]
    async fn call_once_deadline_expiry_is_not_charged_to_peer_health() {
        // Threshold 1: if the timeout were charged at all, the peer trips immediately.
        let health = Arc::new(TrackedPeerHealth::with_params(1, Duration::from_secs(30)));
        let addr = responder(Duration::from_secs(30), 200).await;
        let client = RpcClient::new(
            None,
            Arc::clone(&health) as Arc<dyn PeerHealth>,
            RpcClientConfig::default(),
        );

        let err = client
            .call_once(
                addr,
                "POST",
                "/internal/v1/echo",
                vec![],
                Duration::from_millis(150),
            )
            .await
            .expect_err("the deadline expires");

        assert_eq!(err, RpcError::Timeout);
        assert_eq!(
            health.admit(addr),
            Admission::Healthy,
            "a transfer that outran its own deadline says nothing about reachability"
        );
    }

    /// The other half of the same rule: a peer that *answers* — even to refuse — is alive.
    #[tokio::test]
    async fn call_once_does_not_charge_a_handler_refusal_to_peer_health() {
        let health = Arc::new(TrackedPeerHealth::with_params(1, Duration::from_secs(30)));
        let addr = responder(Duration::ZERO, 500).await;
        let client = RpcClient::new(
            None,
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
        assert_eq!(
            health.admit(addr),
            Admission::Healthy,
            "a peer that replied is reachable, whatever it replied"
        );
    }

    /// A `call_once` that is the half-open trial and runs out its caller-chosen
    /// deadline **releases** the trial (#599's review). It used to record nothing,
    /// which under D-78 left the trial outstanding until the cooldown — and
    /// `call_once` is the path Raft replication and snapshot transfers take.
    #[tokio::test]
    async fn a_call_once_trial_that_times_out_releases_the_window() {
        let (addr, _server) = spawn_slow_responder(Duration::from_secs(5)).await;
        let health = Arc::new(
            TrackedPeerHealth::with_params(1, Duration::from_secs(60))
                .with_half_open_interval(Duration::ZERO),
        );
        health.record_failure(addr, Admission::Healthy); // mark it down
        let client = RpcClient::new(
            None,
            Arc::clone(&health) as Arc<dyn PeerHealth>,
            RpcClientConfig::default(),
        );

        let err = client
            .call_once(
                addr,
                "POST",
                "/internal/v1/echo",
                vec![],
                Duration::from_millis(100),
            )
            .await
            .expect_err("the deadline expires");
        assert_eq!(err, RpcError::Timeout);

        assert!(
            is_trial(health.admit(addr)),
            "the timed-out trial must have released the window: `Refused` means it \
             is still outstanding, `Healthy` would mean the expiry was credited"
        );
    }

    /// A long deadline must not become a long hang against a peer already known to be gone.
    #[tokio::test]
    async fn call_once_fast_fails_an_unhealthy_peer() {
        let client = RpcClient::new(None, Arc::new(NeverHealthy), RpcClientConfig::default());
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

    /// #411: a replication transfer needs exactly ONE attempt, bounded by a
    /// deadline the caller picks from the body size.
    ///
    /// `call` is wrong for it twice over: openraft is already the retry loop, so
    /// a second attempt re-sends the whole 8 MiB body, and its fixed 2 s
    /// `request_timeout` is far too short for a large entry once the transfer is
    /// allowed to outlive openraft's 50 ms RPC deadline.
    #[tokio::test]
    async fn call_once_honours_its_deadline_and_does_not_retry() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Accepts, then answers nothing — the deadline is the only way out.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind silent listener");
        let addr = listener.local_addr().expect("listener addr");
        let seen = Arc::new(AtomicUsize::new(0));
        let counter = seen.clone();
        let _guard = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                // Hold it open rather than dropping it: a closed connection
                // would surface as a transport error and never exercise the
                // deadline this test exists to pin.
                held.push(socket);
            }
        });

        let client = RpcClient::new(
            None,
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                connect_timeout: Duration::from_millis(200),
                // Deliberately huge and deliberately retrying: if either of
                // these bounds the call, the caller's deadline is being ignored.
                request_timeout: Duration::from_secs(30),
                max_retries: 3,
            },
        );

        let started = std::time::Instant::now();
        let result = client
            .call_once(
                addr,
                "POST",
                "/internal/v1/raft/append",
                b"body".to_vec(),
                Duration::from_millis(150),
            )
            .await;
        let elapsed = started.elapsed();

        assert!(
            matches!(result, Err(RpcError::Timeout)),
            "the caller's deadline must surface as Timeout, got {result:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "the 150 ms deadline must bound the call, not the 30 s request_timeout (took {elapsed:?})"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "call_once must make exactly ONE attempt even with max_retries=3"
        );
    }
}
