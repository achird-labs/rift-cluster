//! The clustered `proxyOnce` recording store (#226, Ch.7 §proxyOnce, D-40): exactly-once
//! recording fleet-wide, through upstream's `ProxyRecordingStore` seam (U-16, rift#911).
//! Cluster-native, not Redis-first: D-12's Redis-backed-first ordering was amended once this
//! shipped on consensus (see D-12's amendment); no Redis proxyOnce backend exists.
//!
//! Shape, mirroring the flow store's discipline:
//!
//! - **Ownership.** Each `(port, signature)` has one owner on the HRW ring
//!   (`KeyClass::Proxy`, D-20). Claims are owner-local memory — *Pending dies with the owner*, so
//!   the duplicate-upstream bound is 1 + ownership changes in flight (Ch.6/Ch.7). A
//!   partitioned owner refuses claims (`is_isolated`, fail closed); membership fencing and
//!   `NotOwner` redirects copy `FlowNet::owner_write`.
//! - **Recorded is consensus.** `complete()`/`record()` route to the owner, which validates
//!   the claim token and submits one [`ControlOp::ProxyRecorded`] — marker row *and* stub
//!   mutation in a single log entry, so "recorded but stub-less" is unrepresentable. Only
//!   after commit-ack does the claim answer `AlreadyRecorded`. A new owner after a handoff
//!   answers from the applied table alone; no successor replication is needed because the
//!   Recorded fact is already on consensus.
//! - **Replay.** Steady-state replay never consults this store: the recorded stub is in
//!   replicated config and matches ahead of the proxy stub on every node. `lookup()` covers
//!   the stub-less recording (its replay source forever) and the commit→apply window (from
//!   the owner's completion cache).
//! - **Modes.** The store is manager-scoped, so it resolves each port's proxy mode from
//!   applied config. `proxyOnce` runs the claim machinery; `proxyAlways` grants formality
//!   tokens and publishes every recording as a merge op (the engine skips local insertion for
//!   every mode once `publishes_stubs()` is true — losing proxyAlways publication would be a
//!   silent regression, not a scope cut); `proxyTransparent` never records *responses*, as
//!   upstream, but still publishes generator-built stubs — upstream gates stub generation on
//!   `predicateGenerators`, not mode, and this store is the sole publisher.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rift_cluster_base::seams::{
    ClaimOutcome, ClaimToken, ProxyRecordingStore, ProxyStoreError, RecordedResponse,
    RequestSignature, StubPlacement, StubPublication,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;
use xxhash_rust::xxh64::xxh64;

use crate::bridge::{Bridge, BridgeConfig, CallerClass};
use crate::control::{
    ControlOp, ControlOutcome, ControlRequest, RecordedStub, RecordedStubPlacement, TenantId,
};
use crate::metrics;
use crate::raft::ring::{KeyClass, OwnedKey};
use crate::raft::{NodeId, RaftNode, Ring};
use crate::rpc::{HandlerFuture, Router, RpcError};

const CLAIM_PATH: &str = "/_cluster/proxy/claim";
const RELEASE_PATH: &str = "/_cluster/proxy/release";
const COMPLETE_PATH: &str = "/_cluster/proxy/complete";
const LOOKUP_PATH: &str = "/_cluster/proxy/lookup";

/// Bound on a claim/release/lookup end to end: owner-local map work plus one RPC hop —
/// the flow store's reasoning, and the same 2 s.
const CLAIM_OP_DEADLINE: Duration = Duration::from_secs(2);

/// Bound on a settle (`complete`/`record`/`clear`): the owner's Raft submit rides inside,
/// which may chase a moving leader, so this is generously above `CLAIM_OP_DEADLINE`. The
/// client is already holding a finished upstream response; only recording waits.
const SETTLE_OP_DEADLINE: Duration = Duration::from_secs(10);

/// The owner-side bound on the Raft submit within a settle — strictly below
/// [`SETTLE_OP_DEADLINE`] so the owner's release-on-failure runs before the caller's
/// bridge deadline can abort the op from outside. Per *attempt*, not per call: a redirect
/// retry that already burned wall-clock can still overrun the outer deadline, in which
/// case the abandoned claim self-heals at the claim TTL rather than immediately — rare
/// (it needs a membership move *and* a slow submit inside one op) and bounded, but real.
const SUBMIT_DEADLINE: Duration = Duration::from_secs(8);

/// How long the owner remembers a completed recording after commit-ack. Only needs to
/// outlive the commit→apply window on the slowest replica (milliseconds in practice); kept
/// generous because the cost is a few cached responses, and misses fall back to an extra
/// upstream call bounded by ownership changes.
const COMPLETE_CACHE_TTL: Duration = Duration::from_secs(120);

/// How long a resolved `(tenant, mode)` for a port is trusted before re-reading applied
/// config. Modes change only when an imposter is replaced — rare — while `try_claim` runs
/// per proxied request on the data plane; 2 s keeps the table read off the hot path without
/// letting a replaced imposter's mode linger meaningfully. A request in flight *across* a
/// mode flip can dispatch its claim and its settle under different modes; the mismatched
/// half degrades to a released-or-stale claim (self-healing at the claim TTL), never to a
/// wrong recording — and the replace itself purges the port's markers at apply.
const MODE_CACHE_TTL: Duration = Duration::from_secs(2);

/// Attempts per owner op across a `Fenced`/`NotOwner` redirect: the first dispatch plus one
/// rebuild against the fresh ring. A membership that moves twice inside one op is churn to
/// surface (`Unavailable`), not chase.
const OWNER_REDIRECT_ATTEMPTS: usize = 2;

/// Default claim deadline: how long a Pending claim may sit before the signature is
/// re-claimable. Generously above any upstream call the engine would wait for, so an honest
/// slow winner is not expired mid-flight; a crashed winner's claim frees itself after this.
/// Tests shrink it through [`ProxyBindConfig`].
///
/// D-40: fixed, not derived from an upstream timeout — the recording seam (U-16) carries none.
const DEFAULT_CLAIM_TTL: Duration = Duration::from_secs(60);

/// The production rendering of a claim's HRW key. Public so tests predicting an owner from
/// the ring can never disagree with the store about what is hashed.
#[must_use]
pub fn proxy_sig_key(port: u16, sig: &RequestSignature) -> String {
    format!("{port}:{}", sig_hash(sig))
}

/// Hex `xxh64` of the signature's canonical JSON. The signature is strings end to end, so
/// serialization is order-stable across nodes — the property the hash needs.
fn sig_hash(sig: &RequestSignature) -> String {
    let canonical =
        serde_json::to_vec(sig).expect("RequestSignature is strings only; serialization is total");
    format!("{:016x}", xxh64(&canonical, 0))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A port's proxy mode, as the store dispatches on it. Parsed from the applied config's
/// proxy response rather than imported from upstream: the wire strings are the contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PortMode {
    Once,
    Always,
    Transparent,
}

#[derive(Clone)]
struct PortIdentity {
    tenant: String,
    mode: PortMode,
    resolved_at: Instant,
}

struct PendingClaim {
    token: u64,
    deadline: Instant,
}

struct CachedRecording {
    resp_json: String,
    at: Instant,
    /// The commit revision (applying log index) of the `ProxyRecorded` op this entry
    /// mirrors. The cache is only trusted while the local applied state has not yet
    /// caught up to it — past that, the row is the truth (see `completed_lookup`).
    revision: u64,
}

/// Binding-time configuration, [`FlowBindConfig`](super::FlowBindConfig)'s sibling.
#[derive(Debug, Clone, Copy)]
pub struct ProxyBindConfig {
    pub bridge: BridgeConfig,
    /// How long a Pending claim may sit before its signature is re-claimable (and its
    /// token stale). See [`DEFAULT_CLAIM_TTL`].
    pub claim_ttl: Duration,
}

impl Default for ProxyBindConfig {
    fn default() -> Self {
        Self {
            bridge: BridgeConfig::default(),
            claim_ttl: DEFAULT_CLAIM_TTL,
        }
    }
}

/// The proxy-claim subsystem of one node: the owner-side claim table, the completion
/// cache, and the RPC client side — everything above the `RaftNode` it binds to.
pub struct ProxyNet {
    node: OnceLock<Weak<RaftNode>>,
    bridge: OnceLock<Bridge>,
    claim_ttl: OnceLock<Duration>,
    /// Owner-local Pending claims by `(port, sig-hash)`. Expiry is lazy — checked when the
    /// entry is consulted — so no sweeper thread exists to race the owner path.
    pending: parking_lot::Mutex<HashMap<(u16, String), PendingClaim>>,
    /// Recordings this owner committed, kept until the fleet has surely applied them.
    completed: parking_lot::Mutex<HashMap<(u16, String), CachedRecording>>,
    mode_cache: parking_lot::Mutex<HashMap<u16, PortIdentity>>,
    next_token: AtomicU64,
}

impl ProxyNet {
    #[must_use]
    pub fn new() -> Arc<Self> {
        // Token uniqueness must survive an owner restart: a claim granted before the
        // restart must not validate against a token minted after it. Seeding from the
        // clock (nanos) makes cross-restart collision practically impossible without any
        // persisted counter.
        let seed = SystemTime::now().duration_since(UNIX_EPOCH).map_or(1, |d| {
            u64::try_from(d.as_nanos() & u128::from(u64::MAX)).unwrap_or(1)
        });
        Arc::new(Self {
            node: OnceLock::new(),
            bridge: OnceLock::new(),
            claim_ttl: OnceLock::new(),
            pending: parking_lot::Mutex::new(HashMap::new()),
            completed: parking_lot::Mutex::new(HashMap::new()),
            mode_cache: parking_lot::Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(seed),
        })
    }

    /// Attach the node once it exists and start the bridge. Binding twice is a no-op; the
    /// second caller wanted what the first one got — `FlowNet::bind`'s contract.
    ///
    /// # Errors
    ///
    /// The bridge's private runtime could not start.
    pub fn bind(
        self: &Arc<Self>,
        node: &Arc<RaftNode>,
        config: ProxyBindConfig,
    ) -> std::io::Result<()> {
        if self.bridge.get().is_none() {
            let bridge = Bridge::start(config.bridge)?;
            let _ = self.bridge.set(bridge);
        }
        let _ = self.claim_ttl.set(config.claim_ttl);
        let _ = self.node.set(Arc::downgrade(node));
        Ok(())
    }

    /// Live Pending claims for `port` on this node — owner-side introspection for tests
    /// and metrics; expired entries are not counted.
    #[must_use]
    pub fn pending_claims(&self, port: u16) -> usize {
        let now = Instant::now();
        self.pending
            .lock()
            .iter()
            .filter(|((p, _), claim)| *p == port && claim.deadline > now)
            .count()
    }

    fn claim_ttl(&self) -> Duration {
        self.claim_ttl.get().copied().unwrap_or(DEFAULT_CLAIM_TTL)
    }

    /// The live node and its current ring, or the loud not-ready error.
    fn view(&self) -> Result<(Arc<RaftNode>, Ring), String> {
        let node = self
            .node
            .get()
            .ok_or("proxy store: cluster is still starting")?
            .upgrade()
            .ok_or("proxy store: cluster node has shut down")?;
        let ring = node.ring();
        if ring.is_empty() {
            return Err("proxy store: no applied membership yet".to_owned());
        }
        Ok((node, ring))
    }

    fn mint_token(&self) -> u64 {
        self.next_token.fetch_add(1, Ordering::SeqCst)
    }

    /// Resolve `(tenant, mode)` for a port from applied config, through a short TTL cache.
    fn port_identity(&self, port: u16) -> Result<PortIdentity, ProxyStoreError> {
        if let Some(hit) = self.mode_cache.lock().get(&port)
            && hit.resolved_at.elapsed() < MODE_CACHE_TTL
        {
            return Ok(hit.clone());
        }
        let (node, _ring) = self.view().map_err(ProxyStoreError::Unavailable)?;
        let tenant = node
            .configured_ports()
            .map_err(|e| ProxyStoreError::Unavailable(format!("proxy store: {e}")))?
            .into_iter()
            .find_map(|(tenant, p)| (p == port).then(|| tenant.as_str().to_owned()))
            .ok_or_else(|| {
                ProxyStoreError::Unavailable(format!(
                    "proxy store: no applied imposter on port {port}"
                ))
            })?;
        let config_json = node
            .get_imposter(&tenant, port)
            .map_err(|e| ProxyStoreError::Unavailable(format!("proxy store: {e}")))?
            .ok_or_else(|| {
                ProxyStoreError::Unavailable(format!(
                    "proxy store: no applied imposter on port {port}"
                ))
            })?;
        let mode = parse_proxy_mode(&config_json);
        let identity = PortIdentity {
            tenant,
            mode,
            resolved_at: Instant::now(),
        };
        self.mode_cache.lock().insert(port, identity.clone());
        Ok(identity)
    }

    // -- owner-side handlers (invoked locally or via the routes below) -------

    /// Fence and ownership discipline shared by every owner-side entry —
    /// `FlowNet::owner_write`'s preamble.
    ///
    /// Deliberately does **not** check isolation. The two owner entries need opposite
    /// treatments: `owner_claim` adds its own `is_isolated` refusal (a partitioned owner
    /// must not *grant* — it cannot commit the recording, and a healed majority may have
    /// re-homed the key), while `owner_complete` must be allowed to *attempt* the Raft
    /// submit — the submit can only succeed through a real quorum, and its failure is
    /// exactly what releases the claim and keeps the signature retryable (AC3). Refusing
    /// a complete up front would instead leave the claim Pending until the deadline.
    fn owner_gate(
        &self,
        ring: &Ring,
        node: &RaftNode,
        m_idx: u64,
        key: &str,
    ) -> Option<OwnerRefusal> {
        if m_idx != ring.m_idx() {
            return Some(OwnerRefusal::Fenced {
                owner_m_idx: ring.m_idx(),
            });
        }
        let owned = OwnedKey::new(KeyClass::Proxy, key);
        match ring.owner(owned) {
            Some(owner) if owner == node.id() => {}
            Some(owner) => return Some(OwnerRefusal::NotOwner { owner }),
            None => {
                return Some(OwnerRefusal::Error {
                    reason: "proxy store: ring has no members".to_owned(),
                });
            }
        }
        None
    }

    fn owner_claim(self: &Arc<Self>, req: &ClaimReq) -> ClaimReply {
        let (node, ring) = match self.view() {
            Ok(view) => view,
            Err(reason) => return ClaimReply::Error { reason },
        };
        let key = format!("{}:{}", req.port, req.sig_hash);
        if let Some(refusal) = self.owner_gate(&ring, &node, req.m_idx, &key) {
            return refusal.into_claim();
        }
        // Fail closed while partitioned — see `owner_gate`'s doc for why only the claim
        // entry carries this check.
        if node.is_isolated() {
            return ClaimReply::Error {
                reason: "proxy store: owner is isolated from the cluster".to_owned(),
            };
        }
        match node.proxy_recorded(&req.tenant, req.port, &req.sig_hash) {
            Ok(Some(_)) => return ClaimReply::AlreadyRecorded,
            Ok(None) => {}
            Err(e) => {
                return ClaimReply::Error {
                    reason: format!("proxy store: {e}"),
                };
            }
        }
        if self
            .completed_lookup(&node, req.port, &req.sig_hash)
            .is_some()
        {
            return ClaimReply::AlreadyRecorded;
        }
        let mut pending = self.pending.lock();
        let now = Instant::now();
        let slot = (req.port, req.sig_hash.clone());
        if let Some(claim) = pending.get(&slot) {
            if claim.deadline > now {
                return ClaimReply::InFlight;
            }
            // The previous winner ran out its deadline: the signature frees itself and the
            // old token is stale from here on.
            metrics::proxy_claim_release("deadline");
        }
        let token = self.mint_token();
        pending.insert(
            slot,
            PendingClaim {
                token,
                deadline: now + self.claim_ttl(),
            },
        );
        ClaimReply::Claimed { token }
    }

    fn owner_release(&self, req: &ReleaseReq) {
        // Deliberately lenient: no fence, no ownership check. A release is only ever
        // narrowing (it frees a claim this node granted), the token match below already
        // rejects strangers, and refusing a release over a stale m_idx would wedge the
        // signature for the full deadline instead.
        let mut pending = self.pending.lock();
        let slot = (req.port, req.sig_hash.clone());
        if pending.get(&slot).is_some_and(|c| c.token == req.token) {
            pending.remove(&slot);
            metrics::proxy_claim_release("upstream_failure");
        }
    }

    async fn owner_complete(self: &Arc<Self>, req: CompleteReq) -> SettleReply {
        let (node, ring) = match self.view() {
            Ok(view) => view,
            Err(reason) => return SettleReply::Unavailable { reason },
        };
        let key = format!("{}:{}", req.port, req.sig_hash);
        if let Some(refusal) = self.owner_gate(&ring, &node, req.m_idx, &key) {
            return refusal.into_settle();
        }
        {
            let pending = self.pending.lock();
            let slot = (req.port, req.sig_hash.clone());
            match pending.get(&slot) {
                Some(claim) if claim.token == req.token && claim.deadline > Instant::now() => {}
                // Stale token or expired claim: drop the recording, never clobber — the
                // upstream `ClaimToken` contract, applied fleet-wide.
                _ => return SettleReply::Stale,
            }
        }
        let resp_json = match serde_json::to_string(&req.resp) {
            Ok(json) => json,
            Err(e) => {
                // Practically unreachable (bytes and strings serialize totally), but every
                // failure exit from this function releases the claim — an exception would
                // wedge the signature until the deadline for no reason.
                self.pending
                    .lock()
                    .remove(&(req.port, req.sig_hash.clone()));
                metrics::proxy_claim_release("publish_failure");
                return SettleReply::Unavailable {
                    reason: format!("proxy store: encode recording: {e}"),
                };
            }
        };
        let request = ControlRequest {
            op_id: Uuid::new_v4(),
            principal: None,
            issued_at_secs: now_secs(),
            expected_revision: None,
            op: ControlOp::ProxyRecorded {
                tenant: TenantId::new(req.tenant.clone()),
                port: req.port,
                sig_hash: req.sig_hash.clone(),
                resp: req.resp,
                stub: req.stub,
            },
        };
        // Bounded below the bridge's `SETTLE_OP_DEADLINE`: a quorum-less submit can hang
        // until the transport gives up, and if the *bridge* deadline fired first it would
        // abort this future before the release below ever ran — the same Pending wedge the
        // release exists to prevent, arriving by a different route. A submit that commits
        // after this timeout is harmless: the claim is released AND the fact is applied,
        // so the next claim answers `AlreadyRecorded` — still exactly once.
        let submitted = match tokio::time::timeout(SUBMIT_DEADLINE, node.submit(request)).await {
            Ok(submitted) => submitted,
            Err(_) => {
                let slot = (req.port, req.sig_hash.clone());
                self.pending.lock().remove(&slot);
                metrics::proxy_claim_release("publish_failure");
                return SettleReply::Unavailable {
                    reason: "proxy store: publication timed out".to_owned(),
                };
            }
        };
        let slot = (req.port, req.sig_hash.clone());
        match submitted {
            Ok(response) => match response.outcome {
                ControlOutcome::Applied => {
                    self.pending.lock().remove(&slot);
                    self.completed.lock().insert(
                        slot,
                        CachedRecording {
                            resp_json,
                            at: Instant::now(),
                            revision: response.revision,
                        },
                    );
                    metrics::proxy_recording();
                    SettleReply::Done
                }
                ControlOutcome::Failed { reason } => {
                    // A committed refusal (imposter deleted, quota): release so the
                    // signature is retryable, and say why.
                    self.pending.lock().remove(&slot);
                    metrics::proxy_claim_release("publish_failure");
                    SettleReply::Unavailable { reason }
                }
            },
            Err(e) => {
                // The publication did not commit. Releasing here is what keeps the failure
                // retryable instead of wedging Pending until the deadline (#226 AC3).
                self.pending.lock().remove(&slot);
                metrics::proxy_claim_release("publish_failure");
                SettleReply::Unavailable {
                    reason: format!("proxy store: publication failed: {e}"),
                }
            }
        }
    }

    /// A completion-cache hit, valid only inside the commit→apply window it exists to
    /// bridge. Once the node's `last_applied` has passed the entry's commit revision, the
    /// applied row is the truth: present means the caller already answered from it; absent
    /// means a *later* purge (an explicit clear, or an imposter delete/replace) removed
    /// it, and honoring the cache would resurrect a recording the fleet agreed to forget —
    /// which is also what makes purges converge with the log instead of needing any
    /// cross-node cache invalidation.
    fn completed_lookup(&self, node: &RaftNode, port: u16, sig_hash: &str) -> Option<String> {
        let mut completed = self.completed.lock();
        let slot = (port, sig_hash.to_owned());
        let cached = completed.get(&slot)?;
        if cached.at.elapsed() >= COMPLETE_CACHE_TTL {
            completed.remove(&slot);
            return None;
        }
        if node
            .status()
            .last_applied
            .is_some_and(|applied| applied >= cached.revision)
        {
            completed.remove(&slot);
            return None;
        }
        Some(cached.resp_json.clone())
    }

    fn owner_lookup(&self, req: &LookupReq) -> LookupReply {
        let resp_json = match self.view() {
            Ok((node, _)) => match node.proxy_recorded(&req.tenant, req.port, &req.sig_hash) {
                Ok(Some(json)) => Some(json),
                Ok(None) => self.completed_lookup(&node, req.port, &req.sig_hash),
                Err(e) => {
                    // A storage read failure is unhealthy-node signal, not a cache miss —
                    // say so before degrading to the completion cache (which may still
                    // hold an intact copy).
                    tracing::error!(
                        port = req.port,
                        error = %e,
                        "proxy marker read failed; answering from the completion cache"
                    );
                    self.completed_lookup(&node, req.port, &req.sig_hash)
                }
            },
            Err(_) => None,
        };
        LookupReply {
            resp: resp_json.and_then(|json| match serde_json::from_str(&json) {
                Ok(resp) => Some(resp),
                Err(e) => {
                    // Wrong-but-quiet is worse than loud: a corrupt row must not read as
                    // "never recorded" with nothing server-side to correlate — the same
                    // house rule the apply path's corrupt-config arms follow.
                    tracing::error!(
                        port = req.port,
                        error = %e,
                        "corrupt stored proxy recording; replay unavailable"
                    );
                    None
                }
            }),
        }
    }

    /// Drop this node's in-memory claim state for a port. Deliberately also drops live
    /// Pending claims: a clear means "forget recordings", so an in-flight winner's later
    /// `complete` settling as `Stale` is the intended reading of that race, not collateral.
    fn clear_local(&self, port: u16) {
        self.pending.lock().retain(|(p, _), _| *p != port);
        self.completed.lock().retain(|(p, _), _| *p != port);
        self.mode_cache.lock().remove(&port);
    }

    /// Resolve the owner of `key` on the current ring.
    fn owner_of(ring: &Ring, key: &str) -> Result<NodeId, RpcError> {
        ring.owner(OwnedKey::new(KeyClass::Proxy, key))
            .ok_or_else(|| RpcError::Handler("proxy store: ring has no members".to_owned()))
    }
}

/// The first proxy response's `mode` in a config — mirroring upstream
/// `Imposter::extract_proxy_mode` **exactly** (`imposter/core/mod.rs`): lowercased;
/// unknown, empty and absent all mean transparent; no proxy stub at all means transparent.
/// Divergence here is a cluster node treating an imposter differently from single-node
/// rift, which is precisely the fidelity #226's acceptance forbids — a mode-less proxy
/// stub round-trips as `"mode": ""`, and reading that as anything but transparent would
/// silently turn a forward-everything proxy into a record-once one.
fn parse_proxy_mode(config_json: &str) -> PortMode {
    let config = match serde_json::from_str::<Value>(config_json) {
        Ok(config) => config,
        Err(e) => {
            // The stored string is exactly what the apply path wrote after a successful
            // round-trip, so failing to parse it is a storage-integrity signal, not an
            // absent-field domain case (that one is the quiet fallthrough below).
            // Transparent is the fail-closed degraded answer: it forwards and never
            // records, so a corrupt config can neither gate traffic nor replay wrong
            // data — and it must not be silent.
            tracing::error!(
                error = %e,
                "corrupt applied config; treating proxy mode as transparent"
            );
            return PortMode::Transparent;
        }
    };
    let mode = config["stubs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|stub| stub["responses"].as_array())
        .flatten()
        .find_map(|resp| {
            resp.get("proxy")
                .map(|proxy| proxy["mode"].as_str().unwrap_or("").to_lowercase())
        });
    match mode.as_deref() {
        Some("proxyonce") => PortMode::Once,
        Some("proxyalways") => PortMode::Always,
        _ => PortMode::Transparent,
    }
}

enum OwnerRefusal {
    Fenced { owner_m_idx: u64 },
    NotOwner { owner: NodeId },
    Error { reason: String },
}

impl OwnerRefusal {
    fn into_claim(self) -> ClaimReply {
        match self {
            Self::Fenced { owner_m_idx } => ClaimReply::Fenced { owner_m_idx },
            Self::NotOwner { owner } => ClaimReply::NotOwner { owner },
            Self::Error { reason } => ClaimReply::Error { reason },
        }
    }

    fn into_settle(self) -> SettleReply {
        match self {
            Self::Fenced { owner_m_idx } => SettleReply::Fenced { owner_m_idx },
            Self::NotOwner { owner } => SettleReply::NotOwner { owner },
            Self::Error { reason } => SettleReply::Unavailable { reason },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ClaimReq {
    tenant: String,
    port: u16,
    sig_hash: String,
    m_idx: u64,
}

#[derive(Debug, Serialize, Deserialize)]
enum ClaimReply {
    Claimed { token: u64 },
    InFlight,
    AlreadyRecorded,
    Fenced { owner_m_idx: u64 },
    NotOwner { owner: NodeId },
    Error { reason: String },
}

#[derive(Debug, Serialize, Deserialize)]
struct ReleaseReq {
    tenant: String,
    port: u16,
    sig_hash: String,
    token: u64,
    m_idx: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CompleteReq {
    tenant: String,
    port: u16,
    sig_hash: String,
    token: u64,
    m_idx: u64,
    resp: RecordedResponse,
    stub: Option<RecordedStub>,
}

#[derive(Debug, Serialize, Deserialize)]
enum SettleReply {
    Done,
    /// Stale token or expired claim: the recording was dropped, deliberately and quietly —
    /// the upstream contract for a late loser.
    Stale,
    Fenced {
        owner_m_idx: u64,
    },
    NotOwner {
        owner: NodeId,
    },
    Unavailable {
        reason: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct LookupReq {
    tenant: String,
    port: u16,
    sig_hash: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct LookupReply {
    resp: Option<RecordedResponse>,
}

/// The wire surface: four POST routes on the cluster port, HMAC-authed and
/// version-negotiated by the transport like every other route. Deliberately no clear
/// route: purges converge through the applied log plus `completed_lookup`'s revision
/// check, not through a best-effort fan-out a partitioned peer could miss forever.
#[must_use]
pub fn proxy_routes(net: Arc<ProxyNet>) -> Router {
    let claim_net = Arc::clone(&net);
    let release_net = Arc::clone(&net);
    let complete_net = Arc::clone(&net);
    let lookup_net = net;

    Router::new()
        .route(
            "POST",
            CLAIM_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&claim_net);
                Box::pin(async move {
                    let req: ClaimReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("proxy/claim decode: {e}")))?;
                    let reply = net.owner_claim(&req);
                    serde_json::to_vec(&reply).map_err(|e| RpcError::Handler(e.to_string()))
                })
            }),
        )
        .route(
            "POST",
            RELEASE_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&release_net);
                Box::pin(async move {
                    let req: ReleaseReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("proxy/release decode: {e}")))?;
                    net.owner_release(&req);
                    Ok(b"{}".to_vec())
                })
            }),
        )
        .route(
            "POST",
            COMPLETE_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&complete_net);
                Box::pin(async move {
                    let req: CompleteReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("proxy/complete decode: {e}")))?;
                    let reply = net.owner_complete(req).await;
                    serde_json::to_vec(&reply).map_err(|e| RpcError::Handler(e.to_string()))
                })
            }),
        )
        .route(
            "POST",
            LOOKUP_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&lookup_net);
                Box::pin(async move {
                    let req: LookupReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("proxy/lookup decode: {e}")))?;
                    let reply = net.owner_lookup(&req);
                    serde_json::to_vec(&reply).map_err(|e| RpcError::Handler(e.to_string()))
                })
            }),
        )
}

/// The store `cluster_manager` installs via `with_proxy_store`: every imposter on a
/// `--cluster` node records through the fleet's claim machinery. `publishes_stubs()` is
/// `true`, so this store is the sole publisher of recorded stubs — via consensus.
pub struct ClusterProxyStore {
    net: Arc<ProxyNet>,
}

impl ClusterProxyStore {
    #[must_use]
    pub fn new(net: Arc<ProxyNet>) -> Self {
        Self { net }
    }

    fn blocking_claim(
        &self,
        tenant: &str,
        port: u16,
        sig: &RequestSignature,
    ) -> Result<ClaimOutcome, ProxyStoreError> {
        let bridge = self
            .net
            .bridge
            .get()
            .ok_or_else(|| ProxyStoreError::Unavailable("proxy store: not bound".to_owned()))?;
        let net = Arc::clone(&self.net);
        let hash = sig_hash(sig);
        let key = format!("{port}:{hash}");
        let tenant = tenant.to_owned();
        let outcome = bridge
            .call(CallerClass::DataPlane, CLAIM_OP_DEADLINE, async move {
                // Two attempts: a `Fenced`/`NotOwner` redirect means membership moved
                // between resolve and dispatch, so the request — its `m_idx` included —
                // is rebuilt from the fresh ring, not resent.
                let mut last = None;
                for _ in 0..OWNER_REDIRECT_ATTEMPTS {
                    let (node, ring) = net.view().map_err(RpcError::Handler)?;
                    let owner = ProxyNet::owner_of(&ring, &key)?;
                    let req = ClaimReq {
                        tenant: tenant.clone(),
                        port,
                        sig_hash: hash.clone(),
                        m_idx: ring.m_idx(),
                    };
                    let reply = if owner == node.id() {
                        net.owner_claim(&req)
                    } else {
                        let body = serde_json::to_vec(&req)
                            .map_err(|e| RpcError::Handler(e.to_string()))?;
                        let raw = node
                            .call_member_typed(owner, "POST", CLAIM_PATH, body)
                            .await?;
                        serde_json::from_slice(&raw)
                            .map_err(|e| RpcError::Handler(e.to_string()))?
                    };
                    match reply {
                        ClaimReply::Fenced { .. } | ClaimReply::NotOwner { .. } => {
                            last = Some(reply);
                        }
                        settled => return Ok(settled),
                    }
                }
                last.ok_or_else(|| {
                    RpcError::Handler("proxy store: claim never dispatched".to_owned())
                })
            })
            .map_err(|e| ProxyStoreError::Unavailable(format!("proxy store: {e}")))?;
        match outcome {
            ClaimReply::Claimed { token } => {
                metrics::proxy_claim("granted");
                Ok(ClaimOutcome::Claimed(ClaimToken::new(token)))
            }
            ClaimReply::InFlight => {
                metrics::proxy_claim("inflight");
                Ok(ClaimOutcome::InFlight)
            }
            ClaimReply::AlreadyRecorded => {
                metrics::proxy_claim("already_recorded");
                Ok(ClaimOutcome::AlreadyRecorded)
            }
            ClaimReply::Fenced { .. } | ClaimReply::NotOwner { .. } => Err(
                ProxyStoreError::Unavailable("proxy store: ownership unsettled".to_owned()),
            ),
            ClaimReply::Error { reason } => Err(ProxyStoreError::Unavailable(reason)),
        }
    }

    fn blocking_settle(
        &self,
        tenant: &str,
        port: u16,
        sig: &RequestSignature,
        token: ClaimToken,
        resp: RecordedResponse,
        stub: Option<RecordedStub>,
    ) -> Result<(), ProxyStoreError> {
        let bridge = self
            .net
            .bridge
            .get()
            .ok_or_else(|| ProxyStoreError::Unavailable("proxy store: not bound".to_owned()))?;
        let net = Arc::clone(&self.net);
        let hash = sig_hash(sig);
        let key = format!("{port}:{hash}");
        let tenant = tenant.to_owned();
        let reply = bridge
            .call(CallerClass::DataPlane, SETTLE_OP_DEADLINE, async move {
                let mut last = None;
                for _ in 0..OWNER_REDIRECT_ATTEMPTS {
                    let (node, ring) = net.view().map_err(RpcError::Handler)?;
                    let owner = ProxyNet::owner_of(&ring, &key)?;
                    // Cloned per attempt: the request is consumed by the local owner call
                    // or serialized for the wire, and a redirect retry needs a fresh one.
                    // At most OWNER_REDIRECT_ATTEMPTS copies, once per recording — not per
                    // request — so the copy is cheaper than restructuring around it.
                    let req = CompleteReq {
                        tenant: tenant.clone(),
                        port,
                        sig_hash: hash.clone(),
                        token: token.value(),
                        m_idx: ring.m_idx(),
                        resp: resp.clone(),
                        stub: stub.clone(),
                    };
                    let reply = if owner == node.id() {
                        net.owner_complete(req).await
                    } else {
                        let body = serde_json::to_vec(&req)
                            .map_err(|e| RpcError::Handler(e.to_string()))?;
                        let raw = node
                            .call_member_typed(owner, "POST", COMPLETE_PATH, body)
                            .await?;
                        serde_json::from_slice(&raw)
                            .map_err(|e| RpcError::Handler(e.to_string()))?
                    };
                    match reply {
                        SettleReply::Fenced { .. } | SettleReply::NotOwner { .. } => {
                            last = Some(reply);
                        }
                        settled => return Ok(settled),
                    }
                }
                last.ok_or_else(|| {
                    RpcError::Handler("proxy store: complete never dispatched".to_owned())
                })
            })
            .map_err(|e| ProxyStoreError::Unavailable(format!("proxy store: {e}")))?;
        match reply {
            // `Stale` is a quiet drop by contract: the late loser's recording must not
            // clobber the winner's, and the caller still got its upstream response.
            SettleReply::Done | SettleReply::Stale => Ok(()),
            SettleReply::Fenced { .. } | SettleReply::NotOwner { .. } => Err(
                ProxyStoreError::Unavailable("proxy store: ownership unsettled".to_owned()),
            ),
            SettleReply::Unavailable { reason } => Err(ProxyStoreError::Unavailable(reason)),
        }
    }

    /// Publish a recording with no claim behind it — the `proxyAlways` path, where every
    /// request records and merge determinism lives in the apply, not in a claim.
    fn blocking_publish_unclaimed(
        &self,
        tenant: &str,
        port: u16,
        sig: &RequestSignature,
        resp: RecordedResponse,
        stub: Option<RecordedStub>,
    ) -> Result<(), ProxyStoreError> {
        let bridge = self
            .net
            .bridge
            .get()
            .ok_or_else(|| ProxyStoreError::Unavailable("proxy store: not bound".to_owned()))?;
        let net = Arc::clone(&self.net);
        let hash = sig_hash(sig);
        let tenant = TenantId::new(tenant);
        bridge
            .call(CallerClass::DataPlane, SETTLE_OP_DEADLINE, async move {
                let (node, _) = net.view().map_err(RpcError::Handler)?;
                let request = ControlRequest {
                    op_id: Uuid::new_v4(),
                    principal: None,
                    issued_at_secs: now_secs(),
                    expected_revision: None,
                    op: ControlOp::ProxyRecorded {
                        tenant,
                        port,
                        // proxyAlways merges by predicate equality at apply; the marker row
                        // still lands per signature, first-wins, which is harmless — the
                        // mode never consults it.
                        sig_hash: hash,
                        resp,
                        stub,
                    },
                };
                match node.submit(request).await {
                    Ok(response) => match response.outcome {
                        ControlOutcome::Applied => {
                            metrics::proxy_recording();
                            Ok(())
                        }
                        ControlOutcome::Failed { reason } => Err(RpcError::Handler(reason)),
                    },
                    Err(e) => Err(RpcError::Handler(e.to_string())),
                }
            })
            .map_err(|e| ProxyStoreError::Unavailable(format!("proxy store: {e}")))
    }

    /// The publication path `complete()` adapts onto — public because upstream's
    /// `StubPublication` is engine-constructed (`#[non_exhaustive]`), so the acceptance
    /// gate exercises this face with the same arguments the adapter passes.
    ///
    /// # Errors
    ///
    /// `Unavailable` when the claim could not be settled or the publication did not commit;
    /// the engine then releases the claim and still returns the upstream response.
    pub fn complete_recorded(
        &self,
        port: u16,
        sig: RequestSignature,
        token: ClaimToken,
        resp: RecordedResponse,
        recorded: RecordedStub,
    ) -> Result<(), ProxyStoreError> {
        let identity = self.net.port_identity(port)?;
        match identity.mode {
            PortMode::Once => {
                self.blocking_settle(&identity.tenant, port, &sig, token, resp, Some(recorded))
            }
            // proxyAlways and proxyTransparent both publish without a claim. Transparent
            // never records *responses*, but the engine's stub generation is gated on
            // `predicateGenerators`, not mode — a transparent imposter with generators
            // gets its stub inserted upstream, and with `publishes_stubs()` making this
            // store the sole publisher, dropping it here would be the silent regression
            // the module doc forbids for proxyAlways, applied to the third mode.
            PortMode::Always | PortMode::Transparent => {
                self.blocking_publish_unclaimed(&identity.tenant, port, &sig, resp, Some(recorded))
            }
        }
    }
}

impl ProxyRecordingStore for ClusterProxyStore {
    fn try_claim(
        &self,
        port: u16,
        sig: &RequestSignature,
    ) -> Result<ClaimOutcome, ProxyStoreError> {
        let identity = self.net.port_identity(port)?;
        match identity.mode {
            PortMode::Once => self.blocking_claim(&identity.tenant, port, sig),
            // proxyAlways / proxyTransparent never gate — the claim is a formality so the
            // caller path is uniform, exactly as `LocalProxyStore` answers.
            PortMode::Always | PortMode::Transparent => Ok(ClaimOutcome::Claimed(ClaimToken::new(
                self.net.mint_token(),
            ))),
        }
    }

    fn release_claim(&self, port: u16, sig: &RequestSignature, token: ClaimToken) {
        let identity = match self.net.port_identity(port) {
            Ok(identity) => identity,
            Err(e) => {
                // Same self-healing bound as the RPC-failure warn below: the claim frees
                // itself at the deadline, but the skipped release should not be invisible.
                tracing::warn!(port, error = %e, "proxy claim release skipped: port identity unresolved");
                return;
            }
        };
        if identity.mode != PortMode::Once {
            return;
        }
        let Some(bridge) = self.net.bridge.get() else {
            tracing::warn!(port, "proxy claim release before the cluster bound");
            return;
        };
        let net = Arc::clone(&self.net);
        let hash = sig_hash(sig);
        let key = format!("{port}:{hash}");
        let released = bridge.call(CallerClass::DataPlane, CLAIM_OP_DEADLINE, async move {
            let (node, ring) = net.view().map_err(RpcError::Handler)?;
            let owner = ProxyNet::owner_of(&ring, &key)?;
            let req = ReleaseReq {
                tenant: identity.tenant,
                port,
                sig_hash: hash,
                token: token.value(),
                m_idx: ring.m_idx(),
            };
            if owner == node.id() {
                net.owner_release(&req);
                Ok(())
            } else {
                let body =
                    serde_json::to_vec(&req).map_err(|e| RpcError::Handler(e.to_string()))?;
                node.call_member_typed(owner, "POST", RELEASE_PATH, body)
                    .await?;
                Ok(())
            }
        });
        if let Err(e) = released {
            // Not silent, not fatal: an unreleased claim frees itself at the deadline, so
            // the signature wedges for at most `claim_ttl`, and the log says why.
            tracing::warn!(port, error = %e, "proxy claim release did not reach the owner");
        }
    }

    fn record(
        &self,
        port: u16,
        sig: RequestSignature,
        token: ClaimToken,
        resp: RecordedResponse,
    ) -> Result<(), ProxyStoreError> {
        let identity = self.net.port_identity(port)?;
        match identity.mode {
            // A stub-less proxyOnce recording: the consensus row is the replay source.
            PortMode::Once => self.blocking_settle(&identity.tenant, port, &sig, token, resp, None),
            // proxyAlways with no generated stub publishes nothing: there is no stub to
            // replicate and the mode never replays from `lookup`. Upstream's in-memory
            // response list exists to feed later stub generation, which the clustered
            // store does not model — documented in Ch.7.
            PortMode::Always | PortMode::Transparent => Ok(()),
        }
    }

    fn complete(
        &self,
        port: u16,
        sig: RequestSignature,
        token: ClaimToken,
        resp: RecordedResponse,
        publication: &StubPublication<'_>,
    ) -> Result<(), ProxyStoreError> {
        let placement = match publication.placement {
            StubPlacement::BeforeProxy => Some(RecordedStubPlacement::BeforeProxy),
            StubPlacement::AfterProxyMerging => Some(RecordedStubPlacement::AfterProxyMerging),
            // A placement this build does not know: treat the stub as unpublishable
            // (upstream's own guidance) and keep the recording itself — the row still
            // replays.
            _ => None,
        };
        match placement {
            Some(placement) => self.complete_recorded(
                port,
                sig,
                token,
                resp,
                RecordedStub {
                    stub: Box::new(publication.stub.clone()),
                    placement,
                    proxy_to: publication.proxy_to.to_owned(),
                },
            ),
            None => self.record(port, sig, token, resp),
        }
    }

    fn publishes_stubs(&self) -> bool {
        true
    }

    fn lookup(&self, port: u16, sig: &RequestSignature) -> Option<RecordedResponse> {
        let identity = self.net.port_identity(port).ok()?;
        let hash = sig_hash(sig);
        // Local applied state first: after the op applies everywhere this answers with no
        // RPC, and after the recorded stub applies the engine stops asking altogether.
        if let Ok((node, _)) = self.net.view()
            && let Ok(Some(json)) = node.proxy_recorded(&identity.tenant, port, &hash)
        {
            match serde_json::from_str(&json) {
                Ok(resp) => return Some(resp),
                Err(e) => {
                    // Corrupt local copy: loud, then fall through to the owner path below —
                    // its completion cache may still hold an intact copy. Silent `None`
                    // here would read as "never recorded" with nothing to correlate.
                    tracing::error!(
                        port,
                        error = %e,
                        "corrupt stored proxy recording on this node; asking the owner"
                    );
                }
            }
        }
        // The commit→apply window: the owner committed it and remembers.
        let bridge = self.net.bridge.get()?;
        let net = Arc::clone(&self.net);
        let key = format!("{port}:{hash}");
        let reply = bridge
            .call(CallerClass::DataPlane, CLAIM_OP_DEADLINE, async move {
                let (node, ring) = net.view().map_err(RpcError::Handler)?;
                let owner = ProxyNet::owner_of(&ring, &key)?;
                let req = LookupReq {
                    tenant: identity.tenant,
                    port,
                    sig_hash: hash,
                };
                if owner == node.id() {
                    Ok(net.owner_lookup(&req))
                } else {
                    let body =
                        serde_json::to_vec(&req).map_err(|e| RpcError::Handler(e.to_string()))?;
                    let raw = node
                        .call_member_typed(owner, "POST", LOOKUP_PATH, body)
                        .await?;
                    serde_json::from_slice::<LookupReply>(&raw)
                        .map_err(|e| RpcError::Handler(e.to_string()))
                }
            })
            .ok()?;
        reply.resp
    }

    /// Local caches only — deliberately **no Raft write and no RPC**, ever.
    ///
    /// Two reasons, one per caller. The manager calls this from its port-reclaim path,
    /// which every node's engine drive reaches on every imposter delete or replace while
    /// `apply` awaits it — a submit from there either blocks the apply loop it needs
    /// (the C22/C23/C26 write-deadline storms) or, spawned, has *every replica* minting
    /// its own audited op per reclaim (the C26 audit-chain drift). And nothing durable
    /// needs doing here anyway: the delete/replace ops purge the marker rows atomically
    /// at apply, and the explicit `DELETE .../savedProxyResponses` terminates at the
    /// front door as one `ProxyRecordedClear` op. Stale completion-cache entries on
    /// *other* nodes are not fanned out either — `completed_lookup` invalidates them
    /// against the applied state, so purges converge with the log.
    fn clear(&self, port: u16) {
        self.net.clear_local(port);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the `extract_proxy_mode` mirror: a mode-less proxy stub round-trips as
    /// `"mode": ""`, and upstream reads empty, unknown and absent all as transparent.
    /// Reading any of them as `Once` would turn a forward-everything proxy into a
    /// record-once one — the exact single-node divergence #226's fidelity gate forbids.
    #[test]
    fn proxy_mode_parsing_mirrors_upstream_extract_proxy_mode() {
        let with_mode = |mode: &str| {
            format!(
                r#"{{"port":1,"protocol":"http","stubs":[{{"responses":[{{"proxy":{{"to":"http://u","mode":"{mode}"}}}}]}}]}}"#
            )
        };
        assert_eq!(parse_proxy_mode(&with_mode("proxyOnce")), PortMode::Once);
        assert_eq!(parse_proxy_mode(&with_mode("PROXYONCE")), PortMode::Once);
        assert_eq!(
            parse_proxy_mode(&with_mode("proxyAlways")),
            PortMode::Always
        );
        assert_eq!(
            parse_proxy_mode(&with_mode("proxyTransparent")),
            PortMode::Transparent
        );
        assert_eq!(parse_proxy_mode(&with_mode("")), PortMode::Transparent);
        assert_eq!(
            parse_proxy_mode(&with_mode("somethingElse")),
            PortMode::Transparent
        );

        let mode_less =
            r#"{"port":1,"protocol":"http","stubs":[{"responses":[{"proxy":{"to":"http://u"}}]}]}"#;
        assert_eq!(parse_proxy_mode(mode_less), PortMode::Transparent);

        let no_proxy_stub = r#"{"port":1,"protocol":"http","stubs":[]}"#;
        assert_eq!(parse_proxy_mode(no_proxy_stub), PortMode::Transparent);

        assert_eq!(parse_proxy_mode("not json"), PortMode::Transparent);
    }
}
