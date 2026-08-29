//! Fleet-wide response cycling (issue #466, decision D-47): one cursor per stub,
//! owned by one node, so `responses: [A, B, C]` behind a round-robin load
//! balancer answers `A, B, C, A, …` instead of each node cycling its own copy.
//!
//! Opt-in per imposter (`_rift.sequencing.mode: "owner"`), because D-10 already
//! decided the default: sequencing is **the one stateful op where availability
//! wins**. Blocking every cyclic response during a leadership blip is worse than
//! a possible duplicate index, so an unreachable owner is a *fallback*, never a
//! `503` — see [`ClusteredSequencer::route`].
//!
//! Shape, mirroring the flow store's discipline (D-9, D-17, D-3):
//!
//! - **Ownership.** Each `(port, stub_key, scope)` has one owner on the HRW
//!   ring under [`KeyClass::Sequence`]. Keyed by `stub_key` rather than the
//!   engine's `slot`, because `slot` is node-local and cannot be a cluster key
//!   (RFC-001 §8.3). The documented consequence: editing a *keyless* stub
//!   changes its `stub_key` and so restarts its cluster cursor, where a
//!   single-node `LocalSequencer` would preserve it. A stub that needs
//!   cross-node sequencing should carry an explicit `id`.
//! - **Not replicated, not persisted.** D-8: replicating every advance would put
//!   a network write on the hottest stateful path. Cursors are test-run-scoped;
//!   a membership change hands the key to a new owner that starts at zero, and
//!   that reset is the documented contract rather than a fault.
//! - **Sync seam, async cluster.** `ResponseSequencer` is synchronous and is
//!   called from response building on a tokio worker, so a remote hop parks on
//!   the bridge as [`CallerClass::DataPlane`] — the class whose permits are
//!   capped precisely so cursor traffic cannot starve the stateless path
//!   (RFC-001 §11.3).
//! - **An isolated owner refuses** (D-17), and the caller falls back. Placed
//!   after the ownership check so a misroute is still a misroute (#465).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use rift_cluster_base::seams::{ImposterConfig, ResponseSequencer, SequenceKey};

use crate::bridge::{Bridge, CallerClass};
use crate::raft::{KeyClass, OwnedKey, RaftNode};
use crate::rpc::{Router, RpcError};

/// Annotation key for a decision the fleet could not answer. The decorator
/// turns it into a `Rift-Cluster-*` header, so a caller can tell a locally
/// cycled response from a fleet-ordered one (D-10's flagged degradation).
const ANNOTATION: &str = "cluster.sequence";

/// What answered one cursor decision. Mirrors `rift_cluster_flow_reads_total`'s
/// `path` label, and is the closed set behind
/// `rift_cluster_sequence_decisions_total{path}` — so the metric's label values
/// cannot drift from the paths [`ClusteredSequencer::route`] actually has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecisionPath {
    /// This node owns the cursor; served from its own memory, no hop.
    Owner,
    /// One RPC to the owner — the whole cost of `owner` mode on a non-owner.
    Forward,
    /// The imposter never opted in, so the node cycles its own cursor (D-10's
    /// default). Not a degradation.
    Local,
    /// `owner` mode, but the fleet could not answer: cycled locally and
    /// annotated (D-10). Also counted by `rift_cluster_sequence_fallbacks_total`.
    Fallback,
}

impl DecisionPath {
    fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Forward => "forward",
            Self::Local => "local",
            Self::Fallback => "fallback",
        }
    }
}

/// Wire path for an advancing cursor read.
const NEXT_PATH: &str = "/_cluster/seq/next";
/// Wire path for a non-advancing cursor read.
const PEEK_PATH: &str = "/_cluster/seq/peek";
/// Wire path for a cursor reset (config-time, fanned out to every member).
const RESET_PATH: &str = "/_cluster/seq/reset";

/// Budget for one owner hop. Matches the flow store's: one LAN RPC, and a
/// caller that waits longer than this is worse off than one that cycles locally.
const SEQ_OP_DEADLINE: Duration = Duration::from_secs(2);

/// How an imposter wants its response cursors kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SequencingMode {
    /// Per-process cursors — byte-identical to a single-node rift. The default,
    /// and what every imposter that says nothing gets (D-10).
    #[default]
    Local,
    /// One cursor per stub, owned by one node, consistent fleet-wide.
    Owner,
}

/// The key this module owns inside the `_rift.sequencing` block (upstream
/// rift#978 declared it; before that the block was dropped on parse).
const KEY_MODE: &str = "mode";

impl SequencingMode {
    /// Parse an imposter's `_rift.sequencing` block. Absent block ⇒ [`Self::Local`].
    ///
    /// # Errors
    ///
    /// A value this build cannot interpret. The message is client-facing — it
    /// becomes a 400's `reason` — so it names the key, the value and the
    /// accepted set.
    pub fn from_imposter(config: &ImposterConfig) -> Result<Self, String> {
        let Some(sequencing) = config.rift.as_ref().and_then(|r| r.sequencing.as_ref()) else {
            return Ok(Self::Local);
        };
        match sequencing.mode.as_deref() {
            None => Ok(Self::Local),
            Some("local") => Ok(Self::Local),
            Some("owner") => Ok(Self::Owner),
            Some(other) => Err(format!(
                "sequencing.{KEY_MODE} must be \"local\" or \"owner\", got {other:?}"
            )),
        }
    }
}

/// Which mode each port is running, as of the last applied config set.
///
/// A manager-wide [`ResponseSequencer`] is handed only a [`SequenceKey`], which
/// carries a port and no config — and unlike `FlowStoreProvider` the seam has no
/// per-imposter hook to read one from. This registry is that missing lookup.
///
/// Fed from the Raft apply loop's `EngineAction::Sync`, which is the single
/// funnel every config passes through to become real on a node, and which
/// carries the *complete* desired set — so the map is exact by construction and
/// a port that disappears drops out. The two alternatives were rejected:
/// `RaftNode::imposter_config` needs a tenant the key does not carry and is a
/// storage read on a 20–40k RPS path, and `FlowStoreProvider::provide` is not
/// re-called when an existing imposter's config changes in place, so a mode
/// change would go unnoticed — silently stale, which is worse than absent.
#[derive(Debug, Default)]
pub struct SequencingRegistry {
    modes: RwLock<HashMap<u16, SequencingMode>>,
}

impl SequencingRegistry {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Rebuild from the applied config set. Replaces rather than merges: the
    /// set is authoritative, so a port missing from it no longer exists here.
    ///
    /// A config whose block this build cannot parse falls back to `Local` and
    /// logs — `validate` refuses bad values before they commit, so reaching here
    /// means a config written by a newer build, and defaulting to the mode that
    /// changes nothing beats both a panic on the apply path and inheriting a
    /// stale mode.
    pub fn apply(&self, desired: &[ImposterConfig]) {
        let next: HashMap<u16, SequencingMode> = desired
            .iter()
            .filter_map(|config| {
                let port = config.port?;
                let mode = SequencingMode::from_imposter(config).unwrap_or_else(|reason| {
                    tracing::error!(
                        port,
                        %reason,
                        "sequencing carried a value this build cannot parse; using local"
                    );
                    SequencingMode::Local
                });
                Some((port, mode))
            })
            .collect();
        *self.modes.write() = next;
    }

    /// The mode for `port`. Unknown ports are [`SequencingMode::Local`]: an
    /// imposter this node has not applied yet must not be cycled through a
    /// cursor nobody owns.
    #[must_use]
    pub fn mode(&self, port: u16) -> SequencingMode {
        self.modes
            .read()
            .get(&port)
            .copied()
            .unwrap_or(SequencingMode::Local)
    }
}

/// One cursor decision, addressed to the owner.
#[derive(Debug, Serialize, Deserialize)]
struct SeqReq {
    m_idx: u64,
    port: u16,
    stub_key: String,
    scope: String,
    response_count: usize,
    repeats: Vec<u32>,
}

/// The owner's answer. Mirrors the flow store's reply vocabulary so the two
/// owner-routed paths fail the same way for the same reasons.
#[derive(Debug, Serialize, Deserialize)]
enum SeqReply {
    /// The index to serve.
    Index { index: usize },
    /// The op carried a stale membership index; rebuild the ring and retry once.
    Fenced { owner_m_idx: u64 },
    /// This node does not own the key at the shared `m_idx` — only a buggy
    /// member misroutes, since a correct one computes the same HRW owner.
    NotOwner { owner: crate::NodeId },
    /// The owner declined: isolated, or its state is unreadable.
    Error { reason: String },
}

/// A reset, fanned out to every member.
#[derive(Debug, Serialize, Deserialize)]
struct ResetReq {
    port: u16,
    stub_key: Option<String>,
}

type CursorKey = (u16, String, String);

/// The clustered [`ResponseSequencer`]: one per node, serving every imposter.
pub struct ClusteredSequencer {
    /// Late-bound like the flow net's: the manager exists before the node does.
    /// `Weak` so this never keeps the node alive past shutdown.
    node: OnceLock<Weak<RaftNode>>,
    /// Sync→async for the seam (D-9). Created at bind time, so an unclustered
    /// process never pays for the bridge runtime.
    bridge: OnceLock<Bridge>,
    /// Cursors this node owns, plus the local-mode and fallback cursors. One map:
    /// a fallback advance and an owned advance are the same operation on the same
    /// key, and keeping two would let them disagree about where a stub is.
    cursors: RwLock<HashMap<CursorKey, rift_cluster_base::seams::RuleCycler>>,
    registry: Arc<SequencingRegistry>,
}

impl ClusteredSequencer {
    #[must_use]
    pub fn new(registry: Arc<SequencingRegistry>) -> Arc<Self> {
        Arc::new(Self {
            node: OnceLock::new(),
            bridge: OnceLock::new(),
            cursors: RwLock::new(HashMap::new()),
            registry,
        })
    }

    /// Attach the cluster. Before this the sequencer answers every call from its
    /// local cursors, which is what a starting node should do — and what an
    /// unclustered process keeps doing, since it never binds.
    ///
    /// # Errors
    ///
    /// The bridge's runtime could not start.
    pub fn bind(
        self: &Arc<Self>,
        node: &Arc<RaftNode>,
        config: crate::bridge::BridgeConfig,
    ) -> std::io::Result<()> {
        if self.bridge.get().is_none() {
            let bridge = Bridge::start(config)?;
            let _ = self.bridge.set(bridge);
        }
        let _ = self.node.set(Arc::downgrade(node));
        Ok(())
    }

    fn view(&self) -> Option<(Arc<RaftNode>, crate::raft::Ring)> {
        let node = self.node.get()?.upgrade()?;
        let ring = node.ring();
        (!ring.is_empty()).then_some((node, ring))
    }

    fn key_of(key: &SequenceKey<'_>) -> CursorKey {
        (key.port, key.stub_key.to_owned(), key.scope.to_owned())
    }

    /// Advance or peek this node's own cursor for `key`.
    fn locally(
        &self,
        key: &CursorKey,
        response_count: usize,
        repeats: &[u32],
        advance: bool,
    ) -> usize {
        if response_count == 0 {
            return 0;
        }
        let mut cursors = self.cursors.write();
        let cycler = cursors.entry(key.clone()).or_default();
        let index = if advance {
            cycler.get_response_index_advance(response_count as u32, |i| {
                repeats.get(i as usize).copied()
            }) as usize
        } else {
            cycler.peek_response_index(response_count as u32) as usize
        };
        // Clamp: right after `responses` shrinks the cycler can return the stale
        // pre-clamp index once, and the seam's contract is `< response_count` —
        // a violation is a 500, never a silently wrong response.
        index.min(response_count - 1)
    }

    /// Route one decision: owner-answered when the imposter asked for it and the
    /// fleet can answer, this node's own cursor otherwise.
    ///
    /// **Every cluster failure falls back rather than erroring.** D-10 settled
    /// that for this op specifically: an unreachable owner must not block every
    /// cyclic response, so the cost of a blip is a possible duplicate index and
    /// a flagged response, not a 503. The annotation is what keeps that honest —
    /// a caller can see that the index it got was not fleet-ordered.
    fn route(
        &self,
        key: SequenceKey<'_>,
        response_count: usize,
        repeats: &[u32],
        advance: bool,
    ) -> usize {
        let cursor_key = Self::key_of(&key);
        let (index, path) = if self.registry.mode(key.port) != SequencingMode::Owner {
            (
                self.locally(&cursor_key, response_count, repeats, advance),
                DecisionPath::Local,
            )
        } else {
            match self.owner_index(&cursor_key, response_count, repeats, advance) {
                Some(answer) => answer,
                None => {
                    crate::metrics::sequence_fallback();
                    rift_cluster_base::seams::annotate(ANNOTATION, "local-fallback".to_owned());
                    (
                        self.locally(&cursor_key, response_count, repeats, advance),
                        DecisionPath::Fallback,
                    )
                }
            }
        };
        // Counted here rather than in the branches so every decision is counted
        // exactly once, whichever way it was answered — that is the property
        // D-63's pin reads back.
        crate::metrics::sequence_decision(Self::op_of(advance), path.as_str());
        index
    }

    /// The `op` label: which of the seam's two reads this decision was.
    fn op_of(advance: bool) -> &'static str {
        if advance { "next" } else { "peek" }
    }

    /// The owner's answer and how it was reached, or `None` when the fleet
    /// could not give one.
    fn owner_index(
        &self,
        cursor_key: &CursorKey,
        response_count: usize,
        repeats: &[u32],
        advance: bool,
    ) -> Option<(usize, DecisionPath)> {
        let bridge = self.bridge.get()?;
        let (node, ring) = self.view()?;
        let ring_key = wire_key(cursor_key);
        let owner = ring.owner(OwnedKey::new(KeyClass::Sequence, &ring_key))?;

        if owner == node.id() {
            // D-17: an isolated owner must not serve its own cursor either —
            // a healed majority may have re-homed this key already.
            if node.is_isolated() {
                return None;
            }
            return Some((
                self.locally(cursor_key, response_count, repeats, advance),
                DecisionPath::Owner,
            ));
        }

        let req = SeqReq {
            m_idx: ring.m_idx(),
            port: cursor_key.0,
            stub_key: cursor_key.1.clone(),
            scope: cursor_key.2.clone(),
            response_count,
            repeats: repeats.to_vec(),
        };
        let path = if advance { NEXT_PATH } else { PEEK_PATH };
        let reply = bridge
            .call(CallerClass::DataPlane, SEQ_OP_DEADLINE, async move {
                let body =
                    serde_json::to_vec(&req).map_err(|e| RpcError::Handler(e.to_string()))?;
                let raw = node
                    .call_member(owner, "POST", path, body)
                    .await
                    .map_err(RpcError::Transport)?;
                serde_json::from_slice::<SeqReply>(&raw)
                    .map_err(|e| RpcError::Handler(e.to_string()))
            })
            .ok()?;

        match reply {
            SeqReply::Index { index } if index < response_count => {
                Some((index, DecisionPath::Forward))
            }
            // Anything else is the fleet declining to answer: fenced, misrouted,
            // an isolated owner, or an index the seam's contract forbids. All of
            // them mean "cycle locally and say so" rather than fail the request.
            _ => None,
        }
    }

    /// Owner-side handler for a routed decision.
    fn owner_decide(&self, req: &SeqReq, advance: bool) -> SeqReply {
        let Some((node, ring)) = self.view() else {
            return SeqReply::Error {
                reason: "sequencer: no applied membership yet".to_owned(),
            };
        };
        if req.m_idx != ring.m_idx() {
            return SeqReply::Fenced {
                owner_m_idx: ring.m_idx(),
            };
        }
        let cursor_key: CursorKey = (req.port, req.stub_key.clone(), req.scope.clone());
        let ring_key = wire_key(&cursor_key);
        match ring.owner(OwnedKey::new(KeyClass::Sequence, &ring_key)) {
            Some(owner) if owner == node.id() => {}
            Some(owner) => return SeqReply::NotOwner { owner },
            None => {
                return SeqReply::Error {
                    reason: "sequencer: ring has no members".to_owned(),
                };
            }
        }
        // After the ownership check, so a misroute is still reported as one (#465).
        if node.is_isolated() {
            return SeqReply::Error {
                reason: "sequencer: owner is isolated from the cluster".to_owned(),
            };
        }
        SeqReply::Index {
            index: self.locally(&cursor_key, req.response_count, &req.repeats, advance),
        }
    }

    /// Drop cursors for a port, or for one stub on it.
    fn forget(&self, port: u16, stub_key: Option<&str>) {
        let mut cursors = self.cursors.write();
        match stub_key {
            None => cursors.retain(|(p, _, _), _| *p != port),
            Some(key) => cursors.retain(|(p, k, _), _| *p != port || k != key),
        }
    }
}

/// First pause between reset attempts, doubling to [`RESET_MAX_ATTEMPTS`].
///
/// A full second, not the 200 ms a retry loop would otherwise reach for, and the reason is the
/// RPC client's own circuit breaker: three consecutive liveness failures mark a peer unhealthy
/// for `DEFAULT_COOLDOWN` (5 s), during which `RpcClient::call` fast-fails without touching the
/// wire (`rpc/client.rs`). The client already burns four internal attempts per call, so a peer
/// that sheds trips the breaker inside our *first* attempt — and a 200 ms schedule would spend
/// every remaining attempt inside that cooldown, answering "not healthy" without ever asking
/// again. Doubling from 1 s puts attempts 4 and 5 at +7 s and +15 s, past it.
const RESET_BACKOFF_MIN: Duration = Duration::from_secs(1);
/// How many times one peer is asked to drop a scope's cursors before it is reported unreached
/// (D-57, #514). Five attempts on the schedule above spans about fifteen seconds — enough to
/// outlast the peer-health cooldown twice, and deliberately not enough to keep chasing a member
/// that is down: such a member re-keys on the next membership change (D-8), and applies the
/// committed op itself in any case.
const RESET_MAX_ATTEMPTS: u32 = 5;
/// Ceiling on one whole fan-out, whatever the peers do.
///
/// The old shape inherited a bound from `bridge.call`'s deadline; spawning the fan-out dropped
/// it and nothing else supplies one — an attempt sweeps every resolved address of a peer at a
/// 2 s request timeout with the client's own retries on top, so five attempts against a
/// black-holing dual-stack member could otherwise run for minutes. This also bounds how long the
/// spawned task holds its `Arc<RaftNode>`, which the sequencer deliberately holds only as a
/// `Weak` everywhere else.
const RESET_TOTAL_BUDGET: Duration = Duration::from_secs(60);

/// What one delivery attempt established about a peer.
enum Delivery {
    /// The peer dropped the scope's cursors.
    Done,
    /// The peer is no longer in the applied membership, so it holds no cursors this reset is
    /// about. Not a failure, not retried, and deliberately **not** named in the incomplete
    /// warning — a departed member is not something an operator needs to chase.
    NotAMember,
}

/// Deliver a cursor reset to every peer, each independently retried, and report the ones that
/// never accepted it (D-57, #514).
///
/// Per-peer and concurrent, rather than a sequential loop under one shared deadline: the old
/// shape gave every peer after the first whatever was left of a single 2 s budget, so one slow
/// member could consume the whole fan-out and the rest were never asked at all — silently,
/// because each failure was a `debug!` and the aggregate result was discarded.
///
/// Takes `send` rather than a `RaftNode` so the retry policy itself is testable without a fleet.
async fn deliver_reset<F, Fut, E>(
    peers: Vec<crate::NodeId>,
    backoff_min: Duration,
    send: F,
) -> Vec<crate::NodeId>
where
    F: Fn(crate::NodeId) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<Delivery, E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let mut tasks = tokio::task::JoinSet::new();
    // Task id -> peer, so a panicked delivery can still be named below. Without it the peer that
    // task was carrying would vanish from `unreached` and the sweep would report itself complete
    // — the exact silence this function exists to remove, in the branch that is our own bug.
    let mut carrying: HashMap<tokio::task::Id, crate::NodeId> = HashMap::new();
    for peer in peers {
        let send = send.clone();
        let handle = tasks.spawn(async move {
            let mut backoff = backoff_min;
            for attempt in 1..=RESET_MAX_ATTEMPTS {
                match send(peer).await {
                    Ok(Delivery::Done | Delivery::NotAMember) => return None,
                    Err(e) => {
                        tracing::debug!(
                            peer, attempt, error = %e,
                            "sequencer reset: peer did not accept it"
                        );
                    }
                }
                if attempt < RESET_MAX_ATTEMPTS {
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2);
                }
            }
            Some(peer)
        });
        carrying.insert(handle.id(), peer);
    }
    let mut unreached = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Some(peer)) => unreached.push(peer),
            Ok(None) => {}
            Err(e) => {
                // A panicked delivery task is a defect of ours, and its peer is reported
                // unreached rather than dropped: we do not know that the reset landed.
                if let Some(peer) = carrying.get(&e.id()) {
                    unreached.push(*peer);
                }
                tracing::error!(error = %e, "sequencer reset delivery panicked");
            }
        }
    }
    unreached.sort_unstable();
    unreached
}

/// The ring key for a cursor. One string so the hash covers all three parts;
/// `\u{1}` because it cannot appear in a port, a stub key or a space name.
fn wire_key((port, stub_key, scope): &CursorKey) -> String {
    format!("{port}\u{1}{stub_key}\u{1}{scope}")
}

impl ResponseSequencer for ClusteredSequencer {
    fn next(
        &self,
        key: SequenceKey<'_>,
        response_count: usize,
        repeats: &[u32],
    ) -> anyhow::Result<usize> {
        Ok(self.route(key, response_count, repeats, true))
    }

    fn peek(
        &self,
        key: SequenceKey<'_>,
        response_count: usize,
        repeats: &[u32],
    ) -> anyhow::Result<usize> {
        Ok(self.route(key, response_count, repeats, false))
    }

    /// Config-time, not hot path: the engine calls this on stub delete, bulk
    /// replace and imposter teardown.
    ///
    /// Fanned out to every member rather than routed to an owner: the port-wide
    /// form (`None`) names no single key and so has no owner, and a cursor held
    /// by a member that is down is gone anyway.
    ///
    /// A **backstop**, not the only mechanism (D-57): this hook also runs on every member from
    /// its own `drive_engine` when the committed config op applies, so each node normally drops
    /// its own cursors without being asked. The fan-out covers what that misses — a direct call
    /// on one node, and a member whose engine refused the apply — and since #514 it retries and
    /// reports rather than losing the reset silently.
    fn reset_scope(&self, port: u16, stub_key: Option<&str>) {
        self.forget(port, stub_key);
        let (Some(bridge), Some((node, ring))) = (self.bridge.get(), self.view()) else {
            return;
        };
        let req = ResetReq {
            port,
            stub_key: stub_key.map(ToOwned::to_owned),
        };
        let peers: Vec<crate::NodeId> = ring
            .members()
            .iter()
            .copied()
            .filter(|id| *id != node.id())
            .collect();
        if peers.is_empty() {
            return;
        }
        let body = match serde_json::to_vec(&req) {
            Ok(body) => body,
            // Nothing retryable about a body this process could not encode, and nothing to be
            // gained by taking the caller down over it — but it must not be silent: every peer's
            // cursor is about to be left stale.
            Err(e) => {
                tracing::error!(port, error = %e, "sequencer reset: could not encode the request");
                return;
            }
        };
        // Spawned, not run under `bridge.call`. Two reasons, both learned from #514: a
        // `DataPlane` permit can be *shed* under load, which silently dropped the whole fan-out
        // and left the ring owner cycling a deleted stub; and the caller is the engine's
        // config-time GC hook, which has no business waiting out a fleet round trip. `forget`
        // above already did the part the caller's next decision depends on.
        bridge.handle().spawn(async move {
            let fan_out = deliver_reset(peers, RESET_BACKOFF_MIN, move |peer| {
                let node = Arc::clone(&node);
                let body = body.clone();
                async move {
                    // A member that has left is not a failed delivery: it holds no cursors this
                    // reset is about, and retrying it five times only to name it in a fault
                    // warning would send an operator after a node that is correctly gone.
                    if node.member_authority(peer).is_none() {
                        return Ok(Delivery::NotAMember);
                    }
                    node.call_member(peer, "POST", RESET_PATH, body)
                        .await
                        .map(|_| Delivery::Done)
                }
            });
            let unreached = match tokio::time::timeout(RESET_TOTAL_BUDGET, fan_out).await {
                Ok(unreached) => unreached,
                // The budget is a ceiling, not an expectation. Which members were still
                // outstanding is not recoverable once the whole fan-out is cancelled, so this
                // reports the sweep incomplete without naming them.
                Err(_) => {
                    tracing::warn!(
                        port,
                        budget_secs = RESET_TOTAL_BUDGET.as_secs(),
                        "sequencer reset fan-out exceeded its budget and was abandoned"
                    );
                    crate::metrics::sequence_reset_incomplete();
                    return;
                }
            };
            if !unreached.is_empty() {
                crate::metrics::sequence_reset_incomplete();
                // Named, not counted-and-forgotten: a member that missed a reset keeps cycling a
                // stub that no longer exists until the next membership change re-keys it, and the
                // operator can only act on that if they know which one (the D-53 shape).
                tracing::warn!(
                    ?unreached,
                    "sequencer reset did not reach every member; their cursors for this scope \
                     stay stale until they are asked again or membership changes"
                );
            }
        });
    }
}

/// The wire surface peers reach this node's cursors through.
#[must_use]
pub fn seq_routes(seq: Arc<ClusteredSequencer>) -> Router {
    let next_seq = Arc::clone(&seq);
    let peek_seq = Arc::clone(&seq);
    let reset_seq = seq;

    Router::new()
        .route(
            "POST",
            NEXT_PATH,
            Arc::new(move |body: Vec<u8>| {
                let seq = Arc::clone(&next_seq);
                Box::pin(async move {
                    let req: SeqReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("seq/next decode: {e}")))?;
                    let reply = seq.owner_decide(&req, true);
                    serde_json::to_vec(&reply).map_err(|e| RpcError::Handler(e.to_string()))
                }) as crate::rpc::HandlerFuture
            }),
        )
        .route(
            "POST",
            PEEK_PATH,
            Arc::new(move |body: Vec<u8>| {
                let seq = Arc::clone(&peek_seq);
                Box::pin(async move {
                    let req: SeqReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("seq/peek decode: {e}")))?;
                    let reply = seq.owner_decide(&req, false);
                    serde_json::to_vec(&reply).map_err(|e| RpcError::Handler(e.to_string()))
                }) as crate::rpc::HandlerFuture
            }),
        )
        .route(
            "POST",
            RESET_PATH,
            Arc::new(move |body: Vec<u8>| {
                let seq = Arc::clone(&reset_seq);
                Box::pin(async move {
                    let req: ResetReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("seq/reset decode: {e}")))?;
                    seq.forget(req.port, req.stub_key.as_deref());
                    Ok(Vec::new())
                }) as crate::rpc::HandlerFuture
            }),
        )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::{Delivery, RESET_MAX_ATTEMPTS, deliver_reset};

    /// Pins D-57 (#514): a peer that refuses the first attempts is asked again, and the reset is
    /// reported delivered once it lands. Before this, one `call_member` was made per peer and its
    /// error was a `debug!` — a reset lost to a busy peer was lost for good.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reset_a_peer_first_refuses_is_retried_until_it_lands() {
        static ATTEMPTS: AtomicU32 = AtomicU32::new(0);
        ATTEMPTS.store(0, Ordering::SeqCst);

        let unreached = deliver_reset(vec![2], Duration::from_millis(1), |_peer| async {
            if ATTEMPTS.fetch_add(1, Ordering::SeqCst) < 2 {
                Err("shed".to_owned())
            } else {
                Ok(Delivery::Done)
            }
        })
        .await;

        assert_eq!(
            unreached,
            Vec::<crate::NodeId>::new(),
            "it landed on the third attempt"
        );
        assert_eq!(
            ATTEMPTS.load(Ordering::SeqCst),
            3,
            "two refusals, then the one that stuck"
        );
    }

    /// Pins D-57 (#514): a peer that never accepts is **named**, not silently dropped. A stale
    /// fleet-wide cursor is an operator's problem, and they can only act on it knowing which
    /// member to look at.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_peer_that_never_accepts_a_reset_is_named() {
        let unreached = deliver_reset(vec![2, 3], Duration::from_millis(1), |peer| async move {
            if peer == 3 {
                Err("down".to_owned())
            } else {
                Ok(Delivery::Done)
            }
        })
        .await;

        assert_eq!(unreached, vec![3], "only the member that never accepted it");
    }

    /// Pins D-57 (#514): a member that has left the membership is **not** retried and **not**
    /// named. It holds no cursors this reset is about, so chasing it five times and then
    /// reporting it in a fault warning would send an operator after a node that is correctly
    /// gone — the counter beside that warning is documented as meaning something is wrong.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_member_that_has_left_is_not_retried_and_not_named() {
        static ATTEMPTS: AtomicU32 = AtomicU32::new(0);
        ATTEMPTS.store(0, Ordering::SeqCst);

        let unreached = deliver_reset(vec![2], Duration::from_millis(1), |_peer| async {
            ATTEMPTS.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(Delivery::NotAMember)
        })
        .await;

        assert_eq!(unreached, Vec::<crate::NodeId>::new(), "not a fault");
        assert_eq!(
            ATTEMPTS.load(Ordering::SeqCst),
            1,
            "asked once, not retried"
        );
    }

    /// Pins D-57 (#514): attempts are bounded. A member that is simply down must not have the
    /// fan-out chasing it forever — the next membership change re-keys its cursors anyway (D-8).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reset_gives_up_after_a_bounded_number_of_attempts() {
        static ATTEMPTS: AtomicU32 = AtomicU32::new(0);
        ATTEMPTS.store(0, Ordering::SeqCst);

        let unreached = deliver_reset(vec![2], Duration::from_millis(1), |_peer| async {
            ATTEMPTS.fetch_add(1, Ordering::SeqCst);
            Err::<Delivery, _>("timeout".to_owned())
        })
        .await;

        assert_eq!(unreached, vec![2]);
        assert_eq!(
            ATTEMPTS.load(Ordering::SeqCst),
            RESET_MAX_ATTEMPTS,
            "exactly the bound, neither one short nor unbounded"
        );
    }

    /// Peers are asked concurrently, not in a sequential loop under one shared budget — the shape
    /// that let one slow member starve every peer after it (D-57).
    ///
    /// Wall-clock, with the bound placed midway between the two outcomes rather than just under
    /// the sequential one: three 300 ms deliveries take ~300 ms together and 900 ms in series, so
    /// 600 ms leaves 300 ms of slack on both sides. (`start_paused` would make this exact, but it
    /// needs tokio's `test-util` feature, and feature unification would carry that into
    /// production builds for one assertion.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn peers_are_asked_concurrently_not_one_after_another() {
        let started = std::time::Instant::now();
        let unreached = deliver_reset(vec![2, 3, 4], Duration::from_millis(1), |_peer| async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok::<_, String>(Delivery::Done)
        })
        .await;

        assert!(unreached.is_empty());
        assert!(
            started.elapsed() < Duration::from_millis(600),
            "three 300 ms deliveries run together, not in series: took {:?}",
            started.elapsed()
        );
    }
}
