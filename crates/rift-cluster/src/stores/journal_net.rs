//! Merge-on-read for the fleet request journal (issue #223, Ch.7 §merge-on-read).
//!
//! Issue #222 gave every node a writer shard whose entries carry the merge key
//! `(node_id, seq, clear_gen)`. This module is the half that makes those shards add up to one
//! answer: the pure k-way merge below, the wire types peers exchange, and — once wired — the
//! replica cache an anti-entropy pull keeps warm.
//!
//! The merge is deliberately a **free function over owned slices**, not a method on a live
//! network. Every convergence property this slice promises (identical sets on every node,
//! deterministic order, dedup, post-eviction agreement) is a property of that function alone, so
//! it is provable without a cluster — which is what lets the chaos tier assert the *distributed*
//! claims (partition honesty, dead-writer survival) instead of re-deriving the merge itself.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::journal::{ClusterJournal, ShardEntry, ShardRead};
use crate::raft::{NodeId, RaftNode};
use crate::rpc::{HandlerFuture, Router, RpcError};
use rift_cluster_base::seams::RequestJournal;

/// One shard's contribution to a merged read: whose it is, and what it held.
///
/// `pub(crate)`: only [`JournalNet::slices_for`] (also `pub(crate)`) produces these and only
/// [`merge_shards`] (also `pub(crate)`) consumes them — nothing outside this module has any
/// business naming the type.
#[derive(Debug, Clone)]
pub(crate) struct ShardSlice {
    pub node_id: NodeId,
    pub read: ShardRead,
}

/// The merged view of one port across the fleet.
///
/// Left fully `pub`, unlike [`ShardSlice`]: it is the return type of [`JournalNet::merge_read`],
/// which is `pub` and called cross-crate (the front door's `terminate_read_saved_requests`) — a
/// `pub(crate)` return type there would be a private-type-in-a-public-interface lint turned error
/// under this workspace's `-D warnings`, for a type whose fields (`entries`, `partial`) the caller
/// already reads directly.
#[derive(Debug, Clone)]
pub struct MergeOutcome {
    pub entries: Vec<ShardEntry>,
    /// At least one roster peer was unreachable or over budget, so recent entries may be
    /// missing. Renders as `Rift-Cluster-Partial: true`. Never means "a peer was omitted
    /// entirely" — that peer's replica-cache entries still merge in.
    pub partial: bool,
}

/// K-way merge of every shard's slice of one port.
///
/// Ordering is the request's own recorded timestamp, ties broken by `(node_id, seq)`. Note this
/// is *not* [`ShardEntry`]'s `recorded_at_millis`, which is a local monotonic arrival time and
/// therefore incomparable across nodes — two nodes' clocks would order the same fleet differently
/// and the acceptance criterion is that they do not.
///
/// Timestamps compare as **strings**. Upstream writes `chrono::Utc::now().to_rfc3339()`, a
/// fixed-offset UTC encoding whose lexicographic order is chronological; where it is not
/// (`0.5` vs `0.50` naming the same instant) the two entries are chronologically equal anyway, so
/// any order is admissible and the only thing that matters — that every node picks the *same* one
/// — still holds. Parsing to an instant would buy nothing and cost a dependency on every read.
///
/// Three filters run before the sort, each with a convergence reason:
/// - **dedup on `(node_id, seq, clear_gen)`** — the same entry reaches a reader twice (own shard
///   and a replica) and must appear once;
/// - **eviction floor** — an entry is dropped when its *originating* node has evicted through its
///   seq, using the highest watermark any slice reports for that node. A replica that has not yet
///   learned of an eviction would otherwise keep resurrecting entries the origin has dropped, and
///   the two nodes would disagree about the visible set forever;
/// - **clear generation** — an entry stamped with a generation older than the port's current one
///   is cleared. Pinned to 0 everywhere until #224 bumps it, so this is a no-op today; it is
///   honoured from day one so #224 is a producer change rather than a reader change.
#[must_use]
pub(crate) fn merge_shards(slices: &[ShardSlice], partial: bool) -> MergeOutcome {
    // The port's live generation is the highest any shard reports. #224 raises it through the
    // Raft log, so a lagging shard is behind, never ahead.
    let current_gen = slices
        .iter()
        .map(|slice| slice.read.clear_gen)
        .max()
        .unwrap_or(0);

    let mut entries: Vec<ShardEntry> = Vec::new();
    let mut seen: std::collections::HashSet<(NodeId, u64, u64)> = std::collections::HashSet::new();
    for slice in slices {
        for entry in &slice.read.entries {
            if entry.clear_gen < current_gen {
                continue;
            }
            if entry.seq <= evicted_through(slices, entry.node_id) {
                continue;
            }
            if seen.insert((entry.node_id, entry.seq, entry.clear_gen)) {
                entries.push(entry.clone());
            }
        }
    }

    entries.sort_by(|a, b| {
        a.request
            .timestamp
            .cmp(&b.request.timestamp)
            .then_with(|| a.node_id.cmp(&b.node_id))
            .then_with(|| a.seq.cmp(&b.seq))
    });

    MergeOutcome { entries, partial }
}

/// The highest seq any slice reports `node` as having evicted through (inclusive).
fn evicted_through(slices: &[ShardSlice], node: NodeId) -> u64 {
    slices
        .iter()
        .filter(|slice| slice.node_id == node)
        .map(|slice| slice.read.evicted_below_seq)
        .max()
        .unwrap_or(0)
}

/// `numberOfRequests` for one port: the sum of every node's G-counter slot.
///
/// A G-counter sums rather than maxes: each node only ever increments its own slot, so the fleet
/// total is the sum, and a missing peer understates it (which is what `partial` declares) rather
/// than corrupting it. The sum saturates because an overstated count is a bad answer, while a
/// wrapped one is a wrong answer that looks plausible.
///
/// Takes bare slots rather than [`ShardSlice`]s so that the summation the gate tests pin is the
/// one that actually serves requests. [`JournalNet::fleet_counts`] answers production's
/// `numberOfRequests` from `/_cluster/journal/counts` replies, which carry only slots and never
/// full shards (see its doc); a `&[ShardSlice]` signature would therefore have forced it to
/// re-implement this fold independently — which is exactly what issue #223's review caught, with
/// the resulting dead-code lint on this function as the tell.
#[must_use]
pub(crate) fn fleet_count(slots: impl IntoIterator<Item = u64>) -> u64 {
    slots.into_iter().fold(0u64, u64::saturating_add)
}

// ---------------------------------------------------------------------------
// Wire types — `/_cluster/journal/since` and `/_cluster/journal/counts`.
// ---------------------------------------------------------------------------

/// `POST /_cluster/journal/since` — "your own writer shard of `port`, after `from`".
///
/// `pub(crate)`: a wire shape between two instances of this same crate's [`journal_routes`]
/// handler and [`JournalNet`]'s own callers — nothing outside the crate dials this RPC directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SinceReq {
    pub port: u16,
    /// Exclusive: `0` asks for the whole shard.
    pub from: u64,
}

/// A peer's answer: its own shard only, never its replicas of anyone else's.
///
/// Serving only the writer's own shard is what keeps the merge acyclic — a peer that forwarded
/// its replicas would re-introduce entries the asker already has and, worse, propagate a third
/// node's stale watermark as if the peer had observed it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SinceReply {
    pub entries: Vec<WireEntry>,
    pub evicted_below_seq: u64,
    pub clear_gen: u64,
    pub count_slot: u64,
}

/// `POST /_cluster/journal/counts` — G-counter slots for a set of ports in one round trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CountsReq {
    pub ports: Vec<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CountsReply {
    /// `(port, this node's slot)`. A list rather than a map: JSON object keys are strings, and a
    /// `u16` round-tripping through one is a decode failure waiting to happen.
    pub slots: Vec<(u16, u64)>,
}

/// `POST /_cluster/journal/clear` — issue #223's transitional `DELETE savedRequests` fan-out: drop
/// this node's own shard of `port` (see [`JournalNet::clear_peers`]'s doc for the whole mechanism,
/// and why it is explicitly a placeholder for #224).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClearReq {
    pub port: u16,
}

/// A [`ShardEntry`] as it crosses the wire.
///
/// A distinct type rather than serde derives on `ShardEntry` itself: the in-memory entry carries
/// `recorded_at_millis`, a *local* monotonic arrival time whose value is meaningless on any other
/// node. Shipping it would invite a reader to order by it — the one ordering the acceptance
/// criteria forbid — so the wire simply does not carry it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WireEntry {
    pub node_id: NodeId,
    pub seq: u64,
    pub clear_gen: u64,
    pub flow_id: String,
    pub request: rift_cluster_base::seams::RecordedRequest,
}

// ---------------------------------------------------------------------------
// JournalNet — the per-node network layer.
// ---------------------------------------------------------------------------

const SINCE_PATH: &str = "/_cluster/journal/since";
const COUNTS_PATH: &str = "/_cluster/journal/counts";
const CLEAR_PATH: &str = "/_cluster/journal/clear";

/// How often a node pulls every roster peer's journal shard into its replica
/// cache. Mirrors `FlowNet`'s cadence (#126): a missed push (there is no push
/// here — every node pulls every peer, since this journal has no owner) heals
/// within one tick.
pub const DEFAULT_ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(5);

/// Budget for one whole anti-entropy tick, across every `(peer, port)` pair — the same shape as
/// [`Self::merge_read`]'s per-read budget, scoped to the tick as a whole rather than one pull: a
/// slow-but-healthy peer eats into every other pull's share of this budget instead of
/// serialising the wait, which is what lets one tick survive a fleet bigger than a couple of
/// nodes without falling behind its own 5 s cadence.
const ANTI_ENTROPY_BUDGET: Duration = Duration::from_secs(2);

/// Everything one node needs to answer and originate the two journal RPCs: the
/// local writer shard, a warm cache of every peer's, and the late-bound node
/// that supplies roster membership.
///
/// There is no owner here, unlike [`super::flow::FlowNet`] — every entry
/// belongs to whoever wrote it, and every node caches every *other* node's
/// shard, not just the ones it happens to hold. That is what lets
/// [`Self::slices_for`] answer a merge with no network hop on the read path;
/// the hop happens ahead of time, in [`Self::anti_entropy_tick`].
pub struct JournalNet {
    journal: Arc<ClusterJournal>,
    /// Every peer's shard of every port this node has pulled, keyed by whose
    /// it is and which port. [`Self::anti_entropy_tick`] is the only writer;
    /// [`Self::slices_for`] is the only reader outside of it.
    replicas: RwLock<HashMap<(NodeId, u16), ShardRead>>,
    /// Late-bound, exactly like `FlowNet`'s node slot: the journal (and this
    /// net) is built before the `RaftNode` exists. `Weak` so the anti-entropy
    /// loop can never keep the node alive past shutdown.
    node: OnceLock<Weak<RaftNode>>,
}

impl JournalNet {
    #[must_use]
    pub fn new(journal: Arc<ClusterJournal>) -> Arc<Self> {
        Arc::new(Self {
            journal,
            replicas: RwLock::new(HashMap::new()),
            node: OnceLock::new(),
        })
    }

    /// Attach the node once it exists. Binding twice is a no-op — the second
    /// caller wanted what the first one got, same contract as `FlowNet::bind`.
    pub fn bind(&self, node: &Arc<RaftNode>) {
        let _ = self.node.set(Arc::downgrade(node));
    }

    fn node(&self) -> Option<Arc<RaftNode>> {
        self.node.get()?.upgrade()
    }

    /// This node's own shard of `port` plus every cached replica shard of it —
    /// exactly the slice set [`merge_shards`] expects. Always includes the
    /// local shard, whether or not [`Self::bind`] has run yet: the journal
    /// knows its own writer id from construction (see
    /// [`ClusterJournal::node_id`]), independent of the `RaftNode` this net
    /// binds to later.
    #[must_use]
    pub(crate) fn slices_for(&self, port: u16) -> Vec<ShardSlice> {
        let mut slices = vec![ShardSlice {
            node_id: self.journal.node_id(),
            read: self.journal.read_shard_since(port, 0),
        }];
        slices.extend(
            self.replicas
                .read()
                .iter()
                .filter(|((_, replica_port), _)| *replica_port == port)
                .map(|((node_id, _), read)| ShardSlice {
                    node_id: *node_id,
                    read: read.clone(),
                }),
        );
        slices
    }

    /// Merge one peer's `since` reply into the replica cache: adopt the peer's
    /// watermark/generation/slot wholesale — there is nothing to reconcile there, because a
    /// peer's own shard is the sole authority on its own state — and fold its entries in by
    /// `seq` rather than appending them.
    ///
    /// Three things a blind `extend` got wrong (issue #223 review, B4):
    ///
    /// - **Replace-by-seq, not append.** `from` is read (in the caller) before the network
    ///   `.await`, so a GET racing the 5 s anti-entropy tick, or two concurrent GETs, can both
    ///   compute the same `from` and both land here with overlapping entries. Keying the merge
    ///   on `seq` — unique per `(peer, port)`, the peer's own monotone counter — makes a repeat
    ///   delivery a no-op instead of a duplicate that `merge_shards`'s own dedup would then have
    ///   to keep hiding forever.
    /// - **Trim to the newly adopted watermark.** The same "an entry the origin has evicted must
    ///   not linger" rule [`merge_shards`]'s eviction filter applies at merge time applies here
    ///   too, so the cache does not grow by holding entries no merge will ever surface again.
    /// - **Bound the result.** Unbounded, this cache defeats the local shard's own retention cap
    ///   by `(peers − 1)×` — every other node's shard, kept in full, forever. [`shard_cap`] is
    ///   [`ClusterJournal`]'s own retention formula, reused rather than reinvented so a cached
    ///   peer and this node's own shard agree on how much of one port is worth holding at once.
    ///
    /// [`shard_cap`]: super::journal::ClusterJournal::shard_cap
    fn merge_reply(&self, peer: NodeId, port: u16, reply: SinceReply) {
        let mut replicas = self.replicas.write();
        let cached = replicas.entry((peer, port)).or_insert_with(|| ShardRead {
            entries: Vec::new(),
            evicted_below_seq: 0,
            clear_gen: 0,
            count_slot: 0,
        });

        // Fold the existing cache and the new reply together, keyed on `seq` — a repeat entry
        // (the race above, or a peer that re-sends unchanged history) simply overwrites itself.
        let mut by_seq: HashMap<u64, ShardEntry> = cached
            .entries
            .drain(..)
            .map(|entry| (entry.seq, entry))
            .collect();
        for wire in reply.entries {
            let entry = from_wire(wire);
            by_seq.insert(entry.seq, entry);
        }

        cached.evicted_below_seq = reply.evicted_below_seq;
        cached.clear_gen = reply.clear_gen;
        cached.count_slot = reply.count_slot;

        // Drop everything the watermark just adopted already covers — resurrecting it would
        // only be undone again by `merge_shards`'s own eviction filter at read time, at the cost
        // of holding it here indefinitely.
        by_seq.retain(|&seq, _| seq > cached.evicted_below_seq);

        let mut entries: Vec<ShardEntry> = by_seq.into_values().collect();
        entries.sort_by_key(|entry| entry.seq);

        // Same cap the local shard enforces on itself, oldest (lowest seq) first — this cache is
        // a replica of exactly one other node's shard, so its retention policy should agree with
        // the original's rather than accumulate without bound.
        let cap = self.journal.shard_cap();
        if entries.len() > cap {
            let drop = entries.len() - cap;
            entries.drain(0..drop);
        }

        cached.entries = entries;
    }

    /// One pass of the anti-entropy pull (issue #223): for every port this
    /// node knows and every other roster voter, ask what changed since the
    /// highest seq already cached for that `(peer, port)`, and merge the
    /// reply in. Modeled on `FlowNet::anti_entropy_tick`'s "one bad peer must
    /// never abort the tick" contract — a failed or unreachable peer is
    /// logged and counted, and the loop moves on to the next.
    ///
    /// No separate `TrackedPeerHealth` check: `call_member`'s own client
    /// already fast-fails a peer the transport has marked unhealthy, before
    /// it touches the network, so that peer lands in the same error branch
    /// as any other unreachable one — there is no second place to skip it.
    ///
    /// Fanned out over a `JoinSet` under [`ANTI_ENTROPY_BUDGET`], exactly like
    /// [`Self::merge_read`]'s own pull (issue #223 review). Sequential and unbudgeted, the
    /// original shape cost one unreachable peer `ports × request_timeout` before the loop even
    /// reached the next peer — the caches this tick warms go cold precisely when a degraded
    /// fleet needs `merge_read` to lean on them hardest.
    pub async fn anti_entropy_tick(self: &Arc<Self>) {
        let Some(node) = self.node() else { return };
        let ports = self.journal.known_ports();
        if ports.is_empty() {
            return;
        }
        let peers: Vec<NodeId> = node
            .ring()
            .members()
            .iter()
            .copied()
            .filter(|&id| id != node.id())
            .collect();
        if peers.is_empty() {
            return;
        }

        let mut set = tokio::task::JoinSet::new();
        for &peer in &peers {
            for &port in &ports {
                let node = Arc::clone(&node);
                let from = self.replicas.read().get(&(peer, port)).map_or(0, |read| {
                    read.entries.iter().map(|e| e.seq).max().unwrap_or(0)
                });
                set.spawn(async move {
                    let body = match serde_json::to_vec(&SinceReq { port, from }) {
                        Ok(body) => body,
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "journal anti-entropy: request did not encode"
                            );
                            return (peer, port, None);
                        }
                    };
                    match node.call_member(peer, "POST", SINCE_PATH, body).await {
                        Ok(reply) => match serde_json::from_slice::<SinceReply>(&reply) {
                            Ok(reply) => (peer, port, Some(reply)),
                            Err(e) => {
                                tracing::warn!(
                                    peer,
                                    port,
                                    error = %e,
                                    "journal anti-entropy: bad reply"
                                );
                                crate::metrics::journal_peer_pull_failure(&peer.to_string());
                                (peer, port, None)
                            }
                        },
                        // Covers both an unreachable peer and one the transport's
                        // own health tracking has already given up on — the next
                        // tick is the retry either way.
                        Err(e) => {
                            tracing::debug!(
                                peer,
                                port,
                                error = %e,
                                "journal anti-entropy: peer unreachable"
                            );
                            crate::metrics::journal_peer_pull_failure(&peer.to_string());
                            (peer, port, None)
                        }
                    }
                });
            }
        }

        // Which `(peer, port)` pairs got an answer (successful or not) before the budget ran
        // out — the complement, after `abort_all`, is exactly the set the timeout itself is
        // responsible for losing, which must be counted too, not just the ones that errored.
        let mut answered: std::collections::HashSet<(NodeId, u16)> =
            std::collections::HashSet::new();
        let drained = tokio::time::timeout(ANTI_ENTROPY_BUDGET, async {
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok((peer, port, Some(reply))) => {
                        answered.insert((peer, port));
                        self.merge_reply(peer, port, reply);
                    }
                    Ok((peer, port, None)) => {
                        // Already logged and counted inside the task, at whichever level fit
                        // the failure (a bad reply is a `warn`-worthy version-skew smell; an
                        // unreachable peer is the `debug`-level expected shape of a partition).
                        answered.insert((peer, port));
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "journal anti-entropy: peer pull task panicked"
                        );
                    }
                }
            }
        })
        .await;
        if drained.is_err() {
            set.abort_all();
            for &peer in &peers {
                for &port in &ports {
                    if !answered.contains(&(peer, port)) {
                        tracing::debug!(
                            peer,
                            port,
                            "journal anti-entropy: peer pull lost to budget"
                        );
                        crate::metrics::journal_peer_pull_failure(&peer.to_string());
                    }
                }
            }
        }
    }

    /// The front door's merge-on-read entry point (issue #223): freshen `port`'s slices from
    /// every other roster voter within `budget`, then run the pure [`merge_shards`] the 15 gate
    /// tests already prove convergent. This is the only place that calls it outside those tests
    /// and the only place that records the observability they deliberately keep out of scope —
    /// `merge_shards` stays a function of its inputs alone.
    #[must_use]
    pub async fn merge_read(&self, port: u16, budget: Duration) -> MergeOutcome {
        let start = std::time::Instant::now();
        let partial = self.pull_since_budgeted(port, budget).await;
        let outcome = merge_shards(&self.slices_for(port), partial);
        crate::metrics::journal_merge_observed(start.elapsed());
        if outcome.partial {
            crate::metrics::journal_partial_read();
        }
        outcome
    }

    /// Pull `port` fresh from every other roster voter, concurrently, merging each reply into the
    /// replica cache as it lands. Bounded by `budget` **in total**, not per peer — a slow peer eats
    /// into every other peer's share of the budget rather than serialising the wait, which is what
    /// lets a 2 s budget survive a fleet bigger than a couple of nodes.
    ///
    /// Returns whether the read this call backs should be marked partial: any peer that errored,
    /// answered something unparseable, or was still outstanding when the budget ran out. A peer
    /// that misses this call keeps whatever the replica cache already held for it — `slices_for`
    /// reads that cache regardless of how this returns — so partial only ever means "possibly
    /// missing entries newer than the last successful pull," never "this peer's history vanished."
    ///
    /// Every failure is logged at `warn` with `peer`, `port` and the real `error`, and counted by
    /// [`crate::metrics::journal_peer_pull_failure`] — issue #223 review, B5. Before this, the
    /// `Err(_)` arms below discarded the error entirely, so the one path that produces a
    /// user-visible `Rift-Cluster-Partial` produced no metric and no trail: an operator could not
    /// tell a partition (heals on its own) from a `SinceReply` decode failure (version skew,
    /// which will not) from a budget that is simply too small. The budget-expiry sweep at the end
    /// closes the same gap for a peer whose task was still outstanding when `timeout` fired —
    /// aborted, not errored, but just as lost to this read.
    async fn pull_since_budgeted(&self, port: u16, budget: Duration) -> bool {
        let Some(node) = self.node() else {
            // No node bound yet: there is no roster to ask, so there is nothing to be partial
            // about — the same posture `anti_entropy_tick` takes when `self.node()` is `None`.
            return false;
        };
        let peers: Vec<NodeId> = node
            .ring()
            .members()
            .iter()
            .copied()
            .filter(|&id| id != node.id())
            .collect();
        if peers.is_empty() {
            return false;
        }

        let mut set = tokio::task::JoinSet::new();
        for peer in peers.iter().copied() {
            let node = Arc::clone(&node);
            let from = self.replicas.read().get(&(peer, port)).map_or(0, |read| {
                read.entries.iter().map(|e| e.seq).max().unwrap_or(0)
            });
            set.spawn(async move {
                let outcome = async {
                    let body =
                        serde_json::to_vec(&SinceReq { port, from }).map_err(|e| e.to_string())?;
                    let reply = node
                        .call_member(peer, "POST", SINCE_PATH, body)
                        .await
                        .map_err(|e| e.to_string())?;
                    serde_json::from_slice::<SinceReply>(&reply).map_err(|e| e.to_string())
                }
                .await;
                (peer, outcome)
            });
        }

        let mut partial = false;
        let mut answered: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
        let drained = tokio::time::timeout(budget, async {
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok((peer, Ok(reply))) => {
                        answered.insert(peer);
                        self.merge_reply(peer, port, reply);
                    }
                    Ok((peer, Err(e))) => {
                        answered.insert(peer);
                        tracing::warn!(peer, port, error = %e, "journal merge-on-read: peer pull failed");
                        crate::metrics::journal_peer_pull_failure(&peer.to_string());
                        partial = true;
                    }
                    Err(e) => {
                        tracing::warn!(port, error = %e, "journal merge-on-read: peer pull task panicked");
                        partial = true;
                    }
                }
            }
        })
        .await;
        if drained.is_err() {
            partial = true;
            set.abort_all();
            for &peer in peers.iter().filter(|peer| !answered.contains(peer)) {
                tracing::warn!(
                    peer,
                    port,
                    "journal merge-on-read: peer pull lost to budget"
                );
                crate::metrics::journal_peer_pull_failure(&peer.to_string());
            }
        }
        partial
    }

    /// `numberOfRequests` for a batch of ports (issue #223): the same budgeted, concurrent fan-out
    /// as [`Self::merge_read`], but over `/_cluster/journal/counts` — a peer's G-counter slot for
    /// every listed port in one round trip, not the full shard a merged read needs. Reuses no
    /// cache: `CountsReply` carries only slots, not entries, so folding it into `slices_for`'s
    /// `ShardRead` cache would mean fabricating the rest of the shape. The seed-from-cache and
    /// sum-with-`fleet_count`'s-semantics steps below give the identical partial-honesty contract
    /// without it.
    ///
    /// Failure handling mirrors [`Self::pull_since_budgeted`] (issue #223 review, B5): every
    /// `Err` is logged at `warn` with the real error and counted by
    /// [`crate::metrics::journal_peer_pull_failure`], and a peer whose task was still
    /// outstanding when `budget` expired is swept and counted the same way, not left silent just
    /// because it was aborted rather than errored.
    #[must_use]
    pub async fn fleet_counts(&self, ports: &[u16], budget: Duration) -> (HashMap<u16, u64>, bool) {
        // Slots are collected per port and folded through `fleet_count` at the end, rather than
        // accumulated by a second `saturating_add` written independently here: production's
        // `numberOfRequests` and the mutation-verified gate tests must exercise the *same*
        // summation, or a mutation to one is invisible to the other (issue #223 review, B7).
        // Seeded with this node's own slot; peers' slots are appended below.
        let mut per_port: HashMap<u16, Vec<u64>> = ports
            .iter()
            .map(|&port| (port, vec![self.journal.count(port)]))
            .collect();
        let totals = |per_port: &HashMap<u16, Vec<u64>>| -> HashMap<u16, u64> {
            per_port
                .iter()
                .map(|(&port, slots)| (port, fleet_count(slots.iter().copied())))
                .collect()
        };
        let Some(node) = self.node() else {
            return (totals(&per_port), false);
        };
        let peers: Vec<NodeId> = node
            .ring()
            .members()
            .iter()
            .copied()
            .filter(|&id| id != node.id())
            .collect();
        if peers.is_empty() {
            return (totals(&per_port), false);
        }

        // Seeded from the replica cache first — anti-entropy's last known slot — so a peer that
        // misses this call's budget still contributes what is known rather than vanishing from the
        // sum: the same "partial never means omitted" contract `merge_read` upholds for entries.
        let mut slots: HashMap<(NodeId, u16), u64> = HashMap::new();
        {
            let replicas = self.replicas.read();
            for &peer in &peers {
                for &port in ports {
                    if let Some(cached) = replicas.get(&(peer, port)) {
                        slots.insert((peer, port), cached.count_slot);
                    }
                }
            }
        }

        let mut set = tokio::task::JoinSet::new();
        for peer in peers.iter().copied() {
            let node = Arc::clone(&node);
            let req_ports = ports.to_vec();
            set.spawn(async move {
                let outcome = async {
                    let body = serde_json::to_vec(&CountsReq { ports: req_ports })
                        .map_err(|e| e.to_string())?;
                    let reply = node
                        .call_member(peer, "POST", COUNTS_PATH, body)
                        .await
                        .map_err(|e| e.to_string())?;
                    serde_json::from_slice::<CountsReply>(&reply).map_err(|e| e.to_string())
                }
                .await;
                (peer, outcome)
            });
        }

        let mut partial = false;
        let mut answered: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
        let drained = tokio::time::timeout(budget, async {
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok((peer, Ok(reply))) => {
                        answered.insert(peer);
                        for (port, slot) in reply.slots {
                            slots.insert((peer, port), slot);
                        }
                    }
                    Ok((peer, Err(e))) => {
                        answered.insert(peer);
                        tracing::warn!(peer, error = %e, "journal counts: peer pull failed");
                        crate::metrics::journal_peer_pull_failure(&peer.to_string());
                        partial = true;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "journal counts: peer pull task panicked");
                        partial = true;
                    }
                }
            }
        })
        .await;
        if drained.is_err() {
            partial = true;
            set.abort_all();
            for &peer in peers.iter().filter(|peer| !answered.contains(peer)) {
                tracing::warn!(peer, "journal counts: peer pull lost to budget");
                crate::metrics::journal_peer_pull_failure(&peer.to_string());
            }
        }

        for ((_, port), slot) in slots {
            if let Some(port_slots) = per_port.get_mut(&port) {
                port_slots.push(slot);
            }
        }
        (totals(&per_port), partial)
    }

    /// `DELETE savedRequests`'s fan-out half (issue #223 item 4): tell every other roster voter to
    /// drop its own shard of `port`, best-effort. The caller owns the *local* clear — the front
    /// reuses its existing proxy to the local engine for that, since #222 made that engine's
    /// journal this same `ClusterJournal` — this only reaches the others.
    ///
    /// **Explicitly transitional.** #224 replaces this whole mechanism with a Raft-committed clear
    /// that bumps `clear_gen` (the field `merge_shards` already honours, pinned at 0 until then) so
    /// a clear converges by consensus instead of a best-effort broadcast a partitioned peer can
    /// simply miss forever. Two simplifications fall out of that and stay until #224 lands: a peer
    /// missed by the fan-out keeps the deleted entries in its own local shard and in this node's
    /// replica cache of it (which is exactly what the caller's `Rift-Cluster-Partial` reports), and
    /// the wire clear is unconditionally a *full* clear — the `?match=` narrowing a local clear
    /// honours is not propagated, because there is nowhere on this wire to carry a match predicate.
    ///
    /// Returns whether any peer was unreachable or missed `budget`.
    ///
    /// Failure handling mirrors [`Self::pull_since_budgeted`] (issue #223 review, B5): a peer
    /// that did not confirm is logged at `warn` with the real error and counted by
    /// [`crate::metrics::journal_peer_pull_failure`] rather than silently folded into `partial`,
    /// and the budget-expiry sweep counts a peer whose confirmation was still outstanding when
    /// `budget` ran out the same way.
    #[must_use]
    pub async fn clear_peers(&self, port: u16, budget: Duration) -> bool {
        let Some(node) = self.node() else {
            return false;
        };
        let peers: Vec<NodeId> = node
            .ring()
            .members()
            .iter()
            .copied()
            .filter(|&id| id != node.id())
            .collect();
        if peers.is_empty() {
            return false;
        }

        let mut set = tokio::task::JoinSet::new();
        for peer in peers.iter().copied() {
            let node = Arc::clone(&node);
            set.spawn(async move {
                let outcome = async {
                    let body = serde_json::to_vec(&ClearReq { port }).map_err(|e| e.to_string())?;
                    node.call_member(peer, "POST", CLEAR_PATH, body)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok::<(), String>(())
                }
                .await;
                (peer, outcome)
            });
        }

        let mut partial = false;
        let mut answered: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
        let drained = tokio::time::timeout(budget, async {
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok((peer, Ok(()))) => {
                        answered.insert(peer);
                        // Best-effort mirror of the fresh state: this peer's entries are gone, so
                        // this node's cache of it should stop claiming otherwise rather than wait
                        // out a full anti-entropy interval to notice independently.
                        self.replicas.write().remove(&(peer, port));
                    }
                    Ok((peer, Err(e))) => {
                        answered.insert(peer);
                        tracing::warn!(peer, port, error = %e, "journal clear fan-out: peer did not confirm");
                        crate::metrics::journal_peer_pull_failure(&peer.to_string());
                        partial = true;
                    }
                    Err(e) => {
                        tracing::warn!(port, error = %e, "journal clear fan-out: peer task panicked");
                        partial = true;
                    }
                }
            }
        })
        .await;
        if drained.is_err() {
            partial = true;
            set.abort_all();
            for &peer in peers.iter().filter(|peer| !answered.contains(peer)) {
                tracing::warn!(peer, port, "journal clear fan-out: peer lost to budget");
                crate::metrics::journal_peer_pull_failure(&peer.to_string());
            }
        }
        partial
    }
}

/// Start the anti-entropy loop on `handle`. Holds only a `Weak<JournalNet>` —
/// the same lifecycle as `FlowNet::bind`'s spawn site — so the loop can never
/// keep the net (and transitively the node) alive past shutdown; it exits the
/// tick it discovers the net is gone.
pub fn spawn_anti_entropy(
    net: &Arc<JournalNet>,
    handle: &tokio::runtime::Handle,
    interval: Duration,
) {
    let net = Arc::downgrade(net);
    handle.spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let Some(net) = net.upgrade() else { break };
            net.anti_entropy_tick().await;
        }
    });
}

fn to_wire(entry: &ShardEntry) -> WireEntry {
    WireEntry {
        node_id: entry.node_id,
        seq: entry.seq,
        clear_gen: entry.clear_gen,
        flow_id: entry.flow_id.clone(),
        request: entry.request.clone(),
    }
}

/// A [`WireEntry`] landing in the replica cache. `recorded_at_millis` is
/// stamped `0`: it is a local arrival clock (see [`ShardEntry`]'s doc) that
/// the wire form never carried in the first place, and the replica cache
/// never age-evicts on it — only the *owning* node's own shard does.
fn from_wire(entry: WireEntry) -> ShardEntry {
    ShardEntry {
        node_id: entry.node_id,
        seq: entry.seq,
        clear_gen: entry.clear_gen,
        flow_id: entry.flow_id,
        request: entry.request,
        recorded_at_millis: 0,
    }
}

/// The wire surface: three POST routes on the cluster port, matching
/// [`super::flow::flow_routes`]'s shape.
#[must_use]
pub fn journal_routes(net: Arc<JournalNet>) -> Router {
    let since_net = Arc::clone(&net);
    let counts_net = Arc::clone(&net);
    let clear_net = net;

    Router::new()
        .route(
            "POST",
            SINCE_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&since_net);
                Box::pin(async move {
                    let req: SinceReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("journal/since decode: {e}")))?;
                    // This node's own writer shard only, never the replica
                    // cache — forwarding a replica would let a peer relay a
                    // third node's watermark as if it had observed it itself,
                    // which is exactly what `SinceReply`'s contract forbids.
                    let read = net.journal.read_shard_since(req.port, req.from);
                    let reply = SinceReply {
                        entries: read.entries.iter().map(to_wire).collect(),
                        evicted_below_seq: read.evicted_below_seq,
                        clear_gen: read.clear_gen,
                        count_slot: read.count_slot,
                    };
                    serde_json::to_vec(&reply).map_err(|e| RpcError::Handler(e.to_string()))
                })
            }),
        )
        .route(
            "POST",
            COUNTS_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&counts_net);
                Box::pin(async move {
                    let req: CountsReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("journal/counts decode: {e}")))?;
                    // `RequestJournal::count`, not `read_shard_since(port,
                    // u64::MAX)`: both skip cloning the shard (the `since`
                    // filter admits nothing at `u64::MAX`), but `count` reads
                    // the atomic slot directly instead of walking the deque
                    // to confirm that.
                    let slots = req
                        .ports
                        .iter()
                        .map(|&port| (port, net.journal.count(port)))
                        .collect();
                    serde_json::to_vec(&CountsReply { slots })
                        .map_err(|e| RpcError::Handler(e.to_string()))
                })
            }),
        )
        .route(
            "POST",
            CLEAR_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&clear_net);
                Box::pin(async move {
                    let req: ClearReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("journal/clear decode: {e}")))?;
                    // `RequestJournal::clear`, the same trait method upstream's own `DELETE
                    // savedRequests` handler calls locally — this route just lets a peer trigger it
                    // remotely, transitionally, until #224 replaces the whole mechanism.
                    net.journal
                        .clear(req.port)
                        .map_err(|e| RpcError::Handler(e.to_string()))?;
                    // Half of the fix is not enough (issue #223 review, B2): `ClusterJournal::clear`
                    // only empties *this* node's own writer shard of `port` — it leaves `seq` and
                    // `evicted_below_seq` untouched, so it advances no watermark `merge_shards`'
                    // eviction filter could use to drop a replica's pre-clear copy. Left alone, this
                    // node's own `replicas` cache of every *other* peer's shard of `port` — populated
                    // by earlier anti-entropy pulls — would keep resurrecting entries those peers are
                    // clearing for the identical reason, forever, with no `Rift-Cluster-Partial` to
                    // show for it (every peer "succeeded"). The initiator already forgets a
                    // successfully-cleared peer's cache entry in `clear_peers`; this is that same
                    // forgetting on the *receiving* side, for every peer this node has cached, not
                    // just the initiator — the fan-out reaches every fleet member for the same clear,
                    // so whichever node a cached copy belonged to is clearing its own shard for the
                    // same reason, and the next anti-entropy tick refreshes with the post-clear
                    // (empty) truth rather than the stale one this drops.
                    net.replicas
                        .write()
                        .retain(|(_, cached_port), _| *cached_port != req.port);
                    Ok(Vec::new())
                })
            }),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rift_cluster_base::seams::{RecordedRequest, ResponseMode};

    fn req_at(timestamp: &str, path: &str) -> RecordedRequest {
        RecordedRequest {
            mode: ResponseMode::Text,
            request_from: "t".into(),
            method: "GET".into(),
            path: path.into(),
            query: Default::default(),
            headers: Default::default(),
            body: None,
            timestamp: timestamp.into(),
            match_outcome: None,
        }
    }

    fn entry_at(
        node_id: NodeId,
        seq: u64,
        clear_gen: u64,
        request: RecordedRequest,
        recorded_at_millis: u64,
    ) -> ShardEntry {
        ShardEntry {
            node_id,
            seq,
            clear_gen,
            flow_id: String::new(),
            request,
            recorded_at_millis,
        }
    }

    fn entry(node_id: NodeId, seq: u64, timestamp: &str) -> ShardEntry {
        entry_at(node_id, seq, 0, req_at(timestamp, &format!("/p{seq}")), 0)
    }

    fn entry_gen(node_id: NodeId, seq: u64, clear_gen: u64, timestamp: &str) -> ShardEntry {
        entry_at(
            node_id,
            seq,
            clear_gen,
            req_at(timestamp, &format!("/p{seq}")),
            0,
        )
    }

    fn slice(node_id: NodeId, entries: Vec<ShardEntry>) -> ShardSlice {
        ShardSlice {
            node_id,
            read: ShardRead {
                count_slot: entries.len() as u64,
                entries,
                evicted_below_seq: 0,
                clear_gen: 0,
            },
        }
    }

    fn paths(outcome: &MergeOutcome) -> Vec<&str> {
        outcome
            .entries
            .iter()
            .map(|e| e.request.path.as_str())
            .collect()
    }

    // ---- AC1: every node returns the identical full set, in the identical order -------

    /// The merge is a pure function of the slice set, so feeding the same shards in any peer
    /// order must produce byte-identical output. This is the property behind "reads from each
    /// node return the identical full set" — without it, three nodes answer three orders.
    #[test]
    fn merged_order_is_independent_of_the_order_peers_answered_in() {
        let a = slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")]);
        let b = slice(2, vec![entry(2, 1, "2026-01-01T00:00:02Z")]);
        let c = slice(3, vec![entry(3, 1, "2026-01-01T00:00:00Z")]);

        let one = merge_shards(&[a.clone(), b.clone(), c.clone()], false);
        let two = merge_shards(&[c, a, b], false);

        assert_eq!(paths(&one), paths(&two));
        assert_eq!(
            paths(&one),
            vec!["/p1", "/p1", "/p1"],
            "all three entries survive"
        );
        let nodes: Vec<NodeId> = one.entries.iter().map(|e| e.node_id).collect();
        assert_eq!(nodes, vec![3, 1, 2], "ordered by recorded timestamp");
    }

    /// Equal timestamps are broken by `(node_id, seq)` — deterministic, and the same on every
    /// node, which is the only thing convergence actually requires.
    #[test]
    fn equal_timestamps_break_deterministically_by_node_then_seq() {
        let same = "2026-01-01T00:00:00Z";
        let a = slice(2, vec![entry(2, 2, same), entry(2, 1, same)]);
        let b = slice(1, vec![entry(1, 5, same)]);

        let merged = merge_shards(&[a, b], false);
        let key: Vec<(NodeId, u64)> = merged.entries.iter().map(|e| (e.node_id, e.seq)).collect();
        assert_eq!(key, vec![(1, 5), (2, 1), (2, 2)]);
    }

    /// Ordering must not fall back to `recorded_at_millis`: it is a local arrival clock, so a
    /// node whose clock is far ahead would sort its own entries last on itself and first
    /// elsewhere. Same entries, wildly different local stamps, same order.
    #[test]
    fn local_arrival_time_does_not_influence_the_merged_order() {
        let early_local = entry_at(1, 1, 0, req_at("2026-01-01T00:00:09Z", "/late"), 0);
        let late_local = entry_at(2, 1, 0, req_at("2026-01-01T00:00:01Z", "/early"), 9_000_000);

        let merged = merge_shards(
            &[slice(1, vec![early_local]), slice(2, vec![late_local])],
            false,
        );
        assert_eq!(paths(&merged), vec!["/early", "/late"]);
    }

    // ---- AC1: dedup on the merge key -------------------------------------------------

    /// The same entry arrives twice — once from the origin's own shard, once from a replica
    /// another node forwarded. It must appear once.
    #[test]
    fn an_entry_reaching_the_merge_twice_appears_once() {
        let original = entry(1, 1, "2026-01-01T00:00:00Z");
        let replica_copy = original.clone();

        let merged = merge_shards(
            &[slice(1, vec![original]), slice(1, vec![replica_copy])],
            false,
        );
        assert_eq!(merged.entries.len(), 1);
    }

    /// Dedup is keyed on all three of `(node_id, seq, clear_gen)`. Same seq from two different
    /// writers is two different requests and both must survive — a dedup on `seq` alone would
    /// silently drop one node's traffic, which is the failure this key exists to prevent.
    #[test]
    fn the_same_seq_from_two_writers_is_two_entries() {
        let merged = merge_shards(
            &[
                slice(1, vec![entry(1, 7, "2026-01-01T00:00:00Z")]),
                slice(2, vec![entry(2, 7, "2026-01-01T00:00:01Z")]),
            ],
            false,
        );
        assert_eq!(merged.entries.len(), 2);
    }

    // ---- AC1: post-eviction convergence ----------------------------------------------

    /// A replica still holding entries the origin has evicted must not resurrect them: the
    /// origin's watermark is authoritative for the origin's own seqs, and every node applies it,
    /// so all of them converge on the same visible set.
    #[test]
    fn a_replica_does_not_resurrect_what_the_origin_evicted() {
        let mut origin = slice(1, vec![entry(1, 3, "2026-01-01T00:00:03Z")]);
        origin.read.evicted_below_seq = 2; // seqs 1 and 2 are gone, inclusive

        let stale_replica = slice(
            1,
            vec![
                entry(1, 1, "2026-01-01T00:00:01Z"),
                entry(1, 2, "2026-01-01T00:00:02Z"),
                entry(1, 3, "2026-01-01T00:00:03Z"),
            ],
        );

        let merged = merge_shards(&[origin, stale_replica], false);
        assert_eq!(
            paths(&merged),
            vec!["/p3"],
            "only the un-evicted entry survives"
        );
    }

    /// The floor is per-originating-node, not global: node 2 evicting through seq 5 must not
    /// delete node 1's seq 1..=5.
    #[test]
    fn the_eviction_floor_is_scoped_to_the_node_that_evicted() {
        let mut evicted = slice(2, vec![entry(2, 6, "2026-01-01T00:00:06Z")]);
        evicted.read.evicted_below_seq = 5;
        let untouched = slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")]);

        let merged = merge_shards(&[evicted, untouched], false);
        assert_eq!(paths(&merged), vec!["/p1", "/p6"]);
    }

    // ---- AC4 (forward-compat): clear generations -------------------------------------

    /// #224 bumps the generation; the reader must already honour it, or #224 becomes a change to
    /// every reader instead of a change to the writer.
    #[test]
    fn entries_from_a_superseded_clear_generation_are_dropped() {
        let mut cleared = slice(1, vec![entry_gen(1, 1, 0, "2026-01-01T00:00:01Z")]);
        cleared.read.clear_gen = 1;
        let mut current = slice(2, vec![entry_gen(2, 1, 1, "2026-01-01T00:00:02Z")]);
        current.read.clear_gen = 1;

        let merged = merge_shards(&[cleared, current], false);
        let nodes: Vec<NodeId> = merged.entries.iter().map(|e| e.node_id).collect();
        assert_eq!(nodes, vec![2], "the pre-clear entry is gone");
    }

    /// Today every shard pins generation 0, and that must merge exactly as it did before the
    /// generation filter existed — the no-op guarantee the issue states explicitly.
    #[test]
    fn generation_zero_everywhere_drops_nothing() {
        let merged = merge_shards(
            &[
                slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")]),
                slice(2, vec![entry(2, 1, "2026-01-01T00:00:02Z")]),
            ],
            false,
        );
        assert_eq!(merged.entries.len(), 2);
    }

    // ---- AC1/AC3: partial honesty ----------------------------------------------------

    /// Partial is declared by the caller (it alone knows a peer timed out) and carried through
    /// untouched. An all-healthy merge must never be stamped — the Ch.12 strict-mode gate
    /// asserts the header's *absence*, so a merge that stamped defensively would fail it.
    #[test]
    fn a_complete_merge_is_not_stamped_partial() {
        let merged = merge_shards(
            &[slice(1, vec![entry(1, 1, "2026-01-01T00:00:00Z")])],
            false,
        );
        assert!(!merged.partial);
    }

    /// Partial means "possibly missing recent entries", never "that peer was omitted": whatever
    /// the replica cache held for the unreachable peer still merges in.
    #[test]
    fn a_partial_merge_still_returns_the_unreachable_peers_cached_entries() {
        let live = slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")]);
        let cached_from_dead_peer = slice(2, vec![entry(2, 1, "2026-01-01T00:00:02Z")]);

        let merged = merge_shards(&[live, cached_from_dead_peer], true);
        assert!(merged.partial);
        assert_eq!(merged.entries.len(), 2, "the dead peer's entries survive");
    }

    // ---- AC1: fleet numberOfRequests -------------------------------------------------

    /// The G-counter sums; it does not max and does not count entries. Slots exceed retained
    /// entries as soon as anything is evicted, and `numberOfRequests` must keep reporting
    /// everything ever recorded.
    #[test]
    fn fleet_count_sums_every_nodes_slot() {
        let mut a = slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")]);
        a.read.count_slot = 900;
        let mut b = slice(2, vec![]);
        b.read.count_slot = 100;

        assert_eq!(fleet_count([a, b].iter().map(|s| s.read.count_slot)), 1_000);
    }

    /// A pathological slot pair must not wrap to a small number — an overstated count is a bad
    /// answer, a wrapped one is a wrong answer that looks plausible.
    #[test]
    fn fleet_count_saturates_rather_than_wrapping() {
        let mut a = slice(1, vec![]);
        a.read.count_slot = u64::MAX;
        let mut b = slice(2, vec![]);
        b.read.count_slot = 5;

        assert_eq!(
            fleet_count([a, b].iter().map(|s| s.read.count_slot)),
            u64::MAX
        );
    }

    // ---- wire contract ---------------------------------------------------------------

    /// The wire entry must round-trip, and must **not** carry `recorded_at_millis` — a local
    /// arrival clock has no meaning on the receiving node.
    #[test]
    fn the_wire_entry_round_trips_and_omits_local_arrival_time() {
        let wire = WireEntry {
            node_id: 2,
            seq: 9,
            clear_gen: 0,
            flow_id: "f".into(),
            request: req_at("2026-01-01T00:00:00Z", "/x"),
        };
        let json = serde_json::to_string(&wire).expect("WireEntry serializes");
        assert!(
            !json.contains("recorded_at") && !json.contains("recordedAt"),
            "local arrival time must not cross the wire: {json}"
        );
        let back: WireEntry = serde_json::from_str(&json).expect("WireEntry round-trips");
        assert_eq!((back.node_id, back.seq, back.clear_gen), (2, 9, 0));
        assert_eq!(back.request.path, "/x");
    }

    /// Port-keyed counts travel as pairs, not as a JSON object — object keys are strings and a
    /// `u16` laundered through one is a decode failure waiting to happen.
    #[test]
    fn counts_reply_round_trips_port_keys_as_integers() {
        let reply = CountsReply {
            slots: vec![(4545, 12), (8080, 0)],
        };
        let json = serde_json::to_string(&reply).expect("CountsReply serializes");
        let back: CountsReply = serde_json::from_str(&json).expect("CountsReply round-trips");
        assert_eq!(back.slots, vec![(4545, 12), (8080, 0)]);
    }
}
