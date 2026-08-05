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

use super::journal::{ClusterJournal, JournalCursor, ShardEntry, ShardRead};
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
    /// This answer may be missing entries, for either of two reasons. Renders as
    /// `Rift-Cluster-Partial: true`.
    ///
    /// * **Reachability** — a roster peer was unreachable, unparseable or over budget, so
    ///   entries newer than the last successful pull may be missing.
    /// * **Content** (issue #349) — this node is a crash-restarted writer, and a peer still
    ///   caches entries of *our own* shard that we lost with the process. Those entries are
    ///   gone here for good, not merely late.
    ///
    /// Neither ever means "a peer was omitted entirely" — that peer's replica-cache entries
    /// still merge in.
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
/// Four filters run before the sort, each with a convergence reason:
/// - **dedup on `(node_id, seq, clear_gen)`** — the same entry reaches a reader twice (own shard
///   and a replica) and must appear once;
/// - **eviction floor** — an entry is dropped when its *originating* node has evicted through its
///   seq, using the highest watermark any slice reports for that node. A replica that has not yet
///   learned of an eviction would otherwise keep resurrecting entries the origin has dropped, and
///   the two nodes would disagree about the visible set forever;
/// - **port clear generation** — an entry stamped with a `clear_gen` older than the port's
///   current one (the highest any slice reports) is dropped;
/// - **space clear generation** — independently, an entry is dropped when its `space_gen` is
///   older than the highest generation any slice reports for that entry's `flow_id`. Built as a
///   fleet-wide `HashMap<space, max generation>` rather than compared per-slice: a generation
///   known to only one peer must still clear every other peer's entries for that space (#224),
///   the same reason the port threshold above is a fleet-wide max rather than a per-slice one.
///
/// The two generations are independent components, not one collapsed stamp, and the port one
/// always wins: it is checked first and short-circuits, so a port-wide clear drops an entry
/// even when that entry's `space_gen` is numerically higher than the space threshold (an
/// earlier scoped teardown of the same space, before the wider clear). Folding them into a
/// single `max(port_gen, space_gen)` stamp would defeat exactly that case.
#[must_use]
pub(crate) fn merge_shards(slices: &[ShardSlice], partial: bool) -> MergeOutcome {
    // The port's live generation is the highest any shard reports. #224 raises it through the
    // Raft log, so a lagging shard is behind, never ahead.
    let current_gen = slices
        .iter()
        .map(|slice| slice.read.clear_gen)
        .max()
        .unwrap_or(0);

    // Same idea, per space: the generation a merge honours for `flow_id` is the highest any
    // slice reports for it, not just what the entry's own origin has learned.
    let mut space_gen_floor: HashMap<&str, u64> = HashMap::new();
    for slice in slices {
        for (space, generation) in &slice.read.space_gens {
            space_gen_floor
                .entry(space.as_str())
                .and_modify(|floor| *floor = (*floor).max(*generation))
                .or_insert(*generation);
        }
    }

    let mut entries: Vec<ShardEntry> = Vec::new();
    let mut seen: std::collections::HashSet<(NodeId, u64, u64)> = std::collections::HashSet::new();
    for slice in slices {
        for entry in &slice.read.entries {
            if entry.clear_gen < current_gen {
                continue;
            }
            let space_floor = space_gen_floor
                .get(entry.flow_id.as_str())
                .copied()
                .unwrap_or(0);
            if entry.space_gen < space_floor {
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

/// One page of a merged cursor walk (issue #225): [`merge_shards`]'s answer, narrowed to what a
/// reader holding `cursor` has not already consumed, plus the token that fetches the next page.
///
/// `pub` for the same reason [`MergeOutcome`] is — it is [`JournalNet::merge_read_since`]'s
/// return type, and that is called from the front door in another crate.
#[derive(Debug, Clone)]
pub struct MergeSince {
    pub entries: Vec<ShardEntry>,
    /// Carried through from [`MergeOutcome::partial`] unchanged: a degraded fleet is orthogonal
    /// to where the reader is in the walk.
    pub partial: bool,
    /// The position to present for the next page. Never regresses below the cursor it was
    /// derived from, in any component — see [`merge_shards_since`].
    pub next: JournalCursor,
    /// At least one shard's presented position predated that shard's eviction watermark: entries
    /// the reader had not yet seen were dropped by retention before it reached them. Renders as
    /// `x-rift-truncated: true`, the same meaning upstream gives the header.
    pub truncated: bool,
}

/// [`merge_shards`], resumed from a vector cursor — the merged read a `?since=` token addresses.
///
/// The merge itself is unchanged and still does all the convergence work; this adds exactly
/// three things on top of it, each a property a walk cannot be correct without.
///
/// **Which entries.** An entry is withheld when its own shard's position in the cursor is at or
/// past its `seq`. Filtering per shard is the entire point of a *vector* cursor: one scalar
/// cannot say "I have seen node 1 through 40 and node 2 through 3", and a merge that orders by
/// timestamp interleaves shards arbitrarily, so any single index is a position in no shard in
/// particular. Per-shard filtering is what makes the walk gapless and duplicate-free per shard
/// even as the merged *order* shifts under it.
///
/// **Where the next page starts.** A shard advances to the highest of three values: the position
/// the reader presented, that shard's eviction watermark, and the highest `seq` the shard
/// currently holds. Taking the maximum rather than "the highest seq actually emitted" is what
/// stops the walk re-scanning entries it can never serve — one dropped by a clear generation
/// (#224) or by eviction is gone permanently, so a cursor that stayed below it would re-examine
/// and re-reject the same range on every page forever. Taking it *with* the presented position
/// is what makes the token monotone: a shard whose slice has gone backwards (a replica cache
/// that lost entries, a peer rolled back) never rewinds a reader that was already ahead of it.
///
/// **Membership.** A shard in the cursor with no slice this round keeps its position untouched —
/// `next` starts as a copy of the presented `pos`, so a node that is dead, partitioned, or lost
/// to the pull budget freezes rather than disappearing. Dropping it would rewind it to 0 and
/// replay its whole history the moment it returned. A shard with a slice and no cursor entry is
/// the mirror case: absent reads as 0, so a joining node enters the walk at the beginning and
/// none of its entries are skipped as "already seen".
///
/// `truncated` is set when a presented position is strictly below that shard's watermark.
/// `evicted_below_seq` is inclusive — that seq itself is gone — so a reader *at* the watermark
/// has seen everything eviction removed and is not truncated, while one below it has a real
/// hole. Both directions of that boundary matter: too eager and the bit cries wolf on every
/// read, too lax and it stays silent about data the reader will never be shown.
///
/// **`cursor: None` is a baseline read, and is not the same thing as a cursor whose positions
/// are all 0.** A baseline read is a snapshot of everything still retained, so by definition it
/// has no hole and is *never* truncated; an explicit position of 0 is a reader claiming to have
/// consumed nothing, who therefore *has* missed whatever eviction removed. Upstream draws
/// exactly this line (`since.is_some_and(..)` in `rift-mock-core`'s journal), so does this
/// crate's own single-node path, and
/// [`super::journal::ClusterJournal`]'s `cursor_since_zero_differs_from_baseline_only_in_truncation`
/// pins it. Collapsing the two here would make every uncursored read of an evicting port claim
/// truncation forever — the header crying wolf on the most common read there is.
#[must_use]
pub(crate) fn merge_shards_since(
    slices: &[ShardSlice],
    partial: bool,
    cursor: Option<&JournalCursor>,
) -> MergeSince {
    let position_of = |node: NodeId| {
        cursor
            .and_then(|cursor| cursor.pos.get(&node).copied())
            .unwrap_or(0)
    };

    let entries = merge_shards(slices, partial)
        .entries
        .into_iter()
        .filter(|entry| entry.seq > position_of(entry.node_id))
        .collect();

    let mut pos = cursor.map(|cursor| cursor.pos.clone()).unwrap_or_default();
    let mut truncated = false;
    for slice in slices {
        let evicted = evicted_through(slices, slice.node_id);
        let held = slice.read.entries.iter().map(|e| e.seq).max().unwrap_or(0);
        let position = position_of(slice.node_id);
        // `cursor.is_some()`: a baseline read is a snapshot and cannot have a hole, however
        // much this shard has evicted. See this function's doc.
        if cursor.is_some() && position < evicted {
            truncated = true;
        }
        // Folded with `max` rather than `insert` for the same reason `evicted_through` maxes:
        // two slices can name the same node, and the later one must not be able to lower it.
        let advanced = position.max(evicted).max(held);
        pos.entry(slice.node_id)
            .and_modify(|current| *current = (*current).max(advanced))
            .or_insert(advanced);
    }

    MergeSince {
        entries,
        partial,
        next: JournalCursor {
            // Monotone like the positions: never below what the caller presented, so a node
            // that has not yet applied a clear cannot rewind a reader's token by reporting the
            // older generation.
            generation: slices
                .iter()
                .map(|slice| slice.read.clear_gen)
                .max()
                .unwrap_or(0)
                .max(cursor.map_or(0, |cursor| cursor.generation)),
            pos,
        },
        truncated,
    }
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

/// `numberOfRequests` for one port: the sum of every node's G-counter slot **at the port's
/// current clear generation** (issue #224).
///
/// Each item is `(slot, clear_gen)` — a node's count alongside the port generation it was
/// reported at. The fleet's current generation is the highest any item reports, exactly the
/// rule [`merge_shards`] uses for its own `current_gen`, and a slot reported at an older
/// generation is dropped from the sum rather than added to it.
///
/// That exclusion, not a zero, is the reader's half of the invariant [`merge_shards`]'s port
/// filter already gives the entries: [`super::journal::ClusterJournal::zero_count`] zeroes a
/// node's *own* slot the instant *that node* applies the clear, but a peer that has not yet
/// applied it still reports its pre-clear slot until its own apply catches up. Summing that
/// stale slot in the meantime would answer a non-zero COUNT for the exact generation whose
/// ENTRIES the merge has already emptied — the two halves of one response disagreeing about
/// whether the clear happened yet. Dropping the slot instead keeps a reader's COUNT and
/// ENTRIES answering the same generation the moment the reader itself has learned of the bump,
/// with no coordination on the write side and no waiting for the laggard to catch up.
///
/// Only the **port** generation gates the sum. A `(slot, clear_gen)` pair never carries a
/// `space_gen` — there is no per-space slot to begin with — so a space-scoped clear, which
/// deliberately leaves `numberOfRequests` untouched (`clear_flow`/`retain`'s existing
/// contract, matched by the apply path's own `space.is_none()` guard around `zero_count`), has
/// nothing here that could affect it.
///
/// A G-counter sums rather than maxes: each node only ever increments its own slot, so the fleet
/// total is the sum, and a missing peer understates it (which is what `partial` declares) rather
/// than corrupting it. The sum saturates because an overstated count is a bad answer, while a
/// wrapped one is a wrong answer that looks plausible.
///
/// Takes `(slot, clear_gen)` pairs rather than [`ShardSlice`]s so that the summation the gate
/// tests pin is the one that actually serves requests. [`JournalNet::fleet_counts`] answers
/// production's `numberOfRequests` from `/_cluster/journal/counts` replies, which carry only
/// `(port, slot, clear_gen)` triples and never full shards (see [`CountsReply`]'s doc); a
/// `&[ShardSlice]` signature would therefore have forced it to re-implement this fold
/// independently — which is exactly what issue #223's review caught, with the resulting
/// dead-code lint on this function as the tell. A caller that already holds full
/// [`ShardSlice`]s (the gate tests below) maps `.read.count_slot` / `.read.clear_gen` into the
/// same pair rather than inventing a parallel fold of its own.
#[must_use]
pub(crate) fn fleet_count(slots: impl IntoIterator<Item = (u64, u64)>) -> u64 {
    let slots: Vec<(u64, u64)> = slots.into_iter().collect();
    let current_gen = slots
        .iter()
        .map(|&(_, generation)| generation)
        .max()
        .unwrap_or(0);
    slots
        .into_iter()
        .filter(|&(_, generation)| generation >= current_gen)
        .fold(0u64, |sum, (slot, _)| sum.saturating_add(slot))
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
    /// Who is asking, so the responder can answer [`SinceReply::asker_cached_min`] (issue #349).
    ///
    /// `#[serde(default)]` for fleet skew: an older node omits it and decodes to node 0, which is
    /// not a real node id, so the responder finds no cache for it and reports "nothing" — the
    /// pre-#349 behaviour exactly. A decode failure is already counted as a pull failure, so the
    /// skew path was load-bearing before this field and is unchanged by it.
    #[serde(default)]
    pub asker: NodeId,
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
    pub space_gens: Vec<(String, u64)>,
    pub count_slot: u64,
    /// The **lowest** seq this responder still caches belonging to the ASKER's shard of this
    /// port; `0` when it caches none (seqs are 1-based). Issue #349.
    ///
    /// This is the one datum a restarted writer needs to know it is answering short. It does not
    /// violate the acyclicity contract above: that forbids forwarding a peer's *entries*, and
    /// this forwards a single scalar about the asker's OWN shard, back to the only node that can
    /// interpret it.
    ///
    /// Why the minimum and not the maximum: once the boot floor is in place (#351) the restarted
    /// node's floor sits above every pre-crash seq, so a max-comparison goes permanently false
    /// while the answer is still short. And once a peer caches a mix of pre- and post-crash
    /// entries, the max exceeds the floor while the old entries are still missing. The minimum is
    /// the only scalar that stays truthful for the whole divergence window.
    ///
    /// **This stamp does not time out, and on a quiet port it does not clear.** The replica cache
    /// has no age-based eviction: it shrinks only when the origin's watermark advances, when the
    /// cache's own cap drops entries, or when the cache is trimmed to a newly adopted watermark.
    /// A restarted writer's fresh shard reports `evicted_below_seq: 0`, so the first two need it
    /// to record `shard_cap` *new* entries on that port before the peer's cached minimum rises
    /// above the boot floor. A port that goes quiet after the restart — a redeployed pod that is
    /// not receiving traffic — keeps the stamp indefinitely.
    ///
    /// That is the honest answer, because the condition it reports is itself permanent: those
    /// entries are not late, they are gone. But it makes a restarted node a *sustained* source of
    /// `Rift-Cluster-Partial`, which is not what `RiftJournalReadsDegraded`'s runbook used to
    /// assume, and that alert's description was corrected alongside this change.
    #[serde(default)]
    pub asker_cached_min: u64,
}

/// `POST /_cluster/journal/counts` — G-counter slots for a set of ports in one round trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CountsReq {
    pub ports: Vec<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CountsReply {
    /// `(port, this node's slot, this node's port clear generation)`. A list rather than a
    /// map: JSON object keys are strings, and a `u16` round-tripping through one is a decode
    /// failure waiting to happen. The generation rides alongside the slot — never a
    /// `space_gen`, which has no single fleet-wide meaning here — so [`JournalNet::fleet_counts`]
    /// can gate a peer's slot by it the same way [`fleet_count`] gates every slot (issue #224):
    /// a slot from a shard still behind the port's current generation must be excluded from the
    /// sum, not summed and quietly wrong.
    pub slots: Vec<(u16, u64, u64)>,
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
    pub space_gen: u64,
    pub flow_id: String,
    pub request: rift_cluster_base::seams::RecordedRequest,
}

// ---------------------------------------------------------------------------
// JournalNet — the per-node network layer.
// ---------------------------------------------------------------------------

const SINCE_PATH: &str = "/_cluster/journal/since";
const COUNTS_PATH: &str = "/_cluster/journal/counts";

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
/// One peer's cached shard, plus what **this cache** threw away (issue #340).
///
/// The extra field exists because two different things can make an entry unavailable, and only one
/// of them is the origin's business:
///
/// - `read.evicted_below_seq` is the **origin's** watermark, adopted wholesale on every merge. It
///   says what the peer itself no longer holds.
/// - `dropped_below_seq` is **this cache's own**, raised when [`JournalNet::merge_reply`] discards
///   low entries under [`shard_cap`] pressure. The origin still has them; this node does not.
///
/// Before this existed, the cap-pressure drop raised nothing at all, so a reader walked *past* the
/// discarded range while `evicted_through` — reading only the origin's watermark — reported the
/// range as present. The entries were simply absent from the merged view and `x-rift-truncated`
/// stayed `false`. With a cursor (issue #225) that is permanent for the walk: the reader's position
/// has already advanced beyond them.
///
/// Kept as a separate field rather than folded into `read.evicted_below_seq` by making that
/// adoption monotone. A peer that restarts legitimately reports a *lower* watermark along with
/// fresh low seqs; a sticky origin watermark would make the retain in `merge_reply` discard that
/// peer's entire new shard on arrival. The origin's field stays exactly as the origin reports it,
/// and the two are combined only where a reader is told what is missing.
///
/// [`shard_cap`]: super::journal::ClusterJournal::shard_cap
struct CachedShard {
    read: ShardRead,
    /// Highest seq this cache itself dropped under cap pressure, inclusive. Monotone: only ever
    /// raised, and only for seqs this cache actually held and discarded — never adopted from the
    /// wire.
    dropped_below_seq: u64,
}

pub struct JournalNet {
    journal: Arc<ClusterJournal>,
    /// Every peer's shard of every port this node has pulled, keyed by whose
    /// it is and which port. [`Self::anti_entropy_tick`] is the only writer;
    /// [`Self::slices_for`] is the only reader outside of it.
    replicas: RwLock<HashMap<(NodeId, u16), CachedShard>>,
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

    /// This node's writer id — the shard a legacy scalar `?since=` is attributed to (issue #225).
    ///
    /// Reads the journal's own id rather than the bound `RaftNode`'s, for the reason
    /// [`Self::slices_for`] gives: the journal knows its writer id from construction, so this
    /// answers correctly whether or not [`Self::bind`] has run.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.journal.node_id()
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
                .map(|((node_id, _), cached)| ShardSlice {
                    node_id: *node_id,
                    // The presented watermark is the **max** of what the origin evicted and what
                    // this cache dropped (issue #340). This one line is the whole read-side fix:
                    // `evicted_through`, `merge_shards_since`'s truncation check and its cursor
                    // advance all read `evicted_below_seq`, so raising it here makes every one of
                    // them honest about a cap-pressure drop with no change of their own.
                    read: ShardRead {
                        evicted_below_seq: cached
                            .read
                            .evicted_below_seq
                            .max(cached.dropped_below_seq),
                        ..cached.read.clone()
                    },
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
        let cached = replicas.entry((peer, port)).or_insert_with(|| CachedShard {
            read: ShardRead {
                entries: Vec::new(),
                evicted_below_seq: 0,
                clear_gen: 0,
                space_gens: Vec::new(),
                count_slot: 0,
            },
            dropped_below_seq: 0,
        });

        // Fold the existing cache and the new reply together, keyed on `seq` — a repeat entry
        // (the race above, or a peer that re-sends unchanged history) simply overwrites itself.
        let mut by_seq: HashMap<u64, ShardEntry> = cached
            .read
            .entries
            .drain(..)
            .map(|entry| (entry.seq, entry))
            .collect();
        for wire in reply.entries {
            let entry = from_wire(wire);
            by_seq.insert(entry.seq, entry);
        }

        cached.read.evicted_below_seq = reply.evicted_below_seq;
        cached.read.clear_gen = reply.clear_gen;
        cached.read.space_gens = reply.space_gens;
        cached.read.count_slot = reply.count_slot;

        // Drop everything either watermark already covers — the origin's (just adopted) and this
        // cache's own (issue #340). Folding `dropped_below_seq` in here matters: without it, a
        // refill that starts from a low `from` would resurrect entries the presented watermark
        // already declares gone, and the merged view would flip-flop between having them and not.
        let floor = cached.read.evicted_below_seq.max(cached.dropped_below_seq);
        by_seq.retain(|&seq, _| seq > floor);

        let mut entries: Vec<ShardEntry> = by_seq.into_values().collect();
        entries.sort_by_key(|entry| entry.seq);

        // Same cap the local shard enforces on itself, oldest (lowest seq) first — this cache is
        // a replica of exactly one other node's shard, so its retention policy should agree with
        // the original's rather than accumulate without bound.
        let cap = self.journal.shard_cap();
        if entries.len() > cap {
            let drop = entries.len() - cap;
            // Record what is being discarded *before* discarding it (issue #340). The origin still
            // holds these; this node will not re-fetch them, because both pull paths compute their
            // `from` as the max cached seq and so never look below this point again. Raising the
            // cache's own watermark is what turns a silent hole into a declared one.
            cached.dropped_below_seq = cached.dropped_below_seq.max(entries[drop - 1].seq);
            entries.drain(0..drop);
        }

        cached.read.entries = entries;
    }

    /// The lowest seq this node still caches of `asker`'s shard of `port` **that a merge would
    /// actually surface**; 0 if none.
    ///
    /// The responder half of issue #349 — see [`SinceReply::asker_cached_min`].
    ///
    /// The generation filter is not optional decoration, it is what stops a false positive. A
    /// `clear` does not purge anything from this cache: [`ClusterJournal::clear`] empties only
    /// the origin's own deque and deliberately leaves its watermark alone, `set_clear_gen` raises
    /// a generation and touches nothing else, and [`Self::merge_reply`]'s trim is by watermark,
    /// not by generation. Cleared entries therefore sit here indefinitely at their original low
    /// seqs, invisible to every merged read on every node — and reporting one of those as
    /// `asker_cached_min` would tell a restarted asker it had lost an entry that no reader can
    /// see anyway, stamping `Rift-Cluster-Partial` on a fleet where nothing is missing and every
    /// node agrees.
    ///
    /// So the filter mirrors [`merge_shards`]'s exactly: an entry counts only if its `clear_gen`
    /// is at the port's live generation and its `space_gen` is at that space's. Both floors are
    /// the highest any shard reports, for the reason `merge_shards` gives — #224 raises them
    /// through the Raft log, so a lagging shard is behind, never ahead.
    #[must_use]
    pub(crate) fn asker_cached_min(&self, asker: NodeId, port: u16) -> u64 {
        // Read the local shard's generations before taking the replica lock, so the two locks are
        // never held at once.
        let local = self.journal.read_shard_since(port, u64::MAX);
        let replicas = self.replicas.read();
        let Some(cached) = replicas.get(&(asker, port)) else {
            return 0;
        };

        let mut current_gen = local.clear_gen;
        let mut space_floor: HashMap<&str, u64> = HashMap::new();
        for (space, generation) in &local.space_gens {
            space_floor.insert(space.as_str(), *generation);
        }
        for ((_, replica_port), replica) in replicas.iter() {
            if *replica_port != port {
                continue;
            }
            current_gen = current_gen.max(replica.read.clear_gen);
            for (space, generation) in &replica.read.space_gens {
                space_floor
                    .entry(space.as_str())
                    .and_modify(|floor| *floor = (*floor).max(*generation))
                    .or_insert(*generation);
            }
        }

        cached
            .read
            .entries
            .iter()
            .filter(|entry| {
                entry.clear_gen >= current_gen
                    && entry.space_gen
                        >= space_floor
                            .get(entry.flow_id.as_str())
                            .copied()
                            .unwrap_or(0)
            })
            .map(|entry| entry.seq)
            .min()
            .unwrap_or(0)
    }

    /// Whether `cached_min`, as reported by a peer about THIS node's shard of `port`, names an
    /// entry a crash took from us.
    ///
    /// The asker half of issue #349. `cached_min == 0` means the peer caches nothing of ours, so
    /// there is nothing to be short of. Otherwise the question is whether that seq predates this
    /// boot: #351 guarantees every seq at or below the boot floor was issued by a previous boot,
    /// and a peer only ever caches what we actually served it, so a cached seq in that range is
    /// necessarily an entry this process no longer holds.
    ///
    /// Note this is one-directional on purpose. It answers "am I missing something the fleet
    /// still has", never "is the peer stale" — a peer caching seqs ABOVE our floor is simply
    /// holding entries we also hold, which is the steady state and must not stamp.
    pub(crate) fn lost_to_crash(&self, cached_min: u64, port: u16) -> bool {
        cached_min > 0 && cached_min <= self.journal.boot_floor(port)
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
                let from = self.replicas.read().get(&(peer, port)).map_or(0, |cached| {
                    cached.read.entries.iter().map(|e| e.seq).max().unwrap_or(0)
                });
                let asker = node.id();
                set.spawn(async move {
                    let body = match serde_json::to_vec(&SinceReq { port, from, asker }) {
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
        // A baseline read is the cursorless case of the same walk, so it goes through the same
        // code rather than beside it: with no cursor the per-shard filter withholds nothing and
        // the result is `merge_shards` verbatim. Expressing it this way is what stops the two
        // read paths drifting into disagreeing about eviction, clear generations, or order.
        let page = self.merge_read_since(port, None, budget).await;
        MergeOutcome {
            entries: page.entries,
            partial: page.partial,
        }
    }

    /// The cursored form of [`Self::merge_read`] (issue #225): the same fleet-wide merge, resumed
    /// from a vector cursor and answering with the token that fetches the next page. See
    /// [`merge_shards_since`] for the walk's semantics — this adds the network fan-out and the
    /// observability, exactly as [`Self::merge_read`] did before it.
    ///
    /// `cursor: None` is a **baseline** read, not a cursor at position zero; the difference is
    /// the truncation bit, and [`merge_shards_since`]'s doc says why.
    #[must_use]
    pub async fn merge_read_since(
        &self,
        port: u16,
        cursor: Option<&JournalCursor>,
        budget: Duration,
    ) -> MergeSince {
        let partial = self.pull_since_budgeted(port, budget).await;
        // Timed from *after* the fan-out, not around it (issue #319). The
        // histogram's own registration calls this "an in-memory sort … not I/O"
        // and sizes its buckets to top out at 0.5 s on that basis, but the timer
        // started before `pull_since_budgeted` — a network phase bounded by a 2 s
        // budget. So the metric contradicted both its doc and its buckets, and
        // p95/p99 pinned flat at 0.5 s exactly when the fan-out was degrading,
        // because `histogram_quantile` returns the highest finite bucket bound
        // once a quantile lands in `+Inf`. The panel went blind at the moment it
        // mattered. Measuring the fan-out against its budget is a real want, but
        // it is a different series with different buckets — #228's C29 asks for
        // exactly that, and reusing this one for it is what broke it.
        let start = std::time::Instant::now();
        let page = merge_shards_since(&self.slices_for(port), partial, cursor);
        crate::metrics::journal_merge_observed(start.elapsed());
        if page.partial {
            crate::metrics::journal_partial_read();
        }
        page
    }

    /// Pull `port` fresh from every other roster voter, concurrently, merging each reply into the
    /// replica cache as it lands. Bounded by `budget` **in total**, not per peer — a slow peer eats
    /// into every other peer's share of the budget rather than serialising the wait, which is what
    /// lets a 2 s budget survive a fleet bigger than a couple of nodes.
    ///
    /// Returns whether the read this call backs should be marked partial. Two distinct
    /// conditions set it, and conflating them is how the second one went unnoticed until #349:
    ///
    /// * **Reachability** — any peer that errored, answered something unparseable, or was still
    ///   outstanding when the budget ran out. A peer that misses this call keeps whatever the
    ///   replica cache already held for it (`slices_for` reads that cache regardless of how this
    ///   returns), so this sense means "possibly missing entries newer than the last successful
    ///   pull," never "this peer's history vanished."
    /// * **Content** — a peer answered successfully and reported that it still caches entries of
    ///   THIS node's own shard at or below our boot floor (#351). That is a crash-restarted
    ///   writer discovering the entries it lost, so here the answer really is short, and stays
    ///   short until the peers' caches evict that range.
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
            let from = self.replicas.read().get(&(peer, port)).map_or(0, |cached| {
                cached.read.entries.iter().map(|e| e.seq).max().unwrap_or(0)
            });
            let asker = node.id();
            set.spawn(async move {
                let outcome = async {
                    let body = serde_json::to_vec(&SinceReq { port, from, asker })
                        .map_err(|e| e.to_string())?;
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
                        // Read before the move: `merge_reply` takes `reply` by value.
                        let cached_min = reply.asker_cached_min;
                        self.merge_reply(peer, port, reply);
                        // The second thing `partial` can mean (issue #349). Every other arm here
                        // is about REACHABILITY — a peer we could not ask. This one is about
                        // CONTENT: the peer answered fine, and its answer reveals that it still
                        // holds entries of OUR shard that we no longer do.
                        //
                        // `<=` the boot floor is what makes that inference exact rather than a
                        // guess: #351 guarantees every seq at or below the floor was issued by a
                        // previous boot, and a peer only caches what we actually served it. So a
                        // cached seq in that range is, by construction, an entry this process
                        // lost when it crashed — our merged answer is short, and the standing
                        // gate in 12-testing.md says a short answer must say so.
                        //
                        // It clears when the peers' caches finally carry that range away, which
                        // needs `shard_cap` new recordings on this port — NOT on a timer, and
                        // not at all on a port that goes quiet. See the field's doc: the
                        // condition is permanent, so the stamp is too.
                        partial |= self.lost_to_crash(cached_min, port);
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
    /// as [`Self::merge_read`], but over `/_cluster/journal/counts` — a peer's G-counter slot and
    /// port clear generation for every listed port in one round trip, not the full shard a merged
    /// read needs. Reuses no cache: `CountsReply` carries only slots and generations, not entries,
    /// so folding it into `slices_for`'s `ShardRead` cache would mean fabricating the rest of the
    /// shape. The seed-from-cache and sum-with-`fleet_count`'s-semantics steps below give the
    /// identical partial-honesty contract without it — including the generation gate (issue #224):
    /// a peer whose reply (or cached last-known state) is still behind the port's current
    /// generation contributes nothing to the sum, the same as it would contribute no entries to
    /// [`Self::merge_read`].
    ///
    /// Failure handling mirrors [`Self::pull_since_budgeted`] (issue #223 review, B5): every
    /// `Err` is logged at `warn` with the real error and counted by
    /// [`crate::metrics::journal_peer_pull_failure`], and a peer whose task was still
    /// outstanding when `budget` expired is swept and counted the same way, not left silent just
    /// because it was aborted rather than errored.
    #[must_use]
    pub async fn fleet_counts(&self, ports: &[u16], budget: Duration) -> (HashMap<u16, u64>, bool) {
        // Slots are collected per port as `(slot, clear_gen)` pairs and folded through
        // `fleet_count` at the end, rather than accumulated by a second `saturating_add`
        // written independently here: production's `numberOfRequests` and the
        // mutation-verified gate tests must exercise the *same* summation, or a mutation to
        // one is invisible to the other (issue #223 review, B7) — and, since #224, the same
        // generation gate too. Seeded with this node's own slot and generation; peers' pairs
        // are appended below.
        let mut per_port: HashMap<u16, Vec<(u64, u64)>> = ports
            .iter()
            .map(|&port| {
                (
                    port,
                    vec![(self.journal.count(port), self.journal.clear_gen(port))],
                )
            })
            .collect();
        let totals = |per_port: &HashMap<u16, Vec<(u64, u64)>>| -> HashMap<u16, u64> {
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

        // Seeded from the replica cache first — anti-entropy's last known slot and the
        // generation it was reported at — so a peer that misses this call's budget still
        // contributes what is known rather than vanishing from the sum: the same "partial
        // never means omitted" contract `merge_read` upholds for entries.
        let mut slots: HashMap<(NodeId, u16), (u64, u64)> = HashMap::new();
        {
            let replicas = self.replicas.read();
            for &peer in &peers {
                for &port in ports {
                    if let Some(cached) = replicas.get(&(peer, port)) {
                        slots.insert(
                            (peer, port),
                            (cached.read.count_slot, cached.read.clear_gen),
                        );
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
                        for (port, slot, clear_gen) in reply.slots {
                            slots.insert((peer, port), (slot, clear_gen));
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
        space_gen: entry.space_gen,
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
        space_gen: entry.space_gen,
        flow_id: entry.flow_id,
        request: entry.request,
        recorded_at_millis: 0,
    }
}

/// The wire surface: two POST routes on the cluster port, matching
/// [`super::flow::flow_routes`]'s shape.
#[must_use]
pub fn journal_routes(net: Arc<JournalNet>) -> Router {
    let since_net = Arc::clone(&net);
    let counts_net = net;

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
                    // What we still hold OF THE ASKER'S shard, so a restarted asker can tell that
                    // it is answering short (issue #349). One map lookup under the same read lock
                    // the merge already takes; entries are seq-sorted, so the minimum is the
                    // front. Not entries, just a scalar about the asker's own shard — see the
                    // field's doc for why that does not breach the acyclicity contract.
                    let asker_cached_min = net.asker_cached_min(req.asker, req.port);
                    let reply = SinceReply {
                        entries: read.entries.iter().map(to_wire).collect(),
                        evicted_below_seq: read.evicted_below_seq,
                        clear_gen: read.clear_gen,
                        space_gens: read.space_gens,
                        count_slot: read.count_slot,
                        asker_cached_min,
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
                    // `RequestJournal::count` and `ClusterJournal::clear_gen`, not
                    // `read_shard_since(port, u64::MAX)`: both skip cloning the shard (the
                    // `since` filter admits nothing at `u64::MAX`), but reading the two atomics
                    // directly instead of walking the deque is what confirms that without the
                    // clone in the first place. The generation rides alongside the slot so a
                    // reader can gate a stale peer's slot out of `numberOfRequests` (issue #224).
                    let slots = req
                        .ports
                        .iter()
                        .map(|&port| (port, net.journal.count(port), net.journal.clear_gen(port)))
                        .collect();
                    serde_json::to_vec(&CountsReply { slots })
                        .map_err(|e| RpcError::Handler(e.to_string()))
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
            space_gen: 0,
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

    /// An entry carrying both generation components and a real `flow_id`, for the space-scoped
    /// cases (issue #224).
    fn entry_space(
        node_id: NodeId,
        seq: u64,
        clear_gen: u64,
        space_gen: u64,
        flow_id: &str,
        timestamp: &str,
    ) -> ShardEntry {
        ShardEntry {
            node_id,
            seq,
            clear_gen,
            space_gen,
            flow_id: flow_id.to_owned(),
            request: req_at(timestamp, &format!("/p{seq}")),
            recorded_at_millis: 0,
        }
    }

    /// Issue #340: what the replica cache throws away under cap pressure must be *declared*.
    ///
    /// These drive a real [`JournalNet`] rather than hand-built slices, because the defect lives in
    /// the seam between `merge_reply` (which drops) and `slices_for` (which presents) — the pure
    /// merge functions were never wrong, and testing them again would prove nothing.
    mod cache_drop_watermark {
        use super::*;
        use crate::stores::journal::JournalConfig;

        const PEER: NodeId = 7;
        const PORT: u16 = 4545;
        /// The cap every test here works against. Unbound, `shard_cap()` reads as one voter, so it
        /// is `max(min_shard_cap, fleet_capacity / 1)` — i.e. exactly `fleet_capacity`. That makes
        /// the cap controllable without reaching for `bind_fixed_voters`, which is private to
        /// `journal.rs`.
        const CAP: usize = 4;

        fn net() -> Arc<JournalNet> {
            let journal = ClusterJournal::with_parts(
                1,
                JournalConfig {
                    fleet_capacity: CAP,
                    min_shard_cap: 1,
                    ..JournalConfig::default()
                },
                Arc::new(crate::stores::journal::MonotonicClock::default()),
            );
            assert_eq!(journal.shard_cap(), CAP, "the fixture must control the cap");
            JournalNet::new(journal)
        }

        /// A reply carrying `seqs`, with the origin declaring nothing evicted.
        fn reply(seqs: std::ops::RangeInclusive<u64>, evicted_below_seq: u64) -> SinceReply {
            SinceReply {
                entries: seqs
                    .map(|seq| to_wire(&entry(PEER, seq, "2026-01-01T00:00:00Z")))
                    .collect(),
                evicted_below_seq,
                clear_gen: 0,
                space_gens: Vec::new(),
                count_slot: 0,
                asker_cached_min: 0,
            }
        }

        fn peer_slice(net: &JournalNet) -> ShardSlice {
            net.slices_for(PORT)
                .into_iter()
                .find(|slice| slice.node_id == PEER)
                .expect("the peer's cached shard is presented")
        }

        /// The core of the fix: a drop this cache made is visible in what it presents.
        ///
        /// Before, `merge_reply` discarded the lowest seqs and raised nothing — `evicted_below_seq`
        /// was only ever assigned from the *origin's* watermark, which says what the peer no longer
        /// holds, not what this cache threw away.
        #[test]
        fn an_over_cap_drop_raises_the_presented_watermark() {
            let net = net();
            // Two more than the cap, so seqs 1 and 2 are dropped.
            net.merge_reply(PEER, PORT, reply(1..=(CAP as u64 + 2), 0));

            let slice = peer_slice(&net);
            assert_eq!(
                slice.read.entries.len(),
                CAP,
                "the cap is still enforced: {:?}",
                slice.read.entries.iter().map(|e| e.seq).collect::<Vec<_>>()
            );
            assert_eq!(
                slice.read.evicted_below_seq, 2,
                "seqs 1 and 2 were dropped by this cache, so the presented watermark must cover \
                 them — the origin still holds them, but this node no longer does"
            );
        }

        /// The scenario the issue asks to pin, end to end.
        ///
        /// A reader whose cursor sits below the dropped range walks *past* it — `merge_shards_since`
        /// advances the position to the max seq held. That advance is fine; doing it in silence is
        /// not, because with a cursor (#225) those entries are skipped permanently for that walk.
        /// `x-rift-truncated` is the mechanism that exists to say so, and before this fix it stayed
        /// `false` because `evicted_through` could only see the origin's watermark.
        #[test]
        fn a_cursored_walk_over_a_dropped_range_declares_truncation() {
            let net = net();
            net.merge_reply(PEER, PORT, reply(1..=(CAP as u64 + 2), 0));

            let slices = net.slices_for(PORT);
            // Positioned below the hole: this reader has seen nothing from the peer yet.
            let cursor = JournalCursor {
                generation: 0,
                pos: [(PEER, 0u64)].into_iter().collect(),
            };

            let merged = merge_shards_since(&slices, false, Some(&cursor));
            assert!(
                merged.truncated,
                "entries 1-2 are gone from this node and the reader stepped over them — that must \
                 be declared, not silent"
            );
            assert!(
                merged.next.pos.get(&PEER).copied().unwrap_or(0) >= CAP as u64 + 2,
                "and the cursor still advances past the hole rather than stalling on it"
            );
        }

        /// The origin's watermark is adopted wholesale, but it must not *lower* what this cache
        /// already declared gone.
        ///
        /// This is why `dropped_below_seq` is a separate field rather than a monotone
        /// `evicted_below_seq`: a restarted peer legitimately reports a lower watermark with fresh
        /// low seqs, and making the origin's field sticky would make the retain discard that peer's
        /// entire new shard. The origin's value stays exactly what the origin says; the *presented*
        /// value is the max of the two.
        #[test]
        fn a_lower_origin_watermark_does_not_lower_the_presented_one() {
            let net = net();
            net.merge_reply(PEER, PORT, reply(1..=(CAP as u64 + 2), 0));
            assert_eq!(peer_slice(&net).read.evicted_below_seq, 2);

            // A later reply that evicts nothing at all must not un-declare the earlier drop.
            net.merge_reply(PEER, PORT, reply(5..=6, 0));
            assert!(
                peer_slice(&net).read.evicted_below_seq >= 2,
                "the cache's own drop is monotone — the origin cannot talk it back down"
            );
        }

        /// A baseline (uncursored) read is a snapshot and can never be truncated. Existing
        /// doctrine, restated here because raising a watermark is exactly the change most likely to
        /// break it — and a header that cried wolf on the most common read there is would be worse
        /// than the silence this fix removes.
        #[test]
        fn a_baseline_read_is_still_untruncated_after_a_drop() {
            let net = net();
            net.merge_reply(PEER, PORT, reply(1..=(CAP as u64 + 2), 0));

            let merged = merge_shards_since(&net.slices_for(PORT), false, None);
            assert!(
                !merged.truncated,
                "a baseline read is a snapshot — it has no position to have stepped over"
            );
        }

        /// The cap is not exceeded by a refill either: a later reply starting below the drop must
        /// not resurrect entries the presented watermark already declares gone, or the merged view
        /// would flip-flop between having them and not.
        #[test]
        fn a_low_refill_does_not_resurrect_dropped_entries() {
            let net = net();
            net.merge_reply(PEER, PORT, reply(1..=(CAP as u64 + 2), 0));
            // A refill from zero — what a `from = 0` pull would deliver.
            net.merge_reply(PEER, PORT, reply(1..=2, 0));

            let slice = peer_slice(&net);
            let seqs: Vec<u64> = slice.read.entries.iter().map(|e| e.seq).collect();
            assert!(
                seqs.iter().all(|&seq| seq > 2),
                "entries at or below the cache's own watermark must stay gone: {seqs:?}"
            );
        }
    }

    fn slice(node_id: NodeId, entries: Vec<ShardEntry>) -> ShardSlice {
        ShardSlice {
            node_id,
            read: ShardRead {
                count_slot: entries.len() as u64,
                entries,
                evicted_below_seq: 0,
                clear_gen: 0,
                space_gens: Vec::new(),
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

    // ---- Clear generations, live (issue #224) ----------------------------------------

    /// A reader that has applied the bump answers post-clear state or nothing — never a mix of
    /// both generations. This is the partition invariant (AC2) at merge scope: the minority's
    /// stale entries are dropped the moment *this reader* knows the generation moved, without any
    /// node re-issuing the clear.
    #[test]
    fn a_reader_that_has_applied_the_bump_never_answers_a_mixed_generation() {
        // The lagging peer still reports generation 0 and its pre-clear entries.
        let lagging = slice(1, vec![entry_gen(1, 1, 0, "2026-01-01T00:00:01Z")]);
        let mut applied = slice(2, vec![entry_gen(2, 1, 1, "2026-01-01T00:00:02Z")]);
        applied.read.clear_gen = 1;

        let merged = merge_shards(&[lagging, applied], false);
        assert!(
            merged.entries.iter().all(|e| e.clear_gen == 1),
            "the threshold is the max generation any slice reports, so one peer having \
             applied the bump is enough to drop every older entry fleet-wide"
        );
    }

    /// A **port-wide** clear must clear a space's entries too, even when that space carries a
    /// numerically higher space generation from an earlier scoped teardown.
    ///
    /// This is the case that rules out collapsing the two generations into one stamp
    /// (`max(port_gen, space_gen)`): under that encoding this entry's stamp would be 5, the
    /// port bump to 1 would leave the threshold at 5, and a full clear would silently fail to
    /// clear it. They are separate components precisely so this cannot happen.
    #[test]
    fn a_port_bump_drops_entries_carrying_a_higher_space_generation() {
        let mut scoped = slice(
            1,
            vec![entry_space(1, 1, 0, 5, "flow-a", "2026-01-01T00:00:01Z")],
        );
        scoped.read.space_gens = vec![("flow-a".to_owned(), 5)];
        scoped.read.clear_gen = 1;

        let merged = merge_shards(&[scoped], false);
        assert!(
            merged.entries.is_empty(),
            "a port-wide clear outranks any space generation — it clears the whole port"
        );
    }

    /// Space teardown is surgical: only the torn-down space loses entries (AC4).
    #[test]
    fn a_space_bump_drops_only_that_spaces_entries() {
        let mut shard = slice(
            1,
            vec![
                entry_space(1, 1, 0, 0, "flow-a", "2026-01-01T00:00:01Z"),
                entry_space(1, 2, 0, 0, "flow-b", "2026-01-01T00:00:02Z"),
                entry_space(1, 3, 0, 1, "flow-a", "2026-01-01T00:00:03Z"),
            ],
        );
        shard.read.space_gens = vec![("flow-a".to_owned(), 1)];

        let merged = merge_shards(&[shard], false);
        let kept: Vec<(&str, u64)> = merged
            .entries
            .iter()
            .map(|e| (e.flow_id.as_str(), e.seq))
            .collect();
        assert_eq!(
            kept,
            vec![("flow-b", 2), ("flow-a", 3)],
            "flow-a's pre-teardown entry is dropped; its post-teardown entry and flow-b's \
             untouched entry both survive"
        );
    }

    /// A space generation known to one peer applies to every peer's entries for that space —
    /// the map is fleet state, not per-shard state.
    #[test]
    fn a_space_generation_from_any_slice_applies_to_every_slice() {
        let stale = slice(
            1,
            vec![entry_space(1, 1, 0, 0, "flow-a", "2026-01-01T00:00:01Z")],
        );
        let mut knows = slice(
            2,
            vec![entry_space(2, 1, 0, 2, "flow-a", "2026-01-01T00:00:02Z")],
        );
        knows.read.space_gens = vec![("flow-a".to_owned(), 2)];

        let merged = merge_shards(&[stale, knows], false);
        assert_eq!(
            merged.entries.len(),
            1,
            "the lagging peer's pre-teardown entry is dropped using the generation the \
             other peer reports"
        );
        assert_eq!(merged.entries[0].node_id, 2);
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

        // A caller holding full `ShardSlice`s (the shape `slices_for` produces) maps both
        // fields straight through rather than inventing a parallel fold — see `fleet_count`'s
        // own doc for why that is exactly the shape it takes.
        assert_eq!(
            fleet_count([a, b].iter().map(|s| (s.read.count_slot, s.read.clear_gen))),
            1_000
        );
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
            fleet_count([a, b].iter().map(|s| (s.read.count_slot, s.read.clear_gen))),
            u64::MAX
        );
    }

    /// #224: a shard still reporting the pre-clear generation must not contribute its slot to
    /// the sum — summing it would answer a non-zero COUNT for the exact generation whose
    /// ENTRIES `merge_shards`'s port filter has already emptied, the two halves of one response
    /// disagreeing about whether the clear happened.
    #[test]
    fn fleet_count_drops_slots_from_a_superseded_generation() {
        let superseded = (7, 0);
        let current = (3, 1);

        assert_eq!(
            fleet_count([superseded, current]),
            3,
            "only the current-generation slot is counted"
        );
    }

    /// Today every shard pins generation 0, and the sum must still count every slot — the
    /// same no-op guarantee `generation_zero_everywhere_drops_nothing` pins for `merge_shards`,
    /// at this function's own layer.
    #[test]
    fn fleet_count_sums_every_slot_at_the_current_generation() {
        assert_eq!(fleet_count([(900, 0), (100, 0)]), 1_000);
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
            space_gen: 0,
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
            slots: vec![(4545, 12, 0), (8080, 0, 1)],
        };
        let json = serde_json::to_string(&reply).expect("CountsReply serializes");
        let back: CountsReply = serde_json::from_str(&json).expect("CountsReply round-trips");
        assert_eq!(back.slots, vec![(4545, 12, 0), (8080, 0, 1)]);
    }

    // ---- Vector cursors: the merge-walk (issue #225) -----------------------------------
    //
    // `merge_shards` answers "the whole port, merged". These answer "the whole port *after a
    // position you already hold*" — the same merge, filtered per shard, plus the token the next
    // page is fetched with. The properties under test are the ones a walk cannot be correct
    // without: every entry exactly once, no per-shard gaps, a token that never regresses, and a
    // truncation bit that is set exactly when the reader really did miss something.

    /// A slice with an explicit eviction watermark. [`slice`] always reports `0`, which is the
    /// one value that can never make the truncation bit interesting.
    fn slice_evicted(
        node_id: NodeId,
        entries: Vec<ShardEntry>,
        evicted_below_seq: u64,
    ) -> ShardSlice {
        let mut slice = slice(node_id, entries);
        slice.read.evicted_below_seq = evicted_below_seq;
        slice
    }

    /// A slice reporting a port clear generation — the walk's half of #224.
    fn slice_gen(node_id: NodeId, entries: Vec<ShardEntry>, clear_gen: u64) -> ShardSlice {
        let mut slice = slice(node_id, entries);
        slice.read.clear_gen = clear_gen;
        slice
    }

    fn empty_cursor() -> JournalCursor {
        JournalCursor::start()
    }

    fn since_paths(page: &MergeSince) -> Vec<&str> {
        page.entries
            .iter()
            .map(|e| e.request.path.as_str())
            .collect()
    }

    /// AC1, the whole of it in one walk: page until the walk is dry, and the pages concatenated
    /// must be the merged set exactly once each — no duplicate across a page boundary (the
    /// classic off-by-one in an exclusive cursor) and no entry skipped.
    #[test]
    fn a_cursor_walk_yields_every_entry_exactly_once() {
        let slices = vec![
            slice(
                1,
                vec![
                    entry(1, 1, "2026-01-01T00:00:01Z"),
                    entry(1, 2, "2026-01-01T00:00:04Z"),
                ],
            ),
            slice(
                2,
                vec![
                    entry(2, 1, "2026-01-01T00:00:02Z"),
                    entry(2, 2, "2026-01-01T00:00:05Z"),
                ],
            ),
            slice(3, vec![entry(3, 1, "2026-01-01T00:00:03Z")]),
        ];

        let first = merge_shards_since(&slices, false, None);
        assert_eq!(
            since_paths(&first),
            merge_shards(&slices, false)
                .entries
                .iter()
                .map(|e| e.request.path.as_str())
                .collect::<Vec<_>>(),
            "an empty cursor must yield exactly what an uncursored merged read yields"
        );

        // The walk is exhausted, not looping: a second page from the issued token is empty, and
        // the token it issues in turn has not moved.
        let second = merge_shards_since(&slices, false, Some(&first.next));
        assert!(
            second.entries.is_empty(),
            "re-presenting the issued token must not replay the page it was issued after: {:?}",
            since_paths(&second)
        );
        assert_eq!(
            second.next, first.next,
            "an empty page must not move the cursor"
        );
    }

    /// The incremental half of AC1: entries recorded after the token was issued appear, and only
    /// those. This is what makes the console's 2 s poll a delta fetch rather than a refetch.
    #[test]
    fn a_second_page_carries_only_what_arrived_after_the_first() {
        let before = vec![
            slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")]),
            slice(2, vec![entry(2, 1, "2026-01-01T00:00:02Z")]),
        ];
        let first = merge_shards_since(&before, false, None);
        assert_eq!(since_paths(&first).len(), 2);

        let after = vec![
            slice(
                1,
                vec![
                    entry(1, 1, "2026-01-01T00:00:01Z"),
                    entry(1, 2, "2026-01-01T00:00:03Z"),
                ],
            ),
            slice(2, vec![entry(2, 1, "2026-01-01T00:00:02Z")]),
        ];
        let second = merge_shards_since(&after, false, Some(&first.next));
        assert_eq!(
            since_paths(&second),
            vec!["/p2"],
            "only the entry recorded after the first page may appear"
        );
    }

    /// AC1's membership clause, half one: a shard that stops answering must keep its position
    /// rather than vanish from the token. Dropping it would rewind that shard to 0 the moment it
    /// came back, replaying its entire history into a walk that had already consumed it.
    #[test]
    fn a_missing_shard_keeps_its_position_instead_of_rewinding_to_zero() {
        let both = vec![
            slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")]),
            slice(2, vec![entry(2, 7, "2026-01-01T00:00:02Z")]),
        ];
        let first = merge_shards_since(&both, false, None);
        assert_eq!(first.next.pos.get(&2).copied(), Some(7));

        // Node 2 drops out of the slice set entirely — dead, or lost to the pull budget.
        let survivors = vec![slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")])];
        let next = merge_shards_since(&survivors, true, Some(&first.next));
        assert_eq!(
            next.next.pos.get(&2).copied(),
            Some(7),
            "a dead shard's position must freeze, not disappear"
        );
        assert!(
            next.entries.is_empty(),
            "the surviving shard had nothing new to add"
        );
    }

    /// AC1's membership clause, half two: a shard nobody has read yet enters at 0, so its whole
    /// history surfaces on the next page rather than being skipped as "already seen".
    #[test]
    fn a_shard_that_joins_after_the_walk_started_enters_at_zero() {
        let before = vec![slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")])];
        let first = merge_shards_since(&before, false, None);
        assert!(
            !first.next.pos.contains_key(&9),
            "node 9 has not been heard from yet"
        );

        let after = vec![
            slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")]),
            slice(
                9,
                vec![
                    entry(9, 1, "2026-01-01T00:00:02Z"),
                    entry(9, 2, "2026-01-01T00:00:03Z"),
                ],
            ),
        ];
        let second = merge_shards_since(&after, false, Some(&first.next));
        assert_eq!(
            since_paths(&second),
            vec!["/p1", "/p2"],
            "a joining shard's entries all surface — it enters the walk at 0"
        );
    }

    /// AC2: a clear landing mid-walk must not replay the cleared entries, and the token must not
    /// regress. The cleared entries are already invisible to `merge_shards`; what this pins is
    /// that the *cursor* also steps over them, so the reader never sees them again and never
    /// re-scans the seq range they occupied.
    #[test]
    fn a_clear_mid_walk_neither_replays_nor_regresses() {
        let before = vec![slice_gen(
            1,
            vec![
                entry_gen(1, 1, 0, "2026-01-01T00:00:01Z"),
                entry_gen(1, 2, 0, "2026-01-01T00:00:02Z"),
            ],
            0,
        )];
        let first = merge_shards_since(&before, false, None);
        assert_eq!(since_paths(&first), vec!["/p1", "/p2"]);

        // A clear bumps the port to generation 1; the pre-clear entries are still physically
        // present in the shard but belong to the superseded generation, and one post-clear entry
        // has landed since.
        let after = vec![slice_gen(
            1,
            vec![
                entry_gen(1, 1, 0, "2026-01-01T00:00:01Z"),
                entry_gen(1, 2, 0, "2026-01-01T00:00:02Z"),
                entry_gen(1, 3, 1, "2026-01-01T00:00:03Z"),
            ],
            1,
        )];
        let second = merge_shards_since(&after, false, Some(&first.next));
        assert_eq!(
            since_paths(&second),
            vec!["/p3"],
            "only the post-clear entry may appear — the cleared ones must not replay"
        );
        assert!(
            second.next.pos.get(&1).copied().unwrap_or(0)
                >= first.next.pos.get(&1).copied().unwrap_or(0),
            "the token must never regress across a clear"
        );
        assert_eq!(
            second.next.generation, 1,
            "the issued token carries the generation it was issued under"
        );
    }

    /// AC3, both directions. `evicted_below_seq` is **inclusive** — that seq itself is gone — so
    /// a reader at exactly the watermark has seen everything eviction removed and has NOT been
    /// truncated. Getting this boundary wrong in either direction is the whole bug: one way
    /// cries wolf on every read, the other stays silent about real data loss.
    #[test]
    fn truncation_is_reported_exactly_when_the_position_predates_the_watermark() {
        let slices = vec![slice_evicted(
            1,
            vec![entry(1, 6, "2026-01-01T00:00:06Z")],
            5,
        )];

        for (position, expected) in [(3u64, true), (4, true), (5, false), (6, false)] {
            let cursor = JournalCursor {
                generation: 0,
                pos: [(1u64, position)].into_iter().collect(),
            };
            let page = merge_shards_since(&slices, false, Some(&cursor));
            assert_eq!(
                page.truncated, expected,
                "position {position} against watermark 5 must report truncated={expected}"
            );
        }
    }

    /// A **baseline** read is a snapshot of what is retained and therefore cannot have a hole,
    /// however much the shard has evicted — while a reader presenting an explicit position of 0
    /// claims to have consumed nothing and so *has* missed what eviction removed.
    ///
    /// That distinction is the whole reason the cursor is an `Option` here rather than a
    /// zeroed-out value: collapsing them makes every ordinary uncursored read of an evicting
    /// port claim truncation forever, which is the header crying wolf on the most common read
    /// there is. Upstream draws the same line (`since.is_some_and(..)`), and this crate's
    /// single-node path is pinned to it by
    /// `cursor_since_zero_differs_from_baseline_only_in_truncation`; this is that test's
    /// merged analogue.
    #[test]
    fn a_baseline_read_is_never_truncated_but_an_explicit_zero_is() {
        let slices = vec![slice_evicted(
            1,
            vec![entry(1, 6, "2026-01-01T00:00:06Z")],
            5,
        )];

        let baseline = merge_shards_since(&slices, false, None);
        assert!(
            !baseline.truncated,
            "a baseline read sees everything retained, so it has no hole to report"
        );

        let from_zero = merge_shards_since(&slices, false, Some(&empty_cursor()));
        assert!(
            from_zero.truncated,
            "a reader claiming position 0 has provably missed the evicted entries"
        );

        // The truncation bit must be the *only* difference between the two, exactly as the
        // single-node test asserts of its own pair.
        assert_eq!(since_paths(&baseline), since_paths(&from_zero));
        assert_eq!(baseline.next, from_zero.next);
    }

    /// AC2's monotonicity clause on its own: a token presented from *ahead* of what the slices
    /// hold — a peer that has since been rolled back, or a replica cache that lost entries — must
    /// come back unchanged, never rewound to what this node can currently see.
    #[test]
    fn the_next_cursor_never_regresses_below_the_one_presented() {
        let slices = vec![slice(1, vec![entry(1, 2, "2026-01-01T00:00:02Z")])];
        let ahead = JournalCursor {
            generation: 0,
            pos: [(1u64, 99u64)].into_iter().collect(),
        };
        let page = merge_shards_since(&slices, false, Some(&ahead));
        assert_eq!(
            page.next.pos.get(&1).copied(),
            Some(99),
            "a position ahead of the shard must be preserved, not rewound"
        );
        assert!(
            page.entries.is_empty(),
            "nothing in the shard is newer than the presented position"
        );
    }

    /// A position that predates an eviction watermark must still advance past it, or the walk
    /// re-scans the evicted range on every single page forever.
    #[test]
    fn a_position_below_the_watermark_advances_past_it() {
        let slices = vec![slice_evicted(
            1,
            vec![entry(1, 9, "2026-01-01T00:00:09Z")],
            5,
        )];
        let stale = JournalCursor {
            generation: 0,
            pos: [(1u64, 2u64)].into_iter().collect(),
        };
        let page = merge_shards_since(&slices, false, Some(&stale));
        assert!(page.truncated);
        assert_eq!(
            page.next.pos.get(&1).copied(),
            Some(9),
            "the token must clear the watermark and the entries it actually served"
        );
    }

    /// AC6's cluster half: a one-voter fleet is just the single-shard case of the same walk, and
    /// the token it issues must still page correctly rather than degenerating into something
    /// only a multi-node fleet can walk.
    #[test]
    fn a_single_shard_fleet_still_issues_a_walkable_token() {
        let slices = vec![slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")])];
        let first = merge_shards_since(&slices, false, None);
        assert_eq!(since_paths(&first), vec!["/p1"]);
        assert_eq!(first.next.pos.get(&1).copied(), Some(1));
        assert!(
            merge_shards_since(&slices, false, Some(&first.next))
                .entries
                .is_empty()
        );
        // And it survives the wire, which is the only way the front door can hand it back.
        let token = first.next.encode();
        assert_eq!(
            JournalCursor::decode(&token).expect("the issued token round-trips"),
            first.next
        );
    }

    /// Two slices can name the same node — this node's own shard alongside a replica copy of
    /// it — and the fold that builds `next` must take the **max** rather than letting whichever
    /// slice happens to come last overwrite the position. An `insert` here would make the token
    /// depend on slice order, which is exactly the non-determinism `merge_shards` is built to
    /// avoid, and in the bad ordering it would hand back a position *behind* what was served.
    #[test]
    fn two_slices_for_one_node_fold_to_the_higher_position() {
        let stale = slice(1, vec![entry(1, 2, "2026-01-01T00:00:02Z")]);
        let fresh = slice(
            1,
            vec![
                entry(1, 2, "2026-01-01T00:00:02Z"),
                entry(1, 9, "2026-01-01T00:00:09Z"),
            ],
        );

        for (label, slices) in [
            ("fresh last", vec![stale.clone(), fresh.clone()]),
            ("stale last", vec![fresh, stale]),
        ] {
            let page = merge_shards_since(&slices, false, None);
            assert_eq!(
                page.next.pos.get(&1).copied(),
                Some(9),
                "{label}: the token must reach the highest seq either slice held"
            );
        }
    }

    /// `partial` is orthogonal to the cursor and must pass through untouched — a walk over a
    /// degraded fleet is still a walk, and the honesty bit belongs to the reader either way.
    #[test]
    fn a_partial_merge_stays_partial_through_the_walk() {
        let slices = vec![slice(1, vec![entry(1, 1, "2026-01-01T00:00:01Z")])];
        assert!(merge_shards_since(&slices, true, None).partial);
        assert!(!merge_shards_since(&slices, false, None).partial);
    }

    /// Issue #349 — a crash-restarted writer stamps `Rift-Cluster-Partial` instead of quietly
    /// answering short.
    ///
    /// The two halves are tested separately because the drain loop that joins them
    /// (`pull_since_budgeted`) needs a bound `RaftNode` and real RPC, which only the
    /// compose-level suites have. What is provable here is exactly what the loop composes: what
    /// a responder reports, and what an asker concludes from it.
    ///
    /// The composed behaviour is covered end-to-end by **C28**
    /// (`c28_fleet_journal_is_exact_under_node_kill`, `tests/cluster-chaos/tests/scenarios.rs`),
    /// which lands a real 3-node SIGKILL-and-restart and asserts the victim answers short and
    /// STAMPED while the survivors stay complete and unstamped. That scenario arrived with #228
    /// after this work began, and its phase-3 assertion — written to pin the unstamped answer as
    /// a documented gap — is flipped by this change, in this PR.
    mod restart_partial_stamp {
        use super::*;

        const ASKER: NodeId = 3;
        const PORT: u16 = 7070;

        /// A journal with durable floors under `dir`, so `boot_floor` is a real value rather
        /// than the always-0 an ephemeral journal reports.
        fn net_over(dir: &std::path::Path) -> Arc<JournalNet> {
            JournalNet::new(
                ClusterJournal::with_state_dir(1, dir).expect("journal over a state dir"),
            )
        }

        fn reply_of(asker: NodeId, seqs: std::ops::RangeInclusive<u64>) -> SinceReply {
            SinceReply {
                entries: seqs
                    .map(|seq| to_wire(&entry(asker, seq, "2026-01-01T00:00:00Z")))
                    .collect(),
                evicted_below_seq: 0,
                clear_gen: 0,
                space_gens: Vec::new(),
                count_slot: 0,
                asker_cached_min: 0,
            }
        }

        #[test]
        fn a_responder_reports_the_lowest_seq_it_caches_of_the_asker() {
            let net = JournalNet::new(ClusterJournal::new(1));
            // Positive control: nothing cached yet, so nothing to report.
            assert_eq!(net.asker_cached_min(ASKER, PORT), 0);

            net.merge_reply(ASKER, PORT, reply_of(ASKER, 4..=9));
            assert_eq!(
                net.asker_cached_min(ASKER, PORT),
                4,
                "the minimum, not the maximum -- see SinceReply::asker_cached_min"
            );
            // A different node, and a different port, are separate questions.
            assert_eq!(net.asker_cached_min(ASKER + 1, PORT), 0);
            assert_eq!(net.asker_cached_min(ASKER, PORT + 1), 0);
        }

        #[test]
        fn the_gap_is_stamped_when_a_peer_still_holds_what_the_crash_took() {
            let dir = tempfile::TempDir::new().expect("tempdir");
            // Boot once and record, so a floor is persisted; then "crash" and come back.
            {
                let first = ClusterJournal::with_state_dir(1, dir.path()).expect("first boot");
                first.record_indexed(PORT, "f", req_at("t", "/pre"));
            }
            let net = net_over(dir.path());
            let floor = net.journal.boot_floor(PORT);
            assert!(
                floor > 0,
                "positive control: the restart really has a floor"
            );

            // A peer reports caching one of our pre-crash entries.
            assert!(
                net.lost_to_crash(1, PORT),
                "seq 1 is at or below the boot floor {floor}, so it is an entry we lost"
            );
        }

        #[test]
        fn no_stamp_in_steady_state() {
            // The regression that would matter most: a false positive here degrades every read
            // in a healthy fleet, and the strict-mode standing gate would start failing.
            let net = JournalNet::new(ClusterJournal::new(1));
            assert_eq!(net.journal.boot_floor(PORT), 0, "first boot has no floor");
            assert!(!net.lost_to_crash(0, PORT), "nothing cached of ours");
            assert!(
                !net.lost_to_crash(1, PORT),
                "a first boot can have lost nothing"
            );
            assert!(!net.lost_to_crash(u64::MAX, PORT));
        }

        #[test]
        fn a_peer_holding_only_post_restart_entries_does_not_stamp() {
            let dir = tempfile::TempDir::new().expect("tempdir");
            {
                let first = ClusterJournal::with_state_dir(1, dir.path()).expect("first boot");
                first.record_indexed(PORT, "f", req_at("t", "/pre"));
            }
            let net = net_over(dir.path());
            let floor = net.journal.boot_floor(PORT);

            assert!(net.lost_to_crash(floor, PORT), "at the floor is still lost");
            assert!(
                !net.lost_to_crash(floor + 1, PORT),
                "above the floor is an entry from THIS boot, which we also hold"
            );
        }

        #[test]
        fn the_stamp_clears_itself_once_the_peer_evicts_the_old_range() {
            // The divergence window is bounded by the peers' caches, not latched forever.
            let dir = tempfile::TempDir::new().expect("tempdir");
            {
                let first = ClusterJournal::with_state_dir(1, dir.path()).expect("first boot");
                first.record_indexed(PORT, "f", req_at("t", "/pre"));
            }
            let net = net_over(dir.path());
            let floor = net.journal.boot_floor(PORT);

            // While the peer still caches seq 1 of ours, we are short and say so.
            assert!(net.lost_to_crash(1, PORT));
            // Once eviction has carried its cached minimum past our floor, we are not.
            assert!(!net.lost_to_crash(floor + 5, PORT));
        }

        #[test]
        fn a_reply_from_an_older_node_decodes_without_the_new_fields() {
            // Fleet skew: both fields are `#[serde(default)]`, so a node that predates #349
            // still parses. A decode failure here would be counted as a pull failure and stamp
            // partial for the wrong reason entirely.
            let old_wire = r#"{"entries":[],"evicted_below_seq":0,"clear_gen":0,
                               "space_gens":[],"count_slot":0}"#;
            let reply: SinceReply =
                serde_json::from_str(old_wire).expect("an older reply must still decode");
            assert_eq!(
                reply.asker_cached_min, 0,
                "absent reads as 'caches nothing'"
            );

            let old_req = r#"{"port":7070,"from":0}"#;
            let req: SinceReq =
                serde_json::from_str(old_req).expect("an older request must still decode");
            assert_eq!(req.asker, 0);

            // And node 0 is not a real node id, so an old asker gets the pre-#349 answer.
            let net = JournalNet::new(ClusterJournal::new(1));
            net.merge_reply(ASKER, PORT, reply_of(ASKER, 1..=3));
            assert_eq!(net.asker_cached_min(req.asker, req.port), 0);
        }

        #[test]
        fn a_cleared_entry_in_a_peers_cache_does_not_stamp() {
            // The false positive that matters most. `clear` purges nothing from a replica
            // cache -- the origin empties its own deque, the generation rises, and cleared
            // entries are filtered at READ time -- so a peer keeps holding them at their
            // original low seqs forever. Reporting one of those would stamp partial on a
            // fleet where every node's merged read is identical and nothing is missing.
            let net = JournalNet::new(ClusterJournal::new(1));

            // The peer caches three of the asker's entries, all at clear generation 0.
            net.merge_reply(ASKER, PORT, reply_of(ASKER, 1..=3));
            // Positive control FIRST: while they are live, they are reported.
            assert_eq!(
                net.asker_cached_min(ASKER, PORT),
                1,
                "control: an uncleared entry is reported"
            );

            // Now the port is cleared fleet-wide. #224 raises the generation through Raft, so
            // every node -- including this responder -- sees it.
            net.journal.set_clear_gen(PORT, None, 1);
            assert_eq!(
                net.asker_cached_min(ASKER, PORT),
                0,
                "a cleared entry is invisible to every merge, so it is not something the \
                 asker lost -- reporting it would stamp partial with nothing missing"
            );

            // An entry recorded after the clear is live again, and is reported again.
            let mut fresh = reply_of(ASKER, 9..=9);
            for wire in &mut fresh.entries {
                wire.clear_gen = 1;
            }
            fresh.clear_gen = 1;
            net.merge_reply(ASKER, PORT, fresh);
            assert_eq!(net.asker_cached_min(ASKER, PORT), 9);
        }

        #[test]
        fn a_scope_cleared_entry_does_not_stamp_either() {
            // Same shape one level down: `clear_flow` bumps a space generation rather than the
            // port's, and `merge_shards` filters on it identically.
            let net = JournalNet::new(ClusterJournal::new(1));
            let mut reply = reply_of(ASKER, 1..=2);
            for wire in &mut reply.entries {
                wire.flow_id = "space-a".into();
            }
            net.merge_reply(ASKER, PORT, reply);
            assert_eq!(net.asker_cached_min(ASKER, PORT), 1, "control");

            net.journal.set_clear_gen(PORT, Some("space-a"), 1);
            assert_eq!(
                net.asker_cached_min(ASKER, PORT),
                0,
                "a scope-cleared entry is as invisible as a port-cleared one"
            );
        }

        #[test]
        fn the_new_fields_are_carried_on_the_wire() {
            // Round-trip, so a field that stops serializing fails here rather than silently
            // disabling the stamp across the whole fleet.
            let mut reply = reply_of(ASKER, 1..=2);
            reply.asker_cached_min = 42;
            let round: SinceReply =
                serde_json::from_slice(&serde_json::to_vec(&reply).expect("encode"))
                    .expect("decode");
            assert_eq!(round.asker_cached_min, 42);

            let req = SinceReq {
                port: PORT,
                from: 5,
                asker: ASKER,
            };
            let round: SinceReq =
                serde_json::from_slice(&serde_json::to_vec(&req).expect("encode")).expect("decode");
            assert_eq!(round.asker, ASKER);
        }
    }
}
