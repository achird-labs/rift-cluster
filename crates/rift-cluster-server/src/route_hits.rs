//! Per-route dispatch counts for the front door (issue #368).
//!
//! The route table's HITS column answers the question an operator actually has about a route
//! table — which of these is doing anything — and its most useful value is a zero: a route that
//! has never taken a request is either wrong or dead.
//!
//! A count is fleet-wide state, so it gets the treatment the request journal already has
//! (#223) and the per-node bind status got in #369: **counted per node, merged on read, and
//! stamped partial when a peer could not be reached.** A single node's figure presented as the
//! fleet's would be worse than none — it reads as "this route is quiet" when the truth is "this
//! node is quiet".
//!
//! Three distinctions this module exists to keep apart, because collapsing any of them produces a
//! confident wrong answer rather than a visible gap:
//!
//! - **zero vs absent** — a route in the table that nobody counted reports `0`, not nothing.
//! - **zero vs unknown** — a peer that answered and has no count for a route contributes `0`; a
//!   peer that could not be asked contributes [`PeerHits::Unknown`], and the response says so.
//! - **took none vs cannot take any** — a tenant whose routes are never compiled into the shared
//!   front door reports [`RouteHits::NotInstalled`], never a zero.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use rift_cluster::{NodeId, RaftNode};
use rift_cluster_base::seams::RouteObserver;

/// This node's own counts, on the cluster port. Node-local by design: it is the *target* of the
/// admin port's fan-out, so making it fleet-wide would have every peer ask every other peer.
pub(crate) const CLUSTER_ROUTE_HITS_PATH: &str = "/_cluster/route-hits";

/// How long the fan-out waits for every peer before answering with what it has.
///
/// The same budget, for the same reason, as the members fan-out (`fleet::MEMBER_PEER_BUDGET`): an
/// operator read that answers promptly with stated coverage beats one that hangs on a dead node.
const PEER_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

/// This node's dispatch count per route id, since process start.
///
/// Deliberately in memory and deliberately not replicated: a hit is an observation *this node*
/// made, not a fact about the cluster, and writing one to Raft per front-door request would put
/// the data plane's throughput through the control plane. The cost is that a restart resets this
/// node's contribution, which the contract discloses rather than hides.
///
/// An `AtomicU64` per id under an `RwLock` over the map, mirroring the sibling seam
/// (`ClusterJournal::note_request`): the front door counts on every worker thread at once, so the
/// steady path takes only a read lock and the write lock is reached once per id ever seen.
#[derive(Debug, Default)]
pub struct RouteHitCounter {
    counts: RwLock<HashMap<String, AtomicU64>>,
}

impl RouteHitCounter {
    /// Count one dispatch against `route_id`.
    ///
    /// `Relaxed` is sufficient and is what this wants: the counter carries no happens-before
    /// relationship to anything else, and a reader only needs each increment to land exactly once,
    /// not to be ordered against other routes' increments.
    pub fn note_dispatch(&self, route_id: &str) {
        if let Some(count) = self.counts.read().get(route_id) {
            count.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // First sighting of this id. Re-checked under the write lock because another thread may
        // have inserted it in the gap, in which case its count must be kept rather than reset.
        self.counts
            .write()
            .entry(route_id.to_owned())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// This node's counts as of now.
    #[must_use]
    pub fn snapshot(&self) -> HashMap<String, u64> {
        self.counts
            .read()
            .iter()
            .map(|(id, count)| (id.clone(), count.load(Ordering::Relaxed)))
            .collect()
    }

    /// The [`CLUSTER_ROUTE_HITS_PATH`] body: raw, unpruned, unjoined against any table.
    ///
    /// Joining against a tenant's route table happens only at the merge, so this stays an honest
    /// answer to "what has this node counted" rather than a second opinion about what exists.
    #[must_use]
    pub fn body(&self) -> serde_json::Value {
        // Sorted, so two nodes' bodies can be diffed by eye — the operator habit `/_cluster/*`
        // exists to support.
        let sorted: BTreeMap<String, u64> = self.snapshot().into_iter().collect();
        serde_json::json!({ "hits": sorted })
    }
}

impl RouteObserver for RouteHitCounter {
    fn note_dispatch(&self, route_id: &str) {
        RouteHitCounter::note_dispatch(self, route_id);
    }
}

/// One peer's contribution to the fleet sum.
///
/// The two variants are the point of the type. An `Answered` peer with no entry for a route has
/// genuinely counted nothing there; an `Unknown` peer might have counted anything. Representing
/// the second as an empty map is the `unwrap_or_default()` bug #369 was filed for — it turns "I
/// could not ask" into "it answered zero".
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerHits {
    Answered(HashMap<String, u64>),
    Unknown,
}

impl PeerHits {
    /// Read a peer's [`CLUSTER_ROUTE_HITS_PATH`] reply.
    ///
    /// Fails closed on the whole peer rather than salvaging the readable part: a partially-read
    /// peer would contribute a silently-too-low count under an answer that claims to be complete.
    /// The shapes that land here in practice are a pre-#368 build (a valid body with no `hits`)
    /// and a future one whose encoding changed.
    pub(crate) fn from_reply(reply: &serde_json::Value) -> Self {
        let Some(object) = reply.get("hits").and_then(serde_json::Value::as_object) else {
            return Self::Unknown;
        };
        let mut counts = HashMap::with_capacity(object.len());
        for (id, value) in object {
            let Some(count) = value.as_u64() else {
                return Self::Unknown;
            };
            counts.insert(id.clone(), count);
        }
        Self::Answered(counts)
    }

    fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }
}

/// The answer to a `GET /front-door/route-hits`.
///
/// An enum rather than `{ installed: bool, hits: Option<_> }` so that "not installed, but here are
/// some counts" cannot be built at all. That pairing would be a claim about traffic taken by
/// routes that are structurally incapable of taking any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RouteHits {
    /// This tenant's routes are compiled into the shared front door: one entry per id in its
    /// stored table, each the sum across every node that answered.
    Installed(BTreeMap<String, u64>),
    /// Stored, and readable through `GET /front-door/routes`, but never compiled into the shared
    /// front door — so no route of this tenant's can take a dispatch. Distinct from "took none".
    NotInstalled,
}

impl RouteHits {
    #[must_use]
    pub(crate) fn body(&self) -> serde_json::Value {
        match self {
            Self::Installed(hits) => serde_json::json!({ "installed": true, "hits": hits }),
            Self::NotInstalled => {
                serde_json::json!({ "installed": false, "hits": serde_json::Value::Null })
            }
        }
    }
}

/// Sum `local` and every answered peer, keyed by the ids in the tenant's current table.
///
/// Returns the map and whether the answer is a **floor** — some peer's contribution is unknown, so
/// every sum is at least this and possibly more. The caller stamps `Rift-Cluster-Partial` on that.
///
/// Extracted as a pure function on purpose: every wire test in this repo runs a solo node, so the
/// peer arms below are reached by no integration test at all. #369 shipped a transposed pair of
/// peer fields through a 1282-test green suite for exactly this reason.
pub(crate) fn merge_route_hits(
    table_ids: &[String],
    local: &HashMap<String, u64>,
    peers: &[PeerHits],
) -> (BTreeMap<String, u64>, bool) {
    let hits: BTreeMap<String, u64> = table_ids
        .iter()
        .map(|id| {
            // Absent from a node's map means that node counted none for it — the zero is the
            // signal, so it is materialized here rather than left out.
            let mut total = local.get(id).copied().unwrap_or(0);
            for peer in peers {
                if let PeerHits::Answered(counts) = peer {
                    total = total.saturating_add(counts.get(id).copied().unwrap_or(0));
                }
            }
            (id.clone(), total)
        })
        .collect();

    // An empty table has no fact to be unsure about: a peer that did not answer could not have
    // changed any entry of a map with no entries, so `{}` is complete even though coverage was
    // not. Stamping partial there would tell an operator an empty answer might be hiding routes.
    let partial = !table_ids.is_empty() && peers.iter().any(PeerHits::is_unknown);
    (hits, partial)
}

/// Ask every other voter for its counts, concurrently, within [`PEER_BUDGET`].
///
/// Concurrently rather than serially for the reason the members fan-out documents: one unreachable
/// node would otherwise spend the whole budget before the next was tried, so a single dead peer
/// would blank every peer after it.
pub(crate) async fn peer_hits(node: &Arc<RaftNode>) -> Vec<PeerHits> {
    let status = node.status();
    let me = status.node_id;
    let peers: Vec<NodeId> = status
        .voters
        .iter()
        .copied()
        .filter(|&id| id != me)
        .collect();
    if peers.is_empty() {
        return Vec::new();
    }

    let mut set = tokio::task::JoinSet::new();
    for peer in peers.iter().copied() {
        let node = Arc::clone(node);
        set.spawn(async move {
            let outcome = node
                .call_member(peer, "GET", CLUSTER_ROUTE_HITS_PATH, Vec::new())
                .await;
            (peer, outcome)
        });
    }

    let mut answered: HashMap<NodeId, PeerHits> = HashMap::new();
    let drained = tokio::time::timeout(PEER_BUDGET, async {
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((peer, Ok(bytes))) => {
                    let hits = match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(reply) => PeerHits::from_reply(&reply),
                        Err(e) => {
                            tracing::warn!(peer, error = %e, "route-hits fan-out: unreadable reply");
                            PeerHits::Unknown
                        }
                    };
                    if hits.is_unknown() {
                        tracing::warn!(peer, "route-hits fan-out: peer published no readable counts");
                    }
                    answered.insert(peer, hits);
                }
                Ok((peer, Err(e))) => {
                    // Logged, never folded into the count: "this node could not be reached" is
                    // what the partial stamp says, and the reason belongs in the logs rather than
                    // in an API body.
                    tracing::warn!(peer, error = %e, "route-hits fan-out: peer did not answer");
                }
                Err(e) => tracing::warn!(error = %e, "route-hits fan-out: task failed"),
            }
        }
    })
    .await;
    // A timeout leaves the remaining tasks unjoined and their peers absent from `answered`, which
    // is exactly the `Unknown` the fold below produces. Dropping the set aborts them.
    let _ = drained;

    // One entry per peer, in voter order: a peer missing from `answered` — unreachable, errored,
    // or still running when the budget expired — is `Unknown`, never omitted. Omitting it would
    // shrink the fleet to the nodes that happened to answer.
    peers
        .into_iter()
        .map(|peer| answered.remove(&peer).unwrap_or(PeerHits::Unknown))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use serde_json::json;

    use super::*;

    fn ids(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    fn counts(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
        pairs.iter().map(|(id, n)| ((*id).to_owned(), *n)).collect()
    }

    fn answered(pairs: &[(&str, u64)]) -> PeerHits {
        PeerHits::Answered(counts(pairs))
    }

    fn expected(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
        pairs.iter().map(|(id, n)| ((*id).to_owned(), *n)).collect()
    }

    /// The signal this issue exists for: a route that has taken nothing reports `0`, present in
    /// the map. An absence would render as "no data" — a route that is wrong or dead is exactly
    /// what an operator needs to see, so it must be a number.
    #[test]
    fn a_route_that_took_nothing_is_a_zero_not_an_absence() {
        let (hits, partial) =
            merge_route_hits(&ids(&["busy", "idle"]), &counts(&[("busy", 4)]), &[]);

        assert_eq!(hits, expected(&[("busy", 4), ("idle", 0)]));
        assert!(!partial);
    }

    /// A counter for a route the table no longer names is stranded, not reported. A `PUT` that
    /// replaces the table, or a `DELETE`, leaves this node counting an id nobody asked about.
    #[test]
    fn a_count_for_an_id_absent_from_the_table_is_not_reported() {
        let (hits, _) = merge_route_hits(
            &ids(&["kept"]),
            &counts(&[("kept", 1), ("deleted", 99)]),
            &[],
        );

        assert_eq!(hits, expected(&[("kept", 1)]));
    }

    /// A peer that answered and simply carries no count for an id has genuinely counted zero
    /// there — it is up, it is reporting, that route took nothing on it. Distinct from a peer
    /// that could not be asked, below.
    #[test]
    fn a_peer_that_answered_without_an_id_contributes_zero_not_unknown() {
        let (hits, partial) = merge_route_hits(
            &ids(&["a", "b"]),
            &counts(&[("a", 1), ("b", 2)]),
            &[answered(&[("a", 10)])],
        );

        assert_eq!(hits, expected(&[("a", 11), ("b", 2)]));
        assert!(!partial, "an answered peer leaves no gap to declare");
    }

    /// The #369 rule, applied here: unknown is never folded to zero. The sum this node can prove
    /// is a floor, and the response has to say so.
    #[test]
    fn an_unknown_peer_makes_the_answer_partial_and_the_sums_floors() {
        let (hits, partial) = merge_route_hits(
            &ids(&["a"]),
            &counts(&[("a", 7)]),
            &[answered(&[("a", 5)]), PeerHits::Unknown],
        );

        assert_eq!(
            hits,
            expected(&[("a", 12)]),
            "the reachable nodes still sum"
        );
        assert!(
            partial,
            "a peer that could not be counted makes this a floor"
        );
    }

    /// A solo node has no peer to be unsure about, so it is never partial — the common case must
    /// not carry a degraded-coverage stamp.
    #[test]
    fn a_solo_node_with_no_peers_is_never_partial() {
        let (hits, partial) = merge_route_hits(&ids(&["a"]), &counts(&[("a", 3)]), &[]);

        assert_eq!(hits, expected(&[("a", 3)]));
        assert!(!partial);
    }

    /// An empty table has no fact to be unsure about. A peer that did not answer could not have
    /// changed any of the zero entries, so the body is complete even though coverage was not —
    /// stamping partial here would tell an operator a `{}` might be hiding something.
    #[test]
    fn an_empty_table_is_complete_even_when_a_peer_is_unknown() {
        let (hits, partial) = merge_route_hits(&[], &counts(&[]), &[PeerHits::Unknown]);

        assert!(hits.is_empty());
        assert!(!partial);
    }

    /// Unreachable in practice at front-door rates, but a wrap would report a near-zero count for
    /// the busiest route in the fleet — the one figure an operator would act on.
    #[test]
    fn sums_saturate_rather_than_wrapping() {
        let (hits, _) = merge_route_hits(
            &ids(&["a"]),
            &counts(&[("a", u64::MAX)]),
            &[answered(&[("a", 5)])],
        );

        assert_eq!(hits, expected(&[("a", u64::MAX)]));
    }

    #[test]
    fn a_reply_carrying_a_hits_object_is_that_peers_answer() {
        assert_eq!(
            PeerHits::from_reply(&json!({ "hits": { "a": 3, "b": 0 } })),
            answered(&[("a", 3), ("b", 0)])
        );
    }

    /// A peer that is up and has counted nothing at all. Distinct from [`PeerHits::Unknown`]:
    /// this one's zeros are real and must not stamp the answer partial.
    #[test]
    fn a_reply_with_an_empty_hits_object_answered_with_nothing_counted() {
        assert_eq!(
            PeerHits::from_reply(&json!({ "hits": {} })),
            PeerHits::Answered(HashMap::new())
        );
    }

    /// The rolling-upgrade shape: a peer running a build from before this endpoint existed
    /// answers a perfectly valid body that simply has no `hits` in it. Folding that to an empty
    /// map would assert the peer counted zero for every route in the fleet — the exact
    /// `unwrap_or_default()` bug #369 was filed for.
    #[test]
    fn a_reply_with_no_hits_object_is_unknown_never_an_empty_count() {
        assert_eq!(
            PeerHits::from_reply(&json!({ "node_id": "7" })),
            PeerHits::Unknown
        );
    }

    #[test]
    fn a_reply_whose_hits_is_not_an_object_is_unknown() {
        assert_eq!(
            PeerHits::from_reply(&json!({ "hits": "nope" })),
            PeerHits::Unknown
        );
    }

    /// Fails closed on the whole peer rather than dropping the unreadable id: a partially-read
    /// peer would contribute a silently-too-low count under a complete-looking answer.
    #[test]
    fn a_reply_with_an_unreadable_count_is_unknown_not_partially_read() {
        assert_eq!(
            PeerHits::from_reply(&json!({ "hits": { "a": 3, "b": "x" } })),
            PeerHits::Unknown
        );
        assert_eq!(
            PeerHits::from_reply(&json!({ "hits": { "a": -1 } })),
            PeerHits::Unknown
        );
    }

    #[test]
    fn the_installed_body_carries_the_flag_and_the_map() {
        assert_eq!(
            RouteHits::Installed(expected(&[("a", 2), ("b", 0)])).body(),
            json!({ "installed": true, "hits": { "a": 2, "b": 0 } })
        );
    }

    /// `hits: null`, never `{}` and never a map of zeros. A tenant whose routes are not compiled
    /// into the shared front door cannot take a dispatch at all, and a zero would assert it took
    /// none — a claim about traffic where the truth is about installation.
    #[test]
    fn the_not_installed_body_nulls_the_map_rather_than_zeroing_it() {
        assert_eq!(
            RouteHits::NotInstalled.body(),
            json!({ "installed": false, "hits": null })
        );
    }

    #[test]
    fn note_dispatch_counts_each_id_separately() {
        let counter = RouteHitCounter::default();

        counter.note_dispatch("a");
        counter.note_dispatch("a");
        counter.note_dispatch("b");

        assert_eq!(counter.snapshot(), counts(&[("a", 2), ("b", 1)]));
    }

    /// A route nothing has dispatched to has no entry here at all — the zero is supplied by the
    /// merge, from the table, not invented by the counter.
    #[test]
    fn a_counter_that_has_seen_nothing_is_empty() {
        assert!(RouteHitCounter::default().snapshot().is_empty());
    }

    /// The whole point of an atomic per id: the front door counts on every worker thread at once,
    /// and a read-modify-write under a shared read lock would drop counts under exactly the load
    /// an operator is trying to see.
    #[test]
    fn concurrent_dispatches_of_one_id_lose_no_counts() {
        let counter = Arc::new(RouteHitCounter::default());
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let counter = Arc::clone(&counter);
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        counter.note_dispatch("hot");
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("counting thread");
        }

        assert_eq!(counter.snapshot(), counts(&[("hot", 8000)]));
    }
}
