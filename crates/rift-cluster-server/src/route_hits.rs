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
    ///
    /// `front_door` is the other half of the honest answer (issue #403). Counts alone cannot
    /// distinguish "no request reached this route" from "this node could not have dispatched one",
    /// and a node that binds no listener reports the same zeros as a node that binds one and is
    /// quiet. It is passed in rather than held here because this counter is created before the
    /// listener is bound, and a flag that is mutable after construction could be read as `false`
    /// by a request that merely arrived early — a definite answer sourced from a race.
    #[must_use]
    pub fn body(&self, front_door: bool) -> serde_json::Value {
        // Sorted, so two nodes' bodies can be diffed by eye — the operator habit `/_cluster/*`
        // exists to support.
        let sorted: BTreeMap<String, u64> = self.snapshot().into_iter().collect();
        serde_json::json!({ "hits": sorted, "front_door": front_door })
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
    Answered {
        counts: HashMap<String, u64>,
        /// Whether that peer binds a front-door listener — `None` when it did not say (issue
        /// #403). A struct variant so an answer cannot be recorded without stating what is known
        /// about the listener, and so `None` ("did not say") stays distinct from `Some(false)`
        /// ("said it has none").
        front_door: Option<bool>,
    },
    Unknown,
}

impl PeerHits {
    /// Read a peer's [`CLUSTER_ROUTE_HITS_PATH`] reply.
    ///
    /// Fails closed on the whole peer rather than salvaging the readable part: a partially-read
    /// peer would contribute a silently-too-low count under an answer that claims to be complete.
    /// The shapes that land here in practice are a pre-#368 build (a valid body with no `hits`)
    /// and a future one whose encoding changed.
    ///
    /// `front_door` is the deliberate exception to that rule, and only that one field is affected.
    /// A pre-#403 peer sends perfectly good counts and no flag; failing it closed would blank real
    /// counts fleet-wide for the length of every rolling upgrade. So an absent — or unreadable —
    /// flag folds to `None`, the field's own "not known", and the counts are kept. `None` rather
    /// than `false` because a garbled flag is something we could not read, and reading it as "no
    /// listener" would let it help *prove* an absence.
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
        Self::Answered {
            counts,
            front_door: reply.get("front_door").and_then(serde_json::Value::as_bool),
        }
    }

    fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }

    /// What this peer said about its front door: `Some(bool)` if it said anything, `None` if it
    /// did not — which a peer we could not reach at all also, correctly, did not.
    fn front_door(&self) -> Option<bool> {
        match self {
            Self::Answered { front_door, .. } => *front_door,
            Self::Unknown => None,
        }
    }
}

/// Whether any node in the fleet binds a front-door listener (issue #403).
///
/// Three states rather than a `bool`, for the reason the whole module exists: "no listener is
/// bound anywhere" and "we could not establish that" produce the same zeros in the counts, and
/// only the first of them explains those zeros. Collapsing them would let the console diagnose a
/// listener-less fleet off the back of an unreachable peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrontDoorPresence {
    /// Some node — this one, or a peer that answered — binds one.
    Bound,
    /// Proven absence: every member was asked, and every one of them said it binds none.
    ///
    /// Named `Absent` rather than `None` so it cannot be confused with `Option::None` — the two
    /// mean opposite things here, since `Option::None` on a peer's flag is precisely the "did not
    /// say" that makes absence *unprovable*. The wire string is still `"none"`, via `as_str`.
    Absent,
    /// Not established: a member could not be asked, was never asked, or answered without the flag.
    Unknown,
}

impl FrontDoorPresence {
    fn as_str(self) -> &'static str {
        match self {
            Self::Bound => "bound",
            Self::Absent => "none",
            Self::Unknown => "unknown",
        }
    }
}

/// Fold this node's own listener state together with every peer's into one fleet answer.
///
/// Pure, and separate from [`merge_route_hits`], for the reason that function's doc records: every
/// wire test in this repo runs a solo node, so the peer arms here are reached by no integration
/// test at all.
///
/// The order of the checks is the contract. A single positive settles it — one bound listener is
/// all it takes for a route to be dispatchable, and no number of unknown peers can subtract from
/// that. Absence is the opposite: it is only claimable when every member of the fleet was asked
/// and every one of them denied binding one. That is what makes [`FrontDoorPresence::Absent`] and
/// the `Rift-Cluster-Partial` stamp mutually exclusive by construction rather than by convention.
///
/// `unasked_members` is the count of members the fan-out never reached out to at all — as opposed
/// to `peers`, which are the ones it asked and may have heard nothing from. Both block a proof of
/// absence, but only the second makes the *counts* partial, so they cannot be collapsed.
pub(crate) fn merge_front_door(
    local: bool,
    peers: &[PeerHits],
    unasked_members: usize,
) -> FrontDoorPresence {
    // One witness settles a positive, and nothing can subtract from it — not an unreachable peer,
    // and not a member we never asked. So this is checked before coverage is considered at all.
    if local || peers.iter().any(|peer| peer.front_door() == Some(true)) {
        return FrontDoorPresence::Bound;
    }
    // Members outside the fan-out (learners: `peer_hits` enumerates voters, and a fleet past the
    // auto-voter ceiling keeps the rest as learners that still bind listeners and still take
    // traffic). Their silence is not denial, so absence cannot be proven while any exist.
    if unasked_members > 0 {
        return FrontDoorPresence::Unknown;
    }
    if peers.iter().all(|peer| peer.front_door() == Some(false)) {
        FrontDoorPresence::Absent
    } else {
        FrontDoorPresence::Unknown
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
    Installed {
        hits: BTreeMap<String, u64>,
        /// Whether anything in the fleet could have dispatched to them (issue #403). Carried here
        /// rather than as a third top-level state because the two facts are differently scoped —
        /// installation is a *tenant* question, listener presence a *fleet* one — and a tenant can
        /// be on the wrong side of both at once.
        front_door: FrontDoorPresence,
    },
    /// Stored, and readable through `GET /front-door/routes`, but never compiled into the shared
    /// front door — so no route of this tenant's can take a dispatch. Distinct from "took none".
    NotInstalled,
}

impl RouteHits {
    #[must_use]
    pub(crate) fn body(&self) -> serde_json::Value {
        match self {
            Self::Installed { hits, front_door } => serde_json::json!({
                "installed": true,
                "hits": hits,
                "front_door": front_door.as_str(),
            }),
            // No `front_door` here, and the variant carries none to add: `installed: false`
            // already says these routes cannot take a dispatch, so whether some node binds a
            // listener changes nothing about them.
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
                if let PeerHits::Answered { counts, .. } = peer {
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

    /// A peer that answered and said nothing about its front door — which is what a reply with no
    /// `front_door` field decodes to, so the count tests keep comparing against the real shape.
    fn answered(pairs: &[(&str, u64)]) -> PeerHits {
        PeerHits::Answered {
            counts: counts(pairs),
            front_door: None,
        }
    }

    /// A peer that answered, with an explicit statement about its front door. `None` is a
    /// pre-#403 build, which said nothing.
    fn answered_fd(pairs: &[(&str, u64)], front_door: Option<bool>) -> PeerHits {
        PeerHits::Answered {
            counts: counts(pairs),
            front_door,
        }
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
            PeerHits::Answered {
                counts: HashMap::new(),
                front_door: None,
            }
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
            RouteHits::Installed {
                hits: expected(&[("a", 2), ("b", 0)]),
                front_door: FrontDoorPresence::Bound,
            }
            .body(),
            json!({ "installed": true, "hits": { "a": 2, "b": 0 }, "front_door": "bound" })
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

    // ── Front-door presence (#403) ──────────────────────────────────────────────────────────
    //
    // A fleet where no node binds a listener reports an honest zero for every route, and the Hits
    // column reads that as "took nothing" — the state an operator investigates as a broken route.
    // The counts are not wrong; the missing fact is that nothing could have dispatched at all.
    // Same distinction as `NotInstalled`, one level below the tenant.

    #[test]
    fn a_node_publishes_whether_it_binds_a_front_door() {
        let counter = RouteHitCounter::default();
        counter.note_dispatch("checkout");

        assert_eq!(
            counter.body(true),
            json!({ "hits": { "checkout": 1 }, "front_door": true })
        );
        assert_eq!(
            counter.body(false),
            json!({ "hits": { "checkout": 1 }, "front_door": false })
        );
    }

    #[test]
    fn a_peers_front_door_flag_is_read_when_it_is_there() {
        assert_eq!(
            PeerHits::from_reply(&json!({ "hits": { "a": 2 }, "front_door": true })),
            answered_fd(&[("a", 2)], Some(true))
        );
        assert_eq!(
            PeerHits::from_reply(&json!({ "hits": { "a": 2 }, "front_door": false })),
            answered_fd(&[("a", 2)], Some(false))
        );
    }

    /// A pre-#403 peer mid-rolling-upgrade. Failing it closed would blank real counts fleet-wide
    /// for the duration of every upgrade, so only the field it did not send is forgotten.
    #[test]
    fn a_reply_with_no_front_door_field_keeps_its_counts_and_forgets_only_the_flag() {
        assert_eq!(
            PeerHits::from_reply(&json!({ "hits": { "a": 7 } })),
            answered_fd(&[("a", 7)], None)
        );
    }

    /// Unreadable is not `false`. A malformed flag is a thing we could not read, which is exactly
    /// what `None` means — reading it as "no listener" would let a garbled field prove absence.
    #[test]
    fn a_non_bool_front_door_is_unreadable_not_false() {
        assert_eq!(
            PeerHits::from_reply(&json!({ "hits": { "a": 1 }, "front_door": "yes" })),
            answered_fd(&[("a", 1)], None)
        );
    }

    /// The pre-existing fail-closed rule for `hits` is untouched: a peer whose counts cannot be
    /// read contributes nothing and makes the answer partial, flag or no flag.
    #[test]
    fn unreadable_hits_still_fails_the_whole_peer_closed() {
        assert_eq!(
            PeerHits::from_reply(&json!({ "front_door": true })),
            PeerHits::Unknown
        );
        assert_eq!(
            PeerHits::from_reply(&json!({ "hits": { "a": -1 }, "front_door": true })),
            PeerHits::Unknown
        );
    }

    #[test]
    fn this_nodes_own_listener_proves_the_fleet_has_one() {
        assert_eq!(merge_front_door(true, &[], 0), FrontDoorPresence::Bound);
    }

    #[test]
    fn an_answered_peers_listener_beats_this_nodes_absence() {
        assert_eq!(
            merge_front_door(false, &[answered_fd(&[], Some(true))], 0),
            FrontDoorPresence::Bound
        );
    }

    /// Proven absence needs total coverage: every voter answered, and every one of them said no.
    #[test]
    fn every_voter_answering_no_listener_proves_none() {
        assert_eq!(
            merge_front_door(
                false,
                &[answered_fd(&[], Some(false)), answered_fd(&[], Some(false))],
                0
            ),
            FrontDoorPresence::Absent
        );
    }

    /// The `e2e-console.sh` fixture's exact shape: one node, started without `--front-door`. A
    /// solo node is the whole fleet, so its own absence is proof.
    #[test]
    fn a_solo_node_with_no_listener_is_proven_none() {
        assert_eq!(merge_front_door(false, &[], 0), FrontDoorPresence::Absent);
    }

    /// #369's rule: a peer we could not ask might have been running one, so absence is unproven.
    /// Folding this to `None` would let the console diagnose a listener-less fleet off a timeout.
    #[test]
    fn an_unreachable_peer_leaves_presence_unknown_not_none() {
        assert_eq!(
            merge_front_door(
                false,
                &[answered_fd(&[], Some(false)), PeerHits::Unknown],
                0
            ),
            FrontDoorPresence::Unknown
        );
    }

    /// Same reasoning one step in: a peer that answered but predates this field never said whether
    /// it binds one, so the fleet's absence is not established.
    #[test]
    fn a_pre_403_peer_leaves_presence_unknown_not_none() {
        assert_eq!(
            merge_front_door(false, &[answered_fd(&[], None)], 0),
            FrontDoorPresence::Unknown
        );
    }

    /// Unknown peers cannot subtract from a positive: this node is running one, which settles it.
    #[test]
    fn a_local_listener_outranks_an_unknown_peer() {
        assert_eq!(
            merge_front_door(true, &[PeerHits::Unknown], 0),
            FrontDoorPresence::Bound
        );
    }

    /// The invariant that makes `none` safe to render as a diagnosis: it is claimable only on
    /// full coverage, and full coverage is exactly what makes the sums complete. So the console
    /// can never be told "no listener anywhere" over an answer that is admittedly missing a node.
    ///
    /// Asserted as explicit triples rather than under an `if presence == Absent` guard. Under a
    /// guard, the only cases that reach the assertion are ones where `partial` is false anyway —
    /// so the check passes without testing anything, and the test's real strength is limited to
    /// mutations that happen to make the guard fire elsewhere. Stating both expected values for
    /// every shape makes each case carry the invariant itself.
    #[test]
    fn none_is_claimable_only_when_the_answer_is_also_complete() {
        let table = ids(&["checkout"]);
        let local = counts(&[("checkout", 0)]);
        // (peers, unasked_members, expected presence, expected partial)
        let cases: Vec<(Vec<PeerHits>, usize, FrontDoorPresence, bool)> = vec![
            (vec![], 0, FrontDoorPresence::Absent, false),
            (
                vec![answered_fd(&[("checkout", 0)], Some(false))],
                0,
                FrontDoorPresence::Absent,
                false,
            ),
            (
                vec![answered_fd(&[], Some(false)), PeerHits::Unknown],
                0,
                FrontDoorPresence::Unknown,
                true,
            ),
            (
                vec![answered_fd(&[], None)],
                0,
                FrontDoorPresence::Unknown,
                false,
            ),
            (vec![PeerHits::Unknown], 0, FrontDoorPresence::Unknown, true),
            // Asked nobody and everybody denied, but a learner was never in the fan-out at all.
            (
                vec![answered_fd(&[], Some(false))],
                1,
                FrontDoorPresence::Unknown,
                false,
            ),
        ];
        for (peers, unasked, want_presence, want_partial) in cases {
            let (_, partial) = merge_route_hits(&table, &local, &peers);
            let presence = merge_front_door(false, &peers, unasked);
            assert_eq!(
                presence, want_presence,
                "presence for {peers:?} unasked={unasked}"
            );
            assert_eq!(partial, want_partial, "partial for {peers:?}");
            assert!(
                presence != FrontDoorPresence::Absent || !partial,
                "proven absence was claimed over an incomplete answer: {peers:?}"
            );
        }
    }

    /// A learner replicates and serves the data plane in full but holds no vote, so `peer_hits`
    /// — which enumerates voters — never asks it. On a fleet past the auto-voter ceiling that is
    /// a permanent steady state, not a transient: the unasked node may well be the one binding a
    /// listener and taking every request, so its silence cannot complete a proof of absence.
    #[test]
    fn a_member_the_fan_out_never_asked_leaves_presence_unknown() {
        assert_eq!(
            merge_front_door(false, &[answered_fd(&[], Some(false))], 1),
            FrontDoorPresence::Unknown
        );
    }

    /// ...but a positive still settles it. One witness is enough, and an unasked member cannot
    /// subtract from a listener this node can see for itself.
    #[test]
    fn a_local_listener_outranks_a_member_that_was_never_asked() {
        assert_eq!(merge_front_door(true, &[], 3), FrontDoorPresence::Bound);
    }

    /// The counts half of the rolling-upgrade rule, named so it cannot erode silently: a peer that
    /// answered without the flag still contributes every count it reported.
    #[test]
    fn a_pre_403_peers_counts_still_contribute_to_the_sums() {
        let (hits, partial) = merge_route_hits(
            &ids(&["checkout"]),
            &counts(&[("checkout", 2)]),
            &[answered_fd(&[("checkout", 5)], None)],
        );
        assert_eq!(hits, expected(&[("checkout", 7)]));
        assert!(!partial, "a peer that answered is not a coverage gap");
    }

    #[test]
    fn the_installed_body_carries_the_presence_as_a_string() {
        let hits = expected(&[("checkout", 3)]);
        assert_eq!(
            RouteHits::Installed {
                hits: hits.clone(),
                front_door: FrontDoorPresence::Absent,
            }
            .body(),
            json!({ "installed": true, "hits": { "checkout": 3 }, "front_door": "none" })
        );
        assert_eq!(
            RouteHits::Installed {
                hits: hits.clone(),
                front_door: FrontDoorPresence::Bound,
            }
            .body(),
            json!({ "installed": true, "hits": { "checkout": 3 }, "front_door": "bound" })
        );
        assert_eq!(
            RouteHits::Installed {
                hits,
                front_door: FrontDoorPresence::Unknown,
            }
            .body(),
            json!({ "installed": true, "hits": { "checkout": 3 }, "front_door": "unknown" })
        );
    }

    /// `installed: false` already says these routes cannot dispatch. Whether some node binds a
    /// listener adds nothing to that, and the enum is shaped so the pairing cannot be built.
    #[test]
    fn the_not_installed_body_is_unchanged_and_says_nothing_about_listeners() {
        assert_eq!(
            RouteHits::NotInstalled.body(),
            json!({ "installed": false, "hits": serde_json::Value::Null })
        );
    }

    /// An empty table is complete rather than partial (the rule above), and the presence question
    /// is still answerable — this is a fleet with routes to add, not a broken one.
    #[test]
    fn an_empty_table_still_reports_presence() {
        let (hits, partial) = merge_route_hits(&[], &HashMap::new(), &[]);
        assert!(hits.is_empty());
        assert!(!partial);
        assert_eq!(merge_front_door(false, &[], 0), FrontDoorPresence::Absent);
    }
}
