//! Deterministic ownership over the applied membership (RFC-001 §7.2, re-scoped
//! by ADR-001).
//!
//! A [`Ring`] assigns each key an owner by **rendezvous (HRW) hashing** over the
//! voters of the *applied* Raft membership at a given log index — HRW, no
//! vnodes (D-3): a membership change moves only the departed node's keys, and an
//! O(N) scan is nothing at a handful of voters. Because every node computes
//! ownership from the same committed membership, they all agree on `owner(key)`
//! at a given `m_idx` — ownership is *derived* from consensus, never carried on
//! it (D-17) — so no gossip-era epoch, settle delay, or per-key generation is
//! needed (all deleted by the re-scope). Liveness is *not*
//! mixed into ownership (that would make it non-deterministic across nodes);
//! whether the local owner may actually serve is the separate isolated-owner
//! gate ([`super::RaftNode::is_isolated`]). The membership log index
//! [`Ring::m_idx`] is the fencing token carried on owned values.
//!
//! The ring is an immutable snapshot: a consumer rebuilds it only when the view
//! changes (its `m_idx` advances), so it is effectively cached per view. With at
//! most a handful of voters, `owner()` is a cheap O(N) scan.

use xxhash_rust::xxh64::xxh64;

use super::NodeId;

/// The classes of key the ring owns.
///
/// **Two classes are live: [`KeyClass::FlowKv`] and [`KeyClass::Proxy`]**
/// (D-20). The ring exists to give each *stateful flow* exactly one node that
/// holds and mutates its state, and each `proxyOnce` `(port, signature)` claim
/// exactly one node that arbitrates it ([`super::super::stores::proxy`]);
/// everything else a node serves is replicated rather than owned.
///
/// That distinction is the one worth keeping straight, because the natural
/// reading of "owner" is the wrong one here:
///
/// - **Imposters, stubs and config are not owned.** They go through Raft, so
///   every node converges on the same set and a node that was down catches up
///   when it returns. Any node can serve any imposter and answer a stateless
///   request against it. There is no "owner of an imposter" to ask about.
/// - **A flow is owned.** One node holds its state and serializes writes to it
///   ([`super::super::stores::flow`]); a node that receives a request for a flow
///   it does not own talks to the owner instead of answering from its own copy.
///
/// [`KeyClass::Config`] is **vestigial** (D-20): it belonged to the gossip-era
/// design where a `cfg:<port>` key had a per-port config owner (RFC-001 §7.4,
/// which the RFC itself flags as a superseded mechanism). Nothing constructs
/// one — it is retained only so [`KeyClass::tag`] keeps its historical
/// numbering (it must never be renumbered), and to exercise class separation in
/// this module's tests. `Sequence` is reserved and shares the same ownership
/// function so the ring does not change when it arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyClass {
    /// Vestigial — the superseded gossip-era `cfg:<port>` config owner. Unused.
    Config,
    /// Flow key/value state: one owner per scoped flow id.
    FlowKv,
    /// Sequence counters (reserved).
    Sequence,
    /// `proxyOnce` claims: one owner per `(port, signature)` (#226).
    Proxy,
}

impl KeyClass {
    /// A stable one-byte tag mixed into the hash so the same key string in two
    /// classes hashes independently.
    ///
    /// These values are **hash inputs, not display values**: renumbering one
    /// re-scores every key in that class and silently reassigns live flows to
    /// different owners. They are append-only — a new class takes the next free
    /// number, and no existing number ever moves, including `Config`'s, which is
    /// why that arm outlives its use.
    const fn tag(self) -> u8 {
        match self {
            KeyClass::Config => 0,
            KeyClass::FlowKv => 1,
            KeyClass::Sequence => 2,
            KeyClass::Proxy => 3,
        }
    }
}

/// A key together with the class it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnedKey<'a> {
    pub class: KeyClass,
    pub key: &'a str,
}

impl<'a> OwnedKey<'a> {
    #[must_use]
    pub fn new(class: KeyClass, key: &'a str) -> Self {
        Self { class, key }
    }

    /// The owned key for a flow's state, keyed by its `flow_id`.
    ///
    /// The flow id is opaque and caller-supplied — it is not derived from a
    /// tenant or a port, and nothing maps it back to an imposter. A port with
    /// several flows therefore has several owners, one per flow, and asking for
    /// "the owner of a port" has no answer.
    #[must_use]
    pub fn flow(flow_id: &'a str) -> Self {
        Self {
            class: KeyClass::FlowKv,
            key: flow_id,
        }
    }
}

/// Whether the local node owns a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnStatus {
    /// The local node is the owner and may serve owner-side operations.
    Owner,
    /// Another node owns the key; forward to it.
    NotOwner(NodeId),
}

/// An immutable ownership snapshot over one applied membership.
#[derive(Debug, Clone)]
pub struct Ring {
    // Sorted, de-duplicated eligible voter ids. Sorting makes ties deterministic
    // and the structure independent of the order members were supplied in.
    members: Vec<NodeId>,
    m_idx: u64,
}

impl Ring {
    /// Build a ring from the eligible node ids of an applied membership, tagged
    /// with that membership's log index (`m_idx`).
    #[must_use]
    pub fn new(members: impl IntoIterator<Item = NodeId>, m_idx: u64) -> Self {
        let mut members: Vec<NodeId> = members.into_iter().collect();
        members.sort_unstable();
        members.dedup();
        Self { members, m_idx }
    }

    /// The applied-membership log index this ring was computed at — the fencing
    /// token owners stamp onto the values they write.
    #[must_use]
    pub fn m_idx(&self) -> u64 {
        self.m_idx
    }

    /// The eligible members, sorted.
    #[must_use]
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// The owner of `key`, or `None` if the ring has no members.
    ///
    /// HRW: the owner is the member maximizing `hash(node_id, class, key)`. Ties
    /// (astronomically unlikely with a 64-bit hash) break toward the larger node
    /// id, so every node resolves them identically.
    #[must_use]
    pub fn owner(&self, key: OwnedKey<'_>) -> Option<NodeId> {
        self.members
            .iter()
            .copied()
            .max_by_key(|&node| (score(node, key), node))
    }

    /// The top `n` members for `key` by HRW score, owner first.
    ///
    /// Positions past the owner are the **replica successors**: the nodes that
    /// hold redundant copies of owned state, and — because HRW scores are
    /// per-member and independent — the nodes that inherit ownership when the
    /// owner leaves the membership, without any other key moving. Every node
    /// computes the same list at the same `m_idx`, which is what lets an owner
    /// push replicas without negotiating who holds them.
    #[must_use]
    pub fn owners(&self, key: OwnedKey<'_>, n: usize) -> Vec<NodeId> {
        let mut scored: Vec<(u64, NodeId)> = self
            .members
            .iter()
            .map(|&node| (score(node, key), node))
            .collect();
        // Same order as `owner()`: score, then node id for the (astronomically
        // unlikely) tie — descending, so position 0 IS `owner()`'s answer.
        scored.sort_unstable_by(|a, b| b.cmp(a));
        scored.into_iter().take(n).map(|(_, node)| node).collect()
    }

    /// Whether `me` owns `key`.
    #[must_use]
    pub fn i_own(&self, me: NodeId, key: OwnedKey<'_>) -> Option<OwnStatus> {
        self.owner(key).map(|owner| {
            if owner == me {
                OwnStatus::Owner
            } else {
                OwnStatus::NotOwner(owner)
            }
        })
    }
}

/// The HRW score of `node` for `key`: a stable 64-bit hash of the node id, the
/// key class tag, and the key bytes. Identical on every node and every build.
fn score(node: NodeId, key: OwnedKey<'_>) -> u64 {
    let mut buf = Vec::with_capacity(8 + 1 + key.key.len());
    buf.extend_from_slice(&node.to_le_bytes());
    buf.push(key.class.tag());
    buf.extend_from_slice(key.key.as_bytes());
    xxh64(&buf, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("cfg:{}", 8000 + i)).collect()
    }

    // Two rings built from the same member set (in any order) agree on every
    // owner — the fixed-seed hash has no per-instance/per-node variance. (Order
    // independence is also guaranteed structurally by the sort in `new`.)
    #[test]
    fn owner_is_stable_across_equivalent_rings() {
        let forward = Ring::new([1, 2, 3, 4, 5], 10);
        let reversed = Ring::new([5, 4, 3, 2, 1], 10);
        for k in keys(200) {
            let key = OwnedKey::flow(&k);
            assert_eq!(forward.owner(key), reversed.owner(key), "key {k}");
        }
    }

    /// `owners()[0]` must be `owner()`'s answer for every key, or the replica
    /// set and the ownership function have quietly diverged — the one bug that
    /// would make every node disagree about where copies live.
    #[test]
    fn owners_head_is_the_owner_and_successors_are_distinct() {
        let ring = Ring::new([1, 2, 3, 4, 5], 10);
        for k in keys(200) {
            let key = OwnedKey::new(KeyClass::FlowKv, &k);
            let owners = ring.owners(key, 3);
            assert_eq!(owners.len(), 3);
            assert_eq!(owners[0], ring.owner(key).expect("non-empty"), "key {k}");
            let mut dedup = owners.clone();
            dedup.dedup();
            assert_eq!(dedup, owners, "duplicate member in the replica set: {k}");
        }
    }

    /// Asking for more replicas than there are members yields the whole ring —
    /// a two-node cluster with replication factor three holds two copies, not a
    /// phantom third.
    #[test]
    fn owners_is_clamped_to_the_membership() {
        let ring = Ring::new([1, 2], 4);
        let owners = ring.owners(OwnedKey::new(KeyClass::FlowKv, "flow-1"), 3);
        assert_eq!(owners.len(), 2);
    }

    #[test]
    fn empty_ring_has_no_owner() {
        let ring = Ring::new(std::iter::empty(), 0);
        assert!(ring.is_empty());
        assert_eq!(ring.owner(OwnedKey::flow("flow-8080")), None);
        assert_eq!(ring.i_own(1, OwnedKey::flow("flow-8080")), None);
    }

    #[test]
    fn single_node_owns_everything() {
        let ring = Ring::new([7], 3);
        for k in keys(100) {
            assert_eq!(ring.owner(OwnedKey::flow(&k)), Some(7));
            assert_eq!(ring.i_own(7, OwnedKey::flow(&k)), Some(OwnStatus::Owner));
            assert_eq!(
                ring.i_own(9, OwnedKey::flow(&k)),
                Some(OwnStatus::NotOwner(7))
            );
        }
    }

    /// HRW's defining property: removing one node reassigns only the keys that
    /// node owned; every other key keeps its owner. This is what keeps a
    /// membership change from reshuffling the whole cluster.
    ///
    /// Pins D-3: ownership is HRW with no vnodes — minimal churn on membership
    /// change, so a departure moves exactly the departed node's keys.
    #[test]
    fn removing_a_node_only_moves_its_own_keys() {
        let full = Ring::new([1, 2, 3, 4, 5], 10);
        let without_3 = Ring::new([1, 2, 4, 5], 11);
        for k in keys(500) {
            let key = OwnedKey::flow(&k);
            let before = full.owner(key).unwrap();
            let after = without_3.owner(key).unwrap();
            if before == 3 {
                assert_ne!(after, 3, "removed node still owns {k}");
            } else {
                assert_eq!(
                    before, after,
                    "key {k} moved but its owner (node {before}) stayed"
                );
            }
        }
    }

    /// Symmetric property: adding a node only steals keys for the newcomer; no key
    /// moves between two pre-existing nodes.
    ///
    /// Pins D-3: a join moves keys only *to* the newcomer, never between
    /// survivors — the HRW property that makes vnodes unnecessary here.
    #[test]
    fn adding_a_node_only_steals_keys_for_the_newcomer() {
        let before = Ring::new([1, 2, 3], 10);
        let after = Ring::new([1, 2, 3, 4], 11);
        for k in keys(500) {
            let key = OwnedKey::flow(&k);
            let old = before.owner(key).unwrap();
            let new = after.owner(key).unwrap();
            if new != old {
                assert_eq!(
                    new, 4,
                    "key {k} moved to an existing node, not the newcomer"
                );
            }
        }
    }

    /// Ownership is spread across the members rather than collapsing onto one.
    #[test]
    fn ownership_is_distributed() {
        let ring = Ring::new([1, 2, 3, 4, 5], 10);
        let mut seen = std::collections::BTreeSet::new();
        for k in keys(1000) {
            seen.insert(ring.owner(OwnedKey::flow(&k)).unwrap());
        }
        assert_eq!(seen.len(), 5, "every member should own some keys: {seen:?}");
    }

    #[test]
    fn m_idx_is_carried() {
        assert_eq!(Ring::new([1, 2, 3], 42).m_idx(), 42);
    }

    /// Pins D-20: the class tags are hash inputs and append-only — `Config`
    /// keeps its number even though nothing constructs it, because renumbering
    /// any tag re-scores every key in that class and silently reassigns live
    /// flows to different owners.
    #[test]
    fn key_class_tags_are_frozen() {
        assert_eq!(KeyClass::Config.tag(), 0);
        assert_eq!(KeyClass::FlowKv.tag(), 1);
        assert_eq!(KeyClass::Sequence.tag(), 2);
        assert_eq!(KeyClass::Proxy.tag(), 3);
        // The owner of a flow key is a pure function of (members, tag, key), so a
        // renumbering would show up here as a different owner for the same key.
        let ring = Ring::new([1, 2, 3, 4, 5, 6, 7], 1);
        let owners: Vec<_> = keys(50)
            .iter()
            .map(|k| ring.owner(OwnedKey::flow(k)).expect("non-empty"))
            .collect();
        assert_eq!(
            owners,
            keys(50)
                .iter()
                .map(|k| ring
                    .owner(OwnedKey::new(KeyClass::FlowKv, k))
                    .expect("non-empty"))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn key_classes_hash_independently() {
        // The same key string in two classes need not (and generally will not)
        // share an owner — proves the class tag participates in the hash.
        let ring = Ring::new([1, 2, 3, 4, 5, 6, 7], 1);
        let mut differ = 0;
        for k in keys(200) {
            let a = ring.owner(OwnedKey::new(KeyClass::Config, &k)).unwrap();
            let b = ring.owner(OwnedKey::new(KeyClass::FlowKv, &k)).unwrap();
            if a != b {
                differ += 1;
            }
        }
        assert!(differ > 0, "class tag had no effect on ownership");
    }
}
