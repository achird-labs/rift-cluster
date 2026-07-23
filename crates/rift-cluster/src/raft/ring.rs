//! Deterministic ownership over the applied membership (RFC-001 §7.2, re-scoped
//! by ADR-001).
//!
//! A [`Ring`] assigns each key an owner by **rendezvous (HRW) hashing** over the
//! voters of the *applied* Raft membership at a given log index. Because every
//! node computes ownership from the same committed membership, they all agree on
//! `owner(key)` at a given `m_idx` — no gossip-era epoch, settle delay, or
//! per-key generation is needed (all deleted by the re-scope). Liveness is *not*
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

/// The classes of key the ring owns. Phase 1 uses only [`KeyClass::Config`]; the
/// others are reserved for later phases and share the same ownership function so
/// the ring does not change when they arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyClass {
    /// `cfg:<port>` — imposter configuration (Phase 1).
    Config,
    /// Flow key/value state (Phase 2+).
    FlowKv,
    /// Sequence counters (Phase 2+).
    Sequence,
    /// Proxy recordings (Phase 2+).
    Proxy,
}

impl KeyClass {
    /// A stable one-byte tag mixed into the hash so the same key string in two
    /// classes hashes independently.
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

    /// Convenience for the Phase-1 `cfg:<port>` key.
    #[must_use]
    pub fn config(key: &'a str) -> Self {
        Self {
            class: KeyClass::Config,
            key,
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
            let key = OwnedKey::config(&k);
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
        assert_eq!(ring.owner(OwnedKey::config("cfg:8080")), None);
        assert_eq!(ring.i_own(1, OwnedKey::config("cfg:8080")), None);
    }

    #[test]
    fn single_node_owns_everything() {
        let ring = Ring::new([7], 3);
        for k in keys(100) {
            assert_eq!(ring.owner(OwnedKey::config(&k)), Some(7));
            assert_eq!(ring.i_own(7, OwnedKey::config(&k)), Some(OwnStatus::Owner));
            assert_eq!(
                ring.i_own(9, OwnedKey::config(&k)),
                Some(OwnStatus::NotOwner(7))
            );
        }
    }

    /// HRW's defining property: removing one node reassigns only the keys that
    /// node owned; every other key keeps its owner. This is what keeps a
    /// membership change from reshuffling the whole cluster.
    #[test]
    fn removing_a_node_only_moves_its_own_keys() {
        let full = Ring::new([1, 2, 3, 4, 5], 10);
        let without_3 = Ring::new([1, 2, 4, 5], 11);
        for k in keys(500) {
            let key = OwnedKey::config(&k);
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
    #[test]
    fn adding_a_node_only_steals_keys_for_the_newcomer() {
        let before = Ring::new([1, 2, 3], 10);
        let after = Ring::new([1, 2, 3, 4], 11);
        for k in keys(500) {
            let key = OwnedKey::config(&k);
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
            seen.insert(ring.owner(OwnedKey::config(&k)).unwrap());
        }
        assert_eq!(seen.len(), 5, "every member should own some keys: {seen:?}");
    }

    #[test]
    fn m_idx_is_carried() {
        assert_eq!(Ring::new([1, 2, 3], 42).m_idx(), 42);
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
