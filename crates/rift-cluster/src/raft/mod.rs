//! ADR-001 milestone 1 spike: prove that `redb` can back `openraft`'s storage traits.
//!
//! This module is deliberately self-contained. [`ControlOp`] and [`ControlResponse`]
//! are spike stand-ins for the real control-plane op log and admin response types —
//! they are NOT wired into the rest of the cluster runtime, and `body: String` stands
//! in for the real `ImposterConfig` on purpose (see the ADR at
//! `docs/adr/ADR-001-raft-control-plane.md`).
//!
//! The only claim this module makes is: a `redb`-backed pairing of
//! [`openraft::storage::RaftLogStorage`] and [`openraft::storage::RaftStateMachine`]
//! satisfies openraft's own storage conformance suite,
//! `openraft::testing::Suite::test_all`. See [`store`] for the implementation and
//! its `#[cfg(test)]` wiring of that suite — that test is the acceptance gate for
//! this spike, not a hand-rolled smoke test.

pub mod identity;
pub mod node;
pub mod store;

pub use identity::NodeIdentity;
pub use node::{NodeConfig, NodeError, RaftNode, StatusReport};

use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

/// A cluster node's identity in the Raft group: a `u64` minted by the leader at
/// first join and persisted locally (ADR-001). Replaces the earlier
/// `name@addr#incarnation` scheme — the Raft log is the authority on membership,
/// so a node needs only a stable numeric handle.
pub type NodeId = u64;

/// Application-level operation carried by the Raft log.
///
/// `body` is a spike stand-in for the real `ImposterConfig` — kept as an opaque
/// string so this module does not need to depend on the real config type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlOp {
    PutImposter { port: u16, body: String },
}

/// Application-level response returned from applying a [`ControlOp`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlResponse {
    pub revision: u64,
}

openraft::declare_raft_types!(
    /// The openraft type configuration for the spike's control-plane group.
    pub TypeConfig:
        D = ControlOp,
        R = ControlResponse,
        NodeId = u64,
        Node = BasicNode,
);
