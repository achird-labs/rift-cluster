//! The Raft control plane (ADR-001): redb-backed storage for `openraft`, the
//! node lifecycle, and the state machine that applies committed
//! [`ControlRequest`]s to the local engine.
//!
//! The log's application payload is [`crate::control::ControlRequest`] — the
//! real ADR §4.1 op set over the upstream `ImposterConfig` — and the response is
//! [`crate::control::ControlResponse`], carrying the applying log index as the
//! revision. See the `store` module for the state-machine semantics
//! (deterministic tables first, best-effort engine drive after) and
//! [`crate::control`] for the op set and its validation rules.
//!
//! Pre-#9 state directories carried a spike-era log format (`PutImposter` over
//! an opaque string body); they are wiped, not migrated — the format changed
//! before any release shipped.

pub(crate) mod blob_source;
pub mod identity;
pub(crate) mod network;
pub mod node;
pub mod ring;
pub(crate) mod store;

pub use blob_source::BlobFetchStall;
pub use identity::NodeIdentity;
pub use network::ADMIT_CURRENCY_WAIT;
pub use node::{
    JoinOutcome, JoinedAs, LeaveOutcome, NodeConfig, NodeError, RaftNode, StatusReport,
};
pub use ring::{KeyClass, OwnStatus, OwnedKey, Ring};
pub use store::{DEFAULT_AUDIT_RETENTION_SECS, PullOutcome, SourceRecord, SourceRow};

use openraft::BasicNode;

use crate::control::{ControlRequest, ControlResponse};

/// A cluster node's identity in the Raft group: a `u64` minted by the leader at
/// first join and persisted locally (ADR-001). Replaces the earlier
/// `name@addr#incarnation` scheme — the Raft log is the authority on membership,
/// so a node needs only a stable numeric handle.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// The openraft type configuration for the control-plane group.
    pub(crate) TypeConfig:
        D = ControlRequest,
        R = ControlResponse,
        NodeId = u64,
        Node = BasicNode,
        // The snapshot is a file beside redb, not an in-memory buffer (#436). openraft's default
        // is `Cursor<Vec<u8>>`, which forced the whole payload through memory twice — once to
        // build it and once to re-encode it as a JSON integer array for the redb row.
        SnapshotData = tokio::fs::File,
);
