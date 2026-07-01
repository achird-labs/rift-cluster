//! Distributed clustering for Rift Enterprise.
//!
//! Scaffold for the distributed edition: node membership, control-plane
//! synchronization of imposters/stubs across a cluster, and request sharding.
//! Real implementations land here; this module currently defines the shape.

use serde::{Deserialize, Serialize};

/// Identity of a single Rift node within a cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

/// A point-in-time view of cluster membership.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Membership {
    pub nodes: Vec<NodeId>,
}

impl Membership {
    /// Number of known nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the cluster has no known nodes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}
