//! Distributed clustering for Rift Enterprise.
//!
//! This crate holds the cluster runtime that turns independent Rift nodes into
//! one fleet. It reaches the open-source engine only through the `rift_ee`
//! facade — never through the vendored crates directly — so the open-core
//! boundary is enforced by the dependency graph rather than by convention.
//!
//! What is here today is the transport substrate every later phase builds on:
//!
//! * [`rpc`] — the authenticated cluster port. Peers sign every request with
//!   the shared secret; the server negotiates protocol version, verifies the
//!   credential, and dispatches into a [`rpc::Router`] that ships empty. The
//!   control plane and the state backends register their own endpoints, so the
//!   transport stays agnostic about what it carries.
//! * [`bridge`] — the private cluster-io runtime and the sync→async boundary,
//!   with the permit bounds that keep an owner outage from consuming the data
//!   plane's worker threads.
//! * [`config`] — the startup guards that refuse a cluster which would be
//!   quietly wrong (unauthenticated, unbound, or on an incompatible runtime).
//! * [`decorate`] — the response decorator that turns cluster op notes into
//!   `Rift-Cluster-*` headers, so the open-source handlers stay cluster-unaware.

pub mod bridge;
pub mod config;
pub mod control;
pub mod decorate;
pub mod metrics;
pub mod raft;
pub mod rpc;

pub use bridge::{Bridge, BridgeConfig, CallerClass};
pub use config::{ClusterConfig, ConfigError, RuntimeTopology};
pub use control::{
    ControlOp, ControlOutcome, ControlRequest, ControlResponse, DEFAULT_TENANT, StubEdit,
    StubEditScript, TenantId,
};
pub use decorate::ClusterDecorator;
pub use raft::{
    KeyClass, LeaveOutcome, NodeConfig, NodeError, NodeId, NodeIdentity, OwnStatus, OwnedKey,
    RaftNode, Ring, StatusReport, TypeConfig,
};
pub use rpc::{Authority, AuthorityError, Router, RpcClient, RpcError, RpcServer};

use serde::{Deserialize, Serialize};

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
