//! Distributed clustering for RiftCluster.
//!
//! This crate holds the cluster runtime that turns independent Rift nodes into
//! one fleet. It reaches the open-source engine only through the `rift_cluster_base`
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

pub mod audit_export;
pub mod bridge;
pub mod config;
pub mod control;
pub mod decorate;
pub mod metrics;
pub mod pull_on_miss;
pub mod raft;
pub mod rpc;
pub mod sources;
pub mod stores;

pub use bridge::{Bridge, BridgeConfig, CallerClass};
pub use config::{ClusterConfig, ConfigError, RuntimeTopology};
pub use control::{
    AUDIT_RESOURCE_ALL, AuditRow, AuditSink, ControlOp, ControlOutcome, ControlRequest,
    ControlResponse, DEFAULT_AUDIT_BATCH_MAX_ROWS, DEFAULT_TENANT, Digest, FLEET_SCOPE,
    MAX_AUDIT_BATCH_MAX_ROWS, MAX_SOURCE_PAYLOAD_BYTES, OnDrift, PreconditionTarget,
    SESSION_KEY_BYTES, SessionKey, SourceMode, SourceProvenance, StubEdit, StubEditScript,
    TenantId, precondition_target,
};
pub use decorate::ClusterDecorator;
pub use pull_on_miss::PullOnMissInterceptor;
pub use raft::{
    DEFAULT_AUDIT_RETENTION_SECS, KeyClass, LeaveOutcome, NodeConfig, NodeError, NodeId,
    NodeIdentity, OwnStatus, OwnedKey, PullOutcome, RaftNode, Ring, SourceRecord, StatusReport,
};
pub use rpc::{Authority, AuthorityError, Router, RpcClient, RpcError, RpcServer};
pub use sources::scheduler::{PollStatus, SourceScheduler};
pub use sources::{PullError, PullReport, SourcePuller};

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
