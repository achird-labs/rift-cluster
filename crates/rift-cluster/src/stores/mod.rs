//! Durable backing for per-imposter runtime state.
//!
//! RFC-001 §2.1 says rift is "not a database — runtime state is test-run-scoped".
//! That stands for response cursors and the request journal. It is **superseded
//! for flow state** (issue #16, Gap E): a scenario mid-flight across a fleet is
//! the one piece of runtime state whose loss is indistinguishable, to the test
//! being run, from the product misbehaving. A full-cluster restart that resets
//! every flow to step one fails the test suite that was using it.
//!
//! [`FlowShard`] is this node's slice of that state — both the flows it owns and
//! the ones it holds as a replica. It is deliberately unaware of ownership,
//! fencing and replication: those belong to the store that fronts it (#120).
//! What lives here is durability, expiry and recovery, which are decisions about
//! a local file.

pub mod flow;
pub mod flow_config;
pub mod shard;

pub use flow::{ClusteredFlowStoreProvider, FlowBindConfig, FlowNet, flow_routes};
pub use flow_config::{ContextScope, FlowConfig, ReadConsistency};
pub use shard::{Durability, FlowShard, ShardConfig, ShardError, Versioned};
