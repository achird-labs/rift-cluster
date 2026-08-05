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
pub mod journal;
pub mod journal_net;
mod journal_seq;
pub mod proxy;
pub mod shard;

pub use flow::{ClusteredFlowStoreProvider, FlowBindConfig, FlowNet, flow_routes};
pub use flow_config::{ContextScope, FlowConfig, ReadConsistency};
// `JournalCursor`/`CursorError` join this list because the front door names them directly
// (issue #225): it decodes the client's `?since=` token before the read and encodes the issued
// one into `x-rift-next-index` after it, so unlike the merge internals below these are part of
// the surface a caller outside this crate legitimately reaches for.
pub use journal::{
    ClusterJournal, CursorError, JournalConfig, JournalCursor, ShardEntry, ShardRead,
};
pub use proxy::{ClusterProxyStore, ProxyBindConfig, ProxyNet, proxy_routes, proxy_sig_key};
// Trimmed to what is used outside this module (issue #223 review): the wire types, `ShardSlice`,
// `merge_shards` and `fleet_count` are consumed only by this module's own `JournalNet` and its
// `mod tests` (which see ancestor-private items regardless of what is re-exported here). Every
// caller outside `rift-cluster` reaches the fleet journal through `JournalNet`'s own methods, so
// naming the wire shapes or the pure merge function itself here would advertise internals nothing
// legitimately calls. `MergeOutcome` stays out of this list for the same "not used outside" reason
// even though its own declaration stays `pub` — see its doc for why it cannot be `pub(crate)`.
pub use journal_net::{
    DEFAULT_ANTI_ENTROPY_INTERVAL, JournalNet, TailEvent, TailPage, journal_routes,
    spawn_anti_entropy,
};
pub use shard::{Durability, FlowShard, ShardConfig, ShardError, Versioned};
