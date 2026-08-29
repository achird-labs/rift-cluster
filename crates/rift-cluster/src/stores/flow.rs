//! The clustered flow store: owner-authoritative flow state for every imposter
//! on a `--cluster` node (#120, Gap C of #16).
//!
//! # The problem this solves
//!
//! Scripts drive response content and fault decisions off flow state, and a
//! round-robin load balancer sends consecutive requests of one flow to
//! different nodes. Upstream's in-memory store is per-process, so a scenario's
//! step 2 lands on a node that never saw step 1. Ownership fixes that: every
//! flow has one owner (HRW over the applied membership, [`KeyClass::FlowKv`]),
//! every write is serialized through it, and — by default — every read is
//! answered by it. Flow state never rides the Raft log — ownership is derived
//! from committed membership, the state itself is owner-held, successor-
//! replicated and WAL-backed (D-17). Correct under any LB — the receiving node
//! is *not* assumed to be the owner; LB affinity is stickiness only (D-13) —
//! one LAN RPC worst-case, opt-out per imposter via `readConsistency: "local"`
//! (D-10: owner-authoritative by default, replica reads by explicit choice).
//!
//! # Structure
//!
//! [`FlowNet`] is the per-node subsystem: the durable [`FlowShard`] (#119), the
//! sync→async [`Bridge`], the owner-side write path, and the replication push.
//! [`ClusteredFlowStore`] is the thin per-imposter face the engine sees — it
//! implements upstream's synchronous [`FlowStore`] and carries only that
//! imposter's parsed [`FlowConfig`]. [`flow_routes`] is the wire surface other
//! nodes reach this one through, registered on the cluster port before the
//! node starts.
//!
//! # Late binding
//!
//! The engine manager (and therefore the provider) is constructed before the
//! [`RaftNode`] exists — the same ordering problem `PullOnMissInterceptor`
//! solves with a `OnceLock`, solved the same way here. Before [`FlowNet::bind`]
//! every clustered op fails loudly with "cluster starting" rather than falling
//! back to a builtin store: a fallback would hand one imposter process-local
//! semantics forever, which is a silent split-brain per imposter and exactly
//! the wrong-but-quiet failure the error rules prohibit.
//!
//! # Deviations from the #120 design text, recorded
//!
//! - One `flow/write` route with an op enum rather than `put`/`cas`/`incr`
//!   routes: a single owner-side entry point means one fencing check and one
//!   serialization site, and read-modify-write ops need that serialization
//!   anyway.
//! - No `Rift-Cluster-Degraded` response header: the engine reaches this store
//!   through `spawn_blocking` (it is `is_blocking`), and the annotation scope
//!   that becomes response headers is task-local — it does not cross that
//!   boundary. A strong read that cannot reach the owner therefore **errors**
//!   (the engine propagates store errors per upstream #318); it never silently
//!   serves the replica.
//! - Repair shipped as one unit in #126 — lazy owner **adoption** on first
//!   touch per membership epoch, the replica-side **anti-entropy** pull loop,
//!   and versioned delete **tombstones** — because each is dead code without
//!   the others. Full-content sync, no digests: scenario flows are a handful
//!   of keys, and the simple protocol is the one whose failure modes fit in a
//!   head. Known residual: a replica that missed a brand-new flow's every push
//!   repairs at takeover, not by anti-entropy (it cannot pull what it never
//!   heard of); and `ClearFlow` replication keeps its #120 semantics (the
//!   owner's copy is always right, and flow-level tombstones are not worth
//!   their complexity for a rare admin op).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bridge::{Bridge, BridgeConfig, CallerClass};
use crate::control::TenantId;
use crate::metrics;
use crate::raft::ring::{KeyClass, OwnedKey};
use crate::raft::{NodeId, RaftNode, Ring};
use crate::rpc::{HandlerFuture, Router, RpcError};
use crate::stores::flow_config::{ContextScope, FlowConfig, ReadConsistency};
use crate::stores::shard::{Durability, FlowShard, Versioned};
use rift_cluster_base::seams::BackendUnavailable;

/// Bound on one flow op end to end: the bridge park, the owner RPC inside it,
/// and the owner's own shard write. Matches the RPC layer's default request
/// timeout so the inner call times out with (not after) the outer one.
const FLOW_OP_DEADLINE: Duration = Duration::from_secs(2);

/// How long a delete's tombstone outlives the delete. Long enough that any
/// delayed replication push (bounded by the RPC deadline and its retries) has
/// lost the race before the record of the delete disappears; short enough that
/// deleted keys do not accumulate. The sweep reaps them like any expired entry.
const TOMBSTONE_TTL: Duration = Duration::from_secs(60);

/// Copies of each flow: the owner plus two successors. Matches the config
/// plane's replication factor reasoning in RFC-001 §7.3 — survives one node
/// loss with a copy to spare, and three is the whole fleet at minimum size.
const REPLICAS: usize = 3;

const WRITE_PATH: &str = "/_cluster/flow/write";
const GET_PATH: &str = "/_cluster/flow/get";
const REPLICATE_PATH: &str = "/_cluster/flow/replicate";
const SYNC_PATH: &str = "/_cluster/flow/sync";
const COUNTS_PATH: &str = "/_cluster/flow/counts";
const SPACES_PATH: &str = "/_cluster/flow/spaces";

/// The refusal every owner-side entry returns while this node cannot see a
/// quorum — D-17's isolated-owner rule. One constant because every one of those
/// entries and the acceptance gate match on it, and a literal that drifted in
/// one of them would turn that gate into a test of nothing.
const ISOLATED_REFUSAL: &str = "flow store: owner is isolated from the cluster";

/// The not-ready refusals (D-65): the cluster cannot take the op *yet* — or
/// any more — and nothing about the request is wrong. Constants for the same
/// reason [`ISOLATED_REFUSAL`] is one: an owner answers them in band, and the
/// store face recognises them by text (see [`is_not_ready`]).
const NOT_READY_STARTING: &str = "flow store: cluster is still starting";
const NOT_READY_SHUT_DOWN: &str = "flow store: cluster node has shut down";
const NOT_READY_NO_MEMBERSHIP: &str = "flow store: no applied membership yet";
const NOT_READY_NO_OWNER: &str = "flow store: ring has no members";

/// Whether an in-band refusal is one of the not-ready states.
fn is_not_ready(reason: &str) -> bool {
    matches!(
        reason,
        NOT_READY_STARTING | NOT_READY_SHUT_DOWN | NOT_READY_NO_MEMBERSHIP | NOT_READY_NO_OWNER
    )
}

/// The store-face error for a failure caused by the cluster's *state* (D-65):
/// the D-17 isolation refusal, an owner that could not be reached, a shed
/// bridge, a node that is not ready. It carries [`BackendUnavailable`], the
/// one type upstream's `backend_error_response` keys a 503 on (#318) — the
/// same type `rift-store-redis` attaches to every Redis outage. A fault in the
/// request or in this node stays a plain error and answers 500, because a
/// retry cannot help it.
fn unavailable(detail: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(BackendUnavailable {
        feature: "flowState",
        detail: detail.into(),
    })
}

/// A not-ready refusal made where the error is still an [`RpcError`] — inside
/// a bridge closure or a cluster-port handler. `Unavailable`, not `Handler`:
/// on the cluster port that is a 503 rather than a 500, a forwarding hop
/// relays it as the owner's own unavailability (D-61), and [`store_error`]
/// types it at the face.
fn rpc_not_ready(reason: String) -> RpcError {
    RpcError::Unavailable {
        detail: reason,
        op_id: None,
    }
}

/// The store-face error for a clustered op that failed on the wire or at the
/// owner. Cluster state — see [`unavailable`] — is unavailability; the peer's
/// own text is kept as the detail so the D-61 runbook signal survives the wrap.
fn store_error(e: RpcError) -> anyhow::Error {
    if matches!(e, RpcError::Unavailable { .. }) || e.is_liveness_failure() {
        unavailable(e.to_string())
    } else {
        anyhow::anyhow!("flow store: {e}")
    }
}

/// How often a replica pulls the flows it holds from their owners (#16's
/// anti-entropy interval). A missed push heals within one tick. Deliberately
/// not a CLI flag — it is a repair cadence, not an operator trade-off — but
/// configurable through [`FlowBindConfig`] so tests need not wait 5 s.
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(5);

/// Flows per sync request. Bounds one request's body well under the
/// transport's cap while keeping a whole node's repair to a handful of round
/// trips per owner.
const SYNC_CHUNK: usize = 256;

/// How a repair (adoption or anti-entropy) writes what it pulled: **memory
/// only**.
///
/// The rule this encodes is that *disk copies come from writes, never from
/// repairs*. A repair restores the in-memory view that serves reads; the
/// durable copy is written by the write path, on the owner, at the mode the
/// imposter actually chose, and reaches replicas through a replication push
/// that now carries that same mode (#121).
///
/// The alternative — repairing at `Async` — is what this replaced, and it made
/// `durability: "none"` meaningless: a flow the imposter asked to keep off disk
/// was written to disk by every replica's repair loop within one tick, and then
/// adopted back after a restart. A repair path does not know which imposter a
/// `flow_id` belongs to, so it cannot ask; not persisting is the choice that is
/// right for every mode rather than wrong for one.
const REPAIR_DURABILITY: Durability = Durability::None;

// ---------------------------------------------------------------------------
// Wire types. JSON like every other cluster route; versioned implicitly by the
// cluster port's protocol handshake.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct GetReq {
    flow_id: String,
    key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct GetReply {
    entry: Option<Versioned>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WriteReq {
    flow_id: String,
    /// Unused by the flow-level ops (`SetTtl`, `ClearFlow`); empty there.
    key: String,
    op: WriteOp,
    /// The imposter's default TTL, applied to entries this op writes.
    ttl_seconds: Option<i64>,
    durability: Durability,
    /// The sender's ring index — the fencing token (RFC-001 §7.6). The owner
    /// refuses an op minted under a different membership view than its own.
    m_idx: u64,
}

#[derive(Debug, Serialize, Deserialize)]
enum WriteOp {
    Set {
        value: Value,
    },
    Delete,
    /// Read-modify-write at the owner: absent counts as 0, non-`i64` current
    /// values coerce to 0 — the semantics of upstream's default
    /// `increment_by`, kept so single-node and clustered behaviour agree.
    Incr {
        by: i64,
    },
    Cas {
        expected: Option<Value>,
        new: Value,
    },
    /// `ttl_seconds <= 0` drops the whole flow now (upstream #530 semantics).
    SetTtl {
        ttl_seconds: i64,
    },
    /// Per-key TTL; replies with whether the key existed.
    SetKeyTtl {
        ttl_seconds: i64,
    },
    ClearFlow,
}

#[derive(Debug, Serialize, Deserialize)]
enum WriteReply {
    Applied {
        /// The owner-minted entry, for ops that write one.
        entry: Option<Versioned>,
        /// `Incr`'s new value.
        incremented: Option<i64>,
        /// `SetKeyTtl`'s "did the key exist".
        existed: Option<bool>,
    },
    CasConflict {
        current: Option<Value>,
    },
    /// The op's fencing token does not match the owner's applied membership.
    /// The sender's move is to rebuild its ring and retry, not to trust either
    /// side blindly.
    Fenced {
        owner_m_idx: u64,
    },
    /// The receiving node does not own this flow at the shared `m_idx`. With a
    /// matching token this can only mean a buggy (or hostile) member misrouted
    /// the write — a correct member computes the same HRW owner from the same
    /// membership. Refused as defence in depth: applying it would pollute a
    /// replica's shard with owner-versioned entries, and the replicate push
    /// that follows would spread them.
    NotOwner {
        owner: NodeId,
    },
    /// The owner's shard refused (storage failure). In-band rather than an
    /// `RpcError` so the transport's retry logic does not re-run a storage
    /// failure that will not heal by retrying.
    Error {
        reason: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
enum ReplicaOp {
    /// Merged by version: an older push never overwrites a newer entry.
    Put {
        key: String,
        entry: Versioned,
    },
    /// **Inbound wire-compat only** (#126): a peer still on the pre-tombstone
    /// build pushes deletes this way, and dropping the variant would turn its
    /// pushes into decode errors mid-rolling-upgrade. Applied as a physical
    /// delete, exactly as that peer expects. Never *sent* by this build —
    /// deletes go out as versioned tombstone `Put`s, which is what makes
    /// resurrection impossible.
    ///
    /// The reverse skew (this build pushing a tombstone to an old peer) is a
    /// bounded transient: old `Versioned` ignores the unknown `deleted` field,
    /// so the old node stores a live `null` visible to its *local* reads until
    /// the entry's 60 s expiry reaps it. Rolling-upgrade-only, self-healing,
    /// and the owner's copy — what `strong` reads consult — is never affected.
    Delete {
        key: String,
    },
    ClearFlow,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplicateReq {
    flow_id: String,
    op: ReplicaOp,
    /// The durability the *origin* write chose, carried so a replica persists a
    /// flow exactly as much as the imposter asked for.
    ///
    /// This was hardcoded `Async` until #121, which made the mode a lie
    /// fleet-wide: an imposter choosing `none` — documented as "in memory only,
    /// a restart loses everything" — still had its state written to disk by
    /// both replicas, and after a full-cluster restart the new owner adopted it
    /// back (#126). Unasked-for disk traffic on the one mode whose purpose is
    /// avoiding it, and it silently erased the distinction between `none` and
    /// `sync`. C15's mutation test is what surfaced it: flipping the scenario to
    /// `none` changed nothing.
    ///
    /// `serde(default)` is `Async`, exactly the old behaviour, so a push from a
    /// peer that predates this field still applies.
    #[serde(default)]
    durability: Durability,
}

/// One request serves both repair paths (#126): adoption asks for a single
/// flow from each fellow holder; the anti-entropy loop asks an owner for every
/// flow this node holds of theirs, chunked.
#[derive(Debug, Serialize, Deserialize)]
struct SyncReq {
    flow_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SyncReply {
    /// Full contents per flow, **tombstones included** — a repair that omitted
    /// deletions would resurrect exactly what the tombstones exist to bury.
    /// No digests in v1: scenario flows are a handful of keys, and full
    /// content is simple to reason about. Bandwidth cost noted in the module
    /// doc.
    flows: Vec<(String, Vec<(String, Versioned)>)>,
}

/// `POST /_cluster/flow/counts` — this node's OWNED entry count per port
/// (#372), the flow-state half of a tenant's `numberOfFlowEntries` usage.
#[derive(Debug, Serialize, Deserialize)]
struct CountsReq {
    ports: Vec<u16>,
    /// The tenants whose tenant-scoped (`t<tenant>:`, #288) flows the caller
    /// wants counted (#413). `default` so a peer still on a build before this
    /// field decodes the request; it then answers with no tenant slots, and the
    /// tenant half of the fleet-wide sum is a floor for the length of that
    /// rolling upgrade — the same shape any additive wire field has here.
    #[serde(default)]
    tenants: Vec<String>,
    /// The caller's own ring index, checked against the serving node's
    /// (RFC-001 §7.6's fencing token, reused here rather than invented
    /// afresh). A membership change can leave two nodes' rings disagreeing
    /// about who owns a flow for a window — long enough for one flow to be
    /// claimed by both (double-counted) or by neither (undercounted) if each
    /// side just answers under its own view. Comparing tokens turns that into
    /// a detectable divergence instead of a silently wrong number.
    m_idx: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CountsReply {
    /// `(port, this node's owned live-entry count)`. A list rather than a
    /// `HashMap<u16, _>`, matching [`super::journal_net::CountsReply`]'s own
    /// reasoning: a JSON object's keys are strings, and a `u16` round-tripping
    /// through one is a decode failure waiting to happen.
    slots: Vec<(u16, u64)>,
    /// `(tenant, this node's owned live-entry count under `t<tenant>:`)` (#413).
    /// `default` for the same rolling-upgrade reason as [`CountsReq::tenants`]:
    /// an older peer's reply has no such field and decodes as "no tenant
    /// slots".
    #[serde(default)]
    tenant_slots: Vec<(String, u64)>,
}

/// What [`FlowNet::fleet_usage_counts`] answers: live entries charged per port
/// (imposter-scoped flows, `i<port>:`) and per tenant (tenant-scoped flows,
/// `t<tenant>:`, #288/#413). Fleet-scoped `f:` flows are in neither — shared by
/// construction, so charging them anywhere would be arbitrary.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FlowCounts {
    pub by_port: HashMap<u16, u64>,
    pub by_tenant: HashMap<String, u64>,
}

/// `POST /_cluster/flow/spaces` — this node's OWNED spaces under `prefix` (#374): the per-row
/// counterpart to [`CountsReq`]'s per-port totals, needed because the admin listing must name each
/// space and its own live entry count, not just sum them.
#[derive(Debug, Serialize, Deserialize)]
struct SpacesReq {
    /// Diagnostic only — the same role `CountsReq::ports` plays in its own handler's log lines.
    /// `prefix` is what [`owned_spaces`] actually filters on; a fleet-scoped (`f:`) prefix names no
    /// port at all, so `port` cannot substitute for it.
    port: u16,
    prefix: String,
    /// [`CountsReq::m_idx`]'s fencing token, reused verbatim: a peer whose ring has since diverged
    /// from the caller's must refuse rather than answer under a view nobody asked for.
    m_idx: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SpacesReply {
    /// `(unscoped space id, live entry count)` — [`owned_spaces`]'s own return shape, carried
    /// verbatim. No [`NodeId`] rides along: the caller already knows which peer answered from the
    /// connection that carried the reply, and stamping it here too would be a second copy of that
    /// fact, free to disagree with the first — see [`merge_space_rows`]'s doc for why that
    /// disagreement is exactly the bug this design avoids by construction.
    rows: Vec<(String, u64)>,
}

/// One correlated-isolation space (#374): its unscoped id, how many live flow-KV entries it
/// currently holds, and which ring member owns it.
///
/// `owner` is not `Option`: a row is only ever produced by [`merge_space_rows`] stamping the node
/// that actually reported it (this node for `local`, that peer for a peer's own share), so the
/// owner is known by construction rather than looked up separately and possibly disagreeing.
#[derive(Debug, Clone)]
pub struct SpaceRow {
    pub space: String,
    pub entry_count: u64,
    pub owner: NodeId,
}

/// One peer's answer to a spaces fan-out, paired with which peer it came from: `None` means "did
/// not answer" (an error, a panicked task, or one still outstanding when the budget expired) —
/// [`SpacesReply::rows`]'s decoded shape, carried alongside the peer id so [`merge_space_rows`] can
/// stamp each row with the peer that actually reported it. Named so the signatures below stay
/// readable rather than tripping clippy's `type_complexity`.
type PeerSpaceRows = Vec<(NodeId, Option<Vec<(String, u64)>>)>;

// ---------------------------------------------------------------------------
// FlowNet — the per-node subsystem.
// ---------------------------------------------------------------------------

/// Everything flow state needs on one node, shared by the per-imposter stores
/// and the RPC handlers.
pub struct FlowNet {
    shard: FlowShard,
    /// Late-bound, exactly like `PullOnMissInterceptor`: the manager (and its
    /// provider) exists before the node does. `Weak` so this subsystem never
    /// keeps the node alive past shutdown — `RaftNode::Drop` releases the redb
    /// lock and the cluster port.
    node: OnceLock<Weak<RaftNode>>,
    /// Sync→async for the store face (D-9: upstream's `FlowStore` stays
    /// synchronous; the cluster side parks the caller on a bridge). Created at
    /// bind time so an unclustered process never pays for the bridge runtime.
    bridge: OnceLock<Bridge>,
    /// Serializes owner-side read-modify-write. Per node, not per flow: a
    /// coarser lock than strictly needed, chosen for v1 because flow-write
    /// volume is scripts, not the data plane. Sharding it is mechanical if
    /// contention ever shows in `flow_fsync_seconds`.
    rmw: tokio::sync::Mutex<()>,
    /// Adoption markers (#126): the ring index each flow was last verified at.
    /// An owner-side touch of a flow whose marker is behind the current
    /// `m_idx` pulls the flow from its fellow holders first — which is what
    /// makes a takeover serve verified state instead of whatever this node's
    /// replica happened to hold.
    adopted: parking_lot::Mutex<std::collections::HashMap<String, u64>>,
}

/// What [`FlowNet::bind`] needs beyond the node itself.
pub struct FlowBindConfig {
    pub bridge: BridgeConfig,
    /// How often the replica-side repair loop pulls the flows this node holds
    /// but does not own. Tests shorten it; production keeps the default.
    pub anti_entropy_interval: Duration,
}

impl Default for FlowBindConfig {
    fn default() -> Self {
        Self {
            bridge: BridgeConfig::default(),
            anti_entropy_interval: ANTI_ENTROPY_INTERVAL,
        }
    }
}

impl FlowNet {
    #[must_use]
    pub fn new(shard: FlowShard) -> Arc<Self> {
        Arc::new(Self {
            shard,
            node: OnceLock::new(),
            bridge: OnceLock::new(),
            rmw: tokio::sync::Mutex::new(()),
            adopted: parking_lot::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Attach the node once it exists and start the bridge. Binding twice is a
    /// no-op; the second caller wanted what the first one got.
    ///
    /// # Errors
    ///
    /// The bridge's private runtime could not start.
    pub fn bind(
        self: &Arc<Self>,
        node: &Arc<RaftNode>,
        config: FlowBindConfig,
    ) -> std::io::Result<()> {
        let mut start_loop = false;
        if self.bridge.get().is_none() {
            let bridge = Bridge::start(config.bridge)?;
            // A racing second bind loses the set and drops its bridge — same
            // contract as the node slot below. Only the bind that installed the
            // bridge starts the repair loop, so it runs once.
            start_loop = self.bridge.set(bridge).is_ok();
        }
        let _ = self.node.set(Arc::downgrade(node));

        if start_loop && let Some(bridge) = self.bridge.get() {
            // The anti-entropy loop (#126): the replica-side half of repair.
            // Lives on the bridge's own runtime so it needs no ambient one, and
            // holds only weak references — the loop must never keep the node
            // (which owns ports and the redb lock) or the net alive.
            let net = Arc::downgrade(self);
            let interval = config.anti_entropy_interval;
            bridge.handle().spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(net) = net.upgrade() else { break };
                    net.anti_entropy_tick().await;
                }
            });
        }
        Ok(())
    }

    /// The live node and its current ring, or the loud not-ready error.
    fn view(&self) -> Result<(Arc<RaftNode>, Ring), String> {
        let node = self
            .node
            .get()
            .ok_or(NOT_READY_STARTING)?
            .upgrade()
            .ok_or(NOT_READY_SHUT_DOWN)?;
        let ring = node.ring();
        if ring.is_empty() {
            return Err(NOT_READY_NO_MEMBERSHIP.to_owned());
        }
        Ok((node, ring))
    }

    fn shard_value(&self, flow_id: &str, key: &str) -> Option<Value> {
        self.shard.get(flow_id, key).map(|entry| entry.value)
    }

    /// The owner-side write path — the only code that mutates owned flow
    /// state, whether the op arrived locally or over the wire.
    async fn owner_write(self: &Arc<Self>, req: WriteReq) -> WriteReply {
        let (node, ring) = match self.view() {
            Ok(view) => view,
            Err(reason) => return WriteReply::Error { reason },
        };
        if req.m_idx != ring.m_idx() {
            metrics::flow_conflict("fence");
            return WriteReply::Fenced {
                owner_m_idx: ring.m_idx(),
            };
        }
        let owned = OwnedKey::new(KeyClass::FlowKv, &req.flow_id);
        match ring.owner(owned) {
            Some(owner) if owner == node.id() => {}
            Some(owner) => {
                metrics::flow_conflict("misroute");
                return WriteReply::NotOwner { owner };
            }
            None => {
                return WriteReply::Error {
                    reason: NOT_READY_NO_OWNER.to_owned(),
                };
            }
        }

        // D-17's isolated-owner rule: a node that cannot see a quorum must not
        // mutate owned state — a majority that re-homed this key is already
        // serving it, and the `(m_idx, v, origin)` tuple only reconciles the
        // divergence afterwards rather than preventing it. Placed *after* the
        // ownership check so a misroute is still reported as `NotOwner`, and
        // *before* adoption so a partitioned owner opens no RPCs to peers it
        // cannot reach.
        if node.is_isolated() {
            metrics::flow_conflict("isolated");
            return WriteReply::Error {
                reason: ISOLATED_REFUSAL.to_owned(),
            };
        }

        // Verify the copy before first serving it under this membership —
        // outside the RMW lock, because adoption does RPC and holding the
        // per-node lock across the network would stall every other flow's
        // writes for the pull's duration. Racing first-touches both adopt;
        // the merge is idempotent.
        self.ensure_adopted(&node, &ring, &req.flow_id).await;

        let _serialize = self.rmw.lock().await;
        // Re-taken under the lock, deliberately. `is_isolated()` lags a fresh
        // partition by up to `ISOLATION_WINDOW_MS`, and the check above is
        // followed by `ensure_adopted`, whose per-peer RPCs each burn up to the
        // request timeout discovering the peers are gone — so the very call that
        // *first observes* a partition is the one that would otherwise go on to
        // mutate. One watch-borrow bounds the window to the shard write itself.
        if node.is_isolated() {
            metrics::flow_conflict("isolated");
            return WriteReply::Error {
                reason: ISOLATED_REFUSAL.to_owned(),
            };
        }
        let flow = req.flow_id.as_str();
        let expiry = super::shard::expiry_from(
            req.ttl_seconds
                .map(|secs| Duration::from_secs(secs.unsigned_abs())),
        );
        let mint = |prev: Option<&Versioned>, value: Value| Versioned {
            m_idx: ring.m_idx(),
            v: prev.map_or(1, |p| p.v + 1),
            origin: node.id(),
            expires_at: expiry,
            value,
            deleted: false,
        };

        let result: Result<WriteReply, super::shard::ShardError> = match req.op {
            WriteOp::Set { value } => {
                // `get_versioned`, not `get`: a hidden tombstone still carries
                // the highest version, and minting below it would make this
                // write lose the replica merge to the very tombstone it is
                // overwriting — an acknowledged write that never replicates.
                let prev = self.shard.get_versioned(flow, &req.key);
                let entry = mint(prev.as_ref(), value);
                match self
                    .shard
                    .set(flow, &req.key, entry.clone(), req.durability)
                    .await
                {
                    Ok(()) => {
                        self.replicate(
                            &node,
                            &ring,
                            flow,
                            ReplicaOp::Put {
                                key: req.key,
                                entry: entry.clone(),
                            },
                            req.durability,
                        );
                        Ok(WriteReply::Applied {
                            entry: Some(entry),
                            incremented: None,
                            existed: None,
                        })
                    }
                    Err(e) => Err(e),
                }
            }
            WriteOp::Delete => {
                // A delete is a *versioned write* (#126): a tombstone that a
                // delayed replication `Put` loses to by the ordinary version
                // comparison. Minted above the hidden tombstone-aware version,
                // pushed as a `Put` like any other entry — replicas need no
                // delete-specific merge path, so resurrection is structurally
                // impossible rather than guarded.
                let prev = self.shard.get_versioned(flow, &req.key);
                let mut entry = mint(prev.as_ref(), Value::Null);
                entry.deleted = true;
                entry.expires_at = super::shard::expiry_from(Some(TOMBSTONE_TTL));
                match self
                    .shard
                    .set(flow, &req.key, entry.clone(), req.durability)
                    .await
                {
                    Ok(()) => {
                        self.replicate(
                            &node,
                            &ring,
                            flow,
                            ReplicaOp::Put {
                                key: req.key,
                                entry,
                            },
                            req.durability,
                        );
                        Ok(WriteReply::Applied {
                            entry: None,
                            incremented: None,
                            existed: None,
                        })
                    }
                    Err(e) => Err(e),
                }
            }
            WriteOp::Incr { by } => {
                let prev = self.shard.get_versioned(flow, &req.key);
                // Absent ⇒ 0; a non-i64 current value coerces to 0. Upstream's
                // default `increment_by` semantics, kept identical so a script
                // behaves the same on one node and on a fleet. A tombstone is
                // value-absent (counts from 0) while its version still anchors
                // the mint above it.
                let current = prev
                    .as_ref()
                    .filter(|entry| !entry.deleted)
                    .and_then(|entry| entry.value.as_i64())
                    .unwrap_or(0);
                match current.checked_add(by) {
                    None => Ok(WriteReply::Error {
                        reason: format!(
                            "increment_by overflow: {current} + {by} exceeds i64 range"
                        ),
                    }),
                    Some(new_value) => {
                        let entry = mint(prev.as_ref(), Value::from(new_value));
                        match self
                            .shard
                            .set(flow, &req.key, entry.clone(), req.durability)
                            .await
                        {
                            Ok(()) => {
                                self.replicate(
                                    &node,
                                    &ring,
                                    flow,
                                    ReplicaOp::Put {
                                        key: req.key,
                                        entry,
                                    },
                                    req.durability,
                                );
                                Ok(WriteReply::Applied {
                                    entry: None,
                                    incremented: Some(new_value),
                                    existed: None,
                                })
                            }
                            Err(e) => Err(e),
                        }
                    }
                }
            }
            WriteOp::Cas { expected, new } => {
                let prev = self.shard.get_versioned(flow, &req.key);
                // A tombstone is "not present" to CAS semantics — `expected:
                // None` matches a deleted key — while its version anchors the
                // mint above it.
                let current = prev
                    .as_ref()
                    .filter(|entry| !entry.deleted)
                    .map(|entry| entry.value.clone());
                // Canonical-JSON equality, matching upstream's documented CAS
                // comparison contract.
                if current == expected {
                    let entry = mint(prev.as_ref(), new);
                    match self
                        .shard
                        .set(flow, &req.key, entry.clone(), req.durability)
                        .await
                    {
                        Ok(()) => {
                            self.replicate(
                                &node,
                                &ring,
                                flow,
                                ReplicaOp::Put {
                                    key: req.key,
                                    entry: entry.clone(),
                                },
                                req.durability,
                            );
                            Ok(WriteReply::Applied {
                                entry: Some(entry),
                                incremented: None,
                                existed: None,
                            })
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    metrics::flow_conflict("cas");
                    Ok(WriteReply::CasConflict { current })
                }
            }
            WriteOp::SetTtl { ttl_seconds } => {
                if ttl_seconds <= 0 {
                    match self.shard.clear_flow(flow, req.durability).await {
                        Ok(()) => {
                            self.replicate(
                                &node,
                                &ring,
                                flow,
                                ReplicaOp::ClearFlow,
                                req.durability,
                            );
                            Ok(WriteReply::Applied {
                                entry: None,
                                incremented: None,
                                existed: None,
                            })
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    self.rewrite_expiries(&node, &ring, flow, None, ttl_seconds, req.durability)
                        .await
                        .map(|_| WriteReply::Applied {
                            entry: None,
                            incremented: None,
                            existed: None,
                        })
                }
            }
            WriteOp::SetKeyTtl { ttl_seconds } => {
                let existed = self.shard.get(flow, &req.key).is_some();
                if !existed {
                    Ok(WriteReply::Applied {
                        entry: None,
                        incremented: None,
                        existed: Some(false),
                    })
                } else if ttl_seconds <= 0 {
                    // Expire-now is a delete, and a delete is a tombstone —
                    // same reasoning and same wire shape as `WriteOp::Delete`.
                    let prev = self.shard.get_versioned(flow, &req.key);
                    let mut entry = mint(prev.as_ref(), Value::Null);
                    entry.deleted = true;
                    entry.expires_at = super::shard::expiry_from(Some(TOMBSTONE_TTL));
                    match self
                        .shard
                        .set(flow, &req.key, entry.clone(), req.durability)
                        .await
                    {
                        Ok(()) => {
                            self.replicate(
                                &node,
                                &ring,
                                flow,
                                ReplicaOp::Put {
                                    key: req.key,
                                    entry,
                                },
                                req.durability,
                            );
                            Ok(WriteReply::Applied {
                                entry: None,
                                incremented: None,
                                existed: Some(true),
                            })
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    self.rewrite_expiries(
                        &node,
                        &ring,
                        flow,
                        Some(&req.key),
                        ttl_seconds,
                        req.durability,
                    )
                    .await
                    .map(|_| WriteReply::Applied {
                        entry: None,
                        incremented: None,
                        existed: Some(true),
                    })
                }
            }
            WriteOp::ClearFlow => match self.shard.clear_flow(flow, req.durability).await {
                Ok(()) => {
                    self.replicate(&node, &ring, flow, ReplicaOp::ClearFlow, req.durability);
                    Ok(WriteReply::Applied {
                        entry: None,
                        incremented: None,
                        existed: None,
                    })
                }
                Err(e) => Err(e),
            },
        };

        result.unwrap_or_else(|e| WriteReply::Error {
            reason: e.to_string(),
        })
    }

    /// Re-stamp `expires_at` on one key or every key of a flow. Each rewrite
    /// bumps the version so replicas converge on the new expiry.
    async fn rewrite_expiries(
        self: &Arc<Self>,
        node: &Arc<RaftNode>,
        ring: &Ring,
        flow: &str,
        only_key: Option<&str>,
        ttl_seconds: i64,
        durability: Durability,
    ) -> Result<(), super::shard::ShardError> {
        let expiry =
            super::shard::expiry_from(Some(Duration::from_secs(ttl_seconds.unsigned_abs())));
        for (key, prev) in self.shard.flow(flow) {
            if only_key.is_some_and(|k| k != key) {
                continue;
            }
            let entry = Versioned {
                m_idx: ring.m_idx(),
                v: prev.v + 1,
                origin: node.id(),
                expires_at: expiry,
                value: prev.value,
                deleted: prev.deleted,
            };
            self.shard
                .set(flow, &key, entry.clone(), durability)
                .await?;
            self.replicate(node, ring, flow, ReplicaOp::Put { key, entry }, durability);
        }
        Ok(())
    }

    /// Fire-and-forget push to the flow's successors. Failures are logged, not
    /// propagated: replication is redundancy for `local` readers and future
    /// owners, and the write it trails was already durably applied at the
    /// owner — the one copy `strong` consults.
    fn replicate(
        self: &Arc<Self>,
        node: &Arc<RaftNode>,
        ring: &Ring,
        flow_id: &str,
        op: ReplicaOp,
        durability: Durability,
    ) {
        let successors: Vec<NodeId> = ring
            .owners(OwnedKey::new(KeyClass::FlowKv, flow_id), REPLICAS)
            .into_iter()
            .filter(|&id| id != node.id())
            .collect();
        if successors.is_empty() {
            return;
        }
        let body = match serde_json::to_vec(&ReplicateReq {
            flow_id: flow_id.to_owned(),
            op,
            durability,
        }) {
            Ok(body) => body,
            Err(e) => {
                // Wire types are plain data; failing to encode one is a bug,
                // not an environment condition — but a replication push is not
                // worth a panic on the write path. Loud is the floor.
                tracing::error!(error = %e, "flow replicate: request did not encode");
                return;
            }
        };
        let node = Arc::clone(node);
        tokio::spawn(async move {
            for successor in successors {
                if let Err(e) = node
                    .call_member(successor, "POST", REPLICATE_PATH, body.clone())
                    .await
                {
                    tracing::debug!(successor, error = %e, "flow replicate push failed");
                }
            }
        });
    }

    /// A replica applying a pushed op. No fencing — versions decide, so a
    /// delayed push from a deposed owner loses to anything newer on arrival.
    /// Merge one remote entry into the local shard by version. The single
    /// comparison every repair path shares — push replication, adoption, and
    /// anti-entropy — so they cannot disagree about who wins. Returns whether
    /// the remote entry superseded the local record.
    async fn merge_entry(
        &self,
        flow_id: &str,
        key: &str,
        entry: Versioned,
        durability: Durability,
    ) -> bool {
        // One atomic compare-and-install, not a read followed by a write: pushes
        // arrive concurrently, and a check-then-act lets two of them both read the
        // same `current`, both decide they win, and the *older* one land last.
        // `FlowShard::merge` holds the comparison and the install under one lock —
        // and the tombstone case rides the same rule, since a local tombstone
        // simply fails to be superseded by an older Put.
        match self.shard.merge(flow_id, key, entry, durability).await {
            Ok(kept) => kept,
            Err(e) => {
                tracing::error!(error = %e, "flow merge write failed");
                false
            }
        }
    }

    /// Verify this owner's copy of a flow before first serving it under a new
    /// membership (#126): pull the flow from its fellow holders and merge. A
    /// takeover then serves state checked against every surviving replica
    /// instead of whatever this node's own replica happened to hold.
    ///
    /// Lazy and once per `(flow, m_idx)`: the marker is stamped on success, so
    /// steady-state owner ops pay one map lookup. An unreachable fleet leaves
    /// the marker unstamped — the op proceeds on the local copy (bounded
    /// staleness, RFC-001 §7.2.3's honest bound), counted and logged, and the
    /// next touch retries the pull.
    async fn ensure_adopted(&self, node: &Arc<RaftNode>, ring: &Ring, flow_id: &str) {
        let m_idx = ring.m_idx();
        if self.adopted.lock().get(flow_id) == Some(&m_idx) {
            return;
        }

        let holders: Vec<NodeId> = ring
            .owners(OwnedKey::new(KeyClass::FlowKv, flow_id), REPLICAS)
            .into_iter()
            .filter(|&id| id != node.id())
            .collect();
        if holders.is_empty() {
            // Solo: nothing to consult, and nothing to be stale against.
            self.adopted.lock().insert(flow_id.to_owned(), m_idx);
            return;
        }

        let body = match serde_json::to_vec(&SyncReq {
            flow_ids: vec![flow_id.to_owned()],
        }) {
            Ok(body) => body,
            Err(e) => {
                tracing::error!(error = %e, "flow adoption: request did not encode");
                return;
            }
        };

        let mut reached = false;
        let mut entries_seen = 0usize;
        for holder in holders {
            match node
                .call_member(holder, "POST", SYNC_PATH, body.clone())
                .await
            {
                Ok(reply) => match serde_json::from_slice::<SyncReply>(&reply) {
                    Ok(reply) => {
                        reached = true;
                        for (flow, entries) in reply.flows {
                            for (key, entry) in entries {
                                entries_seen += 1;
                                self.merge_entry(&flow, &key, entry, REPAIR_DURABILITY)
                                    .await;
                            }
                        }
                    }
                    Err(e) => tracing::warn!(holder, error = %e, "flow adoption: bad sync reply"),
                },
                Err(e) => tracing::debug!(holder, error = %e, "flow adoption: holder unreachable"),
            }
        }

        if reached {
            metrics::flow_adoption(if entries_seen > 0 { "found" } else { "empty" });
            self.adopted.lock().insert(flow_id.to_owned(), m_idx);
        } else {
            // Serve anyway, on the local copy: a takeover during a partition
            // must not turn every flow op into an error. The staleness bound is
            // one replication round (§7.2.3), the marker stays unstamped so the
            // next touch retries, and the count is the observable.
            metrics::flow_adoption("unreachable");
            tracing::warn!(
                flow_id,
                m_idx,
                "flow adoption: no fellow holder reachable; serving local copy unverified"
            );
        }
    }

    /// One pass of the replica-side repair loop (#126): pull every flow this
    /// node holds but does not own from its owner, and merge. A replica that
    /// missed a push converges one tick later; flows it never heard of at all
    /// repair at takeover instead (`ensure_adopted`), because a replica cannot
    /// pull what it does not know exists.
    async fn anti_entropy_tick(self: &Arc<Self>) {
        let Ok((node, ring)) = self.view() else {
            return;
        };

        // Group the flows this node holds by their current owner.
        let mut by_owner: std::collections::HashMap<NodeId, Vec<String>> =
            std::collections::HashMap::new();
        for flow_id in self.shard.flow_ids() {
            let owned = OwnedKey::new(KeyClass::FlowKv, &flow_id);
            match ring.owner(owned) {
                Some(owner) if owner != node.id() => {
                    by_owner.entry(owner).or_default().push(flow_id);
                }
                _ => {}
            }
        }

        for (owner, flows) in by_owner {
            for chunk in flows.chunks(SYNC_CHUNK) {
                let body = match serde_json::to_vec(&SyncReq {
                    flow_ids: chunk.to_vec(),
                }) {
                    Ok(body) => body,
                    Err(e) => {
                        tracing::error!(error = %e, "flow anti-entropy: request did not encode");
                        return;
                    }
                };
                match node.call_member(owner, "POST", SYNC_PATH, body).await {
                    Ok(reply) => match serde_json::from_slice::<SyncReply>(&reply) {
                        Ok(reply) => {
                            for (flow, entries) in reply.flows {
                                for (key, entry) in entries {
                                    if self
                                        .merge_entry(&flow, &key, entry, REPAIR_DURABILITY)
                                        .await
                                    {
                                        metrics::flow_repair();
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(owner, error = %e, "flow anti-entropy: bad sync reply");
                        }
                    },
                    // An unreachable owner is next tick's problem; the loop is
                    // the retry.
                    Err(e) => {
                        tracing::debug!(owner, error = %e, "flow anti-entropy: owner unreachable")
                    }
                }
            }
        }
    }

    async fn apply_replica(&self, req: ReplicateReq) {
        match req.op {
            ReplicaOp::Put { key, entry } => {
                self.merge_entry(&req.flow_id, &key, entry, req.durability)
                    .await;
            }
            ReplicaOp::Delete { key } => {
                if let Err(e) = self.shard.delete(&req.flow_id, &key, req.durability).await {
                    tracing::error!(error = %e, "flow replica delete failed");
                }
            }
            ReplicaOp::ClearFlow => {
                if let Err(e) = self.shard.clear_flow(&req.flow_id, req.durability).await {
                    tracing::error!(error = %e, "flow replica clear failed");
                }
            }
        }
    }

    /// Run one clustered op from a blocking thread: route to the owner (self or
    /// remote) and wait for the reply, bounded by [`FLOW_OP_DEADLINE`].
    fn blocking_write(
        self: &Arc<Self>,
        req_for: impl FnOnce(u64) -> WriteReq,
    ) -> anyhow::Result<WriteReply> {
        let bridge = self
            .bridge
            .get()
            .ok_or_else(|| unavailable(NOT_READY_STARTING))?;
        let net = Arc::clone(self);
        let (_, ring) = self.view().map_err(unavailable)?;
        let req = req_for(ring.m_idx());

        // `ScriptPool`, whichever surface called: the engine offloads blocking
        // stores off its workers (`run_flow_blocking`), so the calling thread
        // here is a blocking-pool or script thread, never a tokio worker.
        bridge
            .call(CallerClass::ScriptPool, FLOW_OP_DEADLINE, async move {
                let (node, ring) = net.view().map_err(rpc_not_ready)?;
                let key = OwnedKey::new(KeyClass::FlowKv, &req.flow_id);
                let owner = ring
                    .owner(key)
                    .ok_or_else(|| rpc_not_ready(NOT_READY_NO_OWNER.to_owned()))?;
                if owner == node.id() {
                    Ok(net.owner_write(req).await)
                } else {
                    let body =
                        serde_json::to_vec(&req).map_err(|e| RpcError::Handler(e.to_string()))?;
                    let reply = node
                        .call_member_typed(owner, "POST", WRITE_PATH, body)
                        .await?;
                    serde_json::from_slice(&reply).map_err(|e| RpcError::Handler(e.to_string()))
                }
            })
            .map_err(store_error)
    }

    /// A strong read from a blocking thread: owner-answered, wherever the owner
    /// is.
    fn blocking_read(
        self: &Arc<Self>,
        flow_id: &str,
        key: &str,
    ) -> anyhow::Result<Option<Versioned>> {
        let bridge = self
            .bridge
            .get()
            .ok_or_else(|| unavailable(NOT_READY_STARTING))?;
        let net = Arc::clone(self);
        let req = GetReq {
            flow_id: flow_id.to_owned(),
            key: key.to_owned(),
        };

        bridge
            .call(CallerClass::ScriptPool, FLOW_OP_DEADLINE, async move {
                let (node, ring) = net.view().map_err(rpc_not_ready)?;
                let owned = OwnedKey::new(KeyClass::FlowKv, &req.flow_id);
                let owner = ring
                    .owner(owned)
                    .ok_or_else(|| rpc_not_ready(NOT_READY_NO_OWNER.to_owned()))?;
                if owner == node.id() {
                    // D-17: an owner-answered `strong` read is an owner-side
                    // serve, so an isolated node refuses it rather than answer
                    // a value a healed majority may already disagree with.
                    // Before the counter: a refusal is not a read this node
                    // served. `local` reads never reach this branch (D-10).
                    if node.is_isolated() {
                        return Err(RpcError::Unavailable {
                            detail: ISOLATED_REFUSAL.to_owned(),
                            op_id: None,
                        });
                    }
                    metrics::flow_read("owner");
                    net.ensure_adopted(&node, &ring, &req.flow_id).await;
                    Ok(net.shard.get(&req.flow_id, &req.key))
                } else {
                    metrics::flow_read("forward");
                    let body =
                        serde_json::to_vec(&req).map_err(|e| RpcError::Handler(e.to_string()))?;
                    let reply = node
                        .call_member_typed(owner, "POST", GET_PATH, body)
                        .await?;
                    let reply: GetReply = serde_json::from_slice(&reply)
                        .map_err(|e| RpcError::Handler(e.to_string()))?;
                    Ok(reply.entry)
                }
            })
            .map_err(store_error)
    }

    /// This node's OWNED share of `numberOfFlowEntries` (#372): the live entry
    /// count of every flow this node currently owns, charged per port for the
    /// imposter-scoped ones (`i<port>:`) whose port is in `ports` and per tenant
    /// for the tenant-scoped ones (`t<tenant>:`, #288/#413) whose tenant is in
    /// `tenants` — one pass over the shard for both, because a tenant listing
    /// asks for both at once and `entries_by_flow` is the whole shard.
    ///
    /// Filtered to the **owner** only, never every holder of a copy: a flow keeps
    /// [`REPLICAS`] copies for durability, so a raw per-node sum (this node's plus
    /// every peer's) would count each entry up to [`REPLICAS`] times. Restricting
    /// to the owner makes the fleet-wide sum in [`Self::fleet_usage_counts`]
    /// exactly the union of live entries, with no coordination beyond the ring
    /// every node already agrees on.
    ///
    /// Fleet-scoped (`f:`) flows match neither [`imposter_port`] nor [`tenant_of`],
    /// so they contribute to nothing — they are shared by construction, and
    /// charging them to any one imposter or tenant would be arbitrary. The
    /// unresolved placeholders (`i?:`, `t??:`) name nothing to charge either.
    fn owned_counts(
        &self,
        node: &RaftNode,
        ring: &Ring,
        ports: &[u16],
        tenants: &[String],
    ) -> FlowCounts {
        let wanted_ports: HashSet<u16> = ports.iter().copied().collect();
        let wanted_tenants: HashSet<&str> = tenants.iter().map(String::as_str).collect();
        // Seeded with every requested key at `0`, not only the ones this node
        // happens to own something for: `fleet_usage_counts` folds a peer's
        // reply into these maps by key, and a key absent here would silently
        // drop that peer's count instead of adding to zero.
        let mut counts = FlowCounts {
            by_port: ports.iter().map(|&port| (port, 0)).collect(),
            by_tenant: tenants.iter().map(|t| (t.clone(), 0)).collect(),
        };
        for (flow_id, count) in self.shard.entries_by_flow() {
            let charge_port = imposter_port(&flow_id).filter(|port| wanted_ports.contains(port));
            let charge_tenant = if charge_port.is_none() {
                tenant_of(&flow_id).filter(|tenant| wanted_tenants.contains(tenant))
            } else {
                None
            };
            if charge_port.is_none() && charge_tenant.is_none() {
                continue;
            }
            let owned = OwnedKey::new(KeyClass::FlowKv, &flow_id);
            if ring.owner(owned) != Some(node.id()) {
                continue;
            }
            if let Some(port) = charge_port {
                *counts.by_port.entry(port).or_insert(0) += count as u64;
            } else if let Some(tenant) = charge_tenant {
                *counts.by_tenant.entry(tenant.to_owned()).or_insert(0) += count as u64;
            }
        }
        counts
    }

    /// `numberOfFlowEntries` for a batch of ports (#372): this node's own owned
    /// share, plus a budgeted fan-out to every other ring member for theirs —
    /// the same shape as [`super::journal_net::JournalNet::fleet_counts`]. Flow
    /// entries carry no analogue of a journal port's clear generation, but the
    /// ring's own `m_idx` (RFC-001 §7.6) is exactly that kind of gate: each
    /// peer answers under its own ring, and a membership change in flight can
    /// leave rings disagreeing about who owns a flow, so the fan-out stamps
    /// its caller `m_idx` on every request and a peer whose ring has since
    /// diverged refuses rather than answer under a view the caller never
    /// asked for (see the `COUNTS_PATH` handler).
    ///
    /// `partial` is `true` when any peer's answer was missing (an error, a
    /// panicked task, a ring-divergence refusal, or one still outstanding when
    /// `budget` expired) — the same "never a fabricated zero" contract
    /// [`JournalNet::fleet_counts`] upholds. A local ring that is not yet
    /// available (still starting, or no applied membership) answers an empty
    /// map with `partial: true` rather than a count this node cannot vouch
    /// for.
    #[must_use]
    pub async fn fleet_entry_counts(
        self: &Arc<Self>,
        ports: &[u16],
        budget: Duration,
    ) -> (HashMap<u16, u64>, bool) {
        let (counts, partial) = self.fleet_usage_counts(ports, &[], budget).await;
        (counts.by_port, partial)
    }

    /// [`Self::fleet_entry_counts`], plus the tenant-scoped flows of `tenants`
    /// charged per tenant (#413) — one fan-out for both, because a tenant listing
    /// needs both and every peer's answer is one shard scan either way. Same
    /// `partial` contract. A peer still on a build before the tenant half
    /// answers only the port half; the tenant sum is then a floor for the length
    /// of that rolling upgrade, which is the shape any additive wire field has.
    #[must_use]
    pub async fn fleet_usage_counts(
        self: &Arc<Self>,
        ports: &[u16],
        tenants: &[String],
        budget: Duration,
    ) -> (FlowCounts, bool) {
        // Nothing to fan out for: a tenant with no imposters and no
        // tenant-scoped flows to ask about has a locally computable (empty)
        // answer, and asking every peer anyway would stamp `partial: true` on
        // a `0/0/0` body the instant any one of them failed to answer in time
        // — for no reason, since there is nothing for their answers to add.
        if ports.is_empty() && tenants.is_empty() {
            return (FlowCounts::default(), false);
        }

        let (node, ring) = match self.view() {
            Ok(view) => view,
            Err(reason) => {
                tracing::warn!(error = %reason, "flow entry counts: cluster view unavailable");
                return (FlowCounts::default(), true);
            }
        };
        let mut totals = self.owned_counts(&node, &ring, ports, tenants);

        let peers: Vec<NodeId> = ring
            .members()
            .iter()
            .copied()
            .filter(|&id| id != node.id())
            .collect();
        if peers.is_empty() {
            return (totals, false);
        }

        let caller_m_idx = ring.m_idx();
        let mut set = tokio::task::JoinSet::new();
        for peer in peers.iter().copied() {
            let node = Arc::clone(&node);
            let req_ports = ports.to_vec();
            let req_tenants = tenants.to_vec();
            set.spawn(async move {
                let outcome = async {
                    let body = serde_json::to_vec(&CountsReq {
                        ports: req_ports,
                        tenants: req_tenants,
                        m_idx: caller_m_idx,
                    })
                    .map_err(|e| e.to_string())?;
                    let reply = node
                        .call_member(peer, "POST", COUNTS_PATH, body)
                        .await
                        .map_err(|e| e.to_string())?;
                    serde_json::from_slice::<CountsReply>(&reply).map_err(|e| e.to_string())
                }
                .await;
                (peer, outcome)
            });
        }

        let mut partial = false;
        let mut answered: HashSet<NodeId> = HashSet::new();
        let drained = tokio::time::timeout(budget, async {
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok((peer, Ok(reply))) => {
                        answered.insert(peer);
                        merge_peer_counts(&mut totals.by_port, &reply.slots);
                        merge_peer_counts(&mut totals.by_tenant, &reply.tenant_slots);
                    }
                    Ok((peer, Err(e))) => {
                        answered.insert(peer);
                        tracing::warn!(peer, error = %e, "flow entry counts: peer pull failed");
                        partial = true;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "flow entry counts: peer pull task panicked");
                        partial = true;
                    }
                }
            }
        })
        .await;
        if drained.is_err() {
            partial = true;
            set.abort_all();
            for &peer in peers.iter().filter(|peer| !answered.contains(peer)) {
                tracing::warn!(peer, "flow entry counts: peer pull lost to budget");
            }
        }

        (totals, partial)
    }

    /// The fleet-wide list of correlated-isolation spaces held under `prefix` (#374): this node's
    /// owned share, plus a budgeted fan-out to every other ring member for theirs — the identical
    /// shape [`Self::fleet_entry_counts`] uses for totals, right down to the `m_idx` ring-divergence
    /// gate (see the `SPACES_PATH` handler below), because a space listing has the same correctness
    /// requirement a count total does: filtered to the owner so the union is duplicate-free by
    /// construction, never a per-node sum that could double- or under-count a replicated flow.
    ///
    /// `prefix` is the caller's already-rendered [`super::flow_config::ContextScope::prefix_for`]
    /// output, not a port: a fleet-scoped listing's prefix (`f:`) names no port at all, and this
    /// function has no business re-deriving what the admin front already resolved from the
    /// imposter's own config.
    ///
    /// `partial` carries [`Self::fleet_entry_counts`]'s identical contract: `true` when any peer's
    /// answer is missing (an error, a panicked task, a ring-divergence refusal, or one still
    /// outstanding when `budget` expires) — never a fabricated empty list standing in for "no
    /// spaces". A local ring that is not yet available answers `(vec![], true)` for the same reason.
    #[must_use]
    pub async fn fleet_spaces(
        self: &Arc<Self>,
        port: u16,
        prefix: &str,
        budget: Duration,
    ) -> (Vec<SpaceRow>, bool) {
        let (node, ring) = match self.view() {
            Ok(view) => view,
            Err(reason) => {
                tracing::warn!(error = %reason, "flow spaces: cluster view unavailable");
                return (Vec::new(), true);
            }
        };
        let me = node.id();
        let local = owned_spaces(
            &self.shard.entries_by_flow(),
            prefix,
            |scoped_id| ring.owner(OwnedKey::new(KeyClass::FlowKv, scoped_id)),
            me,
        );

        let peers: Vec<NodeId> = ring
            .members()
            .iter()
            .copied()
            .filter(|&id| id != me)
            .collect();
        if peers.is_empty() {
            return merge_space_rows(local, me, Vec::new(), false);
        }

        let caller_m_idx = ring.m_idx();
        let mut set = tokio::task::JoinSet::new();
        for peer in peers.iter().copied() {
            let node = Arc::clone(&node);
            let req = SpacesReq {
                port,
                prefix: prefix.to_owned(),
                m_idx: caller_m_idx,
            };
            set.spawn(async move {
                let outcome = async {
                    let body = serde_json::to_vec(&req).map_err(|e| e.to_string())?;
                    let reply = node
                        .call_member(peer, "POST", SPACES_PATH, body)
                        .await
                        .map_err(|e| e.to_string())?;
                    serde_json::from_slice::<SpacesReply>(&reply).map_err(|e| e.to_string())
                }
                .await;
                (peer, outcome)
            });
        }

        let mut peer_rows: PeerSpaceRows = Vec::new();
        let mut answered: HashSet<NodeId> = HashSet::new();
        let drained = tokio::time::timeout(budget, async {
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok((peer, Ok(reply))) => {
                        answered.insert(peer);
                        peer_rows.push((peer, Some(reply.rows)));
                    }
                    Ok((peer, Err(e))) => {
                        answered.insert(peer);
                        tracing::warn!(peer, error = %e, "flow spaces: peer pull failed");
                        peer_rows.push((peer, None));
                    }
                    Err(e) => {
                        // No peer id survives a panicked join (`JoinSet` erases it on that path);
                        // the unanswered-peer sweep below is what accounts for this one instead.
                        tracing::warn!(error = %e, "flow spaces: peer pull task panicked");
                    }
                }
            }
        })
        .await;
        let mut timed_out = false;
        if drained.is_err() {
            timed_out = true;
            set.abort_all();
        }
        // Covers both a budget cutoff (peers still outstanding when `drained` failed) and a
        // panicked task (never recorded above): either way the peer is not in `answered`, and its
        // share is unknown rather than empty.
        for &peer in peers.iter().filter(|peer| !answered.contains(peer)) {
            tracing::warn!(peer, "flow spaces: peer pull lost (timeout or panic)");
            peer_rows.push((peer, None));
        }

        merge_space_rows(local, me, peer_rows, timed_out)
    }
}

/// Fold one peer's `POST /_cluster/flow/counts` reply into the running fleet-wide per-port totals
/// (issue #401).
///
/// Extracted out of `fleet_entry_counts`'s fan-out so the merge arithmetic is under direct test —
/// the existing multi-node wire tests only kill or diverge the peer, so this line (an *answering*
/// peer's counts actually landing in the total) previously ran under no test at all.
///
/// A port absent from `totals` is one this node never asked about (see
/// `fleet_entry_counts`'s "nothing to fan out for" guard, which means `totals` only ever holds the
/// caller's requested ports) and is skipped rather than inserted — growing the map to a port
/// nobody requested would put a key the caller cannot use into the response. `saturating_add`
/// matches the totals' own construction in `owned_counts`.
fn merge_peer_counts<K: std::hash::Hash + Eq>(
    totals: &mut HashMap<K, u64>,
    peer_slots: &[(K, u64)],
) {
    for (key, count) in peer_slots {
        if let Some(total) = totals.get_mut(key) {
            *total = total.saturating_add(*count);
        }
    }
}

/// The tenant a tenant-scoped flow id belongs to (`t<tenant>:` → `<tenant>`,
/// #288), or `None` for every other namespace and for the unresolved `t??:`
/// placeholder — which names no real tenant to charge, so its entries are
/// charged to nobody, exactly like `i?:`'s (#413).
fn tenant_of(scoped_flow_id: &str) -> Option<&str> {
    scoped_flow_id
        .strip_prefix('t')
        .and_then(|rest| rest.split_once(':'))
        .map(|(tenant, _)| tenant)
        .filter(|tenant| !tenant.is_empty() && !tenant.starts_with('?'))
}

/// The imposter port a scoped flow id belongs to, or `None` for a fleet-scoped
/// (`f:`) id, a tenant-scoped (`t<tenant>:`, #288) id, or the unreachable `i?:`
/// placeholder namespace ([`ContextScope::prefix_for`]) — all deliberately
/// excluded from any port's count: a fleet-scoped flow is shared by
/// construction, so charging its entries to any one imposter's port would be
/// arbitrary; a tenant-scoped flow is shared across its tenant's ports the same
/// way (charging it to the *tenant* is issue #413); and the placeholder names
/// no real port to charge at all.
fn imposter_port(scoped_flow_id: &str) -> Option<u16> {
    scoped_flow_id
        .strip_prefix('i')
        .and_then(|rest| rest.split_once(':'))
        .and_then(|(port, _)| port.parse().ok())
}

/// This node's owned share of the spaces under `prefix` (#374): keep an entry whose scoped id
/// starts with `prefix` **and** whose owner (per `owner_of`, closing over the ring) is `me`, and
/// return it with the prefix stripped exactly once.
///
/// `strip_prefix`, never `replace`/`trim_start_matches`: a caller-chosen flow id can itself look
/// like a scope prefix (`i6400:i6400:foo`, scoped becomes `i6400:i6400:foo`), and stripping every
/// occurrence would hand back `foo` — a different space than the one stripped once, `i6400:foo`.
///
/// Pure and free-standing rather than a method on [`FlowNet`], deliberately: the gate tests pin
/// this filter and the prefix arithmetic without standing a ring (or even a `FlowNet`) up at all.
fn owned_spaces(
    entries: &[(String, usize)],
    prefix: &str,
    owner_of: impl Fn(&str) -> Option<NodeId>,
    me: NodeId,
) -> Vec<(String, u64)> {
    entries
        .iter()
        .filter_map(|(scoped_id, count)| {
            let unscoped = scoped_id.strip_prefix(prefix)?;
            if owner_of(scoped_id) != Some(me) {
                return None;
            }
            Some((unscoped.to_owned(), *count as u64))
        })
        .collect()
}

/// The union of this node's owned share and every peer's (#374): [`SpaceRow`]s stamped with
/// `me` for `local` and with **that peer's own id** for its rows — never `me` for all of them,
/// which is exactly the transposition bug run 32 shipped (see the test this doc references).
///
/// Duplicate-free by construction, not by a dedup pass: each flow has exactly one owner, so the
/// union of every node's owned share can never name the same space twice under two different
/// owners, and `local`/each peer's share can never name the same space under itself either.
///
/// `partial` is `timed_out || any peer's answer was `None`` — the identical polarity
/// [`FlowNet::fleet_entry_counts`] uses, so a `[]` result and a genuinely-empty imposter cannot be
/// told apart from a `[]` a fan-out simply failed to complete: `partial: true` in the latter case is
/// what keeps the console from stating "no spaces" as fact when the truth is "cannot tell you".
///
/// Pure and free-standing for the same reason [`owned_spaces`] is: testable without a cluster.
fn merge_space_rows(
    local: Vec<(String, u64)>,
    me: NodeId,
    peers: PeerSpaceRows,
    timed_out: bool,
) -> (Vec<SpaceRow>, bool) {
    let mut rows: Vec<SpaceRow> = local
        .into_iter()
        .map(|(space, entry_count)| SpaceRow {
            space,
            entry_count,
            owner: me,
        })
        .collect();
    let mut partial = timed_out;
    for (peer, answer) in peers {
        match answer {
            Some(peer_rows) => {
                rows.extend(peer_rows.into_iter().map(|(space, entry_count)| SpaceRow {
                    space,
                    entry_count,
                    owner: peer,
                }))
            }
            None => partial = true,
        }
    }
    (rows, partial)
}

/// The wire surface: six POST routes on the cluster port, HMAC-authed and
/// version-negotiated by the transport like every other route.
#[must_use]
pub fn flow_routes(net: Arc<FlowNet>) -> Router {
    let get_net = Arc::clone(&net);
    let write_net = Arc::clone(&net);
    let replicate_net = Arc::clone(&net);
    let sync_net = Arc::clone(&net);
    let counts_net = Arc::clone(&net);
    let spaces_net = net;

    Router::new()
        .route(
            "POST",
            GET_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&get_net);
                Box::pin(async move {
                    let req: GetReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("flow/get decode: {e}")))?;
                    // Served from the local shard without an ownership check:
                    // the *caller* routed here because its ring said so, and a
                    // membership-change race at worst answers from a
                    // just-deposed owner — the same staleness a `local` read
                    // accepts by contract. Not counted in `flow_reads_total`:
                    // the sender already counted this read as `forward`, and
                    // double-counting would make `owner` mean two things.
                    //
                    // Adoption runs here too (#126): a forwarded strong read is
                    // an owner-side serve, and a fresh owner must verify its
                    // copy before answering with it.
                    // Refuses rather than answering when there is no view, like
                    // every sibling route here: `view()` fails when this node is
                    // unbound, shutting down, or has no applied membership — and
                    // that last state is one `is_isolated()` itself calls isolated
                    // (`uninitialized_node_is_isolated_with_empty_ring`). Serving
                    // from the local shard there would skip the gate in exactly
                    // the state the gate exists for, and answer `None` for a key
                    // this node may simply not have loaded yet, which reads to the
                    // caller as "absent" rather than as a failure.
                    let (node, ring) = net.view().map_err(rpc_not_ready)?;
                    // D-17: the ownership check is deliberately absent here (the
                    // caller routed to us), but isolation is a property of *this*
                    // node rather than of the route — a forwarded owner-read
                    // landing on a node that cannot see a quorum is the
                    // minority-side serve the rule exists to refuse.
                    if node.is_isolated() {
                        return Err(RpcError::Unavailable {
                            detail: ISOLATED_REFUSAL.to_owned(),
                            op_id: None,
                        });
                    }
                    net.ensure_adopted(&node, &ring, &req.flow_id).await;
                    let reply = GetReply {
                        entry: net.shard.get(&req.flow_id, &req.key),
                    };
                    serde_json::to_vec(&reply).map_err(|e| RpcError::Handler(e.to_string()))
                })
            }),
        )
        .route(
            "POST",
            WRITE_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&write_net);
                Box::pin(async move {
                    let req: WriteReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("flow/write decode: {e}")))?;
                    let reply = net.owner_write(req).await;
                    serde_json::to_vec(&reply).map_err(|e| RpcError::Handler(e.to_string()))
                })
            }),
        )
        .route(
            "POST",
            REPLICATE_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&replicate_net);
                Box::pin(async move {
                    let req: ReplicateReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("flow/replicate decode: {e}")))?;
                    net.apply_replica(req).await;
                    Ok(b"{}".to_vec())
                })
            }),
        )
        .route(
            "POST",
            SYNC_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&sync_net);
                Box::pin(async move {
                    let req: SyncReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("flow/sync decode: {e}")))?;
                    // Serves whatever this shard holds for the asked flows —
                    // tombstones included, because a repair that omitted
                    // deletions would resurrect what they bury. No ownership
                    // check: adoption deliberately asks *fellow replicas*, and
                    // the answer is versioned, so a stale holder's reply loses
                    // the merge rather than corrupting anyone.
                    let flows = req
                        .flow_ids
                        .iter()
                        .map(|flow_id| (flow_id.clone(), net.shard.flow(flow_id)))
                        .collect();
                    serde_json::to_vec(&SyncReply { flows })
                        .map_err(|e| RpcError::Handler(e.to_string()))
                })
            }),
        )
        .route(
            "POST",
            COUNTS_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&counts_net);
                Box::pin(async move {
                    let req: CountsReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("flow/counts decode: {e}")))?;
                    // Errors rather than answering an unfiltered (and therefore
                    // over-counted) local sum: without a ring there is no way to
                    // tell an owned entry from a replica's copy, and a wrong
                    // count that looks like a real one is worse than the asker
                    // marking this peer's contribution `partial`.
                    let (node, ring) = net.view().map_err(RpcError::Handler)?;
                    // The caller computed `owned_counts` decisions under
                    // its own ring; answering under a *different* one here is
                    // exactly how a flow gets claimed by two nodes (both
                    // think they own it) or by none (both think the other
                    // does) during a membership change. Refusing rather than
                    // guessing routes this into the same peer-failure path
                    // every other unreachable/erroring peer takes, so the
                    // caller marks its answer `partial` instead of trusting a
                    // count computed under a view it never asked for.
                    if req.m_idx != ring.m_idx() {
                        return Err(RpcError::Handler(format!(
                            "flow/counts: ring diverged (caller m_idx {}, this node {})",
                            req.m_idx,
                            ring.m_idx()
                        )));
                    }
                    let totals = net.owned_counts(&node, &ring, &req.ports, &req.tenants);
                    let slots = req
                        .ports
                        .iter()
                        .map(|&port| (port, totals.by_port.get(&port).copied().unwrap_or(0)))
                        .collect();
                    let tenant_slots = req
                        .tenants
                        .iter()
                        .map(|tenant| {
                            (
                                tenant.clone(),
                                totals.by_tenant.get(tenant).copied().unwrap_or(0),
                            )
                        })
                        .collect();
                    serde_json::to_vec(&CountsReply {
                        slots,
                        tenant_slots,
                    })
                    .map_err(|e| RpcError::Handler(e.to_string()))
                })
            }),
        )
        .route(
            "POST",
            SPACES_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let net = Arc::clone(&spaces_net);
                Box::pin(async move {
                    let req: SpacesReq = serde_json::from_slice(&body)
                        .map_err(|e| RpcError::Handler(format!("flow/spaces decode: {e}")))?;
                    // Same refusal as `COUNTS_PATH`, and the identical reason: without a ring
                    // there is no way to tell an owned space from a replica's copy, and answering
                    // an unfiltered local scan would be exactly the wrong-but-quiet failure
                    // `owned_spaces`'s caller depends on this handler never producing.
                    let (node, ring) = net.view().map_err(RpcError::Handler)?;
                    // `COUNTS_PATH`'s divergence gate, unchanged: the caller computed its own
                    // share under its own ring, and answering under a different one here is how a
                    // flow ends up claimed by two nodes or by none during a membership change.
                    if req.m_idx != ring.m_idx() {
                        return Err(RpcError::Handler(format!(
                            "flow/spaces: ring diverged for port {} (caller m_idx {}, this node {})",
                            req.port,
                            req.m_idx,
                            ring.m_idx()
                        )));
                    }
                    let me = node.id();
                    let rows = owned_spaces(
                        &net.shard.entries_by_flow(),
                        &req.prefix,
                        |scoped_id| ring.owner(OwnedKey::new(KeyClass::FlowKv, scoped_id)),
                        me,
                    );
                    serde_json::to_vec(&SpacesReply { rows })
                        .map_err(|e| RpcError::Handler(e.to_string()))
                })
            }),
        )
}

// ---------------------------------------------------------------------------
// The per-imposter store face.
// ---------------------------------------------------------------------------

/// What the engine sees: upstream's synchronous [`FlowStore`], parameterized by
/// one imposter's [`FlowConfig`], all real work delegated to the shared
/// [`FlowNet`].
pub struct ClusteredFlowStore {
    net: Arc<FlowNet>,
    config: FlowConfig,
    /// This imposter's port, which with [`FlowConfig::scope`] decides the
    /// flow-id namespace (#152).
    ///
    /// The namespace is applied *at the store face* and nowhere below it: shard
    /// tables, HRW ownership, replication, anti-entropy, adoption markers and
    /// the admin `flow_get`/`flow_set` routes all consume whatever id the store
    /// hands them, so scoping here covers every one of those paths uniformly and
    /// none of them has to learn the concept. It also settles the admission
    /// recorded at `REPAIR_DURABILITY` above — a repair path could not
    /// previously tell which imposter a `flow_id` belonged to; now the id itself
    /// says.
    port: Option<u16>,
    /// The tenant that owns this imposter's port (#288), consulted only when
    /// [`FlowConfig::scope`] is [`ContextScope::Tenant`] — every other scope
    /// ignores it, the same way `port` is unused by `Fleet`.
    ///
    /// Filled on the first *successful* lookup and never on a failed one:
    /// `provide` tries eagerly, so the ordinary store resolves exactly once,
    /// but a lookup that fails there (the node handle not yet installed, a
    /// storage error, the config row not yet visible) is retried on the next
    /// op instead of being cached — a store that remembered a transient
    /// failure would render its defensive `t??:` namespace for its whole
    /// lifetime, silently, and that is a wrong namespace, not a degraded one.
    tenant: OnceLock<TenantId>,
    /// Whether an unresolved-tenant failure has been logged at error level for
    /// this store already. The retry is per op (see `tenant`), but the log
    /// must not be: a store whose tenant never resolves — a node handle that
    /// was never bound, a persistent storage error — would otherwise emit one
    /// error line per flow read and write on the request path. First failure
    /// at error, the rest at debug, until a lookup succeeds.
    tenant_failure_logged: std::sync::atomic::AtomicBool,
}

impl ClusteredFlowStore {
    /// Held as scope+port+tenant rather than a rendered prefix so this shares
    /// [`ContextScope::scoped_flow_id`] with the admin front's ownership lookup
    /// (#359). Two renderings of this key would be two answers to "who owns this
    /// flow", and the wrong one sends an operator to the wrong node.
    fn scoped(&self, flow_id: &str) -> String {
        let tenant = if self.config.scope == ContextScope::Tenant {
            self.tenant()
        } else {
            None
        };
        self.config
            .scope
            .scoped_flow_id(self.port, tenant.map(TenantId::as_str), flow_id)
    }

    /// The owning tenant, resolved on first use and cached on success only (see
    /// the field's doc). `None` when it cannot be resolved *right now*; the
    /// caller renders the defensive prefix and the next op tries again.
    fn tenant(&self) -> Option<&TenantId> {
        if let Some(tenant) = self.tenant.get() {
            return Some(tenant);
        }
        let loudly = !self
            .tenant_failure_logged
            .swap(true, std::sync::atomic::Ordering::Relaxed);
        let resolved = resolve_owning_tenant(&self.net, self.port, loudly)?;
        Some(self.tenant.get_or_init(|| resolved))
    }

    fn write(&self, flow_id: &str, key: &str, op: WriteOp) -> anyhow::Result<WriteReply> {
        let (flow_id, key) = (self.scoped(flow_id), key.to_owned());
        let config = self.config;
        self.net.blocking_write(move |m_idx| WriteReq {
            flow_id,
            key,
            op,
            ttl_seconds: config.ttl_seconds,
            durability: config.durability,
            m_idx,
        })
    }

    /// Collapse a reply into "applied or a real error". Fencing is an error at
    /// this face: the script asked for a write and it did not happen; quietly
    /// succeeding would be the silent-drop failure mode.
    ///
    /// D-65: the refusals that say "not now, retry" — a fence, a misroute, the
    /// D-17 isolation refusal and the not-ready states, all of which an owner
    /// answers in band — are unavailability and carry [`BackendUnavailable`].
    /// They are recognised by their text rather than by `WriteReply` variants
    /// of their own: the constants are the discriminators the acceptance gate
    /// already matches on, and a new variant would decode as a handler error
    /// on the old side of a mixed-version fleet. Any other in-band error — the
    /// owner's shard refusing, an increment overflowing — is a fault a retry
    /// cannot heal, and stays plain.
    fn applied(reply: WriteReply) -> anyhow::Result<WriteReply> {
        match reply {
            WriteReply::Fenced { owner_m_idx } => Err(unavailable(format!(
                "flow write fenced: membership changed under the op (owner at m_idx {owner_m_idx}); retry"
            ))),
            WriteReply::NotOwner { owner } => Err(unavailable(format!(
                "flow write misrouted: node {owner} owns this flow; retry"
            ))),
            WriteReply::Error { reason } if reason == ISOLATED_REFUSAL || is_not_ready(&reason) => {
                Err(unavailable(reason))
            }
            WriteReply::Error { reason } => Err(anyhow::anyhow!("flow store: {reason}")),
            applied @ (WriteReply::Applied { .. } | WriteReply::CasConflict { .. }) => Ok(applied),
        }
    }
}

impl rift_cluster_base::seams::FlowStore for ClusteredFlowStore {
    // `flow_ids`/`entry_count` are deliberately **not** overridden here, so both keep upstream's
    // default `Ok(None)` ("cannot enumerate"). At this face — one store, one imposter, whatever
    // this node's local shard happens to hold — the correctness constraint that shapes `FlowNet`'s
    // own admin surface (#374, see `owned_spaces`/`fleet_spaces`) applies just as hard: a flow
    // keeps `REPLICAS` copies, only the owner's copy is authoritative, and a store answering from
    // its local shard alone would be node-dependent and, for a non-owned copy, simply wrong. There
    // is no ring to consult from inside upstream's synchronous, per-imposter `FlowStore` trait —
    // that fan-out is exactly what `FlowNet::fleet_spaces` exists to do instead, over the admin
    // front's `GET /imposters/{port}/spaces`, which is the surface an "Active spaces" listing
    // should actually be built against. Implementing these two here would mean either answering
    // wrong (this node's raw local set) or reinventing the fan-out one layer too low; `None` is the
    // honest "ask the admin surface" answer.
    fn get(&self, flow_id: &str, key: &str) -> anyhow::Result<Option<Value>> {
        // Scoped above the consistency branch, not inside one: the namespace is
        // a property of the imposter, not of which copy answers the read.
        let flow_id = self.scoped(flow_id);
        match self.config.read_consistency {
            ReadConsistency::Local => {
                // The replica read the imposter opted into. Pure memory — no
                // bridge, no permit, no deadline.
                metrics::flow_read("local");
                Ok(self.net.shard_value(&flow_id, key))
            }
            ReadConsistency::Strong => Ok(self
                .net
                .blocking_read(&flow_id, key)?
                .map(|entry| entry.value)),
        }
    }

    fn set(&self, flow_id: &str, key: &str, value: Value) -> anyhow::Result<()> {
        Self::applied(self.write(flow_id, key, WriteOp::Set { value })?).map(|_| ())
    }

    fn exists(&self, flow_id: &str, key: &str) -> anyhow::Result<bool> {
        self.get(flow_id, key).map(|value| value.is_some())
    }

    fn delete(&self, flow_id: &str, key: &str) -> anyhow::Result<()> {
        Self::applied(self.write(flow_id, key, WriteOp::Delete)?).map(|_| ())
    }

    fn is_blocking(&self) -> bool {
        // Always, even for `local` reads: writes still cross the cluster, and
        // the engine decides offload per store, not per call.
        true
    }

    fn increment(&self, flow_id: &str, key: &str) -> anyhow::Result<i64> {
        self.increment_by(flow_id, key, 1)
    }

    fn increment_by(&self, flow_id: &str, key: &str, by: i64) -> anyhow::Result<i64> {
        match Self::applied(self.write(flow_id, key, WriteOp::Incr { by })?)? {
            WriteReply::Applied {
                incremented: Some(value),
                ..
            } => Ok(value),
            other => Err(anyhow::anyhow!(
                "flow store: increment reply carried no value: {other:?}"
            )),
        }
    }

    fn set_ttl(&self, flow_id: &str, ttl_seconds: i64) -> anyhow::Result<()> {
        Self::applied(self.write(flow_id, "", WriteOp::SetTtl { ttl_seconds })?).map(|_| ())
    }

    fn set_key_ttl(&self, flow_id: &str, key: &str, ttl_seconds: i64) -> anyhow::Result<bool> {
        match Self::applied(self.write(flow_id, key, WriteOp::SetKeyTtl { ttl_seconds })?)? {
            WriteReply::Applied {
                existed: Some(existed),
                ..
            } => Ok(existed),
            other => Err(anyhow::anyhow!(
                "flow store: set_key_ttl reply carried no existence bit: {other:?}"
            )),
        }
    }

    fn clear_flow(&self, flow_id: &str) -> anyhow::Result<()> {
        Self::applied(self.write(flow_id, "", WriteOp::ClearFlow)?).map(|_| ())
    }

    fn compare_and_set(
        &self,
        flow_id: &str,
        key: &str,
        expected: Option<&Value>,
        new: Value,
    ) -> anyhow::Result<rift_cluster_base::seams::CasOutcome> {
        let reply = self.write(
            flow_id,
            key,
            WriteOp::Cas {
                expected: expected.cloned(),
                new,
            },
        )?;
        match Self::applied(reply)? {
            WriteReply::CasConflict { current } => {
                Ok(rift_cluster_base::seams::CasOutcome::Conflict(current))
            }
            _ => Ok(rift_cluster_base::seams::CasOutcome::Applied),
        }
    }
}

/// The provider `cluster_manager` installs: every imposter on a `--cluster`
/// node gets the clustered store, configured or not — scenario state behind a
/// round-robin LB is wrong on a process-local store for *every* imposter, not
/// just the ones that thought about it.
///
/// This is D-7: upstream keeps its per-imposter store instances, and the
/// manager-scoped provider hands each of them a thin face over the one shared
/// [`FlowNet`] — which is what lets the store outlive the "constructed before
/// the node exists" ordering (see *Late binding* above) without changing the
/// upstream seam.
pub struct ClusteredFlowStoreProvider {
    net: Arc<FlowNet>,
}

impl ClusteredFlowStoreProvider {
    #[must_use]
    pub fn new(net: Arc<FlowNet>) -> Self {
        Self { net }
    }
}

/// The tenant that owns `port`, read from this node's applied state (#288).
///
/// Ports are fleet-unique across tenants (RFC-002 §3.2), so port → tenant is a
/// function; and by the time the engine drives `provide` the config row exists
/// (apply commits `sm_configs` before the sync action that reaches the provider,
/// and again on `reconcile_engine`), so for a tenant-scoped config this should
/// always resolve. The tenant is deliberately **not** in `ImposterConfig` — the
/// core schema carries no tenancy (open-core rule) — which is why it has to be
/// looked up here rather than read off the config.
///
/// `None` is not an error path in itself — the store still functions, rendering
/// its defensive `t??:` prefix ([`ContextScope::prefix_for`]) — but a
/// tenant-scoped imposter losing its namespace to that placeholder is exactly
/// the cross-tenant bleed the scope exists to prevent, so every distinct cause
/// is logged by name: an operator has to be able to tell "the node handle is
/// not installed" (a wiring bug) from "no owner row yet" (a race, or a port
/// nobody configured) from "the read itself failed" (storage). `loudly` picks
/// the level — error for a store's first failure, debug for the retries after
/// it (see `ClusteredFlowStore::tenant_failure_logged`).
fn resolve_owning_tenant(net: &FlowNet, port: Option<u16>, loudly: bool) -> Option<TenantId> {
    macro_rules! report {
        ($($arg:tt)*) => {
            if loudly {
                tracing::error!($($arg)*);
            } else {
                tracing::debug!($($arg)*);
            }
        };
    }
    let Some(port) = port else {
        report!("tenant-scoped flow store built without a port; cannot resolve its tenant");
        return None;
    };
    let Some(node) = net.node.get().and_then(Weak::upgrade) else {
        report!(
            port,
            "tenant-scoped flow store: no cluster node bound to the flow net (not yet bound, or \
             shut down); the owning tenant cannot be resolved and the store renders its \
             defensive t??: prefix until it can"
        );
        return None;
    };
    match node.owning_tenant(port) {
        Ok(Some(tenant)) => Some(tenant),
        Ok(None) => {
            report!(
                port,
                "tenant-scoped flow store: no applied config row owns this port yet; the store \
                 renders its defensive t??: prefix until one does"
            );
            None
        }
        Err(e) => {
            report!(
                port,
                error = %e,
                "tenant-scoped flow store: owning-tenant lookup failed; the store renders its \
                 defensive t??: prefix until a lookup succeeds"
            );
            None
        }
    }
}

impl rift_cluster_base::seams::FlowStoreProvider for ClusteredFlowStoreProvider {
    fn provide(
        &self,
        config: &rift_cluster_base::seams::ImposterConfig,
    ) -> Option<Arc<dyn rift_cluster_base::seams::FlowStore>> {
        // `validate` refuses bad values pre-commit, so an Err here means a
        // config written by a different (newer) build reached this node.
        // Defaults + a loud log beat both a panic on the apply path and a
        // silent fall-through to the process-local builtin.
        let flow_config = FlowConfig::from_imposter(config).unwrap_or_else(|reason| {
            tracing::error!(
                port = config.port,
                %reason,
                "flowState carried a value this build cannot parse; using defaults"
            );
            FlowConfig::default()
        });

        let store = ClusteredFlowStore {
            net: Arc::clone(&self.net),
            config: flow_config,
            port: config.port,
            tenant: OnceLock::new(),
            tenant_failure_logged: std::sync::atomic::AtomicBool::new(false),
        };
        // Resolve the tenant now rather than on the first op, so the ordinary
        // tenant-scoped store pays the lookup once, here, and a failure is
        // logged at provide time — where an operator reading the log can
        // still connect it to the imposter that just started. A failure here
        // is not cached (see the field's doc); the store retries per op.
        if store.config.scope == ContextScope::Tenant {
            let _ = store.tenant();
        }
        Some(Arc::new(store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `imposter_port` charges only `i<port>:` ids: the fleet (`f:`), tenant (`t<tenant>:`,
    /// #288) and placeholder namespaces are excluded by construction — pinned so a future
    /// prefix cannot start being charged to a port by accident (a tenant id spelled like a port
    /// is the tempting mistake).
    #[test]
    fn imposter_port_charges_only_the_imposter_namespace() {
        assert_eq!(imposter_port("i6400:cart"), Some(6400));
        assert_eq!(imposter_port("f:cart"), None);
        assert_eq!(imposter_port("tacme:cart"), None);
        assert_eq!(imposter_port("t6400:cart"), None);
        assert_eq!(imposter_port("t??:cart"), None);
        assert_eq!(imposter_port("i?:cart"), None);
    }

    /// `tenant_of` charges only `t<tenant>:` ids, and never the unresolved placeholder (#413).
    #[test]
    fn tenant_of_charges_only_the_tenant_namespace() {
        assert_eq!(tenant_of("tacme:cart"), Some("acme"));
        assert_eq!(tenant_of("tdefault:x"), Some("default"));
        assert_eq!(
            tenant_of("t??:cart"),
            None,
            "the placeholder names no tenant"
        );
        assert_eq!(tenant_of("t?:cart"), None);
        assert_eq!(tenant_of("t:cart"), None, "an empty tenant is not a tenant");
        assert_eq!(tenant_of("i6400:cart"), None);
        assert_eq!(tenant_of("f:cart"), None);
        assert_eq!(tenant_of("tacme"), None, "no `:` — not a scoped id at all");
    }

    /// The peer fold works for the tenant half exactly as for ports (#413): a mentioned tenant is
    /// added to, an unmentioned one is left alone, an unrequested one is ignored.
    #[test]
    fn a_peers_tenant_counts_fold_like_its_port_counts() {
        let mut totals: HashMap<String, u64> =
            HashMap::from([("acme".to_owned(), 5), ("beta".to_owned(), 0)]);
        merge_peer_counts(
            &mut totals,
            &[("acme".to_owned(), 3), ("gamma".to_owned(), 9)],
        );
        assert_eq!(
            totals,
            HashMap::from([("acme".to_owned(), 8), ("beta".to_owned(), 0)])
        );
    }

    // -- issue #401: `merge_peer_counts` (the `fleet_entry_counts` peer fold) -

    /// A peer answering with real counts is added, per port, to the running totals — the mutation
    /// this test exists to kill turned this line into a no-op, so every 2-node wire test that only
    /// killed or diverged the peer still passed while an answering peer's counts went uncounted.
    #[test]
    fn a_peer_answering_with_real_counts_is_added_to_the_running_totals_per_port() {
        let mut totals: HashMap<u16, u64> = HashMap::from([(8001, 5), (8002, 10)]);

        merge_peer_counts(&mut totals, &[(8001, 3), (8002, 7)]);

        assert_eq!(totals, HashMap::from([(8001, 8), (8002, 17)]));
    }

    /// A port the peer does not mention is left exactly as it was.
    #[test]
    fn a_port_the_peer_does_not_mention_is_unchanged() {
        let mut totals: HashMap<u16, u64> = HashMap::from([(8001, 5), (8002, 10)]);

        merge_peer_counts(&mut totals, &[(8001, 3)]);

        assert_eq!(totals, HashMap::from([(8001, 8), (8002, 10)]));
    }

    /// A port the peer reports that this node never requested is not inserted — `totals` only
    /// ever holds the caller's own requested ports (see `fleet_entry_counts`), and a peer's count
    /// for anything else must not grow the response with a key the caller cannot use.
    #[test]
    fn a_port_this_node_never_requested_is_not_inserted() {
        let mut totals: HashMap<u16, u64> = HashMap::from([(8001, 5)]);

        merge_peer_counts(&mut totals, &[(8001, 3), (9999, 100)]);

        assert_eq!(totals, HashMap::from([(8001, 8)]));
    }

    /// The merge saturates rather than overflows, matching `owned_counts`'s own arithmetic.
    #[test]
    fn the_merge_saturates_instead_of_overflowing() {
        let mut totals: HashMap<u16, u64> = HashMap::from([(8001, u64::MAX - 1)]);

        merge_peer_counts(&mut totals, &[(8001, 10)]);

        assert_eq!(totals, HashMap::from([(8001, u64::MAX)]));
    }

    /// `owner_of` as a lookup table, so these tests pin the filter and the
    /// prefix arithmetic without standing a ring up.
    fn owners<'a>(pairs: &'a [(&'a str, NodeId)]) -> impl Fn(&str) -> Option<NodeId> + 'a {
        move |key: &str| pairs.iter().find(|(k, _)| *k == key).map(|(_, node)| *node)
    }

    #[test]
    fn owned_spaces_keeps_only_this_nodes_flows_and_strips_the_scope_prefix() {
        let entries = vec![
            ("i6400:alpha".to_owned(), 3usize),
            ("i6400:beta".to_owned(), 7usize),
        ];
        let owner_of = owners(&[("i6400:alpha", 1), ("i6400:beta", 2)]);

        let mine = owned_spaces(&entries, "i6400:", owner_of, 1);

        assert_eq!(mine, vec![("alpha".to_owned(), 3u64)]);
    }

    #[test]
    fn owned_spaces_strips_a_prefix_shaped_flow_id_exactly_once() {
        // The user's own flow id is `i6400:foo`; scoped for port 6400 it becomes
        // `i6400:i6400:foo`. Stripping twice would hand back `foo` — a different
        // space, and one the operator never named.
        let entries = vec![("i6400:i6400:foo".to_owned(), 1usize)];
        let owner_of = owners(&[("i6400:i6400:foo", 1)]);

        let mine = owned_spaces(&entries, "i6400:", owner_of, 1);

        assert_eq!(mine, vec![("i6400:foo".to_owned(), 1u64)]);
    }

    #[test]
    fn owned_spaces_does_not_match_an_adjacent_ports_prefix() {
        // `i64000:` shares its first six characters with `i6400:`; the colon is
        // the only thing that separates port 6400 from port 64000.
        let entries = vec![("i64000:alpha".to_owned(), 5usize)];
        let owner_of = owners(&[("i64000:alpha", 1)]);

        let mine = owned_spaces(&entries, "i6400:", owner_of, 1);

        assert_eq!(mine, Vec::<(String, u64)>::new());
    }

    #[test]
    fn owned_spaces_lists_fleet_scoped_flows_under_the_f_prefix() {
        let entries = vec![
            ("f:shared".to_owned(), 4usize),
            ("i6400:private".to_owned(), 9usize),
        ];
        let owner_of = owners(&[("f:shared", 1), ("i6400:private", 1)]);

        let mine = owned_spaces(&entries, "f:", owner_of, 1);

        assert_eq!(mine, vec![("shared".to_owned(), 4u64)]);
    }

    #[test]
    fn a_peer_row_is_stamped_with_the_peers_node_id_not_this_nodes() {
        // Run 32 shipped a peer-row transposition that passed the entire suite
        // because every wire test was solo-node. This is that branch, executed.
        let (rows, _partial) = merge_space_rows(
            vec![("mine".to_owned(), 1u64)],
            7,
            vec![(9, Some(vec![("theirs".to_owned(), 2u64)]))],
            false,
        );

        let theirs = rows
            .iter()
            .find(|row| row.space == "theirs")
            .expect("the peer's space must be listed");
        assert_eq!(theirs.owner, 9, "a peer's row must name the peer as owner");
        assert_eq!(theirs.entry_count, 2);

        let mine = rows
            .iter()
            .find(|row| row.space == "mine")
            .expect("this node's space must be listed");
        assert_eq!(mine.owner, 7);
    }

    #[test]
    fn merge_is_the_union_of_owned_shares_with_no_duplicate_space() {
        let (rows, partial) = merge_space_rows(
            vec![("a".to_owned(), 1u64)],
            1,
            vec![
                (2, Some(vec![("b".to_owned(), 2u64)])),
                (3, Some(vec![("c".to_owned(), 3u64)])),
            ],
            false,
        );

        let mut spaces: Vec<&str> = rows.iter().map(|row| row.space.as_str()).collect();
        spaces.sort_unstable();
        assert_eq!(spaces, vec!["a", "b", "c"]);
        assert!(!partial);
    }

    #[test]
    fn a_missing_peer_answer_stamps_partial() {
        let (rows, partial) =
            merge_space_rows(vec![("a".to_owned(), 1u64)], 1, vec![(2, None)], false);

        assert_eq!(rows.len(), 1, "the surviving share is still served");
        assert!(
            partial,
            "a peer that did not answer must not be reported as having no spaces"
        );
    }

    #[test]
    fn a_timeout_stamps_partial_even_when_every_peer_answered() {
        let (_rows, partial) = merge_space_rows(
            vec![],
            1,
            vec![(2, Some(vec![("b".to_owned(), 1u64)]))],
            true,
        );

        assert!(partial);
    }

    #[test]
    fn no_spaces_anywhere_is_a_knowable_zero_not_a_partial() {
        // The distinction the console renders: `[]` with `partial: false` is
        // "this imposter has no spaces"; `[]` with `partial: true` is "cannot
        // tell you". Collapsing them would state the first as fact.
        let (rows, partial) = merge_space_rows(vec![], 1, vec![(2, Some(vec![]))], false);

        assert!(rows.is_empty());
        assert!(!partial);
    }

    /// Pins D-65: a failure caused by the cluster's state — isolation, an unreachable owner, a
    /// shed bridge — carries `BackendUnavailable`, the one thing the data plane's
    /// `backend_error_response` keys a 503 on; a fault in the request or in this node stays a
    /// plain error, which answers 500 — both statuses taken from `backend_error_response`, the
    /// production mapping. Every variant has a row, and the match has no wildcard, so a variant
    /// added to `RpcError` must at least be named here before this compiles.
    #[test]
    fn store_error_types_cluster_state_failures_and_nothing_else() {
        use crate::rpc::{AuthError, PROTO_VERSION};
        use rift_cluster_base::seams::{BackendUnavailable, backend_error_response};

        let every_variant_has_a_row = |e: &RpcError| match e {
            RpcError::Unauthorized(_)
            | RpcError::VersionSkew { .. }
            | RpcError::UnknownRoute { .. }
            | RpcError::BodyTooLarge { .. }
            | RpcError::Timeout
            | RpcError::Transport(_)
            | RpcError::Shed
            | RpcError::BadRequest(_)
            | RpcError::Unavailable { .. }
            | RpcError::NotLeader { .. }
            | RpcError::Handler(_)
            | RpcError::NotFound { .. } => {}
        };
        let rows = [
            (
                RpcError::Unavailable {
                    detail: ISOLATED_REFUSAL.to_owned(),
                    op_id: None,
                },
                true,
            ),
            (RpcError::Timeout, true),
            (RpcError::Transport("connection refused".to_owned()), true),
            (RpcError::Shed, true),
            (RpcError::Unauthorized(AuthError::BadMac), false),
            (
                RpcError::VersionSkew {
                    peer: None,
                    ours: PROTO_VERSION,
                },
                false,
            ),
            (
                RpcError::UnknownRoute {
                    method: "POST".to_owned(),
                    path: GET_PATH.to_owned(),
                },
                false,
            ),
            (RpcError::BodyTooLarge { limit: 1 }, false),
            (RpcError::BadRequest("malformed".to_owned()), false),
            (RpcError::NotLeader { leader: None }, false),
            (RpcError::Handler("boom".to_owned()), false),
            (
                RpcError::NotFound {
                    what: "blob".to_owned(),
                },
                false,
            ),
        ];
        every_variant_has_a_row(&RpcError::Timeout);
        for (error, unavailable) in rows {
            let reason = error.to_string();
            let err = store_error(error);
            let typed = err.downcast_ref::<BackendUnavailable>();
            assert_eq!(
                typed.is_some(),
                unavailable,
                "{reason}: classified as {err:#}"
            );
            if let Some(typed) = typed {
                assert_eq!(typed.feature, "flowState", "{reason}");
            }
            assert_eq!(
                backend_error_response(&err).status().as_u16(),
                if unavailable { 503 } else { 500 },
                "{reason}: the data-plane status"
            );
            assert!(
                err.to_string().contains(&reason),
                "the peer's own reason must survive the wrap: {err:#} lost {reason:?}"
            );
        }
    }

    /// Pins D-65 on the write side. The replies that say "the op did not happen; retry" — a
    /// fenced write, a misrouted one, the D-17 isolation refusal and the not-ready states, all
    /// of which travel in band as `WriteReply::Error` — carry `BackendUnavailable`. Any other
    /// in-band error is the owner's own fault and stays plain: a 503 there would invite a retry
    /// that cannot help.
    #[test]
    fn applied_types_the_retryable_refusals_and_nothing_else() {
        use rift_cluster_base::seams::BackendUnavailable;

        let unavailable = |reply: WriteReply| {
            let err = ClusteredFlowStore::applied(reply).expect_err("a refusal is an error");
            let typed = err.downcast_ref::<BackendUnavailable>().is_some();
            (typed, err.to_string())
        };

        let (typed, text) = unavailable(WriteReply::Fenced { owner_m_idx: 7 });
        assert!(typed, "fenced: {text}");
        assert!(
            text.contains("m_idx 7"),
            "the fence still names the owner's token: {text}"
        );

        let (typed, text) = unavailable(WriteReply::NotOwner { owner: 2 });
        assert!(typed, "misrouted: {text}");
        assert!(
            text.contains("node 2 owns"),
            "the misroute still names the owner: {text}"
        );

        let (typed, text) = unavailable(WriteReply::Error {
            reason: ISOLATED_REFUSAL.to_owned(),
        });
        assert!(typed, "isolated: {text}");
        assert!(text.contains("owner is isolated"), "{text}");

        for not_ready in [
            NOT_READY_STARTING,
            NOT_READY_SHUT_DOWN,
            NOT_READY_NO_MEMBERSHIP,
            NOT_READY_NO_OWNER,
        ] {
            let (typed, text) = unavailable(WriteReply::Error {
                reason: not_ready.to_owned(),
            });
            assert!(typed, "an owner that is not ready is unavailable: {text}");
            assert!(text.contains(not_ready), "{text}");
        }

        for fault in [
            "shard write failed: disk full",
            "increment_by overflow: 1 + 2",
        ] {
            let (typed, text) = unavailable(WriteReply::Error {
                reason: fault.to_owned(),
            });
            assert!(!typed, "a fault is not unavailability: {text}");
            assert_eq!(text, format!("flow store: {fault}"));
        }
    }
}
