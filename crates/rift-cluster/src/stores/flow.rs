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
//! answered by it. Correct under any LB, one LAN RPC worst-case, opt-out per
//! imposter via `readConsistency: "local"`.
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
use crate::metrics;
use crate::raft::ring::{KeyClass, OwnedKey};
use crate::raft::{NodeId, RaftNode, Ring};
use crate::rpc::{HandlerFuture, Router, RpcError};
use crate::stores::flow_config::{FlowConfig, ReadConsistency};
use crate::stores::shard::{Durability, FlowShard, Versioned};

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
}

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
    /// Sync→async for the store face. Created at bind time so an unclustered
    /// process never pays for the bridge runtime.
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
            .ok_or("flow store: cluster is still starting")?
            .upgrade()
            .ok_or("flow store: cluster node has shut down")?;
        let ring = node.ring();
        if ring.is_empty() {
            return Err("flow store: no applied membership yet".to_owned());
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
                    reason: "flow store: ring has no members".to_owned(),
                };
            }
        }

        // Verify the copy before first serving it under this membership —
        // outside the RMW lock, because adoption does RPC and holding the
        // per-node lock across the network would stall every other flow's
        // writes for the pull's duration. Racing first-touches both adopt;
        // the merge is idempotent.
        self.ensure_adopted(&node, &ring, &req.flow_id).await;

        let _serialize = self.rmw.lock().await;
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
            .ok_or_else(|| anyhow::anyhow!("flow store: cluster is still starting"))?;
        let net = Arc::clone(self);
        let (_, ring) = self.view().map_err(|e| anyhow::anyhow!(e))?;
        let req = req_for(ring.m_idx());

        // `ScriptPool`, whichever surface called: the engine offloads blocking
        // stores off its workers (`run_flow_blocking`), so the calling thread
        // here is a blocking-pool or script thread, never a tokio worker.
        bridge
            .call(CallerClass::ScriptPool, FLOW_OP_DEADLINE, async move {
                let (node, ring) = net.view().map_err(RpcError::Handler)?;
                let key = OwnedKey::new(KeyClass::FlowKv, &req.flow_id);
                let owner = ring.owner(key).ok_or_else(|| {
                    RpcError::Handler("flow store: ring has no members".to_owned())
                })?;
                if owner == node.id() {
                    Ok(net.owner_write(req).await)
                } else {
                    let body =
                        serde_json::to_vec(&req).map_err(|e| RpcError::Handler(e.to_string()))?;
                    let reply = node
                        .call_member(owner, "POST", WRITE_PATH, body)
                        .await
                        .map_err(RpcError::Transport)?;
                    serde_json::from_slice(&reply).map_err(|e| RpcError::Handler(e.to_string()))
                }
            })
            .map_err(|e| anyhow::anyhow!("flow store: {e}"))
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
            .ok_or_else(|| anyhow::anyhow!("flow store: cluster is still starting"))?;
        let net = Arc::clone(self);
        let req = GetReq {
            flow_id: flow_id.to_owned(),
            key: key.to_owned(),
        };

        bridge
            .call(CallerClass::ScriptPool, FLOW_OP_DEADLINE, async move {
                let (node, ring) = net.view().map_err(RpcError::Handler)?;
                let owned = OwnedKey::new(KeyClass::FlowKv, &req.flow_id);
                let owner = ring.owner(owned).ok_or_else(|| {
                    RpcError::Handler("flow store: ring has no members".to_owned())
                })?;
                if owner == node.id() {
                    metrics::flow_read("owner");
                    net.ensure_adopted(&node, &ring, &req.flow_id).await;
                    Ok(net.shard.get(&req.flow_id, &req.key))
                } else {
                    metrics::flow_read("forward");
                    let body =
                        serde_json::to_vec(&req).map_err(|e| RpcError::Handler(e.to_string()))?;
                    let reply = node
                        .call_member(owner, "POST", GET_PATH, body)
                        .await
                        .map_err(RpcError::Transport)?;
                    let reply: GetReply = serde_json::from_slice(&reply)
                        .map_err(|e| RpcError::Handler(e.to_string()))?;
                    Ok(reply.entry)
                }
            })
            .map_err(|e| anyhow::anyhow!("flow store: {e}"))
    }

    /// This node's OWNED share of `numberOfFlowEntries` for `ports` (#372): the
    /// live entry count of every imposter-scoped flow this node currently owns.
    ///
    /// Filtered to the **owner** only, never every holder of a copy: a flow keeps
    /// [`REPLICAS`] copies for durability, so a raw per-node sum (this node's plus
    /// every peer's) would count each entry up to [`REPLICAS`] times. Restricting
    /// to the owner makes the fleet-wide sum in [`Self::fleet_entry_counts`]
    /// exactly the union of live entries, with no coordination beyond the ring
    /// every node already agrees on.
    ///
    /// Fleet-scoped (`f:`) flows never match [`imposter_port`]'s `i{port}:`
    /// prefix, so they contribute to no port's count — they are shared by
    /// construction, and charging them to any one imposter would be arbitrary.
    fn owned_port_counts(&self, node: &RaftNode, ring: &Ring, ports: &[u16]) -> HashMap<u16, u64> {
        let wanted: HashSet<u16> = ports.iter().copied().collect();
        // Seeded with every requested port at `0`, not only the ones this node
        // happens to own something for: `fleet_entry_counts` folds a peer's reply
        // into this map by key, and a port absent here would silently drop that
        // peer's count instead of adding to zero.
        let mut totals: HashMap<u16, u64> = ports.iter().map(|&port| (port, 0)).collect();
        for (flow_id, count) in self.shard.entries_by_flow() {
            let Some(port) = imposter_port(&flow_id) else {
                continue;
            };
            if !wanted.contains(&port) {
                continue;
            }
            let owned = OwnedKey::new(KeyClass::FlowKv, &flow_id);
            if ring.owner(owned) != Some(node.id()) {
                continue;
            }
            *totals.entry(port).or_insert(0) += count as u64;
        }
        totals
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
        // Nothing to fan out for: a tenant with no imposters has a locally
        // computable (empty) answer, and asking every peer anyway would stamp
        // `partial: true` on a `0/0/0` body the instant any one of them
        // failed to answer in time — for no reason, since there is nothing
        // for their answers to add.
        if ports.is_empty() {
            return (HashMap::new(), false);
        }

        let (node, ring) = match self.view() {
            Ok(view) => view,
            Err(reason) => {
                tracing::warn!(error = %reason, "flow entry counts: cluster view unavailable");
                return (HashMap::new(), true);
            }
        };
        let mut totals = self.owned_port_counts(&node, &ring, ports);

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
            set.spawn(async move {
                let outcome = async {
                    let body = serde_json::to_vec(&CountsReq {
                        ports: req_ports,
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
                        merge_peer_counts(&mut totals, &reply.slots);
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
/// matches the totals' own construction in `owned_port_counts`.
fn merge_peer_counts(totals: &mut HashMap<u16, u64>, peer_slots: &[(u16, u64)]) {
    for &(port, count) in peer_slots {
        if let Some(total) = totals.get_mut(&port) {
            *total = total.saturating_add(count);
        }
    }
}

/// The imposter port a scoped flow id belongs to, or `None` for a fleet-scoped
/// (`f:`) id or the unreachable `i?:` placeholder namespace ([`ContextScope::prefix_for`]) —
/// both deliberately excluded from any port's count: a fleet-scoped flow is
/// shared by construction, so charging its entries to any one imposter's port
/// would be arbitrary, and the placeholder names no real port to charge at all.
fn imposter_port(scoped_flow_id: &str) -> Option<u16> {
    scoped_flow_id
        .strip_prefix('i')
        .and_then(|rest| rest.split_once(':'))
        .and_then(|(port, _)| port.parse().ok())
}

/// The wire surface: four POST routes on the cluster port, HMAC-authed and
/// version-negotiated by the transport like every other route.
#[must_use]
pub fn flow_routes(net: Arc<FlowNet>) -> Router {
    let get_net = Arc::clone(&net);
    let write_net = Arc::clone(&net);
    let replicate_net = Arc::clone(&net);
    let sync_net = Arc::clone(&net);
    let counts_net = net;

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
                    if let Ok((node, ring)) = net.view() {
                        net.ensure_adopted(&node, &ring, &req.flow_id).await;
                    }
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
                    // The caller computed `owned_port_counts` decisions under
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
                    let totals = net.owned_port_counts(&node, &ring, &req.ports);
                    let slots = req
                        .ports
                        .iter()
                        .map(|&port| (port, totals.get(&port).copied().unwrap_or(0)))
                        .collect();
                    serde_json::to_vec(&CountsReply { slots })
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
}

impl ClusteredFlowStore {
    /// Held as scope+port rather than a rendered prefix so this shares
    /// [`ContextScope::scoped_flow_id`] with the admin front's ownership lookup
    /// (#359). Two renderings of this key would be two answers to "who owns this
    /// flow", and the wrong one sends an operator to the wrong node.
    fn scoped(&self, flow_id: &str) -> String {
        self.config.scope.scoped_flow_id(self.port, flow_id)
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
    fn applied(reply: WriteReply) -> anyhow::Result<WriteReply> {
        match reply {
            WriteReply::Fenced { owner_m_idx } => Err(anyhow::anyhow!(
                "flow write fenced: membership changed under the op (owner at m_idx {owner_m_idx}); retry"
            )),
            WriteReply::NotOwner { owner } => Err(anyhow::anyhow!(
                "flow write misrouted: node {owner} owns this flow; retry"
            )),
            WriteReply::Error { reason } => Err(anyhow::anyhow!("flow store: {reason}")),
            applied => Ok(applied),
        }
    }
}

impl rift_cluster_base::seams::FlowStore for ClusteredFlowStore {
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
pub struct ClusteredFlowStoreProvider {
    net: Arc<FlowNet>,
}

impl ClusteredFlowStoreProvider {
    #[must_use]
    pub fn new(net: Arc<FlowNet>) -> Self {
        Self { net }
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
        Some(Arc::new(ClusteredFlowStore {
            net: Arc::clone(&self.net),
            config: flow_config,
            port: config.port,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// The merge saturates rather than overflows, matching `owned_port_counts`'s own arithmetic.
    #[test]
    fn the_merge_saturates_instead_of_overflowing() {
        let mut totals: HashMap<u16, u64> = HashMap::from([(8001, u64::MAX - 1)]);

        merge_peer_counts(&mut totals, &[(8001, 10)]);

        assert_eq!(totals, HashMap::from([(8001, u64::MAX)]));
    }
}
