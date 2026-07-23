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
//! - Anti-entropy pull is deferred (filed as a follow-up): without
//!   owner-failover adoption it repairs nothing that push replication has not
//!   already covered, and half of a repair protocol is dead code.

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

/// Copies of each flow: the owner plus two successors. Matches the config
/// plane's replication factor reasoning in RFC-001 §7.3 — survives one node
/// loss with a copy to spare, and three is the whole fleet at minimum size.
const REPLICAS: usize = 3;

const WRITE_PATH: &str = "/_cluster/flow/write";
const GET_PATH: &str = "/_cluster/flow/get";
const REPLICATE_PATH: &str = "/_cluster/flow/replicate";

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
    /// v1 limitation, deliberate: deletes are applied directly, without
    /// tombstones, so a delayed `Put` can resurrect a deleted key on a replica
    /// until the owner writes again. The owner's copy — the one `strong` reads
    /// consult — is never affected.
    Delete {
        key: String,
    },
    ClearFlow,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplicateReq {
    flow_id: String,
    op: ReplicaOp,
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
}

impl FlowNet {
    #[must_use]
    pub fn new(shard: FlowShard) -> Arc<Self> {
        Arc::new(Self {
            shard,
            node: OnceLock::new(),
            bridge: OnceLock::new(),
            rmw: tokio::sync::Mutex::new(()),
        })
    }

    /// Attach the node once it exists and start the bridge. Binding twice is a
    /// no-op; the second caller wanted what the first one got.
    ///
    /// # Errors
    ///
    /// The bridge's private runtime could not start.
    pub fn bind(&self, node: &Arc<RaftNode>, bridge_config: BridgeConfig) -> std::io::Result<()> {
        if self.bridge.get().is_none() {
            let bridge = Bridge::start(bridge_config)?;
            // A racing second bind loses the set and drops its bridge — same
            // contract as the node slot below.
            let _ = self.bridge.set(bridge);
        }
        let _ = self.node.set(Arc::downgrade(node));
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
        };

        let result: Result<WriteReply, super::shard::ShardError> = match req.op {
            WriteOp::Set { value } => {
                let prev = self.shard.get(flow, &req.key);
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
            WriteOp::Delete => match self.shard.delete(flow, &req.key, req.durability).await {
                Ok(()) => {
                    self.replicate(&node, &ring, flow, ReplicaOp::Delete { key: req.key });
                    Ok(WriteReply::Applied {
                        entry: None,
                        incremented: None,
                        existed: None,
                    })
                }
                Err(e) => Err(e),
            },
            WriteOp::Incr { by } => {
                let prev = self.shard.get(flow, &req.key);
                // Absent ⇒ 0; a non-i64 current value coerces to 0. Upstream's
                // default `increment_by` semantics, kept identical so a script
                // behaves the same on one node and on a fleet.
                let current = prev
                    .as_ref()
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
                let prev = self.shard.get(flow, &req.key);
                let current = prev.as_ref().map(|entry| entry.value.clone());
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
                            self.replicate(&node, &ring, flow, ReplicaOp::ClearFlow);
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
                    match self.shard.delete(flow, &req.key, req.durability).await {
                        Ok(()) => {
                            self.replicate(&node, &ring, flow, ReplicaOp::Delete { key: req.key });
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
                    self.replicate(&node, &ring, flow, ReplicaOp::ClearFlow);
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
            };
            self.shard
                .set(flow, &key, entry.clone(), durability)
                .await?;
            self.replicate(node, ring, flow, ReplicaOp::Put { key, entry });
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
    async fn apply_replica(&self, req: ReplicateReq) {
        match req.op {
            ReplicaOp::Put { key, entry } => {
                let keep = self
                    .shard
                    .get(&req.flow_id, &key)
                    .is_none_or(|current| current.superseded_by(&entry));
                if keep
                    && let Err(e) = self
                        .shard
                        .set(&req.flow_id, &key, entry, Durability::Async)
                        .await
                {
                    tracing::error!(error = %e, "flow replica put failed");
                }
            }
            ReplicaOp::Delete { key } => {
                if let Err(e) = self
                    .shard
                    .delete(&req.flow_id, &key, Durability::Async)
                    .await
                {
                    tracing::error!(error = %e, "flow replica delete failed");
                }
            }
            ReplicaOp::ClearFlow => {
                if let Err(e) = self.shard.clear_flow(&req.flow_id, Durability::Async).await {
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
}

/// The wire surface: three POST routes on the cluster port, HMAC-authed and
/// version-negotiated by the transport like every other route.
#[must_use]
pub fn flow_routes(net: Arc<FlowNet>) -> Router {
    let get_net = Arc::clone(&net);
    let write_net = Arc::clone(&net);
    let replicate_net = net;

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
}

impl ClusteredFlowStore {
    fn write(&self, flow_id: &str, key: &str, op: WriteOp) -> anyhow::Result<WriteReply> {
        let (flow_id, key) = (flow_id.to_owned(), key.to_owned());
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

impl rift_ee::seams::FlowStore for ClusteredFlowStore {
    fn get(&self, flow_id: &str, key: &str) -> anyhow::Result<Option<Value>> {
        match self.config.read_consistency {
            ReadConsistency::Local => {
                // The replica read the imposter opted into. Pure memory — no
                // bridge, no permit, no deadline.
                metrics::flow_read("local");
                Ok(self.net.shard_value(flow_id, key))
            }
            ReadConsistency::Strong => Ok(self
                .net
                .blocking_read(flow_id, key)?
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
    ) -> anyhow::Result<rift_ee::seams::CasOutcome> {
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
                Ok(rift_ee::seams::CasOutcome::Conflict(current))
            }
            _ => Ok(rift_ee::seams::CasOutcome::Applied),
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

impl rift_ee::seams::FlowStoreProvider for ClusteredFlowStoreProvider {
    fn provide(
        &self,
        config: &rift_ee::seams::ImposterConfig,
    ) -> Option<Arc<dyn rift_ee::seams::FlowStore>> {
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
        }))
    }
}
