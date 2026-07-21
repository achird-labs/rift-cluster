//! The Raft control-plane node: an [`openraft::Raft`] wired to the redb-backed
//! storage and the #8 cluster transport, with `--cluster-init` bootstrap, a
//! seed-join path, and a [`StatusReport`] read from Raft's own metrics.
//!
//! Startup has a deliberate ordering. The cluster server must bind before the
//! node knows the address it advertises, but the server's request handlers need
//! the node's [`Raft`]. The `Raft` is therefore built *after* the server binds,
//! and installed into a shared [`OnceCell`] the handlers read — so a peer RPC
//! that arrives in the sliver before installation gets a retryable "not ready"
//! rather than reaching a half-built node.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config, Raft, ServerState};
use rift_ee::seams::{ImposterConfig, ImposterManager};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::network::{
    self, CLUSTER_APPLIED_PATH, CLUSTER_JOIN_PATH, CLUSTER_WRITE_PATH, JoinRequest, RaftSlot,
    RpcNetwork, WriteReply,
};
use super::ring::Ring;
use super::store::{self, RedbStateMachine};
use super::{NodeId, TypeConfig};
use crate::control::{ControlOp, ControlRequest, ControlResponse, TenantId};
use crate::rpc::client::AlwaysHealthy;
use crate::rpc::{
    Router, RpcClient, RpcClientConfig, RpcServer, RpcServerConfig, Signer, Verifier,
};

/// Log-file name for the Raft storage inside the node's data directory.
const RAFT_DB_FILE: &str = "raft.redb";

/// How long `--cluster-init` waits for the founding node to elect itself leader
/// before giving up. A single voter wins immediately; this only bounds a stuck
/// startup.
const INIT_LEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// The upper election timeout the Raft config uses; also the basis for the
/// isolated-owner window below.
const ELECTION_TIMEOUT_MAX_MS: u64 = 300;

/// A leader that a quorum has not acknowledged within this window is treated as
/// isolated (the isolated-owner rule, RFC-001 §7.2): 3× the election timeout.
const ISOLATION_WINDOW_MS: u64 = 3 * ELECTION_TIMEOUT_MAX_MS;

/// Everything a [`RaftNode`] needs to start.
#[derive(Clone)]
pub struct NodeConfig {
    /// This node's persisted Raft id (see [`super::identity`]).
    pub node_id: NodeId,
    /// The address to bind the cluster port on. `:0` binds an ephemeral port.
    pub bind: SocketAddr,
    /// The address peers dial this node on. Defaults to the bound address, which
    /// is correct when bind and advertise are the same host:port.
    pub advertise: Option<SocketAddr>,
    /// Directory holding this node's Raft log/vote/snapshot database.
    pub data_dir: PathBuf,
    /// Shared HMAC secret for the cluster port. `None` runs it unauthenticated
    /// (only via an explicit insecure acknowledgment elsewhere).
    pub secret: Option<String>,
    /// Endpoints to serve on the cluster port alongside the control-plane ones.
    ///
    /// The cluster port is a single authenticated listener, so anything an
    /// embedder wants to expose there (the operator `/_cluster/*` surface, later
    /// phases' state endpoints) is registered here rather than on a second port
    /// with its own credential.
    pub routes: Router,
    /// The local engine committed control ops are applied to. `None` runs the
    /// control plane tables-only (tests, or an embedder that has not wired the
    /// engine yet) — applied configs are then served from the state machine but
    /// no imposters are actually bound.
    pub engine: Option<Arc<ImposterManager>>,
}

// Hand-written so the shared secret never lands in a log line — matching the
// `Signer`/`Verifier` convention of not deriving `Debug` on secret-bearing types.
impl std::fmt::Debug for NodeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeConfig")
            .field("node_id", &self.node_id)
            .field("bind", &self.bind)
            .field("advertise", &self.advertise)
            .field("data_dir", &self.data_dir)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .field("routes", &self.routes.len())
            .field("engine", &self.engine.is_some())
            .finish()
    }
}

/// Everything that can go wrong operating the node, kept as one typed channel so
/// call sites map outcomes rather than parse strings. openraft's own error zoo is
/// deeply generic and per-call; its detail is preserved as the message rather
/// than reproduced as a variant per RPC.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// Opening or using the redb-backed Raft store failed.
    #[error("raft storage: {0}")]
    Storage(String),

    /// Binding the cluster port failed.
    #[error("bind cluster port: {0}")]
    Bind(String),

    /// The openraft runtime failed fatally (it has shut down).
    #[error("raft runtime: {0}")]
    Runtime(String),

    /// `--cluster-init` was refused — most often because this node is already
    /// initialized (its log already carries a membership entry).
    #[error("cluster init: {0}")]
    Init(String),

    /// A client write did not commit (not leader, timed out, or the runtime died).
    #[error("client write: {0}")]
    Write(String),

    /// No leader is reachable to accept a write — no quorum, or leadership is
    /// moving faster than the forwarder can chase it. The admin surface maps
    /// this to a `503` with the `unavailable` error slug.
    #[error("no reachable leader: {0}")]
    Unavailable(String),

    /// A membership change (add-learner, promote, join) failed.
    #[error("membership: {0}")]
    Membership(String),

    /// Timed out waiting for an expected state (e.g. leadership after init).
    #[error("timed out waiting for {what}: {detail}")]
    Timeout { what: &'static str, detail: String },
}

/// A point-in-time view of the node, derived from Raft metrics. This is the
/// StatusReport surface the operator endpoints and the join lifecycle build on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusReport {
    /// This node's id.
    pub node_id: NodeId,
    /// Whether this node is currently the Raft leader.
    pub is_leader: bool,
    /// The id of the current leader, if one is known.
    pub current_leader: Option<NodeId>,
    /// Index of the last log entry applied to the state machine.
    pub last_applied: Option<u64>,
    /// The voter ids in the currently effective membership.
    pub voters: Vec<NodeId>,
}

/// The control-plane Raft node.
pub struct RaftNode {
    id: NodeId,
    advertise: SocketAddr,
    raft: Raft<TypeConfig>,
    // Authenticated client for node-driven admin RPCs (e.g. seed join). Shares
    // the same signer/pool the Raft network uses.
    client: RpcClient,
    // A read-only handle onto the same state machine openraft owns as `&mut`, so
    // the node can answer committed-config reads without going through Raft.
    sm_reader: RedbStateMachine,
    // The cluster server accept loop. Aborted on shutdown/drop so the listener is
    // released with the node.
    server_task: JoinHandle<()>,
}

impl RaftNode {
    /// Open storage, bind the cluster server, and start the Raft runtime. This
    /// does not form or join a cluster; call [`RaftNode::cluster_init`] to
    /// bootstrap a new one or [`RaftNode::join_via`] to attach to an existing one.
    pub async fn start(config: NodeConfig) -> Result<Self, NodeError> {
        let (log_store, state_machine) = store::new(config.data_dir.join(RAFT_DB_FILE))
            .await
            .map_err(|e| NodeError::Storage(e.to_string()))?;
        let state_machine = match &config.engine {
            Some(engine) => state_machine.with_engine(engine.clone()),
            None => state_machine,
        };
        let sm_reader = state_machine.clone();

        let raft_config = Arc::new(
            Config {
                cluster_name: "rift-control-plane".to_owned(),
                election_timeout_min: 150,
                election_timeout_max: ELECTION_TIMEOUT_MAX_MS,
                heartbeat_interval: 50,
                ..Default::default()
            }
            .validate()
            .map_err(|e| NodeError::Init(e.to_string()))?,
        );

        // The handlers need the Raft, which needs the bound server address, which
        // needs the handlers — so the router reads the node through a slot filled
        // in once construction below completes.
        let slot: RaftSlot = Arc::new(OnceCell::new());
        // Control-plane routes register last so a caller's route table can never
        // shadow the Raft endpoints the cluster itself depends on.
        let router = network::control_routes(config.routes.clone(), slot.clone());

        let (signer, verifier) = match &config.secret {
            Some(secret) => (
                Some(Signer::new(secret)),
                Some(Arc::new(Verifier::new(secret))),
            ),
            None => {
                // An unauthenticated cluster port must be observable at the point
                // it is created, not just auditable in the config layer above.
                tracing::warn!(
                    node_id = config.node_id,
                    bind = %config.bind,
                    rift_cluster_insecure = true,
                    "cluster port started WITHOUT authentication (no secret)"
                );
                (None, None)
            }
        };

        let server = RpcServer::bind(config.bind, RpcServerConfig::new(verifier, router))
            .await
            .map_err(|e| NodeError::Bind(e.to_string()))?;
        let local = server
            .local_addr()
            .map_err(|e| NodeError::Bind(e.to_string()))?;
        let advertise = config.advertise.unwrap_or(local);

        let client = RpcClient::new(signer, Arc::new(AlwaysHealthy), RpcClientConfig::default());
        let network = RpcNetwork::new(client.clone());

        let raft = Raft::new(
            config.node_id,
            raft_config,
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| NodeError::Runtime(e.to_string()))?;
        slot.set(raft.clone())
            .map_err(|_| NodeError::Runtime("raft slot already set".to_owned()))?;

        let server_task = tokio::spawn(server.serve());

        Ok(Self {
            id: config.node_id,
            advertise,
            raft,
            client,
            sm_reader,
            server_task,
        })
    }

    /// This node's Raft id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The address peers dial this node on.
    #[must_use]
    pub fn advertise_addr(&self) -> SocketAddr {
        self.advertise
    }

    /// Whether this node's log already carries a cluster membership — i.e. it
    /// has bootstrapped or joined before and a restart should simply resume
    /// from its durable state rather than re-initialize or re-join.
    pub async fn is_initialized(&self) -> Result<bool, NodeError> {
        self.raft
            .is_initialized()
            .await
            .map_err(|e| NodeError::Runtime(e.to_string()))
    }

    /// Bootstrap a brand-new single-node cluster with this node as the sole
    /// voter, then wait for it to elect itself leader.
    ///
    /// Initializing a node that already has a membership entry is refused by
    /// openraft and surfaced as [`NodeError::Init`], so a second `--cluster-init`
    /// (including after a restart) does not silently fork a new cluster.
    pub async fn cluster_init(&self) -> Result<(), NodeError> {
        let members = BTreeMap::from([(self.id, BasicNode::new(self.advertise.to_string()))]);
        self.raft
            .initialize(members)
            .await
            .map_err(|e| NodeError::Init(e.to_string()))?;

        self.raft
            .wait(Some(INIT_LEADER_TIMEOUT))
            .state(ServerState::Leader, "cluster-init awaits self-election")
            .await
            .map_err(|e| NodeError::Timeout {
                what: "leadership after cluster-init",
                detail: e.to_string(),
            })?;
        Ok(())
    }

    /// Leader-side: add `id`@`addr` as a learner, blocking until it has caught up
    /// via replication. Fails with [`NodeError::Membership`] if this node is not
    /// the leader.
    pub async fn add_learner(&self, id: NodeId, addr: SocketAddr) -> Result<(), NodeError> {
        self.raft
            .add_learner(id, BasicNode::new(addr.to_string()), true)
            .await
            .map_err(|e| NodeError::Membership(e.to_string()))?;
        Ok(())
    }

    /// Leader-side: replace the voter set (promoting/demoting learners already
    /// known to the cluster). Each id must already be a member.
    pub async fn change_membership(&self, voters: BTreeSet<NodeId>) -> Result<(), NodeError> {
        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|e| NodeError::Membership(e.to_string()))?;
        Ok(())
    }

    /// Ask an existing cluster member `seed` to admit this node: the seed (if
    /// leader) adds it as a learner, waits for catch-up, and promotes it to voter
    /// while the cluster is under the auto-promote ceiling.
    pub async fn join_via(&self, seed: SocketAddr) -> Result<(), NodeError> {
        let request = JoinRequest {
            node_id: self.id,
            advertise: self.advertise.to_string(),
        };
        let body =
            serde_json::to_vec(&request).map_err(|e| NodeError::Membership(e.to_string()))?;
        self.client
            .call(seed, "POST", CLUSTER_JOIN_PATH, body)
            .await
            .map_err(|e| NodeError::Membership(e.to_string()))?;
        Ok(())
    }

    /// Submit a control op through Raft and return the state machine's
    /// committed response. Fails if this node is not the leader or the entry
    /// does not commit; a *committed* refusal (validation, absent port) is the
    /// response's `Failed` outcome, not an error — the write itself succeeded.
    pub async fn write(&self, request: ControlRequest) -> Result<ControlResponse, NodeError> {
        let response = self
            .raft
            .client_write(request)
            .await
            .map_err(|e| NodeError::Write(e.to_string()))?;
        Ok(response.data)
    }

    /// Convenience: submit a default-tenant `PutImposter` with a freshly minted
    /// `op_id`. The full write path (client-supplied `Idempotency-Key`,
    /// forward-to-leader, barrier) builds [`Self::write`] requests itself.
    pub async fn put_imposter(&self, config: ImposterConfig) -> Result<ControlResponse, NodeError> {
        // A clock before the Unix epoch mints 0, which only weakens this op's
        // dedup TTL (it reads as already-old to the cluster's logical clock) —
        // never the stored response — so it is not worth a panic path.
        let issued_at_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.write(ControlRequest {
            op_id: Uuid::new_v4(),
            principal: None,
            issued_at_secs,
            op: ControlOp::PutImposter {
                tenant: TenantId::default(),
                config: Box::new(config),
            },
        })
        .await
    }

    /// Submit a control op from *any* node: run it locally when this node is
    /// the leader, otherwise forward it to the leader over the authenticated
    /// cluster port — chasing a moving leadership through up to
    /// [`Self::FORWARD_ATTEMPTS`] hops (issue #9, Ch. 4 write path).
    ///
    /// A committed refusal is still `Ok` (see [`Self::write`]); `Unavailable`
    /// means no leader could be reached at all — the no-quorum shape.
    pub async fn submit(&self, request: ControlRequest) -> Result<ControlResponse, NodeError> {
        // Local first: on the leader this is the whole path, and on a follower
        // openraft's refusal carries the freshest leader hint. Cloned because
        // the original is re-serialized for each forward hop below.
        let mut next = match network::local_write(&self.raft, request.clone())
            .await
            .map_err(|e| NodeError::Write(e.to_string()))?
        {
            WriteReply::Done(response) => return Ok(response),
            WriteReply::ForwardTo { leader_addr } => leader_addr,
        };

        let mut detail = String::from("local write refused: not the leader");
        for _ in 0..Self::FORWARD_ATTEMPTS {
            let Some(addr) = next.take() else { break };
            let peer: SocketAddr = match addr.parse() {
                Ok(peer) => peer,
                Err(e) => {
                    detail = format!("leader hint {addr:?} does not parse: {e}");
                    break;
                }
            };
            let body = serde_json::to_vec(&request)
                .map_err(|e| NodeError::Write(format!("encode forwarded write: {e}")))?;
            crate::metrics::write_forwarded();
            match self
                .client
                .call(peer, "POST", CLUSTER_WRITE_PATH, body)
                .await
            {
                Ok(reply) => {
                    let reply: WriteReply = serde_json::from_slice(&reply)
                        .map_err(|e| NodeError::Write(format!("decode forwarded write: {e}")))?;
                    match reply {
                        WriteReply::Done(response) => return Ok(response),
                        WriteReply::ForwardTo { leader_addr } => {
                            detail = format!("{peer} is not the leader");
                            next = leader_addr;
                        }
                    }
                }
                Err(e) => {
                    detail = format!("forward to {peer}: {e}");
                    break;
                }
            }
        }
        Err(NodeError::Unavailable(detail))
    }

    /// How many leader hops [`Self::submit`] chases before reporting the
    /// cluster unavailable. Bounded so a flapping election cannot park a client
    /// indefinitely (issue #9: "3 bounded retries").
    pub const FORWARD_ATTEMPTS: usize = 3;

    /// Wait until every cluster member's applied index has reached `revision`,
    /// or `timeout` elapses — the read-after-write barrier (issue #9). Returns
    /// the ids of members that had NOT confirmed by the deadline; empty means
    /// the whole fleet has applied the write.
    ///
    /// Peers report over [`CLUSTER_APPLIED_PATH`]; this node answers from its
    /// own state machine. A member that cannot be reached is simply unconfirmed
    /// — the barrier degrades to a warning, never an error (the write is
    /// already durable and committed). "Members" is the full membership,
    /// voters and learners alike: the barrier cannot see a remote node's
    /// readiness gate, so a deliberately draining node may be named in the
    /// warning — informational, not a failure.
    pub async fn await_applied(&self, revision: u64, timeout: Duration) -> Vec<NodeId> {
        let members: Vec<(NodeId, String)> = {
            let receiver = self.raft.metrics();
            let metrics = receiver.borrow();
            metrics
                .membership_config
                .nodes()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect()
        };

        let deadline = tokio::time::Instant::now() + timeout;
        let mut pending: BTreeMap<NodeId, String> = members.into_iter().collect();
        loop {
            let confirmed: Vec<NodeId> = {
                let mut confirmed = Vec::new();
                for (id, addr) in &pending {
                    if *id == self.id {
                        let applied = self.raft.metrics().borrow().last_applied.map(|l| l.index);
                        if applied.is_some_and(|a| a >= revision) {
                            confirmed.push(*id);
                        }
                        continue;
                    }
                    let Ok(peer) = addr.parse::<SocketAddr>() else {
                        continue;
                    };
                    if let Ok(reply) = self
                        .client
                        .call(peer, "POST", CLUSTER_APPLIED_PATH, Vec::new())
                        .await
                        && let Ok(reply) = serde_json::from_slice::<network::AppliedReply>(&reply)
                        && reply.applied.is_some_and(|a| a >= revision)
                    {
                        confirmed.push(*id);
                    }
                }
                confirmed
            };
            for id in confirmed {
                pending.remove(&id);
            }
            if pending.is_empty() || tokio::time::Instant::now() >= deadline {
                crate::metrics::barrier_observed(pending.len());
                return pending.into_keys().collect();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// The leader's applied index as it reports it right now — the catch-up
    /// target the reconciled readiness gate waits on. `None` means no leader is
    /// known or it could not be asked; callers treat that as "not yet" and
    /// retry, so the swallowed transport detail costs nothing but a log line
    /// the rpc layer already writes.
    pub async fn leader_applied(&self) -> Option<u64> {
        let (leader_id, addr) = {
            let receiver = self.raft.metrics();
            let metrics = receiver.borrow();
            let leader_id = metrics.current_leader?;
            let addr = metrics
                .membership_config
                .nodes()
                .find(|(id, _)| **id == leader_id)
                .map(|(_, node)| node.addr.clone())?;
            (leader_id, addr)
        };
        if leader_id == self.id {
            return self.raft.metrics().borrow().last_applied.map(|l| l.index);
        }
        let peer: SocketAddr = addr.parse().ok()?;
        let reply = self
            .client
            .call(peer, "POST", CLUSTER_APPLIED_PATH, Vec::new())
            .await
            .ok()?;
        serde_json::from_slice::<network::AppliedReply>(&reply)
            .ok()
            .and_then(|reply| reply.applied)
    }

    /// Durably park an accepted intent before it is submitted (issue #9 R4).
    pub fn park_intent(&self, request: &ControlRequest) -> Result<(), NodeError> {
        self.sm_reader
            .park_intent(request)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Retire a parked intent once its op is terminal.
    pub fn unpark_intent(&self, op_id: &Uuid) -> Result<(), NodeError> {
        self.sm_reader
            .unpark_intent(op_id)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Every intent this node accepted that has not been retired yet.
    pub fn parked_intents(&self) -> Result<Vec<ControlRequest>, NodeError> {
        self.sm_reader
            .parked_intents()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The recorded outcome of `op_id`, if applied within the dedup window.
    pub fn read_op(&self, op_id: &Uuid) -> Result<Option<ControlResponse>, NodeError> {
        self.sm_reader
            .read_op(op_id)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Whether this node still holds a parked intent for `op_id`.
    pub fn intent_parked(&self, op_id: &Uuid) -> Result<bool, NodeError> {
        self.sm_reader
            .intent_parked(op_id)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Drive the attached engine to the currently applied state — the
    /// cold-start / post-join reconcile. A no-op without an engine.
    pub async fn reconcile_engine(&self) -> Result<(), NodeError> {
        self.sm_reader
            .reconcile_engine()
            .await
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Read the committed imposter-config JSON for the default tenant's `port`
    /// from the applied state machine. Answers from local durable state — it
    /// does not require leadership.
    pub fn get_imposter(&self, port: u16) -> Result<Option<String>, NodeError> {
        self.sm_reader
            .read_config(port)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Last engine side-effect failure per port (0 = set-level): the ports whose
    /// committed config the local engine could not realize (e.g. a bind
    /// failure). Empty on a healthy node.
    #[must_use]
    pub fn apply_failures(&self) -> BTreeMap<u16, String> {
        self.sm_reader.apply_failures()
    }

    /// Every port this node has a committed config for, ascending. Like
    /// [`Self::get_imposter`], this answers from applied local state.
    pub fn configured_ports(&self) -> Result<Vec<u16>, NodeError> {
        self.sm_reader
            .configured_ports()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// A snapshot of the node's current status, from Raft metrics.
    #[must_use]
    pub fn status(&self) -> StatusReport {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        StatusReport {
            node_id: self.id,
            is_leader: metrics.state == ServerState::Leader,
            current_leader: metrics.current_leader,
            last_applied: metrics.last_applied.map(|log_id| log_id.index),
            voters: metrics.membership_config.voter_ids().collect(),
        }
    }

    /// The ownership ring computed from this node's applied membership. Its
    /// `m_idx` is the membership log index, so every node at the same index
    /// derives byte-identical ownership.
    #[must_use]
    pub fn ring(&self) -> Ring {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        let m_idx = metrics
            .membership_config
            .log_id()
            .map_or(0, |log_id| log_id.index);
        Ring::new(metrics.membership_config.voter_ids(), m_idx)
    }

    /// Whether this node is isolated from the cluster's quorum and must refuse
    /// owner-side stateful operations (the isolated-owner rule, RFC-001 §7.2).
    ///
    /// This is a safety gate, so it **fails closed** — every uncertain state
    /// reports isolated. A node is isolated when it knows no current leader (a
    /// follower partitioned away loses its leader once the election timeout
    /// elapses), or when it *is* the leader but does not currently hold a quorum
    /// lease: openraft reports `millis_since_quorum_ack == None` for a leader that
    /// no quorum has acknowledged (a just-elected leader before its first
    /// `AppendEntries` round, or one partitioned from its followers), so that case
    /// is treated as isolated too, not healthy.
    #[must_use]
    pub fn is_isolated(&self) -> bool {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        match metrics.current_leader {
            None => true,
            Some(leader) if leader == self.id => metrics
                .millis_since_quorum_ack
                .is_none_or(|ms| ms > ISOLATION_WINDOW_MS),
            Some(_) => false,
        }
    }

    /// Stop the Raft runtime and release the cluster port. Any in-flight client
    /// writes fail.
    ///
    /// Waits for the accept loop to actually stop before returning, so the port
    /// is released by the time this resolves — otherwise a fast restart on the
    /// same address races a listener that has been aborted but not yet dropped.
    pub async fn shutdown(&self) -> Result<(), NodeError> {
        let raft_stopped = self
            .raft
            .shutdown()
            .await
            .map_err(|e| NodeError::Runtime(e.to_string()));
        // Release the cluster port regardless of how the Raft core stopped: a
        // failed core shutdown must not *also* leak the listener, or the next
        // start on this address fails with a misleading bind error that hides the
        // real cause.
        self.server_task.abort();
        while !self.server_task.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        raft_stopped
    }
}

impl Drop for RaftNode {
    fn drop(&mut self) {
        // Best-effort: release the listener if the node is dropped without an
        // explicit shutdown. The Raft core's own tasks stop when `raft` drops.
        self.server_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const SECRET: &str = "cluster-test-secret";

    fn config_in(dir: &TempDir, id: NodeId) -> NodeConfig {
        NodeConfig {
            node_id: id,
            bind: "127.0.0.1:0".parse().expect("valid bind addr"),
            advertise: None,
            data_dir: dir.path().to_path_buf(),
            secret: Some(SECRET.to_owned()),
            routes: Router::new(),
            engine: None,
        }
    }

    /// A minimal real config for `port`, tagged with `name` so tests can tell
    /// bodies apart the way the spike's opaque strings used to.
    fn imposter(port: u16, name: &str) -> ImposterConfig {
        serde_json::from_value(serde_json::json!({
            "port": port,
            "protocol": "http",
            "host": "127.0.0.1",
            "name": name,
        }))
        .expect("test config parses")
    }

    /// The `name` tag of a stored config body.
    fn name_of(body: &str) -> Option<String> {
        serde_json::from_str::<serde_json::Value>(body)
            .ok()?
            .get("name")?
            .as_str()
            .map(str::to_owned)
    }

    /// Raft publishes `last_applied` into its metrics watch asynchronously, so it
    /// can lag a just-returned `client_write` (or a just-booted core) by a
    /// scheduler tick. Poll, bounded, for it to reach `want`.
    async fn wait_last_applied(node: &RaftNode, want: u64) -> Option<u64> {
        for _ in 0..50 {
            if let Some(i) = node.status().last_applied
                && i >= want
            {
                return Some(i);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        node.status().last_applied
    }

    /// Poll, bounded, until `node`'s committed config for `port` carries `want`
    /// as its name tag.
    async fn wait_config(node: &RaftNode, port: u16, want: &str) -> bool {
        for _ in 0..50 {
            let named = node
                .get_imposter(port)
                .unwrap()
                .and_then(|body| name_of(&body));
            if named.as_deref() == Some(want) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    /// Poll, bounded, until `node`'s effective voter set equals `want`. A
    /// follower's local metrics reflect a committed membership change only once
    /// it has received the corresponding AppendEntries, which can lag the leader.
    async fn wait_voters(node: &RaftNode, want: &BTreeSet<NodeId>) -> BTreeSet<NodeId> {
        for _ in 0..50 {
            let voters: BTreeSet<NodeId> = node.status().voters.into_iter().collect();
            if &voters == want {
                return voters;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        node.status().voters.into_iter().collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_node_init_becomes_leader() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("cluster init");

        let status = node.status();
        assert!(status.is_leader, "sole voter must self-elect: {status:?}");
        assert_eq!(status.current_leader, Some(1));
        assert_eq!(status.voters, vec![1]);
        assert_eq!(node.get_imposter(9999).unwrap(), None);
        node.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn initialized_leader_owns_its_ring_and_is_not_isolated() {
        use crate::raft::ring::{OwnStatus, OwnedKey};
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("cluster init");

        let ring = node.ring();
        assert_eq!(
            ring.members(),
            &[1],
            "ring members come from the applied voters"
        );
        assert_eq!(
            ring.i_own(1, OwnedKey::config("cfg:8080")),
            Some(OwnStatus::Owner),
            "the sole voter owns every config key"
        );
        // The gate fails closed, so a just-elected leader reads isolated until it
        // establishes its first quorum lease — poll for it to clear.
        assert!(
            wait_until(|| !node.is_isolated()).await,
            "a healthy single-node leader must stop reporting isolated"
        );
        node.shutdown().await.expect("shutdown");
    }

    /// Poll a predicate, bounded, returning whether it became true within ~5s.
    async fn wait_until(mut pred: impl FnMut() -> bool) -> bool {
        for _ in 0..50 {
            if pred() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        pred()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uninitialized_node_is_isolated_with_empty_ring() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        // Never initialized: no leader, no membership.
        assert!(
            node.is_isolated(),
            "a node with no known leader must report isolated"
        );
        assert!(node.ring().is_empty(), "no applied membership → empty ring");
        node.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_report_reflects_state() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 42)).await.expect("start");

        let before = node.status();
        assert_eq!(before.node_id, 42);
        assert!(!before.is_leader);
        assert_eq!(before.last_applied, None);

        node.cluster_init().await.expect("cluster init");
        let response = node
            .put_imposter(imposter(8080, "stub-body"))
            .await
            .expect("write");
        assert_eq!(
            response.outcome,
            crate::control::ControlOutcome::Applied,
            "a valid put commits as applied"
        );
        let rev = response.revision;

        let after = node.status();
        assert_eq!(after.node_id, 42);
        assert!(after.is_leader);
        assert_eq!(after.voters, vec![42]);
        assert_eq!(
            wait_last_applied(&node, rev).await,
            Some(rev),
            "applied index must reach the committed write"
        );
        node.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_survives_restart() {
        let dir = TempDir::new().expect("tempdir");
        let rev = {
            let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
            node.cluster_init().await.expect("cluster init");
            let rev = node
                .put_imposter(imposter(8080, "durable-body"))
                .await
                .expect("write")
                .revision;
            assert_eq!(
                node.get_imposter(8080).unwrap().and_then(|b| name_of(&b)),
                Some("durable-body".to_owned())
            );
            node.shutdown().await.expect("shutdown");
            rev
        };

        let node = RaftNode::start(config_in(&dir, 1)).await.expect("restart");
        assert_eq!(
            node.get_imposter(8080).unwrap().and_then(|b| name_of(&b)),
            Some("durable-body".to_owned()),
            "config must survive a full restart (R3)"
        );
        assert_eq!(
            wait_last_applied(&node, rev).await,
            Some(rev),
            "applied index must be recovered from durable state after restart"
        );
        node.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_twice_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("first init");
        let second = node.cluster_init().await;
        assert!(
            matches!(second, Err(NodeError::Init(_))),
            "second cluster-init must be refused, got {second:?}"
        );
        node.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_cluster_via_seed_join_replicates() {
        let (d1, d2, d3) = (
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        );
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
        let n3 = RaftNode::start(config_in(&d3, 3)).await.expect("start n3");

        // Seed-join both followers against the leader.
        n2.join_via(n1.advertise_addr()).await.expect("n2 join");
        n3.join_via(n1.advertise_addr()).await.expect("n3 join");

        // The join path auto-promotes, so all three converge on the same voters.
        let want = BTreeSet::from([1, 2, 3]);
        for node in [&n1, &n2, &n3] {
            assert_eq!(
                wait_voters(node, &want).await,
                want,
                "node {} should see all 3 voters",
                node.id()
            );
        }

        // A write on the leader replicates to every node's applied state.
        n1.put_imposter(imposter(8080, "shared"))
            .await
            .expect("write");
        for node in [&n1, &n2, &n3] {
            assert!(
                wait_config(node, 8080, "shared").await,
                "node {} must see the replicated write",
                node.id()
            );
        }

        for node in [&n1, &n2, &n3] {
            node.shutdown().await.expect("shutdown");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_becomes_isolated_when_it_loses_quorum() {
        let (d1, d2, d3) = (
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        );
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
        let n3 = RaftNode::start(config_in(&d3, 3)).await.expect("start n3");
        n2.join_via(n1.advertise_addr()).await.expect("n2 join");
        n3.join_via(n1.advertise_addr()).await.expect("n3 join");
        // n1 (the bootstrapping leader) keeps leadership; wait until it holds a
        // quorum lease so the "was healthy, then lost it" transition is real.
        assert!(
            wait_until(|| n1.status().is_leader && !n1.is_isolated()).await,
            "leader should hold a quorum lease once the cluster is formed"
        );
        // A healthy follower connected to the leader is not isolated.
        assert!(
            !n2.is_isolated(),
            "a follower that hears the leader is not isolated"
        );

        // Kill both followers: the leader can no longer reach a quorum and must
        // report isolated (whether by losing its lease or stepping down) so it
        // refuses owner-side ops — the isolated-owner safety property.
        n2.shutdown().await.ok();
        n3.shutdown().await.ok();
        assert!(
            wait_until(|| n1.is_isolated()).await,
            "a leader that lost its quorum must report isolated"
        );
        n1.shutdown().await.ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn follower_write_is_rejected() {
        let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
        n2.join_via(n1.advertise_addr()).await.expect("n2 join");

        // A write submitted to the follower must not silently succeed: openraft
        // refuses it (forward-to-leader), surfaced as a typed error.
        let err = n2.put_imposter(imposter(8080, "on-follower")).await;
        assert!(
            matches!(err, Err(NodeError::Write(_))),
            "follower write must be rejected, got {err:?}"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_failover_elects_new_leader() {
        let (d1, d2, d3) = (
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        );
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
        let n3 = RaftNode::start(config_in(&d3, 3)).await.expect("start n3");
        n2.join_via(n1.advertise_addr()).await.expect("n2 join");
        n3.join_via(n1.advertise_addr()).await.expect("n3 join");

        // Kill the leader; the remaining two must elect a new one.
        n1.shutdown().await.expect("shutdown n1");

        let mut elected = None;
        for _ in 0..100 {
            for node in [&n2, &n3] {
                let s = node.status();
                if s.is_leader && matches!(s.current_leader, Some(2 | 3)) {
                    elected = Some(node);
                    break;
                }
            }
            if elected.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let leader = elected.expect("a new leader must be elected after the old one dies");

        // The new leader can commit a write (proves it has a live quorum).
        leader
            .put_imposter(imposter(9090, "after-failover"))
            .await
            .expect("write on new leader");

        n2.shutdown().await.ok();
        n3.shutdown().await.ok();
    }

    /// Two nodes seeding off the same leader at the same time. Each admission is
    /// its own membership change, so without a commit barrier between
    /// `add_learner` and the voter promotion the second admission observes the
    /// first one's entry still uncommitted and openraft rejects it outright
    /// (`InProgress`). This is the deterministic form of the intermittent
    /// single-join race in #38.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_seed_joins_all_become_voters() {
        let (d1, d2, d3) = (
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        );
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
        let n3 = RaftNode::start(config_in(&d3, 3)).await.expect("start n3");

        let seed = n1.advertise_addr();
        let (r2, r3) = tokio::join!(n2.join_via(seed), n3.join_via(seed));
        r2.expect("n2 join must not lose the admission race");
        r3.expect("n3 join must not lose the admission race");

        let voters = n1.status().voters;
        assert!(
            voters.contains(&2) && voters.contains(&3),
            "both joiners must be promoted to voter, got {voters:?}"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
        n3.shutdown().await.ok();
    }

    /// The same contention, repeated. One concurrent pass can get lucky on the
    /// interleaving; the failure this guards was intermittent in CI, so the gate
    /// repeats it. (A *sequential* join loop was measured not to reproduce #38 at
    /// all — `add_learner` returns after its own entry applies — so repeating one
    /// would only buy runtime.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_seed_joins_survive_repetition() {
        for round in 0..4 {
            let (d1, d2, d3) = (
                TempDir::new().unwrap(),
                TempDir::new().unwrap(),
                TempDir::new().unwrap(),
            );
            let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
            n1.cluster_init().await.expect("init n1");
            let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
            let n3 = RaftNode::start(config_in(&d3, 3)).await.expect("start n3");

            let seed = n1.advertise_addr();
            let (r2, r3) = tokio::join!(n2.join_via(seed), n3.join_via(seed));
            r2.unwrap_or_else(|e| panic!("round {round}: n2 join lost the admission race: {e}"));
            r3.unwrap_or_else(|e| panic!("round {round}: n3 join lost the admission race: {e}"));

            let voters = n1.status().voters;
            assert!(
                voters.contains(&2) && voters.contains(&3),
                "round {round}: both joiners must be voters, got {voters:?}"
            );

            n1.shutdown().await.ok();
            n2.shutdown().await.ok();
            n3.shutdown().await.ok();
        }
    }
}
