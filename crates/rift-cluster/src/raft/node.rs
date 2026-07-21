//! The Raft control-plane node: an [`openraft::Raft`] wired to the redb-backed
//! storage, plus `--cluster-init` single-node bootstrap and a [`StatusReport`]
//! read from Raft's own metrics.
//!
//! This is the single-node layer. The [`RaftNetworkFactory`] used here is a
//! placeholder that reports every peer unreachable — correct for a one-voter
//! cluster, which never sends an RPC, and it is the seam the real RPC-backed
//! network (over the #8 cluster transport) replaces when multi-node join lands.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{RPCError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, Raft, RaftNetwork, RaftNetworkFactory, ServerState};

use super::store::{self, RedbStateMachine};
use super::{ControlOp, NodeId, TypeConfig};

/// Log-file name for the Raft storage inside the node's data directory.
const RAFT_DB_FILE: &str = "raft.redb";

/// How long `--cluster-init` waits for the founding node to elect itself leader
/// before giving up. A single voter wins immediately; this only bounds a stuck
/// startup.
const INIT_LEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// Everything a [`RaftNode`] needs to start.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// This node's persisted Raft id (see [`super::identity`]).
    pub node_id: NodeId,
    /// The address peers reach this node on, recorded in the membership config.
    pub advertise_addr: String,
    /// Directory holding this node's Raft log/vote/snapshot database.
    pub data_dir: PathBuf,
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
    advertise_addr: String,
    raft: Raft<TypeConfig>,
    // A read-only handle onto the same state machine openraft owns as `&mut`, so
    // the node can answer committed-config reads without going through Raft.
    sm_reader: RedbStateMachine,
}

impl RaftNode {
    /// Open the node's storage and start its Raft runtime. This does not form or
    /// join a cluster; call [`RaftNode::cluster_init`] to bootstrap a new one, or
    /// (once it lands) the join path to attach to an existing one.
    pub async fn start(config: NodeConfig) -> Result<Self, NodeError> {
        let (log_store, state_machine) = store::new(config.data_dir.join(RAFT_DB_FILE))
            .await
            .map_err(|e| NodeError::Storage(e.to_string()))?;
        let sm_reader = state_machine.clone();

        let raft_config = Arc::new(
            Config {
                cluster_name: "rift-control-plane".to_owned(),
                election_timeout_min: 150,
                election_timeout_max: 300,
                heartbeat_interval: 50,
                ..Default::default()
            }
            .validate()
            .map_err(|e| NodeError::Init(e.to_string()))?,
        );

        let raft = Raft::new(
            config.node_id,
            raft_config,
            UnreachableNetwork,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| NodeError::Runtime(e.to_string()))?;

        Ok(Self {
            id: config.node_id,
            advertise_addr: config.advertise_addr,
            raft,
            sm_reader,
        })
    }

    /// This node's Raft id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Bootstrap a brand-new single-node cluster with this node as the sole
    /// voter, then wait for it to elect itself leader.
    ///
    /// Initializing a node that already has a membership entry is refused by
    /// openraft and surfaced as [`NodeError::Init`], so a second `--cluster-init`
    /// (including after a restart) does not silently fork a new cluster.
    pub async fn cluster_init(&self) -> Result<(), NodeError> {
        let members = BTreeMap::from([(self.id, BasicNode::new(self.advertise_addr.clone()))]);
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

    /// Submit an imposter-config write through Raft and return its committed
    /// revision (the applied log index). Fails if this node is not the leader or
    /// the entry does not commit.
    pub async fn put_imposter(&self, port: u16, body: String) -> Result<u64, NodeError> {
        let response = self
            .raft
            .client_write(ControlOp::PutImposter { port, body })
            .await
            .map_err(|e| NodeError::Write(e.to_string()))?;
        Ok(response.data.revision)
    }

    /// Read the committed imposter-config body for `port` from the applied state
    /// machine. Answers from local durable state — it does not require leadership.
    pub fn get_imposter(&self, port: u16) -> Result<Option<String>, NodeError> {
        self.sm_reader
            .read_config(port)
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

    /// Stop the Raft runtime. Any in-flight client writes fail.
    pub async fn shutdown(&self) -> Result<(), NodeError> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| NodeError::Runtime(e.to_string()))
    }
}

/// Placeholder network for the single-node phase: reports every peer unreachable.
///
/// A one-voter cluster never sends a Raft RPC, so these methods are never called
/// in the single-node path; returning [`Unreachable`] (rather than panicking)
/// keeps the type honest for the moment multi-node wiring replaces it, and means
/// a stray call degrades to a retryable backoff instead of a crash.
#[derive(Debug, Clone)]
struct UnreachableNetwork;

impl RaftNetworkFactory<TypeConfig> for UnreachableNetwork {
    type Network = UnreachableNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        self.clone()
    }
}

fn unreachable_rpc(
    target: &str,
) -> RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>> {
    let io = std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("single-node network cannot reach {target}: RPC network not yet wired (#6 item 2)"),
    );
    RPCError::Unreachable(Unreachable::new(&io))
}

impl RaftNetwork<TypeConfig> for UnreachableNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        Err(unreachable_rpc("append_entries"))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<
            NodeId,
            BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let io = std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "single-node network cannot install snapshot: RPC network not yet wired (#6 item 2)",
        );
        Err(RPCError::Unreachable(Unreachable::new(&io)))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>>
    {
        Err(unreachable_rpc("vote"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn config_in(dir: &TempDir, id: NodeId) -> NodeConfig {
        NodeConfig {
            node_id: id,
            advertise_addr: "127.0.0.1:7001".to_owned(),
            data_dir: dir.path().to_path_buf(),
        }
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_node_init_becomes_leader() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("cluster init");

        let status = node.status();
        assert!(status.is_leader, "sole voter must self-elect: {status:?}");
        assert_eq!(status.current_leader, Some(1));
        assert_eq!(status.voters, vec![1]);
        // A port that was never written reads back as absent, not as an error.
        assert_eq!(node.get_imposter(9999).unwrap(), None);
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
        let rev = node
            .put_imposter(8080, "stub-body".to_owned())
            .await
            .expect("write");

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
                .put_imposter(8080, "durable-body".to_owned())
                .await
                .expect("write");
            assert_eq!(
                node.get_imposter(8080).unwrap().as_deref(),
                Some("durable-body")
            );
            node.shutdown().await.expect("shutdown");
            rev
        };

        // Restart against the same data dir: the committed config must still be
        // readable without re-initializing or re-writing anything.
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("restart");
        assert_eq!(
            node.get_imposter(8080).unwrap().as_deref(),
            Some("durable-body"),
            "config must survive a full restart (R3)"
        );

        // The applied cursor is loaded into Raft metrics asynchronously as the
        // core boots, so poll (bounded) rather than reading it the instant after
        // start — the criterion is that it *recovers* to the committed index.
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
}
