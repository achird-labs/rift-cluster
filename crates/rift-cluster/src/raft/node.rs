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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::{BasicNode, Config, Raft, ServerState};
use rift_ee::seams::{ImposterConfig, ImposterManager};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::network::{
    self, CLUSTER_APPLIED_PATH, CLUSTER_JOIN_PATH, CLUSTER_LEAVE_PATH, CLUSTER_WRITE_PATH,
    JoinRequest, LeaveRequest, RaftSlot, RpcNetwork, WriteReply,
};
use super::ring::Ring;
use super::store::{self, RedbStateMachine};
use super::{NodeId, TypeConfig};
use crate::control::{ControlOp, ControlRequest, ControlResponse, TenantId};
use crate::rpc::{
    Authority, DnsResolver, PeerResolver, Router, RpcClient, RpcClientConfig, RpcServer,
    RpcServerConfig, Signer, TrackedPeerHealth, Verifier,
};

/// Log-file name for the Raft storage inside the node's data directory.
const RAFT_DB_FILE: &str = "raft.redb";

/// How long [`RaftNode::shutdown`] waits for the openraft core to drop its
/// storage handles. The core stops within a few scheduler ticks of acknowledging
/// the shutdown; this only bounds a pathologically stuck teardown so shutdown can
/// never hang forever.
const STORAGE_RELEASE_TIMEOUT: Duration = Duration::from_secs(2);

/// The number of `Arc<redb::Database>` clones a stopped `RaftNode` still holds
/// itself: exactly one, the `sm_reader` state-machine handle. Once
/// [`RedbStateMachine::db_refs`] falls to this, every clone openraft owned has
/// been dropped and the redb lock is releasable. Asserted in the tests so a
/// future field that clones the database fails loudly here instead of hanging the
/// shutdown wait.
const NODE_HELD_DB_REFS: usize = 1;

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

/// How long [`RaftNode::leave`] waits between polls while chasing a leader
/// hint that is moving (a demote just committed but the new leader has not
/// settled, or metrics have not caught up yet). Small relative to typical
/// caller deadlines, so a bounded `leave` still gets several tries.
const LEAVE_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How often [`RaftNode::await_membership_loaded`] re-reads the metrics watch.
const MEMBERSHIP_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Everything a [`RaftNode`] needs to start.
#[derive(Clone)]
pub struct NodeConfig {
    /// This node's persisted Raft id (see [`super::identity`]).
    pub node_id: NodeId,
    /// The address to bind the cluster port on. `:0` binds an ephemeral port.
    pub bind: SocketAddr,
    /// The authority peers dial this node on. Defaults to the bound address,
    /// which is correct when bind and advertise are the same host:port.
    pub advertise: Option<Authority>,
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

    /// A write or init was attempted on a node that is not the leader.
    /// `leader` is this node's current best hint, if it has one — never
    /// openraft's `BasicNode`, so this stays a plain, stable id.
    #[error("not leader (leader: {leader:?})")]
    NotLeader { leader: Option<NodeId> },

    /// `--cluster-init` was refused because this node's log already carries a
    /// membership entry (a restart, or a second `--cluster-init`).
    #[error("this node is already initialized")]
    AlreadyInitialized,
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

/// What a [`RaftNode::leave`] actually did.
///
/// `Ok` alone cannot say, because a leave has three successful shapes and only
/// one of them removes this node:
///
/// - it was evicted just now, or was already out of the membership;
/// - it is the only voter, so there is nothing to hand its votes to — openraft
///   refuses to commit an empty voter set, and a solo node stays a full member;
/// - the leader **refused** it, because removing this node would drop the
///   cluster below its voter floor (issue #69) — the common case in a
///   whole-fleet teardown, where every node is asked to leave at once.
///
/// Callers that record a departure must distinguish these, or they persist
/// "this node left" about a node that is still a member and strand it on the
/// next start (issue #72).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaveOutcome {
    /// This node is out of the membership — it was evicted just now, or it
    /// already was.
    Departed,
    /// This node is still a member and deliberately did not leave. Its exit is
    /// crash-equivalent: the fleet still counts it, and a restart resumes.
    Retained,
}

/// The control-plane Raft node.
pub struct RaftNode {
    id: NodeId,
    advertise: Authority,
    raft: Raft<TypeConfig>,
    // Authenticated client for node-driven admin RPCs (e.g. seed join). Shares
    // the same signer/pool the Raft network uses.
    client: RpcClient,
    // The same resolver the Raft network sends through, so a peer address the
    // node dials directly (the leader it asks to evict it) is resolved exactly
    // as replication resolves it — a DNS advertise address must not work for
    // one and silently fail for the other.
    resolver: Arc<dyn PeerResolver>,
    // A read-only handle onto the same state machine openraft owns as `&mut`, so
    // the node can answer committed-config reads without going through Raft.
    sm_reader: RedbStateMachine,
    // The cluster server accept loop. Aborted on shutdown/drop so the listener is
    // released with the node.
    server_task: JoinHandle<()>,
    // Whether shutdown() was ever invoked, so Drop can warn when a node is
    // dropped without the shutdown-then-drop contract — storage release is only
    // guaranteed through shutdown() (see Drop).
    shutdown_invoked: AtomicBool,
    // The most recent reason a leave attempt failed, so the deadline's error can
    // name a cause instead of only reporting that time ran out.
    last_leave_error: Mutex<Option<String>>,
    // Serializes the membership changes this node arbitrates as leader. Shared
    // with the control routes so a departure this node evicts locally and one
    // it evicts for a peer take the same lock — the voter floor and the
    // auto-voter ceiling are only exact if every path holds it (#55, #69).
    membership_gate: network::MembershipGate,
    // Signals that parked intents deserve a drain attempt now, rather than at
    // the composition's next periodic sweep (#83). Lives here because this node
    // owns the parked-intent ledger (`park_intent`/`parked_intents`/
    // `unpark_intent`); whoever drains it is a composition concern, but the
    // "there is something to drain" fact is this node's.
    replay_wake: Arc<tokio::sync::Notify>,
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
        let membership_gate: network::MembershipGate = Arc::new(tokio::sync::Mutex::new(()));
        let router = network::control_routes(
            config.routes.clone(),
            slot.clone(),
            Arc::clone(&membership_gate),
        );

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
        let advertise = config.advertise.unwrap_or_else(|| Authority::from(local));

        let client = RpcClient::new(
            signer,
            Arc::new(TrackedPeerHealth::new()),
            RpcClientConfig::default(),
        );
        let resolver: Arc<dyn PeerResolver> = Arc::new(DnsResolver);
        let network = RpcNetwork::new(client.clone(), Arc::clone(&resolver));

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
            resolver,
            sm_reader,
            server_task,
            shutdown_invoked: AtomicBool::new(false),
            last_leave_error: Mutex::new(None),
            membership_gate,
            replay_wake: Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// This node's Raft id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The authority peers dial this node on.
    #[must_use]
    pub fn advertise(&self) -> &Authority {
        &self.advertise
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
    /// openraft and surfaced as [`NodeError::AlreadyInitialized`], so a second
    /// `--cluster-init` (including after a restart) does not silently fork a
    /// new cluster.
    pub async fn cluster_init(&self) -> Result<(), NodeError> {
        let members = BTreeMap::from([(self.id, BasicNode::new(self.advertise.to_string()))]);
        self.raft.initialize(members).await.map_err(map_init_err)?;

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
    pub async fn add_learner(&self, id: NodeId, addr: &Authority) -> Result<(), NodeError> {
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
    pub async fn join_via(&self, seed: &Authority) -> Result<(), NodeError> {
        let request = JoinRequest {
            node_id: self.id,
            advertise: self.advertise.to_string(),
        };
        let body =
            serde_json::to_vec(&request).map_err(|e| NodeError::Membership(e.to_string()))?;
        self.call_any(seed.as_str(), "POST", CLUSTER_JOIN_PATH, body)
            .await
            .map_err(NodeError::Membership)?;
        Ok(())
    }

    /// Best-effort, deadline-bounded departure from the cluster (issue #6).
    ///
    /// If this node is currently the leader, it evicts itself in one local
    /// call: openraft's own model allows a leader to be a non-voter (see
    /// `RaftState::is_leading` in the vendored openraft source — leadership
    /// only requires *membership*, and a leader stays "leading" through the
    /// demote-to-learner half of the departure, only actually handing off once
    /// the *second* write drops it from membership entirely). If that local
    /// attempt fails partway (leadership moved mid-flight) — or this node was
    /// never leader to begin with — completion falls back to asking whichever
    /// node the current leader hint names, chasing it while leadership settles.
    /// Never blocks past `timeout`: on elapse this returns
    /// [`NodeError::Timeout`] rather than hanging shutdown.
    ///
    /// **Succeeding is not the same as having left.** The leader refuses a
    /// departure that would drop the cluster below its voter floor (issue #69),
    /// and a sole voter cannot leave at all; both return
    /// [`LeaveOutcome::Retained`]. Match on the outcome — never on `Ok` — before
    /// recording anywhere that this node departed.
    pub async fn leave(&self, timeout: Duration) -> Result<LeaveOutcome, NodeError> {
        tokio::time::timeout(timeout, self.leave_inner())
            .await
            .unwrap_or_else(|_| {
                // The loop's last real cause, not just "it timed out": auth
                // failure, protocol skew and an unreachable leader all look the
                // same from out here, and they need different operator actions.
                let cause = self
                    .last_leave_error
                    .lock()
                    .ok()
                    .and_then(|slot| slot.clone())
                    .unwrap_or_else(|| "no attempt reported a cause".to_owned());
                Err(NodeError::Timeout {
                    what: "cluster leave",
                    detail: format!("did not complete within {timeout:?}; last cause: {cause}"),
                })
            })
    }

    async fn leave_inner(&self) -> Result<LeaveOutcome, NodeError> {
        if !self.in_membership() {
            return Ok(LeaveOutcome::Departed);
        }

        // openraft refuses a membership change that would empty the voter set
        // (`EmptyMembership`), so a sole voter can never leave. Spinning until
        // the deadline would burn the entire budget `graceful_leave` shares
        // between leaving and draining, leaving a solo node — a supported mode —
        // with no drain at all.
        if self.is_sole_voter() {
            return Ok(LeaveOutcome::Retained);
        }

        // Why the leader retries *inside* the loop rather than once before it:
        // if the demote commits but the removal does not, this node is a learner
        // that is still leading, and then `current_leader` reports `None` on
        // every node including this one — so the RPC path has nothing to chase
        // and could never finish what the local path started.
        loop {
            if !self.in_membership() {
                return Ok(LeaveOutcome::Departed);
            }

            let attempt = if self.status().is_leader {
                network::evict(&self.raft, &self.membership_gate, self.id)
                    .await
                    .map(|outcome| match outcome {
                        network::EvictOutcome::Removed => LeaveOutcome::Departed,
                        network::EvictOutcome::HeldByFloor => LeaveOutcome::Retained,
                    })
                    .map_err(|e| format!("local eviction: {e}"))
            } else if let Some(authority) = self.leader_authority() {
                self.leave_via(&authority).await
            } else {
                Err("no leader is known".to_owned())
            };

            // A floor refusal ends the loop rather than retrying it: the answer
            // is deterministic while the membership is unchanged, so spinning
            // would only burn the drain budget this shares with the shutdown.
            match attempt {
                Ok(outcome) => return Ok(outcome),
                Err(cause) => self.record_leave_error(cause),
            }
            tokio::time::sleep(LEAVE_POLL_INTERVAL).await;
        }
    }

    /// Ask the node at `authority` to evict this one. Errors are returned as
    /// text so the retry loop can keep the most recent cause for the timeout —
    /// an operator whose fleet stopped leaving needs to know whether it was
    /// auth, protocol skew, or an unreachable leader.
    async fn leave_via(&self, authority: &str) -> Result<LeaveOutcome, String> {
        let request = LeaveRequest { node_id: self.id };
        let body = serde_json::to_vec(&request).map_err(|e| format!("encode leave: {e}"))?;
        let reply = self
            .call_any(authority, "POST", CLUSTER_LEAVE_PATH, body)
            .await
            .map_err(|e| format!("leave via {e}"))?;

        // The leader answers whether it actually removed this node: the voter
        // floor can refuse a departure and still reply successfully (#69).
        // Reading it wrong in the safe direction matters — a refusal misread as
        // a departure would record a marker for a node that is still a member.
        let accepted: network::LeaveAccepted = serde_json::from_slice(&reply)
            .map_err(|e| format!("decode leave reply from {authority}: {e}"))?;
        Ok(if accepted.evicted {
            LeaveOutcome::Departed
        } else {
            LeaveOutcome::Retained
        })
    }

    /// Keep the latest failure so the deadline's error can name a cause. A
    /// poisoned lock loses the diagnostic, never the departure.
    fn record_leave_error(&self, cause: String) {
        if let Ok(mut slot) = self.last_leave_error.lock() {
            *slot = Some(cause);
        }
    }

    /// Whether this node is the only voter, i.e. there is no one to hand to.
    fn is_sole_voter(&self) -> bool {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        let voters: Vec<_> = metrics.membership_config.voter_ids().collect();
        voters == [self.id]
    }

    /// Whether this node's id still appears anywhere in the currently
    /// effective membership (voter or learner) — whether there is anything
    /// left to leave.
    ///
    /// This is *local* knowledge, read from the membership the durable log
    /// carries, and it can be stale-true: eviction is two committed entries and
    /// the leader stops replicating to a node once its removal takes effect, so
    /// a departing node routinely shuts down without ever receiving the entry
    /// that removed it. A restart therefore cannot treat `true` as proof of
    /// membership — that is what the departure marker in `rift-ee-server` is
    /// for (issue #72). `false`, on the other hand, is conclusive.
    #[must_use]
    pub fn in_membership(&self) -> bool {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        metrics
            .membership_config
            .nodes()
            .any(|(id, _)| *id == self.id)
    }

    /// Wait, bounded, for the durable membership to reach this node's metrics.
    /// Returns whether it became visible.
    ///
    /// [`RaftNode::start`] returns once the core is running, but the membership
    /// its log carries lands in the metrics watch a moment later. Until it does,
    /// [`in_membership`](Self::in_membership) and [`known_peers`](Self::known_peers)
    /// both read empty — which is indistinguishable from "this node was removed",
    /// and acting on that would send a perfectly good node down the rejoin path
    /// or, with nothing to rejoin through, refuse its start outright.
    ///
    /// An initialized node always has a non-empty membership — openraft refuses
    /// to commit an empty one — so "non-empty" is the signal that it has loaded.
    pub async fn await_membership_loaded(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.membership_is_loaded() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(MEMBERSHIP_POLL_INTERVAL).await;
        }
    }

    fn membership_is_loaded(&self) -> bool {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        metrics.membership_config.nodes().next().is_some()
    }

    /// The advertise authorities of every *other* node in the membership this
    /// node's durable log carries.
    ///
    /// A node that has to rejoin needs somewhere to ask, and its own log already
    /// records the fleet it belonged to. That is what makes a **founder**
    /// recoverable: it has no `--cluster-seeds` by construction — it founded the
    /// cluster, so there was nothing to seed from — and without this it would
    /// have no way back after a graceful leave (issue #72).
    ///
    /// Entries can be stale, which costs a failed attempt and nothing else: the
    /// caller tries the next peer.
    #[must_use]
    pub fn known_peers(&self) -> Vec<String> {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        metrics
            .membership_config
            .nodes()
            .filter(|(id, _)| **id != self.id)
            .map(|(_, node)| node.addr.clone())
            .collect()
    }

    /// The current leader's advertise authority, if metrics know one right now.
    ///
    /// Deliberately returns the *unresolved* string: resolution blocks, and a
    /// blocking call cannot be interrupted by the `tokio::time::timeout` that
    /// bounds [`leave`](Self::leave) — doing it here would make the "never
    /// blocks past `timeout`" contract false and stall a runtime worker for the
    /// OS resolver's own timeout, on exactly the degraded-DNS pod this is for.
    fn leader_authority(&self) -> Option<String> {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        let leader_id = metrics.current_leader?;
        metrics
            .membership_config
            .nodes()
            .find(|(id, _)| **id == leader_id)
            .map(|(_, node)| node.addr.clone())
    }

    /// Resolve a peer authority off the runtime thread, via the same resolver
    /// replication uses — including its literal-address fast path.
    async fn resolve(&self, authority: &str) -> std::io::Result<Vec<SocketAddr>> {
        network::resolve_authority(&self.resolver, authority).await
    }

    /// Resolve `authority` and call it, trying every address the name yields
    /// until one answers (#79).
    ///
    /// A name that resolves to several addresses — a dual-stack record, a
    /// multi-A headless service — is only unreachable when *all* of them are.
    /// Committing to the first is what made a peer permanently undialable while
    /// a live address sat second in the same answer.
    async fn call_any(
        &self,
        authority: &str,
        method: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        let addrs = self
            .resolve(authority)
            .await
            .map_err(|e| format!("resolve {authority}: {e}"))?;
        let mut last = String::from("no addresses to try");
        for peer in &addrs {
            match self.client.call(*peer, method, path, body.clone()).await {
                Ok(reply) => return Ok(reply),
                Err(e) => last = format!("{peer}: {e}"),
            }
        }
        Err(format!("{authority} unreachable ({last})"))
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
            .map_err(map_write_err)?;
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
            expected_revision: None,
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
            let body = serde_json::to_vec(&request)
                .map_err(|e| NodeError::Write(format!("encode forwarded write: {e}")))?;
            crate::metrics::write_forwarded();
            match self.call_any(&addr, "POST", CLUSTER_WRITE_PATH, body).await {
                Ok(reply) => {
                    let reply: WriteReply = serde_json::from_slice(&reply)
                        .map_err(|e| NodeError::Write(format!("decode forwarded write: {e}")))?;
                    match reply {
                        WriteReply::Done(response) => return Ok(response),
                        WriteReply::ForwardTo { leader_addr } => {
                            detail = format!("{addr} is not the leader");
                            next = leader_addr;
                        }
                    }
                }
                Err(e) => {
                    detail = format!("forward to {e}");
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
    /// Peers report over the cluster applied-index endpoint; this node answers from its
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

        // Resolved once, before the retry loop — not per round. Resolution is a
        // blocking call on the pool, and `spawn_blocking` cannot be cancelled,
        // so a name that is slow or dead would add the OS resolver's own
        // timeout to *every* pass and push this past the `timeout` this
        // function promises to honour. It is the same hazard `leader_authority`
        // documents for `leave`. Freshness is not lost that matters: a barrier
        // is one write, and no address usefully changes inside its window.
        let mut pending: BTreeMap<NodeId, Vec<SocketAddr>> = BTreeMap::new();
        for (id, addr) in members {
            if id == self.id {
                pending.insert(id, Vec::new());
                continue;
            }
            match self.resolve(&addr).await {
                Ok(peers) => {
                    pending.insert(id, peers);
                }
                Err(e) => {
                    // Not inserted, so it can never be confirmed — it is
                    // reported unapplied, which is the barrier's documented
                    // degrade. Logged once, not once per 25 ms round.
                    tracing::debug!(
                        node_id = id,
                        %addr,
                        error = %e,
                        "await_applied: peer address did not resolve; leaving it unconfirmed"
                    );
                    pending.insert(id, Vec::new());
                }
            }
        }

        loop {
            let confirmed: Vec<NodeId> = {
                let mut confirmed = Vec::new();
                for (id, peers) in &pending {
                    if *id == self.id {
                        let applied = self.raft.metrics().borrow().last_applied.map(|l| l.index);
                        if applied.is_some_and(|a| a >= revision) {
                            confirmed.push(*id);
                        }
                        continue;
                    }
                    // Any of the peer's addresses confirming is the peer
                    // confirming: they are the same process. A dead address in
                    // the set costs one fast-failed call per round, not a
                    // falsely-unconfirmed member (#79).
                    for peer in peers {
                        if let Ok(reply) = self
                            .client
                            .call(*peer, "POST", CLUSTER_APPLIED_PATH, Vec::new())
                            .await
                            && let Ok(reply) =
                                serde_json::from_slice::<network::AppliedReply>(&reply)
                            && reply.applied.is_some_and(|a| a >= revision)
                        {
                            confirmed.push(*id);
                            break;
                        }
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
        let reply = match self
            .call_any(&addr, "POST", CLUSTER_APPLIED_PATH, Vec::new())
            .await
        {
            Ok(reply) => reply,
            Err(e) => {
                tracing::debug!(
                    %addr,
                    error = %e,
                    "leader_applied: leader could not be reached; reporting not-yet"
                );
                return None;
            }
        };
        serde_json::from_slice::<network::AppliedReply>(&reply)
            .ok()
            .and_then(|reply| reply.applied)
    }

    /// Ask whoever drains parked intents to do so now (#83).
    ///
    /// Called when an intent has been parked and the caller has *failed* to
    /// apply it — never on the ordinary park-then-submit path, where the caller
    /// is about to submit it anyway and a concurrent drain would duplicate the
    /// submit on every single write.
    ///
    /// Best-effort by construction: a wake with no drainer listening is
    /// dropped, and the periodic sweep remains the backstop.
    pub fn request_replay(&self) {
        self.replay_wake.notify_one();
    }

    /// A handle to wait on [`Self::request_replay`] — the drainer's side.
    ///
    /// Handed out as an `Arc` rather than awaited through `&self` on purpose:
    /// the drainer holds this node by `Weak` precisely so it never keeps it
    /// alive, and awaiting through `&self` would force it to hold a strong
    /// reference across the wait. `RaftNode::Drop` releases the redb lock and
    /// the cluster port, so delaying it by even one wait interval is a race for
    /// anything that restarts a node onto the same state directory.
    #[must_use]
    pub fn replay_waker(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.replay_wake)
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

    /// Stop the Raft runtime, release the cluster port, and wait for storage to
    /// be released. Any in-flight client writes fail.
    ///
    /// Waits for two teardown steps so that by the time this resolves a fast
    /// restart on the same address *and* data directory cannot race the stopping
    /// node:
    /// - the accept loop actually stops, so the cluster port is free (otherwise a
    ///   restart races a listener that has been aborted but not yet dropped);
    /// - the openraft core drops its `Arc<redb::Database>` clones, so the redb
    ///   file lock is releasable. `Raft::shutdown()` returns once the core
    ///   acknowledges the stop, but the core drops its storage a few ticks later;
    ///   until it does, the last database handle keeps the lock and an immediate
    ///   restart fails with "Database already open" (#41).
    ///
    /// After this returns `Ok`, the node's own `sm_reader` holds the *last*
    /// database handle, so dropping the node afterwards releases the redb file
    /// lock synchronously — shutdown-then-drop is the contract. (Behind an
    /// `Arc`, the lock is finally released when the last clone drops.) Dropping
    /// a node *without* calling this gives no such guarantee — see the `Drop`
    /// impl (#54).
    pub async fn shutdown(&self) -> Result<(), NodeError> {
        // Set on invocation, not success: a failed shutdown already returned
        // its error to the caller — a second warn from Drop would point at the
        // wrong contract.
        self.shutdown_invoked.store(true, Ordering::Relaxed);
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
        // Prefer a Raft-core shutdown failure over the storage-release outcome: a
        // core that failed to stop cleanly never drops its storage, so the wait
        // below would time out and mask the actual cause. When the core stopped
        // cleanly, a storage-release timeout is the real (and only) failure.
        let storage_released = self.await_storage_release().await;
        raft_stopped.and(storage_released)
    }

    /// Wait until the only remaining handle on the storage database is this node's
    /// own `sm_reader`, i.e. openraft has dropped its log-store and state-machine
    /// clones. Bounded by [`STORAGE_RELEASE_TIMEOUT`] so a stuck teardown surfaces
    /// as an error instead of hanging shutdown.
    async fn await_storage_release(&self) -> Result<(), NodeError> {
        let deadline = tokio::time::Instant::now() + STORAGE_RELEASE_TIMEOUT;
        while self.sm_reader.db_refs() > NODE_HELD_DB_REFS {
            if tokio::time::Instant::now() >= deadline {
                return Err(NodeError::Runtime(format!(
                    "raft core did not release storage within {STORAGE_RELEASE_TIMEOUT:?} \
                     ({} database handles still live)",
                    self.sm_reader.db_refs()
                )));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok(())
    }

    /// Live `Arc<redb::Database>` clone count, for tests asserting the shutdown
    /// storage-release contract.
    #[cfg(test)]
    fn storage_refs(&self) -> usize {
        self.sm_reader.db_refs()
    }

    /// A test-only extra handle on the storage database, standing in for a clone
    /// openraft has not yet dropped, so a test can force the shutdown
    /// storage-release wait to observe an outstanding reference.
    #[cfg(test)]
    fn clone_storage_handle(&self) -> RedbStateMachine {
        self.sm_reader.clone()
    }
}

/// Map openraft's `initialize` failure onto the typed surface: an
/// already-initialized node is its own variant, matched structurally against
/// openraft's typed error (never its rendered message) so a reworded error
/// text can't silently stop this from firing. Every other failure keeps its
/// raw detail.
fn map_init_err(e: RaftError<NodeId, InitializeError<NodeId, BasicNode>>) -> NodeError {
    match e {
        RaftError::APIError(InitializeError::NotAllowed(_)) => NodeError::AlreadyInitialized,
        other => NodeError::Init(other.to_string()),
    }
}

/// Map openraft's `client_write` failure onto the typed surface: a refusal
/// because this node is not the leader carries the leader hint as a plain
/// `Option<NodeId>` — [`NodeError`] is public API and must never leak
/// openraft's `BasicNode`. Matched structurally, like [`map_init_err`].
fn map_write_err(e: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> NodeError {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => NodeError::NotLeader {
            leader: forward.leader_id,
        },
        other => NodeError::Write(other.to_string()),
    }
}

impl Drop for RaftNode {
    /// Best-effort teardown, deliberately asymmetric with [`RaftNode::shutdown`]:
    /// only the listener is released here. The Raft core stops and drops its
    /// `Arc<redb::Database>` clones asynchronously, a few scheduler ticks later,
    /// so — unlike shutdown-then-drop — a plain drop gives NO guarantee the redb
    /// file lock is free when this returns (#54). Drop cannot await, and a
    /// blocking wait here would stall the very runtime the core needs to finish
    /// tearing down, so callers that will reopen the same data directory must
    /// call [`RaftNode::shutdown`] first. A drop without one is a bug worth a
    /// log line, not a panic (Drop can run mid-unwind).
    fn drop(&mut self) {
        self.server_task.abort();
        if !self.shutdown_invoked.load(Ordering::Relaxed) {
            tracing::warn!(
                node_id = self.id,
                db_refs = self.sm_reader.db_refs(),
                "RaftNode dropped without shutdown(): redb storage releases \
                 asynchronously, so an immediate restart on this data directory \
                 may race the file lock — call shutdown() before dropping"
            );
        }
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

    /// The storage-release contract shutdown promises: when it returns, openraft
    /// has dropped every database handle it held, leaving only the node's own
    /// `sm_reader`. On the old shutdown, which returned before the core wound
    /// down, this count could still be above one until the core caught up — the
    /// window the #41 restart raced. Here it is the guaranteed postcondition.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_waits_for_storage_release() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("cluster init");
        node.shutdown().await.expect("shutdown");

        assert_eq!(
            node.storage_refs(),
            NODE_HELD_DB_REFS,
            "shutdown must not return until openraft has released its storage clones"
        );
    }

    /// The wait is bounded: if a storage handle is never released (here forced by
    /// holding an extra clone for the whole shutdown), shutdown returns a typed
    /// error within the timeout rather than hanging forever. This also
    /// deterministically gates the wait's *existence* — with no wait, shutdown
    /// returns `Ok` and this fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_times_out_if_storage_is_never_released() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("cluster init");

        // Stand in for an openraft clone that never drops, so the release wait can
        // never reach NODE_HELD_DB_REFS.
        let pin = node.clone_storage_handle();
        let started = tokio::time::Instant::now();
        let err = node.shutdown().await;
        assert!(
            matches!(&err, Err(NodeError::Runtime(m)) if m.contains("release storage")),
            "a pinned storage handle must make shutdown time out, got {err:?}"
        );
        assert!(
            started.elapsed() < STORAGE_RELEASE_TIMEOUT * 3,
            "shutdown must return near the release timeout, not hang"
        );
        // #54: shutdown WAS invoked — its failure already reached the caller,
        // so the drop-without-shutdown tripwire must stay silent here.
        let (messages, capture) = WarnCapture::new();
        tracing::subscriber::with_default(capture, || drop(node));
        assert!(
            messages.lock().expect("lock").is_empty(),
            "a failed-but-invoked shutdown must not warn on drop: {:?}",
            messages.lock().expect("lock")
        );
        drop(pin);
    }

    /// The end-to-end guarantee: because shutdown waits for the lock to be
    /// releasable, dropping the node and restarting immediately on the same
    /// directory succeeds every time — no retry-on-lock-contention needed. The
    /// old shutdown made this intermittently fail with "Database already open".
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn immediate_restart_after_shutdown_never_races_the_lock() {
        let dir = TempDir::new().expect("tempdir");
        {
            let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
            node.cluster_init().await.expect("cluster init");
            node.shutdown().await.expect("shutdown");
        }
        for attempt in 0..20 {
            let node = RaftNode::start(config_in(&dir, 1))
                .await
                .unwrap_or_else(|e| panic!("restart {attempt} raced the redb lock: {e}"));
            node.shutdown().await.expect("shutdown");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_twice_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("first init");
        let second = node.cluster_init().await;
        assert!(
            matches!(second, Err(NodeError::AlreadyInitialized)),
            "second cluster-init must be refused as AlreadyInitialized, got {second:?}"
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
        n2.join_via(n1.advertise()).await.expect("n2 join");
        n3.join_via(n1.advertise()).await.expect("n3 join");

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

    /// Issue #72: a sole voter declines to leave, and says so.
    ///
    /// openraft refuses a membership change that would empty the voter set, so
    /// there is nothing to hand this node's votes to and it stays a full member.
    /// Reporting that as a departure is what would let a caller record "this
    /// node left" about a node that did not — and then refuse its next start.
    #[tokio::test]
    async fn a_sole_voter_declines_to_leave_rather_than_reporting_a_departure() {
        let dir = TempDir::new().unwrap();
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("init");

        assert_eq!(
            node.leave(Duration::from_secs(1))
                .await
                .expect("leave resolves"),
            LeaveOutcome::Retained,
            "a sole voter cannot leave, so it must not report a departure"
        );
        assert!(
            node.in_membership(),
            "and it is still a member afterwards, so its next start must resume"
        );

        node.shutdown().await.expect("shutdown");
    }

    /// Issue #72: a node evicted **while it was down** comes back believing it
    /// is still a member, and it can name the peers that can readmit it.
    ///
    /// This is the precondition the whole reconciler rejoin fallback rests on.
    /// Nothing at startup can tell such a node it is out — there is no
    /// departure marker (it never left) and its own log never received the
    /// entry that removed it, so `in_membership()` is stale-true. If either half
    /// of that stopped holding, the fallback would be either unnecessary or
    /// useless, and the node would sit at `/readyz` 503 forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_node_evicted_while_down_returns_stale_in_membership_and_knows_its_peers() {
        let (d1, d2, d3) = (
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        );
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
        let n3 = RaftNode::start(config_in(&d3, 3)).await.expect("start n3");
        n2.join_via(n1.advertise()).await.expect("n2 join");
        n3.join_via(n1.advertise()).await.expect("n3 join");

        let all = BTreeSet::from([1, 2, 3]);
        assert_eq!(wait_voters(&n1, &all).await, all, "three voters to start");

        // n3 goes away first, so it can never observe what happens next. It is
        // dropped as well as shut down: the redb lock outlives `shutdown` until
        // the node itself is gone, and the restart below reopens the same store.
        n3.shutdown().await.expect("shutdown n3");
        drop(n3);

        // Three voters, so the floor permits this removal (#69).
        crate::raft::network::evict(&n1.raft, &n1.membership_gate, 3)
            .await
            .expect("evict the node that is down");
        let survivors = BTreeSet::from([1, 2]);
        assert_eq!(
            wait_voters(&n1, &survivors).await,
            survivors,
            "the eviction must land while n3 is down"
        );

        // Back on its retained directory: its log stopped before the removal.
        let returned = RaftNode::start(config_in(&d3, 3))
            .await
            .expect("restart n3");
        assert!(
            returned
                .await_membership_loaded(Duration::from_secs(5))
                .await,
            "the durable membership must surface after a restart, or every startup decision that \
             reads it is guessing"
        );
        assert!(
            returned.in_membership(),
            "a node evicted while down cannot know it is out — this stale-true reading is \
             precisely why the departure marker alone is not enough"
        );
        let peers = returned.known_peers();
        assert_eq!(
            peers.len(),
            2,
            "it must still name the peers that can readmit it, got {peers:?}"
        );
        assert!(
            peers.contains(&n1.advertise().to_string()),
            "the surviving leader must be among them, got {peers:?}"
        );

        for node in [&n1, &n2, &returned] {
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
        n2.join_via(n1.advertise()).await.expect("n2 join");
        n3.join_via(n1.advertise()).await.expect("n3 join");
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
        n2.join_via(n1.advertise()).await.expect("n2 join");

        // A write submitted to the follower must not silently succeed: openraft
        // refuses it (forward-to-leader), surfaced as a typed NotLeader error
        // carrying the leader hint, not a generic Write string.
        let err = n2.put_imposter(imposter(8080, "on-follower")).await;
        assert!(
            matches!(err, Err(NodeError::NotLeader { leader: Some(1) })),
            "follower write must be rejected as NotLeader with a leader hint, got {err:?}"
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
        n2.join_via(n1.advertise()).await.expect("n2 join");
        n3.join_via(n1.advertise()).await.expect("n3 join");

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

        let seed = n1.advertise();
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

            let seed = n1.advertise();
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
    /// #55 gate: with a ceiling of 2 and two concurrent admissions against a
    /// one-voter cluster, exactly one may promote — the committed voter set
    /// must never exceed the ceiling, and the loser must still join as a
    /// learner. Pre-fix, both admissions read the same pre-promotion count and
    /// both promote. Repeated like the #38 gate: one pass can get lucky.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_admissions_never_exceed_the_ceiling() {
        use crate::raft::network;

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

            let gate = tokio::sync::Mutex::new(());
            let (r2, r3) = tokio::join!(
                network::admit(&n1.raft, &gate, 2, n2.advertise().to_string(), 2),
                network::admit(&n1.raft, &gate, 3, n3.advertise().to_string(), 2),
            );
            r2.unwrap_or_else(|e| {
                panic!("round {round}: losing the promotion race must not fail the join: {e}")
            });
            r3.unwrap_or_else(|e| {
                panic!("round {round}: losing the promotion race must not fail the join: {e}")
            });

            let voters = n1.status().voters;
            assert_eq!(
                voters.len(),
                2,
                "round {round}: auto-promotion exceeded the ceiling: {voters:?}"
            );
            assert!(
                voters.contains(&1),
                "round {round}: the founder stays a voter"
            );
            let promoted: Vec<NodeId> = [2, 3]
                .into_iter()
                .filter(|id| voters.contains(id))
                .collect();
            assert_eq!(
                promoted.len(),
                1,
                "round {round}: exactly one joiner wins the promotion slot, got {promoted:?}"
            );

            let members: BTreeSet<NodeId> = n1
                .raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .map(|(id, _)| *id)
                .collect();
            assert!(
                members.contains(&2) && members.contains(&3),
                "round {round}: the ceiling loser must remain a learner, got {members:?}"
            );

            n1.shutdown().await.ok();
            n2.shutdown().await.ok();
            n3.shutdown().await.ok();
        }
    }

    /// Reserve a currently-free localhost port and release it, so a config can
    /// name a fixed port before anything binds it.
    fn reserved_port() -> u16 {
        let held = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a free port");
        held.local_addr().expect("read reserved port").port()
    }

    /// A free port on whatever address `localhost` actually resolves to.
    ///
    /// A node that advertises `localhost:<port>` must *bind* what that name
    /// resolves to, or the two disagree and nothing can reach it. Which family
    /// wins is host-dependent — RFC 6724 ordering puts `::1` first on many
    /// systems — so this asks rather than assumes, and reserves the port on the
    /// same address it returns. Reserving on `127.0.0.1` and then binding
    /// `[::1]` would prove nothing about the port that actually gets used.
    fn localhost_bind() -> SocketAddr {
        use std::net::ToSocketAddrs;
        let resolved = ("localhost", 0)
            .to_socket_addrs()
            .expect("localhost resolves")
            .next()
            .expect("localhost resolves to at least one address");
        let held = std::net::TcpListener::bind(SocketAddr::new(resolved.ip(), 0))
            .expect("reserve a free port on the address localhost resolves to");
        held.local_addr().expect("read reserved port")
    }

    /// Issue #68: a hostname advertise reaches membership and is resolved on
    /// every send.
    ///
    /// This is the gate the whole issue rests on. Before it, `--cluster-advertise`
    /// was typed `SocketAddr`, so no name could ever enter membership and the
    /// per-send re-resolution added by #6 had nothing to re-resolve. Two things
    /// have to hold: the membership must store the **name verbatim** (storing a
    /// resolved address would pin the peer to whatever DNS said once, which is
    /// the bug this prevents), and replication must still reach that peer —
    /// which it can only do by resolving the name per send.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hostname_advertise_round_trips_membership_and_replicates() {
        let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");

        // A fixed port on whatever `localhost` resolves to, so the advertised
        // name and the bound address cannot disagree.
        let bind = localhost_bind();
        let port = bind.port();
        let mut config = config_in(&d2, 2);
        config.bind = bind;
        config.advertise = Some(
            format!("localhost:{port}")
                .parse::<Authority>()
                .expect("hostname authority"),
        );
        let n2 = RaftNode::start(config).await.expect("start n2");

        n2.join_via(n1.advertise()).await.expect("n2 join");
        let want = BTreeSet::from([1, 2]);
        assert_eq!(wait_voters(&n1, &want).await, want, "both must be voters");

        let stored = {
            let receiver = n1.raft.metrics();
            let metrics = receiver.borrow();
            metrics
                .membership_config
                .nodes()
                .find(|(id, _)| **id == 2)
                .map(|(_, node)| node.addr.clone())
                .expect("n2 is in the membership")
        };
        assert_eq!(
            stored,
            format!("localhost:{port}"),
            "membership must keep the advertised name verbatim, not a resolved address"
        );

        n1.put_imposter(imposter(8080, "via-hostname"))
            .await
            .expect("leader write");
        assert!(
            wait_config(&n2, 8080, "via-hostname").await,
            "replication must reach a peer whose membership address is a hostname"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
    }

    /// A two-node cluster whose **leader** advertises a name that does not
    /// resolve, while binding a real port.
    ///
    /// The follower joins through the bound address, so the cluster forms — but
    /// the leader's membership entry carries the unresolvable name, which is
    /// exactly what a stale or misconfigured DNS record leaves behind. Every
    /// path on the follower that has to dial the leader then has to cope with a
    /// resolution failure. The returned `TempDir`s must be held for the lifetime
    /// of the nodes.
    async fn cluster_with_unresolvable_leader() -> (RaftNode, RaftNode, (TempDir, TempDir)) {
        let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let port = reserved_port();
        let mut c1 = config_in(&d1, 1);
        c1.bind = format!("127.0.0.1:{port}").parse().expect("bind addr");
        c1.advertise = Some(
            "no-such-host.invalid:4790"
                .parse::<Authority>()
                .expect("authority parses even though it will not resolve"),
        );
        let n1 = RaftNode::start(c1).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");

        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
        let bound = format!("127.0.0.1:{port}")
            .parse::<Authority>()
            .expect("bound authority");
        n2.join_via(&bound)
            .await
            .expect("join through the bound address");
        (n1, n2, (d1, d2))
    }

    /// Issue #68: a leader hint that cannot be resolved is reported, not
    /// panicked on and not silently treated as success.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn submit_reports_unavailable_when_leader_hint_cannot_resolve() {
        let (n1, n2, _dirs) = cluster_with_unresolvable_leader().await;

        let err = n2
            .submit(ControlRequest {
                op_id: Uuid::new_v4(),
                principal: None,
                issued_at_secs: 0,
                expected_revision: None,
                op: ControlOp::PutImposter {
                    tenant: TenantId::default(),
                    config: Box::new(imposter(8080, "never-lands")),
                },
            })
            .await
            .expect_err("a write cannot be forwarded to a leader that does not resolve");
        assert!(
            matches!(err, NodeError::Unavailable(_)),
            "an unresolvable leader hint must surface as Unavailable, got {err:?}"
        );
        assert!(
            format!("{err}").contains("no-such-host.invalid"),
            "the error must name the authority that failed to resolve: {err}"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
    }

    /// Issue #68: the write barrier must report a member it cannot resolve as
    /// **unapplied**, never as confirmed.
    ///
    /// This is the most dangerous of the resolve-failure paths. `await_applied`
    /// backs the read-after-write guarantee, so counting an unreachable member
    /// as confirmed would have the barrier claim a durability it never
    /// established — wrong and quiet, which the project's error rules single out
    /// as worse than failing loudly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn await_applied_reports_a_member_that_does_not_resolve_as_unapplied() {
        let (n1, n2, _dirs) = cluster_with_unresolvable_leader().await;

        let revision = n1
            .put_imposter(imposter(8080, "barrier"))
            .await
            .expect("leader write")
            .revision;

        // From n2's side the only other member is n1, whose advertised name
        // does not resolve, so it can never be confirmed.
        let unapplied = n2.await_applied(revision, Duration::from_millis(500)).await;
        assert!(
            unapplied.contains(&1),
            "an unresolvable member must be reported unapplied, got {unapplied:?}"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
    }

    /// Issue #68: the readiness gate reports "not yet" when the leader's
    /// address does not resolve — it must not report a catch-up target it
    /// never actually read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_applied_is_none_when_the_leader_address_does_not_resolve() {
        let (n1, n2, _dirs) = cluster_with_unresolvable_leader().await;

        assert_eq!(
            n2.leader_applied().await,
            None,
            "an unresolvable leader address must read as not-yet, not as a target"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
    }

    /// Issue #68: a seed whose name does not resolve fails the join with a
    /// membership error, rather than surfacing as a generic RPC failure that
    /// reads like "the seed is not up yet".
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn join_via_reports_membership_error_when_the_seed_does_not_resolve() {
        let dir = TempDir::new().unwrap();
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");

        let seed = "no-such-host.invalid:4790"
            .parse::<Authority>()
            .expect("authority parses even though it will not resolve");
        let err = node
            .join_via(&seed)
            .await
            .expect_err("an unresolvable seed cannot be joined");

        assert!(
            matches!(err, NodeError::Membership(_)),
            "an unresolvable seed must be a membership error, got {err:?}"
        );
        assert!(
            format!("{err}").contains("no-such-host.invalid"),
            "the error must name the seed that failed to resolve: {err}"
        );

        node.shutdown().await.ok();
    }

    /// Issue #71: a repeated eviction is a cheap no-op, not a second membership
    /// change.
    ///
    /// The leave RPC handler re-runs `evict` on every retried leave, and
    /// `leave_inner` retries the whole sequence from whichever node leads now,
    /// so a second call against an already-departed node is the normal case —
    /// not an edge one. It must return promptly without submitting anything;
    /// a version that re-submitted would put a membership change on the log for
    /// every retry a flaky network produced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn evict_twice_is_a_cheap_no_op() {
        use crate::raft::network::{self, EvictOutcome};

        let (d1, d2, d3) = (
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        );
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
        let n3 = RaftNode::start(config_in(&d3, 3)).await.expect("start n3");
        n2.join_via(n1.advertise()).await.expect("n2 join");
        n3.join_via(n1.advertise()).await.expect("n3 join");
        let all = BTreeSet::from([1, 2, 3]);
        assert_eq!(wait_voters(&n1, &all).await, all, "three voters to start");

        assert_eq!(
            network::evict(&n1.raft, &n1.membership_gate, 3)
                .await
                .expect("first eviction"),
            EvictOutcome::Removed
        );
        let survivors = BTreeSet::from([1, 2]);
        assert_eq!(
            wait_voters(&n1, &survivors).await,
            survivors,
            "the first eviction must land"
        );
        let membership_after_first = n1.raft.metrics().borrow().membership_config.clone();

        // Bounded: a second eviction that tried to submit another membership
        // change would block on the commit barrier rather than return.
        let second = tokio::time::timeout(
            Duration::from_secs(1),
            network::evict(&n1.raft, &n1.membership_gate, 3),
        )
        .await
        .expect("a repeated eviction must return promptly, not submit and wait")
        .expect("a repeated eviction is not an error");
        assert_eq!(second, EvictOutcome::Removed, "still gone is still removed");

        assert_eq!(
            n1.raft
                .metrics()
                .borrow()
                .membership_config
                .log_id()
                .map(|l| l.index),
            membership_after_first.log_id().map(|l| l.index),
            "the second eviction must not append another membership entry"
        );

        for node in [&n1, &n2, &n3] {
            node.shutdown().await.ok();
        }
    }

    /// Issue #71: a half-finished departure is completed by whoever leads next.
    ///
    /// `evict` is demote-then-remove, two committed entries. If leadership moves
    /// between them the departing node is a learner that is still a member, and
    /// `leave_inner` retries against the new leader. That retry only works
    /// because both halves no-op when already done — this is the test for that
    /// property, and nothing else exercises a leadership change mid-departure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn evict_completes_from_a_new_leader_after_partial_departure() {
        use crate::raft::network::{self, EvictOutcome};

        let (d1, d2, d3) = (
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        );
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
        let n3 = RaftNode::start(config_in(&d3, 3)).await.expect("start n3");
        n2.join_via(n1.advertise()).await.expect("n2 join");
        n3.join_via(n1.advertise()).await.expect("n3 join");
        let all = BTreeSet::from([1, 2, 3]);
        assert_eq!(wait_voters(&n1, &all).await, all, "three voters to start");

        // Half of the departure only: n3 is demoted but still a member.
        network::demote_voter(&n1.raft, 3)
            .await
            .expect("demote the departing node");
        let survivors = BTreeSet::from([1, 2]);
        assert_eq!(
            wait_voters(&n1, &survivors).await,
            survivors,
            "the demote must land"
        );
        assert!(
            n1.raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .any(|(id, _)| *id == 3),
            "n3 must still be a member — this is the half-finished state"
        );

        // Move leadership without losing quorum. Killing n1 would leave one of
        // two voters, which cannot elect.
        //
        // Retried, not triggered once: `elect` only *starts* a campaign, and a
        // campaign can lose — n1 is still a healthy leader, and on a loaded
        // runner n2's timers slip far enough that a single nudge decides
        // nothing. Asserting on one trigger made this test flaky in CI (it went
        // red on an unrelated PR), which is worse than not having it.
        let took_over = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                if n2.status().is_leader {
                    break true;
                }
                if tokio::time::Instant::now() >= deadline {
                    break false;
                }
                n2.raft
                    .trigger()
                    .elect()
                    .await
                    .expect("trigger an election");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        };
        assert!(took_over, "n2 must take over before the retry");

        // Pin the no-op guard directly, before the eviction below. Every other
        // assertion in this test passes without it: an unguarded `demote_voter`
        // against an already-demoted node submits a legal membership entry that
        // commits fine, the removal still lands, and the final state is
        // identical. The only visible difference is the entry that should never
        // have been written — so that is what gets asserted.
        let membership_index = |node: &RaftNode| {
            node.raft
                .metrics()
                .borrow()
                .membership_config
                .log_id()
                .map(|l| l.index)
        };
        let before_redundant_demote = membership_index(&n2);
        network::demote_voter(&n2.raft, 3)
            .await
            .expect("demoting an already-demoted node is not an error");
        assert_eq!(
            membership_index(&n2),
            before_redundant_demote,
            "a redundant demote must append no membership entry — without that guard, every \
             retried departure writes to the log"
        );

        assert_eq!(
            network::evict(&n2.raft, &n2.membership_gate, 3)
                .await
                .expect("the new leader finishes the departure"),
            EvictOutcome::Removed,
            "the demote half must no-op so the removal half can complete"
        );

        for node in [&n1, &n2] {
            assert!(
                wait_until(|| !node
                    .raft
                    .metrics()
                    .borrow()
                    .membership_config
                    .nodes()
                    .any(|(id, _)| *id == 3))
                .await,
                "node {} still lists the departed node",
                node.id()
            );
        }

        // The surviving quorum is live, not merely consistent.
        n2.put_imposter(imposter(8080, "after-churn"))
            .await
            .expect("the new leader must still commit");

        for node in [&n1, &n2, &n3] {
            node.shutdown().await.ok();
        }
    }

    /// Issue #69: the voter floor guards voters, not members.
    ///
    /// Removing a learner cannot cost the cluster a quorum member, so it is
    /// never refused — even when the voter set is already at or below the
    /// floor, as it is here. A floor that counted learners would strand every
    /// ceiling-capped node permanently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn evicting_a_learner_is_never_held_by_the_floor() {
        use crate::raft::network::{self, EvictOutcome};

        let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");

        let gate = tokio::sync::Mutex::new(());
        // A ceiling of 1 keeps n2 a learner, leaving n1 the only voter — which
        // is already below the floor, so a floor that ignored the voter/learner
        // distinction would refuse this.
        network::admit(&n1.raft, &gate, 2, n2.advertise().to_string(), 1)
            .await
            .expect("admit as learner");
        assert_eq!(n1.status().voters, vec![1], "n2 must be a learner");

        assert_eq!(
            network::evict(&n1.raft, &gate, 2)
                .await
                .expect("evicting a learner must succeed"),
            EvictOutcome::Removed,
            "a learner's removal costs no quorum, so the floor must not refuse it"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
    }

    /// #55 gate: a joiner admitted at the ceiling is a functioning replica —
    /// the admission succeeds, the voter set is unchanged, and replicated
    /// config still reaches it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn admission_at_ceiling_still_admits_learner() {
        use crate::raft::network;

        let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");

        let gate = tokio::sync::Mutex::new(());
        network::admit(&n1.raft, &gate, 2, n2.advertise().to_string(), 1)
            .await
            .expect("a ceiling-capped admission must still succeed as learner");

        assert_eq!(n1.status().voters, vec![1], "the ceiling holds at 1 voter");

        // A retried join (the joiner timed out and re-sent) is idempotent:
        // still Ok, still learner-only.
        network::admit(&n1.raft, &gate, 2, n2.advertise().to_string(), 1)
            .await
            .expect("a retried ceiling-capped admission must stay idempotent");
        assert_eq!(n1.status().voters, vec![1], "the retry must not promote");

        n1.put_imposter(imposter(8080, "ceiling-learner"))
            .await
            .expect("leader write");
        assert!(
            wait_config(&n2, 8080, "ceiling-learner").await,
            "a ceiling-capped learner must still replicate config"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
    }
    /// #54: WARN-level messages recorded so drop-path tripwires are assertable.
    struct WarnCapture {
        messages: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl WarnCapture {
        fn new() -> (std::sync::Arc<std::sync::Mutex<Vec<String>>>, Self) {
            let messages = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let capture = Self {
                messages: std::sync::Arc::clone(&messages),
            };
            (messages, capture)
        }
    }

    impl tracing::Subscriber for WarnCapture {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() == tracing::Level::WARN
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct MessageVisitor<'a>(&'a mut String);
            impl tracing::field::Visit for MessageVisitor<'_> {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        use std::fmt::Write;
                        let _ = write!(self.0, "{value:?}");
                    }
                }
            }
            let mut message = String::new();
            event.record(&mut MessageVisitor(&mut message));
            self.messages.lock().expect("capture lock").push(message);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// #54 gate: the guarantee a plain drop DOES provide — openraft's storage
    /// clones are released eventually — locked in so it cannot regress into a
    /// leak. Eventual, not synchronous: only shutdown-then-drop is prompt.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drop_without_shutdown_eventually_releases_storage() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("cluster init");

        let pin = node.clone_storage_handle();
        drop(node);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        // > 1: the pin itself is the one handle that legitimately remains —
        // deliberately not NODE_HELD_DB_REFS, whose 1 is the dropped node's own
        // sm_reader.
        while pin.db_refs() > 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "openraft never released storage after a plain drop"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// #54 gate: dropping a node that was never shut down trips the warn — the
    /// one log line that turns the #41 redb-lock flake into a diagnosis.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drop_without_shutdown_warns() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("cluster init");

        let (messages, capture) = WarnCapture::new();
        tracing::subscriber::with_default(capture, || drop(node));
        let messages = messages.lock().expect("lock");
        assert_eq!(messages.len(), 1, "exactly one warn: {messages:?}");
        assert!(
            messages[0].contains("dropped without shutdown"),
            "{messages:?}"
        );
    }

    /// #54 gate: the shutdown-then-drop contract stays silent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drop_after_shutdown_does_not_warn() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("cluster init");
        node.shutdown().await.expect("shutdown");

        let (messages, capture) = WarnCapture::new();
        tracing::subscriber::with_default(capture, || drop(node));
        assert!(
            messages.lock().expect("lock").is_empty(),
            "a shut-down node must drop silently: {:?}",
            messages.lock().expect("lock")
        );
    }
}
