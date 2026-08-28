//! The Raft control-plane node: an [`openraft::Raft`] wired to the redb-backed
//! storage and the #8 cluster transport, with a solo bootstrap
//! (`cluster_init`), a seed-join path, and a [`StatusReport`] read from Raft's
//! own metrics.
//!
//! This is decision D-15 (ADR-001): membership, imposter configs, the `enabled`
//! bit, tenancy/RBAC records and admin intents share one embedded Raft log, so
//! at any log index every node computes byte-identical membership and
//! therefore byte-identical ownership ([`Ring`]). Flow state stays off it
//! (D-17). Membership changes enter that log only through [`RaftNode::join_via`]
//! and [`RaftNode::leave`] (D-21).
//!
//! Startup has a deliberate ordering. The cluster server must bind before the
//! node knows the address it advertises, but the server's request handlers need
//! the node's [`Raft`]. The `Raft` is therefore built *after* the server binds,
//! and installed into a shared [`OnceCell`] the handlers read — so a peer RPC
//! that arrives in the sliver before installation gets a retryable "not ready"
//! rather than reaching a half-built node.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arc_swap::ArcSwap;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::{Request, Response};
use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::metrics::WaitError;
use openraft::{BasicNode, Config, Raft, ServerState, SnapshotPolicy};
use rift_cluster_base::seams::{CompiledRoutes, ImposterConfig, ImposterManager, RouteTable};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::network::{
    self, AdmittedRole, CLUSTER_APPLIED_PATH, CLUSTER_JOIN_PATH, CLUSTER_LEAVE_PATH,
    CLUSTER_WRITE_PATH, JoinAccepted, JoinRequest, LeaveRequest, RaftSlot, RpcNetwork, WriteReply,
};
use super::ring::Ring;
use super::store::{
    self, DatasetSummary, RedbStateMachine, SourceRecord, SourceRow, SpecBinding, SpecRecord,
};
use super::{NodeId, TypeConfig};
use crate::control::{
    AuditRow, AuditSink, ControlOp, ControlRequest, ControlResponse, Principal, Role, SessionKey,
    SourceProvenance, Tenant, TenantConfigUsage, TenantId,
};
use crate::rpc::{
    Authority, DnsResolver, PeerResolver, Router, RpcClient, RpcClientConfig, RpcError, RpcServer,
    RpcServerConfig, Signer, TrackedPeerHealth, Verifier,
};
use crate::stores::journal::ClusterJournal;

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
///
/// A *running* node holds a second clone: the blob route's `Arc<dyn BlobFallback>`
/// (#486, D-51). That this constant does not count it is therefore an ordering
/// guarantee, not an accident — [`RaftNode::shutdown`] aborts `server_task` and
/// waits for it to finish *before* calling `await_storage_release`, which is what
/// drops the router and with it that clone. Do not reorder those two steps.
///
/// One consequence worth knowing: a fallback read is served inside
/// `spawn_blocking`, and blocking tasks cannot be aborted. A peer-initiated read
/// in flight when shutdown begins therefore holds a database handle that nothing
/// can cancel, and delays the release by however long that read takes. Bounded in
/// practice — the reads are of a spec- or dataset-sized value — but it is the
/// first time peer traffic can push on [`STORAGE_RELEASE_TIMEOUT`].
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

/// The isolated-owner rule itself (RFC-001 §7.2), as a pure function of the three Raft metrics it
/// reads — so the safety gate and the metric that reports it cannot disagree.
///
/// It **fails closed**: every uncertain state is isolated.
///
/// - **No known leader** → isolated. A follower partitioned away loses its leader once the
///   election timeout elapses.
/// - **This node is the leader, with no quorum ack inside the window** → isolated. openraft reports
///   `millis_since_quorum_ack == None` for a leader no quorum has acknowledged (a just-elected
///   leader before its first `AppendEntries` round, or one partitioned from its followers), and
///   `None` must read as isolated rather than healthy — that `is_none_or` is the fail-closed half.
/// - **Someone else leads** → not isolated. Hearing another node's leadership *is* the evidence of
///   contact with the quorum.
///
/// Extracted from [`RaftNode::is_isolated`] by #470: the condition became observable
/// (`rift_cluster_isolated`) at the same time it was already load-bearing for D-17 and D-40, and
/// two readings of one safety rule is one more than a rule can safely have.
#[must_use]
fn isolated_from(
    me: NodeId,
    current_leader: Option<NodeId>,
    millis_since_quorum_ack: Option<u64>,
) -> bool {
    match current_leader {
        None => true,
        Some(leader) if leader == me => {
            millis_since_quorum_ack.is_none_or(|ms| ms > ISOLATION_WINDOW_MS)
        }
        Some(_) => false,
    }
}

/// Grace window #437's blob store GC gives an unreferenced blob before reclaiming it —
/// long enough that a #438 fan-out has time to land the spec/dataset row that will
/// reference a freshly delivered blob before it looks abandoned.
const BLOB_GC_GRACE_SECS: u64 = 3600;

/// How often the blob store's GC sweep runs.
const BLOB_GC_INTERVAL: Duration = Duration::from_secs(60);

/// How long one GC sweep waits on each member's applied-index answer (D-55). Bounded so a hung
/// member costs a sweep one budget, not the RPC client's full retry schedule per address; small
/// against [`BLOB_GC_INTERVAL`], generous against a loopback or in-region round trip. A member
/// that misses it is simply unknown for that sweep — retained, and named in the warning.
const BLOB_GC_FLOOR_BUDGET: Duration = Duration::from_secs(2);

/// How long [`RaftNode::leave`] waits between polls while chasing a leader
/// hint that is moving (a membership entry just committed but the new leader
/// has not settled, or metrics have not caught up yet). Small relative to typical
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
    /// How long audit rows are kept, in seconds; `0` = forever (issue #163).
    ///
    /// **Must be identical on every node of a fleet.** It feeds the retention
    /// GC that runs inside `apply`, so two nodes configured differently would
    /// drop different rows from the same log and their audit tables would
    /// permanently diverge — the one thing the replicated clock exists to
    /// prevent. Node configuration rather than replicated state because it is
    /// an operator's storage budget, not a tenant's policy.
    pub audit_retention_secs: u64,
    /// Snapshot aggressively and purge immediately, so a lagging node must be caught up by a real
    /// `install_snapshot` over the wire rather than by log replication (issue #183).
    ///
    /// **A testability knob, not an operator tuning parameter.** `None` — the default, and the only
    /// value any shipped configuration produces — leaves openraft's defaults untouched. `Some(n)`
    /// sets `snapshot_policy` to `LogsSinceLast(n)` **and** `max_in_snapshot_log_to_keep` to `0`.
    ///
    /// Both, because either alone does nothing. A low snapshot policy makes snapshots get *built*;
    /// it does not make a follower need one. openraft sends a snapshot only when the entries a
    /// follower is missing have been **purged**, and purging is governed by
    /// `max_in_snapshot_log_to_keep` (default 1000) — so with a low policy and the default keep, a
    /// node that missed a few dozen entries still catches up by ordinary replication and the wire
    /// path stays unexercised. That is exactly the trap this knob exists to remove: three chaos
    /// scenarios independently discovered it and each wrote the same correction into the README.
    pub snapshot_log_entries: Option<u64>,
    /// Make this node's blob `?stat` route advertise `applies_digest_only: false`, regardless of
    /// what this build can actually apply (#481).
    ///
    /// **A testability knob, not an operator tuning parameter**, in the same style as
    /// [`Self::snapshot_log_entries`]: `false` — the default, and the only value any shipped
    /// configuration produces — lets the route answer honestly. `true` makes this node's build
    /// pretend to be a pre-#481 one that cannot apply a digest-only `ControlOp`
    /// (`DatasetPut`/`SpecPut` with `csv`/`document: Option<String>` unset), which is the only
    /// way an in-process test can exercise `RaftNode::fan_out_blob`'s capability gate (D-53)
    /// without actually standing up two binary versions side by side.
    pub advertise_as_digest_only_incapable: bool,
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
    /// Opening or using this node's local storage failed — the redb-backed
    /// Raft store, or the node-local blob transfer store (#437).
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
    /// The **learner** ids in the currently effective membership — members that replicate and
    /// serve the data plane in full but hold no vote.
    ///
    /// Reported separately rather than folded into [`voters`](Self::voters) because the two answer
    /// different questions, and because a fan-out that enumerates only voters is silently
    /// incomplete on any fleet past the auto-voter ceiling: nodes beyond it stay learners
    /// indefinitely, still binding listeners and still taking traffic. Anything proving a negative
    /// about "the fleet" has to know they exist.
    pub learners: Vec<NodeId>,
    /// Whether this node was isolated from the quorum at the instant this report was taken —
    /// the isolated-owner rule (RFC-001 §7.2), the same condition [`RaftNode::is_isolated`]
    /// enforces, via the same [`isolated_from`].
    ///
    /// Carried on the report rather than left to a second `is_isolated()` call so that the gauge
    /// (`rift_cluster_isolated`, #470) and the `/_cluster/status` field describe *one* sample. Two
    /// calls a few microseconds apart can straddle an election and disagree, which on a
    /// safety-critical condition is the one thing an operator must not be shown.
    pub isolated: bool,
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

/// The outcome of a successful [`RaftNode::join_via`] (#433): this node **is
/// a member** — that is what "successful" means, and it is all that startup
/// needs. What it is a member *as*, and whether it is still catching up, are
/// carried for logging and for the operator; neither is a reason to fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinOutcome {
    /// What the leader admitted this node as.
    pub role: JoinedAs,
    /// The leader's estimate at admission time. `true` means the leader's
    /// promotion sweep will make this node a voter once it is current;
    /// nothing on this node needs to wait for that.
    pub catching_up: bool,
}

/// What a join made this node, as reported by the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinedAs {
    /// Admitted as a learner; promotion is the leader's job (#433).
    Learner,
    /// A voter — promoted at admission, or already one (a restart).
    Voter,
    /// An older leader that predates #433 answered: it admitted this node the
    /// blocking way, so it is a member and current — it just cannot say which
    /// role in its reply.
    Unknown,
}

/// What one [`RaftNode::fan_out_blob`] achieved (#438).
///
/// Carries the evidence rather than a verdict alone: `quorum` is what the write
/// path branches on, but a fan-out that silently sent nothing and one that moved
/// 8 MiB are indistinguishable from the verdict, and only `bytes_sent` tells
/// them apart.
#[derive(Debug, Clone)]
pub struct FanOutOutcome {
    /// Members that hold the digest, including this node.
    pub acks: BTreeSet<NodeId>,
    /// Peers whose build cannot serve blobs at all. Held separately because
    /// they are *ambiguous*, not negative: they never answered the question.
    pub skewed: BTreeSet<NodeId>,
    /// Bytes this fan-out put on the wire, summed across peers. Zero is
    /// legitimate — every peer may already have held the digest.
    pub bytes_sent: u64,
    /// Whether a majority of **both** the committed and the effective
    /// configuration acked (joint consensus).
    pub quorum: bool,
    /// Whether the two configurations differed, i.e. a membership change was in
    /// flight and the second majority did real work.
    pub joint: bool,
    /// Whether every member of the committed ∪ effective configuration — voters *and*
    /// learners, since a learner applies the log too (#481) — has been confirmed able to apply
    /// a digest-only `ControlOp`, with nobody [`Self::skewed`]. This, not [`Self::quorum`], is
    /// what `fan_out_then_submit` strips the op's bytes on: quorum durability of the *blob*
    /// says nothing about whether a member outside the byte quorum can *decode* a reference to
    /// it. See [`sideload_safe`] for the decision rule.
    pub sideload_safe: bool,
    /// Members [`Self::sideload_safe`] found not confirmed capable, and *why* not, for
    /// diagnosis and the deferred-write metric (#481): `sideload_incapable` were confirmed —
    /// this round or a past one — unable to apply a digest-only entry (an explicit `false`
    /// answer, or [`Self::skewed`]); `sideload_unobserved` have simply never answered the
    /// question (unreachable this round, or never probed). Both are empty when
    /// `sideload_safe` is true.
    pub sideload_incapable: BTreeSet<NodeId>,
    pub sideload_unobserved: BTreeSet<NodeId>,
}

/// The pure decision [`RaftNode::fan_out_blob`] strips an op's bytes on (#481): true iff
/// `members` is non-empty, every one of them is in `capable`, and `skewed` is empty.
///
/// An **empty** `members` answers `false`, deliberately — the same reasoning
/// `network::majority_of` applies to an empty configuration. The question "can everyone apply
/// this" cannot be answered about zero members, and treating an unanswerable question as "yes"
/// is the one direction that cannot be undone: it strips bytes a real (just not-yet-observed)
/// membership might not be able to reconstruct.
fn sideload_safe(
    members: &BTreeSet<NodeId>,
    capable: &BTreeSet<NodeId>,
    skewed: &BTreeSet<NodeId>,
) -> bool {
    !members.is_empty() && members.is_subset(capable) && skewed.is_empty()
}

/// Drop every `capable` id that is not in `current_members` (#481) — so a member that has left
/// the fleet cannot keep occupying a slot in the remembered observation set forever. Harmless to
/// `sideload_safe` either way (extra ids in `capable` beyond `members` never make it stricter or
/// laxer), but unbounded growth across a fleet's lifetime of joins and departures is its own
/// slow leak, and this is the one place that closes it.
/// Fold one fan-out's observations into the remembered capability set (D-53).
///
/// **A fresh `false` beats a remembered `true`.** The set is otherwise grow-only, and a remembered
/// `true` outliving the build it described is a strip that wedges someone: a node id is chosen by
/// the operator, so replacing a machine and rejoining as the *same* id — with an older image — is
/// an ordinary move, and the prune below cannot catch it, because it only ever runs inside a
/// fan-out and there may be no write at all during the absence. An explicit `false` observed now is
/// strictly better evidence than a `true` observed earlier; honouring it is never less safe. It
/// also makes D-53's "a rolling downgrade in place is out of contract" a statement about intent
/// rather than a gap the code depends on.
fn remember_capability(
    capable: &mut BTreeSet<NodeId>,
    observed_capable: &BTreeSet<NodeId>,
    observed_incapable: &BTreeSet<NodeId>,
    current_members: &BTreeSet<NodeId>,
) {
    capable.extend(observed_capable);
    capable.retain(|id| !observed_incapable.contains(id));
    prune_sideload_capable(capable, current_members);
}

fn prune_sideload_capable(capable: &mut BTreeSet<NodeId>, current_members: &BTreeSet<NodeId>) {
    capable.retain(|id| current_members.contains(id));
}

/// Put `payload` to one peer, trying each address its authority resolved to.
///
/// Mirrors `call_any_typed`'s sweep rule: only a *liveness* failure is worth
/// trying the next address for. A peer that answers — even to refuse — has
/// answered, and re-asking a second address would overwrite that answer with a
/// later address's transport error.
async fn send_blob_to_peer(
    transfer: &crate::blobs::client::BlobTransfer,
    addrs: &[SocketAddr],
    digest: &crate::blobs::BlobDigest,
    payload: &[u8],
) -> Result<crate::blobs::client::PutOutcome, RpcError> {
    let mut last = RpcError::Transport("no address answered".to_owned());
    for addr in addrs {
        match transfer.put(*addr, digest, payload).await {
            Ok(outcome) => return Ok(outcome),
            Err(e @ (RpcError::Timeout | RpcError::Transport(_) | RpcError::Shed)) => last = e,
            Err(answered) => return Err(answered),
        }
    }
    Err(last)
}

/// Probe one peer's sideload capability without sending it any bytes (#481) — the learner half
/// of [`RaftNode::fan_out_blob`]'s capability sweep, for a member the byte quorum does not
/// include. Mirrors [`send_blob_to_peer`]'s same-peer address sweep rule exactly: only a
/// liveness failure is worth trying the next address for.
async fn stat_only_peer(
    transfer: &crate::blobs::client::BlobTransfer,
    addrs: &[SocketAddr],
    digest: &crate::blobs::BlobDigest,
) -> Result<bool, RpcError> {
    let mut last = RpcError::Transport("no address answered".to_owned());
    for addr in addrs {
        match transfer.stat_only(*addr, digest).await {
            Ok(capable) => return Ok(capable),
            Err(e @ (RpcError::Timeout | RpcError::Transport(_) | RpcError::Shed)) => last = e,
            Err(answered) => return Err(answered),
        }
    }
    Err(last)
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
    // This node's blob transfer store (#437): node-local, not part of the state
    // machine. `blobs()` exposes it read-write to whatever composes routes on
    // top of it; #438/#439 are the first such callers.
    blobs: Arc<crate::blobs::BlobStore>,
    // The live fetch-on-apply source (#439, D-23). Held so `/_cluster/health` can
    // read the stall it reports (D-48); the state machine holds its own handle.
    blob_source: Arc<super::blob_source::PeerBlobSource>,
    // Periodic GC sweep over `blobs`. Aborted on shutdown/drop, same as
    // `server_task` — nothing about it needs the graceful-drain treatment
    // `spawn_promotion_loop` gets, since it touches no Raft state.
    gc_task: JoinHandle<()>,
    // Whether shutdown() was ever invoked, so Drop can warn when a node is
    // dropped without the shutdown-then-drop contract — storage release is only
    // guaranteed through shutdown() (see Drop).
    shutdown_invoked: AtomicBool,
    // Ends a parked blob fetch so this node can actually stop (D-56, #513). A fetch that no
    // member can satisfy retries for as long as the node is up (D-48), and it runs *inside*
    // openraft's state-machine worker, which holds the storage handle — so without this signal
    // a parked node's `shutdown` times out and leaves its redb file locked. Sent by both
    // `shutdown()` and `Drop`.
    shutdown_signal: tokio::sync::watch::Sender<bool>,
    // The most recent reason a leave attempt failed, so the deadline's error can
    // name a cause instead of only reporting that time ran out.
    last_leave_error: Mutex<Option<String>>,
    // Serializes the membership changes this node arbitrates as leader. Shared
    // with the control routes so a departure this node evicts locally and one
    // it evicts for a peer take the same lock — the voter floor and the
    // auto-voter ceiling are only exact if every path holds it (#55, #69).
    membership_gate: network::MembershipGate,
    /// The auto-voter ceiling both admission phases enforce (#433).
    auto_voter_ceiling: network::AutoVoterCeiling,
    // Signals that parked intents deserve a drain attempt now, rather than at
    // the composition's next periodic sweep (#83). Lives here because this node
    // owns the parked-intent ledger (`park_intent`/`parked_intents`/
    // `unpark_intent`); whoever drains it is a composition concern, but the
    // "there is something to drain" fact is this node's.
    replay_wake: Arc<tokio::sync::Notify>,
    // Members `fan_out_blob` has *ever* observed applying a digest-only `ControlOp` (#481) —
    // remembered, not just this call's answer, because a build does not regress in place: once a
    // member proves it can decode a digest-only entry, a transient probe failure on a later
    // fan-out must not un-mark it. Pruned against the current committed ∪ effective membership
    // on every fan-out (`prune_sideload_capable`) so a departed member's id does not linger
    // forever.
    sideload_capable: Mutex<BTreeSet<NodeId>>,
    // This node's own answer to the same question `sideload_capable` tracks about peers — the
    // same bit its own blob `?stat` route advertises (#481). Read locally rather than over the
    // network: `fan_out_blob` never asks itself. Test-controlled by
    // `NodeConfig::advertise_as_digest_only_incapable`; every shipped configuration leaves it
    // `true`.
    digest_only_capable: bool,
}

impl RaftNode {
    /// The openraft `Config` this node runs with.
    ///
    /// Extracted from `start` so the default is assertable without a cluster: see
    /// `raft_config_default_leaves_the_snapshot_knobs_untouched`. `snapshot_log_entries` is
    /// [`NodeConfig::snapshot_log_entries`] — `None` must leave openraft's own defaults in place,
    /// which is the property that test pins.
    ///
    /// The snapshot policy set here is the *only* thing that ever takes a snapshot or purges the
    /// log (decision D-24): no admin route or console panel calls `trigger().snapshot()` or
    /// `purge_log`; the cluster maintains itself.
    fn raft_config(snapshot_log_entries: Option<u64>) -> Result<Config, NodeError> {
        let mut config = Config {
            cluster_name: "rift-control-plane".to_owned(),
            // Fixed here, not a `NodeConfig` knob (D-42): C6's leadership-transition bound is
            // derived from these numbers, and widening them so a count bound could hold was
            // the rejected alternative. #411 pins them below.
            election_timeout_min: 150,
            election_timeout_max: ELECTION_TIMEOUT_MAX_MS,
            heartbeat_interval: 50,
            // Snapshot transport (#428). openraft bounds each *chunk* by `install_snapshot_timeout`
            // and abandons the whole transfer — back to offset 0 — when one misses, so this is a
            // deadline that must never be tight. Its defaults (3 MiB chunks, 200 ms) cannot be met
            // even on loopback: chunks ride the JSON cluster port as `Vec<u8>`, measured at 4.0× on
            // the wire (`JSON_WIRE_EXPANSION`), so a default chunk is ~12 MiB of JSON and ~900 ms.
            // A fleet holding a few MiB of datasets could therefore never catch up a joining node.
            //
            // 1 MiB keeps a chunk's wire form (~4 MiB) far under the cluster port's 32 MiB body
            // cap. Unlike `heartbeat_interval`, neither knob is coupled to the election timers —
            // raising them costs nothing in failover latency.
            //
            // Note what the chunk size does *not* buy: a follower receiving a snapshot gets no
            // leader-lease refresh from it. `Raft::install_snapshot` only reaches the engine on
            // the final chunk (`install_full_snapshot`); intermediate chunks merely read the vote
            // and buffer, and openraft sends no AppendEntries to a peer while its snapshot streams
            // (`replication/mod.rs`: "can not send other data while sending snapshot"). So a peer
            // that is already a voter hears nothing for the whole install and can time out and
            // campaign — a hole this issue does not close, and one that a longer
            // `install_snapshot_timeout` widens rather than narrows.
            install_snapshot_timeout: 10_000,
            snapshot_max_chunk_size: 1024 * 1024,
            ..Default::default()
        };
        // D-43: the hidden testability knob. Both settings move together, or the wire path
        // stays unexercised; `None` — every shipped configuration — leaves openraft's defaults.
        if let Some(entries) = snapshot_log_entries {
            config.snapshot_policy = SnapshotPolicy::LogsSinceLast(entries);
            // Purge as soon as a snapshot covers the entries. Without this the snapshot exists but
            // the log a follower needs is still there, so it catches up by replication and
            // `install_snapshot` never runs — see the field's doc.
            config.max_in_snapshot_log_to_keep = 0;
        }
        config
            .validate()
            .map_err(|e| NodeError::Init(e.to_string()))
    }

    /// Restart grace (#431): a node that already belongs to a multi-voter cluster
    /// must not campaign the instant it comes back.
    ///
    /// The leader's reconnect is not immediate, and a voter that bumps its term
    /// before hearing the leader is rejected by the lease on every peer while
    /// the leader keeps its own term — a standoff that outlasts any snapshot
    /// install (measured: term 3 → 66 over 58 s with the leader flat at 1).
    /// Elections are held until a leader is heard or
    /// [`Self::RESTART_ELECTION_GRACE`] passes. A fresh node has no state and is
    /// unaffected; a single-voter fleet must elect itself and is exempt.
    /// Cadence of the leader's learner-promotion sweep (#433, phase 2). One
    /// second: an order of magnitude above the replication timers, so the
    /// sweep never competes with catch-up itself, while a caught-up learner
    /// still becomes a voter well inside any operator's patience.
    pub const LEARNER_PROMOTION_INTERVAL: Duration = Duration::from_secs(1);

    /// Phase 2 of two-phase admission (#433): every node runs the sweep on a
    /// fixed cadence; only the leader's ever acts (elsewhere every peer reads
    /// as catching up and the scan falls through, after one cheap metrics
    /// read). Holds the Raft only weakly — a strong handle here would keep
    /// the storage alive after a drop without shutdown, the same cycle the
    /// leading probe had to avoid (#431).
    fn spawn_promotion_loop(
        slot: &RaftSlot,
        gate: &network::MembershipGate,
        ceiling: &network::AutoVoterCeiling,
    ) {
        let slot = Arc::downgrade(slot);
        let gate = Arc::clone(gate);
        let ceiling = Arc::clone(ceiling);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Self::LEARNER_PROMOTION_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let Some(slot) = slot.upgrade() else { return };
                let Some(raft) = slot.get() else { continue };
                if raft.metrics().borrow().state != ServerState::Leader {
                    continue;
                }
                // A promotion racing a deposition surfaces ForwardToLeader or
                // a handler error; the next tick sees the truth. Background
                // sweep: nothing to propagate to, so the failure is a trace.
                if let Err(e) = network::promote_ready_learners(
                    raft,
                    &gate,
                    ceiling.load(std::sync::atomic::Ordering::Relaxed),
                )
                .await
                {
                    tracing::debug!(error = %e, "promotion sweep skipped this tick");
                }
            }
        });
    }

    /// One blob-store GC sweep's worth of work (#437/#480), shared by [`Self::spawn_blob_gc_loop`]
    /// and the test-facing [`Self::run_blob_gc_now`] so the two can never drift apart on what a
    /// sweep actually does.
    ///
    /// A `referenced_digests`/`blob_tombstones` scan failure is propagated with `?` and the sweep
    /// is skipped, never treated as an empty map — an empty referenced set would read as "nothing
    /// is referenced" and delete the whole store on a transient read error, and an empty tombstone
    /// map would read as "nothing is tombstoned" and reap precisely what #480's retention exists
    /// to keep. Plain, synchronous I/O throughout (the redb scans and the store's directory walk),
    /// so the caller is responsible for keeping it off the async runtime.
    ///
    /// Also prunes tombstones the rules can never act on again (D-52, D-55) — those at or below
    /// **both** this node's purge point and the fleet applied floor — which is the only thing
    /// that keeps `sm_blob_tombstones` from growing by a permanent row per delete.
    pub(crate) fn blob_gc_sweep(
        store: &crate::blobs::BlobStore,
        sm_reader: &RedbStateMachine,
        purged: Option<u64>,
        fleet_min_applied: Option<u64>,
        now_secs: u64,
    ) -> Result<u64, String> {
        let referenced = sm_reader.referenced_digests().map_err(|e| e.to_string())?;
        let tombstones = sm_reader.blob_tombstones().map_err(|e| e.to_string())?;
        // `None` (this node's log has never been purged) becomes `0`, the fail-closed reading:
        // `0` can never be a genuine purge boundary (real log indices start at 1), so it protects
        // every tombstoned digest rather than deciding — on no evidence — that any of them are
        // safe to reap. See `blobs::BlobStore::gc`'s doc for the full reasoning. The fleet floor
        // (D-55) reads the same way: `None` is "some member could not be asked", and `0` protects
        // everything tombstoned rather than guessing that member is caught up.
        let purged = purged.unwrap_or(0);
        let fleet_min_applied = fleet_min_applied.unwrap_or(0);
        // Reclaim rows the rules can never act on again (D-52, D-55). Best-effort and logged
        // rather than fatal: failing to prune costs a row, while failing the sweep costs the
        // reclamation of every blob this pass would have freed — so a prune failure must not take
        // the sweep with it. Deliberately after the `blob_tombstones()` read above, so this pass
        // still sees the rows it is about to drop and cannot reap a blob on a half-pruned view.
        //
        // Bounded by the *lower* of the two indices, never `purged` alone: a row this log has
        // passed but some member has not yet applied past still protects its blob under rule C,
        // and pruning it would turn that blob into a never-referenced leftover — reaped by the
        // plain grace rule on the next sweep, with nothing left to say otherwise. `min` with the
        // `0 == unknown` convention is itself fail-closed: either unknown makes the bound `0`.
        let prune_upto = purged.min(fleet_min_applied);
        match sm_reader.prune_blob_tombstones(prune_upto) {
            Ok(dropped) if dropped > 0 => {
                tracing::debug!(
                    dropped,
                    purged,
                    fleet_min_applied,
                    "blob gc: pruned tombstones both the log and the fleet have passed"
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, purged, fleet_min_applied, "blob gc: could not prune tombstones");
            }
        }
        let outcome = store
            .gc(
                &referenced,
                &tombstones,
                purged,
                fleet_min_applied,
                now_secs,
                BLOB_GC_GRACE_SECS,
            )
            .map_err(|e| e.to_string())?;
        // `outcome.retained` is `gc`'s own count of blobs kept under the tombstone rules (D-52
        // rule A, D-55 rule C) — not re-derived here from `tombstones` by hand, which could drift
        // from what `gc` actually did and would count tombstoned digests rather than blobs this
        // node held (a digest may name bytes this node never had).
        if outcome.retained > 0 {
            tracing::debug!(
                retained = outcome.retained,
                purged,
                fleet_min_applied,
                "blob gc: retaining tombstoned blobs the log or the fleet has not passed"
            );
        }
        crate::metrics::blob_gc_retained(outcome.retained);
        Ok(outcome.removed)
    }

    /// The fleet applied floor as one GC sweep should see it (D-55): the probe's answer, or
    /// `None` when the membership could not be read. Both failure shapes are logged here, once
    /// per sweep — `warn!`, not `debug!`, and naming the members that could not be asked: an
    /// unknown floor retains every tombstoned blob on this node until that member answers or is
    /// evicted, which is the "loud or indefinite" trade D-53 made, and it must be loud.
    async fn blob_gc_fleet_floor(
        raft: &Raft<TypeConfig>,
        client: &RpcClient,
        resolver: &Arc<dyn PeerResolver>,
    ) -> Option<u64> {
        match network::fleet_applied_floor(raft, client, resolver, BLOB_GC_FLOOR_BUDGET).await {
            Ok(network::FleetAppliedFloor::Known(min)) => Some(min),
            // Not a member of any cluster yet (started, not initialised or joined): there is no
            // fleet to have a floor, and nothing tombstoned to retain. Not the "unreachable
            // member" warning — that would send an operator looking for a peer that does not exist.
            Ok(network::FleetAppliedFloor::Unknown(unknown)) if unknown.is_empty() => {
                tracing::debug!(
                    "blob gc: no fleet membership yet; nothing to compute a floor over"
                );
                None
            }
            Ok(network::FleetAppliedFloor::Unknown(unknown)) => {
                tracing::warn!(
                    ?unknown,
                    "blob gc: fleet applied floor unknown — retaining every tombstoned blob \
                     until these members answer or leave the membership"
                );
                None
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "blob gc: could not read the membership for the fleet applied floor — \
                     retaining every tombstoned blob this sweep"
                );
                None
            }
        }
    }

    /// Periodic GC sweep over `store` (#437): every [`BLOB_GC_INTERVAL`], run
    /// [`Self::blob_gc_sweep`] against this node's own applied state, its own log purge point,
    /// and the fleet's applied floor (D-55).
    ///
    /// The floor probe is awaited on the runtime — it is a round of small RPCs — and only then is
    /// the sweep itself run in `spawn_blocking`: both the redb scans and the store's directory
    /// walk are synchronous, plain I/O, and holding a runtime worker for either is the stall #444
    /// is open against for snapshot building.
    fn spawn_blob_gc_loop(
        store: Arc<crate::blobs::BlobStore>,
        sm_reader: RedbStateMachine,
        raft: Raft<TypeConfig>,
        client: RpcClient,
        resolver: Arc<dyn PeerResolver>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            // Starts one interval in, not immediately: `interval` yields its
            // first tick at once, which would sweep before this node has
            // applied anything on a restart — computing "what is referenced"
            // from state that has not caught up yet.
            let mut tick = tokio::time::interval_at(
                tokio::time::Instant::now() + BLOB_GC_INTERVAL,
                BLOB_GC_INTERVAL,
            );
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let store = Arc::clone(&store);
                let sm_reader = sm_reader.clone();
                let purged = raft.metrics().borrow().purged.map(|log_id| log_id.index);
                let fleet_min_applied = Self::blob_gc_fleet_floor(&raft, &client, &resolver).await;
                // `0` is the safe direction for *now* (unlike for a file's mtime, where it means
                // "infinitely old" — see `blobs::mtime_secs`): it makes `now - mtime` saturate to
                // zero, which never clears the grace, so a clock this broken reclaims nothing
                // rather than everything.
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let outcome = tokio::task::spawn_blocking(move || {
                    Self::blob_gc_sweep(&store, &sm_reader, purged, fleet_min_applied, now_secs)
                })
                .await;
                match outcome {
                    Ok(Ok(removed)) if removed > 0 => {
                        tracing::debug!(removed, "blob store gc swept");
                    }
                    Ok(Ok(_)) => {}
                    // Not `debug!`: a sweep that keeps failing means the blob
                    // store grows without bound, and the only other symptom is
                    // a disk filling up. At the default level that is invisible
                    // — which would make `GcWatermark`, added to answer "is GC
                    // even running", unable to answer it.
                    Ok(Err(e)) => tracing::warn!(error = %e, "blob store gc sweep skipped"),
                    // A `JoinError` here is a panic inside `spawn_blocking`:
                    // a defect of ours, never an environmental condition.
                    Err(e) => tracing::error!(error = %e, "blob store gc task panicked"),
                }
            }
        })
    }

    /// Run exactly one blob-store GC sweep, with the same inputs [`Self::spawn_blob_gc_loop`]
    /// uses (#480) — test-facing, because the loop's 60 s interval and its constants are private.
    /// Modelled on [`Self::purged_index`]: a plain, hidden accessor a test can call directly
    /// rather than waiting out a real tick. The fleet applied floor (D-55) is probed for real
    /// over the wire, not stubbed: a fleet test's sweep must go through the same fan-out the loop
    /// does, or it pins nothing about the rule.
    ///
    /// `now_secs` is a parameter rather than read from the clock **because a test that swept at
    /// the real `now` could not tell the retention rule from the mtime grace**: a blob written
    /// seconds ago is inside `BLOB_GC_GRACE_SECS` regardless of any tombstone, so such a sweep
    /// keeps it either way and asserts nothing. The unit tests in `blobs::mod` use the same
    /// convention. It ages files only — a *request* (rule B) is aged by `Instant::elapsed` and no
    /// value passed here can move it.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::blob_gc_sweep`] returns: a `referenced_digests`/`blob_tombstones` scan
    /// failure, or a `blobs::BlobStore::gc` filesystem failure, both as a display string.
    #[doc(hidden)]
    pub async fn run_blob_gc_now(&self, now_secs: u64) -> Result<u64, String> {
        let fleet_min_applied =
            Self::blob_gc_fleet_floor(&self.raft, &self.client, &self.resolver).await;
        let store = Arc::clone(&self.blobs);
        let sm_reader = self.sm_reader.clone();
        let purged = self.purged_index();
        // Off the runtime, as the loop does: `blob_gc_sweep` is synchronous redb and directory I/O.
        tokio::task::spawn_blocking(move || {
            Self::blob_gc_sweep(&store, &sm_reader, purged, fleet_min_applied, now_secs)
        })
        .await
        .map_err(|e| format!("blob gc sweep task: {e}"))?
    }

    async fn hold_elections_until_leader_heard(raft: &Raft<TypeConfig>) -> Result<(), NodeError> {
        let has_state = raft
            .is_initialized()
            .await
            .map_err(|e| NodeError::Runtime(e.to_string()))?;
        if !has_state {
            return Ok(());
        }
        // Hold first, decide second — and only on evidence that arrives after
        // the hold begins. Right after `Raft::new` the metrics still carry the
        // *persisted* state: `current_leader` names whoever led before the
        // crash, which is memory, not an audible leader — releasing on it is
        // what let a restarted voter campaign immediately. What cannot come
        // from memory: the vote moving, or an entry reaching the log or state
        // machine. Neither happens without a live leader or candidate on the
        // wire, so those are the release triggers. A single-voter fleet is
        // exempt outright — it must elect itself.
        raft.runtime_config().elect(false);
        let raft = raft.clone();
        tokio::spawn(async move {
            let mut metrics = raft.metrics();
            let (init_vote, init_log, init_applied) = {
                let m = metrics.borrow();
                if m.membership_config.membership().voter_ids().count() == 1 {
                    raft.runtime_config().elect(true);
                    return;
                }
                (m.vote, m.last_log_index, m.last_applied)
            };
            let heard = async {
                loop {
                    if metrics.changed().await.is_err() {
                        return;
                    }
                    let m = metrics.borrow();
                    let voters = m.membership_config.membership().voter_ids().count();
                    if voters == 1
                        || m.vote != init_vote
                        || m.last_log_index > init_log
                        || m.last_applied > init_applied
                    {
                        return;
                    }
                }
            };
            let _ = tokio::time::timeout(Self::RESTART_ELECTION_GRACE, heard).await;
            raft.runtime_config().elect(true);
        });
        Ok(())
    }

    /// Open storage, bind the cluster server, and start the Raft runtime. This
    /// does not form or join a cluster; call [`RaftNode::cluster_init`] to
    /// bootstrap a new one or [`RaftNode::join_via`] to attach to an existing one.
    pub async fn start(config: NodeConfig) -> Result<Self, NodeError> {
        Self::start_inner(config, None, None, None).await
    }

    /// Like [`Self::start`], with the front door's compiled-route handle and this node's local
    /// request journal both attached to the state machine before `Raft::new` (issue #131 for the
    /// routes handle, #224 for the journal) — a separate constructor rather than two more
    /// `NodeConfig` fields so every existing caller (most of which touch neither) keeps
    /// compiling untouched. Same before-construction contract as `NodeConfig::engine`: attaching
    /// here, rather than after this call returns, means catch-up replay during a join drives the
    /// `ArcSwap` and pushes clear generations into the journal too, not just live commits
    /// afterward.
    pub async fn start_with_front_door_routes(
        config: NodeConfig,
        front_door_routes: Arc<ArcSwap<CompiledRoutes>>,
        journal: Arc<ClusterJournal>,
        sequencing: Arc<crate::stores::SequencingRegistry>,
    ) -> Result<Self, NodeError> {
        Self::start_inner(
            config,
            Some(front_door_routes),
            Some(journal),
            Some(sequencing),
        )
        .await
    }

    async fn start_inner(
        config: NodeConfig,
        front_door_routes: Option<Arc<ArcSwap<CompiledRoutes>>>,
        journal: Option<Arc<ClusterJournal>>,
        sequencing: Option<Arc<crate::stores::SequencingRegistry>>,
    ) -> Result<Self, NodeError> {
        let (log_store, state_machine) = store::new(config.data_dir.join(RAFT_DB_FILE))
            .await
            .map_err(|e| NodeError::Storage(e.to_string()))?;
        let state_machine = match &config.engine {
            Some(engine) => state_machine.with_engine(engine.clone()),
            None => state_machine,
        };
        let state_machine = match front_door_routes {
            Some(routes) => state_machine.with_routes_handle(routes),
            None => state_machine,
        };
        let state_machine = match &sequencing {
            Some(registry) => state_machine.with_sequencing_registry(Arc::clone(registry)),
            None => state_machine,
        };
        let state_machine = match &journal {
            Some(journal) => state_machine.with_journal(journal),
            None => state_machine,
        };
        let state_machine = state_machine.with_audit_retention_secs(config.audit_retention_secs);
        // Dataset blobs materialise under the node's own data directory (RFC-005 D1, #285),
        // beside `RAFT_DB_FILE` — node-local derived state, not itself part of the redb file.
        let state_machine = state_machine.with_spool_dir(config.data_dir.join("datasets"));

        // Blob transfer store (#437): node-local, off the redb file, beside the dataset spool
        // dir this state machine already writes under the same data directory.
        let blob_store = Arc::new(
            crate::blobs::BlobStore::open(config.data_dir.join("blobs"))
                .map_err(|e| NodeError::Storage(format!("blob store: {e}")))?,
        );

        // The handlers need the Raft, which needs the bound server address, which
        // needs the handlers — so the router reads the node through a slot filled
        // in once construction below completes. The blob source reads it the same
        // way, for the same reason: it is attached to the state machine here,
        // before the `Raft` exists to be handed to it.
        let slot: RaftSlot = Arc::new(OnceCell::new());

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
        let client = RpcClient::new(
            signer,
            Arc::new(TrackedPeerHealth::new()),
            RpcClientConfig::default(),
        );
        let resolver: Arc<dyn PeerResolver> = Arc::new(DnsResolver);

        // Where a digest-only op's bytes come from when this node lacks them (#439, D-23).
        // Same before-`Raft::new`-and-before-clone contract as `with_journal`: attached here
        // so catch-up replay during a join can fetch, not only live commits afterward.
        let (shutdown_signal, shutdown_rx) = tokio::sync::watch::channel(false);
        let blob_source = Arc::new(super::blob_source::PeerBlobSource::new(
            config.node_id,
            Arc::clone(&blob_store),
            client.clone(),
            Arc::clone(&resolver),
            &slot,
            shutdown_rx,
        ));
        let state_machine = state_machine.with_blob_source(blob_source.clone());
        let sm_reader = state_machine.clone();

        let raft_config = Arc::new(Self::raft_config(config.snapshot_log_entries)?);
        // Control-plane routes register last so a caller's route table can never
        // shadow the Raft endpoints the cluster itself depends on.
        let membership_gate: network::MembershipGate = Arc::new(tokio::sync::Mutex::new(()));
        // One ceiling for both admission phases (#433); see `AutoVoterCeiling`.
        let auto_voter_ceiling: network::AutoVoterCeiling = Arc::new(
            std::sync::atomic::AtomicUsize::new(network::MAX_AUTO_VOTERS),
        );
        let router = network::control_routes(
            config.routes.clone(),
            slot.clone(),
            Arc::clone(&membership_gate),
            Arc::clone(&auto_voter_ceiling),
        );
        // Read once, up front: `?stat` advertises exactly this bit on every response, and
        // `fan_out_blob` below reads it back as this node's own answer rather than asking
        // itself over the network — see `RaftNode::digest_only_capable`'s doc.
        let digest_only_capable = !config.advertise_as_digest_only_incapable;
        // `sm_reader` is the fallback: applied state serves a referenced blob whose bytes
        // never reached this node's transport store (#486, D-51).
        let router = crate::blobs::routes::blob_routes(
            router,
            Arc::clone(&blob_store),
            Arc::new(sm_reader.clone()),
            digest_only_capable,
        );

        let server = RpcServer::bind(config.bind, RpcServerConfig::new(verifier, router))
            .await
            .map_err(|e| NodeError::Bind(e.to_string()))?;
        let local = server
            .local_addr()
            .map_err(|e| NodeError::Bind(e.to_string()))?;
        let advertise = config.advertise.unwrap_or_else(|| Authority::from(local));
        // The liveness tickers consult this before probing: a probe asserts
        // "your leader is alive", which is only true while we lead — a
        // departed or deposed leader's tickers would otherwise keep every
        // follower's leader lease fresh and postpone the very election the
        // fleet needs (the C5 graceful-leave handover). A live read of the
        // metrics watch at the moment of the decision, not a flag maintained
        // by a task whose scheduling lag could leave a stale `true` behind —
        // under load, exactly when it would matter. Before the slot is filled
        // (this node's own `Raft::new` below), nothing leads.
        // Held as a `Weak`: the probe travels into the network factory, which
        // the `Raft` owns — a strong slot here would close the cycle
        // Raft → factory → probe → slot → Raft and keep the storage alive
        // after a drop without shutdown.
        let leading: super::network::LeadingProbe = {
            let slot = Arc::downgrade(&slot);
            Arc::new(move || {
                slot.upgrade()
                    .as_deref()
                    .and_then(OnceCell::get)
                    .is_some_and(|raft| raft.metrics().borrow().state == ServerState::Leader)
            })
        };
        let network = RpcNetwork::new(client.clone(), Arc::clone(&resolver), leading);

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
        Self::hold_elections_until_leader_heard(&raft).await?;
        Self::spawn_promotion_loop(&slot, &membership_gate, &auto_voter_ceiling);

        let server_task = tokio::spawn(server.serve());
        let gc_task = Self::spawn_blob_gc_loop(
            Arc::clone(&blob_store),
            sm_reader.clone(),
            raft.clone(),
            client.clone(),
            Arc::clone(&resolver),
        );

        Ok(Self {
            id: config.node_id,
            advertise,
            raft,
            client,
            resolver,
            sm_reader,
            server_task,
            blobs: blob_store,
            blob_source,
            gc_task,
            shutdown_invoked: AtomicBool::new(false),
            shutdown_signal,
            last_leave_error: Mutex::new(None),
            membership_gate,
            auto_voter_ceiling,
            replay_wake: Arc::new(tokio::sync::Notify::new()),
            sideload_capable: Mutex::new(BTreeSet::new()),
            digest_only_capable,
        })
    }

    /// This node's Raft id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Lower (or raise) the auto-voter ceiling this node enforces in **both**
    /// admission phases — the join handler's in-call promotion and the
    /// promotion sweep. Test-only: it exists so a ceiling test can provoke the
    /// #55 race on a three-node cluster instead of an eleven-node one, with
    /// the sweep bound by the same number.
    #[doc(hidden)]
    pub fn set_auto_voter_ceiling(&self, ceiling: usize) {
        self.auto_voter_ceiling
            .store(ceiling, std::sync::atomic::Ordering::Relaxed);
    }

    /// The authority peers dial this node on.
    #[must_use]
    pub fn advertise(&self) -> &Authority {
        &self.advertise
    }

    /// This node's blob transfer store (#437).
    #[must_use]
    pub fn blobs(&self) -> &Arc<crate::blobs::BlobStore> {
        &self.blobs
    }

    /// The blob fetch this node's apply is currently parked on, if any (#439, D-48) — what
    /// `/_cluster/health` reports as `blob_fetch_stall`. `None` is the healthy answer.
    #[must_use]
    pub fn blob_fetch_stall(&self) -> Option<super::blob_source::BlobFetchStall> {
        self.blob_source.stall()
    }

    /// Store `bytes` locally under `digest`, then fan it out to every other
    /// member and report which of them hold it (#438).
    ///
    /// The blob is **pinned** for the whole call: between storing it and the op
    /// that references it committing, nothing in the state machine points at it,
    /// so the reaper is the only thing that could take it (see
    /// [`crate::blobs::BlobStore::pin`]).
    ///
    /// Peers are dialled concurrently — a 64 MiB blob is 17 chunked round trips
    /// *per member*, and serialising those would multiply the write's latency by
    /// the fleet size.
    ///
    /// This node counts toward the acks because it has just stored the blob.
    /// When it is a *learner* rather than a voter that ack is silently ignored,
    /// because [`QuorumTargets::satisfied_by`] intersects the acks with each
    /// configuration before counting — a non-voter holding the blob is not
    /// evidence about that configuration's durability.
    ///
    /// # Errors
    ///
    /// [`NodeError::Runtime`] if the blob cannot be stored locally or the
    /// membership cannot be read. A peer that refuses, times out, or is
    /// unreachable is **not** an error — it is an absent ack, and the caller
    /// decides what to do about the shortfall via
    /// [`FanOutOutcome::quorum`].
    pub async fn fan_out_blob<'a>(
        &'a self,
        digest: &crate::blobs::BlobDigest,
        bytes: &[u8],
    ) -> Result<(FanOutOutcome, crate::blobs::BlobPin<'a>), NodeError> {
        // Pinned *before* the bytes land, so there is no instant at which the
        // blob exists unprotected. Handed back to the caller rather than
        // released here: the window this guards runs from "a quorum holds it"
        // to "the op that references it commits", and the submit is the
        // caller's. Releasing on return would leave the blob unpinned *and*
        // unreferenced for exactly the stretch the pin exists to cover —
        // survivable in this issue only because the op still carries its bytes,
        // and not survivable at #439.
        let pin = self.blobs.pin(digest);

        let store = Arc::clone(&self.blobs);
        let digest_owned = digest.clone();
        let payload: Arc<[u8]> = Arc::from(bytes);
        let local = Arc::clone(&payload);
        // The store is synchronous, plain file I/O; holding a runtime worker
        // for a multi-MiB write is the stall #444 is open against elsewhere.
        tokio::task::spawn_blocking(move || store.store_whole(&digest_owned, &local))
            .await
            .map_err(|e| NodeError::Runtime(format!("blob store task: {e}")))?
            .map_err(|e| NodeError::Runtime(format!("storing blob locally: {e}")))?;

        // Both views in one read (#481): D-19's byte quorum stays voters-only, but a learner
        // applies the log the same as a voter, so its sideload capability matters even though it
        // is never sent bytes below. Read separately, the two could come from different
        // membership epochs and the gate would be deciding about a configuration that never was.
        let membership = network::joint_members(&self.raft)
            .await
            .map_err(|e| NodeError::Runtime(e.to_string()))?;
        let targets = membership.voters;
        let all_members = membership.all;
        let voter_members = targets.members();

        // Resolve before spawning: `resolve` borrows `self`, and the transfer
        // tasks must own everything they touch. Split here into who gets bytes
        // (`peers`, unchanged from before #481) and who is only probed for
        // capability (`probe_only`, the learners `all_members` adds).
        let mut peers: Vec<(NodeId, Vec<SocketAddr>)> = Vec::new();
        let mut probe_only: Vec<(NodeId, Vec<SocketAddr>)> = Vec::new();
        for id in all_members.iter().copied() {
            if id == self.id {
                continue;
            }
            let Some(authority) = self.member_authority(id) else {
                continue;
            };
            match self.resolve(&authority).await {
                Ok(addrs) if !addrs.is_empty() => {
                    if voter_members.contains(&id) {
                        peers.push((id, addrs));
                    } else {
                        probe_only.push((id, addrs));
                    }
                }
                Ok(_) => {
                    tracing::warn!(node_id = id, %authority, "blob fan-out: authority resolved to no address")
                }
                Err(e) => {
                    tracing::warn!(node_id = id, %authority, error = %e, "blob fan-out: could not resolve peer")
                }
            }
        }

        // One shared client: it is pooled internally, so every peer's transfer
        // reuses the same connections and signer rather than standing up its own.
        let client = Arc::new(self.client.clone());
        let mut tasks = tokio::task::JoinSet::new();
        for (id, addrs) in peers {
            let transfer = crate::blobs::client::BlobTransfer::new(Arc::clone(&client));
            let digest = digest.clone();
            let payload = Arc::clone(&payload);
            tasks.spawn(async move {
                (
                    id,
                    send_blob_to_peer(&transfer, &addrs, &digest, &payload).await,
                )
            });
        }

        // Spawned here, before either set is awaited, so the capability probes overlap the byte
        // transfers instead of following them. Serially this cost `sum` over the learners — and
        // an unreachable learner is never remembered, so that bill was paid on *every* write and
        // could push a write past its caller's timeout. Concurrently it is `max`, and in a healthy
        // fleet it is free: a stat round trip finishes long before a peer that is being sent
        // megabytes. Learners are probed every round rather than skipped once remembered, because
        // skipping is what would let a stale `true` outlive the build it described — see
        // `remember_capability`.
        let mut probes = tokio::task::JoinSet::new();
        for (id, addrs) in probe_only {
            let transfer = crate::blobs::client::BlobTransfer::new(Arc::clone(&client));
            let digest = digest.clone();
            probes.spawn(async move { (id, stat_only_peer(&transfer, &addrs, &digest).await) });
        }

        let mut acks: BTreeSet<NodeId> = BTreeSet::from([self.id]);
        let mut skewed: BTreeSet<NodeId> = BTreeSet::new();
        let mut bytes_sent = 0_u64;
        // This fan-out's own observations, kept apart from the remembered `sideload_capable`
        // until the end: a peer confirmed capable this round is remembered forever, but a peer
        // that merely didn't answer *this* round must not overwrite an earlier `true`.
        let mut observed_capable: BTreeSet<NodeId> = BTreeSet::new();
        let mut observed_incapable: BTreeSet<NodeId> = BTreeSet::new();
        if self.digest_only_capable {
            observed_capable.insert(self.id);
        }
        while let Some(joined) = tasks.join_next().await {
            let (id, result) = match joined {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!(error = %e, "blob fan-out task panicked");
                    continue;
                }
            };
            match result {
                Ok(outcome) => {
                    bytes_sent += outcome.bytes_sent;
                    acks.insert(id);
                    if outcome.applies_digest_only {
                        observed_capable.insert(id);
                    } else {
                        observed_incapable.insert(id);
                    }
                }
                // A build without the blob route cannot answer the question
                // being asked. It is neither "has it" nor "lacks it", and
                // folding it into either would let a version-skewed fleet
                // report a durability it never had — so it counts as nothing
                // and is surfaced separately.
                Err(e @ (RpcError::UnknownRoute { .. } | RpcError::VersionSkew { .. })) => {
                    tracing::warn!(node_id = id, error = %e, "blob fan-out: peer cannot serve blobs; not counted toward quorum");
                    skewed.insert(id);
                }
                Err(e) => {
                    tracing::warn!(node_id = id, error = %e, "blob fan-out: peer did not take the blob");
                }
            }
        }

        // Learners never receive bytes (D-19 is unchanged), but they apply the log the same as a
        // voter, so their sideload capability still has to be probed — a stat-only round trip that
        // costs no bytes.
        while let Some(joined) = probes.join_next().await {
            let (id, result) = match joined {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!(error = %e, "blob capability probe task panicked");
                    continue;
                }
            };
            match result {
                Ok(true) => {
                    observed_capable.insert(id);
                }
                Ok(false) => {
                    observed_incapable.insert(id);
                }
                // A build with no blob route at all (pre-#437) cannot decode a digest-only entry
                // either, so it is *incapable*, not merely unobserved — the same reading the byte
                // path already gives it via `skewed`. Classifying it as unobserved would send an
                // operator to the runbook line for a transient probe failure, which says a leader
                // failover is self-correcting; for a permanently old learner that is the wrong
                // advice, and nothing would ever correct it.
                Err(e @ (RpcError::UnknownRoute { .. } | RpcError::VersionSkew { .. })) => {
                    tracing::warn!(node_id = id, error = %e, "blob fan-out: peer's build cannot serve blobs");
                    observed_incapable.insert(id);
                }
                Err(e) => {
                    tracing::warn!(node_id = id, error = %e, "blob fan-out: could not probe sideload capability");
                }
            }
        }

        let capable_snapshot = {
            let mut capable = self
                .sideload_capable
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            remember_capability(
                &mut capable,
                &observed_capable,
                &observed_incapable,
                &all_members,
            );
            capable.clone()
        };
        let sideload_safe = sideload_safe(&all_members, &capable_snapshot, &skewed);
        let not_capable: BTreeSet<NodeId> =
            all_members.difference(&capable_snapshot).copied().collect();
        // Skewed peers are also known-incapable, just via a different signal (no blob route at
        // all rather than an explicit `false`): a build that old cannot decode a digest-only
        // entry either.
        let sideload_incapable: BTreeSet<NodeId> = not_capable
            .iter()
            .copied()
            .filter(|id| observed_incapable.contains(id) || skewed.contains(id))
            .collect();
        let sideload_unobserved: BTreeSet<NodeId> = not_capable
            .difference(&sideload_incapable)
            .copied()
            .collect();

        Ok((
            FanOutOutcome {
                quorum: targets.satisfied_by(&acks),
                joint: targets.is_joint(),
                acks,
                skewed,
                bytes_sent,
                sideload_safe,
                sideload_incapable,
                sideload_unobserved,
            },
            pin,
        ))
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
    ///
    /// The seed need not be the leader. A follower answers with a typed
    /// [`RpcError::NotLeader`] naming who is, and this re-issues the join there,
    /// chasing a leadership that moves mid-join. The budget is
    /// [`Self::FORWARD_ATTEMPTS`] *sends* — the seed plus that many less one
    /// redirects — for the same reason the write path's [`submit`](Self::submit)
    /// bounds its forwards: a flapping election must not park the caller (#391).
    /// It is one send stingier than `submit`, which spends a local write before
    /// its forwards; the difference is not worth a second constant.
    ///
    /// Seeding at a follower is not an exotic case: `--cluster-seeds` pointing at
    /// one stable member is the obvious thing for an operator to configure, and
    /// before this the joiner retried that member until its deadline expired.
    pub async fn join_via(&self, seed: &Authority) -> Result<JoinOutcome, NodeError> {
        let request = JoinRequest {
            node_id: self.id,
            advertise: self.advertise.to_string(),
        };
        let body =
            serde_json::to_vec(&request).map_err(|e| NodeError::Membership(e.to_string()))?;

        let accepted: JoinAccepted = chase_join(seed.as_str(), Self::FORWARD_ATTEMPTS, |target| {
            let body = body.clone();
            async move {
                let reply = self
                    .call_any_typed(&target, "POST", CLUSTER_JOIN_PATH, body)
                    .await?;
                serde_json::from_slice(&reply)
                    .map_err(|e| RpcError::Handler(format!("decode join reply: {e}")))
            }
        })
        .await?;

        if !accepted.admitted {
            // No current leader sends this, but the field exists on the wire
            // and a refusal misread as an admission would let a non-member
            // start serving.
            return Err(NodeError::Membership(format!(
                "seed {seed} answered the join without admitting this node"
            )));
        }
        Ok(JoinOutcome {
            role: match accepted.role {
                Some(AdmittedRole::Voter) => JoinedAs::Voter,
                Some(AdmittedRole::Learner) => JoinedAs::Learner,
                // An older leader's bare `Ok`: its admission included the full
                // blocking catch-up, so "member, current" is what it meant.
                None => JoinedAs::Unknown,
            },
            catching_up: accepted.catching_up,
        })
    }

    /// Best-effort, deadline-bounded departure from the cluster (issue #6).
    ///
    /// If this node is currently the leader, it evicts itself in one local
    /// call: a voter's departure is a single membership change, which openraft
    /// commits under the joint quorum — in which the leaving leader still
    /// counts — so it keeps leading until the uniform entry takes effect and
    /// then steps down (D-59). If that local
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
        // a departure commits as a joint entry then a uniform one (D-59), and if
        // leadership moves between them this node is still a voter in a joint
        // configuration, with `current_leader` reporting `None` on every node
        // including this one — so the RPC path has nothing to chase and could
        // never finish what the local path started.
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
    /// membership — that is what the departure marker in `rift-cluster-server` is
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

    /// The advertise authority of a specific member, from the applied
    /// membership. `None` when the id is not (or no longer) a member — which a
    /// caller holding a [`Ring`] snapshot can see across a membership change,
    /// and must treat as "re-resolve ownership", not as an error.
    #[must_use]
    pub fn member_authority(&self, id: NodeId) -> Option<String> {
        super::blob_source::authority_of(&self.raft, id)
    }

    /// Call `method path` on member `id`, resolving its advertise authority and
    /// trying every address it yields (#79's any-address contract).
    ///
    /// The flow-state subsystem's transport (#120): it works in [`Ring`] node
    /// ids, and this is the bridge from an id to a wire call without exporting
    /// the resolver/client plumbing.
    ///
    /// # Errors
    ///
    /// The member is unknown, its authority does not resolve, or every address
    /// refused. The text is operator-facing, not client-facing.
    pub async fn call_member(
        &self,
        id: NodeId,
        method: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        let authority = self
            .member_authority(id)
            .ok_or_else(|| format!("node {id} is not in the applied membership"))?;
        self.call_any(&authority, method, path, body).await
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

    /// [`call_any`](Self::call_any) with the typed error preserved.
    ///
    /// Same address-sweep contract as `call_any` with one deliberate difference:
    /// a peer that *answers* ends the sweep even when the answer is a refusal.
    /// Only a liveness failure — unreachable, timed out, shed — is worth trying
    /// the next address for. Without that, a `NotLeader` redirect recovered from
    /// the first address would be overwritten by a later address's transport
    /// error and the hint lost (#391).
    ///
    /// Kept separate from `call_any` rather than made its implementation: the
    /// four other callers have relied on the try-every-address-on-any-error
    /// sweep since #79, and this fix has no business changing them.
    async fn call_any_typed(
        &self,
        authority: &str,
        method: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, RpcError> {
        let addrs = self
            .resolve(authority)
            .await
            .map_err(|e| RpcError::Transport(format!("resolve {authority}: {e}")))?;
        let mut last = RpcError::Transport(format!("{authority}: no addresses to try"));
        for peer in &addrs {
            match self.client.call(*peer, method, path, body.clone()).await {
                Ok(reply) => return Ok(reply),
                Err(e) if e.is_liveness_failure() => last = e,
                Err(e) => return Err(e),
            }
        }
        Err(last)
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

    /// How long a restarting member waits to hear a leader before it may
    /// campaign. Generously above the leader's 50 ms reconnect retry, so the
    /// common case — the leader is fine, this node was just down — never
    /// bumps the term; a genuinely dead leader still gets replaced, just not by
    /// a node that has been up for 200 ms.
    pub const RESTART_ELECTION_GRACE: Duration = Duration::from_secs(3);

    /// Wait for **this node's own** state machine to apply `revision`, up to
    /// `timeout`. Returns whether it landed.
    ///
    /// The local half of [`Self::await_applied`]: no peer is consulted, so it
    /// costs a metrics-watch subscription and no RPC. Event-driven rather than
    /// polled — openraft wakes it when the applied index moves.
    ///
    /// `--cluster-write-barrier none` needs exactly this and nothing more.
    /// "none" means a write does not wait for the *fleet*; it never meant the
    /// answering node may describe a state it cannot itself show. The admin
    /// front renders a create by re-reading the resource it just committed, so
    /// without this wait that re-read races the local apply and a durably
    /// committed write can answer `404` — a status the client cannot tell
    /// apart from "no such imposter" (#99).
    pub async fn await_local_applied(&self, revision: u64, timeout: Duration) -> bool {
        match self
            .raft
            .wait(Some(timeout))
            .applied_index_at_least(Some(revision), "local write barrier")
            .await
        {
            Ok(_) => true,
            Err(e) => {
                // Both variants mean unconfirmed, but they mean very different
                // things to an operator: every write in flight during a
                // graceful drain reports `shutting_down`, and a rolling restart
                // would otherwise emit a stream of warnings indistinguishable
                // from a genuinely slow node. Structured so the two can be told
                // apart without parsing the message. Reported, not swallowed:
                // the caller decides what to render, and a silent false here
                // would be indistinguishable from a fast apply.
                let reason = match e {
                    WaitError::Timeout(..) => "timeout",
                    WaitError::ShuttingDown => "shutting_down",
                };
                tracing::debug!(revision, reason, error = %e, "local apply not confirmed");
                false
            }
        }
    }

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

    /// How many intents this node has accepted and not yet retired (issue #360).
    ///
    /// The queue depth without the queue: see
    /// [`RedbStateMachine::parked_intent_count`] for why a polled read must not
    /// go through [`parked_intents`](Self::parked_intents).
    pub fn parked_intent_count(&self) -> Result<u64, NodeError> {
        self.sm_reader
            .parked_intent_count()
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

    /// Read the committed imposter-config JSON for `tenant`'s `port` from the
    /// applied state machine. Answers from local durable state — it does not
    /// require leadership.
    pub fn get_imposter(&self, tenant: &str, port: u16) -> Result<Option<String>, NodeError> {
        self.sm_reader
            .read_config(tenant, port)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The applied proxy-recording marker for `(tenant, port, sig_hash)` (#226): the
    /// recorded-response JSON, or `None` when the signature was never recorded (or was
    /// cleared). Answers from local applied state without leadership — the property that
    /// lets a post-handoff claim owner say `AlreadyRecorded` with no in-memory trace.
    ///
    /// # Errors
    ///
    /// Storage I/O.
    pub fn proxy_recorded(
        &self,
        tenant: &str,
        port: u16,
        sig_hash: &str,
    ) -> Result<Option<String>, NodeError> {
        self.sm_reader
            .proxy_recorded_resp(tenant, port, sig_hash)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Last engine side-effect failure per port (0 = set-level): the ports whose
    /// committed config the local engine could not realize (e.g. a bind
    /// failure). Empty on a healthy node.
    #[must_use]
    pub fn apply_failures(&self) -> BTreeMap<u16, String> {
        self.sm_reader.apply_failures()
    }

    /// Why the local engine serves `port` in-process only, because it could not bind that port
    /// (RFC-001 §7.4.6, issue #143). `None` when the port is healthy or this node does not serve it.
    ///
    /// Use this — not [`Self::apply_failures`] — for anything that *claims* bind divergence. See
    /// [`RedbStateMachine::bind_failure`] for why the general failure map is the wrong source: it
    /// cannot distinguish a failed bind from an unreadable cert or a rejected stub patch, and those
    /// leave the node serving nothing for that port rather than serving it in-process.
    #[must_use]
    pub fn bind_failure(&self, port: u16) -> Option<String> {
        self.sm_reader.bind_failure(port)
    }

    /// Is this node's engine actually holding `port`'s socket? See
    /// [`RedbStateMachine::is_locally_bound`] — and use **this**, never `bind_failure(..).is_none()`,
    /// before treating `127.0.0.1:port` as this imposter.
    #[must_use]
    pub fn is_locally_bound(&self, port: u16) -> bool {
        self.sm_reader.is_locally_bound(port)
    }

    /// Answer `req` as `port`'s imposter would, in-process — the try endpoint's dispatch (issue
    /// #344). See [`RedbStateMachine::dispatch_to_imposter`]: `None` means this node's engine does
    /// not hold `port` at all, and must never be answered as if it did; the try must be answered by
    /// the imposter this node owns, by construction, instead of a loopback dial BSD can route
    /// elsewhere.
    ///
    /// **Tenant-blind, deliberately.** This resolves by port alone and answers whatever imposter is
    /// there; it is the caller's job to have already proved the port belongs to the acting tenant
    /// (the admin front's `addressed_port` → `authorize_action` ownership gate does, before
    /// `terminate_try_imposter` ever reaches this). A new caller that skipped that gate would have
    /// built a cross-tenant dispatch in one line — do not add one without it.
    pub fn dispatch_to_imposter(
        &self,
        port: u16,
        req: Request<Incoming>,
    ) -> Option<impl Future<Output = Response<Full<Bytes>>> + Send + 'static> {
        self.sm_reader.dispatch_to_imposter(port, req)
    }

    /// Every port this node's engine holds, split by bound vs. failed (issue #369, blocker B4). See
    /// [`RedbStateMachine::local_bind_report`] — a single in-memory pass over the engine's own
    /// imposter set, not a redb transaction, which is what makes it safe to call on every 5-second
    /// `/_cluster/members` poll.
    #[must_use]
    pub fn local_bind_report(&self) -> Option<(Vec<u16>, BTreeMap<u16, String>)> {
        self.sm_reader.local_bind_report()
    }

    /// `(tenant, port)` for every port this node has a committed config for,
    /// fleet-wide, ascending. Like [`Self::get_imposter`], this answers from
    /// applied local state. Fleet-wide and not tenant-scoped on purpose — it
    /// backs the operator surface `GET /_cluster/config`, not a tenant-facing
    /// read.
    pub fn configured_ports(&self) -> Result<Vec<(TenantId, u16)>, NodeError> {
        self.sm_reader
            .configured_ports()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// `tenant`'s front-door route table, as currently applied. Like
    /// [`Self::get_imposter`], this answers from local durable state — it
    /// does not require leadership. Issue #131: upstream has no `GET
    /// /front-door/routes` for the clustered admin front to proxy to, so this
    /// is the only read path.
    pub fn route_table(&self, tenant: &str) -> Result<RouteTable, NodeError> {
        self.sm_reader
            .route_table(tenant)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// `tenant`'s route table together with the revision it is at (issue #210),
    /// read as one consistent snapshot. This is what `GET /front-door/routes`
    /// answers: the table, and the token a client feeds back as `If-Match` to
    /// make its next write conditional on having read this exact table.
    ///
    /// A tenant whose table has never been written is at revision `0`.
    ///
    /// # Errors
    /// Storage I/O, or a stored route that will not parse.
    pub fn route_table_with_revision(&self, tenant: &str) -> Result<(RouteTable, u64), NodeError> {
        self.sm_reader
            .route_table_with_revision(tenant)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The stored revision of `tenant`'s imposter on `port`, or `None` when
    /// applied state holds no such record — the read half of the
    /// single-imposter `If-Match` contract (C5, issue #188).
    pub fn imposter_revision(&self, tenant: &str, port: u16) -> Result<Option<u64>, NodeError> {
        self.sm_reader
            .imposter_revision(tenant, port)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The applied config JSON for `tenant`'s `port`, or `None` if none is applied.
    ///
    /// Answers from the applied state machine, so a follower or a restarted node serves it without
    /// waiting to become leader — the same read path as [`Self::imposter_revision`].
    ///
    /// Exists for the ownership lookup (#359): a flow's owner is decided by its id under the
    /// imposter's `flowState.contextScope`, and that scope is only knowable from the imposter's own
    /// config. The JSON is returned unparsed because the caller wants one field out of it and the
    /// parse belongs where that field is interpreted.
    pub fn imposter_config(&self, tenant: &str, port: u16) -> Result<Option<String>, NodeError> {
        self.sm_reader
            .read_config(tenant, port)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Block until this node's leadership differs from `was_leader`, or until
    /// `timeout` elapses; return its leadership as of the moment this returns.
    ///
    /// Event-driven off the same `RaftMetrics` watch the forward-to-leader path
    /// reads (issue #135) — deliberately *not* a second leadership source. Two
    /// independent notions of "am I the leader" is how a fleet ends up with two
    /// pollers, which is the exact failure the tracking scheduler exists to
    /// prevent.
    ///
    /// The bounded wait is the safety net, not the mechanism: the watch also
    /// fires on ordinary metrics movement (a committed entry), so the caller
    /// re-reconciles promptly on a `SourcePut` without needing its own signal.
    /// A closed watch (the Raft core is gone) reports the current value and
    /// lets the caller notice on its next upgrade.
    ///
    /// Returns a plain `bool` rather than the metrics themselves: openraft is
    /// an implementation detail of this crate and must not reach its public API.
    pub async fn await_leadership_change(&self, was_leader: bool, timeout: Duration) -> bool {
        let mut receiver = self.raft.metrics();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Read and drop the watch guard in one statement: holding a
            // `borrow()` across the await below would block the sender.
            let now = receiver.borrow().state == ServerState::Leader;
            if now != was_leader {
                return now;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return now;
            }
            match tokio::time::timeout(remaining, receiver.changed()).await {
                // Timed out, or the Raft core dropped the sender: report what
                // is true now and let the caller decide what to do about it.
                Err(_) | Ok(Err(_)) => return receiver.borrow().state == ServerState::Leader,
                // Metrics moved — re-read and check again.
                Ok(Ok(())) => {}
            }
        }
    }

    /// Whether this node is currently the Raft leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().state == ServerState::Leader
    }

    /// Every imposter source `tenant` has declared, id-ascending (issue #134).
    /// Like [`Self::get_imposter`], this answers from local applied state and
    /// needs no leadership — which is what lets any node serve
    /// `GET /admin/sources`.
    pub fn sources(&self, tenant: &str) -> Result<Vec<SourceRecord>, NodeError> {
        self.sm_reader
            .sources(tenant)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Every declared source in the fleet paired with its owning tenant,
    /// `(tenant, id)`-ascending (issue #241). What the poll scheduler
    /// reconciles against, so that a tracking source polls whichever tenant
    /// declared it.
    ///
    /// A row whose stored value will not decode comes back with
    /// [`SourceRow::record`] `= Err` rather than failing the call — one
    /// tenant's corruption must not park the fleet's reconciliation (#243).
    pub fn sources_all(&self) -> Result<Vec<SourceRow>, NodeError> {
        self.sm_reader
            .sources_all()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// One source by id, or `None` when `tenant` has no such source.
    pub fn source(&self, tenant: &str, id: &str) -> Result<Option<SourceRecord>, NodeError> {
        self.sm_reader
            .source(tenant, id)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Every declared spec `tenant` has, id-ascending (RFC-004 S2, #278). Like
    /// [`Self::sources`], this answers from local applied state and needs no leadership.
    pub fn specs(&self, tenant: &str) -> Result<Vec<SpecRecord>, NodeError> {
        self.sm_reader
            .specs(tenant)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// One spec by id, or `None` when `tenant` has no such spec (RFC-004 S2, #278).
    pub fn spec(&self, tenant: &str, id: &str) -> Result<Option<SpecRecord>, NodeError> {
        self.sm_reader
            .spec(tenant, id)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The document stored under `digest`, or `None` if no spec currently holds it (RFC-004 S2,
    /// #278).
    pub fn spec_document(&self, digest: &str) -> Result<Option<String>, NodeError> {
        self.sm_reader
            .spec_document(digest)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// `tenant`'s port `port`'s spec provenance, or `None` when the port holds no imposter or
    /// its imposter is not spec-bound (RFC-004 S2, #278).
    pub fn spec_binding(&self, tenant: &str, port: u16) -> Result<Option<SpecBinding>, NodeError> {
        self.sm_reader
            .spec_binding(tenant, port)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// How many distinct spec documents are currently held, fleet-wide (RFC-004 S2, #278).
    pub fn spec_blob_count(&self) -> Result<usize, NodeError> {
        self.sm_reader
            .spec_blob_count()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Every live dataset version `tenant` holds, name-ascending then version-ascending
    /// (RFC-005 D1, #285). Like [`Self::specs`], this answers from local applied state and
    /// needs no leadership.
    pub fn datasets(&self, tenant: &str) -> Result<Vec<DatasetSummary>, NodeError> {
        self.sm_reader
            .datasets(tenant)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The latest live version of `tenant`'s dataset `name`, or `None` when there is none
    /// (RFC-005 D1, #285).
    pub fn dataset(&self, tenant: &str, name: &str) -> Result<Option<DatasetSummary>, NodeError> {
        self.sm_reader
            .dataset(tenant, name)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The path a dataset blob's csv bytes are (or would be) materialised at, or `None` when
    /// this node has no spool directory attached (RFC-005 D1, #285). Answers regardless of
    /// whether `digest` names anything this node currently holds.
    #[must_use]
    pub fn spool_path(&self, digest: &str) -> Option<std::path::PathBuf> {
        self.sm_reader.spool_path(digest)
    }

    /// The CSV bytes behind `digest` (RFC-005 §5, #287), or `None` when this node holds none.
    pub fn dataset_blob(&self, digest: &str) -> Result<Option<String>, NodeError> {
        self.sm_reader
            .dataset_blob(digest)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// How many live stubs bind each of `tenant`'s datasets, and whether the tally is complete
    /// (RFC-005 §5, #287). One config-table scan for all names.
    pub fn dataset_binding_counts(
        &self,
        tenant: &str,
    ) -> Result<(std::collections::HashMap<String, usize>, bool), NodeError> {
        self.sm_reader
            .dataset_binding_counts(tenant)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// How many distinct dataset documents are currently held, fleet-wide (RFC-005 D1, #285).
    pub fn dataset_blob_count(&self) -> Result<usize, NodeError> {
        self.sm_reader
            .dataset_blob_count()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// `(tenant, port, provenance)` for every source-owned config fleet-wide,
    /// ascending by tenant then port. Fleet-wide and not tenant-scoped on
    /// purpose, like [`Self::configured_ports`]: this backs the same
    /// operator surface, not a tenant-facing one.
    pub fn config_provenance(&self) -> Result<Vec<(TenantId, u16, SourceProvenance)>, NodeError> {
        self.sm_reader
            .config_provenance()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The tenant that owns `port`'s applied config, or `None` if no tenant
    /// has one. **Not O(1)** — see [`RedbStateMachine::owning_tenant`] for
    /// the real cost.
    pub fn owning_tenant(&self, port: u16) -> Result<Option<TenantId>, NodeError> {
        self.sm_reader
            .owning_tenant(port)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The principal record for `id`, or `None` if no such principal exists
    /// (issue #161). Answers from local applied state — authenticating a
    /// request must not require this node to be leader.
    pub fn principal(&self, id: &str) -> Result<Option<Principal>, NodeError> {
        self.sm_reader
            .principal(id)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Every tenant `id` is bound in, with the role for each (RFC-002 §4,
    /// issue #161) — the read `authz::decide` is built on. Like
    /// [`Self::principal`], this answers from local applied state.
    pub fn principal_bindings(&self, id: &str) -> Result<Vec<(TenantId, Role)>, NodeError> {
        self.sm_reader
            .principal_bindings(id)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// One tenant record by id, tombstone included, or `None` when no row
    /// exists (issue #162). Answers from local applied state.
    pub fn tenant(&self, id: &str) -> Result<Option<Tenant>, NodeError> {
        self.sm_reader
            .tenant(id)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Every tenant record, id-ascending, tombstones included (issue #162) —
    /// what `GET /admin/tenants` reports.
    pub fn tenants(&self) -> Result<Vec<Tenant>, NodeError> {
        self.sm_reader
            .tenants()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Every tenant's config-table usage, id-keyed, in one scan (issue #372) —
    /// the imposter/stub half of what `GET /admin/tenants` reports alongside
    /// `quotas`. See [`RedbStateMachine::tenant_config_usage`] for why one scan
    /// serves every tenant.
    pub fn tenant_config_usage(&self) -> Result<HashMap<String, TenantConfigUsage>, NodeError> {
        self.sm_reader
            .tenant_config_usage()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Every principal bound to `tenant` and the role it holds there
    /// (issue #162) — what `GET /admin/tenants/:id/principals` reports.
    pub fn tenant_principals(&self, tenant: &str) -> Result<Vec<(Principal, Role)>, NodeError> {
        self.sm_reader
            .tenant_principals(tenant)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Audit rows at or after `since`, ascending by revision, optionally
    /// narrowed to one tenant (RFC-002 §9, issue #163).
    ///
    /// Answers from local applied state — **no fan-out**. Every replica derives
    /// the same rows from the same log, so any node can answer for the fleet.
    /// (Contrast the M3 request journal, #147, which is per-node and needs
    /// merge-on-read.)
    pub fn audit_since(
        &self,
        since: u64,
        tenant: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AuditRow>, NodeError> {
        self.sm_reader
            .audit_since(since, tenant, limit)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The fleet's declared audit export sink, or `None` (issue #164).
    ///
    /// # Errors
    /// Storage I/O, or a stored sink record that will not parse.
    pub fn audit_sink(&self) -> Result<Option<AuditSink>, NodeError> {
        self.sm_reader
            .audit_sink()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The fleet's session-signing key, or `None` when no console login has minted one yet
    /// (RFC-006 §5.3, issue #185).
    ///
    /// # Errors
    /// Storage I/O, or a stored key record that will not parse.
    pub fn session_key(&self) -> Result<Option<SessionKey>, NodeError> {
        self.sm_reader
            .session_key()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The fleet's operator-set name, or `None` when nobody has named it yet (issue #373).
    ///
    /// # Errors
    /// Storage I/O.
    pub fn fleet_name(&self) -> Result<Option<String>, NodeError> {
        self.sm_reader
            .fleet_name()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The last revision shipped to the audit export sink; `0` when nothing has
    /// shipped (issue #164).
    ///
    /// # Errors
    /// Storage I/O.
    pub fn audit_checkpoint(&self) -> Result<u64, NodeError> {
        self.sm_reader
            .audit_checkpoint()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The highest revision retention GC has removed from the audit table; `0`
    /// if it has never removed anything (issue #164). The exporter's only
    /// evidence that rows were *lost* rather than never written.
    ///
    /// # Errors
    /// Storage I/O.
    pub fn audit_gc_watermark(&self) -> Result<u64, NodeError> {
        self.sm_reader
            .audit_gc_watermark()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// The applied clear generation for `port` (or `port`'s `space`, when given); `0` if
    /// `ControlOp::JournalClearGen` has never committed for that key (issue #224).
    ///
    /// # Errors
    /// Storage I/O.
    pub fn journal_gen(
        &self,
        tenant: &str,
        port: u16,
        space: Option<&str>,
    ) -> Result<u64, NodeError> {
        self.sm_reader
            .journal_gen(tenant, port, space)
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// Whether the fleet has any principal defined at all (RFC-002 §3.4):
    /// governs the legacy-admin-plane bypass and the
    /// `rift_cluster_no_principals` gauge.
    pub fn has_any_principals(&self) -> Result<bool, NodeError> {
        self.sm_reader
            .has_any_principals()
            .map_err(|e| NodeError::Storage(e.to_string()))
    }

    /// This node's current Raft term. Test-facing: the #431 probe asserts a
    /// restarted voter's term never runs ahead of the leader's.
    #[doc(hidden)]
    #[must_use]
    pub fn raft_term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    /// The index of the newest snapshot this node holds, or `None` before it
    /// holds one. Test-facing: #492's joiner probe polls this to measure how
    /// long an `install_snapshot` actually took, so the fixture's margin over
    /// [`ADMIT_CURRENCY_WAIT`](crate::ADMIT_CURRENCY_WAIT) is checked rather
    /// than assumed.
    ///
    /// Reported as a bare index, like [`StatusReport::last_applied`], so a
    /// test never has to name an openraft type.
    #[doc(hidden)]
    #[must_use]
    pub fn snapshot_index(&self) -> Option<u64> {
        self.raft
            .metrics()
            .borrow()
            .snapshot
            .map(|log_id| log_id.index)
    }

    /// The index the log has been purged up to, or `None` if nothing has been
    /// purged. Test-facing: #492's joiner probe polls it to establish that the
    /// log the joiner would otherwise be caught up *from* is gone, so
    /// `install_snapshot` is openraft's only remaining route — the assumption
    /// the whole test rests on, previously left to a fixed sleep.
    #[doc(hidden)]
    #[must_use]
    pub fn purged_index(&self) -> Option<u64> {
        self.raft
            .metrics()
            .borrow()
            .purged
            .map(|log_id| log_id.index)
    }

    /// The index of the last entry in this node's log, or `None` on an empty log. Test-facing
    /// (D-55, #504): the fleet test for rule C has to establish the one state the purge point
    /// cannot see — a replica whose log is ahead of its applied index — and that is this number
    /// against `status().last_applied`.
    #[doc(hidden)]
    #[must_use]
    pub fn last_log_index(&self) -> Option<u64> {
        self.raft.metrics().borrow().last_log_index
    }

    /// The leader's view of each peer's matched log index, sorted by node id —
    /// **empty on a non-leader**, since openraft populates replication metrics
    /// only while leading. Test-facing: #492's failure messages carry it,
    /// because `matching` against the leader's `last_log` is what decides
    /// whether admission sees a joiner as current.
    #[doc(hidden)]
    #[must_use]
    pub fn replication_matching(&self) -> Vec<(NodeId, Option<u64>)> {
        self.raft
            .metrics()
            .borrow()
            .replication
            .as_ref()
            .map(|peers| {
                peers
                    .iter()
                    .map(|(id, matched)| (*id, matched.map(|log_id| log_id.index)))
                    .collect()
            })
            .unwrap_or_default()
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
            // `nodes()` is every member; whatever is in it and not a voter is a learner.
            learners: {
                let voters: std::collections::BTreeSet<NodeId> =
                    metrics.membership_config.voter_ids().collect();
                metrics
                    .membership_config
                    .nodes()
                    .map(|(id, _)| *id)
                    .filter(|id| !voters.contains(id))
                    .collect()
            },
            // Same borrow, same instant, same rule as `is_isolated`.
            isolated: isolated_from(
                self.id,
                metrics.current_leader,
                metrics.millis_since_quorum_ack,
            ),
        }
    }

    /// How many voters the applied membership has.
    ///
    /// Cheaper than [`status`](Self::status) or [`ring`](Self::ring), which
    /// collect the ids into a `Vec` and — in `ring`'s case — sort and dedup
    /// them. Not allocation-free, though: openraft's `voter_ids()` builds a
    /// `BTreeSet` internally, so callers on a per-request path (the journal's
    /// shard cap, issue #222) must still cache the result rather than consult
    /// this per request.
    #[must_use]
    pub fn voter_count(&self) -> usize {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        metrics.membership_config.voter_ids().count()
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
    /// reports isolated. See [`isolated_from`] for the rule itself; this is the
    /// live-metrics reading of it, and [`StatusReport::isolated`] is the sampled
    /// one. Both go through that one function so the condition an operator
    /// alerts on and the condition the write path enforces cannot drift apart.
    #[must_use]
    pub fn is_isolated(&self) -> bool {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        isolated_from(
            self.id,
            metrics.current_leader,
            metrics.millis_since_quorum_ack,
        )
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
        // First, before the core is asked to stop: a fetch parked inside `apply` occupies
        // openraft's state-machine worker, and that worker is what holds the storage handle
        // `await_storage_release` below waits for. Signalling after would wait out a timeout
        // this is meant to prevent; signalling when nothing is parked costs one atomic store.
        // The `Err` is discarded because it means every receiver is gone — the `PeerBlobSource`
        // itself has dropped — so there is no parked fetch left to tell.
        let _ = self.shutdown_signal.send(true);
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
        self.gc_task.abort();
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

    /// Wait until the only remaining handles on the storage database are this node's
    /// own `sm_reader`, i.e. openraft has dropped its log-store and
    /// state-machine clones. Bounded by [`STORAGE_RELEASE_TIMEOUT`] so a stuck teardown
    /// surfaces as an error instead of hanging shutdown.
    async fn await_storage_release(&self) -> Result<(), NodeError> {
        let deadline = tokio::time::Instant::now() + STORAGE_RELEASE_TIMEOUT;
        while self.sm_reader.db_refs() > NODE_HELD_DB_REFS {
            if tokio::time::Instant::now() >= deadline {
                return Err(NodeError::Runtime(format!(
                    "raft core did not release storage within {STORAGE_RELEASE_TIMEOUT:?} \
                     ({} database handles still live); if this node was parked on a blob fetch, \
                     the D-56 shutdown signal did not reach it",
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

/// Drive one join across at most `max_attempts` sends, following a leader
/// redirect between them by calling `send` with the address to try next.
///
/// `max_attempts` counts *sends*, not redirects followed: the first is the seed
/// itself, so the budget allows `max_attempts - 1` hops. Split out from
/// [`RaftNode::join_via`] so that bound is testable without a flapping cluster —
/// the transport is the closure's business, the give-up rule is this function's.
/// A leadership that keeps moving must cost a bounded number of round trips,
/// never a ping-pong between two nodes each naming the other.
async fn chase_join<T, F, Fut>(seed: &str, max_attempts: usize, mut send: F) -> Result<T, NodeError>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<T, RpcError>>,
{
    let mut target = seed.to_owned();
    for attempt in 0..max_attempts {
        // Counted here rather than where the hint arrives: the last hint of an
        // exhausted chase is never acted on, and a counter of "joins forwarded"
        // that includes a forward that never happened is a lie an operator
        // would read as one more round trip than the fleet actually made.
        if attempt > 0 {
            crate::metrics::join_forwarded();
        }
        let error = match send(target.clone()).await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        // Only a named leader is worth another hop. A hintless refusal means an
        // election is unsettled, and there is nowhere better to ask — the
        // caller's own seed loop backs off, which is the right place to wait.
        let Some(next) = next_join_hop(&error) else {
            // Named, because after a hop the failure belongs to the node we
            // were redirected *to*. The caller prefixes the original seed, so
            // without this a leader's failure is reported against the follower
            // that correctly sent us there.
            return Err(NodeError::Membership(format!("{target}: {error}")));
        };
        target = next;
    }
    // Naming the last target separates the two shapes an operator has to tell
    // apart: a genuine flap (a different leader each attempt, cluster unstable)
    // and a cycle (two nodes each naming the other, a real misconfiguration).
    Err(NodeError::Membership(format!(
        "gave up after {max_attempts} attempts while joining via {seed} \
         (last redirected to {target})"
    )))
}

/// Where to re-issue a join after a failed attempt, or `None` to stop.
///
/// Only a [`RpcError::NotLeader`] that actually names someone is worth another
/// hop. A hintless one means an election is in flight: there is no better node
/// to ask, and looping here would spin inside a single join attempt instead of
/// letting the caller's seed loop back off, which is where waiting belongs.
fn next_join_hop(error: &RpcError) -> Option<String> {
    match error {
        RpcError::NotLeader { leader } => leader.clone(),
        _ => None,
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
        self.gc_task.abort();
        // Also here, not only in `shutdown()`: a parked fetch would otherwise keep the storage
        // handle alive indefinitely after a plain drop, which is the one case
        // `drop_without_shutdown_eventually_releases_storage` could not have covered. `Err` is
        // discarded for the reason it is in `shutdown()`: no receivers left means nothing parked.
        let _ = self.shutdown_signal.send(true);
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
    use crate::control::DEFAULT_TENANT;
    use tempfile::TempDir;

    const SECRET: &str = "cluster-test-secret";

    // ---- sideload capability (#481) --------------------------------------

    /// The pure decision rule `fan_out_then_submit` strips bytes on, exercised on literal sets
    /// so this stays independent of any live fan-out. Every branch the doc comment claims:
    /// every member capable and nobody skewed is safe; missing even one member's capability is
    /// not; a skewed member (however few) is never safe even with full capability coverage;
    /// and an empty membership is `false`, not vacuously `true`.
    #[test]
    fn sideload_safe_needs_every_member_observed_capable() {
        let members = BTreeSet::from([1, 2, 3]);
        let empty: BTreeSet<NodeId> = BTreeSet::new();

        assert!(
            sideload_safe(&members, &BTreeSet::from([1, 2, 3]), &empty),
            "every member capable, nobody skewed: safe"
        );
        assert!(
            !sideload_safe(&members, &BTreeSet::from([1, 2]), &empty),
            "member 3's capability was never confirmed: not safe"
        );
        assert!(
            !sideload_safe(&members, &BTreeSet::from([1, 2, 3]), &BTreeSet::from([2])),
            "member 2 is skewed even though everyone answered capable: not safe"
        );
        assert!(
            !sideload_safe(&BTreeSet::new(), &BTreeSet::from([1, 2, 3]), &empty),
            "an empty membership cannot be evaluated, so it must not read as safe"
        );
    }

    /// Pins D-53's eviction rule: an explicit `false` observed **now** must beat a `true`
    /// remembered earlier. Without it the set is grow-only, and a machine replaced but rejoining
    /// under the same operator-chosen node id — from an older image — inherits the previous
    /// build's capability and gets handed a stripped entry its log store cannot decode, stopping
    /// its Raft core. That is precisely the wedge #481 exists to prevent, reached through the
    /// mechanism meant to prevent it.
    #[test]
    fn an_observed_incapable_evicts_a_remembered_capable() {
        let members = BTreeSet::from([1, 2, 3]);
        let mut capable = BTreeSet::from([1, 2, 3]);

        // Node 3 was replaced in place and now answers `false`.
        remember_capability(
            &mut capable,
            &BTreeSet::from([1, 2]),
            &BTreeSet::from([3]),
            &members,
        );

        assert_eq!(capable, BTreeSet::from([1, 2]));
        assert!(
            !sideload_safe(&members, &capable, &BTreeSet::new()),
            "a member that just said it cannot apply digest-only ops must block the strip"
        );
    }

    /// The reverse must still hold, or the eviction would have turned the gate into a permanent
    /// off switch: a member that answers `true` is remembered.
    #[test]
    fn an_observed_capable_is_remembered_across_fan_outs() {
        let members = BTreeSet::from([1, 2]);
        let mut capable = BTreeSet::new();

        remember_capability(&mut capable, &members, &BTreeSet::new(), &members);
        // A later fan-out observes nobody (say every peer was briefly unreachable).
        remember_capability(&mut capable, &BTreeSet::new(), &BTreeSet::new(), &members);

        assert_eq!(
            capable, members,
            "a positive observation survives a silent round"
        );
        assert!(sideload_safe(&members, &capable, &BTreeSet::new()));
    }

    /// Pins the other half of #481: a member observed capable is remembered, but only for as
    /// long as it is still part of the membership. Once it departs, `prune_sideload_capable`
    /// drops it — a departed member's id must not occupy the persisted set forever.
    #[test]
    fn a_member_that_left_is_forgotten() {
        let mut capable = BTreeSet::from([1, 2, 3]);
        prune_sideload_capable(&mut capable, &BTreeSet::from([1, 2]));
        assert_eq!(
            capable,
            BTreeSet::from([1, 2]),
            "member 3 left the membership and must be forgotten, not just ignored"
        );
    }

    fn config_in(dir: &TempDir, id: NodeId) -> NodeConfig {
        NodeConfig {
            node_id: id,
            bind: "127.0.0.1:0".parse().expect("valid bind addr"),
            advertise: None,
            data_dir: dir.path().to_path_buf(),
            secret: Some(SECRET.to_owned()),
            routes: Router::new(),
            engine: None,
            audit_retention_secs: crate::raft::store::DEFAULT_AUDIT_RETENTION_SECS,
            snapshot_log_entries: None,
            advertise_as_digest_only_incapable: false,
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
                .get_imposter(DEFAULT_TENANT, port)
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
        assert_eq!(node.get_imposter(DEFAULT_TENANT, 9999).unwrap(), None);
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
            ring.i_own(1, OwnedKey::flow("flow-8080")),
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

    /// The primitive behind `--cluster-write-barrier none` (#99): a node must
    /// be able to wait for *its own* apply without consulting a peer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn await_local_applied_confirms_the_write_it_committed() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 7)).await.expect("start");
        node.cluster_init().await.expect("cluster init");

        let rev = node
            .put_imposter(imposter(8080, "local-apply"))
            .await
            .expect("write")
            .revision;

        assert!(
            node.await_local_applied(rev, Duration::from_secs(5)).await,
            "the node that committed revision {rev} must be able to confirm \
             its own apply of it"
        );
        assert!(
            node.status().last_applied.is_some_and(|a| a >= rev),
            "await_local_applied returned true before the apply landed"
        );

        node.shutdown().await.expect("shutdown");
    }

    /// The honest-failure half: an index that will never arrive must time out
    /// and say so, not hang and not report success. `admin_front` relies on
    /// this to fall through to surfacing the re-read's real status.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn await_local_applied_times_out_on_an_index_that_never_lands() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 8)).await.expect("start");
        node.cluster_init().await.expect("cluster init");

        let unreachable = node.status().last_applied.unwrap_or(0) + 10_000;
        let started = tokio::time::Instant::now();
        assert!(
            !node
                .await_local_applied(unreachable, Duration::from_millis(250))
                .await,
            "an index no write will produce must report unconfirmed"
        );
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "await_local_applied overran its own timeout"
        );

        node.shutdown().await.expect("shutdown");
    }

    /// Pins D-16: the Raft log lives in redb with `Durability::Immediate`, so a
    /// committed config is still applied after the node reopens its store.
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
                node.get_imposter(DEFAULT_TENANT, 8080)
                    .unwrap()
                    .and_then(|b| name_of(&b)),
                Some("durable-body".to_owned())
            );
            node.shutdown().await.expect("shutdown");
            rev
        };

        let node = RaftNode::start(config_in(&dir, 1)).await.expect("restart");
        assert_eq!(
            node.get_imposter(DEFAULT_TENANT, 8080)
                .unwrap()
                .and_then(|b| name_of(&b)),
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
    /// `sm_reader`. On the old shutdown, which returned before the core
    /// wound down, this count could still be above [`NODE_HELD_DB_REFS`] until the core
    /// caught up — the window the #41 restart raced. Here it is the guaranteed
    /// postcondition.
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

    /// Pins D-15: membership is a Raft-log value — a node admitted through a
    /// seed join is a voter on every member's view, and a config committed on
    /// the leader is applied byte-identically on the joiners.
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
    ///
    /// Pins D-26: only a real departure may be recorded — `Retained` is the
    /// outcome the `departed` marker must never be written for.
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

    /// Pins D-17: a routine election isolates a node for the election **round trip**, not for the
    /// election *timeout*. `current_leader` is `None` only while this node's vote is uncommitted —
    /// from campaigning or granting a vote until the winner's first `AppendEntries` — which is why
    /// the measured pause is ~13–40 ms and not the sub-second figure D-17 used to state (#472).
    ///
    /// Its job is to catch a future openraft that clears `current_leader` at **lease expiry**
    /// instead of at campaign: that would silently grow the pause to one election timeout
    /// (150–300 ms) and invalidate the numbers D-17 now records, with no other test noticing.
    ///
    /// The 400 ms bound is deliberately loose against a measured ~13–40 ms (and a ≤ 300 ms
    /// split-vote round) so it does not flake on CI's 2-vCPU runners. Red-confirmed by setting the
    /// bound to zero: this observes a real 32–40 ms window locally, so it is measuring the pause
    /// rather than passing on a window it never sees. Sampling stalls are the one
    /// way this could report an isolation it never observed, so the failure message carries the
    /// largest gap between consecutive samples — a run that failed because the sampler was starved
    /// says so, instead of looking like a real regression.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_routine_election_isolates_a_node_for_the_round_trip_not_the_timeout() {
        use std::time::{Duration, Instant};

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

        // Start from a genuinely healthy fleet, or "isolated" below would just be "never settled".
        assert!(
            wait_until(|| n1.status().is_leader
                && !n1.is_isolated()
                && !n2.is_isolated()
                && !n3.is_isolated())
            .await,
            "the cluster must be healthy before the leader is killed"
        );

        n1.shutdown().await.ok();

        // Sample both survivors until each knows a new leader, then settle briefly.
        let survivors = [(2u64, &n2), (3u64, &n3)];
        let mut longest = [Duration::ZERO; 2];
        let mut run_start: [Option<Instant>; 2] = [None, None];
        let mut widest_gap = Duration::ZERO;
        let mut last_sample = Instant::now();

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut settled_at: Option<Instant> = None;
        while Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let now = Instant::now();
            widest_gap = widest_gap.max(now - last_sample);
            last_sample = now;

            for (i, (_, node)) in survivors.iter().enumerate() {
                if node.is_isolated() {
                    run_start[i].get_or_insert(now);
                } else if let Some(started) = run_start[i].take() {
                    longest[i] = longest[i].max(now - started);
                }
            }

            let new_leader_known = survivors
                .iter()
                .all(|(_, node)| matches!(node.status().current_leader, Some(id) if id != 1));
            match (new_leader_known, settled_at) {
                (true, None) => settled_at = Some(now + Duration::from_millis(200)),
                (true, Some(at)) if now >= at => break,
                (false, _) => settled_at = None,
                _ => {}
            }
        }
        // Close any run still open at the end so it is not silently dropped.
        let end = Instant::now();
        for i in 0..2 {
            if let Some(started) = run_start[i].take() {
                longest[i] = longest[i].max(end - started);
            }
        }

        for (i, (id, _)) in survivors.iter().enumerate() {
            assert!(
                longest[i] < Duration::from_millis(400),
                "node {id} was isolated for {:?} across one routine election; D-17 records ~13–40 ms \
                 and this bound is 400 ms. Largest gap between consecutive samples was {:?} — if \
                 that approaches the reported isolation the sampler was starved and this is a CI \
                 load artefact, not a regression.",
                longest[i],
                widest_gap
            );
        }

        for node in [&n2, &n3] {
            node.shutdown().await.ok();
        }
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
        // The premise — two voters survive the leader — must be established,
        // not assumed: a joiner still a learner at the kill cannot elect.
        assert_eq!(
            wait_voters(&n1, &BTreeSet::from([1, 2, 3])).await,
            BTreeSet::from([1, 2, 3]),
            "three voters must form before the leader is killed"
        );

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

        // `is_leader` above was a *past* observation, and the write below is a
        // present one. A node that has just won an election has not yet been
        // acknowledged by a quorum, so `put_imposter` can legitimately answer
        // `NotLeader { leader: None }` in the gap — the same past-state-as-present
        // reading that produced #430-#433. Wait for the lease this node already
        // knows how to report rather than assuming the sample still holds.
        assert!(
            wait_until(|| !leader.is_isolated()).await,
            "the elected leader must reach a quorum lease before it can commit"
        );

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

        // Formation is bounded, not instantaneous (#433): a joiner that
        // missed the in-call currency window is promoted by the sweep.
        wait_voters(&n1, &BTreeSet::from([1, 2, 3])).await;
        let voters = n1.status().voters;
        assert!(
            voters.contains(&2) && voters.contains(&3),
            "both joiners must be promoted to voter, got {voters:?}"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
        n3.shutdown().await.ok();
    }

    /// #391: a joiner whose only seed is a healthy *follower* must still join.
    ///
    /// openraft answers `add_learner` on a non-leader with `ForwardToLeader`,
    /// naming the leader. Before the fix that hint was flattened into an
    /// `RpcError::Handler` string, so the joiner retried the same follower until
    /// `SEED_JOIN_DEADLINE` and died with the leader's address sitting unused in
    /// the error text. This is the operator-facing shape too: `--cluster-seeds`
    /// pointing at one stable member is the obvious thing to configure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_node_seeded_at_a_follower_joins_by_chasing_the_leader() {
        let (d1, d2, d3) = (
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        );
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
        n2.join_via(n1.advertise())
            .await
            .expect("n2 join via leader");
        wait_voters(&n1, &BTreeSet::from([1, 2])).await;

        // Asserted, not assumed: if leadership happened to move to n2 the join
        // below would succeed locally and prove nothing at all.
        let seed_status = n2.status();
        assert!(
            !seed_status.is_leader,
            "the seed must be a follower for this gate to mean anything"
        );
        assert_eq!(
            seed_status.current_leader,
            Some(1),
            "n1 must still hold leadership when n3 seeds at n2"
        );

        let n3 = RaftNode::start(config_in(&d3, 3)).await.expect("start n3");
        n3.join_via(n2.advertise())
            .await
            .expect("a node seeded at a follower must chase the leader and join");

        let voters = n1.status().voters;
        assert!(
            voters.contains(&3),
            "the follower-seeded joiner must reach the leader and be promoted, got {voters:?}"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
        n3.shutdown().await.ok();
    }

    /// #391: the classifier that decides whether a failed admit is worth
    /// re-issuing elsewhere. Only a `NotLeader` carrying an address is — a
    /// hintless `NotLeader` (election unsettled) and every other error must stop
    /// the chase so the caller's outer seed loop can back off instead of
    /// spinning inside one join attempt.
    #[test]
    fn only_a_named_leader_redirects_the_join() {
        use crate::rpc::RpcError;

        assert_eq!(
            next_join_hop(&RpcError::NotLeader {
                leader: Some("10.0.0.7:7000".into()),
            }),
            Some("10.0.0.7:7000".to_owned()),
        );
        assert_eq!(
            next_join_hop(&RpcError::NotLeader { leader: None }),
            None,
            "an unsettled election names nobody; retrying here would spin"
        );
        assert_eq!(
            next_join_hop(&RpcError::Handler("add learner: boom".into())),
            None,
            "a genuine handler failure is not a redirect"
        );
        assert_eq!(next_join_hop(&RpcError::Timeout), None);
    }

    /// #391: a leadership that never settles must cost a bounded number of
    /// round trips. Two nodes each naming the other is the shape that would
    /// otherwise ping-pong forever inside a single join attempt, with the
    /// caller's own deadline the only thing stopping it.
    #[tokio::test]
    async fn a_join_gives_up_after_a_bounded_number_of_redirects() {
        use crate::rpc::RpcError;
        use std::cell::RefCell;

        let seen = RefCell::new(Vec::new());
        // Every hop redirects, and the two addresses point at each other.
        let err = chase_join::<(), _, _>("a:1", RaftNode::FORWARD_ATTEMPTS, |target| {
            seen.borrow_mut().push(target.clone());
            async move {
                Err(RpcError::NotLeader {
                    leader: Some(if target == "a:1" { "b:2" } else { "a:1" }.to_owned()),
                })
            }
        })
        .await
        .expect_err("an endless redirect must not be followed forever");

        assert_eq!(
            seen.borrow().len(),
            RaftNode::FORWARD_ATTEMPTS,
            "exactly {} attempts, no more and no fewer",
            RaftNode::FORWARD_ATTEMPTS
        );
        let message = err.to_string();
        assert!(
            message.contains("last redirected to"),
            "giving up must name where the chase ended, so a flap and a cycle \
             can be told apart: {message}"
        );
    }

    /// #391: a hop that succeeds ends the chase immediately — the bound is a
    /// ceiling, not a number of attempts to make.
    #[tokio::test]
    async fn a_join_stops_at_the_first_hop_that_succeeds() {
        use crate::rpc::RpcError;
        use std::cell::RefCell;

        let calls = RefCell::new(0_usize);
        chase_join("a:1", RaftNode::FORWARD_ATTEMPTS, |_target| {
            let n = {
                let mut c = calls.borrow_mut();
                *c += 1;
                *c
            };
            async move {
                if n == 1 {
                    Err(RpcError::NotLeader {
                        leader: Some("b:2".to_owned()),
                    })
                } else {
                    Ok(())
                }
            }
        })
        .await
        .expect("the second hop succeeds");

        assert_eq!(*calls.borrow(), 2, "one redirect, then done");
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

            wait_voters(&n1, &BTreeSet::from([1, 2, 3])).await;
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
    ///
    /// Pins D-27: the auto-voter ceiling is exact for what the fleet does on
    /// its own — the committed voter set never exceeds it, and the node that
    /// loses the race is still admitted as a learner.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_admissions_never_exceed_the_ceiling() {
        for round in 0..3 {
            let (d1, d2, d3) = (
                TempDir::new().unwrap(),
                TempDir::new().unwrap(),
                TempDir::new().unwrap(),
            );
            let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
            // The ceiling is a node property read by both admission phases
            // (#433), so it is set on the node and the joins go through the
            // real handler: driving `admit` directly with a private ceiling
            // would leave the node's own promotion sweep bound by a different
            // number, and the sweep would then "exceed" a ceiling it was
            // never given.
            n1.set_auto_voter_ceiling(2);
            n1.cluster_init().await.expect("init n1");
            let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");
            let n3 = RaftNode::start(config_in(&d3, 3)).await.expect("start n3");

            let seed = n1.advertise();
            let (r2, r3) = tokio::join!(n2.join_via(seed), n3.join_via(seed));
            r2.unwrap_or_else(|e| {
                panic!("round {round}: losing the promotion race must not fail the join: {e}")
            });
            r3.unwrap_or_else(|e| {
                panic!("round {round}: losing the promotion race must not fail the join: {e}")
            });

            // Watched across several promotion-sweep ticks, not sampled once:
            // the invariant is that the committed voter set *never* exceeds
            // the ceiling — in-call promotion and the sweep together — and
            // that it reaches it.
            let watch_until =
                tokio::time::Instant::now() + RaftNode::LEARNER_PROMOTION_INTERVAL * 3;
            let mut reached = false;
            while tokio::time::Instant::now() < watch_until {
                let voters = n1.status().voters;
                assert!(
                    voters.len() <= 2,
                    "round {round}: auto-promotion exceeded the ceiling: {voters:?}"
                );
                reached |= voters.len() == 2;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let voters = n1.status().voters;
            assert!(
                reached && voters.len() == 2,
                "round {round}: the ceiling must be reached and held, got {voters:?}"
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
        wait_voters(&n1, &BTreeSet::from([1, 2])).await;
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

    /// Pins D-55 (#504), fail closed: a member the floor probe cannot reach is **named** in
    /// `unknown` and the floor is `None` — never the minimum over whoever did answer. Counting
    /// only the reachable members would make the sweep reap exactly the blob the unreachable one,
    /// parked and restarting, is about to replay a `PUT` for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fleet_applied_floor_names_a_member_that_does_not_resolve_and_reports_no_floor() {
        let (n1, n2, _dirs) = cluster_with_unresolvable_leader().await;

        let floor = network::fleet_applied_floor(
            &n2.raft,
            &n2.client,
            &n2.resolver,
            Duration::from_millis(500),
        )
        .await
        .expect("membership is readable");

        assert_eq!(
            floor,
            network::FleetAppliedFloor::Unknown(vec![1]),
            "one member unknown means no floor at all, and that member is named — only that one"
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
    }

    /// Pins D-55 (#504): the per-member budget bounds the *round trip*, not just resolution. A
    /// member that accepts the connection and never answers costs the sweep one budget and is
    /// named unknown — without the bound, the RPC client's own timeout and retries would hold
    /// every sweep for several times longer, per hung member.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fleet_applied_floor_bounds_a_member_that_accepts_but_never_answers() {
        // A listener that accepts every connection and then holds it open, saying nothing.
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a silent listener");
        let silent_addr = silent.local_addr().expect("silent addr");
        let hold = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = silent.accept().await {
                held.push(stream);
            }
        });

        // Same shape as `cluster_with_unresolvable_leader`: the leader binds a real port but
        // advertises the silent one, so every probe from the follower dials a socket that hangs.
        let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let port = reserved_port();
        let mut c1 = config_in(&d1, 1);
        c1.bind = format!("127.0.0.1:{port}").parse().expect("bind addr");
        c1.advertise = Some(
            silent_addr
                .to_string()
                .parse::<Authority>()
                .expect("a literal authority"),
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
        wait_voters(&n1, &BTreeSet::from([1, 2])).await;

        let budget = Duration::from_millis(300);
        let asked = tokio::time::Instant::now();
        let floor = network::fleet_applied_floor(&n2.raft, &n2.client, &n2.resolver, budget)
            .await
            .expect("membership is readable");
        let took = asked.elapsed();

        assert_eq!(floor, network::FleetAppliedFloor::Unknown(vec![1]));
        assert!(
            took < Duration::from_millis(1500),
            "the probe must return within about one budget ({budget:?}), took {took:?} — \
             the RPC client's own timeout is {:?} per attempt, retried",
            crate::rpc::DEFAULT_REQUEST_TIMEOUT
        );

        hold.abort();
        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
    }

    /// Pins D-55 (#504): a node is a member of its own fleet and answers from its own metrics —
    /// no RPC to itself. On a single-node fleet the floor is exactly this node's applied index.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fleet_applied_floor_of_a_single_node_is_its_own_applied_index() {
        let dir = TempDir::new().expect("tempdir");
        let node = RaftNode::start(config_in(&dir, 1)).await.expect("start");
        node.cluster_init().await.expect("cluster init");
        let revision = node
            .put_imposter(imposter(8080, "alone"))
            .await
            .expect("write")
            .revision;

        let floor = network::fleet_applied_floor(
            &node.raft,
            &node.client,
            &node.resolver,
            Duration::from_millis(500),
        )
        .await
        .expect("membership is readable");

        let applied = node.status().last_applied.expect("the write was applied");
        assert_eq!(
            floor,
            network::FleetAppliedFloor::Known(applied),
            "the only member is this node, so the floor is its own applied index"
        );
        assert!(
            applied >= revision,
            "and that index has at least reached the write just applied ({revision})"
        );

        node.shutdown().await.ok();
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
        let (index_after_first, _, _) = committed_membership(&n1.raft).await;

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

        let (index_after_second, _, _) = committed_membership(&n1.raft).await;
        assert_eq!(
            index_after_second, index_after_first,
            "the second eviction must not append another membership entry"
        );

        for node in [&n1, &n2, &n3] {
            node.shutdown().await.ok();
        }
    }

    /// Reads the *committed* membership inside the RaftCore loop — the log index,
    /// the voter set and the full member set in one pass.
    ///
    /// Committed rather than `metrics()`/`status()` on purpose: the metrics watch
    /// lags the state it mirrors and reports *effective* membership, so an
    /// assertion built on it is honest diagnostics, not a guarantee — the soft
    /// spot a reviewer flagged on #495.
    /// Ordered `(index, members, voters)` to match
    /// `network::committed_members_and_voters`' `(members, voters)`. Both sets
    /// are `BTreeSet<NodeId>`, so a transposition between the two would compile
    /// silently.
    async fn committed_membership(
        raft: &Raft<TypeConfig>,
    ) -> (Option<u64>, BTreeSet<NodeId>, BTreeSet<NodeId>) {
        raft.with_raft_state(|state| {
            let committed = state.membership_state.committed();
            (
                committed.log_id().as_ref().map(|l| l.index),
                committed
                    .nodes()
                    .map(|(id, _)| *id)
                    .collect::<BTreeSet<_>>(),
                committed.voter_ids().collect::<BTreeSet<_>>(),
            )
        })
        .await
        .expect("read committed membership")
    }

    /// Pins D-59: a voter departs by one `RemoveVoters(retain = false)`, so no
    /// committed membership ever presents it as a learner — which is the
    /// promotion sweep's whole criterion (#496).
    ///
    /// Demote-then-remove left the departing node a caught-up learner that was
    /// still a member between its two halves, which is exactly what
    /// `promote_ready_learners` promotes; #495 observed the sweep voting such a
    /// node back in (`voters=[1, 2, 3]`) after a widened takeover window. The fix
    /// is structural rather than a new interlock, so the test is too: it samples
    /// committed membership throughout a real departure and asserts the learner
    /// state never exists at all.
    ///
    /// Two assertions do the discriminating, and both distinguish the shapes
    /// rather than merely describing the new one:
    ///
    /// - **no learner sample.** Under demote-then-remove the committed uniform
    ///   config after the demote holds the leaver in `nodes()` and out of
    ///   `voter_ids()` — a learner — across a whole further membership round
    ///   trip, so a sampler catches it.
    /// - **exactly two membership entries.** One `RemoveVoters` commits a joint
    ///   then a uniform entry; demote-then-remove committed three (joint, uniform,
    ///   then `RemoveNodes`). Deterministic, and it fails on any re-promotion,
    ///   which costs two more.
    ///
    /// The sweep runs here at full speed against the **default** ceiling — no
    /// `set_auto_voter_ceiling` pin — so it is willing to promote throughout.
    /// That pin is what #495 had to add to this area, and removing the need for
    /// it is the point of the change.
    ///
    /// It does **not** exercise the sweep's scan/gate window: the leaver is a
    /// voter for this test's whole duration, so the sweep short-circuits on
    /// `voters.contains(&id)` and never reaches the membership re-check. That
    /// window needs a *learner* candidate and is pinned separately by
    /// `the_sweep_skips_a_candidate_removed_between_its_scan_and_its_gate`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_departing_voter_is_never_a_learner_in_committed_membership() {
        use crate::raft::network::{self, EvictOutcome, MAX_AUTO_VOTERS};

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

        let (index_before, _, _) = committed_membership(&n1.raft).await;

        let stop = Arc::new(AtomicBool::new(false));
        let learner_samples = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let sweep_errors = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

        // Samples committed membership without taking the gate, so it observes
        // states the departure passes through rather than only its endpoints.
        let sampler = {
            let raft = n1.raft.clone();
            let (stop, samples) = (Arc::clone(&stop), Arc::clone(&learner_samples));
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let (index, voters, members) = committed_membership(&raft).await;
                    if members.contains(&3) && !voters.contains(&3) {
                        samples.lock().expect("sampler lock").push(format!(
                            "committed index={index:?} voters={voters:?} members={members:?}"
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        };

        // The sweep, run far faster than its 1 s production cadence so it lands
        // inside the departure instead of around it.
        let sweep = {
            let raft = n1.raft.clone();
            let gate = Arc::clone(&n1.membership_gate);
            let (stop, errors) = (Arc::clone(&stop), Arc::clone(&sweep_errors));
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    if let Err(e) =
                        network::promote_ready_learners(&raft, &gate, MAX_AUTO_VOTERS).await
                    {
                        // A leadership move would make this legitimate; nothing in
                        // this test moves leadership, so it is not filtered away.
                        errors.lock().expect("sweep lock").push(e.to_string());
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        };

        assert_eq!(
            network::evict(&n1.raft, &n1.membership_gate, 3)
                .await
                .expect("evict the departing voter"),
            EvictOutcome::Removed
        );

        stop.store(true, Ordering::Relaxed);
        sampler.await.expect("sampler task");
        sweep.await.expect("sweep task");

        let samples = learner_samples.lock().expect("sampler lock").clone();
        assert!(
            samples.is_empty(),
            "the departing node was a learner in committed membership — the promotion sweep's \
             exact criterion (#496). Samples: {samples:?}"
        );

        let errors = sweep_errors.lock().expect("sweep lock").clone();
        assert!(
            errors.is_empty(),
            "a departure must not make the concurrent sweep fail: {errors:?}"
        );

        let (index_after, members_after, voters_after) = committed_membership(&n1.raft).await;
        assert!(
            !members_after.contains(&3),
            "the departed node must be out of committed membership: members={members_after:?}"
        );
        assert_eq!(
            voters_after,
            BTreeSet::from([1, 2]),
            "the survivors keep the quorum"
        );
        assert_eq!(
            index_after.zip(index_before).map(|(a, b)| a - b),
            Some(2),
            "a voter's departure is one `RemoveVoters`: a joint entry then a uniform one, and \
             nothing else. Three means demote-then-remove is back; more means the sweep wrote \
             entries nobody asked for. This is a *log*-index delta, exact only because nothing \
             else writes in this fixture. before={index_before:?} after={index_after:?}"
        );

        for node in [&n1, &n2, &n3] {
            node.shutdown().await.ok();
        }
    }

    /// Issue #71: a departure is completed — or found already complete — by
    /// whoever leads next, and the retry costs nothing.
    ///
    /// `leave_inner` retries the whole sequence against whichever node leads now,
    /// so `evict` running against an already-departed node on a *different*
    /// leader is the normal case, not an edge one. It must return promptly and
    /// append nothing.
    ///
    /// This test used to construct a genuinely half-finished departure by calling
    /// `demote_voter` on its own and asserting the demote half no-op'd. Under
    /// D-59 that state does not exist: a voter departs by one
    /// `RemoveVoters(retain = false)`, whose two entries openraft commits inside a
    /// single `change_membership` call, so a departure is observable only as
    /// not-started or done. The residual half-finished state — joint committed,
    /// uniform not — needs the leader to die *between* those two internal steps,
    /// which no public API can stage; what makes it safe is argued from
    /// openraft's coherence rule and pinned by
    /// `a_departing_voter_is_never_a_learner_in_committed_membership`, since in
    /// that state the leaver is still a voter and so still invisible to the sweep.
    ///
    /// The auto-voter ceiling is deliberately **not** pinned here. #495 had to pin
    /// it in this test because the sweep re-promoted the departing node during the
    /// takeover window (#496); if that pin is ever needed again, the structural
    /// fix has regressed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn evict_completes_from_a_new_leader() {
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
                .expect("the departure lands on the original leader"),
            EvictOutcome::Removed
        );
        let survivors = BTreeSet::from([1, 2]);
        assert_eq!(
            wait_voters(&n1, &survivors).await,
            survivors,
            "the departure must land before leadership moves"
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

        // The property `leave_inner`'s retry rests on, asserted on the entry it
        // must not write. An unguarded `evict` submits a legal membership change
        // that commits fine and leaves an identical final state — the only
        // visible difference is the entry, so that is what gets asserted.
        let (index_before, _, _) = committed_membership(&n2.raft).await;
        let retried = tokio::time::timeout(
            Duration::from_secs(1),
            network::evict(&n2.raft, &n2.membership_gate, 3),
        )
        .await
        .expect("a retried departure must return promptly, not submit and wait")
        .expect("a retried departure is not an error");
        assert_eq!(
            retried,
            EvictOutcome::Removed,
            "already gone is still removed"
        );

        let (index_after, members_after, _) = committed_membership(&n2.raft).await;
        assert_eq!(
            index_after, index_before,
            "the retry from a new leader must append no membership entry — without that guard \
             every retried departure a flaky network produces writes to the log"
        );
        assert!(
            !members_after.contains(&3),
            "the departed node must stay out of membership: members={members_after:?}"
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

    /// Pins the sweep's under-gate re-check at its call site (#496), which the
    /// predicate's own unit test cannot reach.
    ///
    /// `promote_ready_learners` scans members, then takes the gate per candidate.
    /// A departure can commit in that interval, and judged on the voter set alone
    /// the vanished node still looks promotable — it is not a voter, and the set
    /// is under the ceiling — so the sweep submits `AddVoterIds` for a node that
    /// is no longer a learner. openraft answers `LearnerNotFound`, and `?` turns
    /// that into a skipped tick for every candidate behind it: one departure
    /// quietly stalling unrelated promotions.
    ///
    /// The interval is held open rather than raced for. The test takes the gate
    /// itself, lets the sweep block on it *after* its scan, removes the candidate
    /// through a direct membership change, and only then releases — so the sweep
    /// is guaranteed to re-read a membership its scan did not see. Without the
    /// membership half of `still_promotable` this returns `LearnerNotFound`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_sweep_skips_a_candidate_removed_between_its_scan_and_its_gate() {
        use crate::raft::network::{self, MAX_AUTO_VOTERS};
        use openraft::ChangeMembers;

        let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let n1 = RaftNode::start(config_in(&d1, 1)).await.expect("start n1");
        n1.cluster_init().await.expect("init n1");
        let n2 = RaftNode::start(config_in(&d2, 2)).await.expect("start n2");

        // The node's own promotion loop is pinned out of the way so the only
        // sweep in play is the one this test drives, on the gate it owns.
        n1.set_auto_voter_ceiling(1);
        let gate: network::MembershipGate = Arc::new(tokio::sync::Mutex::new(()));
        network::admit(&n1.raft, &gate, 2, n2.advertise().to_string(), 1)
            .await
            .expect("admit as learner");
        assert_eq!(n1.status().voters, vec![1], "n2 must be a learner");

        // A replicated write proves n2 is caught up, so the sweep gets past
        // `is_catching_up` and actually reaches the gate.
        n1.put_imposter(imposter(8080, "sweep-race"))
            .await
            .expect("leader write");
        assert!(
            wait_config(&n2, 8080, "sweep-race").await,
            "the learner must be caught up before the sweep will consider it"
        );

        let held = gate.lock().await;

        // Passes the full ceiling, so the sweep *wants* to promote n2 and is
        // stopped only by the membership re-check under the gate.
        let sweep = {
            let (raft, gate) = (n1.raft.clone(), Arc::clone(&gate));
            tokio::spawn(async move {
                network::promote_ready_learners(&raft, &gate, MAX_AUTO_VOTERS).await
            })
        };
        // Long enough for the scan to complete and the task to park on the gate.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // The departure, submitted directly rather than through `evict` — `evict`
        // would take the gate this test is holding.
        n1.raft
            .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([2])), false)
            .await
            .expect("remove the candidate while the sweep is parked on the gate");

        drop(held);

        sweep.await.expect("sweep task").expect(
            "a candidate removed between the scan and the gate must be skipped, not fail \
                     the tick for every candidate behind it",
        );

        n1.shutdown().await.ok();
        n2.shutdown().await.ok();
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
    ///
    /// Pins D-27: beyond `MAX_AUTO_VOTERS` a node stays a learner, and a
    /// promotion never evicts an existing voter to make room.
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
        // deliberately not NODE_HELD_DB_REFS, which counts what a *live* node holds of its
        // own (`sm_reader`); once `node` is fully dropped here, it
        // survives it, leaving only the pin.
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

    /// #411 pins the timers the single-flight fix deliberately did NOT change.
    ///
    /// The rejected fix for #411 was to raise `heartbeat_interval` so a big
    /// entry fits one round trip. That would have forced `election_timeout_min`
    /// above it and stretched elections (and the isolated-owner window, which is
    /// `3 x election_max`) by an order of magnitude — trading failover latency
    /// for upload size, against ADR-001's "~1-3 s" elections. The fix went into
    /// the network adapter instead, so these three numbers must stay put; if a
    /// later change moves them, that trade is being made silently.
    #[test]
    fn raft_config_pins_the_replication_timers() {
        let config = RaftNode::raft_config(None).expect("default config validates");
        assert_eq!(
            config.heartbeat_interval, 50,
            "heartbeat_interval is openraft's AppendEntries RPC ceiling; #411 is fixed in the \
             network adapter precisely so this does not have to move"
        );
        assert_eq!(
            config.election_timeout_min, 150,
            "election_timeout_min must stay 3x the heartbeat"
        );
        assert_eq!(
            config.election_timeout_max, 300,
            "election_timeout_max must stay 6x the heartbeat; the isolated-owner window is 3x it"
        );
    }

    /// Issue #183 AC1: with no knob set, the openraft config is byte-identical to what it was
    /// before the knob existed.
    ///
    /// Asserted against openraft's **own** defaults rather than against literals, so the test keeps
    /// meaning across an openraft bump: if a future version changes either default, this still says
    /// "we did not override it", which is the actual claim. A shipped fleet only ever takes this
    /// path — `snapshot_log_entries` is set by nothing but the chaos overlay.
    ///
    /// Pins D-43: the knob is unset by every shipped configuration, so the default must be
    /// byte-identical to openraft's own snapshot policy and purge threshold.
    #[test]
    fn raft_config_default_leaves_the_snapshot_knobs_untouched() {
        let defaults = Config::default();
        let ours = RaftNode::raft_config(None).expect("default config validates");
        assert_eq!(
            format!("{:?}", ours.snapshot_policy),
            format!("{:?}", defaults.snapshot_policy),
            "the default must not change the snapshot policy"
        );
        assert_eq!(
            ours.max_in_snapshot_log_to_keep, defaults.max_in_snapshot_log_to_keep,
            "the default must not change log purging"
        );
    }

    /// How much larger a Raft RPC gets on the wire than the bytes it carries.
    ///
    /// Every Raft RPC is `serde_json`, and a `Vec<u8>` payload — a snapshot chunk — serialises as
    /// an array of decimal integers, measured at 4.0x for the byte distribution a snapshot
    /// actually contains. Named rather than inlined so this number and the sizing comment in
    /// `raft_config` cannot drift apart: if the snapshot wire format ever stops being JSON,
    /// exactly one number changes.
    const JSON_WIRE_EXPANSION: u64 = 4;

    /// Issue #428: the snapshot *transport* knobs are deliberately ours, unlike the snapshot
    /// *policy* the test above pins to openraft's defaults.
    ///
    /// openraft aborts a whole snapshot transfer and restarts it from offset 0 when any single
    /// chunk misses `install_snapshot_timeout`, and chunks ride the JSON cluster port as
    /// `Vec<u8>` — measured at 4.0× on the wire, ~300 ms per MiB on loopback. At openraft's own
    /// defaults (3 MiB chunks, 200 ms) a chunk cannot finish even on loopback, so a fleet holding
    /// a few MiB of datasets could never catch up a joining node at all.
    ///
    /// The second half of this test is the "and nothing else moved" claim: raising a snapshot
    /// deadline must not become a failover change, which is exactly what raising
    /// `heartbeat_interval` (issue #411's fork) would have been.
    #[test]
    fn raft_config_pins_the_snapshot_transport_knobs() {
        let ours = RaftNode::raft_config(None).expect("default config validates");

        assert_eq!(
            ours.install_snapshot_timeout, 10_000,
            "a chunk that misses this restarts the entire snapshot, so it is deliberately generous"
        );
        assert_eq!(ours.snapshot_max_chunk_size, 1024 * 1024);
        // A chunk's *wire* form is what has to fit the cluster port: ~4× the raw bytes, plus the
        // request envelope (vote, meta, offset).
        assert!(
            ours.snapshot_max_chunk_size * JSON_WIRE_EXPANSION + 64 * 1024
                < crate::rpc::DEFAULT_MAX_BODY_BYTES,
            "a JSON-expanded chunk must stay under the cluster port's body cap"
        );

        assert_eq!(
            ours.heartbeat_interval, 50,
            "#428 must not move the replication timers"
        );
        assert_eq!(ours.election_timeout_min, 150);
        assert_eq!(ours.election_timeout_max, ELECTION_TIMEOUT_MAX_MS);
    }

    /// A shipped fleet maintains its own log without being asked (#365).
    ///
    /// The console has no snapshot or compaction control, and it never will: that is the cluster's
    /// own business. The claim only holds while the effective default is an *automatic* policy.
    /// `SnapshotPolicy::Never` would make openraft wait for a manual
    /// `trigger().snapshot()` — which nothing in this codebase calls — and the log would grow
    /// without bound while no follower could ever be caught up by `install_snapshot`.
    ///
    /// Deliberately not the same claim as
    /// `raft_config_default_leaves_the_snapshot_knobs_untouched`, which pins "we do not override
    /// openraft" and would keep passing if a future openraft shipped `Never` as *its* default.
    /// This pins the property an operator actually depends on.
    ///
    /// Pins D-24: the shipped default is an automatic snapshot policy with a finite purge
    /// horizon — the cluster maintains its own log without an admin action.
    #[test]
    fn a_shipped_fleet_snapshots_and_purges_without_being_asked() {
        let ours = RaftNode::raft_config(None).expect("default config validates");
        match ours.snapshot_policy {
            SnapshotPolicy::LogsSinceLast(threshold) => assert!(
                threshold > 0,
                "a zero threshold would rebuild a snapshot on every entry"
            ),
            SnapshotPolicy::Never => panic!(
                "the shipped default must snapshot on its own — nothing calls trigger().snapshot(), \
                 so `Never` means the log is never compacted"
            ),
        }
        assert!(
            ours.max_in_snapshot_log_to_keep < u64::MAX,
            "logs a snapshot already covers must eventually be purged, or snapshotting buys \
             nothing back"
        );
    }

    /// …and the knob sets **both** halves. Setting only the policy is the trap issue #183 exists to
    /// remove: snapshots would be built but no follower would ever need one over the wire, because
    /// the log it is missing would still be there to replicate.
    ///
    /// Pins D-43: `Some(n)` sets `LogsSinceLast(n)` **and** `max_in_snapshot_log_to_keep = 0`
    /// together — either alone leaves `install_snapshot` unreachable in the chaos tier.
    #[test]
    fn the_snapshot_knob_sets_the_policy_and_purges_immediately() {
        let ours = RaftNode::raft_config(Some(10)).expect("config validates");
        assert!(
            matches!(ours.snapshot_policy, SnapshotPolicy::LogsSinceLast(10)),
            "policy was {:?}",
            ours.snapshot_policy
        );
        assert_eq!(
            ours.max_in_snapshot_log_to_keep, 0,
            "a snapshot policy without immediate purge never forces install_snapshot"
        );
    }

    // ---- the isolated-owner rule (#470) ------------------------------------------------
    //
    // `is_isolated` reads a live metrics watch, so before #470 the rule could only be
    // exercised by standing a cluster up and partitioning it — which is why the arm that
    // matters most (a leader with *no* quorum ack yet) had no direct test at all. The rule is
    // now a pure function of its three inputs, so every arm is reachable, and the gauge
    // `rift_cluster_isolated` reports the same function these pin.

    const ME: NodeId = 1;
    const OTHER: NodeId = 2;

    /// Fails closed: no known leader is isolated. A follower partitioned away loses its
    /// leader once the election timeout elapses, and that is exactly when it must stop
    /// acting as an owner.
    #[test]
    fn a_node_that_knows_no_leader_is_isolated() {
        assert!(isolated_from(ME, None, None));
        // Even a fresh quorum ack cannot rescue it: with no leader there is no quorum to
        // have acknowledged anything, so the ack is stale by construction.
        assert!(isolated_from(ME, None, Some(0)));
    }

    /// The fail-closed half of the rule, and the one most easily broken by "simplifying"
    /// `is_none_or` to `is_some_and`. openraft reports `None` for a leader no quorum has
    /// acknowledged — a just-elected leader before its first `AppendEntries` round, or one
    /// partitioned from its followers. Reading that as healthy would let a leader that has
    /// never been acknowledged accept owner-side writes.
    #[test]
    fn a_leader_with_no_quorum_ack_yet_is_isolated_not_healthy() {
        assert!(isolated_from(ME, Some(ME), None));
    }

    /// A leader a quorum acknowledged inside the window holds its lease and is not isolated.
    #[test]
    fn a_leader_acknowledged_inside_the_window_is_not_isolated() {
        assert!(!isolated_from(ME, Some(ME), Some(0)));
        assert!(!isolated_from(ME, Some(ME), Some(ISOLATION_WINDOW_MS - 1)));
    }

    /// The boundary is exclusive: the window itself still counts as held. Pinned because an
    /// off-by-one here silently widens or narrows a safety gate.
    #[test]
    fn the_isolation_window_boundary_is_exclusive() {
        assert!(!isolated_from(ME, Some(ME), Some(ISOLATION_WINDOW_MS)));
        assert!(isolated_from(ME, Some(ME), Some(ISOLATION_WINDOW_MS + 1)));
    }

    /// Hearing another node's leadership *is* evidence of contact with the quorum, so this
    /// node is not isolated — and its own quorum-ack figure is irrelevant, because that
    /// number describes a leadership it does not hold.
    #[test]
    fn a_follower_that_can_see_a_leader_is_not_isolated() {
        assert!(!isolated_from(ME, Some(OTHER), None));
        assert!(!isolated_from(ME, Some(OTHER), Some(u64::MAX)));
    }
}
