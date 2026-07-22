//! RPC-backed [`RaftNetwork`] over the #8 cluster transport, plus the receiving
//! endpoints and the seed-join admission path.
//!
//! openraft drives the *sending* side through [`RpcNetwork`] (a
//! [`RaftNetworkFactory`]); the *receiving* side is a set of handlers registered
//! into the cluster [`Router`] by [`control_routes`], each of which decodes the
//! request, hands it to the local [`Raft`], and encodes the reply. A node's own
//! [`Raft`] does not exist yet when its router is built (the router is needed to
//! bind the server, whose address the node then advertises), so the handlers read
//! the node through a shared [`OnceCell`] the node fills in once construction
//! completes — before it accepts any peer traffic.

use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{
    ChangeMembershipError, ClientWriteError, InProgress, NetworkError, RPCError, RaftError,
    Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, ChangeMembers, Raft, RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, OnceCell};

use super::{NodeId, TypeConfig};
use crate::control::{ControlRequest, ControlResponse};
use crate::rpc::{Authority, HandlerFuture, PeerResolver, Router, RpcClient, RpcError};

/// AppendEntries receiving endpoint.
pub(crate) const RAFT_APPEND_PATH: &str = "/internal/v1/raft/append";
/// RequestVote receiving endpoint.
pub(crate) const RAFT_VOTE_PATH: &str = "/internal/v1/raft/vote";
/// InstallSnapshot receiving endpoint.
pub(crate) const RAFT_SNAPSHOT_PATH: &str = "/internal/v1/raft/snapshot";
/// Seed-join endpoint: a starting node asks an existing member to admit it.
pub(crate) const CLUSTER_JOIN_PATH: &str = "/internal/v1/cluster/join";
/// Leave endpoint (issue #6): a leaving node (or whoever is completing its
/// departure on its behalf) asks the leader to finish evicting it.
pub(crate) const CLUSTER_LEAVE_PATH: &str = "/internal/v1/cluster/leave";
/// Write-forward endpoint: a non-leader node hands a [`ControlRequest`] to the
/// leader (issue #9). The reply distinguishes "committed" from "I am not the
/// leader either — try there", so the forwarder can chase a moved leadership.
pub(crate) const CLUSTER_WRITE_PATH: &str = "/internal/v1/cluster/write";
/// Applied-index endpoint: reports how far this node's state machine has
/// applied, for the write barrier (issue #9).
pub(crate) const CLUSTER_APPLIED_PATH: &str = "/internal/v1/applied";

/// The maximum voter count the cluster auto-promotes a joining learner up to.
/// Beyond this a larger quorum costs more than it buys, so extra members stay
/// learners until an operator changes membership explicitly.
pub(crate) const MAX_AUTO_VOTERS: usize = 9;

/// The fewest voters a graceful departure may leave behind.
///
/// A whole-fleet teardown SIGTERMs every node, and each one leaving in turn
/// walks a three-node membership to a single voter — the entire control plane
/// on one authoritative volume, and a cold start that cannot proceed until
/// exactly that node returns. Two is the smallest floor that stops the walk
/// while leaving every rolling restart of a fleet of three or more untouched:
/// there, only one node leaves at a time and the fleet never drops past two.
///
/// This is availability and durability hardening, not a safety invariant —
/// Raft's joint consensus keeps each individual membership change safe on its
/// own, and openraft's refusal to commit an empty voter set is the hard
/// backstop underneath.
pub(crate) const MIN_VOTERS: usize = 2;

/// How long a membership change waits for an entry to commit. Kept under
/// [`DEFAULT_REQUEST_TIMEOUT`] so the wait expires *inside* the joiner's own RPC
/// budget: a longer bound would be unobservable — the joiner would give up
/// first, drop this handler mid-admission, and retry, piling a second concurrent
/// admission onto the leader it was already contending with. Promotions also
/// hold the admission gate (#55), so a slow one delays queued admissions past
/// their own budgets transitively; that only re-triggers the same cheap,
/// idempotent retry, and a leader too wedged to commit promptly cannot admit
/// anyone anyway.
const ADMIT_COMMIT_TIMEOUT: Duration = Duration::from_millis(1_500);

/// How many times a membership change re-submits after losing the slot to a
/// concurrent admission. Each attempt waits for the competing entry rather than
/// spinning, and every attempt but the last lets exactly one competitor through.
/// Promotions are serialized on the admission gate (#55), so the contention this
/// absorbs is concurrent *learner-add* entries — bounded by how many nodes can
/// be admitted at once, [`MAX_AUTO_VOTERS`], with room to spare. Uncontended
/// joins never reach attempt 2.
const ADMIT_MAX_ATTEMPTS: usize = 12;

/// openraft's error type for the two membership entry points used here.
type MembershipError = RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>;

/// A node's request to be admitted to the cluster, sent to a seed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinRequest {
    pub node_id: NodeId,
    pub advertise: String,
}

/// A node's request to be fully removed from the cluster, sent to the leader
/// (issue #6). Mirrors [`JoinRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LeaveRequest {
    pub node_id: NodeId,
}

/// A shared, late-filled handle to a node's [`Raft`]. The router captures a clone
/// before the `Raft` exists; the node sets it once, after construction.
pub(crate) type RaftSlot = Arc<OnceCell<Raft<TypeConfig>>>;

/// The sending side: builds a per-target client over one pooled [`RpcClient`].
#[derive(Clone)]
pub(crate) struct RpcNetwork {
    client: RpcClient,
    resolver: Arc<dyn PeerResolver>,
}

impl RpcNetwork {
    pub(crate) fn new(client: RpcClient, resolver: Arc<dyn PeerResolver>) -> Self {
        Self { client, resolver }
    }
}

impl RaftNetworkFactory<TypeConfig> for RpcNetwork {
    type Network = PeerClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        PeerClient {
            client: self.client.clone(),
            target,
            addr: node.addr.clone(),
            resolver: Arc::clone(&self.resolver),
        }
    }
}

/// A network client aimed at one peer. Cheap to build; the underlying connection
/// pool is shared across every peer via the cloned [`RpcClient`].
pub(crate) struct PeerClient {
    client: RpcClient,
    target: NodeId,
    addr: String,
    resolver: Arc<dyn PeerResolver>,
}

impl PeerClient {
    async fn send<Req, Resp, E>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error,
    {
        // Resolved fresh on every send (never cached), so a peer's advertise
        // address that is a hostname (a StatefulSet's headless DNS entry) picks
        // up a changed pod IP on the very next attempt.
        let addrs = resolve_peer(&self.resolver, self.target, &self.addr)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let body = serde_json::to_vec(req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // Try each resolved address in the resolver's order until one answers
        // (#79).
        //
        // The steady-state cost of a permanently-dead address is bounded, not
        // zero: `RpcClient` tracks health per `SocketAddr`, so after
        // `DEFAULT_FAILURE_THRESHOLD` consecutive failures that address is
        // fast-failed for the cooldown instead of burning a connect timeout,
        // and the live one is reached without any pinning state of our own.
        // Each cooldown expiry lets it be tried once more — which is the point,
        // since an address that comes back must be usable again.
        let mut last: Option<RpcError> = None;
        for peer in &addrs {
            match self.client.call(*peer, "POST", path, body.clone()).await {
                Ok(response) => {
                    return serde_json::from_slice(&response)
                        .map_err(|e| RPCError::Network(NetworkError::new(&e)));
                }
                Err(e) => last = Some(e),
            }
        }

        // Classify on the last failure rather than flattening everything to
        // `Unreachable`: a peer that is up but still booting answers `Handler`,
        // which openraft retries promptly, and reporting that as unreachable
        // would make it back off from a node that is seconds from ready.
        let context = format!("{} ({} address(es) tried)", self.addr, addrs.len());
        Err(match last {
            Some(e) => map_rpc_err(&e, &context),
            None => RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
                "{context}: no addresses to try"
            )))),
        })
    }
}

/// Resolve `authority` through `resolver`, fresh for this one call. The
/// resolver's own lookup can block (the default does a real DNS/hosts lookup),
/// so it always runs on the blocking pool rather than the async runtime.
///
/// The one seam every peer-address resolution in the crate goes through, so a
/// hostname advertise address (issue #68) is resolved identically whether the
/// caller is Raft replication, a seed join, or a leader-hint chase.
/// Returns every address the authority names, in resolver order; callers dial
/// them in turn until one answers (#79).
pub(crate) async fn resolve_authority(
    resolver: &Arc<dyn PeerResolver>,
    authority: &str,
) -> std::io::Result<Vec<SocketAddr>> {
    // A literal `IP:port` needs no resolver. This is now the backward-compat
    // short-circuit for clusters that advertise a literal address rather than
    // a claim that hostnames never occur — #68 makes a hostname an equally
    // valid membership address. It still earns its keep: replication (an
    // append_entries per heartbeat per peer) stays off the blocking pool for
    // that common case, at the cost of only a parse the resolver would do
    // anyway.
    if let Ok(addr) = authority.parse::<SocketAddr>() {
        return Ok(vec![addr]);
    }
    let resolver = Arc::clone(resolver);
    let owned = authority.to_owned();
    let addrs = match tokio::task::spawn_blocking(move || resolver.resolve(&owned)).await {
        Ok(result) => result?,
        Err(join_err) => return Err(std::io::Error::other(join_err.to_string())),
    };
    // The trait forbids an empty `Ok`, but a third-party or test resolver can
    // still return one, and an empty list would degrade into a loop that tries
    // nothing and reports no cause. Enforced here so every caller is covered by
    // one check rather than each remembering it.
    if addrs.is_empty() {
        return Err(std::io::Error::other(format!(
            "resolver returned no addresses for {authority}"
        )));
    }
    Ok(addrs)
}

/// Resolve `authority` for `target`, adding which peer the failure belongs to
/// — [`resolve_authority`] itself has no notion of *whose* address it was
/// asked to resolve.
async fn resolve_peer(
    resolver: &Arc<dyn PeerResolver>,
    target: NodeId,
    authority: &str,
) -> Result<Vec<SocketAddr>, AddrError> {
    resolve_authority(resolver, authority)
        .await
        .map_err(|source| AddrError {
            target,
            addr: authority.to_owned(),
            source,
        })
}

impl RaftNetwork<TypeConfig> for PeerClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.send(RAFT_APPEND_PATH, &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.send(RAFT_VOTE_PATH, &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        self.send(RAFT_SNAPSHOT_PATH, &rpc).await
    }
}

/// Error carrying which peer's address failed to resolve.
#[derive(Debug, thiserror::Error)]
#[error("peer {target} has unresolvable address {addr:?}: {source}")]
struct AddrError {
    target: NodeId,
    addr: String,
    source: std::io::Error,
}

/// Map a transport failure onto openraft's RPC error space.
///
/// Only a peer that is up but momentarily unready (a `Handler` 500 — most often
/// "raft not yet initialized" during the peer's own startup) is a `Network`
/// error, which openraft retries promptly. Everything else — a dead or slow peer
/// (timeout/transport/shed) *and* deterministic-permanent faults (a secret
/// mismatch, a protocol-major skew, an unknown route) — becomes `Unreachable` so
/// openraft backs off rather than hammering a peer that will not answer any
/// sooner for being asked again immediately.
/// `context` names the peer the failure belongs to. It is threaded in rather
/// than left to the raw `RpcError` because a send now tries every address a
/// name resolves to (#79), and the surviving error must say *which peer* was
/// unreachable — the authority is what an operator configured; the addresses
/// are a resolution detail they never wrote down.
fn map_rpc_err<E>(e: &RpcError, context: &str) -> RPCError<NodeId, BasicNode, RaftError<NodeId, E>>
where
    E: std::error::Error,
{
    let detail = std::io::Error::other(format!("{context}: {e}"));
    if matches!(e, RpcError::Handler(_)) {
        RPCError::Network(NetworkError::new(&detail))
    } else {
        RPCError::Unreachable(Unreachable::new(&detail))
    }
}

/// Serializes every membership change this node arbitrates — the promotion
/// phase of seed-join admissions and the eviction phase of departures.
///
/// Both need the same thing: a committed read that no concurrent change can
/// invalidate between the read and the write. Admissions need it so the
/// auto-voter ceiling is exact (#55); departures need it so the voter floor is
/// (#69). They share one gate because they are the same critical section over
/// the same state — two gates would let a join and a leave interleave and each
/// see a count the other was about to change.
///
/// One per node, and both operations only succeed on the leader, so the
/// leader's gate is the cluster-wide serialization point for its term.
pub(crate) type MembershipGate = Arc<Mutex<()>>;

/// Register the control-plane receiving endpoints (Raft RPCs + seed join) onto
/// `router`, all reading the node through `slot`.
///
/// `gate` is supplied by the caller rather than created here because the node
/// itself also evicts locally — when the departing node *is* the leader — and
/// that path has to share this serialization to keep the floor exact.
#[must_use]
pub(crate) fn control_routes(router: Router, slot: RaftSlot, gate: MembershipGate) -> Router {
    let append = slot.clone();
    let vote = slot.clone();
    let snapshot = slot.clone();
    let write = slot.clone();
    let applied = slot.clone();
    let leave = slot.clone();
    let join = slot;
    let admission_gate = Arc::clone(&gate);
    let eviction_gate = gate;
    // Two names, one gate — each route closure needs its own handle, and the
    // names say which critical section each one serves. They must stay the same
    // lock: admissions and departures both read the committed voter set and act
    // on it, so splitting them would let a join and a leave each see a count the
    // other was about to change.

    router
        .route(
            "POST",
            CLUSTER_WRITE_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let slot = write.clone();
                Box::pin(async move {
                    let raft = raft_of(&slot)?;
                    let request = decode::<ControlRequest>(&body)?;
                    let reply = local_write(raft, request).await?;
                    encode(&reply)
                })
            }),
        )
        .route(
            "POST",
            CLUSTER_APPLIED_PATH,
            Arc::new(move |_body: Vec<u8>| -> HandlerFuture {
                let slot = applied.clone();
                Box::pin(async move {
                    let raft = raft_of(&slot)?;
                    let applied = raft.metrics().borrow().last_applied.map(|id| id.index);
                    encode(&AppliedReply { applied })
                })
            }),
        )
        .route(
            "POST",
            RAFT_APPEND_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let slot = append.clone();
                Box::pin(async move {
                    let raft = raft_of(&slot)?;
                    let rpc = decode::<AppendEntriesRequest<TypeConfig>>(&body)?;
                    let resp = raft
                        .append_entries(rpc)
                        .await
                        .map_err(|e| RpcError::Handler(e.to_string()))?;
                    encode(&resp)
                })
            }),
        )
        .route(
            "POST",
            RAFT_VOTE_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let slot = vote.clone();
                Box::pin(async move {
                    let raft = raft_of(&slot)?;
                    let rpc = decode::<VoteRequest<NodeId>>(&body)?;
                    let resp = raft
                        .vote(rpc)
                        .await
                        .map_err(|e| RpcError::Handler(e.to_string()))?;
                    encode(&resp)
                })
            }),
        )
        .route(
            "POST",
            RAFT_SNAPSHOT_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let slot = snapshot.clone();
                Box::pin(async move {
                    let raft = raft_of(&slot)?;
                    let rpc = decode::<InstallSnapshotRequest<TypeConfig>>(&body)?;
                    let resp = raft
                        .install_snapshot(rpc)
                        .await
                        .map_err(|e| RpcError::Handler(e.to_string()))?;
                    encode(&resp)
                })
            }),
        )
        .route(
            "POST",
            CLUSTER_JOIN_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let slot = join.clone();
                let gate = Arc::clone(&admission_gate);
                Box::pin(async move {
                    let raft = raft_of(&slot)?;
                    let req = decode::<JoinRequest>(&body)?;
                    admit(raft, &gate, req.node_id, req.advertise, MAX_AUTO_VOTERS).await?;
                    encode(&JoinAccepted { admitted: true })
                })
            }),
        )
        .route(
            "POST",
            CLUSTER_LEAVE_PATH,
            Arc::new(move |body: Vec<u8>| -> HandlerFuture {
                let slot = leave.clone();
                let gate = Arc::clone(&eviction_gate);
                Box::pin(async move {
                    let raft = raft_of(&slot)?;
                    let req = decode::<LeaveRequest>(&body)?;
                    let outcome = evict(raft, &gate, req.node_id).await?;
                    // A floor refusal is a normal reply on the same shape, not
                    // an error: the departing node needs to learn it is still a
                    // member so it exits crash-equivalent rather than recording
                    // a departure it did not make.
                    encode(&LeaveAccepted {
                        evicted: outcome == EvictOutcome::Removed,
                    })
                })
            }),
        )
}

/// Reply to a successful [`JoinRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinAccepted {
    admitted: bool,
}

/// Reply to a [`LeaveRequest`] the leader accepted for consideration.
///
/// `evicted` distinguishes the two normal outcomes: the node was removed, or
/// the voter floor refused the removal and it is still a member (#69).
///
/// The shape is unchanged, so an older client still decodes it — but it reads
/// every reply as a departure, which against a refusing leader means it records
/// one for a node that is still a member. Recoverable (that node rejoins
/// through the peers its log names) and moot when #69 and #72 ship together,
/// but it is a reason not to mix versions across a fleet mid-teardown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LeaveAccepted {
    pub(crate) evicted: bool,
}

/// Reply to a forwarded write ([`CLUSTER_WRITE_PATH`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum WriteReply {
    /// The op committed (or deduped); this is the state machine's response.
    Done(ControlResponse),
    /// The contacted node is not the leader. Carries its current hint so the
    /// forwarder can chase a leadership that moved mid-flight.
    ForwardTo { leader_addr: Option<String> },
}

/// Reply to an applied-index probe ([`CLUSTER_APPLIED_PATH`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AppliedReply {
    pub applied: Option<u64>,
}

/// Run a client write on the local Raft, mapping openraft's not-the-leader
/// refusal into [`WriteReply::ForwardTo`] and everything else into a handler
/// error. Shared by the receiving endpoint and [`super::node::RaftNode`]'s own
/// submit path so both classify leadership movement identically.
pub(crate) async fn local_write(
    raft: &Raft<TypeConfig>,
    request: ControlRequest,
) -> Result<WriteReply, RpcError> {
    use openraft::error::{ClientWriteError, RaftError};
    match raft.client_write(request).await {
        Ok(response) => Ok(WriteReply::Done(response.data)),
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
            Ok(WriteReply::ForwardTo {
                leader_addr: forward.leader_node.map(|node| node.addr),
            })
        }
        Err(e) => Err(RpcError::Handler(e.to_string())),
    }
}

/// Admit `id`@`advertise` to the cluster: add it as a learner, wait for it to
/// catch up, then promote it to voter if the cluster is still under the
/// ceiling — at the ceiling the joiner is admitted as a learner only (#55).
/// Must run on the leader — on any other node openraft returns a
/// `ForwardToLeader` error, surfaced here so the caller can retry the leader.
///
/// The handler passes [`MAX_AUTO_VOTERS`]; tests pass a small ceiling to
/// provoke the race without an 11-node cluster.
pub(crate) async fn admit(
    raft: &Raft<TypeConfig>,
    gate: &Mutex<()>,
    id: NodeId,
    advertise: String,
    max_voters: usize,
) -> Result<(), RpcError> {
    // Validated here as well as at the joiner's CLI, because this is the path
    // that actually writes an address into the replicated log: whatever arrives
    // on the wire becomes a durable membership entry, and one that can never
    // resolve is removable only by an admin membership change (issue #68).
    let advertise = advertise
        .parse::<Authority>()
        .map_err(|e| RpcError::Handler(format!("admit {id}: {e}")))?;

    // Concurrent joins are the normal case (a StatefulSet rollout seeds every
    // pod off the same node), so failures name the candidate they belong to.
    // The learner add — the slow, replication-bound phase — deliberately stays
    // outside the gate so concurrent joins still parallelize catch-up.
    membership_change(raft, &format!("admit {id}: add learner"), || {
        raft.add_learner(id, BasicNode::new(advertise.to_string()), true)
    })
    .await?;

    // Promote incrementally with `AddVoterIds`, never by replacing the whole
    // voter set: building a `ReplaceAllVoters` set from any local view would
    // let two concurrent joins each overwrite the other's just-added voter —
    // demoting a live member with no error. `AddVoterIds` only ever adds, so
    // the ceiling read below is a soft gate on *whether* to auto-promote,
    // never the source of the new membership.
    //
    // The gate makes the ceiling exact rather than best-effort (#55): every
    // promotion ends with a wait for its entry to apply (applied ⇒ committed),
    // so by the time a guard drops, the next holder's committed read includes
    // all prior auto-promotions. Without it, N concurrent admissions each read
    // a pre-promotion count and all pass the `< max_voters` check.
    let _serialized = gate.lock().await;
    let voters = committed_voters(raft, id).await?;
    if should_promote(&voters, id, max_voters) {
        membership_change(raft, &format!("admit {id}: promote to voter"), || {
            raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([id])), false)
        })
        .await?;
    } else if voters.contains(&id) {
        // A retried join from a node that is already a voter: nothing to do,
        // but leave a trace so the three-way outcome is always greppable.
        tracing::debug!(
            node_id = id,
            "join for an existing voter; membership unchanged"
        );
    } else {
        tracing::info!(
            node_id = id,
            voters = voters.len(),
            max_voters,
            "auto-voter ceiling reached; admitted as learner only"
        );
    }
    Ok(())
}

/// The committed voter set, read inside the RaftCore loop — the only read that
/// is exact under the admission gate. The metrics watch lags the state it
/// mirrors, and *effective* membership can carry an uncommitted entry from a
/// deposed leader that later truncates; committed-under-gate is neither.
async fn committed_voters(
    raft: &Raft<TypeConfig>,
    id: NodeId,
) -> Result<BTreeSet<NodeId>, RpcError> {
    raft.with_raft_state(|state| {
        state
            .membership_state
            .committed()
            .voter_ids()
            .collect::<BTreeSet<_>>()
    })
    .await
    .map_err(|e| RpcError::Handler(format!("admit {id}: reading committed membership: {e}")))
}

/// Leader-side: demote `node_id` from voter to learner if it currently is one
/// — the first half of a graceful departure (issue #6), the second being
/// [`remove_member`]. A no-op if `node_id` is not currently a voter, so a
/// retried leave (or a leave of a node that was only ever a learner) is
/// idempotent.
///
/// Demoting the *leader itself* does not hand off leadership: openraft's own
/// model allows a leader to be a non-voter (see `RaftState::is_leading` in the
/// vendored source — leadership requires only *membership*, checked via
/// `MembershipState::contains`, not `is_voter`), so a leader that demotes
/// itself keeps leading right through this step. It only steps down once
/// [`remove_member`] drops it from membership entirely — which is exactly why
/// [`super::node::RaftNode::leave`] can run both steps as one local call when
/// it is itself the leaving leader, with no separate transfer step needed.
pub(crate) async fn demote_voter(raft: &Raft<TypeConfig>, node_id: NodeId) -> Result<(), RpcError> {
    let is_voter = raft
        .with_raft_state(move |state| {
            state
                .membership_state
                .committed()
                .voter_ids()
                .any(|id| id == node_id)
        })
        .await
        .map_err(|e| RpcError::Handler(format!("demote {node_id}: reading membership: {e}")))?;
    if !is_voter {
        return Ok(());
    }
    membership_change(
        raft,
        &format!("demote {node_id}: remove from voters"),
        || raft.change_membership(ChangeMembers::RemoveVoters(BTreeSet::from([node_id])), true),
    )
    .await
}

/// Leader-side: drop `node_id` from membership entirely. Must run after
/// [`demote_voter`] — openraft refuses to remove a node that is still a voter
/// (`LearnerNotFound`) — which [`evict`] enforces by sequencing the two. A
/// no-op if `node_id` is already gone, so a retried leave is idempotent.
async fn remove_member(raft: &Raft<TypeConfig>, node_id: NodeId) -> Result<(), RpcError> {
    let is_member = raft
        .with_raft_state(move |state| {
            state
                .membership_state
                .committed()
                .nodes()
                .any(|(id, _)| *id == node_id)
        })
        .await
        .map_err(|e| RpcError::Handler(format!("evict {node_id}: reading membership: {e}")))?;
    if !is_member {
        return Ok(());
    }
    membership_change(raft, &format!("evict {node_id}: remove node"), || {
        raft.change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), false)
    })
    .await
}

/// Leader-side: evict `node_id` from the cluster — demote it from voter to
/// learner if needed, then drop it from membership (issue #6). Mirrors
/// [`admit`]'s use of [`membership_change`] so both admission and departure
/// share the same commit barrier and `InProgress` retry (#38). Must run on the
/// leader — on any other node openraft returns `ForwardToLeader`, surfaced
/// here so the caller (the [`CLUSTER_LEAVE_PATH`] handler) can retry against
/// the leader.
///
/// **`Ok` does not mean the node was removed.** The voter floor
/// ([`MIN_VOTERS`], [`held_by_floor`]) refuses a departure that would leave the
/// cluster with too few voters, and that refusal is a successful outcome —
/// [`EvictOutcome::HeldByFloor`] — not an error. Callers that record a
/// departure must match on the outcome, never on `Ok` alone: treating a refusal
/// as a departure persists "this node left" about a node that is still a
/// member (issue #69, and the shape of the bug that did this in #72).
///
/// Idempotent otherwise: retried, or called against a node already gone, is
/// [`EvictOutcome::Removed`].
pub(crate) async fn evict(
    raft: &Raft<TypeConfig>,
    gate: &Mutex<()>,
    node_id: NodeId,
) -> Result<EvictOutcome, RpcError> {
    // Held across the read and both writes, for the reason the ceiling is
    // (#55): the floor is only exact if no other departure can commit between
    // this node counting the voters and acting on that count. Two nodes
    // SIGTERMed together would otherwise both read three voters and both leave.
    let _serialized = gate.lock().await;

    let voters = committed_voters(raft, node_id).await?;
    // The permit path validates itself — `change_membership` below answers
    // `ForwardToLeader` on a node that only thinks it leads, so a stale read can
    // never commit a removal. The refusal path returns before any write and so
    // has no such check, which is deliberate: confirming leadership means
    // openraft's `is_leader`, a quorum round trip, inside this lock and on every
    // departure. A stale refusal costs nothing — the node exits still a member
    // and resumes — so the asymmetry buys robustness that is already there.
    if held_by_floor(&voters, node_id) {
        tracing::info!(
            node_id,
            voters = voters.len(),
            min_voters = MIN_VOTERS,
            "refusing a departure that would drop the cluster below the voter floor"
        );
        return Ok(EvictOutcome::HeldByFloor);
    }

    demote_voter(raft, node_id).await?;
    remove_member(raft, node_id).await?;
    Ok(EvictOutcome::Removed)
}

/// What [`evict`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvictOutcome {
    /// The node is out of the membership — removed now, or already gone.
    Removed,
    /// The voter floor refused the removal; the node is still a member.
    HeldByFloor,
}

/// The soft auto-promotion gate: promote while under the ceiling, never
/// re-promote an existing voter.
fn should_promote(voters: &BTreeSet<NodeId>, id: NodeId, max_voters: usize) -> bool {
    voters.len() < max_voters && !voters.contains(&id)
}

/// The departure floor: refuse a **voter**'s removal when it would leave fewer
/// than [`MIN_VOTERS`] behind.
///
/// Only voters are counted and only voters are refused: removing a learner
/// costs the cluster no quorum member, and a floor that refused learners would
/// strand every node the auto-voter ceiling capped.
fn held_by_floor(voters: &BTreeSet<NodeId>, node_id: NodeId) -> bool {
    voters.contains(&node_id) && voters.len() <= MIN_VOTERS
}

/// Submit one membership change, waiting out whatever change currently holds the
/// slot.
///
/// openraft accepts a membership change only while the previous one is committed
/// (`effective == committed`), and a joining node routinely arrives before that
/// holds: the founding node's own bootstrap entry, or a *concurrent* admission of
/// another joiner, can still be in flight. The change is then rejected outright
/// with [`InProgress`] and the join fails — the intermittent seed-join failure in
/// #38, which reproduces reliably when two nodes seed off one leader.
///
/// So a rejection here is not terminal: wait for the entry the error names, then
/// re-submit. The retry keys off openraft's *typed* error rather than its
/// rendered message, and it waits on that exact entry instead of sleeping a
/// guessed interval. On success the applied-index wait is what lets the caller
/// read a membership view that already includes this change.
async fn membership_change<F, Fut>(
    raft: &Raft<TypeConfig>,
    what: &str,
    submit: F,
) -> Result<(), RpcError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<ClientWriteResponse<TypeConfig>, MembershipError>>,
{
    let mut waited_on = None;
    for _ in 0..ADMIT_MAX_ATTEMPTS {
        match submit().await {
            Ok(resp) => return wait_applied(raft, resp.log_id.index, what).await,
            Err(e) => {
                let Some(pending) = in_progress(&e) else {
                    return Err(RpcError::Handler(format!("{what}: {e}")));
                };
                // openraft only reports this rejection because some membership
                // entry is uncommitted, so it always names one; a rejection
                // without an entry to wait for would leave nothing to retry
                // against and must not become a hot re-submit loop.
                let index = pending
                    .membership_log_id
                    .as_ref()
                    .map(|l| l.index)
                    .ok_or_else(|| {
                        RpcError::Handler(format!(
                            "{what}: membership change in progress but no entry to wait for: {e}"
                        ))
                    })?;
                waited_on = Some(index);
                wait_applied(raft, index, what).await?;
            }
        }
    }
    Err(RpcError::Handler(format!(
        "{what}: contended by concurrent membership changes through all \
         {ADMIT_MAX_ATTEMPTS} attempts (last waited on entry {waited_on:?})"
    )))
}

/// Wait for `index` to be applied locally, which implies it is committed — the
/// precondition openraft enforces before the next membership change.
async fn wait_applied(raft: &Raft<TypeConfig>, index: u64, what: &str) -> Result<(), RpcError> {
    raft.wait(Some(ADMIT_COMMIT_TIMEOUT))
        .applied_index_at_least(Some(index), "membership entry applied")
        .await
        .map(|_| ())
        .map_err(|e| RpcError::Handler(format!("{what}: awaiting membership entry {index}: {e}")))
}

/// The typed "a membership change is already under way" rejection, or `None` for
/// every other failure. Structural match on openraft's error — a string match on
/// the message would be exactly the fragile shape the typed errors exist to
/// avoid.
fn in_progress(e: &MembershipError) -> Option<&InProgress<NodeId>> {
    match e {
        RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::InProgress(pending),
        )) => Some(pending),
        _ => None,
    }
}

fn raft_of(slot: &RaftSlot) -> Result<&Raft<TypeConfig>, RpcError> {
    slot.get()
        .ok_or_else(|| RpcError::Handler("raft node not yet initialized".to_owned()))
}

fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, RpcError> {
    serde_json::from_slice(body).map_err(|e| RpcError::Handler(format!("decode: {e}")))
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, RpcError> {
    serde_json::to_vec(value).map_err(|e| RpcError::Handler(format!("encode: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::error::{Fatal, ForwardToLeader, LearnerNotFound};
    use openraft::{CommittedLeaderId, LogId};

    fn log_id(index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(1, 0), index)
    }

    /// The retry decision must read openraft's typed error, not its rendered
    /// message: a message match would silently stop retrying the day openraft
    /// rewords the error, turning every contended join back into the #38 failure.
    /// Asserting on the *classifier* is what keeps that regression out — the
    /// end-to-end join tests pass either way.
    #[test]
    fn in_progress_matches_only_the_typed_rejection() {
        let pending = InProgress {
            committed: Some(log_id(3)),
            membership_log_id: Some(log_id(4)),
        };
        let contended: MembershipError = RaftError::APIError(
            ClientWriteError::ChangeMembershipError(ChangeMembershipError::InProgress(pending)),
        );

        let matched = in_progress(&contended).expect("the in-progress rejection must be retryable");
        assert_eq!(
            matched.membership_log_id.as_ref().map(|l| l.index),
            Some(4),
            "the entry to wait for comes from the typed error, not from parsing its text"
        );
    }

    /// Everything that is not that rejection must fail the join immediately.
    /// Retrying a fatal error would turn a hard failure into a slow one.
    #[test]
    fn in_progress_rejects_every_other_failure() {
        let others: Vec<MembershipError> = vec![
            RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_id: Some(1),
                leader_node: Some(BasicNode::new("127.0.0.1:1".to_owned())),
            })),
            RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::LearnerNotFound(LearnerNotFound { node_id: 7 }),
            )),
            RaftError::Fatal(Fatal::Stopped),
        ];

        for e in &others {
            assert!(
                in_progress(e).is_none(),
                "must not be treated as retryable: {e}"
            );
        }
    }
    #[test]
    fn should_promote_gates_on_ceiling_and_membership() {
        let voters: BTreeSet<NodeId> = BTreeSet::from([1, 2]);
        assert!(
            should_promote(&voters, 3, 3),
            "under the ceiling, a non-member promotes"
        );
        assert!(
            !should_promote(&voters, 3, 2),
            "at the ceiling, no promotion"
        );
        assert!(
            !should_promote(&voters, 3, 1),
            "an already-over-grown voter set must never keep growing"
        );
        assert!(
            !should_promote(&voters, 2, 3),
            "an existing voter is never re-promoted"
        );
    }

    /// Issue #69: the departure floor, as a predicate.
    ///
    /// Tested here rather than only through a cluster because the two ways to
    /// get it wrong are both silent: counting learners strands every
    /// ceiling-capped node, and an off-by-one at the boundary either lets the
    /// fleet walk to one voter or freezes a healthy three-node membership.
    #[test]
    fn held_by_floor_refuses_only_a_voter_that_would_breach_the_floor() {
        let three: BTreeSet<NodeId> = BTreeSet::from([1, 2, 3]);
        let two: BTreeSet<NodeId> = BTreeSet::from([1, 2]);

        assert!(
            !held_by_floor(&three, 3),
            "leaving three voters lands on the floor, not below it — permitted"
        );
        assert!(
            held_by_floor(&two, 2),
            "leaving two voters would drop to one — refused"
        );
        assert!(
            !held_by_floor(&two, 9),
            "a learner is not a voter, so its removal costs no quorum member"
        );
        assert!(
            held_by_floor(&BTreeSet::from([1]), 1),
            "a sole voter is refused by the floor as well as by openraft"
        );
    }

    /// A resolver that counts invocations and always resolves to a fixed
    /// address, standing in for the mock the DNS re-resolution gate needs.
    struct CountingResolver {
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        addr: SocketAddr,
    }

    impl PeerResolver for CountingResolver {
        fn resolve(&self, _authority: &str) -> std::io::Result<Vec<SocketAddr>> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(vec![self.addr])
        }
    }

    /// A resolver returning a fixed answer *set*, in the order given — the seam
    /// the fan-out gate needs. Counts per-authority calls so a test can prove
    /// how often resolution happened as well as what it yielded.
    struct ListResolver {
        addrs: Vec<SocketAddr>,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl PeerResolver for ListResolver {
        fn resolve(&self, _authority: &str) -> std::io::Result<Vec<SocketAddr>> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.addrs.clone())
        }
    }

    /// Issue #68 gate: a literal address never reaches the resolver.
    ///
    /// This runs per `append_entries`, per heartbeat, per peer, so the fast
    /// path is what keeps replication off the blocking pool for every cluster
    /// that advertises literal IPs — which is every cluster that existed before
    /// hostnames were allowed. Nothing else pins it: a refactor that always
    /// went through `spawn_blocking` would still be *correct*, and every other
    /// test would still pass, while quietly moving the hot path onto a
    /// thread pool.
    #[tokio::test]
    async fn a_literal_address_skips_the_resolver_entirely() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let resolver: Arc<dyn PeerResolver> = Arc::new(CountingResolver {
            calls: calls.clone(),
            addr: "127.0.0.1:9".parse().expect("valid addr"),
        });

        for literal in ["127.0.0.1:4790", "[::1]:4790"] {
            let resolved = resolve_peer(&resolver, 1, literal)
                .await
                .expect("a literal resolves without DNS");
            assert_eq!(
                resolved,
                vec![literal.parse::<SocketAddr>().expect("literal")],
                "the fast path must return the literal itself, not the resolver's answer"
            );
        }

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a literal address must never reach the resolver"
        );
    }

    /// A minimal listener that answers any request with a serialized
    /// `VoteResponse`.
    ///
    /// Deliberately raw rather than a real `RpcServer`: the fan-out gate is
    /// about which *address* got dialed, and a canned 200 keeps the test from
    /// depending on server-side routing, auth, or Raft state that has nothing
    /// to do with it. Dropping the returned guard stops the listener.
    async fn spawn_vote_responder() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        use openraft::Vote;
        use openraft::raft::VoteResponse;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind responder");
        let addr = listener.local_addr().expect("responder addr");

        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 4096];
                    let _ = socket.read(&mut buf).await;
                    let reply: VoteResponse<NodeId> = VoteResponse {
                        vote: Vote::new(1, 1),
                        vote_granted: true,
                        last_log_id: None,
                    };
                    let body = serde_json::to_vec(&reply).expect("encode vote reply");
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                    let _ = socket.flush().await;
                });
            }
        });
        (addr, handle)
    }

    /// #79 gate: a peer whose name resolves to several addresses is dialed at
    /// each until one answers.
    ///
    /// The failure this pins is silent and total: a dual-stack name whose first
    /// address nobody listens on made every send fail forever, even though a
    /// reachable address sat second in the very same answer. Committing to
    /// `.next()` is what did it.
    #[tokio::test]
    async fn a_send_tries_every_resolved_address() {
        use crate::rpc::{AlwaysHealthy, RpcClientConfig};
        use openraft::Vote;
        use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
        use openraft::raft::VoteRequest;

        // A real listener that answers, so "reachable" means answered rather
        // than merely connectable.
        let (server_addr, _guard) = spawn_vote_responder().await;
        // Port 1 is reserved and nothing binds it: a guaranteed-dead address,
        // deliberately placed FIRST so a first-answer implementation fails.
        let dead: SocketAddr = "127.0.0.1:1".parse().expect("valid addr");

        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let resolver: Arc<dyn PeerResolver> = Arc::new(ListResolver {
            addrs: vec![dead, server_addr],
            calls: calls.clone(),
        });

        let client = RpcClient::new(
            None,
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                connect_timeout: Duration::from_millis(100),
                request_timeout: Duration::from_millis(500),
                max_retries: 0,
            },
        );
        let mut network = RpcNetwork::new(client, resolver);
        let mut peer = network
            .new_client(2, &BasicNode::new("dual-stack-peer:4790".to_owned()))
            .await;

        let request = VoteRequest {
            vote: Vote::new(1, 1),
            last_log_id: None,
        };
        let reply = peer
            .vote(request, RPCOption::new(Duration::from_millis(500)))
            .await;

        assert!(
            reply.is_ok(),
            "the second resolved address answers, so the send must succeed: {reply:?}"
        );
    }

    /// #79: a dead address in the answer set stops costing a connect attempt
    /// once the health tracker has seen enough failures.
    ///
    /// Fan-out would be a bad trade if every send re-paid a connect timeout on
    /// a permanently-dead address. It does not, because health is keyed per
    /// `SocketAddr` — but that is a property of how fan-out and the tracker
    /// compose, which nothing else pins, so it is asserted rather than assumed.
    #[tokio::test]
    async fn a_dead_address_is_fast_failed_once_the_tracker_has_seen_it() {
        use crate::rpc::{RpcClientConfig, TrackedPeerHealth};

        let dead: SocketAddr = "127.0.0.1:1".parse().expect("addr");
        // Threshold 1 so a single failure is enough; the production default is
        // 3, and this test is about the composition, not the tuning.
        let health = Arc::new(TrackedPeerHealth::with_params(1, Duration::from_secs(60)));
        let client = RpcClient::new(
            None,
            health.clone(),
            RpcClientConfig {
                connect_timeout: Duration::from_millis(200),
                request_timeout: Duration::from_millis(200),
                max_retries: 0,
            },
        );

        // First call actually dials and fails, recording the failure.
        let _ = client
            .call(dead, "POST", "/internal/v1/raft/vote", Vec::new())
            .await;

        // Second call must be refused locally rather than dialing again: a
        // fast-fail returns far faster than the connect timeout it replaces.
        let started = std::time::Instant::now();
        let second = client
            .call(dead, "POST", "/internal/v1/raft/vote", Vec::new())
            .await;
        let elapsed = started.elapsed();

        assert!(second.is_err(), "a dead address cannot succeed");
        assert!(
            elapsed < Duration::from_millis(100),
            "the second attempt must be fast-failed by the health tracker, not \
             re-dialed; took {elapsed:?}"
        );
    }

    /// #79: when no resolved address answers, the error names the authority and
    /// carries a per-address cause — an operator staring at an unreachable peer
    /// needs to know it was tried, not just that it failed.
    #[tokio::test]
    async fn all_addresses_dead_reports_the_authority() {
        use crate::rpc::{AlwaysHealthy, RpcClientConfig};
        use openraft::Vote;
        use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
        use openraft::raft::VoteRequest;

        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let resolver: Arc<dyn PeerResolver> = Arc::new(ListResolver {
            addrs: vec![
                "127.0.0.1:1".parse().expect("addr"),
                "127.0.0.1:2".parse().expect("addr"),
            ],
            calls: calls.clone(),
        });

        let client = RpcClient::new(
            None,
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                connect_timeout: Duration::from_millis(50),
                request_timeout: Duration::from_millis(50),
                max_retries: 0,
            },
        );
        let mut network = RpcNetwork::new(client, resolver);
        let mut peer = network
            .new_client(2, &BasicNode::new("all-dead:4790".to_owned()))
            .await;

        let request = VoteRequest {
            vote: Vote::new(1, 1),
            last_log_id: None,
        };
        let reply = peer
            .vote(request, RPCOption::new(Duration::from_millis(50)))
            .await;
        let err = reply.expect_err("no address answers");
        assert!(
            format!("{err}").contains("all-dead:4790"),
            "the failure must name the authority that could not be reached, got: {err}"
        );
    }

    /// #79: an empty answer set is an error, not an empty success — callers
    /// must never receive a list they would silently loop zero times over.
    #[tokio::test]
    async fn an_empty_answer_set_is_an_error() {
        struct EmptyResolver;
        impl PeerResolver for EmptyResolver {
            fn resolve(&self, _authority: &str) -> std::io::Result<Vec<SocketAddr>> {
                Ok(Vec::new())
            }
        }
        let resolver: Arc<dyn PeerResolver> = Arc::new(EmptyResolver);
        let result = resolve_peer(&resolver, 1, "empty-name:4790").await;
        assert!(
            result.is_err(),
            "an empty answer set must surface as an error, not as zero addresses to try"
        );
    }

    /// #6 gate: the resolver must be consulted on every send, not resolved
    /// once and cached — a cached resolution would keep dialing a pod's old IP
    /// after a rollout moved it.
    #[tokio::test]
    async fn resolver_is_consulted_per_send() {
        use crate::rpc::{AlwaysHealthy, RpcClientConfig};
        use openraft::Vote;
        use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
        use openraft::raft::VoteRequest;

        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let resolver: Arc<dyn PeerResolver> = Arc::new(CountingResolver {
            calls: calls.clone(),
            addr: "127.0.0.1:1".parse().expect("valid addr"),
        });

        // Nothing listens on the resolved address, so both sends fail — which is
        // fine. What is under test is how many times the resolver was asked,
        // not whether the RPC landed. No retries, so one send is one resolve.
        let client = RpcClient::new(
            None,
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                connect_timeout: Duration::from_millis(50),
                request_timeout: Duration::from_millis(50),
                max_retries: 0,
            },
        );
        let mut network = RpcNetwork::new(client, resolver);

        // Driven through one `PeerClient` from `new_client`, which is the whole
        // point: calling the free `resolve_peer` twice would pass even if
        // resolution were hoisted into `new_client` and cached here — the exact
        // optimisation this test exists to forbid.
        //
        // A hostname, never a literal: `resolve_authority` short-circuits a
        // literal without touching the resolver, so a literal would count zero
        // and prove nothing.
        let mut peer = network
            .new_client(2, &BasicNode::new("some-peer:4790".to_owned()))
            .await;

        for _ in 0..2 {
            let request = VoteRequest {
                vote: Vote::new(1, 1),
                last_log_id: None,
            };
            let _ = peer
                .vote(request, RPCOption::new(Duration::from_millis(50)))
                .await;
        }

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "every send must re-resolve; resolution hoisted into new_client and cached would \
             read 1 here, and would keep dialing a pod's old IP after a rollout moved it"
        );
    }

    /// #6 gate: a hostname advertise address (the shape a StatefulSet's
    /// headless-service DNS entry takes) now resolves, where the old bare
    /// `parse::<SocketAddr>()` rejected anything but a literal IP.
    #[tokio::test]
    async fn hostname_advertise_address_resolves_where_bare_parse_would_fail() {
        use std::net::ToSocketAddrs;
        assert!(
            "localhost:0".parse::<SocketAddr>().is_err(),
            "sanity: a hostname must not parse as a literal SocketAddr — that is the bug this fixes"
        );

        let resolver: Arc<dyn PeerResolver> = Arc::new(crate::rpc::DnsResolver);
        let addrs = resolve_peer(&resolver, 1, "localhost:4790")
            .await
            .expect("the default resolver must resolve a hostname:port authority");
        assert!(
            !addrs.is_empty(),
            "resolution must never succeed with an empty answer set"
        );
        assert!(
            addrs.iter().all(|a| a.port() == 4790),
            "every resolved address keeps the authority's port, got {addrs:?}"
        );
        // On a dual-stack host `localhost` is exactly the multi-address case
        // #79 exists for, so this doubles as evidence the whole answer set
        // survives rather than being truncated to one.
        assert_eq!(
            addrs.len(),
            "localhost:4790".to_socket_addrs().expect("resolve").count(),
            "the resolver must return every address the OS gave, not the first"
        );
    }
}
