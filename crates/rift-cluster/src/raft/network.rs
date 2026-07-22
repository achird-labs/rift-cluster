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
use crate::rpc::{HandlerFuture, Router, RpcClient, RpcError};

/// AppendEntries receiving endpoint.
pub(crate) const RAFT_APPEND_PATH: &str = "/internal/v1/raft/append";
/// RequestVote receiving endpoint.
pub(crate) const RAFT_VOTE_PATH: &str = "/internal/v1/raft/vote";
/// InstallSnapshot receiving endpoint.
pub(crate) const RAFT_SNAPSHOT_PATH: &str = "/internal/v1/raft/snapshot";
/// Seed-join endpoint: a starting node asks an existing member to admit it.
pub(crate) const CLUSTER_JOIN_PATH: &str = "/internal/v1/cluster/join";
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

/// A shared, late-filled handle to a node's [`Raft`]. The router captures a clone
/// before the `Raft` exists; the node sets it once, after construction.
pub(crate) type RaftSlot = Arc<OnceCell<Raft<TypeConfig>>>;

/// The sending side: builds a per-target client over one pooled [`RpcClient`].
#[derive(Clone)]
pub(crate) struct RpcNetwork {
    client: RpcClient,
}

impl RpcNetwork {
    pub(crate) fn new(client: RpcClient) -> Self {
        Self { client }
    }
}

impl RaftNetworkFactory<TypeConfig> for RpcNetwork {
    type Network = PeerClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        PeerClient {
            client: self.client.clone(),
            target,
            addr: node.addr.clone(),
        }
    }
}

/// A network client aimed at one peer. Cheap to build; the underlying connection
/// pool is shared across every peer via the cloned [`RpcClient`].
pub(crate) struct PeerClient {
    client: RpcClient,
    target: NodeId,
    addr: String,
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
        // A peer address that does not parse is a configuration fault, not a
        // transient outage, but openraft has no "misconfigured" class; Unreachable
        // (which backs off) is the least-wrong mapping and keeps the target's id
        // in the log for diagnosis.
        let peer: SocketAddr = self.addr.parse().map_err(|e: std::net::AddrParseError| {
            RPCError::Unreachable(Unreachable::new(&AddrError {
                target: self.target,
                addr: self.addr.clone(),
                source: e,
            }))
        })?;
        let body = serde_json::to_vec(req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let response = self
            .client
            .call(peer, "POST", path, body)
            .await
            .map_err(map_rpc_err)?;
        serde_json::from_slice(&response).map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
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

/// Error carrying which peer's address failed to parse.
#[derive(Debug, thiserror::Error)]
#[error("peer {target} has unparseable address {addr:?}: {source}")]
struct AddrError {
    target: NodeId,
    addr: String,
    source: std::net::AddrParseError,
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
fn map_rpc_err<E>(e: RpcError) -> RPCError<NodeId, BasicNode, RaftError<NodeId, E>>
where
    E: std::error::Error,
{
    if matches!(e, RpcError::Handler(_)) {
        RPCError::Network(NetworkError::new(&e))
    } else {
        RPCError::Unreachable(Unreachable::new(&e))
    }
}

/// Serializes the promotion phase of seed-join admissions on this node. One
/// per router, and admissions only succeed on the leader, so the leader's gate
/// is the cluster-wide serialization point for auto-promotions — see [`admit`].
type AdmissionGate = Arc<Mutex<()>>;

/// Register the control-plane receiving endpoints (Raft RPCs + seed join) onto
/// `router`, all reading the node through `slot`.
#[must_use]
pub(crate) fn control_routes(router: Router, slot: RaftSlot) -> Router {
    let append = slot.clone();
    let vote = slot.clone();
    let snapshot = slot.clone();
    let write = slot.clone();
    let applied = slot.clone();
    let join = slot;
    let admission_gate: AdmissionGate = Arc::new(Mutex::new(()));

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
}

/// Reply to a successful [`JoinRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinAccepted {
    admitted: bool,
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
    // Concurrent joins are the normal case (a StatefulSet rollout seeds every
    // pod off the same node), so failures name the candidate they belong to.
    // The learner add — the slow, replication-bound phase — deliberately stays
    // outside the gate so concurrent joins still parallelize catch-up.
    membership_change(raft, &format!("admit {id}: add learner"), || {
        raft.add_learner(id, BasicNode::new(advertise.clone()), true)
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

/// The soft auto-promotion gate: promote while under the ceiling, never
/// re-promote an existing voter.
fn should_promote(voters: &BTreeSet<NodeId>, id: NodeId, max_voters: usize) -> bool {
    voters.len() < max_voters && !voters.contains(&id)
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
}
