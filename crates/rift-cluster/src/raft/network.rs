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
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::error::{NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, ChangeMembers, Raft, RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

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
                Box::pin(async move {
                    let raft = raft_of(&slot)?;
                    let req = decode::<JoinRequest>(&body)?;
                    admit(raft, req.node_id, req.advertise).await?;
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
/// catch up, then promote it to voter if the cluster is still under
/// [`MAX_AUTO_VOTERS`]. Must run on the leader — on any other node openraft
/// returns a `ForwardToLeader` error, surfaced here so the caller can retry the
/// leader.
async fn admit(raft: &Raft<TypeConfig>, id: NodeId, advertise: String) -> Result<(), RpcError> {
    raft.add_learner(id, BasicNode::new(advertise), true)
        .await
        .map_err(|e| RpcError::Handler(e.to_string()))?;

    // Promote incrementally with `AddVoterIds`, never by replacing the whole
    // voter set: the voter count read from the metrics *watch* lags the committed
    // membership, so building a `ReplaceAllVoters` set from it would let two
    // concurrent joins each overwrite the other's just-added voter — demoting a
    // live member with no error. `AddVoterIds` only ever adds, so the ceiling
    // read below is a soft gate on *whether* to auto-promote, never the source of
    // the new membership.
    let voters: BTreeSet<NodeId> = raft
        .metrics()
        .borrow()
        .membership_config
        .voter_ids()
        .collect();
    if voters.len() < MAX_AUTO_VOTERS && !voters.contains(&id) {
        raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([id])), false)
            .await
            .map_err(|e| RpcError::Handler(e.to_string()))?;
    }
    Ok(())
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
