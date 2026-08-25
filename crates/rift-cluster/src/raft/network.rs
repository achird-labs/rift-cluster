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
use std::time::{Duration, Instant};

use openraft::error::{
    ChangeMembershipError, ClientWriteError, InProgress, NetworkError, PayloadTooLarge, RPCError,
    RaftError, Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, ChangeMembers, LogId, Raft, RaftNetwork, RaftNetworkFactory, Vote};
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
///
/// A *soft* ceiling (decision D-27): it bounds what the fleet does on its own,
/// and an operator-driven `change_membership` may race it by design.
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
///
/// The floor is decision D-25. It is enforced by the leader alone: the fleet is
/// never told whether a departure is a rolling restart or a teardown (an
/// orchestrator signal was rejected as platform-specific), so the residual
/// raciness of a fast roll is accepted rather than papered over.
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

/// How a [`PeerClient`] ticker asks "does this node lead *right now*?".
///
/// A live observation, not a cached mark: the production probe reads the
/// node's metrics watch at the moment of the decision (`state == Leader`), so
/// there is no updater task whose scheduling lag could leave a stale `true`
/// behind after a step-down — under load, exactly when it would matter.
/// During a snapshot-starvation window the reading may be stale in the other
/// direction, and that is correct: a starved core cannot have stepped down,
/// because it is the starved thing.
pub(crate) type LeadingProbe = Arc<dyn Fn() -> bool + Send + Sync>;

/// The sending side: builds a per-target client over one pooled [`RpcClient`].
#[derive(Clone)]
pub(crate) struct RpcNetwork {
    client: RpcClient,
    resolver: Arc<dyn PeerResolver>,
    /// Whether the node this factory belongs to leads at the moment asked.
    /// Every [`PeerClient`] ticker consults it before probing; see the field
    /// on [`PeerClient`] for why.
    leading: LeadingProbe,
}

impl RpcNetwork {
    pub(crate) fn new(
        client: RpcClient,
        resolver: Arc<dyn PeerResolver>,
        leading: LeadingProbe,
    ) -> Self {
        Self {
            client,
            resolver,
            leading,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for RpcNetwork {
    type Network = PeerClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        let mut peer = PeerClient {
            client: self.client.clone(),
            target,
            addr: node.addr.clone(),
            resolver: Arc::clone(&self.resolver),
            inflight: None,
            probe_inflight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            liveness: Arc::new(std::sync::Mutex::new(PeerLiveness::default())),
            leading: Arc::clone(&self.leading),
            ticker: None,
        };
        peer.ticker = Some(peer.spawn_liveness_ticker());
        peer
    }
}

/// A network client aimed at one peer. Cheap to build; the underlying connection
/// pool is shared across every peer via the cloned [`RpcClient`].
pub(crate) struct PeerClient {
    client: RpcClient,
    target: NodeId,
    addr: String,
    resolver: Arc<dyn PeerResolver>,
    /// The one outstanding AppendEntries transfer, if any — see
    /// [`InflightAppend`]. `RaftNetwork` takes `&mut self` and openraft drives
    /// one `PeerClient` per replication stream sequentially, so this needs no
    /// lock.
    inflight: Option<InflightAppend>,
    /// Whether a keepalive probe is already in flight to this peer.
    ///
    /// openraft re-drives the attach path about every 50 ms, so without this the
    /// adapter would spawn ~20 probes a second. That is not merely wasteful:
    /// each one runs through `call_once`, which feeds `TrackedPeerHealth`, and
    /// three consecutive failures mark the peer unhealthy for a cooldown — after
    /// which the *transfer's* own `call_once` fast-fails with "peer is not
    /// healthy". A slow-but-alive follower could therefore have the keepalive
    /// kill the very transfer it exists to protect. One probe at a time keeps
    /// failures accruing at the probe's timeout rate, like any other call.
    probe_inflight: Arc<std::sync::atomic::AtomicBool>,
    /// Shared with the liveness ticker: the leader's last-seen vote and when
    /// openraft last sent this peer anything. See [`PeerClient::spawn_liveness_ticker`].
    liveness: Arc<std::sync::Mutex<PeerLiveness>>,
    /// Whether this node currently leads. A probe asserts "your leader is
    /// alive", which is only true while we lead. openraft does not drop an
    /// ex-leader's idle replication clients promptly, so without this gate a
    /// gracefully departed (or deposed) leader's tickers keep speaking its
    /// old — still highest — vote, every follower's leader lease stays fresh,
    /// and the election the fleet needs is postponed for as long as the
    /// process lives. That is exactly the C5 rolling-restart outage: the
    /// handover is supposed to complete during the drain, and the drain is
    /// precisely when openraft is silent and the ticker would speak.
    leading: LeadingProbe,
    ticker: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Default)]
struct PeerLiveness {
    vote: Option<Vote<NodeId>>,
    last_sent: Option<std::time::Instant>,
}

impl Drop for PeerClient {
    fn drop(&mut self) {
        // openraft drops the client when it stops replicating to this peer
        // (leader change, membership change); the ticker must not outlive it.
        if let Some(ticker) = self.ticker.take() {
            ticker.abort();
        }
    }
}

/// How long a follower may go without hearing from this leader before the
/// ticker speaks for openraft — the same cadence as `heartbeat_interval`, so a
/// follower sees no difference between openraft's heartbeat and the ticker's.
const LIVENESS_TICK: Duration = Duration::from_millis(50);

/// What an in-flight AppendEntries transfer resolves to.
type AppendOutcome =
    Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>;

/// Floor on the link speed a replication transfer is granted before it is
/// declared failed: 8 MiB at 1 MiB/s is 8 s on top of the ordinary request
/// timeout, which puts a dataset at its 8 MiB quota ceiling at the admin front's
/// own 10 s write deadline. A link slower than this parks the intent for replay
/// rather than holding the transfer open indefinitely; op-id dedup then makes
/// the eventual commit happen exactly once.
const MIN_REPLICATION_BYTES_PER_SEC: u64 = 1024 * 1024;

/// How long a transfer of `body_len` bytes is given, from the client's ordinary
/// per-attempt timeout plus an allowance at [`MIN_REPLICATION_BYTES_PER_SEC`].
///
/// `pub(crate)` rather than private: `blobs::client` (#437) needs the identical
/// size-aware deadline for its own single-attempt transfers, and re-deriving it
/// there would risk the two drifting apart.
pub(crate) fn replication_deadline(request_timeout: Duration, body_len: usize) -> Duration {
    // Milliseconds rather than whole seconds so a few-hundred-KiB entry gets a
    // proportional allowance instead of being truncated to zero.
    let allowance = (body_len as u64).saturating_mul(1_000) / MIN_REPLICATION_BYTES_PER_SEC;
    request_timeout + Duration::from_millis(allowance)
}

/// How one Raft RPC that goes through [`PeerClient::send`] is delivered.
///
/// Only `install_snapshot` is bulk. `append_entries` carries payloads just as large — a
/// `DatasetPut` entry is the whole CSV — but it does not come through `send` at all: #411 gives it
/// its own single-flight path so that concurrent attempts share one in-flight transfer instead of
/// restarting it. `vote` is small and keeps the client's ordinary retry budget.
#[derive(Clone, Copy, Debug)]
enum Delivery {
    /// A small, latency-bound RPC. [`RpcClient`]'s own retries and flat `request_timeout` apply.
    Retried,
    /// A bulk transfer: exactly one attempt, on a deadline scaled to the body size.
    ///
    /// openraft is already the retry loop for these and keeps the transfer's own progress (its
    /// snapshot chunk loop holds the offset), so a retry underneath it re-sends the whole body and
    /// races the caller's next attempt. The flat `request_timeout` is sized for a control RPC: a
    /// multi-MiB chunk cut off by it can never complete however often it is retried (#428).
    BulkSingleAttempt,
}

/// One outstanding AppendEntries transfer, kept so that an identical re-send
/// attaches to it instead of restarting the body from byte 0.
///
/// This is the whole of the #411 fix. openraft wraps every AppendEntries call in
/// `timeout(heartbeat_interval)` — 50 ms — and *drops* the future when it fires,
/// which cancels the HTTP request and discards whatever had already reached the
/// follower. It then re-issues the same range on the next tick, so an entry only
/// ever commits if one attempt happens to complete transfer *and* the follower's
/// fsync inside a single heartbeat. A 512 KiB entry took 23-548 s; 1 MiB and up
/// never committed at all, which capped the fleet far below the 4 MiB spec and
/// 8 MiB dataset quotas that are already accepted at the front door.
struct InflightAppend {
    /// The transfer's identity: a re-send matches only if it is based on the
    /// same term and the same predecessor entry.
    vote: Vote<NodeId>,
    prev_log_id: Option<LogId<NodeId>>,
    /// Last entry the in-flight body carries. A re-send asking for *more* gets
    /// this back as `PartialSuccess`; one asking for less does not match at all.
    last_log_id: Option<LogId<NodeId>>,
    /// Polled by `&mut`, never awaited by value. openraft drops the waiter at
    /// its 50 ms deadline, and a `JoinHandle` keeps the task's output in the
    /// task cell until it is actually joined — so a cancelled poll loses
    /// nothing. (A `oneshot::Receiver` would not do: polling it *takes* the
    /// value, so a drop in the window between delivery and return would lose
    /// the result and silently forget the transfer.) Dropping the handle
    /// detaches the task rather than aborting it, which is what makes
    /// discarding a stale slot safe.
    handle: tokio::task::JoinHandle<AppendOutcome>,
}

impl PeerClient {
    // `RPCError` is openraft's, and every caller hands the result straight back to an
    // openraft `RaftNetwork` method, so this signature is fixed by that trait.
    #[allow(clippy::result_large_err)]
    async fn send<Req, Resp, E>(
        &self,
        path: &str,
        req: &Req,
        delivery: Delivery,
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
        // A bulk transfer's deadline is a budget for this whole send, not for each address in
        // turn. openraft wraps the entire `install_snapshot` call in its own
        // `install_snapshot_timeout` and abandons the transfer when that fires, so a per-address
        // deadline would let a peer advertising N addresses spend N x the budget and blow
        // openraft's bound whenever the first address is reachable-but-hung — restarting the
        // snapshot from offset 0, which is the very failure #428 exists to remove.
        let budget = match delivery {
            Delivery::Retried => None,
            Delivery::BulkSingleAttempt => Some(replication_deadline(
                self.client.request_timeout(),
                body.len(),
            )),
        };
        let started = Instant::now();

        // The body is handed to the last address rather than cloned for it: for a snapshot chunk
        // that is multiple MiB, and the single-address case — every literal advertise address, so
        // nearly all of them — then copies nothing at all.
        let mut body = body;
        let last_index = addrs.len().saturating_sub(1);
        let mut last: Option<RpcError> = None;
        for (index, peer) in addrs.iter().enumerate() {
            let payload = if index == last_index {
                std::mem::take(&mut body)
            } else {
                body.clone()
            };
            let attempt = match budget {
                None => self.client.call(*peer, "POST", path, payload).await,
                Some(budget) => {
                    let Some(remaining) = budget.checked_sub(started.elapsed()) else {
                        break;
                    };
                    self.client
                        .call_once(*peer, "POST", path, payload, remaining)
                        .await
                }
            };
            match attempt {
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

    /// Record that openraft just sent this peer an RPC carrying `vote`, so the
    /// liveness ticker stays quiet while openraft is doing the talking.
    fn note_sent(&self, vote: Vote<NodeId>) {
        if let Ok(mut l) = self.liveness.lock() {
            l.vote = Some(vote);
            l.last_sent = Some(std::time::Instant::now());
        }
    }

    /// A per-peer heartbeat that fills every silent window openraft opens.
    ///
    /// A follower's election timer is refreshed only by an AppendEntries that
    /// reaches its engine. openraft 0.9 sends a follower nothing else while a
    /// large entry is in flight (a heartbeat tick to a lagging follower re-sends
    /// the entry — measured: 114 calls in a 3-node 8 MiB run, none empty), and
    /// nothing at all from the moment it decides to snapshot, through fetching
    /// and parsing the snapshot, through every chunk. Any such window longer
    /// than `election_timeout_min` makes a voter campaign, and once its term
    /// has moved the leader — which rejects a candidate without adopting its
    /// term — never reconciles with it (#431).
    ///
    /// The ticker sends an empty AppendEntries on the leader's last-seen vote
    /// whenever openraft has sent this peer nothing within one heartbeat
    /// interval, and is idle otherwise. `prev_log_id: None` with no entries is
    /// exactly what openraft itself sends a fresh target: the vote check runs
    /// before the log check on the receiver, so the probe refreshes the timer
    /// and can truncate nothing. It goes through [`RpcClient::probe`], not
    /// `call_once`, because the health tracker would otherwise refuse it for
    /// the whole cooldown after the peer's restart — the exact window it exists
    /// to cover (decision D-22). It dies with the `PeerClient`.
    fn spawn_liveness_ticker(&self) -> tokio::task::JoinHandle<()> {
        let client = self.client.clone();
        let resolver = Arc::clone(&self.resolver);
        let target = self.target;
        let addr = self.addr.clone();
        let gate = Arc::clone(&self.probe_inflight);
        let liveness = Arc::clone(&self.liveness);
        let leading = Arc::clone(&self.leading);
        let deadline = self.client.request_timeout();
        tokio::spawn(async move {
            use std::sync::atomic::Ordering;
            loop {
                tokio::time::sleep(LIVENESS_TICK).await;
                // A node that stopped leading must fall silent: its silence is
                // what lets the survivors' leases lapse and a successor win.
                if !leading() {
                    continue;
                }
                let due = liveness.lock().ok().and_then(|l| {
                    let quiet = l.last_sent.is_none_or(|t| t.elapsed() >= LIVENESS_TICK);
                    if quiet { l.vote } else { None }
                });
                let Some(vote) = due else { continue };
                // One probe in flight per peer, shared with the transfer path's
                // gate: without it the ticker would pile up probes behind a slow
                // link instead of pacing them.
                if gate
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    continue;
                }
                let probe = AppendEntriesRequest::<TypeConfig> {
                    vote,
                    prev_log_id: None,
                    entries: Vec::new(),
                    leader_commit: None,
                };
                let resolved = resolve_peer(&resolver, target, &addr).await;
                if let (Ok(body), Ok(addrs)) = (serde_json::to_vec(&probe), resolved) {
                    for peer in &addrs {
                        if client
                            .probe(*peer, "POST", RAFT_APPEND_PATH, body.clone(), deadline)
                            .await
                            .is_ok()
                        {
                            break;
                        }
                    }
                }
                gate.store(false, Ordering::Release);
            }
        })
    }

    /// Serialize `rpc` once and start its transfer on a detached task, returning
    /// the slot that later re-sends attach to.
    ///
    /// The body is built here rather than in the task so an attaching re-send
    /// costs no JSON work at all, and so a serialization failure is reported to
    /// the caller that caused it.
    #[allow(clippy::result_large_err)]
    fn spawn_append(
        &self,
        rpc: &AppendEntriesRequest<TypeConfig>,
        last_log_id: Option<LogId<NodeId>>,
    ) -> Result<InflightAppend, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let body = serde_json::to_vec(rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let deadline = replication_deadline(self.client.request_timeout(), body.len());
        let entries = rpc.entries.len() as u64;

        let client = self.client.clone();
        let resolver = Arc::clone(&self.resolver);
        let target = self.target;
        let addr = self.addr.clone();

        let handle = tokio::spawn(async move {
            send_append(&client, &resolver, target, &addr, body, deadline, entries).await
        });

        Ok(InflightAppend {
            vote: rpc.vote,
            prev_log_id: rpc.prev_log_id,
            last_log_id,
            handle,
        })
    }
}

/// Carry one AppendEntries body to `target`, trying each resolved address.
///
/// A free function rather than a method because it runs on a detached task that
/// must outlive the `PeerClient` borrow — that outliving is the entire point of
/// the single-flight slot.
// Same reason as `PeerClient::send`: the `Err` variant is openraft's `RPCError`
// and this result is handed straight back to a `RaftNetwork` method, so the
// size is fixed by that trait rather than ours to shrink.
#[allow(clippy::result_large_err)]
async fn send_append(
    client: &RpcClient,
    resolver: &Arc<dyn PeerResolver>,
    target: NodeId,
    addr: &str,
    body: Vec<u8>,
    deadline: Duration,
    entries: u64,
) -> AppendOutcome {
    let addrs = resolve_peer(resolver, target, addr)
        .await
        .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

    // `call_once`, not `call`: openraft is already the retry loop, so retrying
    // here would re-send the whole body — the very cost this fix exists to stop
    // paying.
    let mut last: Option<RpcError> = None;
    for peer in &addrs {
        match client
            .call_once(*peer, "POST", RAFT_APPEND_PATH, body.clone(), deadline)
            .await
        {
            Ok(response) => {
                return serde_json::from_slice(&response)
                    .map_err(|e| RPCError::Network(NetworkError::new(&e)));
            }
            Err(e) => last = Some(e),
        }
    }

    let context = format!("{addr} ({} address(es) tried)", addrs.len());
    Err(match last {
        Some(e) => map_append_err(&e, &context, entries),
        None => RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
            "{context}: no addresses to try"
        )))),
    })
}

/// AppendEntries-specific error mapping: a follower's 413 is an *answer*, not
/// silence, and must not be reported as unreachability.
///
/// [`map_rpc_err`] sends `BodyTooLarge` to `Unreachable`, which makes openraft
/// back off and then re-send the identical oversized batch — forever, since
/// backing off never makes the batch smaller. `PayloadTooLarge` instead makes it
/// halve the batch and retry at once, which is the only response that can make
/// progress. A lagging follower catching up across several 4-8 MiB entries hits
/// this the moment `max_payload_entries` batches them past the transport's cap;
/// it never fired before the quota-sized entries of #278/#285 existed. Only
/// AppendEntries can act on the hint, so `vote` and `install_snapshot` keep the
/// original mapping.
fn map_append_err(
    e: &RpcError,
    context: &str,
    entries: u64,
) -> RPCError<NodeId, BasicNode, RaftError<NodeId>> {
    if matches!(e, RpcError::BodyTooLarge { .. }) {
        // openraft debug-asserts a positive hint. A single entry that is itself
        // over the cap cannot be halved further and will fail the same way next
        // time, which is the honest outcome — the alternative is claiming a
        // smaller batch exists when it does not.
        return RPCError::PayloadTooLarge(PayloadTooLarge::new_entries_hint((entries / 2).max(1)));
    }
    map_rpc_err(e, context)
}

/// Resolve `authority` through `resolver`, fresh for this one call. The
/// resolver's own lookup can block (the default does a real DNS/hosts lookup),
/// so it always runs on the blocking pool rather than the async runtime.
///
/// The one seam every peer-address resolution in the crate goes through, so a
/// hostname advertise address (issue #68) is resolved identically whether the
/// caller is Raft replication, a seed join, or a leader-hint chase.
/// Returns every address the authority names, in resolver order; callers dial
/// them in turn until one answers (#79, decision D-28).
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
    /// Retry an unreachable peer every 50 ms rather than openraft's default
    /// 500 ms. A restarting voter's election timeout is 150–300 ms, so the
    /// default lets it campaign before the leader ever tries it again (#431).
    fn backoff(&self) -> openraft::network::Backoff {
        openraft::network::Backoff::new(std::iter::repeat(Duration::from_millis(50)))
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        // An entry-less probe is sent inline, exactly as before, and never
        // queues behind a transfer.
        //
        // Note this branch alone does NOT keep the leader's heartbeats flowing
        // during a big transfer: openraft only produces an empty range for a
        // follower that is already caught up. One whose `matching` is behind
        // gets the missing entry instead, so while a large entry is pending
        // this branch is never taken — which is what the liveness ticker
        // exists to compensate for.
        self.note_sent(rpc.vote);
        if rpc.entries.is_empty() {
            return self.send(RAFT_APPEND_PATH, &rpc, Delivery::Retried).await;
        }

        let last_log_id = rpc.entries.last().map(|entry| entry.log_id);

        // Attach only to a transfer based identically and carrying no more than
        // this request wants:
        //   - a different `vote` belongs to another term;
        //   - a different `prev_log_id` is based on a prefix this caller has not
        //     asked to build on;
        //   - a *longer* in-flight range (openraft rewound after a conflict)
        //     would report the follower as holding entries never sent to it.
        let attaches = self.inflight.as_ref().is_some_and(|slot| {
            slot.vote == rpc.vote
                && slot.prev_log_id == rpc.prev_log_id
                && slot.last_log_id <= last_log_id
        });
        if !attaches {
            // Dropping the handle detaches the old task; it runs to its deadline
            // and its result is discarded. The follower rejects a stale-vote
            // request on its own, so an abandoned transfer is harmless.
            self.inflight = None;
            self.inflight = Some(self.spawn_append(&rpc, last_log_id)?);
        }

        let (outcome, sent_through) = {
            let Some(slot) = self.inflight.as_mut() else {
                // Unreachable: the branch above installs a slot whenever there
                // was none. Reported rather than panicked so a future refactor
                // that breaks the invariant degrades to a retry, not a crash.
                return Err(RPCError::Unreachable(Unreachable::new(
                    &std::io::Error::other("no in-flight append transfer to await"),
                )));
            };
            let sent_through = slot.last_log_id;
            // `&mut handle`: if openraft's deadline drops us here, the slot —
            // and the running transfer — survive for the next re-send.
            ((&mut slot.handle).await, sent_through)
        };
        self.inflight = None;

        match outcome {
            // The transfer carried only a prefix of what this caller wants, so
            // report the prefix: openraft advances `matching` and sends the
            // remainder as its own request.
            Ok(Ok(AppendEntriesResponse::Success)) if sent_through < last_log_id => {
                Ok(AppendEntriesResponse::PartialSuccess(sent_through))
            }
            Ok(result) => result,
            Err(join) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(format!("append transfer task failed: {join}")),
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.send(RAFT_VOTE_PATH, &rpc, Delivery::Retried).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        self.note_sent(rpc.vote);
        self.send(RAFT_SNAPSHOT_PATH, &rpc, Delivery::BulkSingleAttempt)
            .await
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

/// The auto-voter ceiling a node enforces — one value, read by **both** phases
/// of admission (#433): the join handler's in-call promotion and the leader's
/// promotion sweep. Shared rather than a constant so the two can never
/// disagree, and so a test can lower it on one node and have the sweep honor
/// the same bound. Defaults to [`MAX_AUTO_VOTERS`].
pub(crate) type AutoVoterCeiling = Arc<std::sync::atomic::AtomicUsize>;

/// Register the control-plane receiving endpoints (Raft RPCs + seed join) onto
/// `router`, all reading the node through `slot`.
///
/// `gate` is supplied by the caller rather than created here because the node
/// itself also evicts locally — when the departing node *is* the leader — and
/// that path has to share this serialization to keep the floor exact.
/// `ceiling` likewise: both admission phases must read the one value.
#[must_use]
pub(crate) fn control_routes(
    router: Router,
    slot: RaftSlot,
    gate: MembershipGate,
    ceiling: AutoVoterCeiling,
) -> Router {
    let admission_ceiling = ceiling;
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
                let ceiling = Arc::clone(&admission_ceiling);
                Box::pin(async move {
                    let raft = raft_of(&slot)?;
                    let req = decode::<JoinRequest>(&body)?;
                    let admission = admit(
                        raft,
                        &gate,
                        req.node_id,
                        req.advertise,
                        ceiling.load(std::sync::atomic::Ordering::Relaxed),
                    )
                    .await?;
                    encode(&JoinAccepted {
                        admitted: true,
                        role: Some(admission.role),
                        catching_up: admission.catching_up,
                    })
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
///
/// `role` and `catching_up` arrived with two-phase admission (#433). Both
/// default, so a reply from an older leader — the bare `{"admitted":true}` —
/// still decodes: `role: None` (unknown) and `catching_up: false`, which is
/// the only thing an old leader's `Ok` could mean (its admission included the
/// full blocking catch-up). An older joiner reading a new leader's reply
/// ignores the extra fields. A mixed-version fleet joins either way.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinAccepted {
    pub(crate) admitted: bool,
    #[serde(default)]
    pub(crate) role: Option<AdmittedRole>,
    #[serde(default)]
    pub(crate) catching_up: bool,
}

/// What the joiner is a member *as*, after phase-1 admission (#433).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum AdmittedRole {
    Learner,
    Voter,
}

/// Phase-1 admission outcome (#433): the membership entry is committed and
/// the node **is a member** — that is the fast, consensus-bound fact `admit`
/// reports. Whether the member is *current* is a slow, replication-bound fact;
/// `catching_up` carries the leader's estimate of it, and the leader's
/// promotion sweep ([`promote_ready_learners`]) acts on it later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Admission {
    pub(crate) role: AdmittedRole,
    pub(crate) catching_up: bool,
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

/// Admit `id`@`advertise` to the cluster — **phase 1 of two-phase admission**
/// (#433, the etcd learner pattern): commit the membership entry that makes
/// the node a learner, and return. Catch-up is not waited for; it is a
/// replication-bound fact the reply reports as `catching_up` and the leader's
/// [`promote_ready_learners`] sweep acts on later. A joiner that is already
/// current is still promoted here, under the same gate and ceiling as before,
/// so a small fleet forms voters in one call exactly as it always did.
/// Must run on the leader — on any other node openraft returns a
/// `ForwardToLeader` error, surfaced here as a typed [`RpcError::NotLeader`]
/// carrying the leader's address so the caller can re-issue there (#391).
///
/// The handler passes [`MAX_AUTO_VOTERS`]; tests pass a small ceiling to
/// provoke the race without an 11-node cluster.
///
/// This is the only way a member enters the fleet (decision D-21): admission is
/// initiated by the joining node over the signed cluster port, and no admin
/// route or console action adds a learner or a voter.
pub(crate) async fn admit(
    raft: &Raft<TypeConfig>,
    gate: &Mutex<()>,
    id: NodeId,
    advertise: String,
    max_voters: usize,
) -> Result<Admission, RpcError> {
    // Validated here as well as at the joiner's CLI, because this is the path
    // that actually writes an address into the replicated log: whatever arrives
    // on the wire becomes a durable membership entry, and one that can never
    // resolve is removable only by an admin membership change (issue #68).
    let advertise = advertise
        .parse::<Authority>()
        .map_err(|e| RpcError::Handler(format!("admit {id}: {e}")))?;

    // Concurrent joins are the normal case (a StatefulSet rollout seeds every
    // pod off the same node), so failures name the candidate they belong to.
    //
    // `blocking = false` is the load-bearing word of #433: with `true`,
    // admission *included* the joiner's full catch-up, which for a multi-MiB
    // snapshot cannot fit `ADMIT_COMMIT_TIMEOUT` by construction — and a
    // catch-up longer than the seed deadline failed startup for a node Raft
    // already listed as a member. Non-blocking, this waits (inside
    // `membership_change`) only for the membership *entry* to commit: the
    // fast, consensus-bound half of the job. The slow half belongs to
    // replication and to [`promote_ready_learners`].
    membership_change(raft, &format!("admit {id}: add learner"), || {
        raft.add_learner(id, BasicNode::new(advertise.to_string()), false)
    })
    .await?;

    // Promote incrementally with `AddVoterIds`, never by replacing the whole
    // voter set (decision D-27): building a `ReplaceAllVoters` set from any
    // local view would let two concurrent joins each overwrite the other's
    // just-added voter — demoting a live member with no error. `AddVoterIds`
    // only ever adds, so the ceiling read below is a soft gate on *whether* to
    // auto-promote, never the source of the new membership.
    //
    // The gate makes the ceiling exact rather than best-effort (#55): every
    // promotion ends with a wait for its entry to apply (applied ⇒ committed),
    // so by the time a guard drops, the next holder's committed read includes
    // all prior auto-promotions. Without it, N concurrent admissions each read
    // a pre-promotion count and all pass the `< max_voters` check.
    // Give the joiner one short window to become current before deciding its
    // role. A fresh or small fleet's joiner is current within milliseconds,
    // and the admit-promotes-in-one-call contract is worth keeping for it —
    // every caller from a three-node bootstrap to the failover tests relies
    // on a returned join meaning a formed voter. The window is a fraction of
    // the joiner's RPC budget, so the case #433 exists for — a multi-MiB
    // snapshot catch-up — still returns fast, as a learner with
    // `catching_up: true`, and the promotion sweep takes it from there. An
    // already-current voter (a restart) skips the wait entirely.
    if !committed_voters(raft, id).await?.contains(&id) {
        let currency_deadline = tokio::time::Instant::now() + ADMIT_CURRENCY_WAIT;
        while is_catching_up(raft, id) && tokio::time::Instant::now() < currency_deadline {
            tokio::time::sleep(ADMIT_CURRENCY_POLL).await;
        }
    }

    let _serialized = gate.lock().await;
    let voters = committed_voters(raft, id).await?;
    let catching_up = is_catching_up(raft, id);
    if should_promote(&voters, id, max_voters) && !catching_up {
        membership_change(raft, &format!("admit {id}: promote to voter"), || {
            raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([id])), false)
        })
        .await?;
        return Ok(Admission {
            role: AdmittedRole::Voter,
            catching_up: false,
        });
    }
    if voters.contains(&id) {
        // A retried join from a node that is already a voter (a restart):
        // nothing to do, but leave a trace so the three-way outcome is always
        // greppable.
        tracing::debug!(
            node_id = id,
            catching_up,
            "join for an existing voter; membership unchanged"
        );
        return Ok(Admission {
            role: AdmittedRole::Voter,
            catching_up,
        });
    }
    if should_promote(&voters, id, max_voters) {
        tracing::info!(
            node_id = id,
            "admitted as learner, still catching up; the promotion sweep takes it from here"
        );
    } else {
        tracing::info!(
            node_id = id,
            voters = voters.len(),
            max_voters,
            "auto-voter ceiling reached; admitted as learner only"
        );
    }
    Ok(Admission {
        role: AdmittedRole::Learner,
        catching_up,
    })
}

/// Whether `id`'s replication trails this leader by more than
/// [`REPLICATION_LAG_THRESHOLD`] entries — or has no replication stream data
/// yet at all, which a just-added learner does not and must read as "not
/// caught up". Leader-only: on a non-leader `replication` is `None` and every
/// peer reads as catching up, which is the safe direction for both callers.
fn is_catching_up(raft: &Raft<TypeConfig>, id: NodeId) -> bool {
    let metrics = raft.metrics();
    let m = metrics.borrow();
    let Some(last) = m.last_log_index else {
        return true;
    };
    let matching = m
        .replication
        .as_ref()
        .and_then(|r| r.get(&id).copied().flatten());
    match matching {
        Some(log_id) => last.saturating_sub(log_id.index) > REPLICATION_LAG_THRESHOLD,
        None => true,
    }
}

/// How long `admit` waits for a fresh learner to become current before
/// answering with `catching_up: true` — the fast path's budget, not a
/// correctness bound. Well inside `ADMIT_COMMIT_TIMEOUT` and the joiner's own
/// RPC budget, both of which bounded the whole handler before #433.
const ADMIT_CURRENCY_WAIT: Duration = Duration::from_millis(500);

/// The poll cadence inside [`ADMIT_CURRENCY_WAIT`]'s window.
const ADMIT_CURRENCY_POLL: Duration = Duration::from_millis(20);

/// A learner within this many entries of the leader's log converges within
/// one replication round, so promoting it cannot stall the membership change;
/// farther behind, promotion waits for [`promote_ready_learners`] to observe
/// it caught up. Entries, not bytes: openraft replicates in entry batches and
/// its own catch-up accounting is entry-indexed.
const REPLICATION_LAG_THRESHOLD: u64 = 16;

/// **Phase 2 of two-phase admission** (#433): promote every caught-up learner
/// in the committed membership, under the admission gate and beneath the
/// auto-voter ceiling — the same gate and the same `should_promote` as
/// admission itself, so the ceiling stays exact under concurrency (#55).
///
/// Driven by the leader's promotion loop on a fixed cadence; call it anywhere
/// else and it is a cheap no-op (a non-leader sees every peer as catching up,
/// and a promotion racing a leadership change surfaces `ForwardToLeader`,
/// which the loop tolerates by trying again next tick). Idempotent: a voter
/// is not a learner, so a promoted node drops out of the scan.
pub(crate) async fn promote_ready_learners(
    raft: &Raft<TypeConfig>,
    gate: &Mutex<()>,
    max_voters: usize,
) -> Result<(), RpcError> {
    let members: Vec<NodeId> = raft
        .with_raft_state(|state| {
            state
                .membership_state
                .committed()
                .nodes()
                .map(|(id, _)| *id)
                .collect()
        })
        .await
        .map_err(|e| RpcError::Handler(e.to_string()))?;

    for id in members {
        // Re-read the voter set per candidate: each promotion changes it, and
        // the ceiling must be enforced against what has actually committed.
        let voters = committed_voters(raft, id).await?;
        if voters.contains(&id) || !should_promote(&voters, id, max_voters) {
            continue;
        }
        if is_catching_up(raft, id) {
            continue;
        }
        let _serialized = gate.lock().await;
        // The gate was taken after the scan read; re-check under it so a
        // concurrent admission's promotion is seen (#55's exactness argument).
        let voters = committed_voters(raft, id).await?;
        if !should_promote(&voters, id, max_voters) {
            continue;
        }
        membership_change(raft, &format!("promote learner {id}"), || {
            raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([id])), false)
        })
        .await?;
        tracing::info!(node_id = id, "caught-up learner promoted to voter");
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

/// The two voter sets a blob fan-out must satisfy a majority of *both* of
/// (#438, D-19): the committed configuration and the effective one.
///
/// Neither alone is sound, which is why this is a pair rather than a choice:
///
/// - **committed only** — a cluster growing 3→5 with the new config still
///   uncommitted has a committed majority of 2. The op commits, the membership
///   commits after it, and the blob is on 2 of 5: not a majority of the
///   configuration now in force.
/// - **effective only** — effective membership can carry an uncommitted entry
///   from a deposed leader that later truncates (the hazard
///   [`committed_voters`] documents). If the truncated config had *removed*
///   nodes, the majority fanned to may not be a majority of the config that
///   survives.
///
/// A majority of both is the joint-consensus rule Raft itself requires while a
/// joint configuration is in flight, and it is what makes the set of holders
/// one that no single membership change can empty — the precondition #439's
/// fetch-on-apply depends on. Fix this rule and the residual propose-window
/// stops mattering; leave it committed-only and no amount of fetch-on-apply
/// recovers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuorumTargets {
    committed: BTreeSet<NodeId>,
    effective: BTreeSet<NodeId>,
}

impl QuorumTargets {
    #[cfg(test)]
    pub(crate) fn new(committed: BTreeSet<NodeId>, effective: BTreeSet<NodeId>) -> Self {
        Self {
            committed,
            effective,
        }
    }

    /// Every node worth sending to: the union, since a node in either
    /// configuration can count toward that configuration's majority.
    pub(crate) fn members(&self) -> BTreeSet<NodeId> {
        self.committed.union(&self.effective).copied().collect()
    }

    /// Whether `acks` carries a majority of **both** configurations.
    pub(crate) fn satisfied_by(&self, acks: &BTreeSet<NodeId>) -> bool {
        majority_of(&self.committed, acks) && majority_of(&self.effective, acks)
    }

    /// Whether the two configurations differ — i.e. a membership change is in
    /// flight and the second majority is doing real work.
    pub(crate) fn is_joint(&self) -> bool {
        self.committed != self.effective
    }
}

/// Whether `acks` contains a strict majority of `config`.
///
/// Acks from nodes outside `config` are ignored rather than counted: a learner
/// or a departing node holding the blob is not evidence about *this*
/// configuration's durability.
///
/// An **empty** configuration is not satisfied. A real cluster always has at
/// least one voter, so empty means the membership has not loaded yet — the
/// question cannot be answered, and this decides whether a write is durable
/// enough to commit. Answering "yes" to a question that could not be evaluated
/// is the one direction that is unrecoverable, so the unknown case takes the
/// refusing branch.
fn majority_of(config: &BTreeSet<NodeId>, acks: &BTreeSet<NodeId>) -> bool {
    if config.is_empty() {
        return false;
    }
    let held = config.intersection(acks).count();
    held * 2 > config.len()
}

/// Read both voter sets in **one** `with_raft_state` closure (#438, D-19).
///
/// One read, not two: separate reads can observe different membership epochs,
/// and a "joint" pair assembled from two epochs describes a configuration that
/// never existed — the precise failure the joint rule exists to prevent.
///
/// # Errors
///
/// [`RpcError::Handler`] if the RaftCore loop cannot be reached to answer.
pub(crate) async fn joint_voters(raft: &Raft<TypeConfig>) -> Result<QuorumTargets, RpcError> {
    raft.with_raft_state(|state| QuorumTargets {
        committed: state.membership_state.committed().voter_ids().collect(),
        effective: state.membership_state.effective().voter_ids().collect(),
    })
    .await
    .map_err(|e| RpcError::Handler(format!("reading membership for blob fan-out: {e}")))
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
/// ([`MIN_VOTERS`], [`held_by_floor`]; decision D-25) refuses a departure that
/// would leave the cluster with too few voters, and that refusal is a successful outcome —
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
                // Checked before the generic arm: openraft answers a membership
                // change on a non-leader with `ForwardToLeader`, and folding
                // that into a `Handler` string is what stranded a node whose
                // only seed was a follower — the leader's address survived only
                // as prose nobody could act on (#391).
                if let Some(leader) = forward_to_leader(&e) {
                    return Err(RpcError::NotLeader { leader });
                }
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

/// openraft's "you are asking the wrong node" rejection, carrying the leader's
/// advertise address when it knows one. The outer `Option` distinguishes "this
/// is a redirect" from "this is some other error"; the inner one carries the
/// hint, which is absent while an election is unsettled.
///
/// Structural match on the typed error for the same reason [`in_progress`] is:
/// the rendered message is not an interface.
fn forward_to_leader(e: &MembershipError) -> Option<Option<String>> {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
            Some(forward.leader_node.as_ref().map(|node| node.addr.clone()))
        }
        _ => None,
    }
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

    // ---- joint-consensus quorum (#438) ----------------------------------
    //
    // The rule the maintainer ruled on, and the one piece of #438 that can be
    // checked without a cluster. Every expectation below is a literal: a
    // three-node majority is 2 because 2 is written here, never because the
    // implementation says so.

    fn ids(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    #[test]
    fn a_stable_configuration_needs_a_simple_majority() {
        let targets = QuorumTargets::new(ids(&[1, 2, 3]), ids(&[1, 2, 3]));

        assert!(!targets.is_joint());
        assert!(
            !targets.satisfied_by(&ids(&[1])),
            "1 of 3 is not a majority"
        );
        assert!(targets.satisfied_by(&ids(&[1, 2])), "2 of 3 is");
        assert!(targets.satisfied_by(&ids(&[1, 2, 3])));
    }

    /// Pins D-19: the fan-out quorum is a majority of BOTH the committed and the
    /// effective voter configuration — growing 3→5, the committed majority of 2 is
    /// not enough, because the blob would sit on 2 of the 5 nodes now in force.
    #[test]
    fn growing_three_to_five_is_not_satisfied_by_the_committed_majority_alone() {
        // The case that makes committed-only unsound, and the concrete reason
        // this is a pair rather than a choice. Committed is {1,2,3} and its
        // majority is 2; effective is {1..5} and needs 3. Acking {1,2} would
        // commit the op with the blob on 2 of the 5 nodes now in force.
        let targets = QuorumTargets::new(ids(&[1, 2, 3]), ids(&[1, 2, 3, 4, 5]));

        assert!(targets.is_joint());
        assert!(
            !targets.satisfied_by(&ids(&[1, 2])),
            "a committed majority alone must not pass while the config is growing"
        );
        assert!(targets.satisfied_by(&ids(&[1, 2, 3])));
    }

    /// Pins D-19: the effective majority alone is not enough either — shrinking
    /// 5→3, {1,2} is a majority of the effective set but not of the committed one.
    #[test]
    fn shrinking_five_to_three_is_not_satisfied_by_the_effective_majority_alone() {
        // The mirror case, which is what makes effective-only unsound: the
        // effective entry can truncate under a deposed leader. Effective
        // {1,2,3} needs 2; committed {1..5} needs 3, so {1,2} must fail.
        let targets = QuorumTargets::new(ids(&[1, 2, 3, 4, 5]), ids(&[1, 2, 3]));

        assert!(targets.is_joint());
        assert!(
            !targets.satisfied_by(&ids(&[1, 2])),
            "an effective majority alone must not pass while the config is shrinking"
        );
        assert!(targets.satisfied_by(&ids(&[1, 2, 3])));
    }

    /// Pins D-19: an ack from a node outside a configuration does not count toward
    /// that configuration's majority — a learner holding the blob is not evidence
    /// of the voters' durability.
    #[test]
    fn an_ack_from_outside_the_configuration_does_not_count() {
        // Edge 2: the accepting node adds itself to the acks unconditionally
        // because it has just stored the blob. When it is a learner rather than
        // a voter, that ack must be ignored — a non-voter holding the blob says
        // nothing about this configuration's durability. The intersection is
        // what makes the learner case fall out rather than need its own branch.
        let targets = QuorumTargets::new(ids(&[1, 2, 3]), ids(&[1, 2, 3]));

        assert!(
            !targets.satisfied_by(&ids(&[1, 99])),
            "node 99 is not a voter; 1 of 3 is still not a majority"
        );
        assert!(targets.satisfied_by(&ids(&[1, 2, 99])));
    }

    #[test]
    fn a_single_node_configuration_is_satisfied_by_itself() {
        // Edge 3: the solo case must not dial anyone or wait for anyone.
        let targets = QuorumTargets::new(ids(&[7]), ids(&[7]));

        assert!(targets.satisfied_by(&ids(&[7])));
        assert!(!targets.satisfied_by(&ids(&[])));
    }

    #[test]
    fn members_is_the_union_so_a_node_in_either_configuration_is_dialled() {
        // Fanning out to only one configuration's members would make the other
        // majority unreachable by construction.
        let targets = QuorumTargets::new(ids(&[1, 2, 3]), ids(&[3, 4, 5]));

        assert_eq!(targets.members(), ids(&[1, 2, 3, 4, 5]));
    }

    #[test]
    fn an_unloaded_membership_is_never_a_quorum() {
        // Empty means the membership has not loaded, not that the bar is zero.
        // This decides whether a write is durable enough to commit, so the
        // unanswerable case refuses rather than waves through.
        let targets = QuorumTargets::new(ids(&[]), ids(&[]));

        assert!(!targets.satisfied_by(&ids(&[])));
        assert!(!targets.satisfied_by(&ids(&[1, 2, 3])));
    }

    #[test]
    fn a_four_node_configuration_needs_three_not_two() {
        // An even configuration is where an off-by-one in the majority rule
        // hides: `held * 2 > len` gives 3, while `>=` would wrongly accept 2.
        let targets = QuorumTargets::new(ids(&[1, 2, 3, 4]), ids(&[1, 2, 3, 4]));

        assert!(
            !targets.satisfied_by(&ids(&[1, 2])),
            "2 of 4 is a tie, not a majority"
        );
        assert!(targets.satisfied_by(&ids(&[1, 2, 3])));
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
    /// A pre-#433 leader answers the bare `{"admitted":true}`. It must decode
    /// as "member, role unknown, not catching up" — the only thing that
    /// leader's blocking admission could have meant — so a mixed-version
    /// fleet still joins.
    #[test]
    fn an_old_leaders_bare_join_reply_still_decodes() {
        let accepted: JoinAccepted =
            serde_json::from_slice(br#"{"admitted":true}"#).expect("old wire shape decodes");
        assert!(accepted.admitted);
        assert_eq!(accepted.role, None);
        assert!(!accepted.catching_up);
    }

    /// The other direction of the same skew: an old joiner must be able to
    /// ignore the two fields a new leader adds. serde ignores unknown fields
    /// by default, so this pins that no `deny_unknown_fields` sneaks onto the
    /// old shape's stand-in.
    #[test]
    fn a_new_leaders_join_reply_carries_role_and_catch_up() {
        let encoded = serde_json::to_vec(&JoinAccepted {
            admitted: true,
            role: Some(AdmittedRole::Learner),
            catching_up: true,
        })
        .expect("encode");
        let accepted: JoinAccepted = serde_json::from_slice(&encoded).expect("round trip");
        assert_eq!(accepted.role, Some(AdmittedRole::Learner));
        assert!(accepted.catching_up);
    }

    /// Pins D-27: the ceiling is a soft gate on *whether* to auto-promote — at or
    /// over it no promotion happens, an existing voter is never re-promoted, and
    /// an over-grown voter set is never shrunk to fit.
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
    ///
    /// Pins D-25: the floor is two voters — a voter's departure from a
    /// two-voter set is refused, from three it is allowed, and a learner is
    /// never held.
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
    ///
    /// Pins D-28: every resolved address is dialled, in the resolver's order,
    /// until one answers — none is skipped or preferred by family.
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
        let mut network = RpcNetwork::new(client, resolver, Arc::new(|| true));
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
        let mut network = RpcNetwork::new(client, resolver, Arc::new(|| true));
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
    ///
    /// Pins D-28: a peer's hostname is re-resolved on every send.
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
        let mut network = RpcNetwork::new(client, resolver, Arc::new(|| true));

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

    // ---- #411: single-flight AppendEntries transfers -------------------------
    //
    // openraft wraps every AppendEntries call in `timeout(heartbeat_interval)` —
    // 50 ms here — and drops the future when it fires, so a body that cannot
    // cross the link and fsync inside one heartbeat is restarted from byte 0
    // forever and the entry never commits. These tests pin the fix: the transfer
    // outlives the deadline, and an identical re-send *attaches* to it.
    //
    // Every one of them asserts on the responder's REQUEST COUNT, not just on
    // the reply. A test that only checked the reply would pass against an
    // implementation that faithfully re-sent the whole body every 50 ms, which
    // is precisely the bug.

    /// What an append responder answers once it has consumed a whole request.
    #[derive(Clone, Copy)]
    enum AppendReply {
        Success,
        /// The receiving transport's 413 — body over `DEFAULT_MAX_BODY_BYTES`.
        BodyTooLarge,
        /// A handler-side 500.
        ServerError,
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    /// Read one HTTP request off `socket`, draining its whole body.
    ///
    /// The body must be consumed even though the responder ignores its content:
    /// replying while the client is still writing would answer a request the
    /// responder had not actually received, which is the very thing these tests
    /// are counting.
    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> Option<Option<bool>> {
        use tokio::io::AsyncReadExt;

        let mut buf = Vec::new();
        let mut chunk = [0_u8; 8192];
        let head_end = loop {
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                break pos + 4;
            }
            let n = socket.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
        };

        let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
        let len: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);

        while buf.len() < head_end + len {
            let n = socket.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
        }

        // Report whether this request actually carried entries. The single-flight
        // claim is about the BODY, and the adapter also sends empty keepalive
        // probes while a transfer runs — counting both together would let a
        // re-sent 8 MiB body hide behind a legitimate probe.
        let body = serde_json::from_slice::<serde_json::Value>(&buf[head_end..]).ok();
        let carried_entries = body
            .as_ref()
            .and_then(|v| {
                v.get("entries")
                    .and_then(|e| e.as_array().map(|a| !a.is_empty()))
            })
            .unwrap_or(false);
        if carried_entries {
            return Some(Some(true));
        }
        // No entries: a probe has no `prev_log_id` (what openraft sends a fresh
        // target, and what the liveness ticker sends); an ordinary heartbeat
        // carries one and is counted as neither, so a test can assert the ticker
        // stayed silent while openraft was heartbeating.
        let is_probe = body
            .as_ref()
            .and_then(|v| v.get("prev_log_id"))
            .is_none_or(|p| p.is_null());
        Some(if is_probe { Some(false) } else { None })
    }

    /// A responder for `RAFT_APPEND_PATH` that answers `reply` after `delay`,
    /// counting every request that actually reached it.
    async fn spawn_append_responder(
        delay: Duration,
        reply: AppendReply,
    ) -> (
        SocketAddr,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind append responder");
        let addr = listener.local_addr().expect("responder addr");
        let bodies = Arc::new(AtomicUsize::new(0));
        let probes = Arc::new(AtomicUsize::new(0));
        let (body_counter, probe_counter) = (bodies.clone(), probes.clone());

        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let (body_counter, probe_counter) = (body_counter.clone(), probe_counter.clone());
                tokio::spawn(async move {
                    match read_http_request(&mut socket).await {
                        None => return,
                        Some(Some(true)) => body_counter.fetch_add(1, Ordering::SeqCst),
                        Some(Some(false)) => probe_counter.fetch_add(1, Ordering::SeqCst),
                        Some(None) => 0,
                    };
                    tokio::time::sleep(delay).await;

                    let (status, body) = match reply {
                        AppendReply::Success => (
                            "200 OK",
                            serde_json::to_vec(&AppendEntriesResponse::<NodeId>::Success)
                                .expect("encode append reply"),
                        ),
                        AppendReply::BodyTooLarge => (
                            "413 Payload Too Large",
                            br#"{"message":"request body exceeds 32 bytes"}"#.to_vec(),
                        ),
                        AppendReply::ServerError => (
                            "500 Internal Server Error",
                            br#"{"message":"responder refused"}"#.to_vec(),
                        ),
                    };
                    let head = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                    let _ = socket.flush().await;
                });
            }
        });
        (addr, bodies, probes, handle)
    }

    /// Build a `PeerClient` aimed at `addr` with no retries, so a test observes
    /// exactly the adapter's own behaviour. The node is treated as leading, so
    /// the liveness ticker is armed.
    async fn peer_client_for(addr: SocketAddr) -> PeerClient {
        peer_client_with_leading(addr, Arc::new(std::sync::atomic::AtomicBool::new(true))).await
    }

    /// [`peer_client_for`], with the caller holding the `leading` flag. The
    /// flag is wrapped into the production [`LeadingProbe`] shape, so the
    /// ticker reads it at the moment of each decision exactly as it would
    /// read the metrics watch.
    async fn peer_client_with_leading(
        addr: SocketAddr,
        leading: Arc<std::sync::atomic::AtomicBool>,
    ) -> PeerClient {
        let leading: LeadingProbe =
            Arc::new(move || leading.load(std::sync::atomic::Ordering::Acquire));
        use crate::rpc::{AlwaysHealthy, RpcClientConfig};
        use openraft::network::RaftNetworkFactory;

        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let resolver: Arc<dyn PeerResolver> = Arc::new(ListResolver {
            addrs: vec![addr],
            calls,
        });
        let client = RpcClient::new(
            None,
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                connect_timeout: Duration::from_millis(200),
                request_timeout: Duration::from_millis(500),
                max_retries: 0,
            },
        );
        let mut network = RpcNetwork::new(client, resolver, leading);
        network
            .new_client(2, &BasicNode::new(addr.to_string()))
            .await
    }

    fn append_req(term: u64, prev: Option<u64>, last: u64) -> AppendEntriesRequest<TypeConfig> {
        use openraft::EntryPayload;
        use openraft::Vote;
        use openraft::entry::Entry;

        let first = prev.map_or(1, |p| p + 1);
        let entries = (first..=last)
            .map(|i| Entry {
                log_id: log_id(i),
                payload: EntryPayload::<TypeConfig>::Blank,
            })
            .collect();
        AppendEntriesRequest {
            vote: Vote::new_committed(term, 1),
            prev_log_id: prev.map(log_id),
            entries,
            leader_commit: prev.map(log_id),
        }
    }

    /// THE gate for #411: a transfer that outlives openraft's 50 ms deadline is
    /// not restarted — the identical re-send attaches to it and gets its answer.
    ///
    /// Pre-fix this test fails on the request count: the second call re-sends
    /// the whole body, so the responder sees two.
    #[tokio::test]
    async fn an_identical_resend_attaches_to_the_inflight_transfer() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        // Answers well after openraft's per-call deadline, so the first waiter
        // is guaranteed to be dropped while the transfer is still running.
        let (addr, seen, probes, _guard) =
            spawn_append_responder(Duration::from_millis(300), AppendReply::Success).await;
        let mut peer = peer_client_for(addr).await;

        let first = tokio::time::timeout(
            Duration::from_millis(50),
            peer.append_entries(
                append_req(1, None, 3),
                RPCOption::new(Duration::from_millis(50)),
            ),
        )
        .await;
        assert!(
            first.is_err(),
            "the 50 ms deadline must fire while the transfer is still in flight"
        );

        let second = peer
            .append_entries(
                append_req(1, None, 3),
                RPCOption::new(Duration::from_millis(50)),
            )
            .await;
        assert!(
            matches!(second, Ok(AppendEntriesResponse::Success)),
            "the re-send must attach to the in-flight transfer and return its success, got {second:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "the BODY must have been sent exactly ONCE; more means the transfer was restarted"
        );
        assert!(
            probes.load(Ordering::SeqCst) >= 1,
            "attaching must also ping the follower so its election timer resets \
             while the transfer is still crossing the link"
        );
    }

    /// A re-send that extends the range reports the in-flight prefix as
    /// `PartialSuccess`, so openraft sends only the remainder.
    #[tokio::test]
    async fn a_longer_resend_returns_partial_success_for_the_inflight_prefix() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        let (addr, seen, _probes, _guard) =
            spawn_append_responder(Duration::from_millis(300), AppendReply::Success).await;
        let mut peer = peer_client_for(addr).await;

        let first = tokio::time::timeout(
            Duration::from_millis(50),
            peer.append_entries(
                append_req(1, None, 3),
                RPCOption::new(Duration::from_millis(50)),
            ),
        )
        .await;
        assert!(first.is_err(), "first call must time out");

        // Same vote and prev_log_id, but openraft now wants through index 6.
        let second = peer
            .append_entries(
                append_req(1, None, 6),
                RPCOption::new(Duration::from_millis(50)),
            )
            .await;
        assert_eq!(
            second.ok(),
            Some(AppendEntriesResponse::PartialSuccess(Some(log_id(3)))),
            "the in-flight transfer only carried through index 3, so that is the matching id"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "attaching must not re-send the body"
        );
    }

    /// A different `prev_log_id` is a different transfer — it must not be
    /// answered by the in-flight one, which is based on a prefix the follower
    /// may never have accepted.
    #[tokio::test]
    async fn a_resend_with_a_different_prev_log_id_starts_a_fresh_transfer() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        let (addr, seen, _probes, _guard) =
            spawn_append_responder(Duration::from_millis(150), AppendReply::Success).await;
        let mut peer = peer_client_for(addr).await;

        let first = tokio::time::timeout(
            Duration::from_millis(50),
            peer.append_entries(
                append_req(1, None, 3),
                RPCOption::new(Duration::from_millis(50)),
            ),
        )
        .await;
        assert!(first.is_err(), "first call must time out");

        let second = peer
            .append_entries(
                append_req(1, Some(7), 9),
                RPCOption::new(Duration::from_millis(2000)),
            )
            .await;
        assert!(
            matches!(second, Ok(AppendEntriesResponse::Success)),
            "a differently-based request must be sent on its own, got {second:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            2,
            "a different prev_log_id must NOT be served by the in-flight transfer"
        );
    }

    /// A vote change means a new leader term; the in-flight transfer belongs to
    /// the old one and cannot answer for it.
    #[tokio::test]
    async fn a_resend_under_a_different_vote_starts_a_fresh_transfer() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        let (addr, seen, _probes, _guard) =
            spawn_append_responder(Duration::from_millis(150), AppendReply::Success).await;
        let mut peer = peer_client_for(addr).await;

        let first = tokio::time::timeout(
            Duration::from_millis(50),
            peer.append_entries(
                append_req(1, None, 3),
                RPCOption::new(Duration::from_millis(50)),
            ),
        )
        .await;
        assert!(first.is_err(), "first call must time out");

        let second = peer
            .append_entries(
                append_req(2, None, 3),
                RPCOption::new(Duration::from_millis(2000)),
            )
            .await;
        assert!(
            matches!(second, Ok(AppendEntriesResponse::Success)),
            "a new term must get its own transfer, got {second:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            2,
            "a different vote must NOT be served by the in-flight transfer"
        );
    }

    /// openraft rewinds the range after a conflict. The in-flight transfer
    /// carries MORE than the caller now wants, so reporting it as matching
    /// would claim the follower holds entries it was never sent.
    #[tokio::test]
    async fn a_shrunk_range_starts_a_fresh_transfer() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        let (addr, seen, _probes, _guard) =
            spawn_append_responder(Duration::from_millis(150), AppendReply::Success).await;
        let mut peer = peer_client_for(addr).await;

        let first = tokio::time::timeout(
            Duration::from_millis(50),
            peer.append_entries(
                append_req(1, None, 6),
                RPCOption::new(Duration::from_millis(50)),
            ),
        )
        .await;
        assert!(first.is_err(), "first call must time out");

        let second = peer
            .append_entries(
                append_req(1, None, 3),
                RPCOption::new(Duration::from_millis(2000)),
            )
            .await;
        assert!(
            matches!(second, Ok(AppendEntriesResponse::Success)),
            "a rewound range must be sent on its own, got {second:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            2,
            "a shrunk range must NOT be answered by the longer in-flight transfer"
        );
    }

    /// Heartbeats share the replication stream. If one queued behind an 8 MiB
    /// transfer the leader would look dead for the whole upload and the cluster
    /// would elect around it — trading the bug for a worse one.
    #[tokio::test]
    async fn a_heartbeat_is_not_queued_behind_an_inflight_transfer() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        let (addr, seen, probes, _guard) =
            spawn_append_responder(Duration::from_millis(400), AppendReply::Success).await;
        let mut peer = peer_client_for(addr).await;

        let first = tokio::time::timeout(
            Duration::from_millis(50),
            peer.append_entries(
                append_req(1, None, 3),
                RPCOption::new(Duration::from_millis(50)),
            ),
        )
        .await;
        assert!(first.is_err(), "first call must time out");

        // An empty-entries probe: it must reach the peer on its own connection
        // rather than waiting for the 400 ms transfer to finish.
        let mut heartbeat = append_req(1, None, 3);
        heartbeat.entries.clear();
        let beat = peer
            .append_entries(heartbeat, RPCOption::new(Duration::from_millis(2000)))
            .await;

        assert!(
            matches!(beat, Ok(AppendEntriesResponse::Success)),
            "a heartbeat must be answered while a transfer is in flight, got {beat:?}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "the heartbeat carries no entries, so the body must still have been sent once"
        );
        assert!(
            probes.load(Ordering::SeqCst) >= 1,
            "the heartbeat must reach the peer as its own entry-less request, \
             never coalesced into the transfer"
        );
    }

    /// A failed transfer must surface and clear the slot, so the next attempt is
    /// a real retry rather than a replay of the same cached failure.
    #[tokio::test]
    async fn a_transfer_error_surfaces_and_clears_the_slot() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        let (addr, seen, _probes, _guard) =
            spawn_append_responder(Duration::from_millis(0), AppendReply::ServerError).await;
        let mut peer = peer_client_for(addr).await;

        let first = peer
            .append_entries(
                append_req(1, None, 3),
                RPCOption::new(Duration::from_millis(2000)),
            )
            .await;
        assert!(
            first.is_err(),
            "a 500 must surface as an error, got {first:?}"
        );

        let second = peer
            .append_entries(
                append_req(1, None, 3),
                RPCOption::new(Duration::from_millis(2000)),
            )
            .await;
        assert!(
            second.is_err(),
            "the retry must also fail against a still-broken peer"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            2,
            "a cleared slot means the retry is a real request, not a cached failure"
        );
    }

    /// A follower's 413 is an *answer*, not silence. Mapping it to `Unreachable`
    /// makes openraft back off and re-send the identical oversized batch
    /// forever; `PayloadTooLarge` makes it halve the batch and retry at once.
    #[tokio::test]
    async fn a_follower_413_maps_to_payload_too_large_with_a_halved_hint() {
        use openraft::network::{RPCOption, RaftNetwork};

        let (addr, _seen, _probes, _guard) =
            spawn_append_responder(Duration::from_millis(0), AppendReply::BodyTooLarge).await;
        let mut peer = peer_client_for(addr).await;

        // Eight entries in the batch, so the hint must be four.
        let err = peer
            .append_entries(
                append_req(1, None, 8),
                RPCOption::new(Duration::from_millis(2000)),
            )
            .await
            .expect_err("a 413 must be an error");

        match err {
            RPCError::PayloadTooLarge(too_large) => {
                assert_eq!(
                    too_large.entries_hint(),
                    4,
                    "eight entries must be halved to four so openraft retries smaller"
                );
                // `RPCTypes` is private in openraft 0.9, so the action is
                // asserted through the rendered form rather than the enum.
                assert!(
                    too_large.to_string().contains("AppendEntries"),
                    "the hint must be about the AppendEntries payload, got {too_large}"
                );
            }
            other => panic!("a follower 413 must map to PayloadTooLarge, got {other:?}"),
        }
    }

    // ---- #431: the per-peer liveness ticker ---------------------------------

    /// The ticker must be silent while openraft is heartbeating — otherwise it
    /// doubles every follower's inbound traffic for nothing.
    #[tokio::test]
    async fn the_liveness_ticker_is_silent_while_openraft_heartbeats() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        let (addr, _bodies, probes, _guard) =
            spawn_append_responder(Duration::from_millis(0), AppendReply::Success).await;
        let mut peer = peer_client_for(addr).await;

        // Ordinary heartbeats every 25 ms: entry-less, but with a prev_log_id,
        // so the responder counts them as neither body nor probe.
        for _ in 0..12 {
            let mut hb = append_req(1, Some(3), 3);
            hb.entries.clear();
            let _ = peer
                .append_entries(hb, RPCOption::new(Duration::from_millis(500)))
                .await;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            probes.load(Ordering::SeqCst),
            0,
            "openraft was heartbeating the whole time; the ticker must not have spoken"
        );
    }

    /// Once openraft goes quiet, the ticker speaks within a couple of ticks.
    ///
    /// Pins D-22: the silent window openraft opens is filled by a liveness
    /// probe, so a follower's election timer is refreshed while a transfer or
    /// snapshot is in flight.
    #[tokio::test]
    async fn the_liveness_ticker_speaks_when_openraft_goes_quiet() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        let (addr, _bodies, probes, _guard) =
            spawn_append_responder(Duration::from_millis(0), AppendReply::Success).await;
        let mut peer = peer_client_for(addr).await;

        let mut hb = append_req(1, Some(3), 3);
        hb.entries.clear();
        let _ = peer
            .append_entries(hb, RPCOption::new(Duration::from_millis(500)))
            .await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            probes.load(Ordering::SeqCst) >= 1,
            "250 ms of silence from openraft must produce at least one probe"
        );
    }

    /// The ticker must fall silent the moment this node stops leading: openraft
    /// does not drop an ex-leader's idle replication clients promptly, and its
    /// old vote is still the highest — so a probe from it reads as a live
    /// leader and keeps every follower's leader lease fresh, postponing the
    /// election a graceful leave depends on (the C5 rolling-restart outage).
    #[tokio::test]
    async fn the_liveness_ticker_falls_silent_when_this_node_stops_leading() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        let (addr, _bodies, probes, _guard) =
            spawn_append_responder(Duration::from_millis(0), AppendReply::Success).await;
        let leading = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut peer = peer_client_with_leading(addr, Arc::clone(&leading)).await;

        // Note a vote and let openraft go quiet: the armed ticker speaks.
        let mut hb = append_req(1, Some(3), 3);
        hb.entries.clear();
        let _ = peer
            .append_entries(hb, RPCOption::new(Duration::from_millis(500)))
            .await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            probes.load(Ordering::SeqCst) >= 1,
            "while leading, silence must produce probes"
        );

        // Step down. Give any in-flight tick a moment to drain, then require
        // silence: the vote is still noted and openraft is still quiet — only
        // the leadership flag has changed.
        leading.store(false, Ordering::Release);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let after_step_down = probes.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            probes.load(Ordering::SeqCst),
            after_step_down,
            "a node that stopped leading must not probe: its silence is what \
             lets the survivors' leases lapse and a successor win"
        );
    }

    /// The ticker must die with the client: openraft drops a `PeerClient` when it
    /// stops replicating to that peer, and a probe from a dead leader would be a
    /// stale-vote message the follower has to reject.
    #[tokio::test]
    async fn the_liveness_ticker_dies_with_the_client() {
        use openraft::network::{RPCOption, RaftNetwork};
        use std::sync::atomic::Ordering;

        let (addr, _bodies, probes, _guard) =
            spawn_append_responder(Duration::from_millis(0), AppendReply::Success).await;
        let mut peer = peer_client_for(addr).await;
        let mut hb = append_req(1, Some(3), 3);
        hb.entries.clear();
        let _ = peer
            .append_entries(hb, RPCOption::new(Duration::from_millis(500)))
            .await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(peer);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let after_drop = probes.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            probes.load(Ordering::SeqCst),
            after_drop,
            "no probe may be sent after the client is dropped"
        );
    }
}
