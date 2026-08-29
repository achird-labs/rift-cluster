//! Where a digest-only op's bytes come from when this node lacks them (#439; D-23, D-48).
//!
//! [`PeerBlobSource`] is the live [`BlobSource`] the state machine consults *before* it opens
//! its apply transaction (`RedbStateMachine::resolve_blobs`). It asks the write's origin
//! first, then every other joint voter — D-19's set, the one no single membership change can
//! empty — and it never gives up while the node is up: a blob no member can supply parks this
//! node's apply and is reported as *degraded* (D-48), because the alternative, failing the apply
//! on a timer, is fatal to the openraft state machine and would turn any partition longer than a
//! bound into an outage with no self-heal.
//!
//! The one thing that does end a parked fetch is **this node stopping** (D-56, #513). The park
//! occupies openraft's state-machine worker, which owns the storage handle, so a fetch that
//! outlived shutdown would keep the node's database locked and its data directory unopenable.
//! `RaftNode` signals it; the fetch returns [`BlobError::ShuttingDown`] and the entry re-applies
//! on restart, having written nothing.

use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use openraft::Raft;
use tokio::sync::OnceCell;

use super::network::{self, RaftSlot};
use super::{NodeId, TypeConfig};
use crate::blobs::{
    BLOB_FETCH_ESCALATE_AFTER, BlobDigest, BlobError, BlobSource, BlobStore, BlobTransfer,
};
use crate::metrics;
use crate::rpc::{PeerResolver, RpcClient, RpcError};

/// Pause between fetch rounds, doubling from the first to the cap. Capped low because a
/// round is cheap — one refused call per member — and the event being waited for, a holder
/// coming back, should not be noticed minutes late.
const FETCH_BACKOFF_MIN: Duration = Duration::from_millis(200);
const FETCH_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// One fetch that has gone unsatisfied past [`BLOB_FETCH_ESCALATE_AFTER`] — what
/// `/_cluster/health` reports as `blob_fetch_stall` (D-48).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobFetchStall {
    pub digest: String,
    /// When the fetch first went unsatisfied — not when it was escalated — so the reported
    /// duration includes the window it took to become a stall.
    pub since: Instant,
    pub origin: NodeId,
    /// Members asked in the most recent round, whether or not they answered.
    pub tried: Vec<NodeId>,
    /// Members whose build cannot serve blobs at all (`UnknownRoute` / `VersionSkew`): an
    /// upgrade in progress, not a partition, and named separately so the two are not confused.
    pub skewed: Vec<NodeId>,
    /// The last *refusal* a member answered with in the most recent round — a peer that
    /// answered but would not serve the blob (bad credential, malformed request, a mismatched
    /// digest). `None` when every member merely lacked the blob or did not answer, which is
    /// the partition shape. A refusal is what an operator can act on, so it must not be
    /// flattened into "no member holds the blob".
    pub last_error: Option<String>,
}

impl BlobFetchStall {
    #[must_use]
    pub fn stalled_for(&self) -> Duration {
        self.since.elapsed()
    }
}

/// What one round over the members produced.
enum Round {
    Fetched(Vec<u8>),
    Unavailable {
        tried: Vec<NodeId>,
        skewed: Vec<NodeId>,
        last_error: Option<String>,
    },
}

/// What one failed `get` from one address means for the rest of the round.
///
/// Pure, so the peer-error branches — the part of fetch-on-apply no solo-node test ever
/// reaches — are pinned by literal tests rather than by the fleet happening to produce them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchStep {
    /// This peer lacks the blob; the next member may not.
    NextPeer,
    /// This peer's build cannot answer the question at all — both are 404 on the wire and only
    /// the body says which (#437). Not a holder and not a partition: an upgrade in progress.
    Skewed,
    /// Unreachable at this address; the authority may resolve to more.
    NextAddress,
    /// The peer answered and refused — a credential or request-shape problem, or bytes that
    /// did not hash to the digest (`BlobTransfer::get` fails the whole fetch on that). The next
    /// member is asked, and the refusal is carried into the stall record rather than lost.
    Refused,
}

fn classify(error: &RpcError) -> FetchStep {
    match error {
        RpcError::NotFound { .. } => FetchStep::NextPeer,
        RpcError::UnknownRoute { .. } | RpcError::VersionSkew { .. } => FetchStep::Skewed,
        // Not a copy of the variant list: `is_liveness_failure` is its one definition
        // (D-61 §2, #521). Matched after the three arms above, which are D-48's own answers and
        // stay D-48's whatever that rule grows to include.
        e if e.is_liveness_failure() => FetchStep::NextAddress,
        _ => FetchStep::Refused,
    }
}

/// The live [`BlobSource`]: this node's store, its signed client, and the membership view it
/// reads through the same [`RaftSlot`] the control routes use. The slot rather than the
/// `Raft` is held because this is attached to the state machine *before* `Raft::new`
/// exists to be handed over (the `with_journal` contract); only a genuinely absent blob ever
/// waits for the slot to fill, since every restart-replay hits the local store first.
///
/// Held as a `Weak`, like the leading probe in `RaftNode::start_inner`: the `Raft` owns the
/// state machine, which owns this, so a strong slot here would close the cycle
/// Raft → state machine → source → slot → Raft and keep the storage alive after a drop
/// without shutdown (`drop_without_shutdown_eventually_releases_storage` pins that).
pub(crate) struct PeerBlobSource {
    id: NodeId,
    store: Arc<BlobStore>,
    // Shared once: the client is pooled internally, and a stalled fetch runs rounds for as
    // long as the stall lasts.
    client: Arc<RpcClient>,
    resolver: Arc<dyn PeerResolver>,
    raft: Weak<OnceCell<Raft<TypeConfig>>>,
    stall: Mutex<Option<BlobFetchStall>>,
    /// Set once, by [`super::node::RaftNode::shutdown`] (and by its `Drop`), to end a parked
    /// fetch (D-56, #513). Held as a receiver rather than a flag because the fetch must wake
    /// *inside* its backoff — up to `FETCH_BACKOFF_MAX` — not at the end of it: the whole point
    /// is that `shutdown`'s storage-release wait is shorter than that.
    shutdown: tokio::sync::watch::Receiver<bool>,
}

impl PeerBlobSource {
    pub(crate) fn new(
        id: NodeId,
        store: Arc<BlobStore>,
        client: RpcClient,
        resolver: Arc<dyn PeerResolver>,
        raft: &RaftSlot,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            id,
            store,
            client: Arc::new(client),
            resolver,
            raft: Arc::downgrade(raft),
            stall: Mutex::new(None),
            shutdown,
        }
    }

    /// The stall currently being reported, if any.
    pub(crate) fn stall(&self) -> Option<BlobFetchStall> {
        self.stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set_stall(&self, stall: Option<BlobFetchStall>) {
        *self.stall.lock().unwrap_or_else(PoisonError::into_inner) = stall;
    }

    async fn load_inner(&self, digest: &BlobDigest, origin: NodeId) -> Result<Vec<u8>, BlobError> {
        let started = Instant::now();
        let mut escalated = false;
        let mut backoff = FETCH_BACKOFF_MIN;
        // `Receiver::clone` inherits the source receiver's seen-version, and the stored receiver
        // is never advanced (only `borrow()`ed), so `changed()` on this clone fires immediately
        // when the signal has already been sent. The `borrow()` check below is therefore a fast
        // path, not a second guarantee: it skips a network round we would otherwise start and
        // abandon a moment later.
        let mut shutdown = self.shutdown.clone();
        loop {
            // Local first, every round — not only before the first: an operator who repairs a
            // stall by dropping the file into *this* node's store must be noticed, not only a
            // holder coming back on a peer.
            //
            // Ahead of the shutdown check on purpose: bytes this node already holds cost no
            // network and no waiting, so a node on its way down still applies the entry rather
            // than leaving it to replay.
            if let Some(bytes) = read_local(Arc::clone(&self.store), digest).await? {
                self.clear_stall(escalated, digest, started);
                return Ok(bytes);
            }
            if *shutdown.borrow() {
                return Err(self.abandon(digest, started));
            }
            // The round is raced, not just the backoff. One round walks every member and every
            // resolved address, and a single unreachable peer costs `replication_deadline` (a 4
            // MiB allowance on top of the 2 s request timeout, so ~6 s) — many times
            // `STORAGE_RELEASE_TIMEOUT`. Without this the signal would be observed only after
            // the round drained, and `shutdown` would still time out on exactly the partition
            // this exists to survive. Cancelling mid-round is safe: a round only *reads* from
            // peers, and the caller is what commits fetched bytes to the store.
            let round = tokio::select! {
                round = self.fetch_round(digest, origin) => round,
                _ = shutdown.changed() => return Err(self.abandon(digest, started)),
            };
            match round {
                Round::Fetched(bytes) => {
                    // Committed locally before it is handed back, so the next apply of an op
                    // naming this digest — and this node's own restart — hit the local path.
                    // Off the runtime worker: a multi-MiB write is the stall #444 closed.
                    let store = Arc::clone(&self.store);
                    let owned = digest.clone();
                    let stored = tokio::task::spawn_blocking(move || {
                        store.store_whole(&owned, &bytes).map(|()| bytes)
                    })
                    .await
                    .map_err(|e| {
                        BlobError::Io(std::io::Error::other(format!("blob store task: {e}")))
                    })
                    .and_then(|stored| stored);
                    // Whatever happened, the fetch is over: a stall must not stay latched on
                    // the health surface after the thing it described has ended.
                    self.clear_stall(escalated, digest, started);
                    return stored;
                }
                Round::Unavailable {
                    tried,
                    skewed,
                    last_error,
                } => {
                    let waited = started.elapsed();
                    if !escalated && waited >= BLOB_FETCH_ESCALATE_AFTER {
                        // Once, at the transition: from here on this is a fault an operator
                        // should be looking at, not a peer being briefly busy. The node stays
                        // up and keeps asking — see the module doc for why it never gives up.
                        escalated = true;
                        metrics::blob_fetch_stalled();
                        tracing::error!(
                            digest = digest.as_str(),
                            origin,
                            ?tried,
                            ?skewed,
                            last_error = last_error
                                .as_deref()
                                .unwrap_or("none: no member holds the blob"),
                            waited_secs = waited.as_secs(),
                            "blob fetch stalled: no member can supply this blob; apply is parked on it and resumes when a holder returns"
                        );
                    } else {
                        tracing::warn!(
                            digest = digest.as_str(),
                            origin,
                            ?tried,
                            ?skewed,
                            last_error = last_error
                                .as_deref()
                                .unwrap_or("none: no member holds the blob"),
                            waited_secs = waited.as_secs(),
                            "blob fetch found no holder; retrying"
                        );
                    }
                    if escalated {
                        self.set_stall(Some(BlobFetchStall {
                            digest: digest.as_str().to_owned(),
                            since: started,
                            origin,
                            tried,
                            skewed,
                            last_error,
                        }));
                    }
                    // Whichever comes first, for the same reason the round above is raced: a
                    // backoff grown to `FETCH_BACKOFF_MAX` is longer than the storage-release
                    // wait `shutdown` allows itself.
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        _ = shutdown.changed() => return Err(self.abandon(digest, started)),
                    }
                    backoff = (backoff * 2).min(FETCH_BACKOFF_MAX);
                }
            }
        }
    }

    /// End a parked fetch because the node is stopping (D-56, #513).
    ///
    /// Clears the stall on the way out: the health surface it feeds is going away with the node,
    /// and a latched stall would otherwise be the last thing a shutting-down node reported about
    /// itself. Logged at `info!` — this is an ordinary consequence of stopping a node that was
    /// parked, not a fault; the entry re-applies on restart.
    fn abandon(&self, digest: &BlobDigest, started: Instant) -> BlobError {
        self.set_stall(None);
        tracing::info!(
            digest = digest.as_str(),
            parked_secs = started.elapsed().as_secs(),
            "blob fetch abandoned: node is shutting down; the parked entry re-applies on restart"
        );
        BlobError::ShuttingDown
    }

    fn clear_stall(&self, escalated: bool, digest: &BlobDigest, started: Instant) {
        if escalated {
            metrics::blob_fetch_recovered();
            tracing::info!(
                digest = digest.as_str(),
                stalled_secs = started.elapsed().as_secs(),
                "blob fetch recovered; apply resumes"
            );
        }
        self.set_stall(None);
    }

    /// One pass over the members: the origin first, then every other joint voter.
    async fn fetch_round(&self, digest: &BlobDigest, origin: NodeId) -> Round {
        let Some(raft) = self.raft.upgrade().and_then(|slot| slot.get().cloned()) else {
            return Round::Unavailable {
                tried: Vec::new(),
                skewed: Vec::new(),
                last_error: Some("raft not yet started; membership unreadable".to_owned()),
            };
        };
        let raft = &raft;
        let members = match network::joint_voters(raft).await {
            Ok(targets) => targets.members(),
            Err(e) => {
                return Round::Unavailable {
                    tried: Vec::new(),
                    skewed: Vec::new(),
                    last_error: Some(format!("membership unreadable: {e}")),
                };
            }
        };

        // Origin first: it is the one member that stored the bytes before proposing (#438).
        // `0` is what a pre-#439 entry carries — it names no origin — and such an entry never
        // reaches here because it carries its bytes; skipped rather than dialled as node 0.
        let mut order: Vec<NodeId> = Vec::with_capacity(members.len());
        if origin != self.id && origin != 0 {
            order.push(origin);
        }
        order.extend(
            members
                .iter()
                .copied()
                .filter(|&id| id != self.id && id != origin),
        );

        let transfer = BlobTransfer::new(Arc::clone(&self.client));
        let mut tried = Vec::new();
        let mut skewed = Vec::new();
        let mut last_error = None;
        for id in order {
            // Not (or no longer) a member: nothing to dial, not a failure.
            let Some(authority) = authority_of(raft, id) else {
                continue;
            };
            tried.push(id);
            let addrs = match network::resolve_authority(&self.resolver, &authority).await {
                Ok(addrs) if !addrs.is_empty() => addrs,
                Ok(_) => {
                    tracing::warn!(node_id = id, %authority, "blob fetch: authority resolved to no address");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(node_id = id, %authority, error = %e, "blob fetch: could not resolve peer");
                    continue;
                }
            };
            for addr in addrs {
                let error = match transfer.get(addr, digest).await {
                    Ok(bytes) => return Round::Fetched(bytes),
                    Err(e) => e,
                };
                match classify(&error) {
                    FetchStep::NextPeer => break,
                    FetchStep::Skewed => {
                        tracing::warn!(node_id = id, error = %error, "blob fetch: peer cannot serve blobs");
                        skewed.push(id);
                        break;
                    }
                    FetchStep::NextAddress => {
                        tracing::debug!(node_id = id, %addr, error = %error, "blob fetch: address did not answer");
                    }
                    FetchStep::Refused => {
                        tracing::warn!(node_id = id, error = %error, "blob fetch: peer answered but refused to serve the blob");
                        last_error = Some(format!("node {id}: {error}"));
                        break;
                    }
                }
            }
        }
        Round::Unavailable {
            tried,
            skewed,
            last_error,
        }
    }
}

impl BlobSource for PeerBlobSource {
    fn load<'a>(
        &'a self,
        digest: &'a BlobDigest,
        origin: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, BlobError>> + Send + 'a>>
    {
        Box::pin(self.load_inner(digest, origin))
    }
}

/// The committed local copy, verified against its name. `Ok(None)` when there is none —
/// including when there *was* one that did not hash to its digest: a content-addressed file
/// whose contents do not match its name is disk corruption, and `write_chunk` treats an
/// existing committed file as final and never overwrites it, so it is removed here to let the
/// fetch replace it. Loudly, because this should not happen.
async fn read_local(
    store: Arc<BlobStore>,
    digest: &BlobDigest,
) -> Result<Option<Vec<u8>>, BlobError> {
    let path = store.path_of(digest);
    let expected = digest.clone();
    tokio::task::spawn_blocking(move || match std::fs::read(&path) {
        Ok(bytes) if crate::blobs::digest_of_bytes(&bytes) == expected => Ok(Some(bytes)),
        Ok(_) => {
            tracing::error!(
                path = %path.display(),
                "local blob does not hash to its digest; discarding it and refetching"
            );
            std::fs::remove_file(&path)?;
            Ok(None)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(BlobError::Io(e)),
    })
    .await
    .map_err(|e| BlobError::Io(std::io::Error::other(format!("blob read task: {e}"))))?
}

/// The advertise authority of member `id`, from the applied membership; `None` when the id
/// is not (or no longer) a member.
pub(crate) fn authority_of(raft: &Raft<TypeConfig>, id: NodeId) -> Option<String> {
    let receiver = raft.metrics();
    let metrics = receiver.borrow();
    metrics
        .membership_config
        .nodes()
        .find(|(node_id, _)| **node_id == id)
        .map(|(_, node)| node.addr.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blobs::digest_of_bytes;
    use crate::rpc::PROTO_VERSION;

    /// A source whose slot is empty, so every `fetch_round` reports `Unavailable` at once and
    /// `load` is the D-48 retry loop with nothing to find — the park, minus a cluster.
    fn parked_source(
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> (Arc<PeerBlobSource>, tempfile::TempDir, RaftSlot) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let store =
            Arc::new(crate::blobs::BlobStore::open(dir.path().to_path_buf()).expect("store"));
        let client = crate::rpc::RpcClient::new(
            None,
            Arc::new(crate::rpc::TrackedPeerHealth::new()),
            crate::rpc::RpcClientConfig::default(),
        );
        let slot: RaftSlot = Arc::new(OnceCell::new());
        let source = Arc::new(PeerBlobSource::new(
            1,
            store,
            client,
            Arc::new(crate::rpc::DnsResolver),
            &slot,
            shutdown,
        ));
        // The slot is returned so the caller holds it: the source keeps only a `Weak`.
        (source, dir, slot)
    }

    /// Pins D-56 (#513): a parked fetch ends when this node is shutting down, and it ends
    /// *inside* the wait rather than at the end of it. Without that the openraft state-machine
    /// worker stays in `apply`, never drops its `RedbStateMachine` clone, and
    /// `RaftNode::shutdown` times out with the redb file lock still held.
    ///
    /// **The timing is the assertion.** The signal is fired only once the backoff has grown past
    /// `STORAGE_RELEASE_TIMEOUT`: rounds at 0/200/600/1400/3000 ms sleep 200/400/800/1600/3200 ms,
    /// so at 3.5 s the loop is inside a 3.2 s sleep ending at ~6.2 s. A `sleep(backoff)` that is
    /// merely awaited — rather than raced against the signal — therefore returns ~2.7 s late and
    /// fails the bound below, which is what makes this test discriminate the `select!`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_parked_fetch_returns_shutting_down_the_moment_the_signal_fires() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let (source, _dir, _slot) = parked_source(rx);
        let digest = digest_of_bytes(b"nobody holds this");

        let fetching = {
            let source = Arc::clone(&source);
            let digest = digest.clone();
            tokio::spawn(async move { source.load(&digest, 1).await })
        };
        tokio::time::sleep(Duration::from_millis(3_500)).await;
        assert!(
            !fetching.is_finished(),
            "precondition: the fetch must still be parked, deep enough in the backoff that \
             waking at the end of it would be visibly late"
        );

        let fired = Instant::now();
        tx.send(true).expect("the fetch holds the receiver");
        let outcome = tokio::time::timeout(Duration::from_secs(10), fetching)
            .await
            .expect("the fetch must not outlive the signal")
            .expect("no panic");
        let woke_in = fired.elapsed();

        assert!(
            matches!(outcome, Err(BlobError::ShuttingDown)),
            "a shutdown ends the fetch with its own error, never a success or a NotFound: \
             {outcome:?}"
        );
        assert!(
            woke_in < Duration::from_millis(500),
            "must wake on the signal, not at the end of the 3.2 s backoff: took {woke_in:?}"
        );
    }

    /// Pins D-56 (#513): the local read comes *before* the shutdown check, so a node on its way
    /// down still applies an entry whose bytes it already holds. Costs no network and no waiting,
    /// and is one fewer entry to replay on restart.
    ///
    /// This is also what pins the ordering: move the shutdown check above the local read and this
    /// fetch returns `ShuttingDown` instead of the bytes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shutdown_still_serves_bytes_this_node_already_holds() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let (source, _dir, _slot) = parked_source(rx);
        let bytes = b"this node holds these".to_vec();
        let digest = digest_of_bytes(&bytes);
        source
            .store
            .store_whole(&digest, &bytes)
            .expect("seed the local store");
        tx.send(true).expect("receiver held by the source");

        let outcome = tokio::time::timeout(Duration::from_secs(2), source.load(&digest, 1))
            .await
            .expect("a local hit needs no round and must not wait");

        assert_eq!(
            outcome.expect("local bytes are served during shutdown"),
            bytes
        );
    }

    /// Pins D-56 (#513): abandoning clears the stall. A node that is stopping must not leave a
    /// degraded reading as the last thing it said about itself on a health surface that is about
    /// to close. The stall is seeded directly because reaching it honestly costs
    /// `BLOB_FETCH_ESCALATE_AFTER` (30 s) of real parking.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoning_a_fetch_clears_the_stall_it_was_reporting() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let (source, _dir, _slot) = parked_source(rx);
        let digest = digest_of_bytes(b"nobody holds this");
        source.set_stall(Some(BlobFetchStall {
            digest: digest.as_str().to_owned(),
            since: Instant::now(),
            origin: 1,
            tried: vec![1, 2, 3],
            skewed: Vec::new(),
            last_error: None,
        }));
        assert!(
            source.stall().is_some(),
            "precondition: a stall is reported"
        );

        tx.send(true).expect("receiver held by the source");
        let outcome = tokio::time::timeout(Duration::from_secs(2), source.load(&digest, 1))
            .await
            .expect("returns on the signal");

        assert!(
            matches!(outcome, Err(BlobError::ShuttingDown)),
            "{outcome:?}"
        );
        assert_eq!(
            source.stall(),
            None,
            "the stall must be cleared on the way out, not left latched"
        );
    }

    /// D-48 is otherwise unchanged: with no shutdown in sight the loop keeps asking, which is
    /// the property the park rests on. Pins that D-56 narrowed nothing else.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_parked_fetch_keeps_asking_while_the_node_is_up() {
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let (source, _dir, _slot) = parked_source(rx);

        let outcome = tokio::time::timeout(
            Duration::from_millis(750),
            source.load(&digest_of_bytes(b"nobody holds this"), 1),
        )
        .await;

        assert!(
            outcome.is_err(),
            "the fetch must still be retrying, not have returned {outcome:?}"
        );
    }

    // ---- the peer-error branches, pinned by literal values (D-48) ----

    /// Pins D-48: a peer that lacks the blob is not skew and not a refusal — ask the next member.
    #[test]
    fn a_peer_that_lacks_the_blob_sends_the_round_to_the_next_member() {
        assert_eq!(
            classify(&RpcError::NotFound {
                what: "blob".to_owned()
            }),
            FetchStep::NextPeer
        );
    }

    /// Pins D-48: a build without blob routes is an upgrade in progress, reported as skew,
    /// never mistaken for a member that merely lacks the blob (both are 404 on the wire).
    #[test]
    fn a_build_without_blob_routes_is_skewed_not_absent() {
        assert_eq!(
            classify(&RpcError::UnknownRoute {
                method: "GET".to_owned(),
                path: "/internal/v1/blob/x".to_owned()
            }),
            FetchStep::Skewed
        );
        assert_eq!(
            classify(&RpcError::VersionSkew {
                peer: None,
                ours: PROTO_VERSION
            }),
            FetchStep::Skewed
        );
    }

    #[test]
    fn an_unreachable_address_is_not_evidence_about_the_peer() {
        assert_eq!(classify(&RpcError::Timeout), FetchStep::NextAddress);
        assert_eq!(
            classify(&RpcError::Transport("refused".to_owned())),
            FetchStep::NextAddress
        );
        assert_eq!(classify(&RpcError::Shed), FetchStep::NextAddress);
    }

    /// A refusal is carried, not flattened into "no member holds the blob": it names what an
    /// operator can fix.
    #[test]
    fn a_refusal_is_a_refusal_whatever_its_class() {
        assert_eq!(
            classify(&RpcError::BadRequest("malformed".to_owned())),
            FetchStep::Refused
        );
        assert_eq!(
            classify(&RpcError::Handler(
                "blob abc fetched from 127.0.0.1:1 hashes to def".to_owned()
            )),
            FetchStep::Refused
        );
        assert_eq!(
            classify(&RpcError::Unavailable {
                detail: "no leader".to_owned(),
                op_id: None
            }),
            FetchStep::Refused
        );
    }

    /// One representative value of every [`RpcError`] variant.
    ///
    /// The never-called wildcard-free `match` below is the point, in the shape `metrics.rs`
    /// already uses for the reason buckets: a variant added to `RpcError` stops compiling
    /// *there* until it is listed, which puts whoever adds it in this file — the check this
    /// list feeds cannot quietly stop covering the enum. Keys come from `reason()` rather than
    /// a second hand-written label list, since a copied list is the very thing #521 is about.
    fn every_rpc_error_variant() -> Vec<RpcError> {
        let all = vec![
            RpcError::Unauthorized(crate::rpc::AuthError::BadMac),
            RpcError::VersionSkew {
                peer: None,
                ours: PROTO_VERSION,
            },
            RpcError::UnknownRoute {
                method: "GET".to_owned(),
                path: "/internal/v1/blob/x".to_owned(),
            },
            RpcError::BodyTooLarge { limit: 32 },
            RpcError::Timeout,
            RpcError::Transport("connection refused".to_owned()),
            RpcError::Shed,
            RpcError::BadRequest("malformed".to_owned()),
            RpcError::Unavailable {
                detail: "no leader".to_owned(),
                op_id: None,
            },
            RpcError::NotLeader { leader: None },
            RpcError::Handler("blob hashes to something else".to_owned()),
            RpcError::NotFound {
                what: "blob".to_owned(),
            },
        ];

        fn _every_variant_is_listed_above(e: &RpcError) {
            match e {
                RpcError::Unauthorized(_)
                | RpcError::VersionSkew { .. }
                | RpcError::UnknownRoute { .. }
                | RpcError::BodyTooLarge { .. }
                | RpcError::Timeout
                | RpcError::Transport(_)
                | RpcError::Shed
                | RpcError::BadRequest(_)
                | RpcError::Unavailable { .. }
                | RpcError::NotLeader { .. }
                | RpcError::Handler(_)
                | RpcError::NotFound { .. } => {}
            }
        }

        let covered: std::collections::BTreeSet<_> = all.iter().map(RpcError::reason).collect();
        assert_eq!(
            covered.len(),
            all.len(),
            "two values of the same variant: {covered:?}"
        );
        all
    }

    /// Pins D-61 §2 for the fetch classifier: `RpcError::is_liveness_failure` is the *only*
    /// definition of "the peer was not reached", and `classify` must not restate it.
    ///
    /// Outside the three variants D-48 answers for itself (`NotFound` absent, `UnknownRoute`
    /// and `VersionSkew` skewed), every variant routes to `NextAddress` exactly when
    /// `is_liveness_failure` says so.
    ///
    /// What this discriminates, stated precisely because it is narrow: it fails when a *copy*
    /// of the variant list is present here **and** the rule has moved on — which is the drift
    /// #521 is about, a variant added to `is_liveness_failure` that a copy keeps classifying as
    /// `Refused`, sending the round to the next *member* instead of the peer's next *address*.
    /// It cannot fail while `classify` delegates, because then both sides move together — that
    /// is the property the fix creates, not a weakness of the test. The forward guard is the
    /// compile break in [`every_rpc_error_variant`].
    #[test]
    fn classify_follows_is_liveness_failure_for_every_variant() {
        for e in every_rpc_error_variant() {
            if matches!(
                e,
                RpcError::NotFound { .. }
                    | RpcError::UnknownRoute { .. }
                    | RpcError::VersionSkew { .. }
            ) {
                continue;
            }
            let step = classify(&e);
            assert_eq!(
                step == FetchStep::NextAddress,
                e.is_liveness_failure(),
                "{e:?}: classify says {step:?}, is_liveness_failure() says {}",
                e.is_liveness_failure()
            );
        }
    }

    // ---- the local path ----

    fn temp_store() -> (tempfile::TempDir, Arc<BlobStore>) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let store = BlobStore::open(dir.path().join("blobs")).expect("open blob store");
        (dir, Arc::new(store))
    }

    #[tokio::test]
    async fn a_local_blob_that_hashes_to_its_name_is_served() {
        let (_dir, store) = temp_store();
        let bytes = b"id,name\n1,ada\n";
        let digest = digest_of_bytes(bytes);
        store.store_whole(&digest, bytes).expect("store");

        assert_eq!(
            read_local(Arc::clone(&store), &digest).await.expect("read"),
            Some(bytes.to_vec())
        );
    }

    #[tokio::test]
    async fn an_absent_local_blob_is_none() {
        let (_dir, store) = temp_store();
        let digest = digest_of_bytes(b"never stored");
        assert_eq!(read_local(store, &digest).await.expect("read"), None);
    }

    /// A committed file whose contents do not hash to its name is discarded so the fetch can
    /// commit the real bytes — `write_chunk` would otherwise treat the corrupt file as final.
    #[tokio::test]
    async fn a_corrupt_local_blob_is_discarded_so_the_fetch_can_replace_it() {
        let (_dir, store) = temp_store();
        let digest = digest_of_bytes(b"the real bytes");
        // Written behind the store's back: its own writers verify, so this is the only way a
        // file can end up under a name it does not hash to.
        let path = store.path_of(&digest);
        std::fs::create_dir_all(path.parent().expect("root")).expect("mkdir");
        std::fs::write(&path, b"not those bytes").expect("write corrupt file");

        assert_eq!(
            read_local(Arc::clone(&store), &digest).await.expect("read"),
            None,
            "corrupt bytes must never be served as the blob"
        );
        assert!(
            !path.exists(),
            "removed, so a fetch can commit the real bytes under this name"
        );
    }
}
