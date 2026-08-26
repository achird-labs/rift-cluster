//! Where a digest-only op's bytes come from when this node lacks them (#439; D-23, D-48).
//!
//! [`PeerBlobSource`] is the live [`BlobSource`] the state machine consults *before* it opens
//! its apply transaction (`RedbStateMachine::resolve_blobs`). It asks the write's origin
//! first, then every other joint voter — D-19's set, the one no single membership change can
//! empty — and it never gives up: a blob no member can supply parks this node's apply and is
//! reported as *degraded* (D-48), because the alternative, failing the apply, is fatal to the
//! openraft state machine and would turn any partition longer than a bound into an outage
//! with no self-heal.

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
        RpcError::Timeout | RpcError::Transport(_) | RpcError::Shed => FetchStep::NextAddress,
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
}

impl PeerBlobSource {
    pub(crate) fn new(
        id: NodeId,
        store: Arc<BlobStore>,
        client: RpcClient,
        resolver: Arc<dyn PeerResolver>,
        raft: &RaftSlot,
    ) -> Self {
        Self {
            id,
            store,
            client: Arc::new(client),
            resolver,
            raft: Arc::downgrade(raft),
            stall: Mutex::new(None),
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
        loop {
            // Local first, every round — not only before the first: an operator who repairs a
            // stall by dropping the file into *this* node's store must be noticed, not only a
            // holder coming back on a peer.
            if let Some(bytes) = read_local(Arc::clone(&self.store), digest).await? {
                self.clear_stall(escalated, digest, started);
                return Ok(bytes);
            }
            match self.fetch_round(digest, origin).await {
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
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(FETCH_BACKOFF_MAX);
                }
            }
        }
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
