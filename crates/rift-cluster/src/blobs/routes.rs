//! `PUT`/`GET /internal/v1/blob/{digest}` — the signed cluster-port transport
//! for [`super::BlobStore`] (#437).
//!
//! **No `HEAD`, deliberately**, though #437 asks for one as a have/size probe.
//! A `HEAD` response cannot carry a body, which costs it both halves of that
//! job: it cannot report a size, and its 404 cannot say whether the blob is
//! absent or the *route* is (the transport puts that label in the body, see
//! `rpc::server::error_response`). A caller reading a bodiless 404 as "absent"
//! would report a version-skewed peer as merely lacking every blob. `GET
//! ?stat=1` is one round trip, hits the same `stat`, and is unambiguous.
//!
//! Handlers only: this module knows nothing about files. Every store call is
//! synchronous, so each one is wrapped in `spawn_blocking` — hashing tens of
//! MiB on a runtime worker is the stall #444 is open against for snapshot
//! building, and doing it here would just add a second path with the same
//! defect.

use std::sync::Arc;

use serde::Serialize;

use super::{BLOB_CHUNK_MAX_BYTES, BLOB_PATH_PREFIX, BlobDigest, BlobError, BlobStore};
use crate::rpc::{HandlerFuture, Router, RpcError};

/// Applied state, asked for a blob's bytes when this node's transport store
/// does not have them (#486, D-51).
///
/// The state machine holds `sm_spec_blobs`/`sm_dataset_blobs` keyed by the same
/// digest the transport store files are named with, and drops a row in the very
/// transaction that drops its last reference — so what this can answer is
/// exactly the **referenced** set, which is exactly the set a snapshot manifest
/// names. It can therefore never serve a reaped or stale blob, and needs no pin.
///
/// Declared here, next to its one consumer, so `blobs` keeps knowing nothing
/// about `raft`; `raft::store::RedbStateMachine` implements it.
pub(crate) trait BlobFallback: Send + Sync + 'static {
    /// The bytes applied state holds under `digest`, or `None` if it holds none.
    ///
    /// Absence is a domain value, not an error — the same distinction
    /// [`super::BlobStat::have`] draws — so [`BlobError::NotFound`] is left to
    /// mean the route's own answer.
    ///
    /// # Errors
    ///
    /// The lookup itself failing, as a display string. Never conflated with
    /// `Ok(None)`: a read this node could not perform is not evidence that the
    /// blob is absent, and answering 404 to it would tell a fetching peer that
    /// nobody holds the blob.
    fn applied_blob(&self, digest: &BlobDigest) -> Result<Option<Vec<u8>>, String>;
}

/// Register the blob transfer routes onto `router`.
///
/// `fallback` backs the chunk-read half of `GET` only — see [`handle_get`] for
/// why `?stat` deliberately does not consult it.
#[must_use]
pub(crate) fn blob_routes(
    router: Router,
    store: Arc<BlobStore>,
    fallback: Arc<dyn BlobFallback>,
) -> Router {
    let put_store = Arc::clone(&store);
    let get_store = store;

    router
        .route_prefix(
            "PUT",
            BLOB_PATH_PREFIX,
            Arc::new(move |suffix: String, body: Vec<u8>| -> HandlerFuture {
                let store = Arc::clone(&put_store);
                Box::pin(async move { handle_put(store, suffix, body).await })
            }),
        )
        .route_prefix(
            "GET",
            BLOB_PATH_PREFIX,
            Arc::new(move |suffix: String, body: Vec<u8>| -> HandlerFuture {
                let store = Arc::clone(&get_store);
                let fallback = Arc::clone(&fallback);
                Box::pin(async move { handle_get(store, fallback, suffix, body).await })
            }),
        )
}

async fn handle_put(
    store: Arc<BlobStore>,
    suffix: String,
    body: Vec<u8>,
) -> Result<Vec<u8>, RpcError> {
    let (digest_str, query) = split_suffix(&suffix);
    let digest = BlobDigest::parse(digest_str).map_err(|e| map_blob_error(e, digest_str))?;
    let offset = required_u64(query, "offset")?;
    let total = required_u64(query, "total")?;

    let staged =
        tokio::task::spawn_blocking(move || store.write_chunk(&digest, offset, &body, total))
            .await
            .map_err(|e| RpcError::Handler(format!("blob write task: {e}")))?
            .map_err(|e| map_blob_error(e, digest_str))?;

    encode(&serde_json::json!({ "staged": staged }))
}

async fn handle_get(
    store: Arc<BlobStore>,
    fallback: Arc<dyn BlobFallback>,
    suffix: String,
    _body: Vec<u8>,
) -> Result<Vec<u8>, RpcError> {
    let (digest_str, query) = split_suffix(&suffix);
    let digest = BlobDigest::parse(digest_str).map_err(|e| map_blob_error(e, digest_str))?;

    // A peer asking is a peer asking, whichever branch below answers it (#480): `?stat` and a
    // chunk read both mean the same thing to GC — this digest is not just old, someone still
    // wants it. In-memory and cheap enough to run unconditionally rather than gating it behind
    // which branch is about to run.
    store.touch(&digest);

    if has_flag(query, "stat") {
        let stat = tokio::task::spawn_blocking(move || store.stat(&digest))
            .await
            .map_err(|e| RpcError::Handler(format!("blob stat task: {e}")))?
            .map_err(|e| map_blob_error(e, digest_str))?;
        return encode(&stat);
    }

    let offset = required_u64(query, "offset")?;
    let len = required_u64(query, "len")?;
    let fallback_digest = digest.clone();
    let read = tokio::task::spawn_blocking(move || store.read_chunk(&digest, offset, len))
        .await
        .map_err(|e| RpcError::Handler(format!("blob read task: {e}")))?;
    match read {
        Ok(bytes) => Ok(bytes),
        // Only absence falls through to applied state. Any other `BlobError` is
        // a fault of this node's own store — a disk that a second source would
        // hide rather than fix, and that the operator has to be able to see.
        Err(BlobError::NotFound) => {
            // Debug, not warn: on a healthy node this is the rare pre-fan-out case, but a store
            // that is *chronically* missing blobs it should hold would otherwise be papered over
            // silently and indefinitely by state-machine reads.
            tracing::debug!(
                digest = digest_str,
                "blob served from applied state; transport store missed"
            );
            applied_chunk(&fallback, fallback_digest, offset, len, digest_str).await
        }
        Err(e) => Err(map_blob_error(e, digest_str)),
    }
}

/// Serve `digest`'s chunk from applied state — the transport store's miss is
/// not the fleet's (#486, D-51).
///
/// Shapes its answer exactly as [`BlobStore::read_chunk`] shapes its own, cap
/// included, because [`super::client::BlobTransfer::get`] cannot tell the two
/// sources apart and must not have to: it reads chunks until one comes back
/// empty, then hashes the whole assembly against the digest it asked for.
///
/// Unlike `read_chunk`'s `seek` + `read_exact`, this reloads and copies the
/// **whole** blob per call — redb has no partial read into a value — so one
/// fallback transfer costs `ceil(size / BLOB_CHUNK_MAX_BYTES) + 1` full reads.
/// Accepted rather than optimised: this is the last-resort path, not the
/// steady-state one, and the sizes are bounded (a spec is capped at 4 MiB, a
/// dataset at the tenant's `max_dataset_bytes`), so it is a handful of reads
/// against a fetch that is already doing that many round trips. Making the
/// seam offset-aware would buy little and cost `slice_chunk`'s one-to-one
/// correspondence with `read_chunk`, which is what makes the two agree.
async fn applied_chunk(
    fallback: &Arc<dyn BlobFallback>,
    digest: BlobDigest,
    offset: u64,
    len: u64,
    digest_str: &str,
) -> Result<Vec<u8>, RpcError> {
    let fallback = Arc::clone(fallback);
    let held = tokio::task::spawn_blocking(move || fallback.applied_blob(&digest))
        .await
        .map_err(|e| RpcError::Handler(format!("blob fallback task: {e}")))?
        // A lookup that failed is not a blob that is absent. Mapping this to
        // `NotFound` would tell the fetching peer to cross this member off
        // (`FetchStep::NextPeer`) over a transient read, and would lose the
        // reason; as a 500 the sweep records it in the stall and moves on.
        .map_err(|e| {
            // The fetching peer records this on its stall, but the fault is *here*; without this
            // the node that actually failed the read leaves no local trace of it.
            tracing::error!(digest = digest_str, error = %e, "blob fallback lookup failed");
            RpcError::Handler(format!("blob fallback for {digest_str}: {e}"))
        })?;
    match held {
        Some(bytes) => Ok(slice_chunk(&bytes, offset, len)),
        None => Err(map_blob_error(BlobError::NotFound, digest_str)),
    }
}

/// [`BlobStore::read_chunk`]'s slicing rule over bytes already in memory: at
/// most `len`, never past the end, never more than [`BLOB_CHUNK_MAX_BYTES`].
///
/// An offset at or past the end is an empty chunk, not an error — that empty
/// answer is what ends `BlobTransfer::get`'s read loop.
fn slice_chunk(bytes: &[u8], offset: u64, len: u64) -> Vec<u8> {
    let total = bytes.len() as u64;
    if offset >= total {
        return Vec::new();
    }
    let want = len.min(total - offset).min(BLOB_CHUNK_MAX_BYTES as u64) as usize;
    let start = offset as usize;
    bytes[start..start + want].to_vec()
}

/// Split a `PrefixHandler` suffix into its digest and query components. The
/// suffix carries the query verbatim (see [`crate::rpc::routes::PrefixHandler`]'s
/// contract), so this — not the router — owns splitting it.
fn split_suffix(suffix: &str) -> (&str, &str) {
    suffix.split_once('?').unwrap_or((suffix, ""))
}

fn query_pairs(query: &str) -> impl Iterator<Item = (&str, &str)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| pair.split_once('='))
}

fn has_flag(query: &str, name: &str) -> bool {
    query_pairs(query).any(|(k, _)| k == name)
}

fn required_u64(query: &str, name: &str) -> Result<u64, RpcError> {
    let raw = query_pairs(query)
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
        .ok_or_else(|| RpcError::BadRequest(format!("missing query parameter {name:?}")))?;
    raw.parse::<u64>()
        .map_err(|_| RpcError::BadRequest(format!("{name} must be a u64, got {raw:?}")))
}

/// Map a [`BlobError`] onto the transport's typed error, with `digest` (the
/// caller's raw, possibly-invalid string) as the [`RpcError::NotFound::what`]
/// so a 404 names what was actually asked for.
fn map_blob_error(err: BlobError, digest: &str) -> RpcError {
    match err {
        BlobError::MalformedDigest => RpcError::BadRequest(err.to_string()),
        BlobError::NotFound => RpcError::NotFound {
            what: digest.to_owned(),
        },
        BlobError::ChunkTooLarge { .. }
        | BlobError::OffsetGap { .. }
        | BlobError::DigestMismatch => RpcError::BadRequest(err.to_string()),
        BlobError::Io(e) => RpcError::Handler(e.to_string()),
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, RpcError> {
    serde_json::to_vec(value).map_err(|e| RpcError::Handler(format!("encode: {e}")))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::blobs::{BLOB_CHUNK_MAX_BYTES, BlobStat};

    /// sha256 of `b"hello"` — the bytes every fallback test below serves.
    const DIGEST: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    /// sha256 of 5 MiB of `b'A'` — one byte over the chunk cap.
    const FIVE_MIB_A_DIGEST: &str =
        "dbbe5517996826bd5861ac22b745d21d11219055d89243ca1aea0ad31f552b12";

    /// A [`BlobFallback`] that answers one canned result, so a route test can
    /// state what applied state holds without standing a state machine up.
    struct StubFallback(Result<Option<Vec<u8>>, String>);

    impl BlobFallback for StubFallback {
        fn applied_blob(&self, _digest: &BlobDigest) -> Result<Option<Vec<u8>>, String> {
            self.0.clone()
        }
    }

    fn holding(bytes: &[u8]) -> Arc<dyn BlobFallback> {
        Arc::new(StubFallback(Ok(Some(bytes.to_vec()))))
    }

    fn holding_nothing() -> Arc<dyn BlobFallback> {
        Arc::new(StubFallback(Ok(None)))
    }

    /// An *empty* transport store: every fallback test needs the store to miss,
    /// which is the only condition under which applied state is consulted.
    fn empty_store() -> (Arc<BlobStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = BlobStore::open(dir.path().join("blobs")).expect("open blob store");
        (Arc::new(store), dir)
    }

    async fn get_chunk(
        fallback: Arc<dyn BlobFallback>,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, RpcError> {
        let (store, _dir) = empty_store();
        handle_get(
            store,
            fallback,
            format!("{DIGEST}?offset={offset}&len={len}"),
            Vec::new(),
        )
        .await
    }

    /// #486 / D-51. The transport store misses, applied state holds the blob, so
    /// the chunk is served from applied state. This is the whole fix: the fetch
    /// sweep (`PeerBlobSource::fetch_round`) calls `BlobTransfer::get`, which is
    /// nothing but this chunk loop.
    #[tokio::test]
    async fn a_chunk_read_falls_back_to_applied_state_when_the_store_misses() {
        assert_eq!(
            get_chunk(holding(b"hello"), 0, BLOB_CHUNK_MAX_BYTES as u64)
                .await
                .expect("served from applied state"),
            b"hello".to_vec()
        );
        // Mid-blob, so the answer is a real slice and not "the whole thing
        // whatever was asked for".
        assert_eq!(
            get_chunk(holding(b"hello"), 2, 2)
                .await
                .expect("served from applied state"),
            b"ll".to_vec()
        );
    }

    /// The fallback obeys the same cap `BlobStore::read_chunk` does, so a peer
    /// cannot make this node allocate an unbounded response by asking for one.
    #[tokio::test]
    async fn a_fallback_chunk_read_respects_the_chunk_cap() {
        let five_mib = vec![b'A'; 5 * 1024 * 1024];
        let (store, _dir) = empty_store();
        let chunk = handle_get(
            store,
            holding(&five_mib),
            format!("{FIVE_MIB_A_DIGEST}?offset=0&len={}", five_mib.len()),
            Vec::new(),
        )
        .await
        .expect("served from applied state");
        assert_eq!(chunk.len(), BLOB_CHUNK_MAX_BYTES);
        assert!(chunk.iter().all(|b| *b == b'A'));
    }

    /// Load-bearing: `BlobTransfer::get` loops until a chunk comes back
    /// **empty**, so a read at or past the end must be an empty `Ok`, not a 404.
    /// A 404 here would fail every fallback fetch on its last round trip.
    #[tokio::test]
    async fn a_fallback_chunk_read_past_the_end_is_empty_so_the_fetch_loop_ends() {
        assert_eq!(
            get_chunk(holding(b"hello"), 5, 64)
                .await
                .expect("empty tail"),
            Vec::<u8>::new()
        );
        assert_eq!(
            get_chunk(holding(b"hello"), 99, 64)
                .await
                .expect("empty past the end"),
            Vec::<u8>::new()
        );
        // A zero-length blob is a real blob: its very first chunk is the empty
        // one that ends the loop.
        assert_eq!(
            get_chunk(holding(b""), 0, 64).await.expect("empty blob"),
            Vec::<u8>::new()
        );
    }

    /// Absence is still absence. The fetch sweep reads a 404 as "ask the next
    /// member" (`FetchStep::NextPeer`), which is only correct while a 404 keeps
    /// meaning the node genuinely does not hold the blob.
    #[tokio::test]
    async fn a_blob_neither_the_store_nor_applied_state_holds_is_still_not_found() {
        let err = get_chunk(holding_nothing(), 0, 64)
            .await
            .expect_err("nobody holds it");
        assert!(matches!(&err, RpcError::NotFound { what } if what == DIGEST));
        assert_eq!(err.status(), 404);
    }

    /// A lookup this node could not perform is not evidence the blob is absent.
    /// Reporting it as 404 would make the fetching peer cross this member off
    /// its list on a transient redb error; 500 keeps it a refusal the sweep
    /// records in the stall (`FetchStep::Refused`) instead of losing.
    #[tokio::test]
    async fn a_fallback_lookup_failure_is_a_server_error_not_an_absence() {
        let fallback: Arc<dyn BlobFallback> =
            Arc::new(StubFallback(Err("state machine read failed".to_owned())));
        let err = get_chunk(fallback, 0, 64).await.expect_err("lookup failed");
        assert!(
            !matches!(err, RpcError::NotFound { .. }),
            "a failed lookup must never be reported as absence, got {err:?}"
        );
        assert_eq!(err.status(), 500);
    }

    /// The asymmetry is deliberate (#486). `BlobTransfer::put` skips sending
    /// bytes to any peer whose `?stat` says `have`, and that peer's ack counts
    /// toward the fan-out quorum `fan_out_then_submit` strips on (D-19/D-49).
    /// If `?stat` answered from applied state, a member could ack a fan-out
    /// without ever receiving the bytes into its transport store — resting
    /// D-18's quorum durability on a redb row a later delete can drop. `?stat`
    /// therefore stays a pure transport-store probe; the fallback serves reads.
    #[tokio::test]
    async fn a_stat_probe_does_not_consult_applied_state() {
        let (store, _dir) = empty_store();
        let body = handle_get(
            store,
            holding(b"hello"),
            format!("{DIGEST}?stat=1"),
            Vec::new(),
        )
        .await
        .expect("stat answers");
        let stat: BlobStat = serde_json::from_slice(&body).expect("decode stat");
        assert_eq!(
            stat,
            BlobStat {
                have: false,
                size: 0,
                staged: 0
            }
        );
    }

    /// A transport store that already holds `bytes` under `DIGEST` — `empty_store` deliberately
    /// misses, which is fine for the fallback tests above but useless here: rule B (D-52) only
    /// matters for a blob GC would otherwise reap, and an empty store has nothing to protect.
    fn store_holding(bytes: &[u8]) -> (Arc<BlobStore>, tempfile::TempDir) {
        let (store, dir) = empty_store();
        let digest = BlobDigest::parse(DIGEST).expect("digest");
        store
            .write_chunk(&digest, 0, bytes, bytes.len() as u64)
            .expect("commit blob");
        (store, dir)
    }

    /// Mirrors `blobs::mod`'s own `backdate`: age a committed blob's mtime so the plain grace rule
    /// cannot be what keeps it, isolating whatever else (here, `handle_get`'s `store.touch`) is
    /// under test.
    fn backdate(store: &BlobStore, digest: &BlobDigest, by_secs: u64) {
        let path = store.path_of(digest);
        let when = SystemTime::now() - Duration::from_secs(by_secs);
        let file = std::fs::File::options()
            .write(true)
            .open(&path)
            .expect("open the committed blob");
        file.set_times(std::fs::FileTimes::new().set_modified(when))
            .expect("backdate the blob's mtime");
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs()
    }

    /// #480 rule B's wiring point: `handle_get` calls `store.touch` before either branch runs, and
    /// nothing before this test exercised that the route actually reaches it — every fallback test
    /// above asks an *empty* store, so it never even sees a digest worth touching. Backdating the
    /// blob is required, or the still-fresh mtime grace alone would keep it and this test would
    /// pass with the `touch` call deleted.
    #[tokio::test]
    async fn a_stat_probe_marks_the_digest_as_wanted_so_gc_can_see_it() {
        let (store, _dir) = store_holding(b"hello");
        let digest = BlobDigest::parse(DIGEST).expect("digest");
        backdate(&store, &digest, 10_000);

        handle_get(
            Arc::clone(&store),
            holding_nothing(),
            format!("{DIGEST}?stat=1"),
            Vec::new(),
        )
        .await
        .expect("stat answers");

        let outcome = store
            .gc(&HashSet::new(), &HashMap::new(), 0, now_secs(), 3600)
            .expect("gc");
        assert_eq!(
            outcome.removed, 0,
            "the stat probe must have touched the digest"
        );
        assert!(store.stat(&digest).expect("stat").have);
    }

    /// The other branch of the same wiring point: a chunk read must touch too, not just `?stat`.
    /// See the sibling stat-probe test above for why the blob must be backdated.
    #[tokio::test]
    async fn a_chunk_read_marks_the_digest_as_wanted_so_gc_can_see_it() {
        let (store, _dir) = store_holding(b"hello");
        let digest = BlobDigest::parse(DIGEST).expect("digest");
        backdate(&store, &digest, 10_000);

        let bytes = handle_get(
            Arc::clone(&store),
            holding_nothing(),
            format!("{DIGEST}?offset=0&len=64"),
            Vec::new(),
        )
        .await
        .expect("chunk read");
        assert_eq!(
            bytes,
            b"hello".to_vec(),
            "sanity: served from the transport store, not the fallback"
        );

        let outcome = store
            .gc(&HashSet::new(), &HashMap::new(), 0, now_secs(), 3600)
            .expect("gc");
        assert_eq!(
            outcome.removed, 0,
            "the chunk read must have touched the digest"
        );
        assert!(store.stat(&digest).expect("stat").have);
    }

    #[test]
    fn a_suffix_splits_into_its_digest_and_query() {
        assert_eq!(split_suffix(DIGEST), (DIGEST, ""));
        assert_eq!(
            split_suffix(&format!("{DIGEST}?stat=1")),
            (DIGEST, "stat=1")
        );
        assert_eq!(
            split_suffix(&format!("{DIGEST}?offset=0&len=64")),
            (DIGEST, "offset=0&len=64")
        );
    }

    #[test]
    fn a_missing_or_unparseable_query_parameter_is_a_bad_request() {
        // 400, not 500: these name something the caller got wrong and fail
        // identically on every retry.
        assert!(matches!(
            required_u64("len=64", "offset"),
            Err(RpcError::BadRequest(_))
        ));
        assert!(matches!(
            required_u64("offset=nine", "offset"),
            Err(RpcError::BadRequest(_))
        ));
        assert!(matches!(
            required_u64("offset=-1", "offset"),
            Err(RpcError::BadRequest(_))
        ));
        assert_eq!(
            required_u64("offset=0&len=64", "offset").expect("offset"),
            0
        );
        assert_eq!(required_u64("offset=0&len=64", "len").expect("len"), 64);
    }

    #[test]
    fn a_flag_is_read_only_when_it_is_actually_present() {
        assert!(has_flag("stat=1", "stat"));
        assert!(has_flag("offset=0&stat=1", "stat"));
        assert!(!has_flag("offset=0&len=64", "stat"));
        assert!(!has_flag("", "stat"));
    }

    #[test]
    fn every_blob_error_maps_to_its_own_transport_class() {
        // The mapping is what acceptance criterion 3 rests on: a caller must be
        // able to tell "you sent something wrong" (400) from "this node
        // failed" (500) from "I do not have it" (404). An implementation that
        // collapsed these into `Handler` would answer 500 for all of them and
        // still pass an integration test that only asserts `is_err()`.
        assert!(matches!(
            map_blob_error(BlobError::MalformedDigest, "nope"),
            RpcError::BadRequest(_)
        ));
        assert!(matches!(
            map_blob_error(BlobError::ChunkTooLarge { limit: 4 }, DIGEST),
            RpcError::BadRequest(_)
        ));
        assert!(matches!(
            map_blob_error(
                BlobError::OffsetGap {
                    expected: 0,
                    got: 9
                },
                DIGEST
            ),
            RpcError::BadRequest(_)
        ));
        assert!(matches!(
            map_blob_error(BlobError::DigestMismatch, DIGEST),
            RpcError::BadRequest(_)
        ));
        assert!(matches!(
            map_blob_error(BlobError::NotFound, DIGEST),
            RpcError::NotFound { .. }
        ));
        assert!(matches!(
            map_blob_error(BlobError::Io(std::io::Error::other("disk")), DIGEST),
            RpcError::Handler(_)
        ));

        // And the statuses those classes actually answer with.
        assert_eq!(map_blob_error(BlobError::NotFound, DIGEST).status(), 404);
        assert_eq!(
            map_blob_error(BlobError::DigestMismatch, DIGEST).status(),
            400
        );
        assert_eq!(
            map_blob_error(BlobError::Io(std::io::Error::other("disk")), DIGEST).status(),
            500
        );
    }

    #[test]
    fn a_not_found_names_the_digest_that_was_asked_for() {
        let err = map_blob_error(BlobError::NotFound, DIGEST);
        match err {
            RpcError::NotFound { what } => assert_eq!(what, DIGEST),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }
}
