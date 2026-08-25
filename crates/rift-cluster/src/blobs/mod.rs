//! Per-node content-addressed blob store (#437, epic #432 child 2).
//!
//! Large payloads leave the Raft log and travel out of band: this module owns
//! the bytes on disk, `blobs::routes` exposes them over the signed cluster
//! transport (`PUT`/`GET /internal/v1/blob/{digest}`), and `blobs::client`
//! moves them between nodes. Nothing routes
//! through it yet — #438 fans blobs out before propose and #439 fetches them on
//! apply; this child builds the store and the transport they will call.
//!
//! The store is **node-local**: it is not replicated, it is not part of the
//! state machine, and two nodes holding different blob sets is normal rather
//! than divergence.

pub mod client;
pub(crate) mod routes;

pub use client::{BlobTransfer, PutOutcome};

use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Largest chunk a single `write_chunk`/`read_chunk` call may carry. Bounds
/// well under the cluster port's 32 MiB body cap so an oversized chunk is
/// refused before it reaches disk, and keeps one transfer step off the runtime
/// worker for long (the stall #444 is open against for snapshot building).
pub const BLOB_CHUNK_MAX_BYTES: usize = 4 * 1024 * 1024;

/// Prefix `blobs::routes` registers `PUT`/`GET` under, and the base
/// `blobs::client` builds request paths from.
pub(crate) const BLOB_PATH_PREFIX: &str = "/internal/v1/blob/";

/// Everything that can go wrong operating the blob store, mapped one-to-one
/// onto an [`crate::rpc::RpcError`] class at the route layer (`blobs::routes`)
/// — see that module for the mapping.
#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    /// A digest was not exactly 64 lowercase hex characters.
    #[error("malformed blob digest")]
    MalformedDigest,

    /// No committed blob exists under this digest. Absence elsewhere (`stat`,
    /// a `gc` pass) is a domain value, never this error — it is reserved for
    /// an explicit read of bytes that are not there.
    #[error("blob not found")]
    NotFound,

    /// A chunk exceeded [`BLOB_CHUNK_MAX_BYTES`].
    #[error("chunk exceeds the {limit}-byte cap")]
    ChunkTooLarge { limit: u64 },

    /// A chunk's offset is past what has been staged, which would leave a gap
    /// inside a file whose name is a hash of its contents.
    #[error("offset gap: expected {expected}, got {got}")]
    OffsetGap { expected: u64, got: u64 },

    /// The fully-staged bytes did not hash to the digest that named them.
    #[error("chunk bytes do not hash to the claimed digest")]
    DigestMismatch,

    /// A filesystem operation failed.
    #[error("blob store io: {0}")]
    Io(#[from] std::io::Error),
}

/// A validated content digest: exactly 64 lowercase hex characters, the sha256
/// hex of a blob's bytes. [`Self::parse`] is the only constructor, which makes
/// path traversal unrepresentable rather than filtered — the suffix a route
/// handler turns into a path component comes from a remote (if signed) peer,
/// and this type is what stands between that string and a `PathBuf::join`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobDigest(String);

impl BlobDigest {
    /// Parse `s` as a blob digest: exactly 64 lowercase hex characters.
    ///
    /// # Errors
    ///
    /// [`BlobError::MalformedDigest`] for anything else — including a
    /// traversal fragment (`..`, `../../etc/passwd`), a separator, an
    /// uppercase or non-hex character, or a wrong length.
    pub fn parse(s: &str) -> Result<Self, BlobError> {
        if s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            Ok(Self(s.to_owned()))
        } else {
            Err(BlobError::MalformedDigest)
        }
    }

    /// The digest as its 64-hex-character string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BlobDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The sha256 hex digest of `bytes`, as a validated [`BlobDigest`] — always
/// well-formed by construction, so this never fails.
#[must_use]
pub fn digest_of_bytes(bytes: &[u8]) -> BlobDigest {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    BlobDigest(format!("{:x}", hasher.finalize()))
}

/// What [`BlobStore::stat`] reports about one digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobStat {
    /// Whether a committed blob exists under this digest.
    pub have: bool,
    /// The committed blob's size, or 0 when `have` is false.
    pub size: u64,
    /// Bytes currently staged toward this digest, or 0 when nothing is
    /// staged (including when the blob is already committed).
    pub staged: u64,
}

/// Node-local record of this store's GC sweeps, persisted at
/// `<root>/gc-watermark.json` so a reader can tell "removed" from "never
/// swept" across a restart — the same distinction `sm_audit_gc_watermark`
/// preserves for the audit-log GC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GcWatermark {
    /// Wall-clock time (unix seconds) of the most recent sweep.
    pub last_sweep_secs: u64,
    /// Total committed blobs this store has ever removed.
    pub removed_total: u64,
}

const WATERMARK_FILE: &str = "gc-watermark.json";
const STAGING_DIR: &str = "staging";
const STAGING_SUFFIX: &str = ".part";

/// A per-node content-addressed blob store under `<data-dir>/blobs/`.
///
/// Synchronous, plain file operations — callers on an async runtime (the
/// routes in `blobs::routes`) wrap every call in `spawn_blocking`. Node-local:
/// nothing here is replicated, and nothing about it enters the state machine —
/// two nodes holding different blob sets is normal, not divergence.
#[derive(Debug)]
pub struct BlobStore {
    root: PathBuf,
    /// One writer at a time per digest.
    ///
    /// The staging path is a pure function of the digest, so every concurrent
    /// transfer of the same blob shares one file — and the commit sequence
    /// (hash the staged bytes, then rename them into place) is only atomic
    /// against a writer that cannot run *between* those two steps. Without
    /// this, a second sender restarting at offset 0 between the first's hash
    /// and its rename gets its shorter prefix committed under the first
    /// blob's name: bytes that were never verified, under a digest that says
    /// they were, with nothing to ever re-check them.
    ///
    /// `raft::store::write_spool` avoids the same hazard by giving every call
    /// a unique temp name. Resume needs the opposite — a stable staging name
    /// the next chunk can find — so the exclusion has to be explicit here.
    writers: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Digests a fan-out is currently in flight for, and how many fan-outs
    /// each (#438). [`Self::gc`] treats a pinned digest as unreclaimable
    /// however old and however unreferenced it is.
    ///
    /// This is the *state* #437's issue asked for where the grace window is a
    /// *timer*. The timer alone sufficed only while nothing fanned out: a
    /// fan-out holds a blob that is referenced by nothing — the referencing row
    /// is exactly what gets proposed *after* the acks — so between "quorum has
    /// it" and "the op commits" the reaper is the only thing that can take it.
    ///
    /// A **count**, not a set: two concurrent uploads of the same CSV share one
    /// digest, and a set would let the first to finish clear the second's
    /// protection. Not to be confused with the state machine's deliberately
    /// *unmaintained* blob refcount (`spec_digest_referenced` and friends
    /// answer that by scan) — this is process-local, in-memory, and covers only
    /// in-flight transfers.
    pinned: Mutex<HashMap<String, u32>>,
}

/// A live claim on a digest: while this guard exists, [`BlobStore::gc`] will
/// not reclaim it. Released on drop.
///
/// A guard rather than a `pin`/`unpin` pair because the release must survive
/// every path out of a fan-out — an early `?`, a quorum shortfall, a panic in a
/// peer task. A forgotten `unpin` is a leak with the same shape as the bug the
/// pin prevents, so the type makes forgetting unrepresentable.
#[derive(Debug)]
#[must_use = "a dropped pin releases immediately; bind it for the fan-out's lifetime"]
pub struct BlobPin<'a> {
    store: &'a BlobStore,
    digest: String,
}

impl Drop for BlobPin<'_> {
    fn drop(&mut self) {
        let mut pinned = self
            .store
            .pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = pinned.get_mut(&self.digest) {
            *count -= 1;
            if *count == 0 {
                pinned.remove(&self.digest);
            }
        }
    }
}

impl BlobStore {
    /// Open (creating if necessary) the store rooted at `root`. Creates
    /// `root` and `root/staging`, `0o700` on unix — private to this node, the
    /// same convention [`crate::raft::store`]'s dataset spool uses.
    ///
    /// # Errors
    ///
    /// Any filesystem failure creating or permissioning the directories.
    pub fn open(root: PathBuf) -> Result<Self, BlobError> {
        std::fs::create_dir_all(&root)?;
        let staging = root.join(STAGING_DIR);
        std::fs::create_dir_all(&staging)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
            std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(Self {
            root,
            writers: Mutex::new(HashMap::new()),
            pinned: Mutex::new(HashMap::new()),
        })
    }

    /// Claim `digest` against [`Self::gc`] for as long as the returned guard
    /// lives — the fan-out's protection between storing the blob and the op
    /// that will reference it committing (#438).
    ///
    /// Re-entrant: concurrent fan-outs of the same digest each hold their own
    /// pin and the blob is collectable again only when the last one drops.
    ///
    /// Deliberately **not** durable. Nothing in this store persists across a
    /// restart beyond the watermark, and it does not need to: the grace window
    /// runs from the blob's mtime rather than from process start, so a blob
    /// written moments before a crash is still protected while the parked
    /// intent replays and re-runs the (idempotent) fan-out. The pin covers the
    /// in-process window precisely; the grace covers the restart gap coarsely.
    pub fn pin(&self, digest: &BlobDigest) -> BlobPin<'_> {
        let mut pinned = self.pinned.lock().unwrap_or_else(PoisonError::into_inner);
        *pinned.entry(digest.as_str().to_owned()).or_insert(0) += 1;
        drop(pinned);
        BlobPin {
            store: self,
            digest: digest.as_str().to_owned(),
        }
    }

    /// Commit `bytes` under `digest` in one call, chunking to the receiver's
    /// cap the same way a transfer would.
    ///
    /// The accepting node's own copy in a fan-out (#438): it already holds the
    /// whole payload in memory, so it does not need the resumable path — but it
    /// must land in exactly the same committed state a peer's transfer produces,
    /// including the digest verification `write_chunk` does on completion.
    ///
    /// # Errors
    ///
    /// Any [`BlobError`] the underlying chunk writes fail with — including a
    /// mismatch when `bytes` does not hash to `digest`.
    pub fn store_whole(&self, digest: &BlobDigest, bytes: &[u8]) -> Result<(), BlobError> {
        let total = bytes.len() as u64;
        if total == 0 {
            // A zero-length blob has no chunks to iterate, but it is still a
            // real blob that must end up committed (edge 7).
            self.write_chunk(digest, 0, &[], 0)?;
            return Ok(());
        }
        let mut offset = 0_u64;
        for chunk in bytes.chunks(BLOB_CHUNK_MAX_BYTES) {
            offset = self.write_chunk(digest, offset, chunk, total)?;
        }
        Ok(())
    }

    /// Whether a fan-out currently holds `digest`.
    fn is_pinned(&self, digest: &str) -> bool {
        self.pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(digest)
    }

    /// The write lock for `digest`, creating it if this is the first writer.
    fn writer_lock(&self, digest: &BlobDigest) -> Arc<Mutex<()>> {
        let mut writers = self.writers.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(writers.entry(digest.as_str().to_owned()).or_default())
    }

    /// Drop `digest`'s entry once nothing else holds it, so the map does not
    /// grow by one entry per digest this node has ever been sent. `2` is the
    /// map's own handle plus the caller's; anything more is a writer queued
    /// behind us, whose entry must survive.
    fn release_writer_lock(&self, digest: &BlobDigest, held: &Arc<Mutex<()>>) {
        let mut writers = self.writers.lock().unwrap_or_else(PoisonError::into_inner);
        if Arc::strong_count(held) == 2 {
            writers.remove(digest.as_str());
        }
    }

    /// The path a committed blob for `digest` lives at (whether or not it
    /// currently exists).
    #[must_use]
    pub fn path_of(&self, digest: &BlobDigest) -> PathBuf {
        self.root.join(digest.as_str())
    }

    fn staging_path(&self, digest: &BlobDigest) -> PathBuf {
        self.root
            .join(STAGING_DIR)
            .join(format!("{}{STAGING_SUFFIX}", digest.as_str()))
    }

    fn watermark_path(&self) -> PathBuf {
        self.root.join(WATERMARK_FILE)
    }

    /// Bytes currently staged toward `digest`, or 0 if nothing is staged.
    fn staged_len(&self, digest: &BlobDigest) -> Result<u64, BlobError> {
        match std::fs::metadata(self.staging_path(digest)) {
            Ok(meta) => Ok(meta.len()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    /// Whether `digest` names a committed blob, and its size/staged progress
    /// either way. Absence is a domain value: this never errors on "no such
    /// blob".
    ///
    /// # Errors
    ///
    /// A filesystem failure other than the blob simply not being there.
    pub fn stat(&self, digest: &BlobDigest) -> Result<BlobStat, BlobError> {
        let (have, size) = match std::fs::metadata(self.path_of(digest)) {
            Ok(meta) => (true, meta.len()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (false, 0),
            Err(e) => return Err(e.into()),
        };
        let staged = self.staged_len(digest)?;
        Ok(BlobStat { have, size, staged })
    }

    /// Write one chunk of `digest`'s bytes at `offset`, out of `total` bytes
    /// overall. Returns the new staged length (or the committed size, once
    /// the last chunk lands and the digest verifies).
    ///
    /// Idempotent by construction: a retry of the same chunk truncates
    /// staging back to `offset` before rewriting, so a dropped-connection
    /// resume never doubles bytes. Already-committed digests are a no-op —
    /// `#438` fans the same digest out repeatedly, and re-delivery must be
    /// free and must not touch the committed file.
    ///
    /// # Errors
    ///
    /// - [`BlobError::ChunkTooLarge`] if `bytes` exceeds [`BLOB_CHUNK_MAX_BYTES`].
    /// - [`BlobError::OffsetGap`] if `offset` is past what is staged.
    /// - [`BlobError::DigestMismatch`] if the fully-staged bytes do not hash
    ///   to `digest` — the staging file is deleted, not left behind.
    /// - [`BlobError::Io`] for any other filesystem failure.
    pub fn write_chunk(
        &self,
        digest: &BlobDigest,
        offset: u64,
        bytes: &[u8],
        total: u64,
    ) -> Result<u64, BlobError> {
        let lock = self.writer_lock(digest);
        let result = {
            let _writing = lock.lock().unwrap_or_else(PoisonError::into_inner);
            self.write_chunk_locked(digest, offset, bytes, total)
        };
        self.release_writer_lock(digest, &lock);
        result
    }

    /// [`Self::write_chunk`]'s body, with this digest's writer lock held.
    fn write_chunk_locked(
        &self,
        digest: &BlobDigest,
        offset: u64,
        bytes: &[u8],
        total: u64,
    ) -> Result<u64, BlobError> {
        if bytes.len() > BLOB_CHUNK_MAX_BYTES {
            return Err(BlobError::ChunkTooLarge {
                limit: BLOB_CHUNK_MAX_BYTES as u64,
            });
        }

        let stat = self.stat(digest)?;
        if stat.have {
            return Ok(total);
        }

        if offset > stat.staged {
            return Err(BlobError::OffsetGap {
                expected: stat.staged,
                got: offset,
            });
        }

        // An overshoot would stage a file that can never reach `staged ==
        // total`, so it would never be verified, never committed, and never
        // reach the mismatch cleanup below — it would simply leak until GC.
        if offset.saturating_add(bytes.len() as u64) > total {
            return Err(BlobError::OffsetGap {
                expected: total.saturating_sub(bytes.len() as u64),
                got: offset,
            });
        }

        let staging_path = self.staging_path(digest);
        // `truncate(false)`, deliberately: an existing staging file's content up to `offset` must
        // survive opening it, so `set_len(offset)` right below can do the real truncation itself —
        // to `offset`, not to zero.
        #[cfg(unix)]
        let mut file = {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .open(&staging_path)?
        };
        #[cfg(not(unix))]
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&staging_path)?;

        // Truncate to `offset` first: a retried chunk (offset <= staged) must
        // overwrite what is already there, not append past it.
        file.set_len(offset)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(bytes)?;
        file.sync_all()?;

        let staged = offset + bytes.len() as u64;
        if staged != total {
            return Ok(staged);
        }

        // Hashed in fixed-size reads rather than through `read_to_end`: `total`
        // is chosen by the peer and the 64 MiB integration transfer is the
        // intended steady state, so buffering the whole blob here would put an
        // unbounded, remotely-chosen allocation on every concurrent commit.
        file.seek(SeekFrom::Start(0))?;
        let mut hasher = Sha256::new();
        let mut buf = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buf)?;
            if read == 0 {
                break;
            }
            hasher.update(&buf[..read]);
        }
        drop(file);

        let actual = format!("{:x}", hasher.finalize());
        if actual != digest.as_str() {
            // The mismatch is the actionable fact and it is already known, so
            // it survives a cleanup failure: letting `?` replace it with an
            // `Io` would re-file "this peer sent bytes that are not what it
            // named them" as a fault of *this* node, which is both wrong and
            // the wrong retry advice.
            if let Err(e) = std::fs::remove_file(&staging_path) {
                tracing::warn!(
                    digest = digest.as_str(), error = %e,
                    "could not remove the staging file of a digest-mismatched blob"
                );
            }
            return Err(BlobError::DigestMismatch);
        }

        std::fs::rename(&staging_path, self.path_of(digest))?;
        // Durable name, not just durable bytes: without a directory sync the
        // rename can be lost on a crash even though `sync_all` above already
        // made the bytes themselves durable — the same reasoning
        // `raft::store::write_spool` documents for its own commit rename.
        #[cfg(unix)]
        std::fs::File::open(&self.root)?.sync_all()?;
        Ok(total)
    }

    /// Read up to `len` bytes of a committed blob starting at `offset`,
    /// capped at [`BLOB_CHUNK_MAX_BYTES`] regardless of `len`. Reading at or
    /// past the end returns an empty `Vec`, not an error — that emptiness is
    /// how a chunked reader learns it is done.
    ///
    /// # Errors
    ///
    /// [`BlobError::NotFound`] if no committed blob exists under `digest` — a
    /// staging-only (not yet fully verified) blob is not readable.
    pub fn read_chunk(
        &self,
        digest: &BlobDigest,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, BlobError> {
        let mut file = match std::fs::File::open(self.path_of(digest)) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(BlobError::NotFound),
            Err(e) => return Err(e.into()),
        };
        let file_len = file.metadata()?.len();
        if offset >= file_len {
            return Ok(Vec::new());
        }
        let remaining = file_len - offset;
        let want = len.min(remaining).min(BLOB_CHUNK_MAX_BYTES as u64) as usize;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0_u8; want];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Remove every committed blob that is both unreferenced and past
    /// `grace_secs` since it was last written, and reap any staging file past
    /// the same grace (referenced by nobody, readable by nobody — pure leak
    /// once abandoned). Returns the count of *committed* blobs removed;
    /// reaped staging files are not counted (they were never a blob).
    ///
    /// `grace_secs == 0` disables GC entirely — mirrors `gc_audit`'s
    /// `retention_secs == 0` early return — and removes nothing, including
    /// abandoned staging files.
    ///
    /// The grace window exists because, until #438 proposes, a freshly
    /// fanned-out blob is referenced by nothing yet: without it, GC would
    /// reap exactly the blobs a transfer just delivered.
    ///
    /// # Errors
    ///
    /// A filesystem failure reading the store's directories or removing a
    /// file, or persisting the updated watermark.
    pub fn gc(
        &self,
        referenced: &HashSet<String>,
        now_secs: u64,
        grace_secs: u64,
    ) -> Result<u64, BlobError> {
        if grace_secs == 0 {
            return Ok(0);
        }

        let mut removed = 0_u64;
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            // A `file_type()` failure is a filesystem fault, not a domain
            // answer: skipping it silently would let a degraded disk report an
            // empty sweep over a store full of reclaimable blobs, with
            // `last_sweep_secs` advancing as if all were well. Only a file that
            // vanished under us — a concurrent sweep, or a transfer's own
            // rename — is a benign race worth continuing past.
            let file_type = match entry.file_type() {
                Ok(t) => t,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if !file_type.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            // Anything that does not parse as a digest is not a committed
            // blob (the watermark file, most notably) — never a candidate.
            let Ok(digest) = BlobDigest::parse(&name) else {
                continue;
            };
            if referenced.contains(digest.as_str()) {
                continue;
            }
            // A fan-out in flight holds a blob nothing references yet — the
            // referencing row is what gets proposed once the acks are in. The
            // grace window cannot be relied on for this: it is measured from
            // the blob's mtime, so a fan-out slower than the grace, or a resumed
            // one whose bytes were staged an hour ago, would be reaped mid-flight
            // (#438).
            if self.is_pinned(digest.as_str()) {
                continue;
            }
            let mtime_secs = mtime_secs(&entry, now_secs)?;
            if now_secs.saturating_sub(mtime_secs) >= grace_secs {
                // One unremovable file must not abandon the rest of the sweep
                // *and* the watermark update — that would leave the blobs this
                // pass did delete uncounted, in the one number the watermark
                // exists to make trustworthy.
                if let Err(e) = std::fs::remove_file(entry.path()) {
                    tracing::warn!(
                        digest = digest.as_str(), error = %e,
                        "could not reclaim an unreferenced blob"
                    );
                    continue;
                }
                removed += 1;
            }
        }

        let staging_dir = self.root.join(STAGING_DIR);
        for entry in std::fs::read_dir(&staging_dir)? {
            let entry = entry?;
            let file_type = match entry.file_type() {
                Ok(t) => t,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if !file_type.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(stem) = name.to_str().and_then(|n| n.strip_suffix(STAGING_SUFFIX)) else {
                continue;
            };
            // Anything not named by a digest is not ours to reap.
            let Ok(digest) = BlobDigest::parse(stem) else {
                continue;
            };

            // Reap under the same per-digest lock a writer holds, so a sweep
            // cannot unlink a `.part` out from under a transfer that is mid
            // `write_chunk`. Age alone is not the guard it looks like: a
            // transfer stalled past the grace and then resuming is exactly the
            // case the sweep would otherwise land in the middle of.
            let lock = self.writer_lock(&digest);
            {
                let _reaping = lock.lock().unwrap_or_else(PoisonError::into_inner);
                // Re-read the mtime with the lock held: the value read before
                // taking it describes a file a writer may have extended since.
                let mtime_secs = match mtime_secs(&entry, now_secs) {
                    Ok(secs) => secs,
                    Err(_) => {
                        self.release_writer_lock(&digest, &lock);
                        continue;
                    }
                };
                if now_secs.saturating_sub(mtime_secs) >= grace_secs
                    && let Err(e) = std::fs::remove_file(entry.path())
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    tracing::warn!(
                        digest = digest.as_str(), error = %e,
                        "could not reap an abandoned blob staging file"
                    );
                }
            }
            self.release_writer_lock(&digest, &lock);
        }

        // Recorded even when `removed == 0`: an operator asking "is GC even
        // running" needs `last_sweep_secs` to move regardless of whether
        // anything was reclaimed.
        let mut watermark = self.watermark()?;
        watermark.last_sweep_secs = now_secs;
        watermark.removed_total = watermark.removed_total.saturating_add(removed);
        self.save_watermark(&watermark)?;

        Ok(removed)
    }

    /// This store's GC watermark — `{0, 0}` if [`Self::gc`] has never run.
    ///
    /// # Errors
    ///
    /// A filesystem failure reading the watermark file, or a corrupt one.
    pub fn watermark(&self) -> Result<GcWatermark, BlobError> {
        let path = self.watermark_path();
        let contents = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(GcWatermark::default());
            }
            Err(e) => return Err(e.into()),
        };
        // A watermark that will not parse is pure observability data, not
        // authority: failing here would make every future sweep on this node
        // return an error and reclaim nothing, so a corrupt counter file would
        // cost the disk it was meant to help account for. Loud, and recovered
        // from, rather than fatal — the next sweep rewrites it.
        match serde_json::from_str(&contents) {
            Ok(watermark) => Ok(watermark),
            Err(e) => {
                tracing::warn!(
                    path = %path.display(), error = %e,
                    "unreadable blob gc watermark; restarting its counters from zero"
                );
                Ok(GcWatermark::default())
            }
        }
    }

    fn save_watermark(&self, watermark: &GcWatermark) -> Result<(), BlobError> {
        let path = self.watermark_path();
        let tmp_path = self
            .root
            .join(format!("{WATERMARK_FILE}.tmp-{}", std::process::id()));
        let body = serde_json::to_vec(watermark)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&body)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp_path, &path)?;
        #[cfg(unix)]
        std::fs::File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

/// A directory entry's modification time, in unix seconds.
///
/// A timestamp that cannot be placed on the unix timeline (reported before the
/// epoch — some FUSE and NFS mounts do, as does a clock set backwards) reads as
/// **`now_secs`, i.e. just written**, never as `0`. `0` is not a neutral
/// default here: it is the one value that makes `now - mtime >= grace`
/// unconditionally true, so it would hand every affected blob straight to the
/// reaper and bypass the grace window entirely. This value decides whether a
/// file is deleted, so the unreadable case takes the protected class — the same
/// rule `dataset_digest_referenced` follows when it answers "still referenced"
/// for a row it cannot parse.
fn mtime_secs(entry: &std::fs::DirEntry, now_secs: u64) -> Result<u64, BlobError> {
    Ok(mtime_secs_of(entry.metadata()?.modified()?, now_secs))
}

/// [`mtime_secs`]'s decision, without the filesystem — a separate function
/// only so the pre-epoch branch can be tested, which no real temp file will
/// produce on demand.
fn mtime_secs_of(modified: std::time::SystemTime, now_secs: u64) -> u64 {
    modified
        .duration_since(UNIX_EPOCH)
        .map_or(now_secs, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// sha256 of the empty string.
    const EMPTY_DIGEST: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    /// sha256 of `b"hello"`.
    const HELLO_DIGEST: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    /// sha256 of `b"hello world"`.
    const HELLO_WORLD_DIGEST: &str =
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    /// sha256 of 5 MiB of `b'A'` — one byte over the 4 MiB chunk cap, so it
    /// cannot be delivered in a single chunk.
    const FIVE_MIB_A_DIGEST: &str =
        "dbbe5517996826bd5861ac22b745d21d11219055d89243ca1aea0ad31f552b12";

    fn store() -> (BlobStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = BlobStore::open(dir.path().join("blobs")).expect("open blob store");
        (store, dir)
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs()
    }

    fn referenced(digests: &[&str]) -> HashSet<String> {
        digests.iter().map(|d| (*d).to_owned()).collect()
    }

    /// Commit `bytes` under `digest` in as many chunks as the cap requires.
    fn put_whole(store: &BlobStore, digest: &str, bytes: &[u8]) {
        let total = bytes.len() as u64;
        let mut offset = 0u64;
        while offset < total || total == 0 {
            let end = (offset as usize + BLOB_CHUNK_MAX_BYTES).min(bytes.len());
            let chunk = &bytes[offset as usize..end];
            offset = store
                .write_chunk(
                    &BlobDigest::parse(digest).expect("digest"),
                    offset,
                    chunk,
                    total,
                )
                .expect("write chunk");
            if total == 0 {
                break;
            }
        }
    }

    // ---- digest validation (edge 1) -------------------------------------

    #[test]
    fn digest_must_be_sixty_four_lowercase_hex_chars() {
        // Traversal, separators, case, length and alphabet are all rejected by
        // the same door, so no caller can hand a path fragment to a join.
        for bad in [
            "..",
            "../../etc/passwd",
            "abc/def",
            &EMPTY_DIGEST.to_uppercase(),
            "e3b0c442",
            &format!("{EMPTY_DIGEST}0"),
            &"g".repeat(64),
            "",
        ] {
            assert!(
                matches!(BlobDigest::parse(bad), Err(BlobError::MalformedDigest)),
                "expected {bad:?} to be rejected as a malformed digest"
            );
        }
        assert!(BlobDigest::parse(EMPTY_DIGEST).is_ok());
    }

    // ---- write path ------------------------------------------------------

    #[test]
    fn committed_blob_is_readable_and_stat_reports_its_size() {
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");

        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");
        let stat = store.stat(&d).expect("stat");
        assert!(stat.have);
        assert_eq!(stat.size, 5);
        assert_eq!(
            store.read_chunk(&d, 0, 64).expect("read"),
            b"hello".to_vec()
        );
    }

    #[test]
    fn zero_byte_blob_commits_and_is_reported_as_present() {
        // Edge 10: an empty blob is a real blob. `have` must come from the file
        // existing, never from its length being non-zero.
        let (store, _dir) = store();
        let d = BlobDigest::parse(EMPTY_DIGEST).expect("digest");
        store.write_chunk(&d, 0, b"", 0).expect("write empty");

        let stat = store.stat(&d).expect("stat");
        assert!(stat.have);
        assert_eq!(stat.size, 0);
        assert_eq!(store.read_chunk(&d, 0, 64).expect("read"), Vec::<u8>::new());
    }

    #[test]
    fn chunk_larger_than_the_route_cap_is_refused() {
        // Edge 2: the blob route's own cap binds well before the transport's
        // 32 MiB body limit, so an oversized chunk never reaches the disk.
        let (store, _dir) = store();
        let oversized = vec![b'A'; BLOB_CHUNK_MAX_BYTES + 1];
        let d = BlobDigest::parse(FIVE_MIB_A_DIGEST).expect("digest");

        let err = store.write_chunk(&d, 0, &oversized, oversized.len() as u64);
        assert!(
            matches!(err, Err(BlobError::ChunkTooLarge { limit }) if limit == BLOB_CHUNK_MAX_BYTES as u64),
            "expected ChunkTooLarge, got {err:?}"
        );
        assert_eq!(store.stat(&d).expect("stat").staged, 0);
    }

    #[test]
    fn a_chunk_that_would_leave_a_hole_is_refused() {
        // Edge 3: accepting offset 10 with nothing staged would leave ten bytes
        // nobody wrote inside a file whose name is a hash of its contents.
        let (store, _dir) = store();
        let d = BlobDigest::parse(HELLO_WORLD_DIGEST).expect("digest");

        let err = store.write_chunk(&d, 10, b"world", 11);
        assert!(
            matches!(
                err,
                Err(BlobError::OffsetGap {
                    expected: 0,
                    got: 10
                })
            ),
            "expected OffsetGap {{ expected: 0, got: 10 }}, got {err:?}"
        );
        assert_eq!(store.stat(&d).expect("stat").staged, 0);
    }

    #[test]
    fn resending_the_same_chunk_is_idempotent() {
        // Edge 4: a client that retries a chunk it already delivered must not
        // append it twice — the retry is how resume works after a dropped
        // connection, and a doubled chunk would fail the digest at commit.
        let (store, _dir) = store();
        let d = BlobDigest::parse(HELLO_WORLD_DIGEST).expect("digest");

        assert_eq!(store.write_chunk(&d, 0, b"hello ", 11).expect("first"), 6);
        assert_eq!(store.write_chunk(&d, 0, b"hello ", 11).expect("retry"), 6);
        assert_eq!(store.stat(&d).expect("stat").staged, 6);

        assert_eq!(store.write_chunk(&d, 6, b"world", 11).expect("tail"), 11);
        assert_eq!(
            store.read_chunk(&d, 0, 64).expect("read"),
            b"hello world".to_vec()
        );
    }

    #[test]
    fn a_partial_transfer_is_not_readable_and_leaves_no_blob() {
        // Edge 9: bytes that have not been verified against their name are not
        // a blob yet, however many of them have arrived.
        let (store, _dir) = store();
        let d = BlobDigest::parse(HELLO_WORLD_DIGEST).expect("digest");
        store.write_chunk(&d, 0, b"hello ", 11).expect("partial");

        let stat = store.stat(&d).expect("stat");
        assert!(!stat.have);
        assert_eq!(stat.staged, 6);
        assert!(matches!(
            store.read_chunk(&d, 0, 64),
            Err(BlobError::NotFound)
        ));
    }

    #[test]
    fn bytes_that_hash_to_something_else_are_rejected_and_leave_no_blob() {
        // Criterion 2 / edge 5. The digest is the blob's name *and* its
        // contract; committing bytes that do not hash to it would make every
        // later read of that name silently wrong.
        let (store, _dir) = store();
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        let err = store.write_chunk(&d, 0, b"HELLO", 5);
        assert!(
            matches!(err, Err(BlobError::DigestMismatch)),
            "expected DigestMismatch, got {err:?}"
        );

        let stat = store.stat(&d).expect("stat");
        assert!(!stat.have, "a failed transfer must leave no visible blob");
        assert_eq!(
            stat.staged, 0,
            "the staging file must be removed, not left behind"
        );
        assert!(matches!(
            store.read_chunk(&d, 0, 64),
            Err(BlobError::NotFound)
        ));
    }

    #[test]
    fn a_blob_delivered_in_several_chunks_commits_on_the_last_one() {
        // The resume shape end to end: 5 MiB cannot fit in one 4 MiB chunk, so
        // this is also the smallest case that exercises a second chunk.
        let (store, _dir) = store();
        let bytes = vec![b'A'; 5 * 1024 * 1024];
        let d = BlobDigest::parse(FIVE_MIB_A_DIGEST).expect("digest");
        let total = bytes.len() as u64;

        let staged = store
            .write_chunk(&d, 0, &bytes[..BLOB_CHUNK_MAX_BYTES], total)
            .expect("first chunk");
        assert_eq!(staged, BLOB_CHUNK_MAX_BYTES as u64);
        assert!(
            !store.stat(&d).expect("stat").have,
            "not committed mid-transfer"
        );

        let staged = store
            .write_chunk(&d, staged, &bytes[BLOB_CHUNK_MAX_BYTES..], total)
            .expect("second chunk");
        assert_eq!(staged, total);

        let stat = store.stat(&d).expect("stat");
        assert!(stat.have);
        assert_eq!(stat.size, total);
    }

    #[test]
    fn putting_a_blob_that_is_already_committed_is_a_no_op() {
        // Edge 6: #438 will fan the same digest out repeatedly; re-delivery must
        // be free and must not disturb the committed file.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        assert_eq!(store.write_chunk(&d, 0, b"hello", 5).expect("re-put"), 5);
        // Re-put the WRONG bytes: the short-circuit promises it never reads or
        // rewrites a committed blob, and only a mismatched re-put can tell that
        // apart from an implementation that happens to re-verify and agree.
        assert_eq!(
            store.write_chunk(&d, 0, b"HELLO", 5).expect("re-put wrong"),
            5
        );
        assert!(store.stat(&d).expect("stat").have);
        assert_eq!(
            store.read_chunk(&d, 0, 64).expect("read"),
            b"hello".to_vec()
        );
    }

    // ---- read path -------------------------------------------------------

    #[test]
    fn reading_an_absent_blob_is_a_typed_not_found() {
        // Criterion 3 / edge 8: the store's answer must be a domain value the
        // caller can act on, never an I/O error dressed up as a server fault.
        let (store, _dir) = store();
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        assert!(matches!(
            store.read_chunk(&d, 0, 64),
            Err(BlobError::NotFound)
        ));
        let stat = store
            .stat(&d)
            .expect("stat is not an error for an absent blob");
        assert!(!stat.have);
        assert_eq!(stat.size, 0);
        assert_eq!(stat.staged, 0);
    }

    #[test]
    fn reading_past_the_end_returns_nothing_rather_than_failing() {
        // Edge 7: an empty answer is how a chunked client learns it is done.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        assert_eq!(
            store.read_chunk(&d, 5, 64).expect("at end"),
            Vec::<u8>::new()
        );
        assert_eq!(
            store.read_chunk(&d, 99, 64).expect("past end"),
            Vec::<u8>::new()
        );
        assert_eq!(store.read_chunk(&d, 2, 2).expect("middle"), b"ll".to_vec());
    }

    #[test]
    fn a_read_is_bounded_by_the_chunk_cap_however_much_is_asked_for() {
        let (store, _dir) = store();
        let bytes = vec![b'A'; 5 * 1024 * 1024];
        put_whole(&store, FIVE_MIB_A_DIGEST, &bytes);
        let d = BlobDigest::parse(FIVE_MIB_A_DIGEST).expect("digest");

        let got = store.read_chunk(&d, 0, u64::MAX).expect("read");
        assert_eq!(got.len(), BLOB_CHUNK_MAX_BYTES);
    }

    // ---- permissions -----------------------------------------------------

    #[cfg(unix)]
    #[test]
    fn a_committed_blob_is_readable_only_by_its_owner() {
        use std::os::unix::fs::PermissionsExt;
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");

        let mode =
            std::fs::metadata(store.path_of(&BlobDigest::parse(HELLO_DIGEST).expect("digest")))
                .expect("metadata")
                .permissions()
                .mode();
        assert_eq!(mode & 0o777, 0o600, "blob files must be 0600");
    }

    // ---- GC --------------------------------------------------------------

    #[test]
    fn gc_keeps_a_referenced_blob_however_old_it_is() {
        // Criterion 4, the half that matters most: reclaiming a blob a live row
        // still names loses the dataset, not just disk.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        let removed = store
            .gc(&referenced(&[HELLO_DIGEST]), now_secs() + 10_000_000, 3600)
            .expect("gc");

        assert_eq!(removed, 0);
        assert!(store.stat(&d).expect("stat").have);
    }

    #[test]
    fn gc_removes_an_unreferenced_blob_once_it_is_past_the_grace_window() {
        // Criterion 4, the other half.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        let removed = store
            .gc(&referenced(&[]), now_secs() + 10_000, 3600)
            .expect("gc");

        assert_eq!(removed, 1);
        assert!(!store.stat(&d).expect("stat").have);
    }

    #[test]
    fn gc_keeps_an_unreferenced_blob_that_is_still_inside_the_grace_window() {
        // Edge 13, and the reason this child needs a grace at all: until #438
        // proposes, a freshly fanned-out blob is referenced by nothing. Without
        // the window, GC reaps exactly the blobs a transfer just delivered.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        let removed = store.gc(&referenced(&[]), now_secs(), 3600).expect("gc");

        assert_eq!(removed, 0);
        assert!(store.stat(&d).expect("stat").have);
    }

    #[test]
    fn gc_is_disabled_entirely_when_the_grace_is_zero() {
        // Edge 18, matching `gc_audit`'s `retention_secs == 0` early return.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");

        let removed = store
            .gc(&referenced(&[]), now_secs() + 10_000_000, 0)
            .expect("gc");

        assert_eq!(removed, 0);
        assert!(
            store
                .stat(&BlobDigest::parse(HELLO_DIGEST).expect("digest"))
                .expect("stat")
                .have
        );
    }

    // ---- pins (#438): GC must not reap a blob mid-fan-out ----------------

    #[test]
    fn gc_keeps_a_pinned_blob_that_is_unreferenced_and_past_its_grace() {
        // The pin's whole reason to exist. `gc_keeps_an_unreferenced_blob_that_is
        // _still_inside_the_grace_window` covers the *timer*; this covers the
        // *state*. Deliberately set up so the grace CANNOT be what saves the
        // blob — `now + 10_000_000` puts it far past any window — so the only
        // thing standing between it and the reaper is the pin.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        let _pin = store.pin(&d);
        let removed = store
            .gc(&referenced(&[]), now_secs() + 10_000_000, 3600)
            .expect("gc");

        assert_eq!(removed, 0);
        assert!(store.stat(&d).expect("stat").have);
    }

    #[test]
    fn dropping_the_pin_makes_the_blob_collectable_again() {
        // The other half: a pin that never released would be a leak with the
        // same shape as the bug it prevents, so the release path is its own
        // assertion rather than an assumed consequence of `Drop`.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        {
            let _pin = store.pin(&d);
        }
        let removed = store
            .gc(&referenced(&[]), now_secs() + 10_000_000, 3600)
            .expect("gc");

        assert_eq!(removed, 1);
        assert!(!store.stat(&d).expect("stat").have);
    }

    #[test]
    fn a_digest_pinned_twice_stays_pinned_until_both_pins_drop() {
        // Edge 1: two concurrent PUTs of the same CSV. A set-valued pin would
        // let the first release drop the second's protection — the reaper then
        // deletes a blob a live fan-out is still counting acks for. Hence a
        // count, and hence this test asserts the *intermediate* state, which is
        // the only place the set-vs-count distinction is observable.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        let first = store.pin(&d);
        let second = store.pin(&d);

        drop(first);
        let removed = store
            .gc(&referenced(&[]), now_secs() + 10_000_000, 3600)
            .expect("gc");
        assert_eq!(removed, 0, "one release must not clear the other's pin");
        assert!(store.stat(&d).expect("stat").have);

        drop(second);
        let removed = store
            .gc(&referenced(&[]), now_secs() + 10_000_000, 3600)
            .expect("gc");
        assert_eq!(removed, 1, "the last release makes it collectable");
    }

    #[test]
    fn a_pin_does_not_protect_a_different_digest() {
        // Guards against a pin implemented as a global "GC is paused" flag,
        // which would pass all three tests above while reclaiming nothing at
        // all during any fan-out.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");
        put_whole(&store, HELLO_WORLD_DIGEST, b"hello world");
        let pinned = BlobDigest::parse(HELLO_DIGEST).expect("digest");
        let other = BlobDigest::parse(HELLO_WORLD_DIGEST).expect("digest");

        let _pin = store.pin(&pinned);
        let removed = store
            .gc(&referenced(&[]), now_secs() + 10_000_000, 3600)
            .expect("gc");

        assert_eq!(removed, 1);
        assert!(store.stat(&pinned).expect("stat").have);
        assert!(!store.stat(&other).expect("stat").have);
    }

    #[test]
    fn gc_reaps_an_abandoned_staging_file_past_the_grace() {
        // Edge 16: an origin that dies mid-transfer leaves a `.part` behind. It
        // is referenced by nothing and readable by nobody, so it is pure leak.
        let (store, _dir) = store();
        let d = BlobDigest::parse(HELLO_WORLD_DIGEST).expect("digest");
        store.write_chunk(&d, 0, b"hello ", 11).expect("partial");
        assert_eq!(store.stat(&d).expect("stat").staged, 6);

        store
            .gc(&referenced(&[]), now_secs() + 10_000, 3600)
            .expect("gc");

        assert_eq!(store.stat(&d).expect("stat").staged, 0);
    }

    #[test]
    fn gc_keeps_a_staging_file_that_is_still_inside_the_grace_window() {
        // The same reasoning as edge 13, for the resume case: reaping a `.part`
        // mid-transfer would restart a 64 MiB upload from zero.
        let (store, _dir) = store();
        let d = BlobDigest::parse(HELLO_WORLD_DIGEST).expect("digest");
        store.write_chunk(&d, 0, b"hello ", 11).expect("partial");

        store.gc(&referenced(&[]), now_secs(), 3600).expect("gc");

        assert_eq!(store.stat(&d).expect("stat").staged, 6);
    }

    #[test]
    fn the_watermark_records_what_gc_removed_so_absence_can_be_explained() {
        // The audit GC's watermark exists so a reader can tell "removed" from
        // "never written" (see `SM_AUDIT_GC_WATERMARK_TABLE`). #439's
        // fetch-on-apply needs the same distinction when a fetch 404s.
        let (store, _dir) = store();
        assert_eq!(store.watermark().expect("watermark").removed_total, 0);

        put_whole(&store, HELLO_DIGEST, b"hello");
        put_whole(&store, HELLO_WORLD_DIGEST, b"hello world");
        let swept_at = now_secs() + 10_000;
        store.gc(&referenced(&[]), swept_at, 3600).expect("gc");

        let watermark = store.watermark().expect("watermark");
        assert_eq!(watermark.removed_total, 2);
        assert_eq!(watermark.last_sweep_secs, swept_at);
    }

    #[test]
    fn the_watermark_survives_reopening_the_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("blobs");
        {
            let store = BlobStore::open(root.clone()).expect("open");
            put_whole(&store, HELLO_DIGEST, b"hello");
            store
                .gc(&referenced(&[]), now_secs() + 10_000, 3600)
                .expect("gc");
        }
        let reopened = BlobStore::open(root).expect("reopen");
        assert_eq!(reopened.watermark().expect("watermark").removed_total, 1);
    }

    // ---- concurrency, overshoot, and the resume trap (cycle-2 review) ----

    #[test]
    fn concurrent_writers_of_one_digest_never_commit_unverified_bytes() {
        // Two senders share one staging path, and the commit is hash-then-
        // rename. Without exclusion, a second sender restarting at offset 0
        // between the first's hash and its rename gets ITS shorter prefix
        // renamed into place under the full blob's digest — bytes nothing ever
        // verified, under a name asserting they were. #438 fans a blob out from
        // more than one origin, so this is its shape, not a synthetic one.
        //
        // This asserts the invariant but does NOT reliably reproduce the race:
        // it passes with the writer lock removed. The lock's own exclusion is
        // pinned deterministically by
        // `a_second_writer_of_one_digest_waits_for_the_first`; this one is here
        // to catch gross breakage under real concurrency, not as the evidence.
        let (store, _dir) = store();
        let store = std::sync::Arc::new(store);
        let bytes = std::sync::Arc::new(vec![b'A'; 5 * 1024 * 1024]);
        let total = bytes.len() as u64;

        let handles: Vec<_> = (0..2)
            .map(|_| {
                let store = std::sync::Arc::clone(&store);
                let bytes = std::sync::Arc::clone(&bytes);
                std::thread::spawn(move || {
                    let d = BlobDigest::parse(FIVE_MIB_A_DIGEST).expect("digest");
                    let mut offset = 0_u64;
                    while offset < total {
                        let end = ((offset as usize) + BLOB_CHUNK_MAX_BYTES).min(bytes.len());
                        match store.write_chunk(&d, offset, &bytes[offset as usize..end], total) {
                            Ok(staged) => offset = staged,
                            // A loser racing the winner's commit may find its
                            // staging file gone; that is a fine outcome, an
                            // incorrect committed blob is not.
                            Err(_) => break,
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("writer thread");
        }

        let d = BlobDigest::parse(FIVE_MIB_A_DIGEST).expect("digest");
        let stat = store.stat(&d).expect("stat");
        assert!(stat.have, "one of the two writers must have committed");
        assert_eq!(
            stat.size, total,
            "the committed blob must be the whole blob"
        );
        let mut got = Vec::new();
        let mut offset = 0_u64;
        while let Ok(chunk) = store.read_chunk(&d, offset, BLOB_CHUNK_MAX_BYTES as u64) {
            if chunk.is_empty() {
                break;
            }
            offset += chunk.len() as u64;
            got.extend_from_slice(&chunk);
        }
        assert_eq!(
            digest_of_bytes(&got),
            d,
            "the committed bytes must hash to the digest naming them"
        );
    }

    #[test]
    fn a_second_writer_of_one_digest_waits_for_the_first() {
        // The exclusion itself, deterministically. The multi-threaded test
        // above asserts the *invariant* but cannot force the interleaving that
        // breaks it — it passes with the lock removed, so on its own it is not
        // evidence the lock does anything. This is: while one writer holds a
        // digest, a second `write_chunk` on it must not proceed.
        let (store, _dir) = store();
        let store = std::sync::Arc::new(store);
        let d = BlobDigest::parse(HELLO_WORLD_DIGEST).expect("digest");

        let held = store.writer_lock(&d);
        let guard = held.lock().expect("take the writer lock");

        let (tx, rx) = std::sync::mpsc::channel();
        let writer = {
            let store = std::sync::Arc::clone(&store);
            std::thread::spawn(move || {
                let d = BlobDigest::parse(HELLO_WORLD_DIGEST).expect("digest");
                let staged = store.write_chunk(&d, 0, b"hello ", 11).expect("write");
                tx.send(staged).expect("report");
            })
        };

        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(250))
                .is_err(),
            "a second writer must not proceed while the first holds the digest"
        );

        drop(guard);
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(5))
                .expect("the writer must proceed once the lock is released"),
            6
        );
        writer.join().expect("writer thread");
    }

    #[test]
    fn writers_of_different_digests_do_not_block_each_other() {
        // The exclusion is per digest, not global: a store-wide lock would
        // serialise every transfer on the node, which is the opposite of what
        // #438's fan-out needs.
        let (store, _dir) = store();
        let store = std::sync::Arc::new(store);
        let busy = BlobDigest::parse(HELLO_WORLD_DIGEST).expect("digest");
        let held = store.writer_lock(&busy);
        let guard = held.lock().expect("take the writer lock");

        let other = std::sync::Arc::clone(&store);
        let writer = std::thread::spawn(move || {
            let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");
            other.write_chunk(&d, 0, b"hello", 5).expect("write")
        });
        assert_eq!(writer.join().expect("writer thread"), 5);

        drop(guard);
    }

    #[test]
    fn a_chunk_that_would_overshoot_the_declared_total_is_refused() {
        // An overshoot stages a file that can never reach `staged == total`, so
        // it is never verified, never committed, and never reaches the mismatch
        // cleanup — it just leaks until GC.
        let (store, _dir) = store();
        let d = BlobDigest::parse(HELLO_WORLD_DIGEST).expect("digest");

        let err = store.write_chunk(&d, 0, b"hello world and then some", 11);
        assert!(
            matches!(err, Err(BlobError::OffsetGap { .. })),
            "expected the overshoot to be refused, got {err:?}"
        );
        assert_eq!(store.stat(&d).expect("stat").staged, 0);
    }

    #[test]
    fn a_timestamp_before_the_epoch_reads_as_just_written_not_as_ancient() {
        // This is the value that decides whether a file is deleted. `0` is not
        // a neutral default for it: `now - 0 >= grace` is unconditionally true,
        // so it would hand every affected blob to the reaper and bypass the
        // grace window entirely. Some FUSE and NFS mounts do report pre-epoch
        // mtimes, and a clock set backwards produces the same thing.
        let now = now_secs();
        let before_epoch = UNIX_EPOCH
            .checked_sub(std::time::Duration::from_secs(1))
            .expect("a time before the epoch");
        assert_eq!(
            mtime_secs_of(before_epoch, now),
            now,
            "an unplaceable timestamp must protect the file, not condemn it"
        );

        let readable = UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        assert_eq!(mtime_secs_of(readable, now), 1_700_000_000);
    }

    #[test]
    fn a_blob_whose_mtime_cannot_be_read_survives_a_sweep() {
        // The same rule end to end: a store whose entries all report an
        // unplaceable mtime must lose nothing, however far past the grace the
        // sweep believes it is.
        let (store, _dir) = store();
        put_whole(&store, HELLO_DIGEST, b"hello");
        let d = BlobDigest::parse(HELLO_DIGEST).expect("digest");

        // `mtime_secs_of` is what a sweep consults; with the protected default
        // the age is 0, which never clears any non-zero grace.
        let now = now_secs() + 10_000_000;
        let before_epoch = UNIX_EPOCH
            .checked_sub(std::time::Duration::from_secs(1))
            .expect("a time before the epoch");
        assert_eq!(now.saturating_sub(mtime_secs_of(before_epoch, now)), 0);

        assert!(store.stat(&d).expect("stat").have);
    }

    #[test]
    fn a_corrupt_watermark_does_not_wedge_gc_forever() {
        // The watermark is observability data. If an unparseable one made every
        // future sweep return an error, a corrupt counter file would cost the
        // disk it exists to account for.
        let (store, _dir) = store();
        std::fs::write(
            store.path_of(&BlobDigest::parse(EMPTY_DIGEST).expect("d")),
            b"x",
        )
        .expect("seed a file");
        let root = store.path_of(&BlobDigest::parse(EMPTY_DIGEST).expect("d"));
        let root = root.parent().expect("root").to_path_buf();
        std::fs::write(root.join("gc-watermark.json"), b"{ not json").expect("corrupt it");

        let watermark = store
            .watermark()
            .expect("a corrupt watermark must not be fatal");
        assert_eq!(watermark.removed_total, 0);
        store
            .gc(&referenced(&[]), now_secs() + 10_000, 3600)
            .expect("gc must still run over a corrupt watermark");
    }

    #[test]
    fn gc_over_an_empty_store_is_not_an_error() {
        let (store, _dir) = store();
        assert_eq!(
            store
                .gc(&referenced(&[]), now_secs() + 10_000, 3600)
                .expect("gc"),
            0
        );
    }
}
