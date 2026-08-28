//! [`BlobTransfer`]: the node-driven side of the blob transport (#437) — the
//! counterpart to `blobs::routes`, which serves it.
//!
//! Every call is single-attempt (`RpcClient::call_once`, never `call`): a
//! retry underneath a multi-MiB body would re-send it, the trap
//! `raft::network`'s `Delivery` documents for Raft's own bulk transfers
//! (#411/#428). The size-aware deadline is `replication_deadline`, reused here
//! rather than a second copy of the 1 MiB/s floor.

use std::net::SocketAddr;
use std::sync::Arc;

use super::{BLOB_CHUNK_MAX_BYTES, BLOB_PATH_PREFIX, BlobDigest, BlobStat};
use crate::raft::network::replication_deadline;
use crate::rpc::{RpcClient, RpcError};

/// What one [`BlobTransfer::put`] actually did: how much the peer already
/// held before this call, and how many bytes this call put on the wire.
///
/// Two fields rather than one, because "the blob arrived" cannot tell a fresh
/// transfer from a resumed one — only `bytes_sent` can, and a resume that
/// quietly restarted from zero would still leave a correct blob behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutOutcome {
    /// Bytes the peer had already staged before this call.
    pub resumed_from: u64,
    /// Bytes this call actually sent.
    pub bytes_sent: u64,
    /// Whether `peer`'s build applies a digest-only `ControlOp` (#481) — surfaced from the
    /// `?stat` probe `put` already makes of every target before deciding what to send, so a
    /// caller (`RaftNode::fan_out_blob`) learns each byte-recipient's sideload capability for
    /// free rather than issuing a second round trip it already paid for.
    pub applies_digest_only: bool,
}

/// The node-driven side of the blob transport: moves bytes to and from a
/// peer's [`super::BlobStore`] over the signed cluster port.
pub struct BlobTransfer {
    client: Arc<RpcClient>,
}

impl BlobTransfer {
    #[must_use]
    pub fn new(client: Arc<RpcClient>) -> Self {
        Self { client }
    }

    /// `peer`'s view of `digest`: whether it has it, and its size/staged
    /// progress either way.
    ///
    /// # Errors
    ///
    /// Any [`RpcError`] the call itself fails with. Absence is not an error —
    /// see [`BlobStat::have`].
    pub async fn stat(&self, peer: SocketAddr, digest: &BlobDigest) -> Result<BlobStat, RpcError> {
        let path = format!("{BLOB_PATH_PREFIX}{}?stat=1", digest.as_str());
        let deadline = replication_deadline(self.client.request_timeout(), 0);
        let body = self
            .client
            .call_once(peer, "GET", &path, Vec::new(), deadline)
            .await?;
        serde_json::from_slice(&body)
            .map_err(|e| RpcError::Handler(format!("decode blob stat: {e}")))
    }

    /// Whether `peer` has `digest` committed.
    ///
    /// Asks `?stat=1` rather than `HEAD`, deliberately. A `HEAD` response
    /// cannot carry a body, and the body is where a 404 says *which* 404 it is
    /// (`"error": "not_found"` vs `"unknown_route"`) — so a bodiless 404 is
    /// indistinguishable from "this peer has no blob route at all", and
    /// answering `false` to it would report a version-skewed peer as merely
    /// lacking the blob. `?stat=1` is the same single round trip and the same
    /// `stat` call on the peer, and it answers unambiguously.
    ///
    /// # Errors
    ///
    /// Any [`RpcError`] the call itself fails with. A peer that simply does
    /// not hold `digest` is `Ok(false)`, not an error.
    pub async fn have(&self, peer: SocketAddr, digest: &BlobDigest) -> Result<bool, RpcError> {
        Ok(self.stat(peer, digest).await?.have)
    }

    /// Whether `peer`'s build applies a digest-only `ControlOp` (#481), probed without any
    /// intent to send it bytes — the capability half of `RaftNode::fan_out_blob`'s sweep for a
    /// member the byte quorum does not include (a learner: it applies the log the same as a
    /// voter, but D-19 never sends it a `put`, so nothing else would ever ask its `?stat`).
    ///
    /// # Errors
    ///
    /// Any [`RpcError`] the call itself fails with.
    pub async fn stat_only(&self, peer: SocketAddr, digest: &BlobDigest) -> Result<bool, RpcError> {
        Ok(self.stat(peer, digest).await?.applies_digest_only)
    }

    /// Fetch `digest` whole from `peer`, looping `GET` chunks until an empty
    /// answer says the blob has been read to its end.
    ///
    /// # Errors
    ///
    /// [`RpcError::NotFound`] if `peer` does not have `digest` committed, or
    /// any other [`RpcError`] the transport fails with.
    pub async fn get(&self, peer: SocketAddr, digest: &BlobDigest) -> Result<Vec<u8>, RpcError> {
        let mut out = Vec::new();
        let mut offset: u64 = 0;
        loop {
            let path = format!(
                "{BLOB_PATH_PREFIX}{}?offset={offset}&len={BLOB_CHUNK_MAX_BYTES}",
                digest.as_str()
            );
            let deadline =
                replication_deadline(self.client.request_timeout(), BLOB_CHUNK_MAX_BYTES);
            let chunk = self
                .client
                .call_once(peer, "GET", &path, Vec::new(), deadline)
                .await?;
            if chunk.is_empty() {
                // Verify what arrived against the name it was fetched under.
                // The whole premise of this module is content addressing, and
                // the one hash it costs turns any corruption — on the peer's
                // disk, or in a transfer that ended short — into a loud error
                // here rather than bytes #439 would apply as if they were the
                // config they claim to be.
                let actual = super::digest_of_bytes(&out);
                if actual != *digest {
                    return Err(RpcError::Handler(format!(
                        "blob {digest} fetched from {peer} hashes to {actual}"
                    )));
                }
                return Ok(out);
            }
            offset += chunk.len() as u64;
            out.extend_from_slice(&chunk);
        }
    }

    /// Put `bytes` (which must hash to `digest`) to `peer`, first probing
    /// what `peer` already has staged and sending only what is missing.
    ///
    /// # Errors
    ///
    /// Any [`RpcError`] the probe or the transfer fails with — including a
    /// receiver's refusal when `bytes` does not hash to `digest`.
    pub async fn put(
        &self,
        peer: SocketAddr,
        digest: &BlobDigest,
        bytes: &[u8],
    ) -> Result<PutOutcome, RpcError> {
        let total = bytes.len() as u64;
        let stat = self.stat(peer, digest).await?;
        if stat.have {
            return Ok(PutOutcome {
                resumed_from: total,
                bytes_sent: 0,
                applies_digest_only: stat.applies_digest_only,
            });
        }
        if total == 0 {
            // A zero-byte blob is still a real blob (edge 10) — it just has
            // no bytes for the loop below to iterate, so it needs exactly one
            // empty commit chunk sent explicitly.
            self.put_chunk(peer, digest, &[], 0, 0).await?;
            return Ok(PutOutcome {
                resumed_from: 0,
                bytes_sent: 0,
                applies_digest_only: stat.applies_digest_only,
            });
        }
        // Never resume at exactly `total`. `write_chunk` commits only on the
        // chunk that lands with `staged == total`, so a peer already holding a
        // full-length staging file (a rename or directory-sync that failed
        // after the bytes were written) would be sent nothing at all and this
        // would return success over a blob that never committed. Re-sending
        // the final byte re-runs the verify-and-rename instead.
        let resumed_from = stat.staged.min(total.saturating_sub(1));
        let bytes_sent = self
            .send_chunks(peer, digest, bytes, resumed_from, total, None)
            .await?;
        Ok(PutOutcome {
            resumed_from,
            bytes_sent,
            applies_digest_only: stat.applies_digest_only,
        })
    }

    /// Send at most `limit` bytes of `bytes` to `peer` and stop, whether or
    /// not the blob completes — how a test simulates a killed origin.
    ///
    /// `pub` only because the integration suite is a separate crate.
    /// Returns the number of bytes actually sent.
    ///
    /// # Errors
    ///
    /// Any [`RpcError`] a chunk send fails with.
    #[doc(hidden)]
    pub async fn put_prefix(
        &self,
        peer: SocketAddr,
        digest: &BlobDigest,
        bytes: &[u8],
        limit: u64,
    ) -> Result<u64, RpcError> {
        let total = bytes.len() as u64;
        self.send_chunks(peer, digest, bytes, 0, total, Some(limit))
            .await
    }

    /// Send `bytes[start..total]` to `peer` in [`BLOB_CHUNK_MAX_BYTES`]
    /// chunks, stopping early once `limit` bytes (if given) have gone out.
    /// Returns the number of bytes actually sent.
    async fn send_chunks(
        &self,
        peer: SocketAddr,
        digest: &BlobDigest,
        bytes: &[u8],
        start: u64,
        total: u64,
        limit: Option<u64>,
    ) -> Result<u64, RpcError> {
        let mut offset = start;
        let mut sent = 0_u64;
        while offset < total {
            if limit.is_some_and(|limit| sent >= limit) {
                break;
            }
            let remaining_in_blob = (total - offset).min(BLOB_CHUNK_MAX_BYTES as u64);
            let chunk_len = match limit {
                Some(limit) => remaining_in_blob.min(limit - sent),
                None => remaining_in_blob,
            };
            // Unreachable as written — `offset < total` makes the remainder at
            // least 1, and the `sent >= limit` break above makes `limit - sent`
            // at least 1. Kept because it is the only thing standing between a
            // future change to either of those and a loop that re-sends the
            // same offset forever: `offset` advances by `chunk_len`, so a zero
            // here never terminates.
            if chunk_len == 0 {
                break;
            }
            let start_idx = offset as usize;
            let end_idx = start_idx + chunk_len as usize;
            self.put_chunk(peer, digest, &bytes[start_idx..end_idx], offset, total)
                .await?;
            offset += chunk_len;
            sent += chunk_len;
        }
        Ok(sent)
    }

    async fn put_chunk(
        &self,
        peer: SocketAddr,
        digest: &BlobDigest,
        chunk: &[u8],
        offset: u64,
        total: u64,
    ) -> Result<(), RpcError> {
        let path = format!(
            "{BLOB_PATH_PREFIX}{}?offset={offset}&total={total}",
            digest.as_str()
        );
        let deadline = replication_deadline(self.client.request_timeout(), chunk.len());
        self.client
            .call_once(peer, "PUT", &path, chunk.to_vec(), deadline)
            .await?;
        Ok(())
    }
}
