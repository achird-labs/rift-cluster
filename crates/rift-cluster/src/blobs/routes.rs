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

use super::{BLOB_PATH_PREFIX, BlobDigest, BlobError, BlobStore};
use crate::rpc::{HandlerFuture, Router, RpcError};

/// Register the blob transfer routes onto `router`.
#[must_use]
pub(crate) fn blob_routes(router: Router, store: Arc<BlobStore>) -> Router {
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
                Box::pin(async move { handle_get(store, suffix, body).await })
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
    suffix: String,
    _body: Vec<u8>,
) -> Result<Vec<u8>, RpcError> {
    let (digest_str, query) = split_suffix(&suffix);
    let digest = BlobDigest::parse(digest_str).map_err(|e| map_blob_error(e, digest_str))?;

    if has_flag(query, "stat") {
        let stat = tokio::task::spawn_blocking(move || store.stat(&digest))
            .await
            .map_err(|e| RpcError::Handler(format!("blob stat task: {e}")))?
            .map_err(|e| map_blob_error(e, digest_str))?;
        return encode(&stat);
    }

    let offset = required_u64(query, "offset")?;
    let len = required_u64(query, "len")?;
    let bytes = tokio::task::spawn_blocking(move || store.read_chunk(&digest, offset, len))
        .await
        .map_err(|e| RpcError::Handler(format!("blob read task: {e}")))?
        .map_err(|e| map_blob_error(e, digest_str))?;
    Ok(bytes)
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
    use super::*;

    const DIGEST: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

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
