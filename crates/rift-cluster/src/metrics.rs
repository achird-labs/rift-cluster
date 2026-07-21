//! Cluster transport counters (RFC-001 §11.1).
//!
//! Process-local atomics with a snapshot reader. The enterprise binary maps
//! these onto the Prometheus registry the open-source metrics server already
//! serves; keeping the crate registry-free means the transport can be unit
//! tested without standing one up.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

static BRIDGE_INFLIGHT: AtomicI64 = AtomicI64::new(0);
static BRIDGE_REJECTED: AtomicU64 = AtomicU64::new(0);
static RPC_FAILURES: [AtomicU64; REASONS.len()] = [const { AtomicU64::new(0) }; REASONS.len()];

/// Failure reasons, matching `RpcError::reason` and `AuthError::reason`.
///
/// `handler` is last because it doubles as the bucket for a reason this table
/// does not know — see [`rpc_failure`].
const REASONS: [&str; 12] = [
    "malformed",
    "bad_mac",
    "stale_timestamp",
    "replayed_nonce",
    "nonce_cache_full",
    "version_skew",
    "unknown_route",
    "body_too_large",
    "timeout",
    "transport",
    "shed",
    "handler",
];

/// `rift_cluster_bridge_inflight` — callers currently parked on the bridge.
pub fn bridge_inflight_inc() {
    BRIDGE_INFLIGHT.fetch_add(1, Ordering::Relaxed);
}

pub fn bridge_inflight_dec() {
    BRIDGE_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
}

/// `rift_cluster_bridge_rejected_total` — ops shed at the permit bound.
pub fn bridge_rejected() {
    BRIDGE_REJECTED.fetch_add(1, Ordering::Relaxed);
}

/// `rift_cluster_rpc_failures_total{reason}`. An unknown reason is counted as
/// `handler` rather than dropped — a metric that silently loses events is worse
/// than one with a coarse bucket.
pub fn rpc_failure(reason: &str) {
    let index = REASONS
        .iter()
        .position(|r| *r == reason)
        .unwrap_or(REASONS.len() - 1);
    RPC_FAILURES[index].fetch_add(1, Ordering::Relaxed);
}

/// Point-in-time values, for the binary's Prometheus bridge and for tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub bridge_inflight: i64,
    pub bridge_rejected_total: u64,
    pub rpc_failures_total: Vec<(&'static str, u64)>,
}

#[must_use]
pub fn snapshot() -> Snapshot {
    Snapshot {
        bridge_inflight: BRIDGE_INFLIGHT.load(Ordering::Relaxed),
        bridge_rejected_total: BRIDGE_REJECTED.load(Ordering::Relaxed),
        rpc_failures_total: REASONS
            .iter()
            .zip(RPC_FAILURES.iter())
            .map(|(reason, count)| (*reason, count.load(Ordering::Relaxed)))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failures_for(snapshot: &Snapshot, reason: &str) -> u64 {
        snapshot
            .rpc_failures_total
            .iter()
            .find(|(r, _)| *r == reason)
            .map(|(_, c)| *c)
            .expect("reason is a known bucket")
    }

    #[test]
    fn failure_reasons_cover_every_error_variant() {
        // Every reason the transport can emit must have a bucket, or the
        // counter quietly under-reports the failure it was added to surface.
        use crate::rpc::{AuthError, PROTO_VERSION, RpcError};
        let all = [
            RpcError::Unauthorized(AuthError::Malformed),
            RpcError::Unauthorized(AuthError::BadMac),
            RpcError::Unauthorized(AuthError::StaleTimestamp),
            RpcError::Unauthorized(AuthError::ReplayedNonce),
            RpcError::Unauthorized(AuthError::NonceCacheFull),
            RpcError::VersionSkew {
                peer: None,
                ours: PROTO_VERSION,
            },
            RpcError::UnknownRoute {
                method: String::new(),
                path: String::new(),
            },
            RpcError::BodyTooLarge { limit: 1 },
            RpcError::Timeout,
            RpcError::Transport(String::new()),
            RpcError::Shed,
            RpcError::Handler(String::new()),
        ];
        for err in all {
            assert!(
                REASONS.contains(&err.reason()),
                "no bucket for {:?}",
                err.reason()
            );
        }
    }

    #[test]
    fn counters_record_and_snapshot() {
        let before = snapshot();
        bridge_rejected();
        rpc_failure("timeout");
        let after = snapshot();
        assert_eq!(
            after.bridge_rejected_total,
            before.bridge_rejected_total + 1
        );
        assert_eq!(
            failures_for(&after, "timeout"),
            failures_for(&before, "timeout") + 1
        );
    }

    #[test]
    fn unknown_reasons_land_in_the_handler_bucket_rather_than_vanishing() {
        let before = failures_for(&snapshot(), "handler");
        rpc_failure("something-nobody-declared");
        assert_eq!(failures_for(&snapshot(), "handler"), before + 1);
    }

    #[test]
    fn handler_errors_do_not_contaminate_the_shed_signal() {
        // `shed` is the capacity signal an operator sizes the fleet on, so a
        // downstream handler fault must never inflate it.
        use crate::rpc::RpcError;
        let before = failures_for(&snapshot(), "shed");
        rpc_failure(RpcError::Handler("downstream blew up".into()).reason());
        rpc_failure(RpcError::BodyTooLarge { limit: 1 }.reason());
        assert_eq!(failures_for(&snapshot(), "shed"), before);
    }
}
