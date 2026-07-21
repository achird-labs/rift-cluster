//! Cluster metrics (RFC-001 §11.1).
//!
//! Two layers, deliberately:
//!
//! * The transport counters are process-local atomics with a [`snapshot`]
//!   reader, so the transport can be unit tested without standing a registry up.
//! * The fleet gauges are registered directly into the `prometheus` crate's
//!   *global default* registry, which is what the open-source metrics server
//!   serves (`collect_metrics` is a thin wrapper over `prometheus::gather()`).
//!   Registering there is why `GET /metrics` reports them with no change to the
//!   open-source server — but it also means this crate must resolve the *same*
//!   `prometheus` version the core does, or it would populate a second registry
//!   nobody serves. The workspace pins that; see the comment on the dependency.
//!
//! The gauges are sampled rather than pushed: leadership, membership and the
//! ring epoch are derived from Raft metrics that change without anything in this
//! crate being called, so the composition polls [`observe_node`] on a timer.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use lazy_static::lazy_static;
use prometheus::{Gauge, GaugeVec, register_gauge, register_gauge_vec};

use crate::raft::{Ring, StatusReport};

lazy_static! {
    /// `rift_cluster_members{state}` — this node's view of the fleet.
    ///
    /// `voter` is the size of the effective voter set; `leader` is 1 on the node
    /// that currently holds leadership and 0 elsewhere, so summing it across a
    /// fleet answers "is there exactly one leader?".
    static ref MEMBERS: GaugeVec = register_gauge_vec!(
        "rift_cluster_members",
        "Cluster members as seen by this node, by state",
        &["state"]
    )
    .expect("rift_cluster_members registers once");

    /// `rift_cluster_ring_epoch` — the membership log index the ownership ring
    /// was derived from. Two nodes reporting different epochs have not converged.
    static ref RING_EPOCH: Gauge = register_gauge!(
        "rift_cluster_ring_epoch",
        "Membership log index the ownership ring is derived from"
    )
    .expect("rift_cluster_ring_epoch registers once");

    /// `rift_cluster_insecure` — 1 when the cluster port runs unauthenticated.
    /// Exists so a fleet can be audited for it rather than trusting deploy
    /// hygiene.
    static ref INSECURE: Gauge = register_gauge!(
        "rift_cluster_insecure",
        "1 when this node's cluster port runs without authentication"
    )
    .expect("rift_cluster_insecure registers once");
}

/// Record whether this node's cluster port is unauthenticated. Set once at
/// startup, before anything binds.
pub fn set_insecure(insecure: bool) {
    INSECURE.set(f64::from(u8::from(insecure)));
}

/// Publish a sample of the node's Raft-derived state.
///
/// Called on a timer by the composition: nothing in this crate is notified when
/// leadership or membership changes, so a sampled gauge is the honest shape —
/// an event-driven one would silently go stale.
pub fn observe_node(status: &StatusReport, ring: &Ring) {
    // f64 represents integers exactly to 2^53; a voter count or a membership
    // log index reaching that is not a reading worth preserving precisely.
    MEMBERS
        .with_label_values(&["voter"])
        .set(status.voters.len() as f64);
    MEMBERS
        .with_label_values(&["leader"])
        .set(f64::from(u8::from(status.is_leader)));
    RING_EPOCH.set(ring.m_idx() as f64);
}

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
    use std::sync::Mutex;

    /// Read a gauge back out of the *global* registry the metrics server serves,
    /// rather than off the handle — that round trip is the thing under test, and
    /// it is what breaks if this crate ever links a second `prometheus`.
    fn gauge_from_registry(name: &str, label: Option<(&str, &str)>) -> Option<f64> {
        prometheus::gather()
            .into_iter()
            .find(|family| family.get_name() == name)?
            .get_metric()
            .iter()
            .find(|metric| match label {
                None => true,
                Some((key, value)) => metric
                    .get_label()
                    .iter()
                    .any(|l| l.get_name() == key && l.get_value() == value),
            })
            .map(|metric| metric.get_gauge().get_value())
    }

    // The counters are process-global, so tests that read a baseline, mutate,
    // and assert an exact delta race each other under `cargo test`'s default
    // parallelism (one test's increment lands inside another's before/after
    // window). Serialize exactly those tests through this lock; the recover
    // ignores poisoning so one panicking test doesn't cascade into failures in
    // the rest.
    static COUNTER_LOCK: Mutex<()> = Mutex::new(());

    fn counter_guard() -> std::sync::MutexGuard<'static, ()> {
        COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

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
        let _guard = counter_guard();
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
        let _guard = counter_guard();
        let before = failures_for(&snapshot(), "handler");
        rpc_failure("something-nobody-declared");
        assert_eq!(failures_for(&snapshot(), "handler"), before + 1);
    }

    #[test]
    fn handler_errors_do_not_contaminate_the_shed_signal() {
        // `shed` is the capacity signal an operator sizes the fleet on, so a
        // downstream handler fault must never inflate it.
        use crate::rpc::RpcError;
        let _guard = counter_guard();
        let before = failures_for(&snapshot(), "shed");
        rpc_failure(RpcError::Handler("downstream blew up".into()).reason());
        rpc_failure(RpcError::BodyTooLarge { limit: 1 }.reason());
        assert_eq!(failures_for(&snapshot(), "shed"), before);
    }

    #[test]
    fn fleet_gauges_reach_the_registry_the_metrics_server_serves() {
        let _guard = counter_guard();
        let status = StatusReport {
            node_id: 7,
            is_leader: true,
            current_leader: Some(7),
            last_applied: Some(42),
            voters: vec![7, 8, 9],
        };
        observe_node(&status, &Ring::new([7, 8, 9], 31));

        assert_eq!(
            gauge_from_registry("rift_cluster_members", Some(("state", "voter"))),
            Some(3.0)
        );
        assert_eq!(
            gauge_from_registry("rift_cluster_members", Some(("state", "leader"))),
            Some(1.0)
        );
        assert_eq!(
            gauge_from_registry("rift_cluster_ring_epoch", None),
            Some(31.0)
        );
    }

    #[test]
    fn a_follower_reports_no_leadership() {
        let _guard = counter_guard();
        let status = StatusReport {
            node_id: 8,
            is_leader: false,
            current_leader: Some(7),
            last_applied: Some(42),
            voters: vec![7, 8],
        };
        observe_node(&status, &Ring::new([7, 8], 12));
        // Summing this label across a fleet is how an operator asks "is there
        // exactly one leader?", so a follower must publish 0, not omit it.
        assert_eq!(
            gauge_from_registry("rift_cluster_members", Some(("state", "leader"))),
            Some(0.0)
        );
    }

    #[test]
    fn insecure_is_auditable_in_both_directions() {
        let _guard = counter_guard();
        set_insecure(true);
        assert_eq!(
            gauge_from_registry("rift_cluster_insecure", None),
            Some(1.0)
        );
        set_insecure(false);
        assert_eq!(
            gauge_from_registry("rift_cluster_insecure", None),
            Some(0.0)
        );
    }
}
