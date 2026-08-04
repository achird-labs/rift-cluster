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
use prometheus::{
    Gauge, GaugeVec, Histogram, IntCounter, IntCounterVec, register_gauge, register_gauge_vec,
    register_histogram, register_int_counter, register_int_counter_vec,
};

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

    /// `rift_cluster_no_principals` — 1 when the fleet has no principal
    /// defined at all (RFC-002 §3.4, issue #161). This is what makes the
    /// pre-#161 open-admin-plane bypass (no `--api-key`, no principals)
    /// visible on `/metrics` instead of a silent property of an upgraded
    /// fleet — the same audit-over-trust reasoning as `rift_cluster_insecure`.
    static ref NO_PRINCIPALS: Gauge = register_gauge!(
        "rift_cluster_no_principals",
        "1 when the fleet has no principal defined at all"
    )
    .expect("rift_cluster_no_principals registers once");

    // -- config-sync (issue #9) ---------------------------------------------

    /// `rift_cluster_write_forwards_total` — writes this node accepted and
    /// handed to the leader (one per hop, so a chased leadership counts twice).
    static ref WRITE_FORWARDS: IntCounter = register_int_counter!(
        "rift_cluster_write_forwards_total",
        "Admin writes forwarded from this node toward the leader"
    )
    .expect("rift_cluster_write_forwards_total registers once");

    /// `rift_cluster_barrier_waits_total` — write barriers run.
    static ref BARRIER_WAITS: IntCounter = register_int_counter!(
        "rift_cluster_barrier_waits_total",
        "Read-after-write barriers run on this node"
    )
    .expect("rift_cluster_barrier_waits_total registers once");

    /// `rift_cluster_barrier_timeouts_total` — barriers that gave up with
    /// unapplied nodes (the write still answered 2xx, with a warning header).
    static ref BARRIER_TIMEOUTS: IntCounter = register_int_counter!(
        "rift_cluster_barrier_timeouts_total",
        "Barriers that timed out with at least one unapplied node"
    )
    .expect("rift_cluster_barrier_timeouts_total registers once");

    /// `rift_cluster_intents_parked_total` / `_replayed_total` — the R4 ledger's
    /// traffic; `rift_cluster_intents_pending` — its current depth, resampled by
    /// every replay sweep so a restart's carried-over ledger reads true.
    static ref INTENTS_PARKED: IntCounter = register_int_counter!(
        "rift_cluster_intents_parked_total",
        "Admin intents durably parked on this node"
    )
    .expect("rift_cluster_intents_parked_total registers once");
    static ref INTENTS_REPLAYED: IntCounter = register_int_counter!(
        "rift_cluster_intents_replayed_total",
        "Parked intents replayed to completion by this node"
    )
    .expect("rift_cluster_intents_replayed_total registers once");
    static ref INTENTS_PENDING: Gauge = register_gauge!(
        "rift_cluster_intents_pending",
        "Parked intents currently awaiting replay on this node"
    )
    .expect("rift_cluster_intents_pending registers once");

    /// `rift_cluster_dedup_hits_total` — replayed ops the state machine
    /// collapsed to their original response instead of re-applying.
    static ref DEDUP_HITS: IntCounter = register_int_counter!(
        "rift_cluster_dedup_hits_total",
        "Replayed ops collapsed by op-id dedup"
    )
    .expect("rift_cluster_dedup_hits_total registers once");

    /// `rift_cluster_snapshots_installed_total` — snapshots this node received from a peer and
    /// applied, i.e. times it was caught up over the wire rather than by log replication.
    ///
    /// Exists because it was otherwise **unobservable** (issue #183): nothing on the operator
    /// surface distinguished "this node caught up by snapshot install" from "by replication", so a
    /// chaos scenario could only assert the *precondition* for a snapshot install and would stay
    /// green if a regression quietly restored catch-up-by-log — silently evaporating the mutant
    /// coverage the scenario exists to provide.
    static ref SNAPSHOTS_INSTALLED: IntCounter = register_int_counter!(
        "rift_cluster_snapshots_installed_total",
        "Snapshots received from a peer and applied to this node's state machine"
    )
    .expect("rift_cluster_snapshots_installed_total registers once");

    /// `rift_cluster_audit_export_shipped_total` — audit rows accepted by the
    /// sink (issue #164). At-least-once, so this can exceed the number of rows
    /// derived: a leader that dies between shipping and checkpointing re-ships
    /// its last batch, and that is by design rather than a defect.
    static ref AUDIT_EXPORT_SHIPPED: IntCounter = register_int_counter!(
        "rift_cluster_audit_export_shipped_total",
        "Audit rows accepted by the export sink (at-least-once, so re-ships are counted)"
    )
    .expect("rift_cluster_audit_export_shipped_total registers once");

    /// `rift_cluster_audit_export_failures_total` — failed ship attempts.
    /// Rising while `..._shipped_total` is flat is the signal that a sink is
    /// down; the write path is unaffected either way, which is the whole point
    /// of exporting from a background task.
    static ref AUDIT_EXPORT_FAILURES: IntCounter = register_int_counter!(
        "rift_cluster_audit_export_failures_total",
        "Failed attempts to ship a batch to the export sink"
    )
    .expect("rift_cluster_audit_export_failures_total registers once");

    /// `rift_cluster_audit_export_skipped_revisions_total` — the revision span
    /// the exporter found already GC'd when it came back to it (issue #164).
    ///
    /// A **revision** span, not a row count, and the name says so deliberately:
    /// once retention has removed the rows there is nothing left to count, and
    /// revisions are not one-to-one with audit rows (a deduped replay and a
    /// non-audited op each consume a revision without producing one). So this
    /// is an upper bound on what was lost. An upper bound that is named as one
    /// is useful; a precise-looking number that is actually a guess is not.
    static ref AUDIT_EXPORT_SKIPPED: IntCounter = register_int_counter!(
        "rift_cluster_audit_export_skipped_revisions_total",
        "Revision span that aged past audit retention before the exporter shipped it \
         (an upper bound on rows lost)"
    )
    .expect("rift_cluster_audit_export_skipped_revisions_total registers once");

    /// `rift_cluster_audit_export_lag_revisions` — last applied revision minus
    /// the export checkpoint, sampled on the leader. Zero on a follower, which
    /// is correct rather than missing: a follower is not behind on exporting,
    /// it is not exporting.
    static ref AUDIT_EXPORT_LAG: Gauge = register_gauge!(
        "rift_cluster_audit_export_lag_revisions",
        "Revisions between the last applied entry and the audit export checkpoint (leader only)"
    )
    .expect("rift_cluster_audit_export_lag_revisions registers once");

    /// `rift_cluster_source_scheduler_corrupt_rows` — source rows the leader's
    /// poll scheduler could not decode on its last reconcile.
    ///
    /// Deliberately unlabelled. Which row is corrupt is a question for the
    /// transition log, which names tenant and id; a metric label would put
    /// operator-chosen ids into the cardinality budget for a value that is
    /// almost always zero. Nonzero is the alert: each of those sources is held
    /// at whatever cadence it was last started with, and cannot adopt a change
    /// until the record is rewritten.
    static ref SOURCE_SCHEDULER_CORRUPT_ROWS: Gauge = register_gauge!(
        "rift_cluster_source_scheduler_corrupt_rows",
        "Source rows the poll scheduler could not decode on its last reconcile (leader only)"
    )
    .expect("rift_cluster_source_scheduler_corrupt_rows registers once");

    /// `rift_cluster_source_scheduler_read_failures_total` — reconciles that
    /// could not read the source table at all.
    ///
    /// Distinct from the gauge above, and the distinction is the point: a
    /// corrupt *row* now costs only itself, but a table- or transaction-level
    /// failure still parks the whole reconcile. That residue is rare and
    /// transient, and this is what makes it alertable rather than grep-able.
    static ref SOURCE_SCHEDULER_READ_FAILURES: IntCounter = register_int_counter!(
        "rift_cluster_source_scheduler_read_failures_total",
        "Reconciles that could not read the source table"
    )
    .expect("rift_cluster_source_scheduler_read_failures_total registers once");

    /// `rift_cluster_pull_on_miss_checks_total` — no-match requests the net
    /// actually evaluated, i.e. those reaching a **non-leader** node with a
    /// bound cluster handle. Leaders are excluded on purpose: a leader cannot
    /// lag itself, so counting it would dilute the `lagging / checks` ratio
    /// that makes this family worth reading.
    static ref PULL_ON_MISS_CHECKS: IntCounter = register_int_counter!(
        "rift_cluster_pull_on_miss_checks_total",
        "No-match requests evaluated by the pull-on-miss net on a non-leader node"
    )
    .expect("rift_cluster_pull_on_miss_checks_total registers once");

    /// `rift_cluster_pull_on_miss_lagging_total` — checks that found this node
    /// behind the leader. Persistently non-zero means followers are serving
    /// while behind, which is a readiness-gate question, not a matcher one.
    static ref PULL_ON_MISS_LAGGING: IntCounter = register_int_counter!(
        "rift_cluster_pull_on_miss_lagging_total",
        "Pull-on-miss checks that found this node behind the leader"
    )
    .expect("rift_cluster_pull_on_miss_lagging_total registers once");

    /// `rift_cluster_pull_on_miss_retries_total` — requests sent back through
    /// the matcher once. There is deliberately no `rescues_total`: the hook
    /// cannot observe the retry's outcome, so a rescue counter would be a guess.
    /// Rescue evidence is the `rift-cluster-pull-on-miss` response header.
    static ref PULL_ON_MISS_RETRIES: IntCounter = register_int_counter!(
        "rift_cluster_pull_on_miss_retries_total",
        "No-match requests re-matched after a pull-on-miss catch-up wait"
    )
    .expect("rift_cluster_pull_on_miss_retries_total registers once");

    /// `rift_requests_unmatched_total` — requests the fleet answered with a
    /// no-match, the denominator half of the match-rate the dashboards plot.
    ///
    /// **This undercounts, deliberately and by a known amount.** It is
    /// incremented where the pull-on-miss net concludes `Proceed`, which is the
    /// only point in this process that is certainly a terminal miss. The seam
    /// grants at most ONE retry and is not consulted again, so a request that
    /// was retried and missed a second time is invisible here — exactly the
    /// blind spot [`PULL_ON_MISS_RETRIES`] above refuses to guess at. Match rate
    /// derived from this therefore reads slightly HIGH.
    ///
    /// The error is measurable rather than merely acknowledged, but mind which
    /// quantity is bounded: the undercount *on this counter* is at most
    /// `rate(rift_cluster_pull_on_miss_retries_total)`, since every missed retry
    /// is a retry. The overstatement *on a match-rate ratio* is that same
    /// quantity divided by the request rate — the un-normalised retry rate is
    /// not a bound on a 0..1 ratio at all. Closing the gap needs an upstream
    /// seam that reports the retry's outcome.
    ///
    /// Named `rift_requests_*`, not `rift_cluster_*`: it counts a property of
    /// the request stream that the engine owns, and sits beside upstream's
    /// `rift_requests_total` it is divided by.
    static ref REQUESTS_UNMATCHED: IntCounter = register_int_counter!(
        "rift_requests_unmatched_total",
        "Requests answered with a no-match (undercounts by retried-then-missed requests)"
    )
    .expect("rift_requests_unmatched_total registers once");

    /// `rift_cluster_flow_fsync_seconds` — how long a durable flow-state commit
    /// took. Buckets start at 100µs (an SSD's floor) and reach 1s, because the
    /// number worth seeing is the tail: `Sync` writes wait on this, so its p99
    /// is the latency an imposter configured for zero loss actually pays.
    static ref FLOW_FSYNC: Histogram = register_histogram!(
        "rift_cluster_flow_fsync_seconds",
        "Duration of a durable (fsynced) flow-state commit",
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
    )
    .expect("rift_cluster_flow_fsync_seconds registers once");

    /// `rift_cluster_flow_wal_lag_ops` — writes acknowledged but not yet
    /// fsynced. This is exactly the `async` mode's loss window, measured rather
    /// than assumed: persistently high means the fsync ticker is not keeping up
    /// and the interval is a fiction.
    static ref FLOW_WAL_LAG: Gauge = register_gauge!(
        "rift_cluster_flow_wal_lag_ops",
        "Flow-state writes acknowledged but not yet fsynced"
    )
    .expect("rift_cluster_flow_wal_lag_ops registers once");

    /// `rift_cluster_flow_replay_entries_total` — entries read back from disk at
    /// startup. Zero after a restart that should have recovered state is the
    /// signal that durability is not working, and it is visible without waiting
    /// for a test to fail.
    static ref FLOW_REPLAY_ENTRIES: IntCounter = register_int_counter!(
        "rift_cluster_flow_replay_entries_total",
        "Flow-state entries replayed from disk at startup"
    )
    .expect("rift_cluster_flow_replay_entries_total registers once");

    /// `rift_cluster_kv_evicted_flows_total` — whole flows shed under the cap.
    /// Counted in flows, never keys: eviction has no other granularity, because
    /// a half-evicted scenario is worse than an absent one.
    static ref FLOW_EVICTED: IntCounter = register_int_counter!(
        "rift_cluster_kv_evicted_flows_total",
        "Flows shed whole when the per-node flow cap was exceeded"
    )
    .expect("rift_cluster_kv_evicted_flows_total registers once");

    /// `rift_cluster_flow_reads_total{path}` — where each flow-state read was
    /// answered. `owner` = this node owns the key and served from its shard;
    /// `forward` = one RPC to the owner (the whole cost of `strong` on a
    /// non-owner, so `forward / (owner+forward)` is the fraction of strong
    /// reads that paid a network hop); `local` = a replica read an imposter
    /// opted into with `readConsistency: "local"`.
    static ref FLOW_READS: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_flow_reads_total",
        "Flow-state reads by answering path",
        &["path"]
    )
    .expect("rift_cluster_flow_reads_total registers once");

    /// `rift_cluster_cas_conflicts_total{reason}` — owner-side refusals of
    /// a flow write. `cas` = compare-and-set lost to the current value;
    /// `fence` = the op carried a stale membership index (`m_idx`) and was
    /// rejected per RFC-001 §7.6 rather than applied under an ownership the
    /// sender no longer holds; `misroute` = the op reached a node that does
    /// not own the flow at the shared `m_idx`, which only a buggy member does
    /// — persistently non-zero means a peer's ring disagrees with its own
    /// membership index, which is a bug to file, not noise.
    static ref FLOW_CAS_CONFLICTS: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_cas_conflicts_total",
        "Owner-side flow-write refusals, by reason",
        &["reason"]
    )
    .expect("rift_cluster_cas_conflicts_total registers once");

    /// `rift_cluster_flow_adoptions_total{outcome}` — owner-side verification
    /// pulls on first touch of a flow under a new membership (#126). `found` =
    /// a fellow holder returned entries; `empty` = holders reachable, nothing
    /// held (a genuinely new flow); `unreachable` = no holder answered and the
    /// local copy was served unverified — RFC-001 §7.2.3's bounded-staleness
    /// path, which is why this label is the one worth alerting on.
    static ref FLOW_ADOPTIONS: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_flow_adoptions_total",
        "Flow takeover verification pulls, by outcome",
        &["outcome"]
    )
    .expect("rift_cluster_flow_adoptions_total registers once");

    /// `rift_cluster_flow_repairs_total` — entries the anti-entropy loop merged
    /// that superseded the local record (#126). Steady non-zero means pushes
    /// are being missed — the loop is compensating for a fault worth finding.
    static ref FLOW_REPAIRS: IntCounter = register_int_counter!(
        "rift_cluster_flow_repairs_total",
        "Anti-entropy merges that superseded the local record"
    )
    .expect("rift_cluster_flow_repairs_total registers once");

    /// `rift_cluster_source_polls_total{outcome}` — scheduled tracking-source
    /// polls the leader performed (#135), by what they did.
    ///
    /// `unchanged` should dominate a healthy fleet: it is the digest short
    /// circuit firing, which is what makes polling cost no log growth. A rising
    /// `error` rate is the signal an upstream source host is unreachable —
    /// deliberately visible here rather than as a log entry per failure, which
    /// would turn someone else's outage into fleet-wide write traffic.
    ///
    /// Only the leader increments this, so summing across the fleet counts each
    /// poll once — which is also how you catch a fleet that has grown a second
    /// poller.
    static ref SOURCE_POLLS: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_source_polls_total",
        "Scheduled tracking-source polls, by outcome",
        &["outcome"]
    )
    .expect("rift_cluster_source_polls_total registers once");

    /// `rift_cluster_source_poll_seconds` — wall-clock of a scheduled poll,
    /// fetch included. Buckets reach far past any healthy fetch because the
    /// interesting tail is an upstream host that has started hanging.
    static ref SOURCE_POLL_SECONDS: Histogram = register_histogram!(
        "rift_cluster_source_poll_seconds",
        "Duration of a scheduled tracking-source poll, fetch included",
        vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0]
    )
    .expect("rift_cluster_source_poll_seconds registers once");

    /// `rift_cluster_config_revision{port}` — the log index that last wrote
    /// each applied config. Two nodes disagreeing here have not converged.
    static ref CONFIG_REVISION: GaugeVec = register_gauge_vec!(
        "rift_cluster_config_revision",
        "Applied config revision (log index) by imposter port",
        &["port"]
    )
    .expect("rift_cluster_config_revision registers once");

    /// `rift_cluster_bind_failures{port}` — 1 while the local engine cannot
    /// realize a committed config (port 0 is the set-level slot). Resampled
    /// after every engine drive, so a healed port clears.
    static ref BIND_FAILURES: GaugeVec = register_gauge_vec!(
        "rift_cluster_bind_failures",
        "1 while a committed config cannot be realized locally, by port",
        &["port"]
    )
    .expect("rift_cluster_bind_failures registers once");

    /// `rift_cluster_journal_entries{port}` — entries this node's writer shard
    /// currently retains for a port. It is one shard of the fleet journal, not
    /// the fleet total: summing it across nodes is what approximates the whole,
    /// which is exactly what a fleet dashboard should graph.
    static ref JOURNAL_ENTRIES: GaugeVec = register_gauge_vec!(
        "rift_cluster_journal_entries",
        "Recorded requests retained in this node's journal shard, by imposter port",
        &["port"]
    )
    .expect("rift_cluster_journal_entries registers once");

    /// `rift_cluster_journal_evictions_total{reason}` — entries dropped by
    /// retention pressure. `cap` = the shard hit `max(500, 10_000/N)`;
    /// `age` = the entry outlived the age cap. Sustained `cap` evictions mean a
    /// verification read can legitimately miss entries a test expected, so this
    /// is the counter that explains an otherwise baffling assertion failure.
    static ref JOURNAL_EVICTIONS: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_journal_evictions_total",
        "Journal entries dropped by retention pressure, by reason",
        &["reason"]
    )
    .expect("rift_cluster_journal_evictions_total registers once");

    static ref JOURNAL_EVICTIONS_CAP: IntCounter =
        JOURNAL_EVICTIONS.with_label_values(&["cap"]);
    static ref JOURNAL_EVICTIONS_AGE: IntCounter =
        JOURNAL_EVICTIONS.with_label_values(&["age"]);

    /// `rift_cluster_journal_partial_reads_total` — merge-on-read answers
    /// (issue #223) the caller had to stamp `Rift-Cluster-Partial: true`
    /// because a roster peer's shard could not be pulled into the replica
    /// cache in time. Touched by the merge's caller, not by `merge_shards`
    /// itself — the merge only carries the bit its caller already decided.
    static ref JOURNAL_PARTIAL_READS: IntCounter = register_int_counter!(
        "rift_cluster_journal_partial_reads_total",
        "Fleet journal merge-on-read answers stamped partial"
    )
    .expect("rift_cluster_journal_partial_reads_total registers once");

    /// `rift_cluster_journal_merge_seconds` — wall-clock of one k-way merge
    /// across a port's cached shards (issue #223). Buckets stay well under a
    /// request deadline: the merge is an in-memory sort over however many
    /// shards this node has cached, not I/O.
    static ref JOURNAL_MERGE: Histogram = register_histogram!(
        "rift_cluster_journal_merge_seconds",
        "Duration of one fleet journal merge-on-read",
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
    )
    .expect("rift_cluster_journal_merge_seconds registers once");

    /// `rift_cluster_journal_peer_pull_failures_total{peer}` — a fleet journal RPC to `peer`
    /// (issue #223) that came back without a usable reply or confirmation, including a peer the
    /// transport's own health tracking has already given up on and one lost to a caller's budget
    /// rather than outright errored. Covers every caller that dials a peer over this journal's
    /// wire, not just `anti_entropy_tick`: the merge-on-read pull (`pull_since_budgeted`), the
    /// fleet count fan-out (`fleet_counts`) and the clear fan-out (`clear_peers`) all touch it too
    /// (review fix for B5 — before this, the one path that produces a user-visible
    /// `Rift-Cluster-Partial` produced no metric at all). `peer` is a roster node id, so
    /// cardinality tracks fleet size, never request traffic.
    static ref JOURNAL_PEER_PULL_FAILURES: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_journal_peer_pull_failures_total",
        "Anti-entropy journal pulls that failed, by peer",
        &["peer"]
    )
    .expect("rift_cluster_journal_peer_pull_failures_total registers once");
}

/// The retained-depth gauge for one port, resolved once so the recording path never
/// pays `with_label_values`' label formatting and registry hash per request.
pub(crate) fn journal_entries_gauge(port: u16) -> Gauge {
    JOURNAL_ENTRIES.with_label_values(&[&port.to_string()])
}

/// Count what one append's retention pass dropped. Called with both reasons so a
/// caller never has to decide which of them to skip.
pub(crate) fn note_journal_evictions(by_cap: usize, by_age: usize) {
    // Handles resolved once: at a steady-state-full shard every append evicts, so this
    // is a per-request path in exactly the regime where it is hottest.
    if by_cap > 0 {
        JOURNAL_EVICTIONS_CAP.inc_by(by_cap as u64);
    }
    if by_age > 0 {
        JOURNAL_EVICTIONS_AGE.inc_by(by_age as u64);
    }
}

pub(crate) fn journal_partial_read() {
    JOURNAL_PARTIAL_READS.inc();
}

pub(crate) fn journal_merge_observed(elapsed: std::time::Duration) {
    JOURNAL_MERGE.observe(elapsed.as_secs_f64());
}

/// `peer` is the roster node id as a string — a closed, fleet-sized set at
/// the call site, not request-controlled.
pub(crate) fn journal_peer_pull_failure(peer: &str) {
    JOURNAL_PEER_PULL_FAILURES.with_label_values(&[peer]).inc();
}

/// One write handed toward the leader (per hop).
pub(crate) fn write_forwarded() {
    WRITE_FORWARDS.inc();
}

/// A barrier ran; `unapplied` is how many members had not confirmed by its
/// deadline (0 = clean).
pub(crate) fn barrier_observed(unapplied: usize) {
    BARRIER_WAITS.inc();
    if unapplied > 0 {
        BARRIER_TIMEOUTS.inc();
    }
}

pub(crate) fn intent_parked() {
    INTENTS_PARKED.inc();
    INTENTS_PENDING.inc();
}

pub(crate) fn intent_unparked() {
    INTENTS_PENDING.dec();
}

pub fn intent_replayed() {
    INTENTS_REPLAYED.inc();
}

/// Resample the pending-intents depth from the ledger itself. The inc/dec pair
/// drifts across a restart (the gauge resets, the ledger persists), so every
/// replay sweep sets the truth.
pub fn intents_pending_sampled(depth: usize) {
    INTENTS_PENDING.set(depth as f64);
}

pub(crate) fn dedup_hit() {
    DEDUP_HITS.inc();
}

/// A peer's snapshot was applied to this node's state machine (issue #183).
pub(crate) fn snapshot_installed() {
    SNAPSHOTS_INSTALLED.inc();
}

pub(crate) fn audit_export_shipped(rows: usize) {
    AUDIT_EXPORT_SHIPPED.inc_by(rows as u64);
}

pub(crate) fn audit_export_failure() {
    AUDIT_EXPORT_FAILURES.inc();
}

pub(crate) fn audit_export_skipped(revisions: u64) {
    AUDIT_EXPORT_SKIPPED.inc_by(revisions);
}

pub(crate) fn audit_export_lag(revisions: u64) {
    #[expect(
        clippy::cast_precision_loss,
        reason = "a gauge is f64; a lag deep enough to lose precision is already the alarm"
    )]
    AUDIT_EXPORT_LAG.set(revisions as f64);
}

pub(crate) fn flow_fsync_observed(elapsed: std::time::Duration) {
    FLOW_FSYNC.observe(elapsed.as_secs_f64());
}

pub(crate) fn flow_wal_lag(depth: usize) {
    #[expect(
        clippy::cast_precision_loss,
        reason = "a gauge is f64; a lag deep enough to lose precision is already the alarm"
    )]
    FLOW_WAL_LAG.set(depth as f64);
}

pub(crate) fn flow_replayed(entries: usize) {
    FLOW_REPLAY_ENTRIES.inc_by(entries as u64);
}

pub(crate) fn flow_flows_evicted(flows: usize) {
    FLOW_EVICTED.inc_by(flows as u64);
}

/// `path` is one of `owner` / `forward` / `local` — a closed set at the call
/// sites, so an unexpected label cannot explode cardinality.
pub(crate) fn flow_read(path: &str) {
    FLOW_READS.with_label_values(&[path]).inc();
}

/// `outcome` ∈ `found` / `empty` / `unreachable` — closed at the call sites.
pub(crate) fn flow_adoption(outcome: &str) {
    FLOW_ADOPTIONS.with_label_values(&[outcome]).inc();
}

pub(crate) fn flow_repair() {
    FLOW_REPAIRS.inc();
}

/// Record one scheduled tracking-source poll (#135). `outcome` is
/// `applied` | `unchanged` | `skipped` | `error`.
pub(crate) fn source_poll(outcome: &str, elapsed: std::time::Duration) {
    SOURCE_POLLS.with_label_values(&[outcome]).inc();
    SOURCE_POLL_SECONDS.observe(elapsed.as_secs_f64());
}

/// Set on every reconcile, including to zero — a gauge that is only ever
/// *raised* would keep reporting a repaired row as broken.
pub(crate) fn source_scheduler_corrupt_rows(count: usize) {
    SOURCE_SCHEDULER_CORRUPT_ROWS.set(count as f64);
}

pub(crate) fn source_scheduler_read_failure() {
    SOURCE_SCHEDULER_READ_FAILURES.inc();
}

pub(crate) fn flow_conflict(reason: &str) {
    FLOW_CAS_CONFLICTS.with_label_values(&[reason]).inc();
}

pub(crate) fn pull_on_miss_check() {
    PULL_ON_MISS_CHECKS.inc();
}

pub(crate) fn pull_on_miss_lagging() {
    PULL_ON_MISS_LAGGING.inc();
}

pub(crate) fn pull_on_miss_retry() {
    PULL_ON_MISS_RETRIES.inc();
}

/// Record a terminal no-match. See [`REQUESTS_UNMATCHED`] for why a retried
/// request that missed again does not reach this.
pub(crate) fn request_unmatched() {
    REQUESTS_UNMATCHED.inc();
}

/// Counter access for tests in sibling modules.
///
/// The counters are process-global, so any test that reads a baseline, mutates,
/// and asserts a delta races every other such test under `cargo test`'s default
/// parallelism. They all have to take the *same* lock, which is why it lives
/// here rather than in one module's test scope.
#[cfg(test)]
pub(crate) mod testing {
    static COUNTER_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Recovers from poisoning so one panicking test does not cascade.
    pub(crate) fn counter_guard() -> std::sync::MutexGuard<'static, ()> {
        COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn requests_unmatched_total() -> u64 {
        super::REQUESTS_UNMATCHED.get()
    }
}

pub(crate) fn config_applied(port: u16, revision: u64) {
    CONFIG_REVISION
        .with_label_values(&[&port.to_string()])
        .set(revision as f64);
}

pub(crate) fn config_removed(port: u16) {
    // A port with no config has no revision; an error here just means the
    // label was never set.
    let _ = CONFIG_REVISION.remove_label_values(&[&port.to_string()]);
}

/// Resample the engine-drive failure map after a drive: set 1 for every
/// failing port, clearing everything else.
pub(crate) fn observe_apply_failures(failures: &std::collections::BTreeMap<u16, String>) {
    BIND_FAILURES.reset();
    for port in failures.keys() {
        BIND_FAILURES
            .with_label_values(&[&port.to_string()])
            .set(1.0);
    }
}

/// Record whether this node's cluster port is unauthenticated. Set once at
/// startup, before anything binds.
pub fn set_insecure(insecure: bool) {
    INSECURE.set(f64::from(u8::from(insecure)));
}

/// Record whether the fleet has no principal defined at all (issue #161).
/// Unlike [`set_insecure`], this is not a startup-only fact — a `PrincipalPut`
/// can change it at any moment the fleet is running — so the composition
/// samples it on the same timer as the other fleet gauges, not once.
pub fn set_no_principals(no_principals: bool) {
    NO_PRINCIPALS.set(f64::from(u8::from(no_principals)));
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

    // Tests in sibling modules touch these same process-global counters, so the
    // lock has to be shared with them rather than private to this scope.
    use super::testing::counter_guard;

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

    #[test]
    fn no_principals_is_auditable_in_both_directions() {
        let _guard = counter_guard();
        set_no_principals(true);
        assert_eq!(
            gauge_from_registry("rift_cluster_no_principals", None),
            Some(1.0)
        );
        set_no_principals(false);
        assert_eq!(
            gauge_from_registry("rift_cluster_no_principals", None),
            Some(0.0)
        );
    }

    /// Issue #9: the config-sync families reach the global registry (the one
    /// the core `/metrics` endpoint serves), and the resampled gauges converge
    /// to the sampled truth rather than accumulating drift.
    #[test]
    fn config_sync_families_reach_the_registry() {
        let _guard = counter_guard();
        write_forwarded();
        barrier_observed(0);
        barrier_observed(2);
        intent_parked();
        intent_unparked();
        intent_replayed();
        intents_pending_sampled(3);
        dedup_hit();
        pull_on_miss_check();
        pull_on_miss_lagging();
        pull_on_miss_retry();
        request_unmatched();
        flow_fsync_observed(std::time::Duration::from_micros(200));
        flow_wal_lag(4);
        flow_adoption("empty");
        flow_repair();
        flow_replayed(10);
        flow_flows_evicted(2);
        config_applied(8080, 7);
        let mut failures = std::collections::BTreeMap::new();
        failures.insert(8080_u16, "bind".to_owned());
        observe_apply_failures(&failures);

        let families: std::collections::HashSet<String> = prometheus::gather()
            .into_iter()
            .map(|f| f.get_name().to_owned())
            .collect();
        for name in [
            "rift_cluster_write_forwards_total",
            "rift_cluster_barrier_waits_total",
            "rift_cluster_barrier_timeouts_total",
            "rift_cluster_pull_on_miss_checks_total",
            "rift_cluster_pull_on_miss_lagging_total",
            "rift_cluster_pull_on_miss_retries_total",
            "rift_requests_unmatched_total",
            "rift_cluster_intents_parked_total",
            "rift_cluster_intents_replayed_total",
            "rift_cluster_intents_pending",
            "rift_cluster_dedup_hits_total",
            "rift_cluster_flow_fsync_seconds",
            "rift_cluster_flow_wal_lag_ops",
            "rift_cluster_flow_replay_entries_total",
            "rift_cluster_kv_evicted_flows_total",
            "rift_cluster_config_revision",
            "rift_cluster_bind_failures",
        ] {
            assert!(families.contains(name), "{name} missing from the registry");
        }

        assert_eq!(
            gauge_from_registry("rift_cluster_intents_pending", None),
            Some(3.0),
            "the sweep sample overrides inc/dec drift"
        );
        assert_eq!(
            gauge_from_registry("rift_cluster_config_revision", Some(("port", "8080"))),
            Some(7.0)
        );
        assert_eq!(
            gauge_from_registry("rift_cluster_bind_failures", Some(("port", "8080"))),
            Some(1.0)
        );

        // A drive with no failures clears the vector; a removed config drops
        // its revision label.
        observe_apply_failures(&std::collections::BTreeMap::new());
        assert_eq!(
            gauge_from_registry("rift_cluster_bind_failures", Some(("port", "8080"))),
            None
        );
        config_removed(8080);
        assert_eq!(
            gauge_from_registry("rift_cluster_config_revision", Some(("port", "8080"))),
            None
        );
    }
}
