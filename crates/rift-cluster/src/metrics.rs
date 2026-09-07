//! Cluster metric families that remain as **correctness instrumentation** (D-71, #548).
//!
//! This module is not an operator surface. The operator metrics product — the
//! `rift_cluster_*` fleet gauges, the Grafana dashboards, the recording and alert rules,
//! the compose overlay and the CI lanes that checked them — was retired by D-71
//! (RFC-007 §3.2; RFC-001 §11.1 carries the callout). What stayed is exactly the set of
//! families a test reads to pin a core claim, because a *count of things that happened*
//! has no state equivalent: nothing on `/_fleet/members` says how many snapshots a node
//! installed, how many duplicate ops were collapsed, or how many cursor decisions paid an
//! owner hop. Membership, leadership and bind state are read from `/_fleet/members`
//! instead — a list and an agreed leader id are stronger facts than two gauges.
//!
//! The families, and what reads them:
//!
//! | Family | Read by |
//! |---|---|
//! | `rift_cluster_config_revision{port}` | C7 and `wait_revisions_agree` — has *this* node applied *this* config |
//! | `rift_cluster_intents_pending` | C4, C6 — the R4 ledger drains after a heal |
//! | `rift_cluster_dedup_hits_total` | C4 — the replayed duplicate was collapsed, not re-applied |
//! | `rift_cluster_snapshots_installed_total` | C26 — the snapshot wire path ran (otherwise unobservable, #183) |
//! | `rift_cluster_pull_on_miss_retries_total` | C16 — the lagging-follower net re-matched |
//! | `rift_cluster_flow_wal_lag_ops` | flow-shard tests — the `async` loss window, measured |
//! | `rift_cluster_flow_replay_entries_total` | C15 — flow state came back from disk |
//! | `rift_cluster_flow_reads_total{path}` | `flow_store.rs` — the D-13 RPC budget per read path |
//! | `rift_cluster_cas_conflicts_total{reason}` | `flow_store.rs` — fencing and isolation refusals are counted, never dropped |
//! | `rift_cluster_proxy_claims_total{outcome}` | `proxy_claims.rs`, C10 — proxyOnce claims by outcome (D-66) |
//! | `rift_cluster_proxy_recordings_total` | `proxy_claims.rs` — recordings committed to consensus |
//! | `rift_cluster_sequence_fallbacks_total` | C33 — owner identified by killing it ("the assertion, not the index") |
//! | `rift_cluster_sequence_decisions_total{op,path}` | `sequencer.rs` — the D-63 RPC budget: one `next` per decision, never a `peek` |
//!
//! Families owned by surfaces that leave with other children of #544 (audit export, the
//! source scheduler and blob store, `no_principals`, the journal's partial-read count) stay
//! here until those children land and go with them.
//!
//! They ride upstream's `/metrics` because that is where the tests already read them: the
//! families are registered into the `prometheus` crate's *global default* registry, which the
//! open-source metrics server serves (`collect_metrics` is a thin wrapper over
//! `prometheus::gather()`). That works only while this crate resolves the *same*
//! `prometheus` version the core does, or it would populate a second registry nobody
//! serves; the workspace pins that (see the comment on the dependency and
//! `scripts/check-single-prometheus.sh`).

use lazy_static::lazy_static;
use prometheus::{
    Gauge, GaugeVec, Histogram, IntCounter, IntCounterVec, IntGauge, register_gauge,
    register_gauge_vec, register_histogram, register_int_counter, register_int_counter_vec,
    register_int_gauge,
};

lazy_static! {
    /// `rift_cluster_no_principals` — 1 when the fleet has no principal
    /// defined at all (RFC-002 §3.4, issue #161). This is what makes the
    /// pre-#161 open-admin-plane bypass (no `--api-key`, no principals)
    /// visible on `/metrics` instead of a silent property of an upgraded
    /// fleet.
    static ref NO_PRINCIPALS: Gauge = register_gauge!(
        "rift_cluster_no_principals",
        "1 when the fleet has no principal defined at all"
    )
    .expect("rift_cluster_no_principals registers once");

    // -- sideloaded blobs (#439, D-48) ---------------------------------------

    /// `rift_cluster_blob_fetch_stalled` — `1` while this node's apply is parked on a
    /// sideloaded blob no member can supply; `0` otherwise. The metric form of
    /// `/_cluster/health`'s `blob_fetch_stall`. Degraded, not not-ready: the node stays in
    /// the load balancer, and every committed write behind the parked entry is unapplied
    /// on it until a holder returns.
    static ref BLOB_FETCH_STALLED: IntGauge = register_int_gauge!(
        "rift_cluster_blob_fetch_stalled",
        "1 while apply is parked on a blob no member can supply"
    )
    .expect("rift_cluster_blob_fetch_stalled registers once");

    /// `rift_cluster_blob_fetch_stalls_total` — one per stall onset. Rising on a fleet with
    /// no partition says a blob is being reaped before its op commits — the #438 pin failing.
    static ref BLOB_FETCH_STALLS: IntCounter = register_int_counter!(
        "rift_cluster_blob_fetch_stalls_total",
        "Blob fetches that went unsatisfied past the escalation window"
    )
    .expect("rift_cluster_blob_fetch_stalls_total registers once");

    /// `rift_cluster_blob_gc_retained` — tombstoned blobs this node's most recent GC sweep kept
    /// because its own log has not yet been purged past the index that unreferenced them (#480).
    /// A gauge, resampled every sweep like `rift_cluster_intents_pending`: "how many right now"
    /// is the useful reading, and the count falls back to 0 on its own once compaction catches
    /// up — nothing here needs to be reset by hand.
    static ref BLOB_GC_RETAINED: IntGauge = register_int_gauge!(
        "rift_cluster_blob_gc_retained",
        "Tombstoned blobs kept because this node's log has not purged past their unreferencing index"
    )
    .expect("rift_cluster_blob_gc_retained registers once");

    /// `rift_cluster_blob_sideload_deferred_total{reason}` — writes that kept their full bytes
    /// on the log because `fan_out_blob` could not confirm every member's sideload capability
    /// (#481). `member_incapable` = a member is confirmed to run a build that cannot apply a
    /// digest-only `ControlOp` (an explicit `false` `?stat` answer, or no blob route at all);
    /// `member_unobserved` = a member has simply never answered the question (typically a
    /// fresh join, or a transient probe failure). Persistently non-zero on a fleet that is
    /// *not* mid-rolling-upgrade means a member is stuck on an old build.
    static ref BLOB_SIDELOAD_DEFERRED: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_blob_sideload_deferred_total",
        "Writes whose bytes stayed on the log because the fan-out could not confirm every \
         member's sideload capability, by reason",
        &["reason"]
    )
    .expect("rift_cluster_blob_sideload_deferred_total registers once");

    // -- config-sync (issue #9) ---------------------------------------------

    /// `rift_cluster_intents_pending` — the R4 ledger's current depth, resampled by
    /// every replay sweep so a restart's carried-over ledger reads true. C4 asserts it
    /// rises while a minority is partitioned and C6 asserts it drains to zero after the
    /// toxics clear.
    static ref INTENTS_PENDING: Gauge = register_gauge!(
        "rift_cluster_intents_pending",
        "Parked intents currently awaiting replay on this node"
    )
    .expect("rift_cluster_intents_pending registers once");

    /// `rift_cluster_dedup_hits_total` — replayed ops the state machine
    /// collapsed to their original response instead of re-applying. C4's proof
    /// that a parked write replayed after a heal was collapsed, not applied twice.
    static ref DEDUP_HITS: IntCounter = register_int_counter!(
        "rift_cluster_dedup_hits_total",
        "Replayed ops collapsed by op-id dedup"
    )
    .expect("rift_cluster_dedup_hits_total registers once");

    /// `rift_cluster_snapshots_installed_total` — snapshots this node received from a peer and
    /// applied, i.e. times it was caught up over the wire rather than by log replication.
    ///
    /// Exists because it was otherwise **unobservable** (issue #183): nothing else
    /// distinguished "this node caught up by snapshot install" from "by replication", so a
    /// chaos scenario could only assert the *precondition* for a snapshot install and would stay
    /// green if a regression quietly restored catch-up-by-log — silently evaporating the mutant
    /// coverage the scenario exists to provide.
    static ref SNAPSHOTS_INSTALLED: IntCounter = register_int_counter!(
        "rift_cluster_snapshots_installed_total",
        "Snapshots received from a peer and applied to this node's state machine"
    )
    .expect("rift_cluster_snapshots_installed_total registers once");

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

    /// `rift_cluster_pull_on_miss_retries_total` — requests sent back through
    /// the matcher once by the lagging-follower net (#49). C16 reads it to prove
    /// the net fired. There is deliberately no `rescues_total`: the hook cannot
    /// observe the retry's outcome, so a rescue counter would be a guess.
    /// Rescue evidence is the `rift-cluster-pull-on-miss` response header.
    static ref PULL_ON_MISS_RETRIES: IntCounter = register_int_counter!(
        "rift_cluster_pull_on_miss_retries_total",
        "No-match requests re-matched after a pull-on-miss catch-up wait"
    )
    .expect("rift_cluster_pull_on_miss_retries_total registers once");

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
    /// signal that durability is not working; C15 reads it to prove the state
    /// came back from disk and not from a peer.
    static ref FLOW_REPLAY_ENTRIES: IntCounter = register_int_counter!(
        "rift_cluster_flow_replay_entries_total",
        "Flow-state entries replayed from disk at startup"
    )
    .expect("rift_cluster_flow_replay_entries_total registers once");

    /// `rift_cluster_flow_reads_total{path}` — where each flow-state read was
    /// answered. `owner` = this node owns the key and served from its shard;
    /// `forward` = one RPC to the owner (the whole cost of `strong` on a
    /// non-owner, so `forward / (owner+forward)` is the fraction of strong
    /// reads that paid a network hop); `local` = a replica read an imposter
    /// opted into with `readConsistency: "local"`. `flow_store.rs` pins the
    /// D-13 RPC budget on it.
    static ref FLOW_READS: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_flow_reads_total",
        "Flow-state reads by answering path",
        &["path"]
    )
    .expect("rift_cluster_flow_reads_total registers once");

    /// `rift_cluster_proxy_claims_total{outcome}` — proxyOnce claim answers as the data
    /// plane saw them (#226). `granted` = this request won the right to record;
    /// `inflight` = a concurrent winner exists (this request proxies without recording);
    /// `already_recorded` = replay; `refused` = the cluster could not serialize the claim
    /// (isolated, unreachable owner, not ready, unsettled ring) so the request was answered
    /// `503` and **not** forwarded (D-66). A rising `refused` is the proxyOnce reading of
    /// the isolated-owner condition `/_cluster/health` reports: requests are being
    /// correctly failed rather than silently duplicated at the upstream.
    static ref PROXY_CLAIMS: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_proxy_claims_total",
        "proxyOnce claim outcomes, as answered to this node's data plane",
        &["outcome"]
    )
    .expect("rift_cluster_proxy_claims_total registers once");

    /// `rift_cluster_proxy_recordings_total` — recordings this node successfully
    /// committed to the fleet (proxyOnce completions and proxyAlways merges).
    static ref PROXY_RECORDINGS: IntCounter = register_int_counter!(
        "rift_cluster_proxy_recordings_total",
        "Proxy recordings committed to consensus by this node"
    )
    .expect("rift_cluster_proxy_recordings_total registers once");

    /// `rift_cluster_sequence_fallbacks_total` — `owner`-mode cursor decisions
    /// served from this node's own cursor because the fleet could not answer
    /// (D-47). Not an error counter: D-10 makes this the one stateful op that
    /// degrades rather than fails, so this is what makes the degradation
    /// visible at all. C33 identifies the cursor's owner by killing it and
    /// watching this move on the survivors — the assertion, not the index.
    static ref SEQUENCE_FALLBACKS: IntCounter = register_int_counter!(
        "rift_cluster_sequence_fallbacks_total",
        "Owner-mode cursor decisions served locally because the owner could not answer"
    )
    .expect("rift_cluster_sequence_fallbacks_total registers once");

    /// `rift_cluster_sequence_decisions_total{op,path}` — every cursor decision,
    /// by operation and by what answered it. Mirrors
    /// `rift_cluster_flow_reads_total{path}` deliberately: `owner` = this node
    /// owns the cursor and served from memory; `forward` = one RPC to the owner
    /// (the whole cost of `owner` mode on a non-owner); `local` = the imposter
    /// never opted in, so this is the D-10 default path; `fallback` = the fleet
    /// could not answer and the node cycled its own cursor, which is also
    /// counted by `rift_cluster_sequence_fallbacks_total` above.
    ///
    /// `op` is `next` or `peek`. The split is what makes the RPC budget
    /// falsifiable rather than asserted (D-63): the serving path issues exactly
    /// one `next` per decision and never peeks, so `op="peek"` moving at all
    /// means something other than the debug preview reached the sequencer.
    static ref SEQUENCE_DECISIONS: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_sequence_decisions_total",
        "Cursor decisions by operation and answering path",
        &["op", "path"]
    )
    .expect("rift_cluster_sequence_decisions_total registers once");

    /// `rift_cluster_cas_conflicts_total{reason}` — owner-side refusals of
    /// a flow write. `cas` = compare-and-set lost to the current value;
    /// `fence` = the op carried a stale membership index (`m_idx`) and was
    /// rejected per RFC-001 §7.6 rather than applied under an ownership the
    /// sender no longer holds; `misroute` = the op reached a node that does
    /// not own the flow at the shared `m_idx`, which only a buggy member does
    /// — persistently non-zero means a peer's ring disagrees with its own
    /// membership index, which is a bug to file, not noise; `isolated` = the
    /// owner could not see a quorum and refused rather than mutate state a
    /// healed majority may already have re-homed (D-17). Owner-side **write**
    /// refusals only; read-side refusals are deliberately not counted here.
    static ref FLOW_CAS_CONFLICTS: IntCounterVec = register_int_counter_vec!(
        "rift_cluster_cas_conflicts_total",
        "Owner-side flow-write refusals, by reason",
        &["reason"]
    )
    .expect("rift_cluster_cas_conflicts_total registers once");

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
    /// each applied config. Two nodes disagreeing here have not converged; C7
    /// reads it per port to ask whether a joining node has applied *this*
    /// config, which no fleet-level index answers.
    static ref CONFIG_REVISION: GaugeVec = register_gauge_vec!(
        "rift_cluster_config_revision",
        "Applied config revision (log index) by imposter port",
        &["port"]
    )
    .expect("rift_cluster_config_revision registers once");

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
}

pub(crate) fn journal_partial_read() {
    JOURNAL_PARTIAL_READS.inc();
}

/// A write was durably parked on this node; the pending depth rises until the
/// replay sweep retires it (or resamples the truth — see
/// [`intents_pending_sampled`]).
pub(crate) fn intent_parked() {
    INTENTS_PENDING.inc();
}

pub(crate) fn intent_unparked() {
    INTENTS_PENDING.dec();
}

/// A blob fetch crossed the escalation window (#439, D-48). Counted once per stall.
pub(crate) fn blob_fetch_stalled() {
    BLOB_FETCH_STALLED.set(1);
    BLOB_FETCH_STALLS.inc();
}

/// The stalled fetch was satisfied; apply resumes.
pub(crate) fn blob_fetch_recovered() {
    BLOB_FETCH_STALLED.set(0);
}

/// Resample the tombstoned-but-not-yet-purged count from the blob GC sweep that just ran (#480).
pub(crate) fn blob_gc_retained(kept: u64) {
    BLOB_GC_RETAINED.set(i64::try_from(kept).unwrap_or(i64::MAX));
}

/// `reason` ∈ `member_incapable` / `member_unobserved` — closed at the call site
/// (`admin_front::fan_out_then_submit`, which is why this is `pub` rather than `pub(crate)`:
/// that call site lives in the `rift-cluster-server` crate, not this one).
pub fn blob_sideload_deferred(reason: &str) {
    BLOB_SIDELOAD_DEFERRED.with_label_values(&[reason]).inc();
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

/// `path` is one of `owner` / `forward` / `local` — a closed set at the call
/// sites, so an unexpected label cannot explode cardinality.
pub(crate) fn flow_read(path: &str) {
    FLOW_READS.with_label_values(&[path]).inc();
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

/// An `owner`-mode cursor decision the fleet could not answer, served from this
/// node's own cursor instead (D-47).
///
/// The one place a cluster failure is deliberately *not* an error (D-10:
/// sequencing is where availability wins), so this counter is the only signal
/// that a response was cycled locally rather than fleet-wide. Persistently
/// non-zero means owners are unreachable, not that sequencing is off.
pub(crate) fn sequence_fallback() {
    SEQUENCE_FALLBACKS.inc();
}

/// One cursor decision, by operation (`next` / `peek`) and answering path
/// (`owner` / `forward` / `local` / `fallback`) — both closed at the call site
/// by [`crate::stores::sequencer`]'s own enums, so neither label is free text.
///
/// Pins the RPC budget RFC-001 §11.3 used to only assert (D-63): summed over
/// `path`, `op="next"` equals the number of decisions, and `op="peek"` never
/// moves on the serving path.
pub(crate) fn sequence_decision(op: &str, path: &str) {
    SEQUENCE_DECISIONS.with_label_values(&[op, path]).inc();
}

/// `outcome` ∈ `granted` / `inflight` / `already_recorded` / `refused` — closed at the
/// call sites.
pub(crate) fn proxy_claim(outcome: &str) {
    PROXY_CLAIMS.with_label_values(&[outcome]).inc();
}

pub(crate) fn proxy_recording() {
    PROXY_RECORDINGS.inc();
}

pub(crate) fn pull_on_miss_retry() {
    PULL_ON_MISS_RETRIES.inc();
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

/// Record whether the fleet has no principal defined at all (issue #161).
/// Not a startup-only fact — a `PrincipalPut` can change it at any moment the
/// fleet is running — so the composition samples it on a timer, not once.
pub fn set_no_principals(no_principals: bool) {
    NO_PRINCIPALS.set(f64::from(u8::from(no_principals)));
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

    #[test]
    fn no_principals_is_auditable_in_both_directions() {
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

    /// Every family the chaos tier and the in-process tests read reaches the
    /// global registry (the one the core `/metrics` endpoint serves), and the
    /// resampled gauges converge to the sampled truth rather than accumulating
    /// drift. This is the list the module doc promises; a family dropped from
    /// the registry would leave its reader asserting on an absent series.
    #[test]
    fn test_read_families_reach_the_registry() {
        intent_parked();
        intent_unparked();
        intents_pending_sampled(3);
        dedup_hit();
        snapshot_installed();
        pull_on_miss_retry();
        flow_wal_lag(4);
        flow_replayed(10);
        flow_read("owner");
        flow_conflict("cas");
        proxy_claim("granted");
        proxy_recording();
        sequence_fallback();
        sequence_decision("next", "owner");
        config_applied(8080, 7);

        let families: std::collections::HashSet<String> = prometheus::gather()
            .into_iter()
            .map(|f| f.get_name().to_owned())
            .collect();
        for name in [
            "rift_cluster_config_revision",
            "rift_cluster_intents_pending",
            "rift_cluster_dedup_hits_total",
            "rift_cluster_snapshots_installed_total",
            "rift_cluster_pull_on_miss_retries_total",
            "rift_cluster_flow_wal_lag_ops",
            "rift_cluster_flow_replay_entries_total",
            "rift_cluster_flow_reads_total",
            "rift_cluster_cas_conflicts_total",
            "rift_cluster_proxy_claims_total",
            "rift_cluster_proxy_recordings_total",
            "rift_cluster_sequence_fallbacks_total",
            "rift_cluster_sequence_decisions_total",
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

        // A removed config drops its revision label.
        config_removed(8080);
        assert_eq!(
            gauge_from_registry("rift_cluster_config_revision", Some(("port", "8080"))),
            None
        );
    }
}
