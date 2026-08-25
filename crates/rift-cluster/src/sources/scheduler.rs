//! The tracking-source poll scheduler: exactly one poller fleet-wide (#135).
//!
//! A `tracking` source is one the fleet re-fetches on an interval. The whole
//! difficulty is the word *fleet*: N nodes each running a timer would fetch N
//! times per interval, and #134's entire design — one fetch, submitted once,
//! applied identically everywhere — would be undone by the thing that drives it.
//!
//! So the scheduler is **leader-only**, grounded on the same `RaftMetrics`
//! leadership watch the forward-to-leader path reads. Not a second notion of
//! leadership: two independent answers to "am I the leader" is precisely how a
//! fleet ends up with two pollers during an election, and a poller that is
//! wrong for a few hundred milliseconds is a duplicate fetch against someone
//! else's host.
//!
//! ## Why one supervisor and not one timer per source
//!
//! The supervisor owns the whole task set and reconciles it from a single
//! signal. The metrics watch fires on leadership changes *and* on ordinary
//! commit movement, so a `SourcePut` that adds a tracking source wakes the
//! supervisor without any extra plumbing into the apply path — apply stays
//! deterministic and knows nothing about schedulers.
//!
//! ## What a poll costs when nothing changed
//!
//! Nothing durable. A poll runs #134's pull flow unchanged, and the digest
//! short circuit means unchanged content writes **no log entry at all**. That
//! is the property that makes tracking mode affordable: a 30-second poll
//! against a static document grows the log by zero forever.
//!
//! ## Every tenant, one supervisor
//!
//! The supervisor polls **every** tenant's tracking sources, keyed
//! `(tenant, id)` — a source id is unique only within its tenant, so a bare-id
//! running set would poll one tenant's source and silently starve another's of
//! the same name (#241). It reconciles against one whole-table scan
//! (`RaftNode::sources_all`) rather than a tenant list: that list carries
//! tombstones and omits the implicit default tenant, and a tenant's
//! cascade-delete drops its source rows in the same committed op — so they stop
//! appearing, and the next reconcile stops their pollers with nothing to check.
//!
//! ## One tenant's corruption is not every tenant's outage
//!
//! Scanning the whole table means one tenant's unreadable row is now on the
//! path of every tenant's reconciliation, so the scan reports a decode failure
//! **per row** rather than failing outright (#243). The scheduler's response is
//! to *hold* that key: it is neither started, stopped, nor re-intervalled,
//! while every other source reconciles normally.
//!
//! **Held does not mean still working.** The task, if one was already running,
//! stays alive — but `pull` re-reads the record through the strict
//! `RaftNode::source` path, so every attempt fails until the record is
//! rewritten. The source is not being fetched; it is failing on its old
//! cadence, visibly: `lastPollError`, `source_polls_total{outcome="error"}`,
//! and a warn per interval. A source held on a *newly elected* leader has no
//! task at all, since a poller cannot be started without an interval to start
//! it on.
//!
//! Holding is still the least-wrong of the three options, but for a narrower
//! reason than "it keeps polling". Dropping the row silently stops a live
//! poller and reports nothing at all; failing the scan parks the whole fleet
//! over one tenant's bad bytes; holding keeps the failure attributable to the
//! source that caused it and lets a repair recover with no restart, because
//! `SourcePut` makes the row decode again and the ordinary interval-change path
//! adopts it. A delete removes the key from the scan and the ordinary stale
//! path stops it. The hold needs no exit of its own. It is counted in
//! `rift_cluster_source_scheduler_corrupt_rows` and logged on its edges, not on
//! every tick.
//!
//! ## Failures are visible without being written down
//!
//! A failing poll must not write a log entry per failure — that would
//! reintroduce the log growth the short circuit exists to avoid, sideways, and
//! at the worst possible time (an upstream outage is exactly when you do not
//! want fleet-wide writes). So a poll error is recorded **leader-locally**: an
//! in-memory last-error per source, surfaced on `GET /admin/sources/:id` as
//! `lastPollError`, plus a counter. The durable `last` record still only moves
//! when a pull actually applies or is skipped.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::raft::{RaftNode, SourceRow};

use super::SourcePuller;

/// How long the supervisor waits on the leadership watch before reconciling
/// anyway. The watch is the mechanism; this is the safety net that keeps a
/// missed wakeup from parking the scheduler forever.
const RECONCILE_FLOOR: Duration = Duration::from_secs(1);

/// Fraction of the poll interval used as jitter, ±. Without it, N sources
/// declared in one `--imposters` line align their fetches forever and arrive at
/// the upstream host as a burst every interval instead of a trickle.
const JITTER_FRACTION: f64 = 0.10;

/// Leader-local, non-replicated poll state: what the last attempt did, for the
/// sources this node is currently polling.
///
/// Deliberately not in the state machine (D-31: `PollStatus` is node-local,
/// `SourceRecord` is the fleet-replicated half). A poll failure is a property of one
/// node's view of an external host at one moment — replicating it would mean a
/// log entry per failure, which is the log-growth problem the digest short
/// circuit exists to prevent.
#[derive(Debug, Default)]
pub struct PollStatus {
    /// Keyed `(tenant, id)`, matching `SM_SOURCES_TABLE`: a source id is only
    /// unique within its tenant, so a bare-id key would hand tenant A's
    /// failure string — which routinely embeds A's source URI — to tenant B's
    /// viewer asking about B's same-named source (issue #239's admin front is
    /// tenant-scoped, unlike the cluster port that first grew this map).
    last_error: Mutex<BTreeMap<(String, String), String>>,
}

impl PollStatus {
    /// The last poll error for `tenant`'s `id`, if the most recent attempt on
    /// this node failed. `None` once a later attempt succeeds.
    #[must_use]
    pub fn last_error(&self, tenant: &str, id: &str) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(tenant.to_owned(), id.to_owned()))
            .cloned()
    }

    fn record_failure(&self, tenant: &str, id: &str, detail: String) {
        self.last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((tenant.to_owned(), id.to_owned()), detail);
    }

    fn record_success(&self, tenant: &str, id: &str) {
        self.last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&(tenant.to_owned(), id.to_owned()));
    }

    fn forget(&self, tenant: &str, id: &str) {
        self.record_success(tenant, id);
    }
}

/// What the supervisor believes it is currently polling: `(tenant, id)` → the
/// interval it was started with, plus the task's abort handle.
struct Running {
    poll_secs: u64,
    task: tokio::task::JoinHandle<()>,
}

/// A source's fleet-wide identity: a bare id is unique only within its tenant,
/// so it is the pair that names a poller.
type SourceKey = (String, String);

/// The leader-only poll supervisor.
pub struct SourceScheduler {
    puller: Weak<SourcePuller>,
    node: Weak<RaftNode>,
    status: Arc<PollStatus>,
}

impl SourceScheduler {
    /// Start the supervisor on `handle`'s runtime and return the status handle
    /// the admin surface reads.
    ///
    /// Runs on an existing runtime (the bridge's, in the composed server) — the
    /// #120 lesson: a subsystem that starts its own bare `Runtime` panics on
    /// drop when it is torn down from inside async context.
    ///
    /// Holds only `Weak` references. The node owns the cluster port and the
    /// redb lock; a scheduler that kept it alive would keep both past shutdown.
    /// When either upgrade fails, the supervisor exits.
    /// The returned handle **must** be aborted on shutdown, like every other
    /// node-bound background task in the composition. `Weak` handles alone are
    /// not enough: the supervisor upgrades to an `Arc<RaftNode>` for the
    /// duration of each leadership wait, so a task left running keeps the node
    /// — and therefore the cluster port and the redb lock — alive past
    /// shutdown, and the next start fails to bind or to open its own state
    /// directory.
    pub fn spawn(
        handle: &tokio::runtime::Handle,
        node: &Arc<RaftNode>,
        puller: &Arc<SourcePuller>,
    ) -> (Arc<PollStatus>, tokio::task::JoinHandle<()>) {
        let status = Arc::new(PollStatus::default());
        let scheduler = Self {
            puller: Arc::downgrade(puller),
            node: Arc::downgrade(node),
            status: Arc::clone(&status),
        };
        let task = handle.spawn(async move { scheduler.supervise().await });
        (status, task)
    }

    /// The supervisor loop: reconcile the running task set against what the
    /// applied state machine says this node should be polling.
    async fn supervise(self) {
        let mut running: BTreeMap<SourceKey, Running> = BTreeMap::new();
        // Carried across ticks so the corrupt-row log reports edges rather than
        // repeating the condition once a second for as long as it lasts.
        let mut held: BTreeSet<SourceKey> = BTreeSet::new();
        // Start pessimistic. If this node is already leader, the first
        // `await_leadership_change` returns immediately with `true`, which is
        // the reconcile that starts the tasks.
        let mut was_leader = false;

        loop {
            // The `Arc` is held only for the duration of this wait, and the
            // wait is deliberately short: while it is held, the node cannot
            // drop, and the node's `Drop` is what releases the cluster port and
            // the redb lock. `spawn`'s caller aborts this task on shutdown,
            // which drops the future — and the `Arc` with it — immediately;
            // the bounded wait is what keeps the window small even if someone
            // forgets.
            let is_leader = {
                let Some(node) = self.node.upgrade() else {
                    break;
                };
                node.await_leadership_change(was_leader, RECONCILE_FLOOR)
                    .await
            };
            was_leader = is_leader;

            if !is_leader {
                // Lost (or never had) leadership: stop everything. A follower
                // that kept polling is the duplicate-fetch bug.
                Self::stop_all(&mut running, &self.status);
                held.clear();
                continue;
            }
            if self.reconcile(&mut running, &mut held).is_none() {
                break;
            }
        }
        Self::stop_all(&mut running, &self.status);
    }

    /// Bring the running task set in line with the applied source table.
    /// `None` means the node or puller is gone and the supervisor should exit.
    ///
    /// Read, then [`plan`], then apply. The decision is a pure function so the
    /// interesting cases — above all the corrupt-row hold — are unit-testable
    /// without a node, leadership, or a redb file.
    fn reconcile(
        &self,
        running: &mut BTreeMap<SourceKey, Running>,
        held: &mut BTreeSet<SourceKey>,
    ) -> Option<()> {
        let node = self.node.upgrade()?;
        let puller = self.puller.upgrade()?;

        let rows = match node.sources_all() {
            Ok(rows) => rows,
            Err(e) => {
                // A *table*-level read failure — not a row's. It is transient
                // and says nothing about any individual source, so keep what is
                // running and retry on the next tick rather than tearing down
                // healthy pollers. Counted, because this is the one remaining
                // way reconciliation can park wholesale.
                //
                // The corrupt-row gauge is deliberately left at its last value:
                // this tick learned nothing about any row, so zeroing it would
                // report the corruption as resolved, and recomputing it is
                // impossible without the read that just failed.
                crate::metrics::source_scheduler_read_failure();
                tracing::warn!(error = %e, "source scheduler could not read the source table");
                return Some(());
            }
        };

        // The running set's intervals, so `plan` can stay free of task handles.
        // One small clone per tick over a map with an entry per tracking source
        // — cheap, and it is what keeps the decision testable in isolation.
        let current: BTreeMap<SourceKey, u64> = running
            .iter()
            .map(|(key, r)| (key.clone(), r.poll_secs))
            .collect();
        let plan = plan(&rows, &current);

        crate::metrics::source_scheduler_corrupt_rows(plan.held.len());
        Self::log_held_transitions(held, &plan.held);

        for key in plan.stop {
            if let Some(previous) = running.remove(&key) {
                previous.task.abort();
                let (tenant, id) = &key;
                self.status.forget(tenant, id);
                tracing::debug!(tenant = %tenant, source_id = %id, "stopped polling");
            }
        }

        for (key, poll_secs) in plan.start {
            let (tenant, id) = &key;
            tracing::info!(tenant = %tenant, source_id = %id, poll_secs, "polling a tracking source");
            let task = tokio::spawn(poll_loop(
                tenant.clone(),
                id.clone(),
                poll_secs,
                Arc::downgrade(&puller),
                Arc::clone(&self.status),
            ));
            // Abort whatever this key displaces. `plan` never asks to start a
            // key it did not also ask to stop, so this should always be `None`
            // — but dropping a `JoinHandle` detaches its task rather than
            // cancelling it, so an insert that silently displaced one would
            // leave a second poller fetching forever with no handle to stop it.
            // Too cheap a guard to leave to an invariant holding.
            if let Some(displaced) = running.insert(key, Running { poll_secs, task }) {
                displaced.task.abort();
            }
        }
        Some(())
    }

    /// Log the edges of the held set, never its state.
    ///
    /// A corrupt row stays corrupt until someone rewrites it, so logging the
    /// condition every tick would emit the same line once a second forever —
    /// noise that buries the moment it started and the moment it stopped, which
    /// are the two things worth reading.
    ///
    /// This is the **only** place the decode failure itself is reported to the
    /// fleet operator: `sources_all` deliberately does not log it (it runs at
    /// 1 Hz), and the strict per-tenant reads report it only to the tenant that
    /// owns the row. Dropping `detail` here would leave the operator a metric
    /// that says something is broken and no way to learn what.
    fn log_held_transitions(
        previous: &mut BTreeSet<SourceKey>,
        current: &BTreeMap<SourceKey, String>,
    ) {
        for (key, detail) in current {
            if previous.contains(key) {
                continue;
            }
            let (tenant, id) = key;
            tracing::error!(
                tenant = %tenant,
                source_id = %id,
                error = %detail,
                "source record will not decode: this source cannot be polled, started or \
                 re-intervalled until the record is rewritten. Every other source is unaffected."
            );
        }
        for (tenant, id) in previous.iter().filter(|key| !current.contains_key(*key)) {
            tracing::info!(
                tenant = %tenant,
                source_id = %id,
                "source record decodes again (repaired or deleted); it reconciles normally from now"
            );
        }
        // Unconditional, not guarded by an equality check: this set holds one
        // entry per corrupt row, so it is empty on every healthy fleet and the
        // comparison would cost more than the assignment it guards.
        *previous = current.keys().cloned().collect();
    }

    fn stop_all(running: &mut BTreeMap<SourceKey, Running>, status: &PollStatus) {
        for ((tenant, id), r) in std::mem::take(running) {
            r.task.abort();
            status.forget(&tenant, &id);
        }
        // This node is no longer the one polling, so it is no longer the one
        // observing corrupt rows. Leaving the gauge raised would report a
        // follower as holding pollers it does not have.
        crate::metrics::source_scheduler_corrupt_rows(0);
    }
}

/// What one reconcile should do, decided from the scan and the running set
/// alone — no I/O, no task handles, no clock.
#[derive(Debug)]
struct Plan {
    /// Pollers to start, with the interval to start them on.
    start: Vec<(SourceKey, u64)>,
    /// Pollers to abort: no longer wanted, or their interval changed.
    stop: Vec<SourceKey>,
    /// Rows whose stored value would not decode, each with the decode failure.
    /// Nothing is started or stopped for these; the detail is what the
    /// transition log reports, and it is the only place an operator learns
    /// *why* a row is unreadable without going to the owning tenant's own read.
    held: BTreeMap<SourceKey, String>,
}

/// Decide the reconcile.
///
/// The corrupt-row rule (#243): a row that will not decode is *held* — neither
/// started, stopped, nor re-intervalled. It cannot be started, because the
/// interval is precisely what cannot be read; and stopping it would drop the
/// only signal tied to that source, since a running poller keeps failing
/// visibly (`lastPollError` and the error counter) whereas an aborted one goes
/// quiet. Holding is not "keeps polling" — see the module doc.
///
/// Repair and deletion need no special handling. A rewritten row decodes on the
/// next tick and the ordinary interval-change path adopts it; a deleted row
/// leaves the scan entirely, so the key is neither desired nor held and the
/// ordinary stale path stops it.
fn plan(rows: &[SourceRow], running: &BTreeMap<SourceKey, u64>) -> Plan {
    let mut desired: BTreeMap<SourceKey, u64> = BTreeMap::new();
    let mut held: BTreeMap<SourceKey, String> = BTreeMap::new();

    // The key is cloned inside each arm rather than up front: this runs over
    // every source in the fleet once a second, and most rows are neither held
    // nor poll targets, so a key built eagerly would be two allocations
    // discarded on the common path.
    for row in rows {
        match &row.record {
            Ok(source) => {
                // A source that tracks without an interval cannot be polled;
                // `validate` refuses that combination, so this is
                // belt-and-braces against a record written by some other path
                // rather than an expected state. It is *readable*, so it is not
                // held — merely not a poll target.
                if matches!(source.mode, crate::control::SourceMode::Tracking)
                    && let Some(secs) = source.poll_secs
                {
                    desired.insert((row.tenant.clone(), row.id.clone()), secs);
                }
            }
            Err(detail) => {
                held.insert((row.tenant.clone(), row.id.clone()), detail.clone());
            }
        }
    }

    let stop: Vec<SourceKey> = running
        .iter()
        .filter(|(key, secs)| {
            // Held keys are exempt from both halves of the staleness test.
            !held.contains_key(*key) && desired.get(*key).is_none_or(|wanted| wanted != *secs)
        })
        .map(|(key, _)| key.clone())
        .collect();

    let start: Vec<(SourceKey, u64)> = desired
        .into_iter()
        .filter(|(key, secs)| running.get(key).is_none_or(|current| current != secs))
        .collect();

    Plan { start, stop, held }
}

/// One source's poll loop. Sleeps first, so a `SourcePut` does not double up
/// with the pull its own creation path already performed.
async fn poll_loop(
    tenant: String,
    id: String,
    poll_secs: u64,
    puller: Weak<SourcePuller>,
    status: Arc<PollStatus>,
) {
    loop {
        tokio::time::sleep(jittered(poll_secs)).await;
        let Some(puller) = puller.upgrade() else {
            break;
        };
        let started = std::time::Instant::now();
        match puller
            .pull(&tenant, &id, Some("scheduler".to_owned()))
            .await
        {
            Ok(report) => {
                status.record_success(&tenant, &id);
                crate::metrics::source_poll(
                    if report.unchanged {
                        "unchanged"
                    } else if report.skipped {
                        "skipped"
                    } else {
                        "applied"
                    },
                    started.elapsed(),
                );
            }
            Err(e) => {
                // Recorded and counted, never written to the log: an upstream
                // outage must not turn into fleet-wide write traffic. The loop
                // keeps its cadence — the next tick retries.
                let detail = e.to_string();
                tracing::warn!(tenant = %tenant, source_id = %id, error = %detail, "source poll failed");
                status.record_failure(&tenant, &id, detail);
                crate::metrics::source_poll("error", started.elapsed());
            }
        }
    }
}

/// `poll_secs` ± [`JITTER_FRACTION`], so sources declared together do not stay
/// aligned. Node-local scheduling, so a non-deterministic value is fine here —
/// nothing about it reaches the replicated log.
fn jittered(poll_secs: u64) -> Duration {
    use rand::Rng;

    let base = poll_secs as f64;
    let spread = base * JITTER_FRACTION;
    let offset = rand::thread_rng().gen_range(-spread..=spread);
    // Never below a second, however the arithmetic lands.
    Duration::from_secs_f64((base + offset).max(1.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{DEFAULT_TENANT, OnDrift, SourceMode};
    use crate::raft::{SourceRecord, SourceRow};

    fn key(tenant: &str, id: &str) -> SourceKey {
        (tenant.to_owned(), id.to_owned())
    }

    /// A readable row for a `tracking` source on `poll_secs`.
    fn tracking(tenant: &str, id: &str, poll_secs: u64) -> SourceRow {
        row(
            tenant,
            id,
            Ok(record(id, SourceMode::Tracking, Some(poll_secs))),
        )
    }

    /// A readable row for a `pinned` source — declared, never polled.
    fn pinned(tenant: &str, id: &str) -> SourceRow {
        row(tenant, id, Ok(record(id, SourceMode::Pinned, None)))
    }

    /// A row whose stored value will not decode. Its **key** is still readable,
    /// which is the whole basis of the hold behaviour.
    fn corrupt(tenant: &str, id: &str) -> SourceRow {
        row(
            tenant,
            id,
            Err("expected value at line 1 column 1".to_owned()),
        )
    }

    fn row(tenant: &str, id: &str, record: Result<SourceRecord, String>) -> SourceRow {
        SourceRow {
            tenant: tenant.to_owned(),
            id: id.to_owned(),
            record,
        }
    }

    fn record(id: &str, mode: SourceMode, poll_secs: Option<u64>) -> SourceRecord {
        SourceRecord {
            id: id.to_owned(),
            uri: format!("https://h/{id}.json"),
            mode,
            auth_ref: None,
            on_drift: OnDrift::Overwrite,
            poll_secs,
            drifted: false,
            last_version: None,
            last_digest: None,
            last_pulled_at_secs: None,
            last_outcome: None,
            ports: Vec::new(),
            revision: 1,
        }
    }

    fn running(entries: &[(&str, &str, u64)]) -> BTreeMap<SourceKey, u64> {
        entries
            .iter()
            .map(|(tenant, id, secs)| (key(tenant, id), *secs))
            .collect()
    }

    /// The ordinary case, unchanged by #243: declared tracking sources start,
    /// and a `pinned` one is not a poll target.
    #[test]
    fn plan_starts_tracking_sources_and_ignores_pinned_ones() {
        let plan = plan(
            &[tracking("acme", "mocks", 30), pinned("acme", "manual")],
            &BTreeMap::new(),
        );

        assert_eq!(plan.start, vec![(key("acme", "mocks"), 30)]);
        assert!(plan.stop.is_empty());
        assert!(plan.held.is_empty());
    }

    /// Issue #243, the core of it: a corrupt row holds its own poller and
    /// nothing else's.
    ///
    /// Before the fix the whole scan failed, so `reconcile` returned early and
    /// *no* tenant saw a start, a stop or an interval change — a fleet-wide
    /// stall caused by one unreadable row.
    #[test]
    fn plan_holds_a_corrupt_rows_poller_and_leaves_other_tenants_alone() {
        let plan = plan(
            &[
                corrupt("acme", "mocks"),
                tracking(DEFAULT_TENANT, "starts", 30),
                tracking(DEFAULT_TENANT, "keeps", 60),
            ],
            &running(&[
                ("acme", "mocks", 30),
                (DEFAULT_TENANT, "keeps", 60),
                (DEFAULT_TENANT, "gone", 15),
            ]),
        );

        assert_eq!(
            plan.held.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([key("acme", "mocks")])
        );
        assert!(
            !plan.held[&key("acme", "mocks")].is_empty(),
            "the decode failure travels with the key — it is the only thing that tells an \
             operator why the row is unreadable"
        );
        assert!(
            !plan.stop.contains(&key("acme", "mocks")),
            "a row that will not decode still exists — stopping its poller would turn a decode \
             failure into a silent poll stop, which is what the strict read was protecting against"
        );
        assert!(
            !plan.start.iter().any(|(k, _)| *k == key("acme", "mocks")),
            "and it must not be restarted either: its interval is exactly what cannot be read"
        );

        // Meanwhile every other tenant reconciles normally.
        assert_eq!(plan.start, vec![(key(DEFAULT_TENANT, "starts"), 30)]);
        assert_eq!(plan.stop, vec![key(DEFAULT_TENANT, "gone")]);
    }

    /// A corrupt row with nothing running cannot start one: there is no
    /// interval to start it with. It is still *held*, so it is counted and
    /// logged rather than passing unnoticed.
    #[test]
    fn plan_starts_nothing_for_a_corrupt_row_with_no_running_poller() {
        let plan = plan(&[corrupt("acme", "mocks")], &BTreeMap::new());

        assert!(plan.start.is_empty());
        assert!(plan.stop.is_empty());
        assert_eq!(
            plan.held.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([key("acme", "mocks")])
        );
        assert!(
            !plan.held[&key("acme", "mocks")].is_empty(),
            "the decode failure travels with the key — it is the only thing that tells an \
             operator why the row is unreadable"
        );
    }

    /// Repair needs no special path. Once the row decodes again, the normal
    /// interval-change logic sees a running poller on the old cadence and
    /// restarts it on the new one.
    #[test]
    fn plan_adopts_a_repaired_rows_new_interval_through_the_normal_stale_path() {
        let plan = plan(
            &[tracking("acme", "mocks", 60)],
            &running(&[("acme", "mocks", 30)]),
        );

        assert!(plan.held.is_empty());
        assert_eq!(plan.stop, vec![key("acme", "mocks")]);
        assert_eq!(plan.start, vec![(key("acme", "mocks"), 60)]);
    }

    /// Deletion needs no special path either: a `TenantDelete` cascade or a
    /// `SourceDelete` removes the row, so the key leaves both sets and the
    /// poller stops as any other unwanted one does — including a key that was
    /// held as corrupt right up until it was deleted.
    #[test]
    fn plan_stops_a_held_poller_once_its_row_is_deleted() {
        let plan = plan(&[], &running(&[("acme", "mocks", 30)]));

        assert!(plan.held.is_empty());
        assert_eq!(plan.stop, vec![key("acme", "mocks")]);
        assert!(plan.start.is_empty());
    }

    /// A tracking source with no interval cannot be polled. `validate` refuses
    /// that combination, so this is belt-and-braces — but it must read as "not
    /// a poll target", never as "held", or a record written by some other path
    /// would sit in the corrupt-row gauge forever.
    #[test]
    fn plan_does_not_hold_a_readable_tracking_row_that_names_no_interval() {
        let rows = [row(
            "acme",
            "mocks",
            Ok(record("mocks", SourceMode::Tracking, None)),
        )];
        let plan = plan(&rows, &BTreeMap::new());

        assert!(plan.start.is_empty());
        assert!(plan.held.is_empty(), "unpollable is not unreadable");
    }

    fn held(entries: &[(&str, &str)]) -> BTreeMap<SourceKey, String> {
        entries
            .iter()
            .map(|(tenant, id)| (key(tenant, id), "expected value".to_owned()))
            .collect()
    }

    /// The held set is the one piece of state the scheduler carries across
    /// ticks, and its whole job is to make the log edge-triggered. A corrupt row
    /// stays corrupt indefinitely, so a regression here is not a wrong message —
    /// it is the same message once a second forever, which is what buries the
    /// two moments worth reading.
    #[test]
    fn held_transitions_converge_so_a_steady_corrupt_row_is_logged_once() {
        let mut previous = BTreeSet::new();
        let current = held(&[("acme", "mocks")]);

        SourceScheduler::log_held_transitions(&mut previous, &current);
        assert_eq!(
            previous,
            BTreeSet::from([key("acme", "mocks")]),
            "the first tick records the row as held"
        );

        // A second tick with the same corrupt row must find nothing new to say.
        SourceScheduler::log_held_transitions(&mut previous, &current);
        assert_eq!(previous, BTreeSet::from([key("acme", "mocks")]));
    }

    /// Repair and deletion both arrive here as "no longer in the map", and both
    /// must clear the key — otherwise the next corruption of the same row would
    /// be silent, having never been seen to end.
    #[test]
    fn held_transitions_clear_a_key_once_its_row_reads_again() {
        let mut previous = BTreeSet::from([key("acme", "mocks"), key("acme", "billing")]);

        SourceScheduler::log_held_transitions(&mut previous, &held(&[("acme", "billing")]));
        assert_eq!(
            previous,
            BTreeSet::from([key("acme", "billing")]),
            "the repaired key leaves; the still-corrupt one stays"
        );

        SourceScheduler::log_held_transitions(&mut previous, &BTreeMap::new());
        assert!(previous.is_empty());
    }

    #[test]
    fn jitter_stays_within_ten_percent_and_never_collapses() {
        for _ in 0..500 {
            let d = jittered(100).as_secs_f64();
            assert!((90.0..=110.0).contains(&d), "{d}");
        }
        // The floor matters at the poll minimum: 5s ± 10% must not round to
        // something that hammers the source host.
        for _ in 0..500 {
            let d = jittered(crate::control::MIN_POLL_SECS).as_secs_f64();
            assert!(d >= 1.0, "{d}");
            assert!(d <= 5.5, "{d}");
        }
    }

    /// A poll status starts clean, remembers a failure, and forgets it on the
    /// next success — the contract `GET /admin/sources/:id` renders.
    #[test]
    fn poll_status_tracks_the_last_error_per_source() {
        let status = PollStatus::default();
        assert_eq!(status.last_error(DEFAULT_TENANT, "a"), None);

        status.record_failure(DEFAULT_TENANT, "a", "connection refused".to_owned());
        status.record_failure(DEFAULT_TENANT, "b", "404".to_owned());
        assert_eq!(
            status.last_error(DEFAULT_TENANT, "a").as_deref(),
            Some("connection refused")
        );
        assert_eq!(
            status.last_error(DEFAULT_TENANT, "b").as_deref(),
            Some("404")
        );

        status.record_success(DEFAULT_TENANT, "a");
        assert_eq!(
            status.last_error(DEFAULT_TENANT, "a"),
            None,
            "a recovered source must stop reporting a stale failure"
        );
        assert_eq!(
            status.last_error(DEFAULT_TENANT, "b").as_deref(),
            Some("404"),
            "one source recovering says nothing about another"
        );
    }

    /// Issue #239: source ids are only unique within a tenant, so the map must
    /// never answer one tenant's question with another tenant's failure — the
    /// error string embeds the other tenant's source URI.
    #[test]
    fn poll_status_keeps_same_named_sources_of_different_tenants_apart() {
        let status = PollStatus::default();
        status.record_failure(
            DEFAULT_TENANT,
            "payments",
            "https://internal/x timed out".to_owned(),
        );
        assert_eq!(
            status.last_error("acme", "payments"),
            None,
            "acme must not see the default tenant's failure for its own source name"
        );
        assert!(status.last_error(DEFAULT_TENANT, "payments").is_some());
    }
}
