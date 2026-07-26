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
//! ## Failures are visible without being written down
//!
//! A failing poll must not write a log entry per failure — that would
//! reintroduce the log growth the short circuit exists to avoid, sideways, and
//! at the worst possible time (an upstream outage is exactly when you do not
//! want fleet-wide writes). So a poll error is recorded **leader-locally**: an
//! in-memory last-error per source, surfaced on `GET /admin/sources/:id` as
//! `lastPollError`, plus a counter. The durable `last` record still only moves
//! when a pull actually applies or is skipped.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::raft::RaftNode;

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
/// Deliberately not in the state machine. A poll failure is a property of one
/// node's view of an external host at one moment — replicating it would mean a
/// log entry per failure, which is the log-growth problem the digest short
/// circuit exists to prevent.
#[derive(Debug, Default)]
pub struct PollStatus {
    last_error: Mutex<BTreeMap<String, String>>,
}

impl PollStatus {
    /// The last poll error for `id`, if the most recent attempt on this node
    /// failed. `None` once a later attempt succeeds.
    #[must_use]
    pub fn last_error(&self, id: &str) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    fn record_failure(&self, id: &str, detail: String) {
        self.last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.to_owned(), detail);
    }

    fn record_success(&self, id: &str) {
        self.last_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
    }

    fn forget(&self, id: &str) {
        self.record_success(id);
    }
}

/// What the supervisor believes it is currently polling: source id → the
/// interval it was started with, plus the task's abort handle.
struct Running {
    poll_secs: u64,
    task: tokio::task::JoinHandle<()>,
}

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
        let mut running: BTreeMap<String, Running> = BTreeMap::new();
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
                continue;
            }
            if self.reconcile(&mut running).is_none() {
                break;
            }
        }
        Self::stop_all(&mut running, &self.status);
    }

    /// Bring the running task set in line with the applied source table.
    /// `None` means the node or puller is gone and the supervisor should exit.
    fn reconcile(&self, running: &mut BTreeMap<String, Running>) -> Option<()> {
        let node = self.node.upgrade()?;
        let puller = self.puller.upgrade()?;

        let desired: BTreeMap<String, u64> = match node.sources() {
            Ok(sources) => sources
                .into_iter()
                .filter_map(|source| {
                    // A source that tracks without an interval cannot be
                    // polled; `validate` refuses that combination, so this is
                    // belt-and-braces against a record written by some other
                    // path rather than an expected state.
                    matches!(source.mode, crate::control::SourceMode::Tracking)
                        .then_some(())
                        .and(source.poll_secs)
                        .map(|secs| (source.id, secs))
                })
                .collect(),
            Err(e) => {
                // A read failure is this node's problem, not a reason to tear
                // down healthy pollers: keep what is running and retry on the
                // next tick.
                tracing::warn!(error = %e, "source scheduler could not read the source table");
                return Some(());
            }
        };

        // Stop what is no longer wanted, or whose interval changed — a changed
        // interval is a new schedule, and restarting the task is the honest way
        // to adopt it.
        let stale: Vec<String> = running
            .iter()
            .filter(|(id, r)| desired.get(*id).is_none_or(|secs| *secs != r.poll_secs))
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            if let Some(previous) = running.remove(&id) {
                previous.task.abort();
                self.status.forget(&id);
                tracing::debug!(source_id = %id, "stopped polling");
            }
        }

        for (id, poll_secs) in desired {
            if running.contains_key(&id) {
                continue;
            }
            let task = tokio::spawn(poll_loop(
                id.clone(),
                poll_secs,
                Arc::downgrade(&puller),
                Arc::clone(&self.status),
            ));
            running.insert(id.clone(), Running { poll_secs, task });
            tracing::info!(source_id = %id, poll_secs, "polling a tracking source");
        }
        Some(())
    }

    fn stop_all(running: &mut BTreeMap<String, Running>, status: &PollStatus) {
        for (id, r) in std::mem::take(running) {
            r.task.abort();
            status.forget(&id);
        }
    }
}

/// One source's poll loop. Sleeps first, so a `SourcePut` does not double up
/// with the pull its own creation path already performed.
async fn poll_loop(
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
        match puller.pull(&id, Some("scheduler".to_owned())).await {
            Ok(report) => {
                status.record_success(&id);
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
                tracing::warn!(source_id = %id, error = %detail, "source poll failed");
                status.record_failure(&id, detail);
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
        assert_eq!(status.last_error("a"), None);

        status.record_failure("a", "connection refused".to_owned());
        status.record_failure("b", "404".to_owned());
        assert_eq!(
            status.last_error("a").as_deref(),
            Some("connection refused")
        );
        assert_eq!(status.last_error("b").as_deref(), Some("404"));

        status.record_success("a");
        assert_eq!(
            status.last_error("a"),
            None,
            "a recovered source must stop reporting a stale failure"
        );
        assert_eq!(
            status.last_error("b").as_deref(),
            Some("404"),
            "one source recovering says nothing about another"
        );
    }
}
