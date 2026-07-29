//! The leader-only audit export loop (issue #164).
//!
//! Every node derives the same audit rows from the same log (#163), so every
//! node exporting would deliver N copies of everything. **The leader exports**,
//! grounded on the same `RaftMetrics` leadership watch the source scheduler and
//! the forward-to-leader path read — not a second notion of leadership, for the
//! reason `sources/scheduler.rs` spells out: two independent answers to "am I
//! the leader" is how a fleet ends up with two exporters during an election.
//!
//! ## At-least-once, and why it is not exactly-once
//!
//! A batch is **shipped first and checkpointed second**. That ordering is the
//! guarantee: a leader that dies in between re-ships its last batch when the
//! next leader resumes from the last committed checkpoint. The consumer dedups
//! on `(revision, op_id)` — which is why the row carries both.
//!
//! Exactly-once would need a transaction spanning the customer's bucket and the
//! Raft log. No such transaction exists, so the weaker guarantee is stated
//! rather than implied, and the duplicate window is bounded to one batch.
//!
//! ## Backpressure never reaches the write path
//!
//! This loop only *reads* committed state. A sink that is down cannot stall an
//! admin write, because no admin write waits on anything in here. What a dead
//! sink does is grow the lag gauge and the failure counter, and — if it stays
//! down past `--cluster-audit-retention` — leave a gap, which is counted and
//! logged at error level rather than passing silently. A quiet gap in an audit
//! export is the worst failure this feature could have, so it is the one thing
//! the loop refuses to do.

use std::sync::{Arc, Weak};

use anyhow::Context as _;
use std::time::Duration;

use uuid::Uuid;

use crate::control::{AuditSink, ControlOp, ControlRequest, FLEET_SCOPE, TenantId};
use crate::raft::RaftNode;
use crate::sources::auth::CredentialResolver;
use crate::sources::s3::S3Config;

use super::sink::{SinkTransport, transport_for};

/// How long the loop waits on the leadership watch before looking again. The
/// watch is the mechanism; this is the safety net that keeps a missed wakeup
/// from parking the exporter forever. Same role as the source scheduler's
/// `RECONCILE_FLOOR`.
const POLL_FLOOR: Duration = Duration::from_secs(1);

/// First retry delay after a failed ship. Doubles per consecutive failure up to
/// [`MAX_BACKOFF`].
const BASE_BACKOFF: Duration = Duration::from_millis(500);

/// Ceiling on the retry delay. A sink that has been down for an hour is not
/// helped by waiting longer than this, and a bounded ceiling keeps the recovery
/// latency after an outage predictable.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Leader-local, non-replicated export state, for `GET /admin/audit/sink`.
#[derive(Debug, Default)]
pub struct ExportStatus {
    inner: parking_lot::Mutex<ExportStatusInner>,
}

#[derive(Debug, Default, Clone)]
struct ExportStatusInner {
    running: bool,
    last_error: Option<String>,
    shipped_rows: u64,
    consecutive_failures: u32,
}

/// A point-in-time copy of the exporter's leader-local state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportStatusSnapshot {
    /// Whether this node currently has a sink and holds leadership.
    pub running: bool,
    /// The last ship failure, if the most recent attempt failed.
    pub last_error: Option<String>,
    /// Rows this node has shipped since it started.
    pub shipped_rows: u64,
    /// Consecutive failed attempts; `0` once a ship succeeds.
    pub consecutive_failures: u32,
}

impl ExportStatus {
    #[must_use]
    pub fn snapshot(&self) -> ExportStatusSnapshot {
        let inner = self.inner.lock();
        ExportStatusSnapshot {
            running: inner.running,
            last_error: inner.last_error.clone(),
            shipped_rows: inner.shipped_rows,
            consecutive_failures: inner.consecutive_failures,
        }
    }

    fn set_running(&self, running: bool) {
        self.inner.lock().running = running;
    }

    fn record_success(&self, rows: usize) {
        let mut inner = self.inner.lock();
        inner.last_error = None;
        inner.consecutive_failures = 0;
        inner.shipped_rows += rows as u64;
    }

    fn record_failure(&self, error: &str) -> u32 {
        let mut inner = self.inner.lock();
        inner.last_error = Some(error.to_owned());
        inner.consecutive_failures = inner.consecutive_failures.saturating_add(1);
        inner.consecutive_failures
    }
}

/// The node-local half of the exporter: how to reach a bucket, and how to turn
/// an `auth_ref` into a credential. Both are node-local by design — the
/// replicated record names a credential and never carries one.
pub struct ExportContext {
    pub resolver: Arc<dyn CredentialResolver>,
    pub s3: S3Config,
}

impl std::fmt::Debug for ExportContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportContext")
            .field("s3", &self.s3)
            .finish_non_exhaustive()
    }
}

/// The export loop.
pub struct AuditExporter {
    node: Weak<RaftNode>,
    context: Arc<ExportContext>,
    status: Arc<ExportStatus>,
}

impl AuditExporter {
    /// Spawn the export loop.
    ///
    /// Holds only a `Weak` node reference, for the reason `SourceScheduler`
    /// documents: the node owns the cluster port and the redb lock, so a task
    /// that kept it alive would keep both past shutdown and the next start
    /// would fail to bind. **The returned handle must be aborted on shutdown.**
    pub fn spawn(
        handle: &tokio::runtime::Handle,
        node: &Arc<RaftNode>,
        context: Arc<ExportContext>,
    ) -> (Arc<ExportStatus>, tokio::task::JoinHandle<()>) {
        let status = Arc::new(ExportStatus::default());
        let exporter = Self {
            node: Arc::downgrade(node),
            context,
            status: Arc::clone(&status),
        };
        let task = handle.spawn(async move { exporter.run().await });
        (status, task)
    }

    async fn run(self) {
        // Start pessimistic: if this node is already leader the first
        // `await_leadership_change` returns immediately with `true`.
        let mut was_leader = false;
        // Rebuilt whenever the sink record changes, so an operator repointing
        // the sink takes effect without a restart. Keyed on the record itself
        // rather than a version counter — the record *is* the identity.
        let mut active: Option<(AuditSink, Arc<dyn SinkTransport>)> = None;
        let mut backoff = BASE_BACKOFF;
        // Set when a batch comes back full: there is more behind it, so the
        // next pass skips the poll wait.
        let mut drain_immediately = false;

        loop {
            let is_leader = {
                let Some(node) = self.node.upgrade() else {
                    break;
                };
                if drain_immediately {
                    // The last batch came back full, so more is known to be
                    // pending. Waiting the poll floor here would cap the drain
                    // rate at one batch per second — which after a sink outage
                    // means the backlog drains while retention keeps ticking
                    // against it, the one interaction where slow drain becomes
                    // real loss. Re-read leadership without sleeping instead.
                    node.is_leader()
                } else {
                    node.await_leadership_change(was_leader, POLL_FLOOR).await
                }
            };
            was_leader = is_leader;
            drain_immediately = false;

            if !is_leader {
                // A follower that kept exporting is the N-copies bug this
                // whole design exists to prevent. Drop the transport too: it
                // holds a resolved credential, and a node that is not exporting
                // has no business keeping one in memory.
                active = None;
                self.status.set_running(false);
                // Reset rather than leave stale. A node deposed while 40k
                // revisions behind would otherwise keep publishing that lag
                // forever, and a dashboard taking `max()` across the fleet —
                // the natural aggregation for a leader-only gauge — would read
                // a backlog no node actually has.
                crate::metrics::audit_export_lag(0);
                continue;
            }

            let Some(node) = self.node.upgrade() else {
                break;
            };

            match self.step(&node, &mut active).await {
                Ok(StepOutcome::Idle) => {
                    backoff = BASE_BACKOFF;
                }
                Ok(StepOutcome::Shipped { rows, batch_full }) => {
                    backoff = BASE_BACKOFF;
                    drain_immediately = batch_full;
                    self.status.record_success(rows);
                    crate::metrics::audit_export_shipped(rows);
                }
                Err(error) => {
                    let message = format!("{error:#}");
                    // Not running: `running` means "this node is exporting", and
                    // a step that failed did not export. Leaving it true made a
                    // leader whose sink URI never built look identical to one
                    // shipping happily with a stale `lastError`.
                    self.status.set_running(false);
                    let failures = self.status.record_failure(&message);
                    crate::metrics::audit_export_failure();
                    tracing::error!(
                        consecutive_failures = failures,
                        error = %message,
                        "shipping an audit batch to the export sink failed; the fleet stays \
                         writable and the batch will be retried"
                    );
                    // Explicit backoff state rather than recursion: the retry
                    // has to stay in this loop so a leadership change is still
                    // observed while a sink is down.
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
        self.status.set_running(false);
    }

    /// One pass: read the sink, read the backlog, ship it, checkpoint it.
    async fn step(
        &self,
        node: &Arc<RaftNode>,
        active: &mut Option<(AuditSink, Arc<dyn SinkTransport>)>,
        // `Result` rather than a swallowed default throughout: every failure
        // below is a real one that must reach the caller's error branch.
    ) -> anyhow::Result<StepOutcome> {
        let Some(sink) = node
            .audit_sink()
            .context("reading the declared audit sink")?
        else {
            // No sink declared: nothing is built, nothing is read, nothing is
            // shipped. This is the off-by-default path.
            *active = None;
            self.status.set_running(false);
            crate::metrics::audit_export_lag(0);
            return Ok(StepOutcome::Idle);
        };

        // Rebuild only when the record actually changed, so an idle loop is not
        // reconstructing an HTTP client and re-parsing the URI once a second.
        // It does *not* cache the credential: both transports resolve theirs
        // inside `ship`, per attempt, which is what keeps a rotated secret from
        // needing a restart and a revoked one from continuing to work.
        let transport = match active {
            Some((current, transport)) if *current == sink => Arc::clone(transport),
            _ => {
                let built = transport_for(
                    &sink.uri,
                    sink.auth_ref.as_deref(),
                    &self.context.resolver,
                    &self.context.s3,
                )?;
                *active = Some((sink.clone(), Arc::clone(&built)));
                built
            }
        };
        self.status.set_running(true);

        let checkpoint = node
            .audit_checkpoint()
            .context("reading the audit export checkpoint")?;
        let rows = node
            .audit_since(
                checkpoint.saturating_add(1),
                None,
                sink.batch_max_rows as usize,
            )
            .context("reading the audit backlog")?;

        if let Some(applied) = node.status().last_applied {
            crate::metrics::audit_export_lag(applied.saturating_sub(checkpoint));
        }

        let Some(first) = rows.first() else {
            return Ok(StepOutcome::Idle);
        };

        // Retention got there first: `gc_audit` actually removed rows this
        // exporter had not shipped, and they are gone from every replica.
        //
        // The evidence is the GC watermark, not revision arithmetic on the
        // surviving rows. "The next row is not at `checkpoint + 1`" proves
        // nothing: `EntryPayload::Blank` (every election), `Membership`
        // entries, and this exporter's own unaudited `AuditCheckpointPut` each
        // consume a revision without producing an audit row. Deriving loss that
        // way fires on a healthy fleet after every single batch — which would
        // make the one counter operators are told to alert on a rising false
        // positive, and bury the real signal.
        //
        // The floor is `max(checkpoint, sink.revision)`: history from before
        // this sink was declared was never in scope for it, so its expiry is
        // not a loss this exporter caused. (Corner case, stated rather than
        // hidden: delete a sink, let rows age out, re-declare it, and rows lost
        // while no sink was configured are not attributed to the new one.)
        //
        // Counted and logged at error level, never passed over quietly. Still a
        // revision *span* — an upper bound on rows lost, since once GC has run
        // there is nothing left to count exactly.
        let floor = checkpoint.max(sink.revision);
        let gc_watermark = node
            .audit_gc_watermark()
            .context("reading the audit retention watermark")?;
        if gc_watermark > floor {
            let skipped = gc_watermark - floor;
            crate::metrics::audit_export_skipped(skipped);
            tracing::error!(
                from_revision = floor + 1,
                to_revision = gc_watermark,
                skipped_revisions = skipped,
                "audit rows aged past retention before the export sink accepted them; that \
                 window is permanently absent from the exported stream"
            );
        }

        let last_revision = rows.last().map_or(checkpoint, |row| row.revision);
        let batch_start = first.revision;
        let encoded = rows
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .context("encoding the audit batch as JSON lines")?;

        // Ship, *then* checkpoint. Reversing these two lines would turn
        // at-least-once into at-most-once and lose a batch on every leader
        // death — see the module doc.
        let shipped = transport.ship(&encoded, batch_start).await?;

        node.submit(mint(ControlOp::AuditCheckpointPut {
            // The fleet scope, like the sink ops this checkpoint tracks. It is
            // invisible in the audit stream (the op is not audited, by design),
            // but there is no reason for the three audit-export ops to give two
            // different answers to "which tenant is this about".
            tenant: TenantId::new(FLEET_SCOPE),
            revision: last_revision,
        }))
        .await
        .context("committing the audit export checkpoint")?;

        Ok(StepOutcome::Shipped {
            rows: shipped.rows,
            batch_full: rows.len() >= sink.batch_max_rows as usize,
        })
    }
}

enum StepOutcome {
    /// Nothing to do: no sink, or no rows past the checkpoint.
    Idle,
    Shipped {
        rows: usize,
        /// The batch came back at `batch_max_rows`, so the backlog is known to
        /// hold more and the loop should come straight back rather than wait.
        batch_full: bool,
    },
}

/// Mint an unattributed request for an op the exporter submits on its own
/// behalf. `principal: None` is correct here and not an omission: no operator
/// asked for this write, the exporter did.
fn mint(op: ControlOp) -> ControlRequest {
    ControlRequest {
        op_id: Uuid::new_v4(),
        principal: None,
        // Same reasoning as `sources::mint`: a pre-epoch clock mints 0, which
        // only makes this op read as already old to the logical clock.
        issued_at_secs: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
        expected_revision: None,
        op,
    }
}
