//! `redb`-backed implementation of openraft 0.9's split storage API
//! (`RaftLogStorage` + `RaftLogReader` + `RaftStateMachine` + `RaftSnapshotBuilder`).
//!
//! Table layout (one `redb::Database`, opened once and shared via `Arc`):
//!
//! * `raft_log`      — `u64 -> Entry<TypeConfig>` (JSON), keyed by log index.
//! * `raft_log_meta` — `() -> LogMeta` (JSON): the last-purged log id. Not in the
//!   ADR's table sketch, but required — once `purge` deletes every entry at or
//!   before a log id, `get_log_state` has nowhere else to recover it from.
//! * `raft_vote`     — `() -> Vote<u64>` (JSON).
//! * `raft_snapshot` — `() -> StoredSnapshot` (JSON): the last installed/built snapshot.
//! * `sm_configs`    — `(tenant, port) -> StoredImposter` (JSON): the applied
//!   config, its enabled flag, and the revision (log index) that last wrote it.
//! * `sm_routes`     — `(tenant, route id) -> Route` (JSON): the front door's
//!   replicated route table (issue #131). Read as a whole per tenant to
//!   recompile a [`CompiledRoutes`] after every mutating op.
//! * `sm_op_dedup`   — `op_id -> DedupEntry` (JSON): the response recorded for an
//!   applied op, kept for [`DEDUP_TTL_SECS`] so a replayed intent (crash-replay,
//!   client retry with the same `Idempotency-Key`) is exactly-once-in-effect.
//! * `sm_applied`    — `() -> AppliedState` (JSON): last-applied log id + membership.
//!
//! Log and vote writes commit with `Durability::Immediate` per the ADR (log and vote
//! must fsync before ack). Snapshot and state-machine writes use the default
//! (`None`) durability — the snapshot table is a redundant persisted copy for
//! [`RaftStateMachine::get_current_snapshot`], not the durability boundary; the log
//! is.
//!
//! # Apply semantics (issue #9)
//!
//! Apply is **deterministic and infallible** with respect to the local engine:
//!
//! 1. Inside one write transaction: GC expired dedup entries, then per entry —
//!    dedup-check, [`crate::control::validate`], mutate `sm_configs`, record the
//!    response in `sm_op_dedup`. Everything here depends only on the committed
//!    op and the tables, so every replica computes the same tables and the same
//!    [`ControlResponse`]. A deterministic refusal (validation, patching an
//!    absent port) is a *committed* `Failed` outcome, not an apply error.
//! 2. After the transaction commits: drive the local `ImposterManager` (when
//!    one is attached) toward the applied state. A side-effect failure here — a
//!    port that will not bind, an edit the live engine refuses — never fails
//!    apply; it is recorded per port in [`RedbStateMachine::apply_failures`]
//!    for the operator surface (`GET /_cluster/imposters`) and the
//!    `Rift-Cluster-Warnings` header (§7.4.6 semantics preserved).
//!
//! Only real storage I/O errors fail apply — for openraft a storage failure is
//! fatal to the node, and that is the correct severity for a log that can no
//! longer be applied.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use parking_lot::Mutex;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, Table, TableDefinition};
use rift_ee::seams::{
    ApplyReport, CompiledRoutes, ImposterConfig, ImposterError, ImposterManager, Route, RouteTable,
};
use serde::{Deserialize, Serialize};

use super::TypeConfig;
use crate::control::{
    self, ControlOp, ControlRequest, ControlResponse, DEFAULT_TENANT, StubEdit, StubEditScript,
};

type StorageResult<T> = Result<T, StorageError<u64>>;

const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const LOG_META_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("raft_log_meta");
const VOTE_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("raft_vote");
const SNAPSHOT_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("raft_snapshot");
const SM_CONFIGS_TABLE: TableDefinition<(&str, u16), &str> = TableDefinition::new("sm_configs");
/// `(tenant, route id) -> Route` (JSON): the front door's replicated route
/// table (issue #131). One row per route rather than one row per table, so a
/// `DeleteRoute` is a single-key removal instead of a read-modify-write of
/// the whole tenant's set.
const SM_ROUTES_TABLE: TableDefinition<(&str, &str), &str> = TableDefinition::new("sm_routes");
const SM_DEDUP_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_op_dedup");
const SM_APPLIED_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("sm_applied");
/// Node-local durable intents (issue #9 R4): ops this node accepted but has
/// not yet seen commit. NOT replicated state — never in snapshots, never
/// touched by apply; each node parks and replays only what it accepted.
const PENDING_INTENTS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("pending_intents");

/// How long an applied op's response is retained for dedup: 24 h (issue #9).
/// After expiry a replay of the same `op_id` re-applies — the durable-intent
/// replay loop (slice 3) retries on the scale of seconds-to-minutes, so a day
/// bounds the table without weakening the guarantee it exists for.
const DEDUP_TTL_SECS: u64 = 24 * 60 * 60;

/// Persisted marker for the last log id purged from `raft_log`.
#[derive(Debug, Default, Serialize, Deserialize)]
struct LogMeta {
    last_purged_log_id: Option<LogId<u64>>,
}

/// Persisted state-machine cursor: last-applied log id + membership, plus the
/// replicated logical clock — the maximum `issued_at_secs` any applied entry
/// has carried. Dedup TTL/GC run against this, never against a replica's local
/// clock, so every replica expires exactly the same entries at exactly the same
/// log point.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AppliedState {
    last_applied_log: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    #[serde(default)]
    logical_clock_secs: u64,
}

/// What `sm_configs` stores per `(tenant, port)`: the canonical config JSON,
/// whether the imposter is enabled (always `true` until the `SetEnabled` slice
/// lands with #15), and the log index that last wrote this record.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredImposter {
    config_json: String,
    enabled: bool,
    revision: u64,
}

/// What `sm_op_dedup` stores per `op_id`. The applying log index lives inside
/// `response.revision`; a separate copy would be dead data.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DedupEntry {
    response: ControlResponse,
    expires_at_secs: u64,
}

/// The state machine's data, as captured in a snapshot. Dedup entries are part
/// of the replicated state on purpose: a follower catching up via snapshot must
/// still collapse a replayed `op_id` to the original response, or a partition
/// heal would double-apply the intents replayed across it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SnapshotPayload {
    /// `(tenant, port, stored-imposter JSON)` rows of `sm_configs`.
    configs: Vec<(String, u16, String)>,
    /// `(tenant, route id, route JSON)` rows of `sm_routes`. Defaulted so a
    /// snapshot built before issue #131 still installs cleanly on an upgraded
    /// node — it just carries no routes, the same as a fleet that never wrote
    /// any.
    #[serde(default)]
    routes: Vec<(String, String, String)>,
    /// `(op_id, dedup-entry JSON)` rows of `sm_op_dedup`.
    dedup: Vec<(String, String)>,
    last_applied_log: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    #[serde(default)]
    logical_clock_secs: u64,
}

/// A snapshot plus the metadata openraft needs to identify it, as persisted in
/// `raft_snapshot`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

/// Create a fresh (or reopen an existing) `redb` database at `path` and return the
/// log store and state machine that share it.
///
/// Opening the database and initializing its tables is real I/O that can fail
/// (a missing directory, a permissions problem, a corrupt file), so the failure
/// is surfaced as openraft's `StorageError` rather than panicking a node at
/// startup — a control-plane node that cannot open its own log must refuse to
/// start, not abort.
pub async fn new<P: AsRef<Path>>(path: P) -> StorageResult<(RedbLogStore, RedbStateMachine)> {
    let db = Database::create(path).map_err(|e| StorageError::from(StorageIOError::write(&e)))?;
    {
        // `open_table` on a write transaction creates the table if it doesn't exist
        // yet; a read transaction against a table that was never created errors. Do
        // this once up front so every later read sees an (possibly empty) table.
        let write_txn = db
            .begin_write()
            .map_err(|e| StorageError::from(StorageIOError::write(&e)))?;
        // Each table has its own typed schema, and redb records that schema on
        // first creation, so they must be opened at their real types — a generic
        // re-definition under the same name would be rejected as a type mismatch.
        let io = |e: redb::TableError| StorageError::from(StorageIOError::write(&e));
        write_txn.open_table(LOG_TABLE).map_err(io)?;
        write_txn.open_table(LOG_META_TABLE).map_err(io)?;
        write_txn.open_table(VOTE_TABLE).map_err(io)?;
        write_txn.open_table(SNAPSHOT_TABLE).map_err(io)?;
        write_txn.open_table(SM_CONFIGS_TABLE).map_err(io)?;
        write_txn.open_table(SM_ROUTES_TABLE).map_err(io)?;
        write_txn.open_table(SM_DEDUP_TABLE).map_err(io)?;
        write_txn.open_table(SM_APPLIED_TABLE).map_err(io)?;
        write_txn.open_table(PENDING_INTENTS_TABLE).map_err(io)?;
        write_txn
            .commit()
            .map_err(|e| StorageError::from(StorageIOError::write(&e)))?;
    }
    let db = Arc::new(db);
    Ok((RedbLogStore { db: db.clone() }, RedbStateMachine::new(db)))
}

// ---------------------------------------------------------------------------
// Log storage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct RedbLogStore {
    db: Arc<Database>,
}

impl RaftLogReader<TypeConfig> for RedbLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_logs(&e))?;
        let table = read_txn
            .open_table(LOG_TABLE)
            .map_err(|e| StorageIOError::read_logs(&e))?;

        // `saturating_add` rather than `+ 1`: a bound of `u64::MAX` would
        // overflow-panic in debug builds. Log indices never approach that in
        // practice (openraft drives these ranges), but the reader must not be a
        // panic site regardless of what range it is handed.
        let start = match range.start_bound() {
            Bound::Included(x) => Bound::Included(*x),
            Bound::Excluded(x) => Bound::Included(x.saturating_add(1)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(x) => Bound::Excluded(x.saturating_add(1)),
            Bound::Excluded(x) => Bound::Excluded(*x),
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut entries = Vec::new();
        for item in table
            .range((start, end))
            .map_err(|e| StorageIOError::read_logs(&e))?
        {
            let (_, value) = item.map_err(|e| StorageIOError::read_logs(&e))?;
            let entry: Entry<TypeConfig> =
                serde_json::from_slice(value.value()).map_err(|e| StorageIOError::read_logs(&e))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for RedbLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_logs(&e))?;

        let last_log_id = {
            let table = read_txn
                .open_table(LOG_TABLE)
                .map_err(|e| StorageIOError::read_logs(&e))?;
            match table.last().map_err(|e| StorageIOError::read_logs(&e))? {
                None => None,
                Some((_, value)) => {
                    let entry: Entry<TypeConfig> = serde_json::from_slice(value.value())
                        .map_err(|e| StorageIOError::read_logs(&e))?;
                    Some(entry.log_id)
                }
            }
        };

        let last_purged_log_id = {
            let table = read_txn
                .open_table(LOG_META_TABLE)
                .map_err(|e| StorageIOError::read_logs(&e))?;
            table
                .get(())
                .map_err(|e| StorageIOError::read_logs(&e))?
                .map(|g| serde_json::from_slice::<LogMeta>(g.value()))
                .transpose()
                .map_err(|e| StorageIOError::read_logs(&e))?
                .and_then(|m| m.last_purged_log_id)
        };

        let last_log_id = last_log_id.or(last_purged_log_id);

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> StorageResult<()> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageIOError::write_vote(&e))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|e| StorageIOError::write_vote(&e))?;
        {
            let mut table = write_txn
                .open_table(VOTE_TABLE)
                .map_err(|e| StorageIOError::write_vote(&e))?;
            let bytes = serde_json::to_vec(vote).map_err(|e| StorageIOError::write_vote(&e))?;
            table
                .insert((), bytes.as_slice())
                .map_err(|e| StorageIOError::write_vote(&e))?;
        }
        write_txn
            .commit()
            .map_err(|e| StorageIOError::write_vote(&e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<u64>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_vote(&e))?;
        let table = read_txn
            .open_table(VOTE_TABLE)
            .map_err(|e| StorageIOError::read_vote(&e))?;
        table
            .get(())
            .map_err(|e| StorageIOError::read_vote(&e))?
            .map(|g| serde_json::from_slice(g.value()))
            .transpose()
            .map_err(|e| StorageError::from(StorageIOError::read_vote(&e)))
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageIOError::write_logs(&e))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|e| StorageIOError::write_logs(&e))?;
        {
            let mut table = write_txn
                .open_table(LOG_TABLE)
                .map_err(|e| StorageIOError::write_logs(&e))?;
            for entry in entries {
                let bytes =
                    serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?;
                table
                    .insert(entry.log_id.index, bytes.as_slice())
                    .map_err(|e| StorageIOError::write_logs(&e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| StorageIOError::write_logs(&e))?;

        // `redb` commits are synchronous, so by the time we get here the entries are
        // already durable — there is no separate flush to await. On any earlier
        // error we return via `?` above and drop `callback` uncalled, which openraft
        // treats as "this append never happened."
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> StorageResult<()> {
        // Default (non-Immediate) durability is deliberate here and in `purge`:
        // both only remove entries that are safe to lose on a crash — `truncate`
        // drops conflicting, not-yet-committed entries, and `purge` drops entries
        // already captured in a snapshot — and openraft re-drives both on
        // restart. Only `append`/`save_vote` gate on `Immediate`, because those
        // are where losing a write would lose a *committed* entry.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageIOError::write_logs(&e))?;
        {
            let mut table = write_txn
                .open_table(LOG_TABLE)
                .map_err(|e| StorageIOError::write_logs(&e))?;
            table
                .retain_in(log_id.index.., |_, _| false)
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }
        write_txn
            .commit()
            .map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> StorageResult<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageIOError::write_logs(&e))?;
        {
            let mut table = write_txn
                .open_table(LOG_TABLE)
                .map_err(|e| StorageIOError::write_logs(&e))?;
            table
                .retain_in(0..=log_id.index, |_, _| false)
                .map_err(|e| StorageIOError::write_logs(&e))?;

            let mut meta_table = write_txn
                .open_table(LOG_META_TABLE)
                .map_err(|e| StorageIOError::write_logs(&e))?;
            let meta = LogMeta {
                last_purged_log_id: Some(log_id),
            };
            let bytes = serde_json::to_vec(&meta).map_err(|e| StorageIOError::write_logs(&e))?;
            meta_table
                .insert((), bytes.as_slice())
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }
        write_txn
            .commit()
            .map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// What the post-commit engine drive must do for one applied op, in log order.
///
/// `Sync` carries the desired config set *as of that op* (snapshotted inside
/// the apply transaction), so a batch like `Put(A); PatchStubs(A)` replays
/// against the engine with the same intermediate states the tables went
/// through — computing the set after commit would make the later patch
/// double-apply.
#[derive(Debug)]
enum EngineAction {
    Sync(Vec<ImposterConfig>),
    Patch {
        port: u16,
        edit: StubEditScript,
    },
    /// Pause/resume in place via the upstream write-through (#817): never a
    /// wholesale replace, so the imposter's runtime state survives.
    SetEnabled {
        port: u16,
        enabled: bool,
    },
    /// A sync that must NOT run: a stored record failed to parse, and a partial
    /// desired set would delete the live imposters it omits. Recorded as an
    /// apply failure for the named port; the engine keeps its current state.
    RefuseSync {
        port: u16,
        error: String,
    },
    /// Recompile and hot-swap the front door's route table (issue #131),
    /// carrying the desired table computed *as of that op* — the same
    /// intra-transaction snapshot discipline as `Sync` above, and for the same
    /// reason: a batch that both writes and deletes routes must replay against
    /// the ArcSwap through the same intermediate states the table went
    /// through.
    SyncRoutes(RouteTable),
    /// A route sync that must NOT run: a stored record failed to parse. The
    /// front door keeps its last-known-good compiled table rather than
    /// swapping in a partial one.
    RefuseRoutesSync {
        id: String,
        error: String,
    },
}

#[derive(Clone)]
pub struct RedbStateMachine {
    db: Arc<Database>,
    snapshot_idx: Arc<AtomicU64>,
    /// The local engine committed ops are projected onto. `None` in storage
    /// tests and while the embedder has not wired one — the state machine is
    /// then tables-only, which is exactly what the conformance suite exercises.
    engine: Option<Arc<ImposterManager>>,
    /// The front door's hot-swappable compiled table (issue #131). `None` in
    /// storage tests and on a node that never binds a front door — routes are
    /// still replicated and readable from `sm_routes` either way, this is only
    /// the dispatch-side handle. Attach before `Raft::new` for the same reason
    /// as `engine`: replay during join must drive it too, not just live
    /// commits.
    routes: Option<Arc<ArcSwap<CompiledRoutes>>>,
    /// Last engine side-effect failure per port, cleared when a later drive
    /// succeeds for that port. Key 0 is the set-level slot (an `apply_config`
    /// refusal that names no single port). This is node status, not replicated
    /// state — every replica has its own bind outcomes.
    apply_failures: Arc<Mutex<BTreeMap<u16, String>>>,
}

impl std::fmt::Debug for RedbStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbStateMachine")
            .field("engine", &self.engine.is_some())
            .field("routes", &self.routes.is_some())
            .finish_non_exhaustive()
    }
}

impl RedbStateMachine {
    fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            snapshot_idx: Arc::new(AtomicU64::new(0)),
            engine: None,
            routes: None,
            apply_failures: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Attach the local engine committed ops are applied to. Call before the
    /// state machine is handed to `Raft::new` (and before cloning a reader), so
    /// every handle shares the same engine and failure map.
    #[must_use]
    pub fn with_engine(mut self, engine: Arc<ImposterManager>) -> Self {
        self.engine = Some(engine);
        self
    }

    /// Attach the front door's compiled-route handle. Same before-`Raft::new`
    /// contract as [`Self::with_engine`].
    #[must_use]
    pub fn with_routes_handle(mut self, routes: Arc<ArcSwap<CompiledRoutes>>) -> Self {
        self.routes = Some(routes);
        self
    }

    /// How many live handles share this state machine's `redb::Database`. The
    /// node uses this to tell when openraft has dropped its own storage clones on
    /// shutdown — while any remain, the redb file lock is still held and a restart
    /// on the same directory would fail to acquire it.
    pub(crate) fn db_refs(&self) -> usize {
        Arc::strong_count(&self.db)
    }

    // `StorageResult` wraps openraft's `StorageError<u64>`, which is inherently
    // large (an `AnyError` plus an optional backtrace); every trait method here
    // returns it because the trait mandates it, and this private helper matches
    // that shape rather than introduce a second error type just for itself.
    #[allow(clippy::result_large_err)]
    fn read_applied(&self) -> StorageResult<AppliedState> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_APPLIED_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        table
            .get(())
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|g| serde_json::from_slice::<AppliedState>(g.value()))
            .transpose()
            .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))
            .map(Option::unwrap_or_default)
    }
}

impl RedbStateMachine {
    /// Read the applied config JSON for the default tenant's `port`, or `None`
    /// if no config has been applied for it.
    ///
    /// This is the node's read path: reads answer from the applied state machine
    /// directly and never go through Raft, so a follower or a restarted node can
    /// serve committed config without waiting to become leader. Openraft owns the
    /// state machine as `&mut self`, so the node keeps a cheap `Clone` of this
    /// handle (both share one `Arc<Database>`) purely for reads.
    #[allow(clippy::result_large_err)]
    pub fn read_config(&self, port: u16) -> StorageResult<Option<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        table
            .get((DEFAULT_TENANT, port))
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|g| {
                serde_json::from_str::<StoredImposter>(g.value())
                    .map(|stored| stored.config_json)
                    .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))
            })
            .transpose()
    }

    /// Every default-tenant port that currently has an applied config, ascending.
    ///
    /// Ports rather than bodies: the operator endpoints report *what* the node
    /// has converged on, and a fleet's full config set is far larger than the
    /// answer to that question.
    #[allow(clippy::result_large_err)]
    pub fn configured_ports(&self) -> StorageResult<Vec<u16>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut ports = Vec::new();
        for item in table
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, _) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (tenant, port) = key.value();
            if tenant == DEFAULT_TENANT {
                ports.push(port);
            }
        }
        Ok(ports)
    }

    /// The default tenant's route table, as currently applied. Like
    /// [`Self::read_config`], this is the node's own read path — it answers
    /// from local durable state without a Raft round trip. It is also the
    /// *only* read path for routes: upstream has no `GET /front-door/routes`
    /// to proxy to (U-11's admin CRUD was deferred), so `GET
    /// /front-door/routes` in the clustered admin front calls straight
    /// through to this.
    #[allow(clippy::result_large_err)]
    pub fn route_table(&self) -> StorageResult<RouteTable> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_ROUTES_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        match Self::desired_routes(&table)
            .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?
        {
            Ok(table) => Ok(table),
            Err((id, error)) => {
                tracing::error!(route_id = %id, error = %error, "corrupt stored route");
                Err(StorageError::from(StorageIOError::read_state_machine(
                    &std::io::Error::other(format!("corrupt stored route {id}: {error}")),
                )))
            }
        }
    }

    /// Last engine side-effect failure per port (0 = set-level), as recorded by
    /// the most recent drives. Empty when the local engine matches the applied
    /// state.
    #[must_use]
    pub fn apply_failures(&self) -> BTreeMap<u16, String> {
        self.apply_failures.lock().clone()
    }

    /// Durably park an accepted intent (issue #9 R4). Runs with `Immediate`
    /// durability because this write IS the acceptance boundary: once the
    /// client hears anything other than a hard error, the op must survive a
    /// crash. Parking the same op id twice overwrites — idempotent by key.
    #[allow(clippy::result_large_err)]
    pub fn park_intent(&self, request: &ControlRequest) -> StorageResult<()> {
        let key = request.op_id.to_string();
        let value =
            serde_json::to_string(request).map_err(|e| StorageIOError::write_state_machine(&e))?;
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        {
            let mut table = write_txn
                .open_table(PENDING_INTENTS_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            table
                .insert(key.as_str(), value.as_str())
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
        }
        write_txn
            .commit()
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        // Counted per call, not per new row: a same-key re-park (idempotent
        // overwrite) can over-report the pending gauge until the next replay
        // sweep resamples it from the ledger.
        crate::metrics::intent_parked();
        Ok(())
    }

    /// Remove a parked intent once its op is terminal (applied or refused —
    /// both are recorded in `sm_op_dedup`). Removing an absent key is a no-op:
    /// the front and the replay loop can both retire the same intent.
    #[allow(clippy::result_large_err)]
    pub fn unpark_intent(&self, op_id: &uuid::Uuid) -> StorageResult<()> {
        let key = op_id.to_string();
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        let removed = {
            let mut table = write_txn
                .open_table(PENDING_INTENTS_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            table
                .remove(key.as_str())
                .map_err(|e| StorageIOError::write_state_machine(&e))?
                .is_some()
        };
        write_txn
            .commit()
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        if removed {
            crate::metrics::intent_unparked();
        }
        Ok(())
    }

    /// Every parked intent, parsed. An entry that no longer parses cannot ever
    /// be replayed, so it is dropped — loudly, at error level — rather than
    /// wedging the replay loop forever on an unrecoverable row.
    #[allow(clippy::result_large_err)]
    pub fn parked_intents(&self) -> StorageResult<Vec<ControlRequest>> {
        let rows = {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let table = read_txn
                .open_table(PENDING_INTENTS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut rows = Vec::new();
            for item in table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                rows.push((key.value().to_owned(), value.value().to_owned()));
            }
            rows
        };
        let mut intents = Vec::new();
        for (key, value) in rows {
            match serde_json::from_str::<ControlRequest>(&value) {
                Ok(request) => intents.push(request),
                Err(e) => {
                    tracing::error!(op_id = %key, error = %e, "dropping unparseable parked intent");
                    // Best-effort cleanup: a bad row whose delete ALSO fails
                    // must not abort the batch — that would starve every
                    // healthy parked intent behind one corrupt one, forever.
                    if let Ok(op_id) = key.parse::<uuid::Uuid>()
                        && let Err(e) = self.unpark_intent(&op_id)
                    {
                        tracing::error!(op_id = %key, error = %e, "could not remove the corrupt row");
                    }
                }
            }
        }
        Ok(intents)
    }

    /// The recorded outcome of an applied op, if the dedup window still holds
    /// it: what `GET /_cluster/ops/:id` reports for terminal ops.
    #[allow(clippy::result_large_err)]
    pub fn read_op(&self, op_id: &uuid::Uuid) -> StorageResult<Option<ControlResponse>> {
        let key = op_id.to_string();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_DEDUP_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        table
            .get(key.as_str())
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|g| {
                serde_json::from_str::<DedupEntry>(g.value())
                    .map(|entry| entry.response)
                    .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))
            })
            .transpose()
    }

    /// Whether this node still holds a parked intent for `op_id`.
    #[allow(clippy::result_large_err)]
    pub fn intent_parked(&self, op_id: &uuid::Uuid) -> StorageResult<bool> {
        let key = op_id.to_string();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(PENDING_INTENTS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(table
            .get(key.as_str())
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .is_some())
    }

    /// Drive the attached engine to the currently applied state — the
    /// cold-start / post-join reconcile (issue #9 slice 2). Apply only projects
    /// *new* entries onto the engine, so a restarted node must run this once to
    /// materialize what its tables already hold. A no-op without an engine;
    /// engine-side failures land in [`Self::apply_failures`] as usual.
    #[allow(clippy::result_large_err)]
    pub async fn reconcile_engine(&self) -> StorageResult<()> {
        // Both tables read fresh, in one call: a restart's local `ImposterManager`
        // and `ArcSwap<CompiledRoutes>` both start empty (they are process-local,
        // rebuilt from persisted `sm_configs`/`sm_routes`), and a live commit only
        // drives the table it touched — nothing else re-seeds the other on a
        // cold start.
        let (config_action, routes_action) = {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let configs = read_txn
                .open_table(SM_CONFIGS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let config_action = match Self::desired_configs(&configs)
                .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?
            {
                Ok(desired) => EngineAction::Sync(desired),
                Err((port, error)) => EngineAction::RefuseSync { port, error },
            };
            let routes = read_txn
                .open_table(SM_ROUTES_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let routes_action = match Self::desired_routes(&routes)
                .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?
            {
                Ok(table) => EngineAction::SyncRoutes(table),
                Err((id, error)) => EngineAction::RefuseRoutesSync { id, error },
            };
            (config_action, routes_action)
        };
        self.drive_engine(vec![config_action, routes_action]).await;
        Ok(())
    }

    /// Test-only: overwrite a raw `sm_configs` row, bypassing validation — the
    /// broken-record refusal path is unreachable through the public API.
    #[cfg(test)]
    fn inject_raw_config(&self, tenant: &str, port: u16, value: &str) {
        let txn = self.db.begin_write().expect("test txn");
        {
            let mut table = txn.open_table(SM_CONFIGS_TABLE).expect("test table");
            table.insert((tenant, port), value).expect("test insert");
        }
        txn.commit().expect("test commit");
    }

    /// Remove dedup entries whose TTL has passed relative to `now_secs` — the
    /// replicated logical clock in production, an injected value in tests.
    fn gc_dedup(
        table: &mut Table<'_, &'static str, &'static str>,
        now_secs: u64,
    ) -> Result<(), redb::StorageError> {
        table.retain(
            |op_id, value| match serde_json::from_str::<DedupEntry>(value) {
                Ok(entry) => entry.expires_at_secs > now_secs,
                Err(e) => {
                    // A dedup row that will not parse can only weaken replay
                    // collapse for its own op — dropping it is safe, but it is
                    // committed-state corruption and must not vanish silently.
                    tracing::error!(op_id, error = %e, "dropping unparseable sm_op_dedup entry");
                    false
                }
            },
        )
    }

    /// The desired engine state as of now, read from an open (possibly
    /// mid-transaction) view of `sm_configs`: every default-tenant config,
    /// parsed — disabled ones included (a paused imposter stays bound, #817).
    ///
    /// `Ok(Err((port, reason)))` means a stored record failed to parse. That
    /// must abort the sync, not shrink it: `apply_config` deletes every live
    /// imposter missing from the desired set, so silently skipping a broken
    /// record would tear down a healthy imposter and report it as an
    /// operator-issued delete. The caller refuses the sync and records the
    /// failure instead — the engine keeps serving its last-known state.
    fn desired_configs(
        table: &impl ReadableTable<(&'static str, u16), &'static str>,
    ) -> Result<Result<Vec<ImposterConfig>, (u16, String)>, redb::StorageError> {
        let mut desired = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let (tenant, port) = key.value();
            if tenant != DEFAULT_TENANT {
                continue;
            }
            let stored = match serde_json::from_str::<StoredImposter>(value.value()) {
                Ok(stored) => stored,
                Err(e) => {
                    return Ok(Err((port, format!("stored record will not parse: {e}"))));
                }
            };
            // Disabled configs stay in the desired set: upstream keeps a
            // paused imposter bound (serving 503) — dropping it here would
            // read as "delete it" to apply_config (#817).
            match serde_json::from_str::<ImposterConfig>(&stored.config_json) {
                Ok(config) => desired.push(config),
                Err(e) => {
                    return Ok(Err((port, format!("stored config will not parse: {e}"))));
                }
            }
        }
        Ok(Ok(desired))
    }

    /// Build the engine action for a config op: a full sync when every stored
    /// record parses, a recorded refusal when one does not.
    #[allow(clippy::result_large_err)]
    fn sync_action(
        configs: &Table<'_, (&'static str, u16), &'static str>,
    ) -> StorageResult<EngineAction> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        Ok(match Self::desired_configs(configs).map_err(io)? {
            Ok(desired) => EngineAction::Sync(desired),
            Err((port, error)) => EngineAction::RefuseSync { port, error },
        })
    }

    /// The desired route table as of now, read from an open (possibly
    /// mid-transaction) view of `sm_routes`: every default-tenant route.
    ///
    /// `Ok(Err((id, reason)))` means a stored record failed to parse — this
    /// crate is the only writer of `sm_routes`, so it should never happen in
    /// practice, but the read path stays defensive rather than trusting that
    /// (mirrors [`Self::desired_configs`]).
    fn desired_routes(
        table: &impl ReadableTable<(&'static str, &'static str), &'static str>,
    ) -> Result<Result<RouteTable, (String, String)>, redb::StorageError> {
        let mut routes = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let (tenant, id) = key.value();
            if tenant != DEFAULT_TENANT {
                continue;
            }
            match serde_json::from_str::<Route>(value.value()) {
                Ok(route) => routes.push(route),
                Err(e) => {
                    return Ok(Err((
                        id.to_owned(),
                        format!("stored route will not parse: {e}"),
                    )));
                }
            }
        }
        Ok(Ok(RouteTable { routes }))
    }

    /// Build the engine action for a route op: a full sync when every stored
    /// record parses, a recorded refusal when one does not.
    #[allow(clippy::result_large_err)]
    fn sync_routes_action(
        routes: &Table<'_, (&'static str, &'static str), &'static str>,
    ) -> StorageResult<EngineAction> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        Ok(match Self::desired_routes(routes).map_err(io)? {
            Ok(table) => EngineAction::SyncRoutes(table),
            Err((id, error)) => EngineAction::RefuseRoutesSync { id, error },
        })
    }

    /// Check op's `expected_revision` (#46) against the stored revision of the
    /// record it addresses. `Ok(Err(reason))` is a deterministic domain refusal
    /// — recorded as the same committed `Failed` outcome `validate` and
    /// `mutate_tables` refusals use — never a mutation; `Err(_)` is real
    /// storage I/O and fails apply.
    ///
    /// Every *precondition* reason starts with `"revision conflict"`: that
    /// prefix is the front's dispatch key to a 409, so it must never collide
    /// with a message from an unrelated refusal. The one exception is a
    /// corrupt stored record, which keeps `mutate_tables`' existing
    /// `"corrupt stored record"` shape (and its 400 mapping) — corruption is
    /// not a revision conflict and must not read as one.
    #[allow(clippy::result_large_err)]
    fn check_expected_revision(
        configs: &Table<'_, (&'static str, u16), &'static str>,
        op: &ControlOp,
        expected: u64,
    ) -> StorageResult<Result<(), String>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let Some((tenant, port)) = control::precondition_target(op) else {
            return Ok(Err(
                "revision conflict: expected-revision preconditions apply to single-imposter \
                 operations only"
                    .to_owned(),
            ));
        };
        match configs.get((tenant.as_str(), port)).map_err(io)? {
            None => Ok(Err(format!(
                "revision conflict: expected revision {expected} but no imposter on port {port}"
            ))),
            Some(guard) => match serde_json::from_str::<StoredImposter>(guard.value()) {
                Ok(record) if record.revision == expected => Ok(Ok(())),
                Ok(record) => Ok(Err(format!(
                    "revision conflict: expected revision {expected}, stored revision {actual} \
                     on port {port}",
                    actual = record.revision
                ))),
                Err(e) => {
                    tracing::error!(port, error = %e, "corrupt stored record");
                    Ok(Err(format!("corrupt stored record for port {port}: {e}")))
                }
            },
        }
    }

    /// Mutate `sm_configs` for one validated op and return the engine actions it
    /// implies. `Ok(Err(reason))` is a deterministic domain refusal (recorded as
    /// a `Failed` outcome); `Err(_)` is real storage I/O and fails apply.
    ///
    /// The metric gauges set here fire before the batch's commit; they are
    /// observe-only (never inputs to any decision), and an apply that fails
    /// after them is fatal to the node — the process-local registry dies with
    /// the process that briefly over-reported.
    #[allow(clippy::result_large_err)]
    fn mutate_tables(
        configs: &mut Table<'_, (&'static str, u16), &'static str>,
        routes: &mut Table<'_, (&'static str, &'static str), &'static str>,
        op: &ControlOp,
        index: u64,
    ) -> StorageResult<Result<Vec<EngineAction>, String>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        match op {
            ControlOp::PutImposter { tenant, config } => {
                // `validate` guaranteed the port; a missing one here means a
                // caller skipped validation, and a deterministic refusal is the
                // safe answer.
                let Some(port) = config.port else {
                    return Ok(Err("config must carry an explicit port".to_owned()));
                };
                let config_json = serde_json::to_string(config)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                let stored = StoredImposter {
                    config_json,
                    enabled: config.enabled,
                    revision: index,
                };
                let value = serde_json::to_string(&stored)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                configs
                    .insert((tenant.as_str(), port), value.as_str())
                    .map_err(io)?;
                crate::metrics::config_applied(port, index);
                Ok(Ok(vec![Self::sync_action(configs)?]))
            }
            ControlOp::PatchStubs { tenant, port, edit } => {
                // Block-scoped so the read guard's borrow of `configs` ends
                // before the insert below.
                let mut record: StoredImposter = {
                    match configs.get((tenant.as_str(), *port)).map_err(io)? {
                        None => return Ok(Err(format!("no imposter on port {port}"))),
                        Some(guard) => match serde_json::from_str(guard.value()) {
                            Ok(record) => record,
                            Err(e) => {
                                tracing::error!(port = *port, error = %e, "corrupt stored record");
                                return Ok(Err(format!(
                                    "corrupt stored record for port {port}: {e}"
                                )));
                            }
                        },
                    }
                };
                let mut config: ImposterConfig = match serde_json::from_str(&record.config_json) {
                    Ok(config) => config,
                    Err(e) => {
                        tracing::error!(port = *port, error = %e, "corrupt stored config");
                        return Ok(Err(format!("corrupt stored config for port {port}: {e}")));
                    }
                };
                if let Err(reason) = control::apply_edit(&mut config.stubs, edit) {
                    return Ok(Err(reason));
                }
                record.config_json = serde_json::to_string(&config)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                record.revision = index;
                let value = serde_json::to_string(&record)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                configs
                    .insert((tenant.as_str(), *port), value.as_str())
                    .map_err(io)?;
                crate::metrics::config_applied(*port, index);
                Ok(Ok(vec![EngineAction::Patch {
                    port: *port,
                    edit: edit.clone(),
                }]))
            }
            ControlOp::DeleteImposter { tenant, port } => {
                // Removing an absent port is a no-op, not a failure: deletes are
                // idempotent at the state-machine level (the admin-surface 404
                // for a missing imposter is the write path's concern).
                configs.remove((tenant.as_str(), *port)).map_err(io)?;
                crate::metrics::config_removed(*port);
                Ok(Ok(vec![Self::sync_action(configs)?]))
            }
            ControlOp::DeleteAll { tenant } => {
                let tenant = tenant.as_str();
                let removed: Vec<u16> = {
                    let mut removed = Vec::new();
                    for item in configs.iter().map_err(io)? {
                        let (key, _) = item.map_err(io)?;
                        let (t, port) = key.value();
                        if t == tenant {
                            removed.push(port);
                        }
                    }
                    removed
                };
                configs.retain(|(t, _), _| t != tenant).map_err(io)?;
                for port in removed {
                    crate::metrics::config_removed(port);
                }
                Ok(Ok(vec![Self::sync_action(configs)?]))
            }
            ControlOp::SetEnabled {
                tenant,
                port,
                enabled,
            } => {
                let mut record: StoredImposter = {
                    match configs.get((tenant.as_str(), *port)).map_err(io)? {
                        None => return Ok(Err(format!("no imposter on port {port}"))),
                        Some(guard) => match serde_json::from_str(guard.value()) {
                            Ok(record) => record,
                            Err(e) => {
                                tracing::error!(port = *port, error = %e, "corrupt stored record");
                                return Ok(Err(format!(
                                    "corrupt stored record for port {port}: {e}"
                                )));
                            }
                        },
                    }
                };
                // Both copies of the flag move together: the embedded config
                // is what the engine, snapshots and the desired-set builder
                // consume; the record field is a redundant projection kept in
                // sync so later slices can read it without a config parse.
                let mut config: ImposterConfig = match serde_json::from_str(&record.config_json) {
                    Ok(config) => config,
                    Err(e) => {
                        tracing::error!(port = *port, error = %e, "corrupt stored config");
                        return Ok(Err(format!("corrupt stored config for port {port}: {e}")));
                    }
                };
                record.enabled = *enabled;
                config.enabled = *enabled;
                record.config_json = serde_json::to_string(&config)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                record.revision = index;
                let value = serde_json::to_string(&record)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                configs
                    .insert((tenant.as_str(), *port), value.as_str())
                    .map_err(io)?;
                crate::metrics::config_applied(*port, index);
                Ok(Ok(vec![EngineAction::SetEnabled {
                    port: *port,
                    enabled: *enabled,
                }]))
            }
            ControlOp::PutRoutes { tenant, table } => {
                // Whole-table replace: clear this tenant's rows, then insert
                // the validated set. `validate` already confirmed the table
                // as a unit, so there is nothing left to check here — only to
                // store, deterministically, on every replica.
                let tenant_str = tenant.as_str();
                routes.retain(|(t, _), _| t != tenant_str).map_err(io)?;
                for route in &table.routes {
                    let value = serde_json::to_string(route)
                        .map_err(|e| StorageIOError::write_state_machine(&e))?;
                    routes
                        .insert((tenant_str, route.id.as_str()), value.as_str())
                        .map_err(io)?;
                }
                Ok(Ok(vec![Self::sync_routes_action(routes)?]))
            }
            ControlOp::DeleteRoute { tenant, id } => {
                // Idempotent no-op if absent, like `DeleteImposter` — the
                // admin-surface 404 for a missing route (if the operator
                // wants one) is the write path's concern, not apply's.
                routes.remove((tenant.as_str(), id.as_str())).map_err(io)?;
                Ok(Ok(vec![Self::sync_routes_action(routes)?]))
            }
            ControlOp::TenantPut { .. }
            | ControlOp::TenantDelete { .. }
            | ControlOp::PrincipalPut { .. }
            | ControlOp::PrincipalDelete { .. }
            | ControlOp::BindingPut { .. }
            | ControlOp::BindingDelete { .. } => Ok(Err("reserved op: RFC-002 (#17)".to_owned())),
        }
    }

    /// Project the applied state onto the local engine and the front door's
    /// compiled route table, in log order. Failures are recorded (per port for
    /// the engine; logged only for routes, which has no per-node bind state to
    /// track) and never propagate — see the module doc.
    ///
    /// The two projections are independent: a `SyncRoutes` action still swaps
    /// the `ArcSwap` on a node with no attached `engine` (a state machine
    /// wired for a routes-only test, or an embedder that has not attached an
    /// `ImposterManager`), and vice versa. Neither handle's absence gates the
    /// other's actions — unlike the pre-#131 shape, which could return early
    /// only because every action was engine-bound.
    async fn drive_engine(&self, actions: Vec<EngineAction>) {
        for action in actions {
            match action {
                EngineAction::Sync(desired) => {
                    let Some(engine) = &self.engine else { continue };
                    let desired_ports: std::collections::BTreeSet<u16> =
                        desired.iter().filter_map(|c| c.port).collect();
                    match engine.apply_config(desired).await {
                        Ok(report) => self.record_report(&report, &desired_ports),
                        Err(e) => {
                            tracing::error!(error = %e, "engine refused the applied config set");
                            self.apply_failures.lock().insert(0, e.to_string());
                        }
                    }
                }
                EngineAction::RefuseSync { port, error } => {
                    if self.engine.is_none() {
                        continue;
                    }
                    tracing::error!(
                        port,
                        error = %error,
                        "refusing engine sync: a stored record will not parse \
                         (a partial sync would delete live imposters)"
                    );
                    self.apply_failures.lock().insert(port, error);
                }
                EngineAction::SetEnabled { port, enabled } => {
                    let Some(engine) = &self.engine else { continue };
                    match engine.set_imposter_enabled(port, enabled).await {
                        Ok(()) => {
                            self.apply_failures.lock().remove(&port);
                        }
                        Err(e) => {
                            tracing::error!(port, error = %e, "engine refused a committed toggle");
                            self.apply_failures.lock().insert(port, e.to_string());
                        }
                    }
                }
                EngineAction::Patch { port, edit } => {
                    let Some(engine) = &self.engine else { continue };
                    match Self::drive_patch(engine, port, &edit).await {
                        Ok(()) => {
                            self.apply_failures.lock().remove(&port);
                        }
                        Err(e) => {
                            tracing::error!(
                                port,
                                error = %e,
                                "engine refused a committed stub edit"
                            );
                            self.apply_failures.lock().insert(port, e.to_string());
                        }
                    }
                }
                EngineAction::SyncRoutes(table) => {
                    if let Some(routes) = &self.routes {
                        routes.store(Arc::new(CompiledRoutes::new(&table)));
                    }
                }
                EngineAction::RefuseRoutesSync { id, error } => {
                    tracing::error!(
                        route_id = %id,
                        error = %error,
                        "refusing route-table sync: a stored record will not parse \
                         (the front door keeps its last-known-good table)"
                    );
                }
            }
        }
        if self.engine.is_some() {
            crate::metrics::observe_apply_failures(&self.apply_failures.lock());
        }
    }

    async fn drive_patch(
        engine: &ImposterManager,
        port: u16,
        edit: &StubEditScript,
    ) -> Result<(), ImposterError> {
        for step in &edit.0 {
            match step {
                StubEdit::Add { stub, index } => {
                    engine.add_stub(port, stub.clone(), *index).await?;
                }
                StubEdit::ReplaceById { id, stub } => {
                    engine.replace_stub_by_id(port, id, stub.clone()).await?;
                }
                StubEdit::DeleteById { id } => {
                    engine.delete_stub_by_id(port, id).await?;
                }
                StubEdit::Move { from, to } => {
                    engine.move_stub(port, *from, *to).await?;
                }
            }
        }
        Ok(())
    }

    /// Fold a successful sync's report into the failure map, under one lock:
    /// clear the ports it touched (and the set-level slot), drop entries for
    /// ports with no desired config — without that, a bind-failed port that is
    /// later deleted keeps its stale entry forever (the engine never had it, so
    /// no report bucket names it) — then record the ports that failed.
    fn record_report(&self, report: &ApplyReport, desired_ports: &std::collections::BTreeSet<u16>) {
        let mut failures = self.apply_failures.lock();
        for port in report
            .created
            .iter()
            .chain(&report.replaced)
            .chain(&report.stub_patched)
            .chain(&report.toggled)
            .chain(&report.deleted)
        {
            failures.remove(port);
        }
        failures.retain(|port, _| desired_ports.contains(port));
        for (port, error) in &report.failed {
            failures.insert(*port, error.to_string());
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RedbStateMachine {
    async fn build_snapshot(&mut self) -> StorageResult<Snapshot<TypeConfig>> {
        let applied = self.read_applied()?;

        let (configs, routes, dedup) = {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let configs_table = read_txn
                .open_table(SM_CONFIGS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut configs = Vec::new();
            for item in configs_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let (tenant, port) = key.value();
                configs.push((tenant.to_owned(), port, value.value().to_owned()));
            }
            let routes_table = read_txn
                .open_table(SM_ROUTES_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut routes = Vec::new();
            for item in routes_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let (tenant, id) = key.value();
                routes.push((tenant.to_owned(), id.to_owned(), value.value().to_owned()));
            }
            let dedup_table = read_txn
                .open_table(SM_DEDUP_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut dedup = Vec::new();
            for item in dedup_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                dedup.push((key.value().to_owned(), value.value().to_owned()));
            }
            (configs, routes, dedup)
        };

        let payload = SnapshotPayload {
            configs,
            routes,
            dedup,
            last_applied_log: applied.last_applied_log,
            last_membership: applied.last_membership.clone(),
            logical_clock_secs: applied.logical_clock_secs,
        };
        let data =
            serde_json::to_vec(&payload).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = match applied.last_applied_log {
            Some(last) => format!("{}-{}-{snapshot_idx}", last.leader_id, last.index),
            None => format!("--{snapshot_idx}"),
        };

        let meta = SnapshotMeta {
            last_log_id: applied.last_applied_log,
            last_membership: applied.last_membership,
            snapshot_id,
        };

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        let bytes = serde_json::to_vec(&stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        {
            let mut table = write_txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            table
                .insert((), bytes.as_slice())
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        }
        write_txn
            .commit()
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for RedbStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> StorageResult<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>)> {
        let applied = self.read_applied()?;
        Ok((applied.last_applied_log, applied.last_membership))
    }

    async fn apply<I>(&mut self, entries: I) -> StorageResult<Vec<ControlResponse>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut applied = self.read_applied()?;

        let entries_iter = entries.into_iter();
        let mut responses = Vec::with_capacity(entries_iter.size_hint().0);
        let mut engine_actions = Vec::new();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        {
            let mut configs = write_txn
                .open_table(SM_CONFIGS_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut routes = write_txn
                .open_table(SM_ROUTES_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut dedup = write_txn
                .open_table(SM_DEDUP_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            // GC against the *replicated* logical clock (see `AppliedState`),
            // so every replica drops exactly the same entries at the same log
            // point — a local clock here would let a TTL-boundary replay
            // re-apply on one replica and collapse on another.
            Self::gc_dedup(&mut dedup, applied.logical_clock_secs)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;

            for entry in entries_iter {
                let log_id = entry.log_id;
                applied.last_applied_log = Some(log_id);

                match entry.payload {
                    EntryPayload::Blank => {
                        responses.push(ControlResponse::applied(log_id.index));
                    }
                    EntryPayload::Normal(request) => {
                        applied.logical_clock_secs =
                            applied.logical_clock_secs.max(request.issued_at_secs);
                        let op_key = request.op_id.to_string();
                        let previous = dedup
                            .get(op_key.as_str())
                            .map_err(|e| StorageIOError::write_state_machine(&e))?
                            .map(|g| serde_json::from_str::<DedupEntry>(g.value()))
                            .transpose()
                            .map_err(|e| StorageIOError::write_state_machine(&e))?
                            // An entry that expired mid-batch (the sweep above
                            // ran on the pre-batch clock) is not a replay hit.
                            .filter(|prev| prev.expires_at_secs > applied.logical_clock_secs);
                        if let Some(previous) = previous {
                            // Replayed intent: return the original response —
                            // same revision both times — and change nothing.
                            crate::metrics::dedup_hit();
                            responses.push(previous.response);
                            continue;
                        }

                        let outcome = match control::validate(&request.op) {
                            Err(reason) => Err(reason),
                            Ok(()) => match request.expected_revision {
                                Some(expected) => match Self::check_expected_revision(
                                    &configs,
                                    &request.op,
                                    expected,
                                )? {
                                    Err(reason) => Err(reason),
                                    Ok(()) => Self::mutate_tables(
                                        &mut configs,
                                        &mut routes,
                                        &request.op,
                                        log_id.index,
                                    )?,
                                },
                                None => Self::mutate_tables(
                                    &mut configs,
                                    &mut routes,
                                    &request.op,
                                    log_id.index,
                                )?,
                            },
                        };
                        let response = match outcome {
                            Ok(actions) => {
                                engine_actions.extend(actions);
                                ControlResponse::applied(log_id.index)
                            }
                            Err(reason) => ControlResponse::failed(log_id.index, reason),
                        };

                        let dedup_entry = DedupEntry {
                            // Stored copy: the same response must come back for
                            // any replay of this op_id.
                            response: response.clone(),
                            expires_at_secs: applied
                                .logical_clock_secs
                                .saturating_add(DEDUP_TTL_SECS),
                        };
                        let value = serde_json::to_string(&dedup_entry)
                            .map_err(|e| StorageIOError::write_state_machine(&e))?;
                        dedup
                            .insert(op_key.as_str(), value.as_str())
                            .map_err(|e| StorageIOError::write_state_machine(&e))?;
                        responses.push(response);
                    }
                    EntryPayload::Membership(membership) => {
                        applied.last_membership = StoredMembership::new(Some(log_id), membership);
                        responses.push(ControlResponse::applied(log_id.index));
                    }
                }
            }
        }
        {
            let mut applied_table = write_txn
                .open_table(SM_APPLIED_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let bytes = serde_json::to_vec(&applied)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            applied_table
                .insert((), bytes.as_slice())
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
        }
        write_txn
            .commit()
            .map_err(|e| StorageIOError::write_state_machine(&e))?;

        self.drive_engine(engine_actions).await;

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StorageResult<()> {
        let data = snapshot.into_inner();
        let payload: SnapshotPayload = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        let bytes = serde_json::to_vec(&stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        let (config_action, routes_action) = {
            let mut snap_table = write_txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            snap_table
                .insert((), bytes.as_slice())
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

            let mut configs_table = write_txn
                .open_table(SM_CONFIGS_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            configs_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (tenant, port, value) in &payload.configs {
                configs_table
                    .insert((tenant.as_str(), *port), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }
            let config_action = match Self::desired_configs(&configs_table)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?
            {
                Ok(desired) => EngineAction::Sync(desired),
                Err((port, error)) => EngineAction::RefuseSync { port, error },
            };

            let mut routes_table = write_txn
                .open_table(SM_ROUTES_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            routes_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (tenant, id, value) in &payload.routes {
                routes_table
                    .insert((tenant.as_str(), id.as_str()), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }
            let routes_action = match Self::desired_routes(&routes_table)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?
            {
                Ok(table) => EngineAction::SyncRoutes(table),
                Err((id, error)) => EngineAction::RefuseRoutesSync { id, error },
            };

            let mut dedup_table = write_txn
                .open_table(SM_DEDUP_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            dedup_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (op_id, value) in &payload.dedup {
                dedup_table
                    .insert(op_id.as_str(), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            let applied = AppliedState {
                last_applied_log: payload.last_applied_log,
                last_membership: payload.last_membership,
                logical_clock_secs: payload.logical_clock_secs,
            };
            let applied_bytes = serde_json::to_vec(&applied)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            let mut applied_table = write_txn
                .open_table(SM_APPLIED_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            applied_table
                .insert((), applied_bytes.as_slice())
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            (config_action, routes_action)
        };
        write_txn
            .commit()
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        // A snapshot replaces the whole applied state, so the engine and the
        // front door's compiled table both converge on it the same way apply
        // does — after the durable write, best-effort.
        self.drive_engine(vec![config_action, routes_action]).await;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<TypeConfig>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?;
        let table = read_txn
            .open_table(SNAPSHOT_TABLE)
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?;
        let bytes = match table
            .get(())
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?
        {
            Some(guard) => guard.value().to_vec(),
            None => return Ok(None),
        };
        let stored: StoredSnapshot =
            serde_json::from_slice(&bytes).map_err(|e| StorageIOError::read_snapshot(None, &e))?;
        Ok(Some(Snapshot {
            meta: stored.meta,
            snapshot: Box::new(Cursor::new(stored.data)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use openraft::storage::{RaftStateMachine, Snapshot};
    use openraft::testing::{StoreBuilder, Suite};
    use openraft::{
        CommittedLeaderId, Entry, EntryPayload, LogId, RaftSnapshotBuilder, StorageError,
    };
    use redb::ReadableTable;
    use rift_ee::seams::{
        CompiledRoutes, ImposterConfig, ImposterManager, Route, RouteMatch, RouteTable, RouteTarget,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::{DEDUP_TTL_SECS, DedupEntry, RedbLogStore, RedbStateMachine, SM_DEDUP_TABLE, new};
    use crate::control::{
        ControlOp, ControlOutcome, ControlRequest, ControlResponse, StubEdit, StubEditScript,
        TenantId,
    };
    use crate::raft::TypeConfig;

    struct RedbBuilder;

    impl StoreBuilder<TypeConfig, RedbLogStore, RedbStateMachine, TempDir> for RedbBuilder {
        async fn build(
            &self,
        ) -> Result<(TempDir, RedbLogStore, RedbStateMachine), StorageError<u64>> {
            let td = TempDir::new().expect("create temp dir for redb store");
            let (log_store, sm) = new(td.path().join("raft.redb")).await?;
            Ok((td, log_store, sm))
        }
    }

    /// The acceptance gate for ADR-001 milestone 1: this is openraft's own storage
    /// conformance suite, not a hand-rolled smoke test. It runs ~35 scenarios
    /// covering vote persistence, log append/truncate/purge, membership recovery,
    /// state-machine apply, and snapshot build/transfer/install against a real
    /// `redb`-backed store.
    // `StorageError<u64>` is openraft's own error type (it carries an `AnyError` plus
    // a backtrace slot) — its size isn't ours to shrink, and `Suite::test_all`'s `?`
    // propagates it directly.
    #[allow(clippy::result_large_err)]
    #[test]
    fn redb_storage_passes_openraft_suite() -> Result<(), StorageError<u64>> {
        Suite::test_all(RedbBuilder)?;
        Ok(())
    }

    // -- issue #9 state-machine gate ------------------------------------------

    async fn fresh_sm(engine: Option<Arc<ImposterManager>>) -> (TempDir, RedbStateMachine) {
        let td = TempDir::new().expect("tempdir");
        let (_, sm) = new(td.path().join("raft.redb")).await.expect("open store");
        let sm = match engine {
            Some(engine) => sm.with_engine(engine),
            None => sm,
        };
        (td, sm)
    }

    /// Like [`fresh_sm`], with a routes handle attached instead of an engine —
    /// the routes-only tests never need an `ImposterManager`.
    async fn fresh_sm_with_routes() -> (TempDir, RedbStateMachine, Arc<ArcSwap<CompiledRoutes>>) {
        let td = TempDir::new().expect("tempdir");
        let (_, sm) = new(td.path().join("raft.redb")).await.expect("open store");
        let routes = Arc::new(ArcSwap::from_pointee(CompiledRoutes::default()));
        let sm = sm.with_routes_handle(Arc::clone(&routes));
        (td, sm, routes)
    }

    /// A route matching `/<id>`, never the default catch-all: two of these
    /// with different ids never collide with `RouteTable::validate`'s
    /// ambiguity check, so tests can freely build multi-route tables without
    /// every `PutRoutes` needing its own bespoke `RouteMatch`.
    fn test_route(id: &str, port: u16) -> Route {
        Route {
            id: id.to_owned(),
            priority: 0,
            matches: RouteMatch {
                path_prefix: Some(format!("/{id}")),
                ..RouteMatch::default()
            },
            target: RouteTarget {
                port,
                strip_prefix: false,
                set_host: None,
            },
            enabled: true,
        }
    }

    fn put_routes(op_id: u128, routes: Vec<Route>) -> ControlRequest {
        request(
            op_id,
            ControlOp::PutRoutes {
                tenant: TenantId::default(),
                table: RouteTable { routes },
            },
        )
    }

    fn entry(index: u64, request: ControlRequest) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(request),
        }
    }

    fn request(op_id: u128, op: ControlOp) -> ControlRequest {
        request_at(op_id, 0, op)
    }

    fn request_at(op_id: u128, issued_at_secs: u64, op: ControlOp) -> ControlRequest {
        ControlRequest {
            op_id: Uuid::from_u128(op_id),
            principal: None,
            issued_at_secs,
            expected_revision: None,
            op,
        }
    }

    fn config(port: u16, stubs: serde_json::Value) -> ImposterConfig {
        serde_json::from_value(json!({
            "port": port,
            "protocol": "http",
            "host": "127.0.0.1",
            "stubs": stubs,
        }))
        .expect("test config parses")
    }

    fn put(op_id: u128, port: u16, stubs: serde_json::Value) -> ControlRequest {
        request(
            op_id,
            ControlOp::PutImposter {
                tenant: TenantId::default(),
                config: Box::new(config(port, stubs)),
            },
        )
    }

    fn stored_stub_ids(sm: &RedbStateMachine, port: u16) -> Vec<String> {
        let body = sm
            .read_config(port)
            .expect("read config")
            .expect("config present");
        let config: serde_json::Value = serde_json::from_str(&body).expect("parses");
        config["stubs"]
            .as_array()
            .expect("stubs array")
            .iter()
            .map(|s| s["id"].as_str().expect("test stubs carry ids").to_owned())
            .collect()
    }

    fn engine_stub_ids(engine: &ImposterManager, port: u16) -> Vec<String> {
        engine
            .get_imposter(port)
            .expect("imposter exists")
            .get_stubs()
            .iter()
            .map(|s| s.id.clone().expect("test stubs carry ids"))
            .collect()
    }

    #[tokio::test]
    async fn apply_put_records_config_and_revision() {
        let (_td, mut sm) = fresh_sm(None).await;
        let responses = sm
            .apply(vec![entry(5, put(1, 8080, json!([])))])
            .await
            .expect("apply");
        assert_eq!(responses, vec![ControlResponse::applied(5)]);
        let body = sm.read_config(8080).expect("read").expect("present");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("parses");
        assert_eq!(parsed["port"], 8080);
        assert_eq!(sm.configured_ports().expect("ports"), vec![8080]);
    }

    /// A validation refusal is a committed, deterministic outcome: the response
    /// says `failed`, the tables are untouched, and a second node applying the
    /// same entry computes the identical response.
    #[tokio::test]
    async fn validation_failure_is_a_deterministic_no_op() {
        let bad = |op_id: u128| {
            request(
                op_id,
                ControlOp::PutImposter {
                    tenant: TenantId::default(),
                    config: serde_json::from_value(json!({ "port": 1, "protocol": "smtp" }))
                        .expect("parses"),
                },
            )
        };
        let (_td, mut sm) = fresh_sm(None).await;
        let (_td2, mut sm2) = fresh_sm(None).await;
        let responses = sm.apply(vec![entry(3, bad(1))]).await.expect("apply");
        let responses2 = sm2.apply(vec![entry(3, bad(1))]).await.expect("apply");
        assert_eq!(responses, responses2, "replicas must agree");
        match &responses[0].outcome {
            ControlOutcome::Failed { reason } => {
                assert!(reason.contains("protocol"), "{reason}");
            }
            other => panic!("expected failed outcome, got {other:?}"),
        }
        assert_eq!(sm.read_config(1).expect("read"), None, "nothing mutated");
    }

    /// Same `op_id` twice — the crash-replay / same-`Idempotency-Key` case —
    /// applies once and returns the original revision both times.
    #[tokio::test]
    async fn dedup_collapses_a_replayed_op_to_the_original_response() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, put(1, 8080, json!([])))])
            .await
            .expect("apply put");

        let add = |op_id: u128| {
            request(
                op_id,
                ControlOp::PatchStubs {
                    tenant: TenantId::default(),
                    port: 8080,
                    edit: StubEditScript(vec![StubEdit::Add {
                        stub: serde_json::from_value(json!({ "id": "a" })).expect("parses"),
                        index: None,
                    }]),
                },
            )
        };
        let first = sm.apply(vec![entry(2, add(7))]).await.expect("apply");
        assert_eq!(first, vec![ControlResponse::applied(2)]);

        let replay = sm.apply(vec![entry(3, add(7))]).await.expect("replay");
        assert_eq!(
            replay,
            vec![ControlResponse::applied(2)],
            "the replay must return the ORIGINAL revision, not its own index"
        );
        assert_eq!(
            stored_stub_ids(&sm, 8080),
            vec!["a"],
            "the edit must have applied exactly once"
        );

        // A different op_id is a new op, not a replay: this one really runs —
        // and deterministically fails, because id "a" already exists.
        let fresh = sm.apply(vec![entry(4, add(8))]).await.expect("apply");
        assert_eq!(fresh[0].revision, 4);
        assert!(
            matches!(&fresh[0].outcome, ControlOutcome::Failed { .. }),
            "adding a duplicate id must fail deterministically: {fresh:?}"
        );
        assert_eq!(stored_stub_ids(&sm, 8080), vec!["a"]);
    }

    #[test]
    fn dedup_gc_drops_only_expired_entries() {
        let td = TempDir::new().expect("tempdir");
        let db = redb::Database::create(td.path().join("gc.redb")).expect("create");
        let txn = db.begin_write().expect("txn");
        {
            let mut table = txn.open_table(SM_DEDUP_TABLE).expect("table");
            for (op, expires) in [("old", 100_u64), ("live", 100 + DEDUP_TTL_SECS)] {
                let entry = DedupEntry {
                    response: ControlResponse::applied(1),
                    expires_at_secs: expires,
                };
                let value = serde_json::to_string(&entry).expect("serialize");
                table.insert(op, value.as_str()).expect("insert");
            }
            RedbStateMachine::gc_dedup(&mut table, 101).expect("gc");
            assert!(table.get("old").expect("get").is_none(), "expired: dropped");
            assert!(table.get("live").expect("get").is_some(), "live: retained");
        }
        txn.commit().expect("commit");
    }

    #[tokio::test]
    async fn put_drives_the_engine_and_preserves_siblings() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(engine.clone())).await;

        sm.apply(vec![
            entry(1, put(1, 18081, json!([{ "id": "a" }]))),
            entry(2, put(2, 18082, json!([]))),
        ])
        .await
        .expect("apply");
        assert_eq!(engine.count(), 2, "both imposters live in the engine");

        // A sibling-port change must leave 18081 untouched (the #316 contract:
        // identical config → not recreated).
        sm.apply(vec![entry(3, put(3, 18082, json!([{ "id": "b" }])))])
            .await
            .expect("apply");
        assert_eq!(engine.count(), 2);
        assert_eq!(engine_stub_ids(&engine, 18081), vec!["a"]);
        assert_eq!(engine_stub_ids(&engine, 18082), vec!["b"]);
        assert!(
            sm.apply_failures().is_empty(),
            "healthy applies record no failures: {:?}",
            sm.apply_failures()
        );

        engine.shutdown().await;
    }

    /// The core infallibility clause: a port that cannot bind fails the *engine
    /// drive*, never the apply. The config is committed, the response is
    /// `applied`, and the failure is node status.
    #[tokio::test]
    async fn bind_failure_does_not_fail_apply() {
        let blocker = std::net::TcpListener::bind("127.0.0.1:0").expect("bind blocker");
        let port = blocker.local_addr().expect("addr").port();

        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(engine.clone())).await;
        let responses = sm
            .apply(vec![entry(1, put(1, port, json!([])))])
            .await
            .expect("apply must not fail on a bind failure");
        assert_eq!(responses, vec![ControlResponse::applied(1)]);
        assert!(
            sm.read_config(port).expect("read").is_some(),
            "the committed config is in the tables regardless"
        );
        let failures = sm.apply_failures();
        assert!(
            failures.contains_key(&port),
            "the bind failure must be recorded as node status, got {failures:?}"
        );

        // The engine heals once the port frees up: a later committed write
        // clears the recorded failure.
        drop(blocker);
        sm.apply(vec![entry(2, put(2, port, json!([{ "id": "a" }])))])
            .await
            .expect("apply");
        assert!(
            !sm.apply_failures().contains_key(&port),
            "a successful drive clears the failure: {:?}",
            sm.apply_failures()
        );

        engine.shutdown().await;
    }

    #[tokio::test]
    async fn patch_reorders_stubs_in_engine_and_stored_config() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(engine.clone())).await;
        sm.apply(vec![entry(
            1,
            put(
                1,
                18083,
                json!([{ "id": "a" }, { "id": "b" }, { "id": "c" }]),
            ),
        )])
        .await
        .expect("apply put");

        let patch = request(
            2,
            ControlOp::PatchStubs {
                tenant: TenantId::default(),
                port: 18083,
                edit: StubEditScript(vec![StubEdit::Move { from: 2, to: 0 }]),
            },
        );
        let responses = sm.apply(vec![entry(2, patch)]).await.expect("apply patch");
        assert_eq!(responses, vec![ControlResponse::applied(2)]);
        assert_eq!(stored_stub_ids(&sm, 18083), vec!["c", "a", "b"]);
        assert_eq!(engine_stub_ids(&engine, 18083), vec!["c", "a", "b"]);

        engine.shutdown().await;
    }

    #[tokio::test]
    async fn deletes_reconcile_the_engine() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(engine.clone())).await;
        sm.apply(vec![
            entry(1, put(1, 18084, json!([]))),
            entry(2, put(2, 18085, json!([]))),
        ])
        .await
        .expect("apply puts");
        assert_eq!(engine.count(), 2);

        sm.apply(vec![entry(
            3,
            request(
                3,
                ControlOp::DeleteImposter {
                    tenant: TenantId::default(),
                    port: 18084,
                },
            ),
        )])
        .await
        .expect("apply delete");
        assert_eq!(engine.count(), 1);
        assert_eq!(sm.configured_ports().expect("ports"), vec![18085]);

        sm.apply(vec![entry(
            4,
            request(
                4,
                ControlOp::DeleteAll {
                    tenant: TenantId::default(),
                },
            ),
        )])
        .await
        .expect("apply delete-all");
        assert_eq!(engine.count(), 0);
        assert!(sm.configured_ports().expect("ports").is_empty());

        engine.shutdown().await;
    }

    /// Snapshot round-trip carries BOTH tables: a follower installed from
    /// snapshot serves the configs and still collapses a replayed op_id.
    #[tokio::test]
    async fn snapshot_carries_configs_and_dedup_state() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, put(9, 8080, json!([{ "id": "a" }])))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");
        assert!(
            follower.read_config(8080).expect("read").is_some(),
            "installed snapshot serves the config"
        );

        let replay = follower
            .apply(vec![entry(10, put(9, 8080, json!([{ "id": "a" }])))])
            .await
            .expect("replay after install");
        assert_eq!(
            replay,
            vec![ControlResponse::applied(1)],
            "dedup state survived the snapshot: the replay returns the original revision"
        );
    }

    /// A `Failed` outcome is committed state like any other: a replay of the
    /// same op_id must return the identical failure, not re-run validation.
    /// (Also covers PatchStubs on an absent port ⇒ deterministic `Failed`.)
    #[tokio::test]
    async fn a_failed_outcome_is_deduped_like_any_other() {
        let (_td, mut sm) = fresh_sm(None).await;
        let patch = |op_id: u128| {
            request(
                op_id,
                ControlOp::PatchStubs {
                    tenant: TenantId::default(),
                    port: 4444,
                    edit: StubEditScript(vec![StubEdit::Add {
                        stub: serde_json::from_value(json!({ "id": "a" })).expect("parses"),
                        index: None,
                    }]),
                },
            )
        };
        let first = sm.apply(vec![entry(1, patch(7))]).await.expect("apply");
        match &first[0].outcome {
            ControlOutcome::Failed { reason } => assert!(reason.contains("4444"), "{reason}"),
            other => panic!("patching an absent port must fail, got {other:?}"),
        }
        let replay = sm.apply(vec![entry(2, patch(7))]).await.expect("replay");
        assert_eq!(
            replay, first,
            "a Failed outcome must dedup to the identical response and revision"
        );
    }

    #[tokio::test]
    async fn deleting_an_absent_port_is_applied_not_failed() {
        let (_td, mut sm) = fresh_sm(None).await;
        let responses = sm
            .apply(vec![entry(
                1,
                request(
                    1,
                    ControlOp::DeleteImposter {
                        tenant: TenantId::default(),
                        port: 5555,
                    },
                ),
            )])
            .await
            .expect("apply");
        assert_eq!(
            responses,
            vec![ControlResponse::applied(1)],
            "deletes are idempotent at the state-machine level"
        );
    }

    /// A follower that catches up via snapshot must materialize the configs in
    /// its local engine, not just its tables.
    #[tokio::test]
    async fn install_snapshot_drives_an_attached_engine() {
        let (_td, mut leader_sm) = fresh_sm(None).await;
        leader_sm
            .apply(vec![entry(1, put(1, 18086, json!([{ "id": "a" }])))])
            .await
            .expect("apply");
        let mut builder = leader_sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let engine = Arc::new(ImposterManager::new());
        let (_td2, mut follower) = fresh_sm(Some(engine.clone())).await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");
        assert_eq!(engine.count(), 1, "the snapshot's configs must be bound");
        assert_eq!(engine_stub_ids(&engine, 18086), vec!["a"]);

        engine.shutdown().await;
    }

    /// GC runs on the replicated logical clock carried by `issued_at_secs`:
    /// once later entries advance it past an entry's TTL, a replay of that
    /// op_id re-applies — identically on every replica.
    #[tokio::test]
    async fn gc_through_apply_expires_via_the_logical_clock() {
        let (_td, mut sm) = fresh_sm(None).await;
        let put_at = |op_id: u128, issued: u64| {
            request_at(
                op_id,
                issued,
                ControlOp::PutImposter {
                    tenant: TenantId::default(),
                    config: Box::new(config(8080, json!([]))),
                },
            )
        };
        sm.apply(vec![entry(1, put_at(1, 1_000))])
            .await
            .expect("apply");

        // Advance the logical clock past op 1's TTL with an unrelated op.
        sm.apply(vec![entry(
            2,
            request_at(
                2,
                1_000 + DEDUP_TTL_SECS + 1,
                ControlOp::DeleteImposter {
                    tenant: TenantId::default(),
                    port: 9999,
                },
            ),
        )])
        .await
        .expect("apply");

        // The next batch's sweep GCs op 1's entry, so its replay re-applies.
        let replay = sm
            .apply(vec![entry(3, put_at(1, 1_000 + DEDUP_TTL_SECS + 1))])
            .await
            .expect("replay");
        assert_eq!(
            replay,
            vec![ControlResponse::applied(3)],
            "an expired dedup entry no longer collapses the replay"
        );
    }

    /// One unparseable stored record must refuse the whole engine sync — a
    /// partial desired set would read as "delete the missing imposters".
    #[tokio::test]
    async fn a_broken_stored_record_refuses_sync_instead_of_deleting() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(engine.clone())).await;
        sm.apply(vec![entry(1, put(1, 18087, json!([{ "id": "a" }])))])
            .await
            .expect("apply");
        assert_eq!(engine.count(), 1);

        sm.inject_raw_config(crate::control::DEFAULT_TENANT, 18088, "not json");

        let responses = sm
            .apply(vec![entry(2, put(2, 18089, json!([])))])
            .await
            .expect("apply still succeeds — the refusal is engine status");
        assert_eq!(responses, vec![ControlResponse::applied(2)]);
        assert_eq!(
            engine.count(),
            1,
            "the live imposter must NOT be torn down, and the new one must not \
             be created by a partial sync"
        );
        assert_eq!(engine_stub_ids(&engine, 18087), vec!["a"]);
        assert!(
            sm.apply_failures().contains_key(&18088),
            "the broken record is surfaced as node status: {:?}",
            sm.apply_failures()
        );

        engine.shutdown().await;
    }

    /// Issue #9 slice 3: the node-local intent ledger — park, report, retire.
    #[tokio::test]
    async fn intents_park_retire_and_report() {
        let (_td, mut sm) = fresh_sm(None).await;
        let request = put(0xB0B, 8080, json!([]));
        let op_id = request.op_id;
        sm.park_intent(&request).expect("park");
        assert!(sm.intent_parked(&op_id).expect("parked"));
        assert_eq!(sm.parked_intents().expect("list").len(), 1);
        assert!(
            sm.read_op(&op_id).expect("read").is_none(),
            "accepted but not applied: no recorded outcome yet"
        );

        sm.apply(vec![entry(1, request.clone())])
            .await
            .expect("apply");
        assert_eq!(
            sm.read_op(&op_id).expect("read"),
            Some(ControlResponse::applied(1)),
            "the ops surface reads the dedup record"
        );

        sm.unpark_intent(&op_id).expect("unpark");
        assert!(!sm.intent_parked(&op_id).expect("parked"));
        assert!(sm.parked_intents().expect("list").is_empty());
        sm.unpark_intent(&op_id)
            .expect("unparking twice is a no-op");
    }

    /// Issue #9 slice 4 / #15: SetEnabled toggles in place — the imposter is
    /// never recreated, so its runtime state survives a pause/resume cycle.
    #[tokio::test]
    async fn set_enabled_toggles_in_place() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(engine.clone())).await;
        sm.apply(vec![entry(1, put(1, 18090, json!([{ "id": "a" }])))])
            .await
            .expect("apply put");
        let before = engine.get_imposter(18090).expect("bound");
        assert!(before.is_enabled());

        let disable = request(
            2,
            ControlOp::SetEnabled {
                tenant: TenantId::default(),
                port: 18090,
                enabled: false,
            },
        );
        let responses = sm.apply(vec![entry(2, disable)]).await.expect("apply");
        assert_eq!(responses, vec![ControlResponse::applied(2)]);

        let body = sm.read_config(18090).expect("read").expect("present");
        assert!(
            body.contains("\"enabled\":false"),
            "the stored config carries the flag: {body}"
        );
        let after = engine.get_imposter(18090).expect("still bound");
        assert!(
            Arc::ptr_eq(&before, &after),
            "the toggle must not recreate the imposter"
        );
        assert!(!after.is_enabled());

        // A toggle on an absent port is a deterministic refusal.
        let ghost = request(
            3,
            ControlOp::SetEnabled {
                tenant: TenantId::default(),
                port: 19999,
                enabled: false,
            },
        );
        let responses = sm.apply(vec![entry(3, ghost)]).await.expect("apply");
        assert!(
            matches!(&responses[0].outcome, ControlOutcome::Failed { reason } if reason.contains("19999")),
            "{responses:?}"
        );

        engine.shutdown().await;
    }

    /// A paused config must STAY in the engine's desired set: dropping it
    /// would read as "delete the imposter" to apply_config — a pause is not a
    /// teardown (#817).
    #[tokio::test]
    async fn a_disabled_config_stays_bound_through_sibling_syncs() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(engine.clone())).await;
        sm.apply(vec![entry(1, put(1, 18091, json!([{ "id": "a" }])))])
            .await
            .expect("apply put");
        sm.apply(vec![entry(
            2,
            request(
                2,
                ControlOp::SetEnabled {
                    tenant: TenantId::default(),
                    port: 18091,
                    enabled: false,
                },
            ),
        )])
        .await
        .expect("apply disable");

        // A sibling create triggers a full-set sync; the paused imposter must
        // survive it, still bound and still paused.
        sm.apply(vec![entry(3, put(3, 18092, json!([])))])
            .await
            .expect("apply sibling");
        assert_eq!(engine.count(), 2, "the paused imposter was not torn down");
        assert!(!engine.get_imposter(18091).expect("bound").is_enabled());

        engine.shutdown().await;
    }

    /// A bind-failed port that is then deleted must not leave a phantom entry
    /// in `apply_failures` — the port has no config to fail against anymore.
    #[tokio::test]
    async fn deleting_a_bind_failed_port_clears_its_failure() {
        let blocker = std::net::TcpListener::bind("127.0.0.1:0").expect("bind blocker");
        let port = blocker.local_addr().expect("addr").port();

        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(engine.clone())).await;
        sm.apply(vec![entry(1, put(1, port, json!([])))])
            .await
            .expect("apply");
        assert!(sm.apply_failures().contains_key(&port));

        sm.apply(vec![entry(
            2,
            request(
                2,
                ControlOp::DeleteImposter {
                    tenant: TenantId::default(),
                    port,
                },
            ),
        )])
        .await
        .expect("apply delete");
        assert!(
            !sm.apply_failures().contains_key(&port),
            "a deleted port cannot keep a live failure: {:?}",
            sm.apply_failures()
        );

        engine.shutdown().await;
    }
    /// #46 gate: the expected-revision precondition is checked inside apply,
    /// so every replica computes the identical refusal from the same entry.
    #[tokio::test]
    async fn expected_revision_gates_apply_deterministically() {
        let conditioned = |op_id: u128, expected: u64, op: ControlOp| ControlRequest {
            expected_revision: Some(expected),
            ..request(op_id, op)
        };
        let add = |id: &str| ControlOp::PatchStubs {
            tenant: TenantId::default(),
            port: 8080,
            edit: StubEditScript(vec![StubEdit::Add {
                stub: serde_json::from_value(json!({ "id": id })).expect("parses"),
                index: None,
            }]),
        };

        let (_td, mut sm) = fresh_sm(None).await;
        let (_td2, mut sm2) = fresh_sm(None).await;
        for s in [&mut sm, &mut sm2] {
            s.apply(vec![entry(1, put(1, 8080, json!([])))])
                .await
                .expect("put");
        }

        // A matching expectation applies: the record's revision is 1 (the put).
        let first = sm
            .apply(vec![entry(2, conditioned(2, 1, add("a")))])
            .await
            .expect("apply");
        let second = sm2
            .apply(vec![entry(2, conditioned(2, 1, add("a")))])
            .await
            .expect("apply");
        assert_eq!(first, second, "replicas must agree");
        assert_eq!(first, vec![ControlResponse::applied(2)]);

        // A stale expectation (record is now at revision 2) refuses — same
        // committed Failed on both replicas, tables untouched.
        let refused = sm
            .apply(vec![entry(3, conditioned(3, 1, add("b")))])
            .await
            .expect("apply");
        let refused2 = sm2
            .apply(vec![entry(3, conditioned(3, 1, add("b")))])
            .await
            .expect("apply");
        assert_eq!(refused, refused2, "replicas must agree");
        match &refused[0].outcome {
            ControlOutcome::Failed { reason } => {
                assert!(reason.starts_with("revision conflict"), "{reason}");
                assert!(
                    reason.contains('1') && reason.contains('2'),
                    "the refusal names expected and stored revisions: {reason}"
                );
            }
            other => panic!("expected a committed refusal, got {other:?}"),
        }
        assert_eq!(stored_stub_ids(&sm, 8080), vec!["a"], "nothing mutated");

        // Expecting a revision on an absent record cannot hold.
        let absent = sm
            .apply(vec![entry(
                4,
                conditioned(
                    4,
                    5,
                    ControlOp::DeleteImposter {
                        tenant: TenantId::default(),
                        port: 9999,
                    },
                ),
            )])
            .await
            .expect("apply");
        match &absent[0].outcome {
            ControlOutcome::Failed { reason } => {
                assert!(reason.starts_with("revision conflict"), "{reason}");
                assert!(reason.contains("9999"), "{reason}");
            }
            other => panic!("expected a committed refusal, got {other:?}"),
        }

        // A precondition on an op with no single-imposter target is refused
        // deterministically (the front already answers 400 before minting one).
        let multi = sm
            .apply(vec![entry(
                5,
                conditioned(
                    5,
                    1,
                    ControlOp::DeleteAll {
                        tenant: TenantId::default(),
                    },
                ),
            )])
            .await
            .expect("apply");
        assert!(
            matches!(&multi[0].outcome, ControlOutcome::Failed { reason }
                if reason.starts_with("revision conflict")),
            "{multi:?}"
        );
    }

    /// #46 gate: a conflicted op replayed under the same op_id collapses to the
    /// original refusal — a keyed retry of a 409 stays a 409, never re-applies.
    #[tokio::test]
    async fn revision_conflict_replay_dedups_to_the_original_refusal() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, put(1, 8080, json!([])))])
            .await
            .expect("put");

        let stale = |op_id: u128| ControlRequest {
            expected_revision: Some(99),
            ..request(
                op_id,
                ControlOp::SetEnabled {
                    tenant: TenantId::default(),
                    port: 8080,
                    enabled: false,
                },
            )
        };
        let first = sm.apply(vec![entry(2, stale(7))]).await.expect("apply");
        assert!(
            matches!(&first[0].outcome, ControlOutcome::Failed { reason }
                if reason.starts_with("revision conflict")),
            "{first:?}"
        );

        let replay = sm.apply(vec![entry(3, stale(7))]).await.expect("replay");
        assert_eq!(
            replay, first,
            "the replay must return the ORIGINAL refusal, not re-evaluate"
        );
    }

    // -- issue #131: replicated route table ------------------------------------

    #[tokio::test]
    async fn apply_put_routes_records_the_table() {
        let (_td, mut sm, _routes) = fresh_sm_with_routes().await;
        let responses = sm
            .apply(vec![entry(1, put_routes(1, vec![test_route("a", 8080)]))])
            .await
            .expect("apply");
        assert_eq!(responses, vec![ControlResponse::applied(1)]);
        let table = sm.route_table().expect("read route table");
        assert_eq!(table.routes.len(), 1);
        assert_eq!(table.routes[0].id, "a");
    }

    /// A whole-table replace really replaces: a second `PutRoutes` drops
    /// whatever the first one wrote that is not in the new table.
    #[tokio::test]
    async fn put_routes_replaces_the_whole_table() {
        let (_td, mut sm, _routes) = fresh_sm_with_routes().await;
        sm.apply(vec![entry(
            1,
            put_routes(1, vec![test_route("a", 1), test_route("b", 2)]),
        )])
        .await
        .expect("apply first table");
        sm.apply(vec![entry(2, put_routes(2, vec![test_route("c", 3)]))])
            .await
            .expect("apply replacement table");
        let table = sm.route_table().expect("read route table");
        assert_eq!(
            table
                .routes
                .iter()
                .map(|r| r.id.as_str())
                .collect::<Vec<_>>(),
            vec!["c"],
            "the second PutRoutes must replace, not merge"
        );
    }

    #[tokio::test]
    async fn apply_delete_route_removes_it_and_is_idempotent_when_absent() {
        let (_td, mut sm, _routes) = fresh_sm_with_routes().await;
        sm.apply(vec![entry(1, put_routes(1, vec![test_route("a", 1)]))])
            .await
            .expect("apply put");

        let delete = |op_id: u128| {
            request(
                op_id,
                ControlOp::DeleteRoute {
                    tenant: TenantId::default(),
                    id: "a".to_owned(),
                },
            )
        };
        let responses = sm.apply(vec![entry(2, delete(2))]).await.expect("delete");
        assert_eq!(responses, vec![ControlResponse::applied(2)]);
        assert!(sm.route_table().expect("read").routes.is_empty());

        // Deleting again (an absent route) is idempotent, like DeleteImposter.
        let responses = sm
            .apply(vec![entry(3, delete(3))])
            .await
            .expect("delete absent");
        assert_eq!(
            responses,
            vec![ControlResponse::applied(3)],
            "deleting an absent route must not be a Failed outcome"
        );
    }

    /// A committed `PutRoutes` must swap the front door's compiled table, not
    /// just the `sm_routes` rows — this is the mechanism `bind_front_door`
    /// actually reads from.
    #[tokio::test]
    async fn put_routes_swaps_the_attached_compiled_table() {
        let (_td, mut sm, routes) = fresh_sm_with_routes().await;
        assert!(routes.load().is_empty(), "starts empty");
        sm.apply(vec![entry(1, put_routes(1, vec![test_route("a", 8080)]))])
            .await
            .expect("apply");
        assert!(
            !routes.load().is_empty(),
            "a committed PutRoutes must swap the ArcSwap"
        );
        let loaded = routes.load();
        let resolved = loaded
            .resolve(None, &hyper::Method::GET, "/a", &hyper::HeaderMap::new())
            .expect("the route matches its own path prefix");
        assert_eq!(resolved.target.port, 8080);
    }

    /// Same `op_id` twice: the dedup contract applies to route ops exactly as
    /// it does to imposter ops (existing dedup-test pattern, issue #9).
    #[tokio::test]
    async fn dedup_collapses_a_replayed_put_routes() {
        let (_td, mut sm, _routes) = fresh_sm_with_routes().await;
        let first = sm
            .apply(vec![entry(1, put_routes(7, vec![test_route("a", 1)]))])
            .await
            .expect("apply");
        assert_eq!(first, vec![ControlResponse::applied(1)]);

        let replay = sm
            .apply(vec![entry(2, put_routes(7, vec![test_route("b", 2)]))])
            .await
            .expect("replay");
        assert_eq!(
            replay,
            vec![ControlResponse::applied(1)],
            "the replay must return the ORIGINAL revision, not its own index"
        );
        assert_eq!(
            sm.route_table().expect("read").routes[0].id,
            "a",
            "the replayed op_id must not have applied a second time"
        );
    }

    /// Snapshot + restore on a fresh node round-trips `sm_routes` (existing
    /// store-conformance pattern, mirrors `snapshot_carries_configs_and_dedup_state`).
    #[tokio::test]
    async fn snapshot_round_trips_the_route_table() {
        let (_td, mut sm, _routes) = fresh_sm_with_routes().await;
        sm.apply(vec![entry(
            1,
            put_routes(9, vec![test_route("a", 1), test_route("b", 2)]),
        )])
        .await
        .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower, follower_routes) = fresh_sm_with_routes().await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");

        let mut ids: Vec<String> = follower
            .route_table()
            .expect("read")
            .routes
            .into_iter()
            .map(|r| r.id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a".to_owned(), "b".to_owned()]);
        assert!(
            !follower_routes.load().is_empty(),
            "install_snapshot must also drive the attached routes handle"
        );
    }

    /// A restart, unlike a join: the routes `ArcSwap` is process-local and
    /// starts empty every time, even though `sm_routes` already has committed
    /// rows on disk from the previous run. `reconcile_engine` is what
    /// re-seeds it — the same cold-start hook the engine has always used,
    /// extended to routes.
    #[tokio::test]
    async fn reconcile_engine_reseeds_the_routes_handle_after_a_cold_start() {
        let (_td, mut sm, routes) = fresh_sm_with_routes().await;
        sm.apply(vec![entry(1, put_routes(1, vec![test_route("a", 8080)]))])
            .await
            .expect("apply");
        // Simulate the restart: a brand new `ArcSwap`, as a fresh process
        // would construct, while `sm`'s underlying `sm_routes` table (unlike
        // the ArcSwap) is exactly what a restart finds already on disk.
        routes.store(Arc::new(CompiledRoutes::default()));
        assert!(routes.load().is_empty(), "simulated cold start");

        sm.reconcile_engine().await.expect("reconcile");

        assert!(
            !routes.load().is_empty(),
            "reconcile_engine must re-seed the routes handle from sm_routes, \
             not only the engine"
        );
    }
}
