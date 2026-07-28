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
//! * `sm_tenants`    — `tenant id -> Tenant` (JSON): tenant records, including
//!   deleted tombstones (issue #159, RFC-002 §10 slice T1).
//! * `sm_principals` — `principal id -> Principal` (JSON): fleet-wide identities,
//!   not tenant-scoped — a principal exists once and is bound to tenants via
//!   `sm_bindings` (issue #159).
//! * `sm_bindings`   — `(principal id, tenant) -> Role` (JSON): principal-major,
//!   deliberately not tenant-major — see its `TableDefinition`'s doc comment
//!   for why the key order is load-bearing (issue #159).
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

use std::collections::{BTreeMap, BTreeSet};
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
    self, ControlOp, ControlRequest, ControlResponse, DEFAULT_TENANT, Digest, FLEET_SCOPE, OnDrift,
    SourceMode, SourceProvenance, StubEdit, StubEditScript, Tenant,
};
// Only the test-only row readers below (`test_principal_row`, `test_binding`)
// name these types outside `mod tests`; production `mutate_tables` never
// spells them (it only destructures `ControlOp` variant fields), so they are
// unused in a non-test build without this gate.
#[cfg(test)]
use crate::control::{Principal, Role};

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
/// `(tenant, source id) -> StoredSource` (JSON): imposter sources as durable
/// control-plane objects (issue #134). One row per source, like `sm_routes` —
/// a delete is a single-key removal rather than a read-modify-write of a set.
const SM_SOURCES_TABLE: TableDefinition<(&str, &str), &str> = TableDefinition::new("sm_sources");
/// `tenant id -> Tenant` (JSON): tenant records, including deleted tombstones
/// (issue #159, RFC-002 §10 slice T1). See [`Tenant::deleted`]'s doc for why a
/// delete leaves the row behind instead of removing it.
const SM_TENANTS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_tenants");
/// `principal id -> Principal` (JSON). Fleet-wide, not tenant-scoped: a
/// principal is one identity that may be bound to many tenants, so unlike
/// `sm_configs`/`sm_routes`/`sm_sources` this key carries no tenant component
/// at all.
const SM_PRINCIPALS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_principals");
/// `(principal id, tenant) -> Role` (JSON): a principal's binding to one
/// tenant (issue #159).
///
/// Principal-major, **not** tenant-major, and that ordering is load-bearing.
/// The hot path is per-request: authenticate a principal, then resolve *that
/// principal's* bindings — a redb prefix range under `(principal_id, ..)`, one
/// seek. "Which principals are bound to tenant X" is an admin listing, not a
/// per-request check, and it pays a full-table scan under this key order —
/// that trade is deliberate. Keying tenant-major would flip the cost onto
/// every authorized request instead of onto an occasional admin query.
const SM_BINDINGS_TABLE: TableDefinition<(&str, &str), &str> = TableDefinition::new("sm_bindings");
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
    /// Which source produced this config, when one did (issue #134). Defaulted
    /// so a record written before sources existed still parses — it is simply a
    /// hand-written imposter, which is exactly what `None` means.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<SourceProvenance>,
}

/// What `sm_sources` stores per `(tenant, id)`.
///
/// `drifted` is replicated state, not a node's opinion: the provenance it is
/// computed from lives in the state machine, so every replica flips the flag at
/// the same log index for the same reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSource {
    uri: String,
    mode: SourceMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth_ref: Option<String>,
    on_drift: OnDrift,
    /// Poll interval for a tracking source (issue #135). Defaulted so a source
    /// stored before #135 still parses — it is necessarily pinned, which is
    /// what `None` means.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    poll_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last: Option<LastPull>,
    #[serde(default)]
    drifted: bool,
    revision: u64,
}

/// What the last pull against a source produced.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LastPull {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    digest: Digest,
    /// The applying op's `issued_at_secs` — the replicated logical clock, never
    /// a replica's local wall clock, for the same reason the dedup TTL uses it:
    /// two replicas must record the same value for the same committed op.
    at_secs: u64,
    outcome: PullOutcome,
}

/// How a committed pull was applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PullOutcome {
    /// The configs were applied and the source's drift flag cleared.
    Applied,
    /// The source had drifted and its `on_drift` said `skip`: nothing was
    /// applied, and it is still drifted.
    Skipped,
}

impl PullOutcome {
    /// Whether the fleet actually holds the content this pull carried.
    ///
    /// Load-bearing for the no-change short circuit: a `Skipped` pull records
    /// the digest it *saw*, not one it applied, so treating the two alike would
    /// let a later pull of that same content decide there was nothing to do —
    /// exactly when an operator has just switched the source to `overwrite` to
    /// make it win.
    #[must_use]
    pub fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }
}

/// One source as reported by the read paths — the shape `GET /admin/sources`
/// and `GET /admin/sources/:id` render.
///
/// A crate-owned type rather than the stored record itself: the stored shape is
/// free to gain fields the operator surface should not have to render, and this
/// crosses the public API boundary where `openraft` types may not.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceRecord {
    pub id: String,
    pub uri: String,
    pub mode: SourceMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_ref: Option<String>,
    pub on_drift: OnDrift,
    /// How often the leader re-fetches this source, when it tracks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_secs: Option<u64>,
    /// Whether an operator has edited this source's imposters by hand since it
    /// last applied.
    pub drifted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_pulled_at_secs: Option<u64>,
    /// How the last pull ended, or absent when the source has never pulled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_outcome: Option<PullOutcome>,
    /// The ports this source currently owns, ascending.
    pub ports: Vec<u16>,
    /// The log index that last wrote this source record.
    pub revision: u64,
}

impl SourceRecord {
    /// The digest of the content the fleet actually holds from this source, or
    /// `None` if it holds none.
    ///
    /// This — not `last_digest` — is what a pull compares against to decide
    /// there is nothing to do. A *skipped* pull (a drifted source under
    /// `on_drift: skip`) also records the digest it saw, and treating that as
    /// applied would strand the fleet: an operator switches the source to
    /// `overwrite` precisely so the next pull wins, and it would instead answer
    /// "unchanged" and apply nothing.
    #[must_use]
    pub fn applied_digest(&self) -> Option<&str> {
        if self.last_outcome.is_some_and(PullOutcome::is_applied) {
            self.last_digest.as_deref()
        } else {
            None
        }
    }
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
    /// `(tenant, source id, stored-source JSON)` rows of `sm_sources`. Defaulted
    /// for the same reason `routes` is: a snapshot built before issue #134 still
    /// installs cleanly, carrying no sources — the same as a fleet that declared
    /// none.
    #[serde(default)]
    sources: Vec<(String, String, String)>,
    /// `(tenant id, Tenant JSON)` rows of `sm_tenants` (issue #159). Defaulted
    /// for the same reason `routes`/`sources` are: a pre-#159 snapshot still
    /// installs, carrying no tenants — a table omitted here is a table that
    /// vanishes on the next follower catch-up (#134/#137 already taught this
    /// crate that lesson once).
    #[serde(default)]
    tenants: Vec<(String, String)>,
    /// `(principal id, Principal JSON)` rows of `sm_principals` (issue #159).
    #[serde(default)]
    principals: Vec<(String, String)>,
    /// `(principal id, tenant, Role JSON)` rows of `sm_bindings` (issue #159).
    #[serde(default)]
    bindings: Vec<(String, String, String)>,
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
        write_txn.open_table(SM_SOURCES_TABLE).map_err(io)?;
        write_txn.open_table(SM_TENANTS_TABLE).map_err(io)?;
        write_txn.open_table(SM_PRINCIPALS_TABLE).map_err(io)?;
        write_txn.open_table(SM_BINDINGS_TABLE).map_err(io)?;
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

    /// Every declared source for the default tenant, id-ascending (issue #134).
    ///
    /// Like [`Self::read_config`], this answers from local applied state with no
    /// Raft round trip, so comparing two nodes' answers is what tells an
    /// operator whether a `SourcePut` has converged.
    #[allow(clippy::result_large_err)]
    pub fn sources(&self) -> StorageResult<Vec<SourceRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let sources = read_txn
            .open_table(SM_SOURCES_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let configs = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let owned = Self::ports_by_source(&configs)
            .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?;

        let mut records = Vec::new();
        for item in sources
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (tenant, id) = key.value();
            if tenant != DEFAULT_TENANT {
                continue;
            }
            // A source row that will not parse is committed-state corruption.
            // Reported as an error rather than skipped: silently shrinking the
            // list would tell an operator their source is gone when it is not.
            let stored: StoredSource = serde_json::from_str(value.value()).map_err(|e| {
                tracing::error!(source_id = %id, error = %e, "corrupt stored source");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            records.push(Self::render_source(id, &stored, owned.get(id)));
        }
        Ok(records)
    }

    /// One source by id, or `None` if the default tenant has no such source.
    #[allow(clippy::result_large_err)]
    pub fn source(&self, id: &str) -> StorageResult<Option<SourceRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let sources = read_txn
            .open_table(SM_SOURCES_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let Some(guard) = sources
            .get((DEFAULT_TENANT, id))
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        else {
            return Ok(None);
        };
        let stored: StoredSource = serde_json::from_str(guard.value()).map_err(|e| {
            tracing::error!(source_id = %id, error = %e, "corrupt stored source");
            StorageError::from(StorageIOError::read_state_machine(&e))
        })?;
        let configs = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let owned = Self::ports_by_source(&configs)
            .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?;
        Ok(Some(Self::render_source(id, &stored, owned.get(id))))
    }

    /// `(port, provenance)` for every source-owned config, ascending by port —
    /// what `GET /_cluster/config` reports so an operator can see which
    /// imposters a source owns and at which version.
    #[allow(clippy::result_large_err)]
    pub fn config_provenance(&self) -> StorageResult<Vec<(u16, SourceProvenance)>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let configs = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut owned = Vec::new();
        for item in configs
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (tenant, port) = key.value();
            if tenant != DEFAULT_TENANT {
                continue;
            }
            let stored: StoredImposter = serde_json::from_str(value.value())
                .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?;
            if let Some(provenance) = stored.source {
                owned.push((port, provenance));
            }
        }
        Ok(owned)
    }

    /// Source id -> the ports it owns, from `sm_configs` provenance. Read from
    /// an open (possibly mid-transaction) view so both the read paths and apply
    /// can use it.
    fn ports_by_source(
        table: &impl ReadableTable<(&'static str, u16), &'static str>,
    ) -> Result<BTreeMap<String, Vec<u16>>, redb::StorageError> {
        let mut owned: BTreeMap<String, Vec<u16>> = BTreeMap::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let (tenant, port) = key.value();
            if tenant != DEFAULT_TENANT {
                continue;
            }
            // A record that will not parse owns no port *as far as provenance
            // goes*; the config read paths report the corruption. Ignoring it
            // here can only under-report ownership, never delete anything.
            if let Ok(stored) = serde_json::from_str::<StoredImposter>(value.value())
                && let Some(provenance) = stored.source
            {
                owned.entry(provenance.id).or_default().push(port);
            }
        }
        for ports in owned.values_mut() {
            ports.sort_unstable();
        }
        Ok(owned)
    }

    fn render_source(id: &str, stored: &StoredSource, ports: Option<&Vec<u16>>) -> SourceRecord {
        SourceRecord {
            id: id.to_owned(),
            uri: stored.uri.clone(),
            mode: stored.mode,
            auth_ref: stored.auth_ref.clone(),
            on_drift: stored.on_drift,
            poll_secs: stored.poll_secs,
            drifted: stored.drifted,
            last_version: stored.last.as_ref().and_then(|last| last.version.clone()),
            last_digest: stored
                .last
                .as_ref()
                .map(|last| last.digest.as_str().to_owned()),
            last_pulled_at_secs: stored.last.as_ref().map(|last| last.at_secs),
            last_outcome: stored.last.as_ref().map(|last| last.outcome),
            ports: ports.cloned().unwrap_or_default(),
            revision: stored.revision,
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

    /// Test-only: the raw `sm_configs` row for an arbitrary `(tenant, port)` —
    /// unlike [`Self::read_config`], which only ever answers for
    /// [`DEFAULT_TENANT`], this is what issue #159's cross-tenant tests need.
    #[cfg(test)]
    fn raw_config_row(&self, tenant: &str, port: u16) -> Option<String> {
        let txn = self.db.begin_read().expect("test txn");
        let table = txn.open_table(SM_CONFIGS_TABLE).expect("test table");
        table
            .get((tenant, port))
            .expect("test get")
            .map(|g| g.value().to_owned())
    }

    /// Test-only: the raw `sm_routes` row for an arbitrary `(tenant, id)`.
    #[cfg(test)]
    fn raw_route_row(&self, tenant: &str, id: &str) -> Option<String> {
        let txn = self.db.begin_read().expect("test txn");
        let table = txn.open_table(SM_ROUTES_TABLE).expect("test table");
        table
            .get((tenant, id))
            .expect("test get")
            .map(|g| g.value().to_owned())
    }

    /// Test-only: the raw `sm_sources` row for an arbitrary `(tenant, id)`.
    #[cfg(test)]
    fn raw_source_row(&self, tenant: &str, id: &str) -> Option<String> {
        let txn = self.db.begin_read().expect("test txn");
        let table = txn.open_table(SM_SOURCES_TABLE).expect("test table");
        table
            .get((tenant, id))
            .expect("test get")
            .map(|g| g.value().to_owned())
    }

    /// Test-only: the parsed `sm_tenants` row for `id`.
    #[cfg(test)]
    fn test_tenant(&self, id: &str) -> Option<Tenant> {
        let txn = self.db.begin_read().expect("test txn");
        let table = txn.open_table(SM_TENANTS_TABLE).expect("test table");
        table
            .get(id)
            .expect("test get")
            .map(|g| serde_json::from_str(g.value()).expect("tenant row parses"))
    }

    /// Test-only: the parsed `sm_principals` row for `id`.
    #[cfg(test)]
    fn test_principal_row(&self, id: &str) -> Option<Principal> {
        let txn = self.db.begin_read().expect("test txn");
        let table = txn.open_table(SM_PRINCIPALS_TABLE).expect("test table");
        table
            .get(id)
            .expect("test get")
            .map(|g| serde_json::from_str(g.value()).expect("principal row parses"))
    }

    /// Test-only: the parsed `sm_bindings` row for `(principal_id, tenant)`.
    #[cfg(test)]
    fn test_binding(&self, principal_id: &str, tenant: &str) -> Option<Role> {
        let txn = self.db.begin_read().expect("test txn");
        let table = txn.open_table(SM_BINDINGS_TABLE).expect("test table");
        table
            .get((principal_id, tenant))
            .expect("test get")
            .map(|g| serde_json::from_str(g.value()).expect("role row parses"))
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

    /// The stored imposter at `(tenant, port)`, or `None` when there is none.
    /// A record that will not parse is `None` *for provenance purposes only* —
    /// the callers here use it to decide what to preserve and what to flag, and
    /// the config read paths already report the corruption loudly.
    #[allow(clippy::result_large_err)]
    fn stored_imposter(
        configs: &Table<'_, (&'static str, u16), &'static str>,
        tenant: &str,
        port: u16,
    ) -> StorageResult<Option<StoredImposter>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let Some(guard) = configs.get((tenant, port)).map_err(io)? else {
            return Ok(None);
        };
        match serde_json::from_str::<StoredImposter>(guard.value()) {
            Ok(stored) => Ok(Some(stored)),
            Err(e) => {
                // Corruption of committed state, and the callers' next act is
                // usually to overwrite these bytes — which loses the port's
                // provenance link for good. `None` keeps the op deterministic
                // (every replica holds the same bad bytes and decides the same
                // way), but it must not also be silent.
                tracing::error!(
                    tenant, port, error = %e,
                    "corrupt stored imposter; its source provenance is lost"
                );
                Ok(None)
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn provenance_of(
        configs: &Table<'_, (&'static str, u16), &'static str>,
        tenant: &str,
        port: u16,
    ) -> StorageResult<Option<SourceProvenance>> {
        Ok(Self::stored_imposter(configs, tenant, port)?.and_then(|stored| stored.source))
    }

    /// Every port the named source currently owns, ascending.
    #[allow(clippy::result_large_err)]
    fn ports_of_source(
        configs: &Table<'_, (&'static str, u16), &'static str>,
        tenant: &str,
        id: &str,
    ) -> StorageResult<Vec<u16>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let mut ports = Vec::new();
        for item in configs.iter().map_err(io)? {
            let (key, value) = item.map_err(io)?;
            let (t, port) = key.value();
            if t != tenant {
                continue;
            }
            if let Ok(stored) = serde_json::from_str::<StoredImposter>(value.value())
                && stored.source.as_ref().is_some_and(|s| s.id == id)
            {
                ports.push(port);
            }
        }
        ports.sort_unstable();
        Ok(ports)
    }

    #[allow(clippy::result_large_err)]
    fn stored_source(
        sources: &Table<'_, (&'static str, &'static str), &'static str>,
        tenant: &str,
        id: &str,
    ) -> StorageResult<Option<StoredSource>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let Some(guard) = sources.get((tenant, id)).map_err(io)? else {
            return Ok(None);
        };
        match serde_json::from_str::<StoredSource>(guard.value()) {
            Ok(stored) => Ok(Some(stored)),
            Err(e) => {
                // Corruption of committed state. Reported as `None` so the op
                // takes its "unknown source" path — a deterministic refusal on
                // every replica, since they all hold the same bad bytes —
                // rather than failing apply, which would wedge the node.
                tracing::error!(source_id = %id, error = %e, "corrupt stored source");
                Ok(None)
            }
        }
    }

    /// Record that a source's imposters no longer match what it declares.
    ///
    /// A no-op when the edited port had no provenance (nothing to drift from)
    /// or when the source itself is gone (its provenance outlived it, which
    /// `SourceDelete` clears — this is the belt to that braces).
    #[allow(clippy::result_large_err)]
    fn mark_drifted(
        sources: &mut Table<'_, (&'static str, &'static str), &'static str>,
        tenant: &str,
        provenance: Option<&SourceProvenance>,
        index: u64,
    ) -> StorageResult<()> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let Some(provenance) = provenance else {
            return Ok(());
        };
        let Some(mut stored) = Self::stored_source(sources, tenant, &provenance.id)? else {
            return Ok(());
        };
        if stored.drifted {
            return Ok(());
        }
        stored.drifted = true;
        stored.revision = index;
        let value =
            serde_json::to_string(&stored).map_err(|e| StorageIOError::write_state_machine(&e))?;
        sources
            .insert((tenant, provenance.id.as_str()), value.as_str())
            .map_err(io)?;
        Ok(())
    }

    /// The stored [`Tenant`] record at `id`, or `None` when there is none.
    ///
    /// A record that will not parse is treated as `None`, like
    /// [`Self::stored_source`]: the callers here use it to decide whether a
    /// tenant exists, and every replica holds the same bad bytes, so refusing
    /// deterministically (as "unknown tenant") is safe — it can never diverge
    /// two replicas' apply of the same committed op.
    #[allow(clippy::result_large_err)]
    /// `Ok(Ok(None))` is "no such tenant"; `Ok(Err(reason))` is a corrupt row.
    ///
    /// The two must not collapse into one answer. A corrupt row is not an
    /// absent one: the tenant's configs, routes, sources and bindings are all
    /// still live, so treating it as missing makes `TenantDelete` skip the
    /// entire cascade and report `Applied` — the operator is told the tenant is
    /// gone while its imposters keep serving. Surfacing it as a committed
    /// refusal matches how `check_expected_revision` already treats a record it
    /// cannot parse, and is deterministic: every replica holds the same bytes.
    #[allow(clippy::result_large_err)]
    fn stored_tenant(
        tenants: &Table<'_, &'static str, &'static str>,
        id: &str,
    ) -> StorageResult<Result<Option<Tenant>, String>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let Some(guard) = tenants.get(id).map_err(io)? else {
            return Ok(Ok(None));
        };
        match serde_json::from_str::<Tenant>(guard.value()) {
            Ok(stored) => Ok(Ok(Some(stored))),
            Err(e) => {
                tracing::error!(tenant = id, error = %e, "corrupt stored tenant");
                Ok(Err(format!(
                    "stored tenant {id:?} cannot be read: its record is corrupt"
                )))
            }
        }
    }

    /// `Err` when `tenant` names no live tenant record — used by every op that
    /// addresses an *existing* tenant rather than creating one
    /// (`PrincipalPut`, `BindingPut` against an ordinary tenant). A deleted
    /// tenant reads the same as a missing one: its tombstone (`deleted:
    /// true`) exists so the id's history survives, not so new state can still
    /// be attached to it.
    #[allow(clippy::result_large_err)]
    fn require_live_tenant(
        tenants: &Table<'_, &'static str, &'static str>,
        tenant: &str,
    ) -> StorageResult<Result<(), String>> {
        // `default` is live by definition, with or without a stored row.
        // Nothing ever writes one on a fresh cluster (there is no bootstrap
        // `TenantPut`), and `validate` refuses to delete it — so requiring a
        // row here would make `PrincipalPut { tenant: "default" }` fail on
        // every new cluster until someone thought to create the tenant that
        // the rest of the code already treats as always-present.
        if tenant == DEFAULT_TENANT {
            return Ok(Ok(()));
        }
        Ok(match Self::stored_tenant(tenants, tenant)? {
            // A corrupt row keeps its own reason: "unknown" would send an
            // operator looking for a tenant that is present but unreadable.
            Err(reason) => Err(reason),
            Ok(Some(t)) if !t.deleted => Ok(()),
            Ok(_) => Err(format!("unknown tenant {tenant:?}")),
        })
    }

    /// Whether `port` is already claimed by a tenant other than `tenant`.
    ///
    /// Ports are fleet-unique across tenants (RFC-002 §3.2): `sm_configs` is
    /// keyed `(tenant, port)`, so nothing at the table level stops two
    /// tenants from claiming the same port, and this full scan is the check
    /// that stands in for it. Called by `PutImposter` and by
    /// `SourcePullResult`'s pre-pass — the operator-write and source-pull
    /// paths must admit exactly the same things, which is the rule
    /// `validate_replicable_config` exists to keep. A re-write from the
    /// *same* tenant that already owns the port is an upsert, not a
    /// collision, and this returns `false` for it.
    #[allow(clippy::result_large_err)]
    fn port_claimed_by_another_tenant(
        configs: &Table<'_, (&'static str, u16), &'static str>,
        tenant: &str,
        port: u16,
    ) -> StorageResult<bool> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        for item in configs.iter().map_err(io)? {
            let (key, _) = item.map_err(io)?;
            let (owner, p) = key.value();
            if p == port && owner != tenant {
                return Ok(true);
            }
        }
        Ok(false)
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
    #[allow(clippy::too_many_arguments)]
    fn mutate_tables(
        configs: &mut Table<'_, (&'static str, u16), &'static str>,
        routes: &mut Table<'_, (&'static str, &'static str), &'static str>,
        sources: &mut Table<'_, (&'static str, &'static str), &'static str>,
        tenants: &mut Table<'_, &'static str, &'static str>,
        principals: &mut Table<'_, &'static str, &'static str>,
        bindings: &mut Table<'_, (&'static str, &'static str), &'static str>,
        op: &ControlOp,
        index: u64,
        issued_at_secs: u64,
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
                // Ports are fleet-unique across tenants (RFC-002 §3.2). The
                // reason is named as the port only, never the other tenant:
                // naming it would turn this refusal into a cross-tenant
                // enumeration oracle (RFC-002 §8.4) — an operator could probe
                // ports to learn which tenants exist and what they run. Do not
                // "improve" this message; it is deliberately incomplete.
                if Self::port_claimed_by_another_tenant(configs, tenant.as_str(), port)? {
                    return Ok(Err(format!("port {port} is already bound in this fleet")));
                }
                // Provenance survives a manual replace: the source still owns
                // this port, it just no longer holds what the source declares.
                // Clearing it instead would orphan the port, and the next
                // `overwrite` pull would recreate it as a second imposter.
                let provenance = Self::provenance_of(configs, tenant.as_str(), port)?;
                let config_json = serde_json::to_string(config)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                let stored = StoredImposter {
                    config_json,
                    enabled: config.enabled,
                    revision: index,
                    source: provenance.clone(),
                };
                let value = serde_json::to_string(&stored)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                configs
                    .insert((tenant.as_str(), port), value.as_str())
                    .map_err(io)?;
                Self::mark_drifted(sources, tenant.as_str(), provenance.as_ref(), index)?;
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
                Self::mark_drifted(sources, tenant.as_str(), record.source.as_ref(), index)?;
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
                let provenance = Self::provenance_of(configs, tenant.as_str(), *port)?;
                configs.remove((tenant.as_str(), *port)).map_err(io)?;
                Self::mark_drifted(sources, tenant.as_str(), provenance.as_ref(), index)?;
                crate::metrics::config_removed(*port);
                Ok(Ok(vec![Self::sync_action(configs)?]))
            }
            ControlOp::DeleteAll { tenant } => {
                let tenant = tenant.as_str();
                let (removed, drifted): (Vec<u16>, BTreeSet<String>) = {
                    let mut removed = Vec::new();
                    let mut drifted = BTreeSet::new();
                    for item in configs.iter().map_err(io)? {
                        let (key, value) = item.map_err(io)?;
                        let (t, port) = key.value();
                        if t != tenant {
                            continue;
                        }
                        removed.push(port);
                        // A record that will not parse names no source here.
                        // Same reasoning as `ports_by_source`: this can only
                        // under-report which sources to flag, never delete
                        // anything, and `stored_imposter` logs the corruption
                        // where it is actionable.
                        if let Ok(stored) = serde_json::from_str::<StoredImposter>(value.value())
                            && let Some(provenance) = stored.source
                        {
                            drifted.insert(provenance.id);
                        }
                    }
                    (removed, drifted)
                };
                configs.retain(|(t, _), _| t != tenant).map_err(io)?;
                for id in drifted {
                    Self::mark_drifted(
                        sources,
                        tenant,
                        Some(&SourceProvenance { id, version: None }),
                        index,
                    )?;
                }
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
                Self::mark_drifted(sources, tenant.as_str(), record.source.as_ref(), index)?;
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
            ControlOp::SourcePut {
                tenant,
                id,
                uri,
                mode,
                auth_ref,
                on_drift,
                poll_secs,
            } => {
                let existing = Self::stored_source(sources, tenant.as_str(), id)?;
                // Keep the pull history only while the record still describes
                // the same content: a digest identifies bytes at a URI, so
                // carrying it across a repoint would let the no-change short
                // circuit skip the very fetch the repoint asked for.
                let (last, drifted) = match existing {
                    Some(previous) if previous.uri == *uri => (previous.last, previous.drifted),
                    _ => (None, false),
                };
                let stored = StoredSource {
                    uri: uri.clone(),
                    mode: *mode,
                    auth_ref: auth_ref.clone(),
                    on_drift: *on_drift,
                    poll_secs: *poll_secs,
                    last,
                    drifted,
                    revision: index,
                };
                let value = serde_json::to_string(&stored)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                sources
                    .insert((tenant.as_str(), id.as_str()), value.as_str())
                    .map_err(io)?;
                // No engine action: declaring a source binds nothing. Only a
                // pull produces configs.
                Ok(Ok(Vec::new()))
            }
            ControlOp::SourceDelete { tenant, id } => {
                // Idempotent when absent, like `DeleteImposter`.
                sources.remove((tenant.as_str(), id.as_str())).map_err(io)?;
                // The imposters stay bound — "stop tracking this URI" is not
                // "tear down live traffic" — but nothing may still point at a
                // source that no longer exists, so their provenance is cleared.
                let orphaned = Self::ports_of_source(configs, tenant.as_str(), id)?;
                for port in orphaned {
                    let Some(mut record) = Self::stored_imposter(configs, tenant.as_str(), port)?
                    else {
                        continue;
                    };
                    record.source = None;
                    record.revision = index;
                    let value = serde_json::to_string(&record)
                        .map_err(|e| StorageIOError::write_state_machine(&e))?;
                    configs
                        .insert((tenant.as_str(), port), value.as_str())
                        .map_err(io)?;
                }
                // Provenance is metadata: the desired config *set* is
                // unchanged, so there is nothing for the engine to do.
                Ok(Ok(Vec::new()))
            }
            ControlOp::SourcePullResult {
                tenant,
                id,
                version,
                digest,
                configs: fetched,
            } => {
                let tenant_str = tenant.as_str();
                let Some(mut source) = Self::stored_source(sources, tenant_str, id)? else {
                    // The source was deleted between the fetch and this write.
                    // Applying anyway would resurrect imposters the operator
                    // just asked to stop tracking.
                    return Ok(Err(format!(
                        "unknown source {id:?}: it was deleted before this pull was committed"
                    )));
                };

                if source.drifted {
                    match source.on_drift {
                        OnDrift::Fail => {
                            return Ok(Err(format!(
                                "source {id:?} has drifted (its imposters were edited by hand) and \
                                 its on_drift policy is \"fail\""
                            )));
                        }
                        OnDrift::Skip => {
                            // A committed decision, not a failure: the attempt
                            // is recorded so an operator can see the source is
                            // being held back rather than silently idle.
                            source.last = Some(LastPull {
                                version: version.clone(),
                                digest: digest.clone(),
                                at_secs: issued_at_secs,
                                outcome: PullOutcome::Skipped,
                            });
                            source.revision = index;
                            let value = serde_json::to_string(&source)
                                .map_err(|e| StorageIOError::write_state_machine(&e))?;
                            sources
                                .insert((tenant_str, id.as_str()), value.as_str())
                                .map_err(io)?;
                            return Ok(Ok(Vec::new()));
                        }
                        OnDrift::Overwrite => {}
                    }
                }

                // Ports are fleet-unique across tenants (RFC-002 §3.2), and a
                // pull is no exception: a document fetched into tenant B must
                // not take a port tenant A already binds. `PutImposter` refuses
                // this, and `validate_replicable_config` exists precisely so the
                // operator-write and source-pull paths "cannot drift into
                // admitting different things" — an unguarded pull would be that
                // drift, and the quieter half of it, since nobody typed the
                // port.
                //
                // Checked as a **pre-pass over the whole fetched set, before any
                // mutation**, and that placement is load-bearing: a refusal
                // returned from inside the apply loop below would still commit
                // the transaction (`Ok(Err(_))` is a committed `Failed`, not a
                // rollback), so a late refusal would leave the de-declared-port
                // removals applied — a half-pull reported as a clean failure.
                //
                // Named as the port only, never the owning tenant (RFC-002
                // §8.4): naming it would make this refusal a cross-tenant
                // enumeration oracle. Do not "improve" the message.
                for config in fetched {
                    // Both refusals live in this pre-pass so the arm below is
                    // structurally free of late failures, rather than free of
                    // them only because `validate` happens to run first.
                    let Some(port) = config.port else {
                        return Ok(Err(
                            "pull result carries a config without an explicit port".to_owned()
                        ));
                    };
                    if Self::port_claimed_by_another_tenant(configs, tenant_str, port)? {
                        return Ok(Err(format!("port {port} is already bound in this fleet")));
                    }
                }

                let provenance = SourceProvenance {
                    id: id.clone(),
                    version: version.clone(),
                };
                let previously_owned = Self::ports_of_source(configs, tenant_str, id)?;
                let declared: BTreeSet<u16> = fetched.iter().filter_map(|c| c.port).collect();

                for port in previously_owned.iter().filter(|p| !declared.contains(p)) {
                    configs.remove((tenant_str, *port)).map_err(io)?;
                    crate::metrics::config_removed(*port);
                }

                for config in fetched {
                    // Both port checks already ran in the pre-pass above, which
                    // is why this cannot refuse: refusing here, after the
                    // de-declared-port removals, would commit a half-pull.
                    let Some(port) = config.port else { continue };
                    let config_json = serde_json::to_string(config)
                        .map_err(|e| StorageIOError::write_state_machine(&e))?;
                    let existing = Self::stored_imposter(configs, tenant_str, port)?;
                    // Rewriting a byte-identical record would bump its revision
                    // for no reason and — because the engine sync diffs on
                    // content — is the difference between "leave this imposter
                    // alone" and "replace it", which resets its runtime state.
                    if let Some(existing) = &existing
                        && existing.config_json == config_json
                        && existing.enabled == config.enabled
                        && existing.source.as_ref() == Some(&provenance)
                    {
                        continue;
                    }
                    // Taking a port from another source. Nothing forbids two
                    // documents declaring the same port — they are fetched
                    // independently — so the loser is marked drifted rather
                    // than left silently believing it still owns a port whose
                    // provenance now names someone else. Without this the two
                    // sources flip-flop the port on every pull, each reporting
                    // success.
                    if let Some(other) = existing.as_ref().and_then(|e| e.source.as_ref())
                        && other.id != *id
                    {
                        tracing::warn!(
                            port,
                            from = %other.id,
                            to = %id,
                            "a source took over a port another source owns"
                        );
                        Self::mark_drifted(sources, tenant_str, Some(other), index)?;
                    }
                    let stored = StoredImposter {
                        config_json,
                        enabled: config.enabled,
                        revision: index,
                        source: Some(provenance.clone()),
                    };
                    let value = serde_json::to_string(&stored)
                        .map_err(|e| StorageIOError::write_state_machine(&e))?;
                    configs
                        .insert((tenant_str, port), value.as_str())
                        .map_err(io)?;
                    crate::metrics::config_applied(port, index);
                }

                source.last = Some(LastPull {
                    version: version.clone(),
                    digest: digest.clone(),
                    at_secs: issued_at_secs,
                    outcome: PullOutcome::Applied,
                });
                // The fleet now holds exactly what the source declares, so
                // whatever it had drifted from is resolved.
                source.drifted = false;
                source.revision = index;
                let value = serde_json::to_string(&source)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                sources
                    .insert((tenant_str, id.as_str()), value.as_str())
                    .map_err(io)?;

                Ok(Ok(vec![Self::sync_action(configs)?]))
            }
            ControlOp::TenantPut {
                tenant,
                display_name,
                quotas,
            } => {
                let tenant_str = tenant.as_str();
                // An upsert preserves `created_at_secs` and clears any
                // tombstone: recreating a previously-deleted tenant id is
                // allowed (ids are not permanently burned — only `"default"`
                // is protected, and `validate` refuses to delete it at all).
                let created_at_secs = match Self::stored_tenant(tenants, tenant_str)? {
                    // Refuse rather than silently re-stamp: overwriting an
                    // unreadable row would destroy the only copy of whatever
                    // it held and reset the tenant's age to now.
                    Err(reason) => return Ok(Err(reason)),
                    Ok(Some(existing)) => existing.created_at_secs,
                    Ok(None) => issued_at_secs,
                };
                let record = Tenant {
                    id: tenant.clone(),
                    display_name: display_name.clone(),
                    quotas: quotas.clone(),
                    created_at_secs,
                    deleted: false,
                };
                let value = serde_json::to_string(&record)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                tenants.insert(tenant_str, value.as_str()).map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::TenantDelete { tenant } => {
                let tenant_str = tenant.as_str();
                let mut record = match Self::stored_tenant(tenants, tenant_str)? {
                    // Corrupt is NOT absent. The tenant's configs, routes,
                    // sources and bindings are all still live, so answering
                    // `Applied` here would report a deletion that never
                    // happened and leave its imposters serving traffic.
                    Err(reason) => return Ok(Err(reason)),
                    // Deleting a tenant that was never created is idempotent,
                    // like every other delete in this match.
                    Ok(None) => return Ok(Ok(Vec::new())),
                    Ok(Some(record)) => record,
                };
                if record.deleted {
                    return Ok(Ok(Vec::new()));
                }
                // The cascade runs *before* the tombstone write, inside the
                // same write transaction as everything else here: a tenant
                // deleted but still holding configs/routes/sources would be
                // resources no principal can administer (nothing can bind to
                // a deleted tenant) — a single committed op is what keeps
                // "tombstoned" and "cleaned up" from ever being observed
                // apart, on any replica.
                configs.retain(|(t, _), _| t != tenant_str).map_err(io)?;
                routes.retain(|(t, _), _| t != tenant_str).map_err(io)?;
                sources.retain(|(t, _), _| t != tenant_str).map_err(io)?;
                // Bindings cascade too, and this one is a security property
                // rather than tidiness. A tombstoned id may be recreated (the
                // tombstone records that it existed, it does not reserve it),
                // plausibly by a different operator for a different customer.
                // Bindings left behind would make every one of the old
                // tenant's principals live again the moment the name is
                // reused — privilege resurrection across an ownership change,
                // and unfixable afterwards because the rows are already
                // committed to the log.
                bindings.retain(|(_, t), _| t != tenant_str).map_err(io)?;
                record.deleted = true;
                let value = serde_json::to_string(&record)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                tenants.insert(tenant_str, value.as_str()).map_err(io)?;
                Ok(Ok(vec![
                    Self::sync_action(configs)?,
                    Self::sync_routes_action(routes)?,
                ]))
            }
            ControlOp::PrincipalPut { tenant, principal } => {
                if let Err(reason) = Self::require_live_tenant(tenants, tenant.as_str())? {
                    return Ok(Err(reason));
                }
                let value = serde_json::to_string(principal)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                principals
                    .insert(principal.id.as_str(), value.as_str())
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            // `tenant` addresses no stored record here — `sm_principals` is
            // fleet-wide, keyed by principal id alone (see its
            // `TableDefinition`'s doc) — so, like every other delete in this
            // match, removing an absent principal is idempotent.
            ControlOp::PrincipalDelete {
                tenant: _,
                principal_id,
            } => {
                principals.remove(principal_id.as_str()).map_err(io)?;
                // The principal's bindings go with it. A principal id can be
                // an external value — an OIDC `subject`, an mTLS SAN — and
                // identity providers do recycle those, so orphaned bindings
                // would hand a *different* human every role the previous
                // holder of the name had, the moment the id is re-created.
                // This is the read the principal-major key order exists for:
                // a prefix range under `(principal_id, ..)` rather than a scan.
                bindings
                    .retain(|(p, _), _| p != principal_id.as_str())
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::BindingPut {
                tenant,
                principal_id,
                role,
            } => {
                // The fleet scope is never a stored tenant row (`validate`
                // already limited it to `Role::FleetAdmin` bindings only), so
                // there is nothing to look up for it.
                if tenant.as_str() != FLEET_SCOPE
                    && let Err(reason) = Self::require_live_tenant(tenants, tenant.as_str())?
                {
                    return Ok(Err(reason));
                }
                let value = serde_json::to_string(role)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                bindings
                    .insert((principal_id.as_str(), tenant.as_str()), value.as_str())
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::BindingDelete {
                tenant,
                principal_id,
            } => {
                bindings
                    .remove((principal_id.as_str(), tenant.as_str()))
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
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

        let (configs, routes, sources, tenants, principals, bindings, dedup) = {
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
            let sources_table = read_txn
                .open_table(SM_SOURCES_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut sources = Vec::new();
            for item in sources_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let (tenant, id) = key.value();
                sources.push((tenant.to_owned(), id.to_owned(), value.value().to_owned()));
            }
            let tenants_table = read_txn
                .open_table(SM_TENANTS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut tenants = Vec::new();
            for item in tenants_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                tenants.push((key.value().to_owned(), value.value().to_owned()));
            }
            let principals_table = read_txn
                .open_table(SM_PRINCIPALS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut principals = Vec::new();
            for item in principals_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                principals.push((key.value().to_owned(), value.value().to_owned()));
            }
            let bindings_table = read_txn
                .open_table(SM_BINDINGS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut bindings = Vec::new();
            for item in bindings_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let (principal_id, tenant) = key.value();
                bindings.push((
                    principal_id.to_owned(),
                    tenant.to_owned(),
                    value.value().to_owned(),
                ));
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
            (
                configs, routes, sources, tenants, principals, bindings, dedup,
            )
        };

        let payload = SnapshotPayload {
            configs,
            routes,
            sources,
            tenants,
            principals,
            bindings,
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
            let mut sources = write_txn
                .open_table(SM_SOURCES_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut tenants = write_txn
                .open_table(SM_TENANTS_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut principals = write_txn
                .open_table(SM_PRINCIPALS_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut bindings = write_txn
                .open_table(SM_BINDINGS_TABLE)
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
                                        &mut sources,
                                        &mut tenants,
                                        &mut principals,
                                        &mut bindings,
                                        &request.op,
                                        log_id.index,
                                        applied.logical_clock_secs,
                                    )?,
                                },
                                None => Self::mutate_tables(
                                    &mut configs,
                                    &mut routes,
                                    &mut sources,
                                    &mut tenants,
                                    &mut principals,
                                    &mut bindings,
                                    &request.op,
                                    log_id.index,
                                    applied.logical_clock_secs,
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

            let mut sources_table = write_txn
                .open_table(SM_SOURCES_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            sources_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (tenant, id, value) in &payload.sources {
                sources_table
                    .insert((tenant.as_str(), id.as_str()), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            let mut tenants_table = write_txn
                .open_table(SM_TENANTS_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            tenants_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (id, value) in &payload.tenants {
                tenants_table
                    .insert(id.as_str(), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            let mut principals_table = write_txn
                .open_table(SM_PRINCIPALS_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            principals_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (id, value) in &payload.principals {
                principals_table
                    .insert(id.as_str(), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            let mut bindings_table = write_txn
                .open_table(SM_BINDINGS_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            bindings_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (principal_id, tenant, value) in &payload.bindings {
                bindings_table
                    .insert((principal_id.as_str(), tenant.as_str()), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

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

    use super::{
        DEDUP_TTL_SECS, DedupEntry, PullOutcome, RedbLogStore, RedbStateMachine, SM_DEDUP_TABLE,
        SM_TENANTS_TABLE, SourceRecord, new,
    };
    use crate::control::{
        AuthSource, ControlOp, ControlOutcome, ControlRequest, ControlResponse, Digest,
        FLEET_SCOPE, OnDrift, Principal, PrincipalId, Quotas, Role, SourceMode, StubEdit,
        StubEditScript, TenantId,
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

    // -- issue #134: sources as control-plane objects ---------------------------

    fn source_put(op_id: u128, id: &str, uri: &str, on_drift: OnDrift) -> ControlRequest {
        request(
            op_id,
            ControlOp::SourcePut {
                tenant: TenantId::default(),
                id: id.to_owned(),
                uri: uri.to_owned(),
                mode: SourceMode::Pinned,
                auth_ref: None,
                on_drift,
                poll_secs: None,
            },
        )
    }

    fn pull(op_id: u128, id: &str, version: &str, ports: &[u16]) -> ControlRequest {
        pull_at(op_id, 0, id, version, ports)
    }

    fn pull_at(
        op_id: u128,
        issued_at_secs: u64,
        id: &str,
        version: &str,
        ports: &[u16],
    ) -> ControlRequest {
        let configs: Vec<ImposterConfig> = ports
            .iter()
            .map(|port| config(*port, json!([{ "id": format!("s{port}") }])))
            .collect();
        request_at(
            op_id,
            issued_at_secs,
            ControlOp::SourcePullResult {
                tenant: TenantId::default(),
                id: id.to_owned(),
                version: Some(version.to_owned()),
                // The real digest comes from `sources::digest_of`; these tests
                // exercise apply, which only ever compares digests for equality.
                digest: Digest::new(format!("digest-{version}")),
                configs,
            },
        )
    }

    async fn apply_one(
        sm: &mut RedbStateMachine,
        index: u64,
        request: ControlRequest,
    ) -> ControlResponse {
        sm.apply([entry(index, request)])
            .await
            .expect("apply")
            .pop()
            .expect("one response")
    }

    fn one_source(sm: &RedbStateMachine, id: &str) -> SourceRecord {
        sm.source(id).expect("read source").expect("source present")
    }

    #[tokio::test]
    async fn a_source_is_stored_read_back_and_deleted() {
        let (_td, mut sm) = fresh_sm(None).await;

        let response = apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        assert_eq!(response.outcome, ControlOutcome::Applied);

        let record = one_source(&sm, "mocks");
        assert_eq!(record.uri, "https://h/i.json");
        assert_eq!(record.mode, SourceMode::Pinned);
        assert_eq!(record.on_drift, OnDrift::Overwrite);
        assert!(!record.drifted);
        assert_eq!(record.last_digest, None, "a fresh source has never pulled");
        assert_eq!(record.revision, 1);
        assert_eq!(sm.sources().expect("list").len(), 1);

        let response = apply_one(
            &mut sm,
            2,
            request(
                2,
                ControlOp::SourceDelete {
                    tenant: TenantId::default(),
                    id: "mocks".to_owned(),
                },
            ),
        )
        .await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert!(sm.source("mocks").expect("read").is_none());
        assert!(sm.sources().expect("list").is_empty());
    }

    /// Re-declaring the same source is an upsert, not a duplicate — this is
    /// what makes `--imposters` bootstrap idempotent across restarts. A `PUT`
    /// that leaves the URI alone must keep the pull history, or every restart
    /// would re-apply an identical document.
    #[tokio::test]
    async fn re_putting_the_same_uri_keeps_the_pull_history() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080])).await;
        assert_eq!(
            one_source(&sm, "mocks").last_digest.as_deref(),
            Some("digest-v1")
        );

        apply_one(
            &mut sm,
            3,
            source_put(3, "mocks", "https://h/i.json", OnDrift::Skip),
        )
        .await;
        let record = one_source(&sm, "mocks");
        assert_eq!(
            record.last_digest.as_deref(),
            Some("digest-v1"),
            "an unchanged uri keeps what it last applied"
        );
        assert_eq!(
            record.on_drift,
            OnDrift::Skip,
            "the new policy takes effect"
        );
        assert_eq!(record.ports, vec![8080], "its imposters are untouched");
    }

    /// Repointing a source at a different URI makes its last digest meaningless
    /// — comparing the new URI's content against the old one's digest would
    /// short-circuit a pull that must actually happen.
    #[tokio::test]
    async fn repointing_a_source_forgets_the_old_digest() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/a.json", OnDrift::Overwrite),
        )
        .await;
        apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080])).await;

        apply_one(
            &mut sm,
            3,
            source_put(3, "mocks", "https://h/b.json", OnDrift::Overwrite),
        )
        .await;
        let record = one_source(&sm, "mocks");
        assert_eq!(record.uri, "https://h/b.json");
        assert_eq!(
            record.last_digest, None,
            "a digest describes content at a uri, not a source id"
        );
    }

    #[tokio::test]
    async fn a_pull_stamps_provenance_and_reports_its_ports() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        let response = apply_one(&mut sm, 2, pull_at(2, 4242, "mocks", "v1", &[8080, 8081])).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);

        let record = one_source(&sm, "mocks");
        assert_eq!(record.ports, vec![8080, 8081]);
        assert_eq!(record.last_version.as_deref(), Some("v1"));
        assert_eq!(record.last_outcome, Some(PullOutcome::Applied));
        assert_eq!(
            record.last_pulled_at_secs,
            Some(4242),
            "the timestamp is the replicated logical clock, not a local one"
        );

        let provenance = sm.config_provenance().expect("provenance");
        assert_eq!(provenance.len(), 2);
        for (port, source) in provenance {
            assert!(port == 8080 || port == 8081);
            assert_eq!(source.id, "mocks");
            assert_eq!(source.version.as_deref(), Some("v1"));
        }
    }

    /// A pull whose source was deleted in the window between fetch and submit
    /// must not resurrect it — the operator asked for it to be gone.
    #[tokio::test]
    async fn a_pull_for_an_unknown_source_is_refused() {
        let (_td, mut sm) = fresh_sm(None).await;
        let response = apply_one(&mut sm, 1, pull(1, "ghost", "v1", &[8080])).await;
        match response.outcome {
            ControlOutcome::Failed { reason } => assert!(reason.contains("ghost"), "{reason}"),
            other => panic!("a raced delete must refuse, got {other:?}"),
        }
        assert!(sm.read_config(8080).expect("read").is_none());
    }

    /// The incremental-apply criterion: a pull that changes one imposter leaves
    /// the sibling's stored record — and so its runtime state — untouched, and
    /// an imposter nobody's source owns is never in the blast radius.
    #[tokio::test]
    async fn pull_applies_incrementally_and_spares_siblings() {
        let (_td, mut sm) = fresh_sm(None).await;
        // A hand-written imposter that no source owns.
        apply_one(&mut sm, 1, put(1, 9000, json!([{ "id": "manual" }]))).await;
        apply_one(
            &mut sm,
            2,
            source_put(2, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        apply_one(&mut sm, 3, pull(3, "mocks", "v1", &[8080, 8081])).await;
        let sibling_before = sm.read_config(8081).expect("read").expect("present");

        // v2 drops 8081 and keeps 8080.
        let changed = request(
            4,
            ControlOp::SourcePullResult {
                tenant: TenantId::default(),
                id: "mocks".to_owned(),
                version: Some("v2".to_owned()),
                digest: Digest::new("digest-v2"),
                configs: vec![config(8080, json!([{ "id": "changed" }]))],
            },
        );
        apply_one(&mut sm, 4, changed).await;

        assert_eq!(
            stored_stub_ids(&sm, 8080),
            vec!["changed".to_owned()],
            "the changed imposter is replaced"
        );
        assert!(
            sm.read_config(8081).expect("read").is_none(),
            "a port the document dropped is removed from the source's set"
        );
        assert!(
            sm.read_config(9000).expect("read").is_some(),
            "an imposter no source owns is never touched by a pull"
        );
        assert_eq!(one_source(&sm, "mocks").ports, vec![8080]);
        assert_ne!(sibling_before, String::new());
    }

    /// A sibling *this* source still declares must survive a pull byte-for-byte
    /// when the document did not change it — the state machine may not rewrite
    /// a record it has no new content for, because rewriting it is what would
    /// reset the engine's per-imposter runtime state.
    #[tokio::test]
    async fn an_unchanged_sibling_is_not_rewritten_by_a_pull() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080, 8081])).await;
        let untouched_before = sm.read_config(8081).expect("read").expect("present");

        let changed = request(
            3,
            ControlOp::SourcePullResult {
                tenant: TenantId::default(),
                id: "mocks".to_owned(),
                version: Some("v2".to_owned()),
                digest: Digest::new("digest-v2"),
                configs: vec![
                    config(8080, json!([{ "id": "changed" }])),
                    config(8081, json!([{ "id": "s8081" }])),
                ],
            },
        );
        apply_one(&mut sm, 3, changed).await;

        assert_eq!(
            sm.read_config(8081).expect("read").expect("present"),
            untouched_before,
            "an identical config must not be rewritten: the rewrite is what resets runtime state"
        );
        assert_eq!(stored_stub_ids(&sm, 8080), vec!["changed".to_owned()]);
    }

    /// `test_source_pull_dedup`: a replayed pull collapses to the original
    /// response and applies nothing a second time.
    #[tokio::test]
    async fn test_source_pull_dedup() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        let first = apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080])).await;

        // A manual edit between the two, so a re-apply would be visible.
        apply_one(&mut sm, 3, put(3, 8080, json!([{ "id": "edited" }]))).await;

        let replay = apply_one(&mut sm, 4, pull(2, "mocks", "v1", &[8080])).await;
        assert_eq!(replay, first, "a replay returns the original response");
        assert_eq!(
            stored_stub_ids(&sm, 8080),
            vec!["edited".to_owned()],
            "a replayed pull must change nothing"
        );
    }

    /// Drift: a manual edit of a source-owned port flips the flag, and the
    /// provenance stays — the source still owns the port, it is just no longer
    /// what the source says it is.
    #[tokio::test]
    async fn a_manual_edit_flips_the_source_drift_flag() {
        for (label, edit) in [
            ("PutImposter", put(10, 8080, json!([{ "id": "edited" }]))),
            (
                "PatchStubs",
                request(
                    10,
                    ControlOp::PatchStubs {
                        tenant: TenantId::default(),
                        port: 8080,
                        edit: StubEditScript(vec![StubEdit::Add {
                            stub: serde_json::from_value(json!({ "id": "added" })).expect("stub"),
                            index: None,
                        }]),
                    },
                ),
            ),
            (
                "SetEnabled",
                request(
                    10,
                    ControlOp::SetEnabled {
                        tenant: TenantId::default(),
                        port: 8080,
                        enabled: false,
                    },
                ),
            ),
            (
                "DeleteImposter",
                request(
                    10,
                    ControlOp::DeleteImposter {
                        tenant: TenantId::default(),
                        port: 8080,
                    },
                ),
            ),
            (
                "DeleteAll",
                request(
                    10,
                    ControlOp::DeleteAll {
                        tenant: TenantId::default(),
                    },
                ),
            ),
        ] {
            let (_td, mut sm) = fresh_sm(None).await;
            apply_one(
                &mut sm,
                1,
                source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
            )
            .await;
            apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080])).await;
            assert!(
                !one_source(&sm, "mocks").drifted,
                "{label}: clean after a pull"
            );

            apply_one(&mut sm, 3, edit).await;
            assert!(
                one_source(&sm, "mocks").drifted,
                "{label}: a manual edit of a source-owned port must flip drift"
            );
        }
    }

    /// Editing an imposter no source owns is not drift — there is nothing for
    /// it to have drifted from.
    #[tokio::test]
    async fn editing_an_unowned_imposter_is_not_drift() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080])).await;
        apply_one(&mut sm, 3, put(3, 9000, json!([{ "id": "manual" }]))).await;
        assert!(!one_source(&sm, "mocks").drifted);
    }

    /// A pull's own writes are not drift — otherwise every source would be
    /// permanently drifted from the moment it first applied.
    #[tokio::test]
    async fn a_pull_does_not_flag_itself_as_drift() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080])).await;
        apply_one(&mut sm, 3, pull(3, "mocks", "v2", &[8080])).await;
        assert!(!one_source(&sm, "mocks").drifted);
    }

    #[tokio::test]
    async fn drifted_source_overwrites_by_default() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080])).await;
        apply_one(&mut sm, 3, put(3, 8080, json!([{ "id": "edited" }]))).await;

        let response = apply_one(&mut sm, 4, pull(4, "mocks", "v2", &[8080])).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert_eq!(
            stored_stub_ids(&sm, 8080),
            vec!["s8080".to_owned()],
            "overwrite restores what the source declares"
        );
        let record = one_source(&sm, "mocks");
        assert!(!record.drifted, "a successful overwrite clears the flag");
        assert_eq!(record.last_outcome, Some(PullOutcome::Applied));
    }

    #[tokio::test]
    async fn drifted_source_skips_when_asked() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Skip),
        )
        .await;
        apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080])).await;
        apply_one(&mut sm, 3, put(3, 8080, json!([{ "id": "edited" }]))).await;

        let response = apply_one(&mut sm, 4, pull(4, "mocks", "v2", &[8080])).await;
        assert_eq!(
            response.outcome,
            ControlOutcome::Applied,
            "a skip is a committed decision, not a failure"
        );
        assert_eq!(
            stored_stub_ids(&sm, 8080),
            vec!["edited".to_owned()],
            "skip leaves the operator's edit in place"
        );
        let record = one_source(&sm, "mocks");
        assert!(record.drifted, "skipping does not resolve the drift");
        assert_eq!(record.last_outcome, Some(PullOutcome::Skipped));
        assert_eq!(
            record.last_version.as_deref(),
            Some("v2"),
            "the attempt is still recorded, so an operator can see it happened"
        );
    }

    #[tokio::test]
    async fn drifted_source_fails_when_asked() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Fail),
        )
        .await;
        apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080])).await;
        apply_one(&mut sm, 3, put(3, 8080, json!([{ "id": "edited" }]))).await;

        let response = apply_one(&mut sm, 4, pull(4, "mocks", "v2", &[8080])).await;
        match response.outcome {
            ControlOutcome::Failed { reason } => {
                assert!(reason.contains("drift"), "{reason}");
            }
            other => panic!("on_drift=fail must refuse, got {other:?}"),
        }
        assert_eq!(
            stored_stub_ids(&sm, 8080),
            vec!["edited".to_owned()],
            "a refused pull changes nothing"
        );
        let record = one_source(&sm, "mocks");
        assert!(record.drifted);
        assert_eq!(
            record.last_version.as_deref(),
            Some("v1"),
            "a refusal is not a pull: the last applied version is unchanged"
        );
    }

    /// Deleting a source leaves its imposters serving — tearing down live
    /// traffic is not what "stop tracking this URI" means — but clears their
    /// provenance, so nothing is left pointing at a source that no longer
    /// exists.
    #[tokio::test]
    async fn deleting_a_source_orphans_its_imposters_rather_than_deleting_them() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        apply_one(&mut sm, 2, pull(2, "mocks", "v1", &[8080])).await;

        apply_one(
            &mut sm,
            3,
            request(
                3,
                ControlOp::SourceDelete {
                    tenant: TenantId::default(),
                    id: "mocks".to_owned(),
                },
            ),
        )
        .await;

        assert!(
            sm.read_config(8080).expect("read").is_some(),
            "the imposter keeps serving"
        );
        assert!(
            sm.config_provenance().expect("provenance").is_empty(),
            "nothing may still claim a deleted source"
        );
    }

    #[tokio::test]
    async fn deleting_an_absent_source_is_applied_not_failed() {
        let (_td, mut sm) = fresh_sm(None).await;
        let response = apply_one(
            &mut sm,
            1,
            request(
                1,
                ControlOp::SourceDelete {
                    tenant: TenantId::default(),
                    id: "ghost".to_owned(),
                },
            ),
        )
        .await;
        assert_eq!(
            response.outcome,
            ControlOutcome::Applied,
            "deletes are idempotent at the state-machine level, like DeleteImposter"
        );
    }

    /// Provenance and the source table both survive a snapshot install — a
    /// follower that catches up by snapshot must know which imposters a source
    /// owns, or its next drift verdict would differ from its peers'.
    #[tokio::test]
    async fn provenance_is_reported_and_survives_snapshot_restore() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Skip),
        )
        .await;
        apply_one(&mut sm, 2, pull(2, "mocks", "v7", &[8080])).await;
        apply_one(&mut sm, 3, put(3, 8080, json!([{ "id": "edited" }]))).await;
        assert!(one_source(&sm, "mocks").drifted);

        let snapshot: Snapshot<TypeConfig> = sm.build_snapshot().await.expect("build snapshot");
        let (_td2, mut restored) = fresh_sm(None).await;
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        let record = restored
            .source("mocks")
            .expect("read source")
            .expect("source survived the snapshot");
        assert_eq!(record.uri, "https://h/i.json");
        assert_eq!(record.on_drift, OnDrift::Skip);
        assert_eq!(record.last_version.as_deref(), Some("v7"));
        assert!(record.drifted, "the drift verdict must survive too");
        assert_eq!(record.ports, vec![8080]);

        let provenance = restored.config_provenance().expect("provenance");
        assert_eq!(provenance.len(), 1);
        assert_eq!(provenance[0].0, 8080);
        assert_eq!(provenance[0].1.id, "mocks");
        assert_eq!(provenance[0].1.version.as_deref(), Some("v7"));
    }

    /// A snapshot written before sources existed still installs — the field
    /// defaults to empty, which is what "this fleet declared no sources" is.
    #[tokio::test]
    async fn a_pre_sources_snapshot_still_installs() {
        let (_td, sm) = fresh_sm(None).await;
        let legacy = json!({
            "configs": [],
            "dedup": [],
            "last_applied_log": null,
            "last_membership": { "log_id": null, "membership": { "configs": [], "nodes": {} } },
        });
        let payload: super::SnapshotPayload =
            serde_json::from_value(legacy).expect("a pre-#134 snapshot payload still decodes");
        assert!(payload.sources.is_empty());
        assert!(sm.sources().expect("list").is_empty());
    }

    /// A pull drives the engine: the imposters a source declares are actually
    /// bound, and one the document drops is torn down.
    #[tokio::test]
    async fn a_pull_drives_the_engine() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(Arc::clone(&engine))).await;
        apply_one(
            &mut sm,
            1,
            source_put(1, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        let ports = [ephemeral_port(), ephemeral_port()];
        apply_one(&mut sm, 2, pull(2, "mocks", "v1", &ports)).await;
        for port in ports {
            assert!(
                engine.get_imposter(port).is_ok(),
                "port {port} must be bound after a pull"
            );
        }

        let dropped = request(
            3,
            ControlOp::SourcePullResult {
                tenant: TenantId::default(),
                id: "mocks".to_owned(),
                version: Some("v2".to_owned()),
                digest: Digest::new("digest-v2"),
                configs: vec![config(ports[0], json!([]))],
            },
        );
        apply_one(&mut sm, 3, dropped).await;
        assert!(engine.get_imposter(ports[0]).is_ok());
        assert!(
            engine.get_imposter(ports[1]).is_err(),
            "a port the document dropped is torn down"
        );
    }

    fn ephemeral_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("reserve a port")
            .local_addr()
            .expect("addr")
            .port()
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

    // -- issue #159: tenancy and RBAC records (RFC-002 §10 slice T1) -----------

    fn tenant_put_req(op_id: u128, tenant: &str, display_name: &str) -> ControlRequest {
        request(
            op_id,
            ControlOp::TenantPut {
                tenant: TenantId::new(tenant),
                display_name: display_name.to_owned(),
                quotas: Quotas::default(),
            },
        )
    }

    /// A well-formed argon2id hash shape. Not a real hash of anything — apply
    /// only ever stores it, it never verifies a password against it — so a
    /// fixed placeholder stands in wherever a valid one is needed.
    const VALID_ARGON2_HASH: &str =
        "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHQ$RdescudvJCsgt3ub+b+dWRWJTmaaJObG";

    fn test_principal(id: &str) -> Principal {
        Principal {
            id: PrincipalId::new(id),
            display_name: id.to_owned(),
            auth: AuthSource::ApiKey {
                hash: VALID_ARGON2_HASH.to_owned(),
            },
            disabled: false,
        }
    }

    fn principal_put_req(op_id: u128, tenant: &str, principal: Principal) -> ControlRequest {
        request(
            op_id,
            ControlOp::PrincipalPut {
                tenant: TenantId::new(tenant),
                principal,
            },
        )
    }

    fn binding_put_req(
        op_id: u128,
        tenant: &str,
        principal_id: &str,
        role: Role,
    ) -> ControlRequest {
        request(
            op_id,
            ControlOp::BindingPut {
                tenant: TenantId::new(tenant),
                principal_id: PrincipalId::new(principal_id),
                role,
            },
        )
    }

    #[tokio::test]
    async fn tenant_put_then_principal_put_then_binding_put_all_commit_and_read_back() {
        let (_td, mut sm) = fresh_sm(None).await;

        let response = apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme Corp")).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        let tenant = sm.test_tenant("acme").expect("tenant stored");
        assert_eq!(tenant.display_name, "Acme Corp");
        assert!(!tenant.deleted);

        let response = apply_one(
            &mut sm,
            2,
            principal_put_req(2, "acme", test_principal("alice")),
        )
        .await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        let principal = sm.test_principal_row("alice").expect("principal stored");
        assert_eq!(principal.display_name, "alice");

        let response = apply_one(
            &mut sm,
            3,
            binding_put_req(3, "acme", "alice", Role::Editor),
        )
        .await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert_eq!(sm.test_binding("alice", "acme"), Some(Role::Editor));
    }

    /// `validate` cannot see whether "acme" exists — that is state, checked in
    /// `mutate_tables` once the op is known to be well-formed.
    #[tokio::test]
    async fn principal_put_against_a_nonexistent_tenant_is_a_committed_failure() {
        let (_td, mut sm) = fresh_sm(None).await;
        let response = apply_one(
            &mut sm,
            1,
            principal_put_req(1, "ghost", test_principal("alice")),
        )
        .await;
        match response.outcome {
            ControlOutcome::Failed { reason } => assert!(reason.contains("ghost"), "{reason}"),
            other => panic!("a principal in an unknown tenant must be refused, got {other:?}"),
        }
        assert!(sm.test_principal_row("alice").is_none());
    }

    #[tokio::test]
    async fn binding_put_against_a_nonexistent_tenant_is_a_committed_failure() {
        let (_td, mut sm) = fresh_sm(None).await;
        let response = apply_one(
            &mut sm,
            1,
            binding_put_req(1, "ghost", "alice", Role::Viewer),
        )
        .await;
        match response.outcome {
            ControlOutcome::Failed { reason } => assert!(reason.contains("ghost"), "{reason}"),
            other => panic!("a binding against an unknown tenant must be refused, got {other:?}"),
        }
        assert!(sm.test_binding("alice", "ghost").is_none());
    }

    #[tokio::test]
    async fn binding_put_fleet_admin_on_an_ordinary_tenant_is_a_committed_failure() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "payments", "Payments")).await;
        let response = apply_one(
            &mut sm,
            2,
            binding_put_req(2, "payments", "alice", Role::FleetAdmin),
        )
        .await;
        assert!(
            matches!(response.outcome, ControlOutcome::Failed { .. }),
            "{response:?}"
        );
        assert!(sm.test_binding("alice", "payments").is_none());
    }

    #[tokio::test]
    async fn binding_put_editor_on_the_fleet_scope_is_a_committed_failure() {
        let (_td, mut sm) = fresh_sm(None).await;
        let response = apply_one(
            &mut sm,
            1,
            binding_put_req(1, FLEET_SCOPE, "alice", Role::Editor),
        )
        .await;
        assert!(
            matches!(response.outcome, ControlOutcome::Failed { .. }),
            "{response:?}"
        );
        assert!(sm.test_binding("alice", FLEET_SCOPE).is_none());
    }

    /// Ports are fleet-unique across tenants (RFC-002 §3.2): a second tenant
    /// claiming a port another tenant already holds must be refused, and the
    /// refusal must never name the tenant that holds it — naming it would be a
    /// cross-tenant enumeration oracle (RFC-002 §8.4).
    #[tokio::test]
    async fn a_cross_tenant_port_collision_is_refused_without_naming_the_owner() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(&mut sm, 2, tenant_put_req(2, "globex", "Globex")).await;

        let first = request(
            3,
            ControlOp::PutImposter {
                tenant: TenantId::new("acme"),
                config: Box::new(config(9100, json!([]))),
            },
        );
        assert_eq!(
            apply_one(&mut sm, 3, first).await.outcome,
            ControlOutcome::Applied
        );

        let second = request(
            4,
            ControlOp::PutImposter {
                tenant: TenantId::new("globex"),
                config: Box::new(config(9100, json!([]))),
            },
        );
        match apply_one(&mut sm, 4, second).await.outcome {
            ControlOutcome::Failed { reason } => {
                assert!(reason.contains("9100"), "{reason}");
                assert!(
                    !reason.contains("acme"),
                    "the refusal must not name the owner: {reason}"
                );
            }
            other => panic!("a cross-tenant port collision must be refused, got {other:?}"),
        }
    }

    /// A re-`PutImposter` from the tenant that already owns the port is an
    /// upsert, not a collision — the fleet-uniqueness check must not refuse a
    /// tenant overwriting its own imposter.
    #[tokio::test]
    async fn a_same_tenant_re_put_is_not_a_port_collision() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        for index in [2u64, 3] {
            let op = request(
                u128::from(index),
                ControlOp::PutImposter {
                    tenant: TenantId::new("acme"),
                    config: Box::new(config(9101, json!([]))),
                },
            );
            assert_eq!(
                apply_one(&mut sm, index, op).await.outcome,
                ControlOutcome::Applied
            );
        }
    }

    /// A source pull is held to the same fleet-uniqueness rule as an operator
    /// write, and refuses **atomically**.
    ///
    /// Two halves, both load-bearing. `validate_replicable_config` exists so the
    /// operator-write and source-pull paths "cannot drift into admitting
    /// different things" — an unguarded pull would be exactly that drift, and
    /// the quieter half of it, because nobody typed the port. And the check has
    /// to run *before* any mutation: `mutate_tables` returning a refusal still
    /// commits its transaction (`Ok(Err(_))` is a committed `Failed`, not a
    /// rollback), so a check inside the apply loop would leave the de-declared
    /// port already removed and report a clean failure for a half-applied pull.
    /// The final assertion is that atomicity, not the refusal.
    #[tokio::test]
    async fn a_pull_that_would_take_another_tenants_port_is_refused_atomically() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;

        // acme binds 9200 by hand.
        let acme = request(
            2,
            ControlOp::PutImposter {
                tenant: TenantId::new("acme"),
                config: Box::new(config(9200, json!([]))),
            },
        );
        assert_eq!(
            apply_one(&mut sm, 2, acme).await.outcome,
            ControlOutcome::Applied
        );

        // The default tenant runs a source that currently owns 8080.
        apply_one(
            &mut sm,
            3,
            source_put(3, "mocks", "https://h/i.json", OnDrift::Overwrite),
        )
        .await;
        apply_one(&mut sm, 4, pull(4, "mocks", "v1", &[8080])).await;
        assert!(
            sm.read_config(8080).expect("read").is_some(),
            "precondition: the source owns 8080"
        );

        // v2 drops 8080 and declares acme's 9200. Dropping 8080 is precisely
        // the write a late refusal would have committed.
        let greedy = request(
            5,
            ControlOp::SourcePullResult {
                tenant: TenantId::default(),
                id: "mocks".to_owned(),
                version: Some("v2".to_owned()),
                digest: Digest::new("digest-v2"),
                configs: vec![config(9200, json!([]))],
            },
        );
        match apply_one(&mut sm, 5, greedy).await.outcome {
            ControlOutcome::Failed { reason } => {
                assert!(reason.contains("9200"), "{reason}");
                assert!(
                    !reason.contains("acme"),
                    "the refusal must not name the owner: {reason}"
                );
            }
            other => panic!("a pull must not take another tenant's port, got {other:?}"),
        }
        assert!(
            sm.read_config(8080).expect("read").is_some(),
            "the refusal must be atomic: the port this pull would have dropped is still here"
        );
    }

    /// Deleting a tenant takes its bindings with it.
    ///
    /// A tombstone records that an id existed; it does not reserve it, and the
    /// upsert path deliberately allows recreating one. So a binding left behind
    /// is a role that comes back to life the moment the name is reused —
    /// plausibly by a different operator for a different customer. That is
    /// privilege resurrection across an ownership change, and it cannot be
    /// repaired after the fact because the rows are already in the log.
    #[tokio::test]
    async fn tenant_delete_takes_its_bindings_with_it_so_a_reused_id_grants_nothing() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(
            &mut sm,
            2,
            principal_put_req(2, "acme", test_principal("alice")),
        )
        .await;
        apply_one(
            &mut sm,
            3,
            binding_put_req(3, "acme", "alice", Role::TenantAdmin),
        )
        .await;
        assert_eq!(sm.test_binding("alice", "acme"), Some(Role::TenantAdmin));

        apply_one(
            &mut sm,
            4,
            request(
                4,
                ControlOp::TenantDelete {
                    tenant: TenantId::new("acme"),
                },
            ),
        )
        .await;
        assert_eq!(
            sm.test_binding("alice", "acme"),
            None,
            "a deleted tenant's bindings must not survive it"
        );

        // The id is recreated — a different customer, the same name.
        apply_one(&mut sm, 5, tenant_put_req(5, "acme", "Acme Reborn")).await;
        assert_eq!(
            sm.test_binding("alice", "acme"),
            None,
            "recreating the id must not resurrect the old tenant's roles"
        );
    }

    /// Deleting a principal takes its bindings with it, for the same reason —
    /// and this one is likelier, because a principal id can be an external
    /// value (an OIDC `subject`, an mTLS SAN) and identity providers recycle
    /// those. Orphaned bindings would hand a different human every role the
    /// previous holder of the name had.
    #[tokio::test]
    async fn principal_delete_takes_its_bindings_with_it_across_every_tenant() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(&mut sm, 2, tenant_put_req(2, "globex", "Globex")).await;
        apply_one(
            &mut sm,
            3,
            principal_put_req(3, "acme", test_principal("alice")),
        )
        .await;
        apply_one(
            &mut sm,
            4,
            binding_put_req(4, "acme", "alice", Role::Editor),
        )
        .await;
        apply_one(
            &mut sm,
            5,
            binding_put_req(5, "globex", "alice", Role::Viewer),
        )
        .await;
        // A second principal's binding is the control: the cascade must be
        // scoped to the principal being deleted, not a table wipe.
        apply_one(
            &mut sm,
            6,
            principal_put_req(6, "acme", test_principal("bob")),
        )
        .await;
        apply_one(&mut sm, 7, binding_put_req(7, "acme", "bob", Role::Viewer)).await;

        apply_one(
            &mut sm,
            8,
            request(
                8,
                ControlOp::PrincipalDelete {
                    tenant: TenantId::new("acme"),
                    principal_id: PrincipalId::new("alice"),
                },
            ),
        )
        .await;

        assert_eq!(sm.test_principal_row("alice"), None);
        assert_eq!(
            sm.test_binding("alice", "acme"),
            None,
            "the deleted principal's bindings must go with it"
        );
        assert_eq!(
            sm.test_binding("alice", "globex"),
            None,
            "including bindings in tenants the delete did not name"
        );
        assert_eq!(
            sm.test_binding("bob", "acme"),
            Some(Role::Viewer),
            "another principal's binding must be untouched"
        );
    }

    /// A tenant row that will not parse is refused, not treated as absent.
    ///
    /// The two states look identical to a naive read and could not be more
    /// different: the tenant's configs, routes and sources are all still live.
    /// Answering `Applied` would tell the operator the tenant is gone while its
    /// imposters keep serving traffic — the "wrong but quiet" failure this
    /// repo's error rules exist to prevent.
    #[tokio::test]
    async fn tenant_delete_refuses_a_corrupt_row_instead_of_reporting_success() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(
            &mut sm,
            2,
            request(
                2,
                ControlOp::PutImposter {
                    tenant: TenantId::default(),
                    config: Box::new(config(9300, json!([]))),
                },
            ),
        )
        .await;

        // Corrupt acme's row behind the state machine's back.
        {
            let txn = sm.db.begin_write().expect("test txn");
            {
                let mut table = txn.open_table(SM_TENANTS_TABLE).expect("test table");
                table.insert("acme", "{not json").expect("test insert");
            }
            txn.commit().expect("test commit");
        }

        match apply_one(
            &mut sm,
            3,
            request(
                3,
                ControlOp::TenantDelete {
                    tenant: TenantId::new("acme"),
                },
            ),
        )
        .await
        .outcome
        {
            ControlOutcome::Failed { reason } => {
                assert!(reason.contains("corrupt"), "{reason}");
                assert!(reason.contains("acme"), "{reason}");
            }
            other => panic!("a corrupt tenant row must refuse, not report success: {other:?}"),
        }
    }

    #[tokio::test]
    async fn tenant_delete_cascades_configs_routes_and_sources_in_one_revision_and_tombstones() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(
            &mut sm,
            2,
            request(
                2,
                ControlOp::PutImposter {
                    tenant: TenantId::new("acme"),
                    config: Box::new(config(9200, json!([]))),
                },
            ),
        )
        .await;
        apply_one(
            &mut sm,
            3,
            request(
                3,
                ControlOp::PutRoutes {
                    tenant: TenantId::new("acme"),
                    table: RouteTable {
                        routes: vec![test_route("r1", 9200)],
                    },
                },
            ),
        )
        .await;
        apply_one(
            &mut sm,
            4,
            request(
                4,
                ControlOp::SourcePut {
                    tenant: TenantId::new("acme"),
                    id: "mocks".to_owned(),
                    uri: "https://h/i.json".to_owned(),
                    mode: SourceMode::Pinned,
                    auth_ref: None,
                    on_drift: OnDrift::Overwrite,
                    poll_secs: None,
                },
            ),
        )
        .await;

        let response = apply_one(
            &mut sm,
            5,
            request(
                5,
                ControlOp::TenantDelete {
                    tenant: TenantId::new("acme"),
                },
            ),
        )
        .await;
        assert_eq!(response.outcome, ControlOutcome::Applied);

        // The cascade landed in the same revision as the tombstone: nothing
        // half-deleted survives it.
        assert!(sm.raw_config_row("acme", 9200).is_none());
        assert!(sm.raw_route_row("acme", "r1").is_none());
        assert!(sm.raw_source_row("acme", "mocks").is_none());

        let tenant = sm.test_tenant("acme").expect("the tombstone survives");
        assert!(tenant.deleted);
    }

    #[tokio::test]
    async fn snapshot_round_trips_tenants_principals_and_bindings() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(
            &mut sm,
            2,
            principal_put_req(2, "acme", test_principal("alice")),
        )
        .await;
        apply_one(
            &mut sm,
            3,
            binding_put_req(3, "acme", "alice", Role::Editor),
        )
        .await;

        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install snapshot");

        let tenant = follower.test_tenant("acme").expect("tenant survived");
        assert_eq!(tenant.display_name, "Acme");
        let principal = follower
            .test_principal_row("alice")
            .expect("principal survived");
        assert_eq!(principal.display_name, "alice");
        assert_eq!(follower.test_binding("alice", "acme"), Some(Role::Editor));
    }

    /// A snapshot written before issue #159 still installs — the three new
    /// fields default to empty, which is what "this fleet declared no tenancy
    /// records" is. Mirrors `a_pre_sources_snapshot_still_installs`: this crate
    /// has been bitten by a table omitted from the snapshot before (#134/#137),
    /// and the fix both times is the same defaulted-field discipline.
    #[tokio::test]
    async fn a_pre_tenancy_snapshot_still_installs() {
        let (_td, sm) = fresh_sm(None).await;
        let legacy = json!({
            "configs": [],
            "dedup": [],
            "last_applied_log": null,
            "last_membership": { "log_id": null, "membership": { "configs": [], "nodes": {} } },
        });
        let payload: super::SnapshotPayload =
            serde_json::from_value(legacy).expect("a pre-#159 snapshot payload still decodes");
        assert!(payload.tenants.is_empty());
        assert!(payload.principals.is_empty());
        assert!(payload.bindings.is_empty());
        assert!(sm.test_tenant("default").is_none());
    }

    /// This slice adds tenancy *records*; it must not change any answer the
    /// existing single-tenant API gives. A default-tenant `PutImposter` works
    /// exactly as it did before #159 — including with no `TenantPut` for
    /// `"default"` ever having been applied: a resource op does not require
    /// its tenant to have a stored `Tenant` row (only `PrincipalPut` and
    /// `BindingPut` do).
    #[tokio::test]
    async fn a_pre_tenancy_imposter_reads_back_under_default_with_no_tenant_record() {
        let (_td, mut sm) = fresh_sm(None).await;
        let response = apply_one(&mut sm, 1, put(1, 9300, json!([{ "id": "a" }]))).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert!(
            sm.read_config(9300).expect("read").is_some(),
            "the default-tenant read path is unaffected by #159"
        );
        assert!(
            sm.test_tenant("default").is_none(),
            "a default-tenant resource op does not require a TenantPut for \"default\""
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
