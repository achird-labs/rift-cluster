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
//! * `sm_configs`    — `port -> StoredImposter` (JSON): the applied
//!   config, its enabled flag, and the revision (log index) that last wrote it.
//! * `sm_routes`     — `route id -> Route` (JSON): the front door's
//!   replicated route table (issue #131). Read as a whole to recompile a
//!   [`CompiledRoutes`] after every mutating op.
//! * `sm_op_dedup`   — `op_id -> DedupEntry` (JSON): the response recorded for an
//!   applied op, kept for [`DEDUP_TTL_SECS`] so a replayed intent (crash-replay,
//!   client retry with the same `Idempotency-Key`) is exactly-once-in-effect.
//! * `sm_applied`    — `() -> AppliedState` (JSON): last-applied log id + membership.
//!
//! Log and vote writes commit with `Durability::Immediate` per the ADR (log and vote
//! must fsync before ack) — decision D-16, which puts the log, the vote and the
//! snapshot's *metadata* in redb; its amendment (#436) moved the snapshot *payload*
//! to a plain file beside it, see [`RedbStateMachine::snapshot_dir`]. Snapshot and
//! state-machine writes use the default (`None`) durability — the snapshot table is
//! a redundant persisted copy for [`RaftStateMachine::get_current_snapshot`], not
//! the durability boundary; the log is.
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
use std::future::Future;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use arc_swap::ArcSwap;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::{Request, Response};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use parking_lot::Mutex;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, Table,
    TableDefinition,
};
use rift_cluster_base::seams::{
    ApplyReport, CompiledRoutes, ImposterConfig, ImposterError, ImposterManager, Route, RouteTable,
    Stub, StubResponse, handle_imposter_request,
};
use serde::{Deserialize, Serialize};

use super::TypeConfig;
use crate::control::{
    self, ControlOp, ControlRequest, ControlResponse, PreconditionTarget, SessionKey, StubEdit,
    StubEditScript,
};
use crate::stores::flow::FlowNet;
use crate::stores::journal::ClusterJournal;
use crate::stores::sequencer::SequencingRegistry;

type StorageResult<T> = Result<T, StorageError<u64>>;

const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const LOG_META_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("raft_log_meta");
const VOTE_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("raft_vote");
const SNAPSHOT_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("raft_snapshot");
/// `port -> StoredImposter` (JSON). One row per replicated imposter; the port is
/// the whole key since #550 removed tenancy — an imposter is fleet-unique by port
/// and nothing else scopes it.
const SM_CONFIGS_TABLE: TableDefinition<u16, &str> = TableDefinition::new("sm_configs");
/// `route id -> Route` (JSON): the front door's replicated route table (issue
/// #131). One row per route rather than one row per table, so a `DeleteRoute` is
/// a single-key removal instead of a read-modify-write of the whole set.
const SM_ROUTES_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_routes");
/// The log index at which the route table was last mutated (issue #210), under
/// [`ROUTES_REVISION_ROW`]. A missing row reads as `0`.
///
/// A one-row table rather than a field on each `sm_routes` row, because the thing
/// a client conditions a whole-table replace on is the *set*, not any one route —
/// and a per-row copy would have no answer at all when the last mutation was a
/// delete that emptied the table. Same shape as [`SM_SESSION_KEY_TABLE`] and
/// [`SM_FLEET_NAME_TABLE`], and for the same reason: it snapshots, installs and
/// clears through the ordinary table path rather than a hand-written special case.
///
/// `0` for "never written" is load-bearing in the safe direction: a fleet that has
/// never had a route reads `0`, so a client that conditions on `0` and writes
/// first wins, and every later stale token fails. It is never a value that makes a
/// stale precondition pass, because a real mutation stamps a log index and log
/// indices start above zero.
const SM_ROUTES_REVISION_TABLE: TableDefinition<&str, u64> =
    TableDefinition::new("sm_routes_revision");
/// The single key `sm_routes_revision` uses, named rather than `()` for the same
/// reason [`SESSION_KEY_ROW`] is: it reads like the rest of the schema.
const ROUTES_REVISION_ROW: &str = "revision";
/// The fleet's session-signing key as JSON, under [`SESSION_KEY_ROW`] (RFC-006 §5.3, issue
/// #185). A one-row table rather than a field on some metadata blob, so it snapshots, installs
/// and gets cleared through exactly the same code path as every other replicated table, rather
/// than through a hand-written special case that is one commit away from missing the snapshot
/// path and silently logging every console user out after a compaction.
const SM_SESSION_KEY_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_session_key");
/// The single key `sm_session_key` uses, named rather than `()` so it reads like the rest of
/// the schema and a second signing key, if one is ever wanted, is a key change and not a schema
/// migration.
const SESSION_KEY_ROW: &str = "key";
/// The fleet's operator-set name as a plain string, under [`FLEET_NAME_ROW`] (issue #373). A
/// one-row table, same shape as `sm_session_key` and for the same reason: it snapshots, installs
/// and gets cleared through exactly the same code path as every other replicated table, rather
/// than through a hand-written special case.
const SM_FLEET_NAME_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_fleet_name");
/// The single key `sm_fleet_name` uses, named rather than `()` for the same reason
/// [`SESSION_KEY_ROW`] is: it reads like the rest of the schema.
const FLEET_NAME_ROW: &str = "name";
/// `(port, space-tag) -> generation` (issue #224): the applied clear-generation
/// counters `ControlOp::JournalClearGen` bumps. A small, monotone, per-key counter table whose
/// key is two-part: `space-tag` is
/// [`journal_gen_space_key`]'s own encoding of `Option<&str>`, not a bare `&str`, because a
/// port-wide clear (`None`) must never be representable the same way as a space-scoped one
/// (`Some`) no matter what the space is named.
const SM_JOURNAL_GENS_TABLE: TableDefinition<(u16, &str), u64> =
    TableDefinition::new("sm_journal_gens");

/// `(port, sig-hash) -> recorded-response JSON` (#226): the applied proxy-recording
/// markers `ControlOp::ProxyRecorded` writes. A row is both facts at once: *this signature is
/// Recorded* (the claim table any owner — including one elected after a handoff — answers
/// `AlreadyRecorded` from), and *this is the replayable response* (`lookup()`'s durable source
/// for a stub-less proxyOnce recording, which has no recorded stub in config to replay from).
/// Keyed like `sm_journal_gens` minus the space tag: the sig-hash is already a fixed-alphabet
/// hex string, so no encoding is needed to keep key families apart.
const SM_PROXY_RECORDED_TABLE: TableDefinition<(u16, &str), &str> =
    TableDefinition::new("sm_proxy_recorded");

/// What [`place_recorded_stub`] did, carrying exactly what the engine drive needs to
/// reproduce it against the live stub vector.
enum PlacedRecording {
    /// The stub was inserted whole at `index`.
    Inserted { index: usize },
    /// The stub's responses were merged into the existing stub at `index`, addressable by
    /// `id` — a `ReplaceById` drive.
    MergedInto { index: usize, id: String },
    /// Merged into a user-authored stub that carries no id: nothing in a patch script can
    /// address it, so the drive falls back to a full sync.
    MergedAnonymous,
}

/// Deterministically place a recorded stub in a config's stub list — the state-machine
/// transliteration of upstream's `insert_or_append_proxy_stub`
/// (`rift-mock-core/src/imposter/core/proxy.rs`), which operates on the live `StubState`
/// vector and cannot be reused over serialized config. Keep the two in step: the position
/// rules — `proxyOnce` inserts *before* the proxy stub so the recording matches first,
/// `proxyAlways` merges into an existing stub with structurally equal non-empty predicates
/// *after* it (upstream #611) or inserts after — are engine semantics this apply reproduces,
/// not policy of its own. A missing proxy stub degrades to appending at the end, exactly as
/// upstream's `unwrap_or(stubs.len())` does.
fn place_recorded_stub(
    stubs: &mut Vec<Stub>,
    stub: Stub,
    placement: control::RecordedStubPlacement,
    proxy_to: &str,
) -> PlacedRecording {
    let proxy_idx = stubs
        .iter()
        .position(|s| {
            s.responses
                .iter()
                .any(|r| matches!(r, StubResponse::Proxy { proxy } if proxy.to == proxy_to))
        })
        .unwrap_or(stubs.len());
    match placement {
        control::RecordedStubPlacement::BeforeProxy => {
            stubs.insert(proxy_idx, stub);
            PlacedRecording::Inserted { index: proxy_idx }
        }
        control::RecordedStubPlacement::AfterProxyMerging => {
            let merged_idx = stubs
                .iter()
                .enumerate()
                .skip(proxy_idx + 1)
                .find(|(_, existing)| {
                    existing.predicates == stub.predicates && !existing.predicates.is_empty()
                })
                .map(|(idx, _)| idx);
            match merged_idx {
                Some(idx) => {
                    stubs[idx].responses.extend(stub.responses);
                    match stubs[idx].id.clone() {
                        Some(id) => PlacedRecording::MergedInto { index: idx, id },
                        None => PlacedRecording::MergedAnonymous,
                    }
                }
                None => {
                    let insert_index = (proxy_idx + 1).min(stubs.len());
                    stubs.insert(insert_index, stub);
                    PlacedRecording::Inserted {
                        index: insert_index,
                    }
                }
            }
        }
    }
}

/// Encodes the space component of an `sm_journal_gens` key so a port-wide clear (`None`) can
/// never be confused with a space-scoped one — including a hypothetically empty space name.
/// `validate` already refuses `Some("")`, but this encoding does not lean on that refusal (the
/// #224 design note this crate was told twice): every space-scoped key carries a leading `'s'`
/// tag byte a port-wide key can never produce, because the port-wide key is the fixed one-byte
/// string `"p"` — the two families cannot collide regardless of what a space is named.
fn journal_gen_space_key(space: Option<&str>) -> String {
    match space {
        None => "p".to_owned(),
        Some(space) => format!("s{space}"),
    }
}

/// The inverse of [`journal_gen_space_key`]: recovers the `Option<String>` shape a snapshot
/// payload and [`ClusterJournal::set_clear_gen`] both want from a stored key. Any key that is
/// not the literal `"p"` sentinel is a space-scoped key with the tag stripped — `strip_prefix`
/// returning `None` only for `"p"` itself is exactly the case that must decode to `None`.
fn decode_journal_gen_space_key(key: &str) -> Option<String> {
    key.strip_prefix('s').map(str::to_owned)
}

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

/// What `sm_configs` stores per port: the canonical config JSON,
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
///
/// **#549 removed five fields** — `sources`, `specs`, `spec_blobs`, `datasets` and
/// `dataset_blobs` — and **#550 three more** (`tenants`, `principals`, `bindings`)
/// along with the tables they carried, and dropped the tenant component from every
/// surviving row shape. Unlike [`ControlOp`], where a removed variant makes an old log
/// entry undecodable, removing a field here is *backward*-compatible on its own: this
/// struct sets no `deny_unknown_fields`, so a payload built before the removal still
/// parses and its extra keys are dropped — but a **changed tuple arity** is not, and
/// #550 changed several. The removal is a fleet-wide break overall (the `ControlOp`
/// variants and their encodings are gone), so a fleet upgrading across this commit
/// starts from a fresh `cluster-state-dir`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SnapshotPayload {
    /// `(port, stored-imposter JSON)` rows of `sm_configs`.
    configs: Vec<(u16, String)>,
    /// `(route id, route JSON)` rows of `sm_routes`. Defaulted so a snapshot
    /// built before issue #131 still installs cleanly on an upgraded node — it
    /// just carries no routes, the same as a fleet that never wrote any.
    #[serde(default)]
    routes: Vec<(String, String)>,
    /// The `sm_routes_revision` row (issue #210), absent when no route table has
    /// ever been written.
    ///
    /// Defaulted for the same reason `routes` is, and the failure it prevents is
    /// the one #210 exists to close: a node that installs a snapshot without it
    /// reads the table as revision 0, so a client holding a real token would be
    /// *refused* until the next write re-stamps it. Annoying, and deliberately the
    /// safe direction — `None` fails every stale precondition rather than passing
    /// one. The opposite default (inherit the last applied index) would let a
    /// token minted before the join silently pass here.
    #[serde(default)]
    routes_revision: Option<u64>,
    /// The `sm_session_key` row, if a console login has ever minted one (RFC-006 §5.3, issue
    /// #185). `#[serde(default)]` for the #134/#137 reason every table above carries it, and
    /// this is the failure shape if it is ever forgotten here: a node that installs a snapshot without it and then
    /// serves a console login mints a *second* key at a fresh revision, which silently
    /// invalidates every session issued by every other node — the exact fleet-wide logout this
    /// field exists to prevent from happening by accident.
    #[serde(default)]
    session_key: Option<String>,
    /// The `sm_fleet_name` row, if an operator has ever set one (issue #373). `#[serde(default)]`
    /// for the #134/#137 reason every table above carries it: a snapshot built before this field
    /// existed must still install, and a table omitted from this payload is a table that
    /// vanishes on the next follower catch-up. The failure if it were forgotten here is quieter
    /// than most of its siblings but still real: a node that joins by snapshot would silently
    /// forget the fleet's name and every surface reading it through that node would show
    /// "unnamed" until the next rename.
    #[serde(default)]
    fleet_name: Option<String>,
    /// `(port, space, generation)` rows of `sm_journal_gens` (issue #224).
    /// `#[serde(default)]` for the #134/#137 reason every table above carries it: a snapshot
    /// built before this field existed must still install, and the empty vec it decodes to means
    /// exactly what an upgrading fleet's history actually is — no clear has ever committed. The
    /// sharper failure than most of this table's siblings if it were ever forgotten here: a node
    /// that joins by snapshot and reads every generation as `0` would silently resurrect entries
    /// its peers have already agreed are cleared, the very inversion issue #224 exists to close.
    #[serde(default)]
    journal_gens: Vec<(u16, Option<String>, u64)>,
    /// `(port, sig-hash, recorded-response JSON)` rows of `sm_proxy_recorded` (#226).
    /// `#[serde(default)]` for the #134/#137 reason every table above carries it. The failure
    /// if it were forgotten: a node that joins by snapshot answers `Claimed` for signatures
    /// the fleet already recorded, and the engine calls the real upstream a second time — the
    /// exact duplicate `proxyOnce` exists to prevent.
    #[serde(default)]
    proxy_recorded: Vec<(u16, String, String)>,
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
    /// File name under [`RedbStateMachine::snapshot_dir`] holding the payload (#436).
    ///
    /// Invariant: by the time this row commits, that file is fully written, fsynced and renamed
    /// into place — the row never names a payload that is not already durable.
    file: String,
}

/// The pre-#436 row: the payload inlined as a JSON integer array (~3.7x its own size).
///
/// Read-only, and read exactly once — [`RedbStateMachine::migrate_legacy_snapshot_row`] converts it
/// to a file on first open. Distinguishable from [`StoredSnapshot`] by serde without a version tag:
/// a legacy row has no `file`, so the current shape fails with *missing field `file`*, and the
/// legacy shape's extra `data` is simply ignored when the current one is what is present.
#[derive(Debug, Deserialize)]
struct LegacyStoredSnapshot {
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
// `StorageError` is openraft's, carried here because this is the constructor openraft's
// storage contract expects. Same reason as the other sites in this file.
#[allow(clippy::result_large_err)]
pub async fn new<P: AsRef<Path>>(path: P) -> StorageResult<(RedbLogStore, RedbStateMachine)> {
    // Beside the database file, not inside it (#436): `path` is the redb file, so its parent is the
    // node's data directory. Taken before `Database::create` consumes `path`.
    let snapshot_dir = path
        .as_ref()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("snapshot");
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
        write_txn.open_table(SM_ROUTES_REVISION_TABLE).map_err(io)?;
        write_txn.open_table(SM_SESSION_KEY_TABLE).map_err(io)?;
        write_txn.open_table(SM_FLEET_NAME_TABLE).map_err(io)?;
        write_txn.open_table(SM_JOURNAL_GENS_TABLE).map_err(io)?;
        write_txn.open_table(SM_PROXY_RECORDED_TABLE).map_err(io)?;
        write_txn.open_table(SM_DEDUP_TABLE).map_err(io)?;
        write_txn.open_table(SM_APPLIED_TABLE).map_err(io)?;
        write_txn.open_table(PENDING_INTENTS_TABLE).map_err(io)?;
        write_txn
            .commit()
            .map_err(|e| StorageError::from(StorageIOError::write(&e)))?;
    }
    let db = Arc::new(db);
    std::fs::create_dir_all(&snapshot_dir)
        .map_err(|e| StorageError::from(StorageIOError::write(&e)))?;
    let sm = RedbStateMachine::new(db.clone(), snapshot_dir);
    sm.migrate_legacy_snapshot_row()?;
    Ok((RedbLogStore { db }, sm))
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

    // Called by openraft alone, behind the snapshot policy `raft/node.rs` configures — never by
    // an admin route (decision D-24).
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

/// An [`EngineAction`] paired with the principal whose committed op caused it
/// (U-10, upstream #855; issue #163).
///
/// This pairing exists because the upstream attribution channel is a
/// **task-local**, and a task-local does not cross a task boundary. The admin
/// request task opens `with_principal_scope` and then hands the op to openraft;
/// the mutation is applied later, on the state-machine task, where that scope is
/// long gone and `current_principal()` is `None`. So the clustered path carries
/// attribution in the log — `ControlRequest.principal` — and **re-opens** the
/// scope here, around the engine call, which is the only place a listener runs.
/// Without this, every clustered change event reaches M3's SSE with
/// `EventContext::principal == None`, and the fact that the log entry itself is
/// correctly attributed does not help it: it is on the event path, not the log
/// path.
///
/// `principal` is `None` for a drive with no single request behind it — a
/// restart replay or a snapshot install, which materialize a whole table rather
/// than one caller's write. That is the honest answer, and `EventContext`'s own
/// doc asks for exactly it: absent attribution is reported as absent, never
/// guessed.
#[derive(Debug)]
struct AttributedAction {
    principal: Option<String>,
    action: EngineAction,
}

impl AttributedAction {
    /// A drive that no single principal caused: restart reconciliation, or a
    /// snapshot install.
    fn unattributed(action: EngineAction) -> Self {
        Self {
            principal: None,
            action,
        }
    }
}

#[derive(Clone)]
pub struct RedbStateMachine {
    db: Arc<Database>,
    snapshot_idx: Arc<AtomicU64>,
    /// The local engine committed ops are projected onto. `None` in storage
    /// tests and while the embedder has not wired one — the state machine is
    /// then tables-only, which is exactly what the conformance suite exercises.
    engine: Option<Arc<ImposterManager>>,
    /// Per-port sequencing modes, refreshed from every applied config set (#466).
    sequencing: Option<Arc<SequencingRegistry>>,
    /// This node's flow-state shard, reached for exactly one thing: dropping a
    /// deleted imposter's `i<port>:` namespace when the engine removes the
    /// imposter (#565, the D-5 amendment). `None` in storage tests and on a
    /// node with no flow subsystem — the tables and the engine drive are
    /// unaffected; only the clear is skipped.
    flow_net: Option<Arc<FlowNet>>,
    /// The front door's hot-swappable compiled table (issue #131). `None` in
    /// storage tests and on a node that never binds a front door — routes are
    /// still replicated and readable from `sm_routes` either way, this is only
    /// the dispatch-side handle. Attach before `Raft::new` for the same reason
    /// as `engine`: replay during join must drive it too, not just live
    /// commits.
    routes: Option<Arc<ArcSwap<CompiledRoutes>>>,
    /// Where snapshot payload files live: `<redb's parent>/snapshot/` (#436, the D-16 amendment).
    ///
    /// Not an `Option` and not a builder: a snapshot has nowhere else it could go, so "no
    /// directory" is not a representable state. Derived from the path `new` already
    /// receives rather than plumbed through `NodeConfig`, which is also what keeps `raft/node.rs`
    /// out of this change entirely.
    snapshot_dir: PathBuf,
    /// Serialises everything that writes into [`Self::snapshot_dir`] (#436).
    ///
    /// openraft runs `build_snapshot` on a **detached, unabortable task** — `sm::worker` spawns it
    /// and the worker loop immediately takes the next command, which may be an `install_snapshot`
    /// — and its own docs require the builder to "acquire a lock that prevents any write
    /// operations". Without one, a finishing build's GC can unlink the temp file an install is
    /// still streaming into, and the failure surfaces later as a rename `ENOENT` that looks like
    /// disk trouble rather than a self-inflicted race.
    ///
    /// `Arc` because clones share the directory: `get_snapshot_builder` hands openraft a
    /// `self.clone()`.
    snapshot_guard: Arc<Mutex<()>>,
    /// Last engine side-effect failure per port, cleared when a later drive
    /// succeeds for that port. Key 0 is the set-level slot (an `apply_config`
    /// refusal that names no single port). This is node status, not replicated
    /// state — every replica has its own bind outcomes.
    apply_failures: Arc<Mutex<BTreeMap<u16, String>>>,
    /// This node's local request journal, late-bound (issue #224): `apply` pushes a committed
    /// clear generation into it via [`ClusterJournal::set_clear_gen`], and `install_snapshot`
    /// via [`ClusterJournal::reset_clear_gen`] — the monotone guard the apply path needs is
    /// exactly the guard install must *not* have (see that method's doc) — so this replica's
    /// own shards start dropping pre-clear entries immediately, without waiting for a caller to
    /// read `sm_journal_gens` back out. `reconcile_engine` covers the third case, a cold start:
    /// nothing re-delivers past `JournalClearGen` entries once openraft resumes from
    /// `last_applied_log`, so a freshly built journal is rehydrated from `sm_journal_gens`
    /// directly, the same way that method already rehydrates the engine and routes handle.
    ///
    /// `OnceLock<Weak<_>>`, mirroring `ClusterJournal`'s own late-bound `Voters::Node` slot (and
    /// `FlowNet`'s node slot): the journal is built in `compose.rs` before the Raft node exists
    /// (so it cannot be required at construction the way `db` is), and `Weak` for the same
    /// reason those are — this state machine must never be the thing keeping the journal's
    /// memory resident past shutdown. `None` in storage tests and on an embedder that never
    /// wires one, exactly like `engine`; a dropped handle degrades the push into a benign no-op
    /// (see the `JournalClearGen` arm of `mutate_tables`), never a panic — the generation the
    /// fleet agrees on is durable in `sm_journal_gens` either way, and a later snapshot install
    /// replays it into whatever journal eventually catches up.
    journal: OnceLock<Weak<ClusterJournal>>,
}

impl std::fmt::Debug for RedbStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbStateMachine")
            .field("engine", &self.engine.is_some())
            .field("routes", &self.routes.is_some())
            .field("journal", &self.journal.get().is_some())
            .field("snapshot_dir", &self.snapshot_dir)
            .finish_non_exhaustive()
    }
}

impl RedbStateMachine {
    fn new(db: Arc<Database>, snapshot_dir: PathBuf) -> Self {
        Self {
            db,
            snapshot_dir,
            snapshot_guard: Arc::new(Mutex::new(())),
            snapshot_idx: Arc::new(AtomicU64::new(0)),
            engine: None,
            sequencing: None,
            flow_net: None,
            routes: None,
            apply_failures: Arc::new(Mutex::new(BTreeMap::new())),
            journal: OnceLock::new(),
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

    /// Attach the per-port sequencing modes the clustered `ResponseSequencer`
    /// reads (issue #466, D-47). Same before-`Raft::new` contract as
    /// [`Self::with_engine`].
    ///
    /// The apply loop is the only place that sees every config, complete, on
    /// every change — a manager-wide sequencer is handed just a port and has no
    /// per-imposter hook of its own, so this is where its lookup gets filled.
    #[must_use]
    pub fn with_sequencing_registry(mut self, registry: Arc<SequencingRegistry>) -> Self {
        self.sequencing = Some(registry);
        self
    }

    /// Attach this node's flow-state shard, so a committed `DeleteImposter` /
    /// `DeleteAll` drops the deleted port's imposter-scoped flow state on this
    /// node (#565, the D-5 amendment). Same before-`Raft::new` contract as
    /// [`Self::with_engine`]: a delete replayed during a join or installed by
    /// a snapshot must clear too, not just a live commit.
    ///
    /// The apply loop is where this belongs for the same reason the sequencing
    /// registry lives here: it is the one place every node sees every
    /// committed config change, in order, exactly once — which is what makes
    /// the clear deterministic across the fleet rather than a request one node
    /// happened to receive.
    #[must_use]
    pub fn with_flow_net(mut self, flow_net: Arc<FlowNet>) -> Self {
        self.flow_net = Some(flow_net);
        self
    }

    /// Attach the front door's compiled-route handle. Same before-`Raft::new`
    /// contract as [`Self::with_engine`].
    #[must_use]
    pub fn with_routes_handle(mut self, routes: Arc<ArcSwap<CompiledRoutes>>) -> Self {
        self.routes = Some(routes);
        self
    }

    /// Attach this node's local request journal (issue #224), so `apply`/`install_snapshot` can
    /// push a committed clear generation into it. Same before-`Raft::new` contract as
    /// [`Self::with_engine`] — call before this state machine is cloned into `Raft::new` and
    /// into `sm_reader`, so both share the same bound handle from their first apply.
    ///
    /// Stores only a [`Weak`] (see the `journal` field's doc for why); does not need `&mut self`
    /// because the slot binds at most once (`OnceLock::set`), the same idempotent-bind contract
    /// `ClusterJournal::bind` itself keeps.
    #[must_use]
    pub fn with_journal(self, journal: &Arc<ClusterJournal>) -> Self {
        let _ = self.journal.set(Arc::downgrade(journal));
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
    /// Write a snapshot payload durably into [`Self::snapshot_dir`] and point the row at it (#436).
    ///
    /// Temp file -> fsync file -> rename -> fsync directory, so the `SNAPSHOT_TABLE` row committed
    /// afterwards can never name a payload that is not already on disk. `write` streams the payload
    /// in; callers never materialise it as one buffer.
    ///
    /// Superseded payload files are removed once the new one is durable — without that, every build
    /// leaves a full copy of the state machine behind.
    #[allow(clippy::result_large_err)]
    fn write_snapshot_file<F>(
        &self,
        meta: &SnapshotMeta<u64, BasicNode>,
        write: F,
    ) -> StorageResult<()>
    where
        F: FnOnce(&mut std::fs::File) -> std::io::Result<()>,
    {
        let io = |e: std::io::Error| {
            StorageError::from(StorageIOError::write_snapshot(Some(meta.signature()), &e))
        };
        // Held across the whole write: see `snapshot_guard`. A concurrent build and install
        // otherwise share this directory with no coordination at all.
        let _guard = self.snapshot_guard.lock();
        std::fs::create_dir_all(&self.snapshot_dir).map_err(io)?;
        let tmp = self.snapshot_dir.join(format!("tmp-{}", meta.snapshot_id));
        {
            // 0o600: a snapshot payload carries `session_key` and every `principals` row, so it
            // is the most sensitive file this node writes.
            #[cfg(unix)]
            let mut file = {
                use std::os::unix::fs::OpenOptionsExt;
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o600)
                    .open(&tmp)
                    .map_err(io)?
            };
            #[cfg(not(unix))]
            let mut file = std::fs::File::create(&tmp).map_err(io)?;
            write(&mut file).map_err(io)?;
            file.sync_all().map_err(io)?;
        }
        let final_path = self.snapshot_dir.join(&meta.snapshot_id);
        std::fs::rename(&tmp, &final_path).map_err(io)?;
        // Renaming is only durable once the *directory* entry is, which is the step that makes the
        // row's invariant hold across a power loss rather than merely across a process exit.
        #[cfg(unix)]
        std::fs::File::open(&self.snapshot_dir)
            .and_then(|dir| dir.sync_all())
            .map_err(io)?;
        Ok(())
    }

    /// Point `SNAPSHOT_TABLE` at the payload file for `meta`.
    ///
    /// Always called *after* [`Self::write_snapshot_file`], never before: the row is the claim that
    /// a durable payload exists, so committing it first would leave a window where the claim is
    /// false. `install_snapshot` does not use this — it writes the same row inside the single
    /// transaction that installs the tables, so the state and the snapshot that produced it commit
    /// together or not at all.
    #[allow(clippy::result_large_err)]
    fn commit_snapshot_row(&self, meta: &SnapshotMeta<u64, BasicNode>) -> StorageResult<()> {
        let row = serde_json::to_vec(&StoredSnapshot {
            meta: meta.clone(),
            file: meta.snapshot_id.clone(),
        })
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
                .insert((), row.as_slice())
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        }
        write_txn
            .commit()
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        // Only now: until this row is committed the *previous* payload is still the live one, and
        // sweeping it beforehand leaves a window where a crash strands a committed row pointing at
        // a file that has already been deleted.
        self.gc_snapshot_files(&meta.snapshot_id);
        Ok(())
    }

    /// Remove every payload file in [`Self::snapshot_dir`] except `keep`, plus any `tmp-`/
    /// `receiving-` leftovers from an interrupted build or transfer.
    ///
    /// Best-effort by design: a file that cannot be removed is wasted disk, not incorrect state,
    /// and failing a completed snapshot over it would trade a real guarantee for a cosmetic one.
    /// Logged so it is visible rather than silent.
    fn gc_snapshot_files(&self, keep: &str) {
        // Anything touched inside this window may belong to an operation still in flight — an
        // install streaming chunks into its `receiving-` file (which happens *outside*
        // `snapshot_guard`, between `begin_receiving_snapshot` and `install_snapshot`), or a build
        // that has renamed its payload but not yet committed the row naming it. Deleting either is
        // how a GC turns into data loss, so age is the guard: a genuine leftover is minutes old,
        // an in-flight file is seconds old.
        const GRACE: std::time::Duration = std::time::Duration::from_secs(300);

        let entries = match std::fs::read_dir(&self.snapshot_dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(error = %e, "could not scan the snapshot directory to GC old payloads");
                return;
            }
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name == keep {
                continue;
            }
            let recent = entry
                .metadata()
                .and_then(|m| m.modified())
                .and_then(|t| t.elapsed().map_err(std::io::Error::other))
                .map(|age| age < GRACE)
                // Unreadable mtime: treat the file as recent and leave it. Wasted disk is
                // recoverable on the next sweep; deleting a live transfer is not.
                .unwrap_or(true);
            if recent {
                continue;
            }
            if let Err(e) = std::fs::remove_file(entry.path()) {
                tracing::warn!(file = %name, error = %e, "could not remove a superseded snapshot payload");
            }
        }
    }

    /// Open the payload file for `snapshot_id` as the handle openraft streams from.
    #[allow(clippy::result_large_err)]
    fn open_snapshot_file(&self, snapshot_id: &str) -> StorageResult<tokio::fs::File> {
        let path = self.snapshot_dir.join(snapshot_id);
        let file =
            std::fs::File::open(&path).map_err(|e| StorageIOError::read_snapshot(None, &e))?;
        Ok(tokio::fs::File::from_std(file))
    }

    /// Convert a pre-#436 snapshot row (payload inlined as a JSON integer array) into a payload
    /// file plus a `{meta, file}` row, once, at open.
    ///
    /// Migrated rather than discarded: a node restarted onto a new binary part-way through catching
    /// a peer up must not lose the snapshot it already holds. A row already in the current shape,
    /// or no row at all, is left untouched.
    #[allow(clippy::result_large_err)]
    fn migrate_legacy_snapshot_row(&self) -> StorageResult<()> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?;
        let table = read_txn
            .open_table(SNAPSHOT_TABLE)
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?;
        let Some(guard) = table
            .get(())
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?
        else {
            return Ok(());
        };
        let bytes = guard.value().to_vec();
        drop(read_txn);

        if serde_json::from_slice::<StoredSnapshot>(&bytes).is_ok() {
            return Ok(());
        }
        // Not the current shape — the only other thing it can legitimately be is the pre-#436 one.
        // A parse failure here is propagated, not swallowed: silently dropping a snapshot would
        // look identical to a fleet that never had one.
        let legacy: LegacyStoredSnapshot =
            serde_json::from_slice(&bytes).map_err(|e| StorageIOError::read_snapshot(None, &e))?;

        self.write_snapshot_file(&legacy.meta, |file| {
            std::io::Write::write_all(file, &legacy.data)
        })?;
        self.commit_snapshot_row(&legacy.meta)?;
        tracing::info!(
            snapshot_id = %legacy.meta.snapshot_id,
            bytes = legacy.data.len(),
            "migrated a pre-#436 inlined snapshot row to a payload file"
        );
        Ok(())
    }

    /// Read the applied config JSON for `port`, or `None` if no config has been
    /// applied for it.
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
            .get(port)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|g| {
                serde_json::from_str::<StoredImposter>(g.value())
                    .map(|stored| stored.config_json)
                    .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))
            })
            .transpose()
    }

    /// Every port fleet-wide that currently has an applied config, ascending —
    /// `redb` iterates `sm_configs` key-ordered on the port.
    ///
    /// Ports rather than bodies: this backs the operator surface
    /// `GET /_cluster/config`, which reports *what* the node has converged on,
    /// and a fleet's full config set is far larger than the answer to that.
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
            ports.push(key.value());
        }
        Ok(ports)
    }

    /// The route table, as currently applied. Like [`Self::read_config`],
    /// this is the node's own read path — it answers from local durable state
    /// without a Raft round trip. It is also the *only* read path for routes:
    /// upstream has no `GET /front-door/routes` to proxy to (U-11's admin CRUD
    /// was deferred), so `GET /front-door/routes` in the clustered admin front
    /// calls straight through to this.
    #[allow(clippy::result_large_err)]
    pub fn route_table(&self) -> StorageResult<RouteTable> {
        Ok(self.route_table_with_revision()?.0)
    }

    /// The route table and the revision it is at, read in **one** redb
    /// transaction (issue #210).
    ///
    /// One transaction is the whole point, not tidiness: two separate reads
    /// could observe a table and a revision from either side of a concurrent
    /// apply. Reading the table first and the revision second is the dangerous
    /// order — the caller would hold a *newer* revision than the content it
    /// saw, condition a whole-table replace on it, and silently drop the write
    /// that landed in between. A single read transaction sees one consistent
    /// snapshot and the question does not arise.
    #[allow(clippy::result_large_err)]
    pub fn route_table_with_revision(&self) -> StorageResult<(RouteTable, u64)> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let revision = read_txn
            .open_table(SM_ROUTES_REVISION_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .get(ROUTES_REVISION_ROW)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map_or(0, |v| v.value());
        let table = read_txn
            .open_table(SM_ROUTES_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut routes = Vec::new();
        for item in table
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let id = key.value();
            match serde_json::from_str::<Route>(value.value()) {
                Ok(route) => routes.push(route),
                Err(e) => {
                    tracing::error!(route_id = %id, error = %e, "corrupt stored route");
                    return Err(StorageError::from(StorageIOError::read_state_machine(
                        &std::io::Error::other(format!(
                            "corrupt stored route {id}: stored route will not parse: {e}"
                        )),
                    )));
                }
            }
        }
        Ok((RouteTable { routes }, revision))
    }

    /// The stored revision of one imposter, or `None` when the applied state
    /// holds no record for `port`.
    ///
    /// This is the read half of the single-imposter `If-Match` contract (C5,
    /// issue #188): the front stamps it onto the proxied imposter read so an
    /// editor holds a conditionable token *before* its first write. It reads
    /// the same `sm_configs` row `check_expected_revision` compares against —
    /// a token minted anywhere else could disagree with the precondition that
    /// will judge it.
    ///
    /// A record that will not parse reads as `None` rather than an error: the
    /// read itself (served by the engine) still succeeds, and answering it
    /// with no token merely leaves that imposter unconditionable.
    #[allow(clippy::result_large_err)]
    pub fn imposter_revision(&self, port: u16) -> StorageResult<Option<u64>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let Some(guard) = table
            .get(port)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        else {
            return Ok(None);
        };
        match serde_json::from_str::<StoredImposter>(guard.value()) {
            Ok(stored) => Ok(Some(stored.revision)),
            Err(e) => {
                tracing::error!(port, error = %e, "corrupt stored imposter; read carries no revision token");
                Ok(None)
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

    /// Why the local engine is serving `port` **in-process only** — it holds the imposter but never
    /// bound its port (RFC-001 §7.4.6, issue #143). `None` when the port is healthy, when this node
    /// is not serving it at all, or when there is no local engine.
    ///
    /// Narrower than [`Self::apply_failures`] on purpose, and the distinction is the whole point.
    /// That map records *every* kind of engine-side failure — a stored record that will not parse,
    /// a refused `SetEnabled`, a rejected stub patch, an unreadable TLS cert — and stringifies the
    /// error, discarding which kind it was. Reporting any of those as a bind failure would tell an
    /// operator "this node is still serving it in-process", which for every one of those cases is
    /// false: the imposter is not in the map at all, and the read they are looking at is a 404.
    /// So the engine's own [`Imposter::is_bound`] is the authority here, not the failure string.
    #[must_use]
    pub fn bind_failure(&self, port: u16) -> Option<String> {
        let engine = self.engine.as_ref()?;
        if engine
            .get_imposter(port)
            .is_ok_and(|imposter| !imposter.is_bound())
        {
            return self.apply_failures.lock().get(&port).cloned();
        }
        None
    }

    /// Is this node's own engine actually **holding `port`'s socket** right now?
    ///
    /// The positive counterpart to [`Self::bind_failure`], and not a rephrasing of it: that one
    /// answers `None` both for "healthy" and for "this node is not serving the port at all", so
    /// `bind_failure(port).is_none()` is true for a port this node has never heard of. Anything
    /// deciding whether it is safe to *talk to* `127.0.0.1:port` needs the positive form, because
    /// the two cases differ exactly where it matters: an unbound port is a socket some other
    /// process may hold.
    ///
    /// This gap is real, not defensive: a `PutImposter` whose bind fails still commits and still
    /// reads back (`bind_failure_does_not_fail_apply`), by design — a bind failure must not wedge
    /// the replicated log. So a committed config proves the *record* exists and proves nothing
    /// whatever about who is listening on that port.
    #[must_use]
    pub fn is_locally_bound(&self, port: u16) -> bool {
        self.engine.as_ref().is_some_and(|engine| {
            engine
                .get_imposter(port)
                .is_ok_and(|imposter| imposter.is_bound())
        })
    }

    /// Answer a request as `port`'s imposter would, **in-process** (issue #344).
    ///
    /// The try endpoint's whole containment claim is that its answer comes from the imposter this
    /// node owns — not from whatever happens to hold `127.0.0.1:port`, which on BSD can be a
    /// different socket than the one [`Self::is_locally_bound`] just proved this engine holds (the
    /// wildcard/REUSEPORT/`localhost`-vs-`::1` variants a loopback dial cannot tell apart). Routing
    /// the exchange through this engine instead of a socket closes all of those at once, by
    /// construction: there is no address to misroute to, because nothing is addressed.
    ///
    /// `None` when there is no local engine, or `engine.get_imposter(port)` is `Err` — this node
    /// does not hold `port` at all, which must never be answered as if it did. `Some` answers from
    /// the **`Arc<Imposter>` resolved right here**, through [`handle_imposter_request`] — the
    /// per-imposter half of `dispatch_to_port`, the seam #317 gives the `/__rift/` gateway, with
    /// the same synthetic loopback `client_addr` the gateway records. Not `dispatch_to_port`
    /// itself, deliberately: that re-resolves the port inside, and an imposter deleted between
    /// this lookup and that one would be answered with the engine's own "no imposter on port"
    /// `404` — a fabricated answer for a vanished imposter, which is exactly what the `None`
    /// contract above forbids. Holding the `Arc` closes that window: a deleted imposter's last
    /// exchange still runs against the imposter that was there when the try was admitted.
    ///
    /// Returns an owned `'static` future rather than borrowing `&self` across the await, because
    /// the caller (the admin front's `perform_try`) runs it inside a spawned hyper connection,
    /// which must be `'static`. The manager itself never leaves `raft/` — only this one resolved
    /// imposter's exchange does.
    pub fn dispatch_to_imposter(
        &self,
        port: u16,
        req: Request<Incoming>,
    ) -> Option<impl Future<Output = Response<Full<Bytes>>> + Send + 'static> {
        let imposter = self.engine.as_ref()?.get_imposter(port).ok()?;
        // The same address the gateway stamps on what it forwards: the imposter is being
        // reached by this process, not by a peer, and the recorded `request_from` says so.
        let client_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
        Some(async move {
            match handle_imposter_request(req, imposter, client_addr).await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        })
    }

    /// Every port this node's engine holds, split by whether it actually got the socket (issue
    /// #369, blocker B4). `None` when there is no local engine — this node cannot observe binds at
    /// all, which is a different claim from "it observed nothing wrong".
    ///
    /// A single in-memory pass over [`ImposterManager::list_imposters`], not a redb transaction:
    /// `/_cluster/members` (and therefore `/_fleet/members`) used to derive this from
    /// `configured_ports`, a redb read transaction scanning the fleet-wide `SM_CONFIGS` table —
    /// turning a 5-second console poll, fanned out to every peer, into an O(all imposters in the
    /// fleet) table scan on every node. The engine already holds
    /// exactly the set this needs, in memory, so this walks that instead.
    ///
    /// The same narrowing [`Self::bind_failure`] documents applies per port: a port the engine
    /// holds and has bound goes in `bound_ports`; a port it holds, has not bound, and has a
    /// recorded failure for goes in the failure map (never the general `apply_failures` failure —
    /// a parse, cert, or stub-patch failure must not be mislabelled as a bind failure); a port the
    /// engine does not hold at all — never applied on this node — lands in **neither** collection.
    /// `is_bound()` is tested first, so the two collections stay disjoint by construction, the same
    /// invariant `bind_failure`/`is_locally_bound` rest on.
    #[must_use]
    pub fn local_bind_report(&self) -> Option<(Vec<u16>, BTreeMap<u16, String>)> {
        let engine = self.engine.as_ref()?;
        let apply_failures = self.apply_failures.lock();
        let mut bound_ports = Vec::new();
        let mut failures = BTreeMap::new();
        for imposter in engine.list_imposters() {
            let Some(port) = imposter.config.port else {
                continue;
            };
            if imposter.is_bound() {
                bound_ports.push(port);
            } else if let Some(reason) = apply_failures.get(&port) {
                failures.insert(port, reason.clone());
            }
        }
        Some((bound_ports, failures))
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

    /// How many intents are parked, without parsing any of them (issue #360).
    ///
    /// Separate from [`parked_intents`](Self::parked_intents) rather than
    /// `parked_intents()?.len()`, because the two have very different costs and
    /// this one is on a polled path: the console's queue-depth tile reads it
    /// every few seconds, from every node in the fleet. Parsing every parked
    /// `ControlRequest` to arrive at a number would do work proportional to the
    /// backlog precisely when the backlog is the problem.
    ///
    /// The count is the table's, so it includes rows that would not parse.
    /// That is the honest answer for a *depth*: an unparseable row is still
    /// work this node accepted and has not retired, and the replay loop's
    /// decision to drop it is a separate concern from how much is outstanding.
    #[allow(clippy::result_large_err)]
    pub fn parked_intent_count(&self) -> StorageResult<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(PENDING_INTENTS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        table
            .len()
            .map_err(|e| StorageIOError::read_state_machine(&e).into())
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
    ///
    /// Also rehydrates the local [`ClusterJournal`]'s clear generations from `sm_journal_gens`
    /// (issue #224, Blocker 2). The generation lives in two places: durably in redb, and in the
    /// process-local journal that stamps every appended entry. A restart rebuilds only the
    /// latter from scratch — openraft resumes replay from `last_applied_log`, so the
    /// `JournalClearGen` entries that built the durable rows are never re-applied, and nothing
    /// else re-primes the in-memory copy. Without this, a restarted node's `clear_gen` silently
    /// reads back as `0` — "never cleared" — and every subsequent request it records is stamped
    /// as pre-clear, resurrecting it fleet-wide the moment a merge runs, with no error and no
    /// metric. A no-op without a bound journal, same as the engine drive above.
    #[allow(clippy::result_large_err)]
    pub async fn reconcile_engine(&self) -> StorageResult<()> {
        self.reconcile_engine_interleaved(std::future::ready(()))
            .await
    }

    /// [`Self::reconcile_engine`] with a seam inside its orphan sweep, between
    /// the sweep's two reads. Production passes a ready future; the test that
    /// pins the sweep's bound (#567 review) commits an imposter there, on a
    /// second state-machine handle, exactly where the apply loop can in
    /// `compose`. One body for both, so the ordering under test is the
    /// ordering shipped.
    ///
    /// **That point, not "between the sync and the sweep" (#573 review).** The
    /// sweep's verdict is decided by two reads — the held set and the desired
    /// set — and a concurrent apply changes the verdict only while it sits
    /// between them. An apply landing before the first read or after the second
    /// is seen by both or by neither. So this is the worst case, and it is
    /// strictly later than the old seam: a commit here is also a commit after
    /// the engine sync, which is what the earlier placement tested.
    #[allow(clippy::result_large_err)]
    async fn reconcile_engine_interleaved(
        &self,
        between_sweep_reads: impl Future<Output = ()>,
    ) -> StorageResult<()> {
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
        // Whether the sweep below follows a sync that happened. A `RefuseSync`
        // leaves the engine holding whatever it held, and the sweep is the
        // other half of the same reconcile: with the engine not rebuilt it
        // does nothing either, rather than clear against a set the engine
        // itself was not trusted with.
        let config_synced = matches!(config_action, EngineAction::Sync(_));
        // Unattributed: this materializes a whole table on restart, not one
        // caller's write, so there is no principal to name.
        self.drive_engine(vec![
            AttributedAction::unattributed(config_action),
            AttributedAction::unattributed(routes_action),
        ])
        .await;
        if config_synced {
            self.sweep_orphaned_imposter_state(between_sweep_reads)
                .await?;
        } else {
            // Still awaited on the refuse path, so an interleaving a test asks
            // for is never silently dropped along with the sweep.
            between_sweep_reads.await;
        }

        // Blocker 2: rehydrate this node's local journal from the durable generations table —
        // see this method's doc for why nothing else does. Gated on a bound journal before
        // opening the table at all, matching every other late-bound handle's "missing is a
        // benign no-op" contract in this file.
        if let Some(journal) = self.journal.get().and_then(Weak::upgrade) {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let table = read_txn
                .open_table(SM_JOURNAL_GENS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            for item in table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let (port, space_key) = key.value();
                // `set_clear_gen`, not `reset_clear_gen`: this is priming a journal that starts
                // at 0, not correcting one that may be ahead the way a snapshot install must —
                // the monotone guard is harmless here and keeps this call sharing the apply
                // path's contract rather than install's.
                journal.set_clear_gen(
                    port,
                    decode_journal_gen_space_key(space_key).as_deref(),
                    value.value(),
                );
            }
        }

        Ok(())
    }

    /// Test-only: overwrite a raw `sm_configs` row, bypassing validation — the
    /// broken-record refusal path is unreachable through the public API.
    #[cfg(test)]
    fn inject_raw_config(&self, port: u16, value: &str) {
        let txn = self.db.begin_write().expect("test txn");
        {
            let mut table = txn.open_table(SM_CONFIGS_TABLE).expect("test table");
            table.insert(port, value).expect("test insert");
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

    /// The fleet's session-signing key, or `None` when no console login has minted one yet
    /// (RFC-006 §5.3, issue #185). Answered from local applied state.
    ///
    /// # Errors
    /// Storage I/O, or a stored record that will not parse.
    #[allow(clippy::result_large_err)]
    pub fn session_key(&self) -> StorageResult<Option<SessionKey>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_SESSION_KEY_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let Some(value) = table
            .get(SESSION_KEY_ROW)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        else {
            return Ok(None);
        };
        // Surfaced, never defaulted away: an unparseable key record must not read as "no key
        // minted yet", which would silently mint a second one and rotate every session out from
        // under whoever was relying on the first.
        let record: SessionKey = serde_json::from_str(value.value()).map_err(|e| {
            tracing::error!(error = %e, "corrupt stored session key record");
            StorageError::from(StorageIOError::read_state_machine(&e))
        })?;
        Ok(Some(record))
    }

    /// The fleet's operator-set name, or `None` when nobody has named it yet (issue #373).
    /// Answered from local applied state, like `session_key`.
    ///
    /// Stored as the bare string rather than a JSON-wrapped record: unlike `SessionKey` there is
    /// no revision or other metadata to carry alongside it, so wrapping it would only add a
    /// parse step with nothing to parse.
    ///
    /// # Errors
    /// Storage I/O.
    #[allow(clippy::result_large_err)]
    pub fn fleet_name(&self) -> StorageResult<Option<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_FLEET_NAME_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let Some(value) = table
            .get(FLEET_NAME_ROW)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        else {
            return Ok(None);
        };
        Ok(Some(value.value().to_owned()))
    }

    /// The applied clear generation for `port` (or `port`'s `space`, when given); `0` if
    /// `ControlOp::JournalClearGen` has never committed for that key (issue #224).
    ///
    /// # Errors
    /// Storage I/O.
    #[allow(clippy::result_large_err)]
    pub fn journal_gen(&self, port: u16, space: Option<&str>) -> StorageResult<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_JOURNAL_GENS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let space_key = journal_gen_space_key(space);
        Ok(table
            .get((port, space_key.as_str()))
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map_or(0, |v| v.value()))
    }

    /// The applied proxy-recording marker for `(port, sig_hash)` (#226): the
    /// recorded-response JSON `ControlOp::ProxyRecorded` committed, or `None` when the
    /// signature has never been recorded (or was cleared). Local durable state — any node
    /// answers without leadership, which is what lets a post-handoff owner say
    /// `AlreadyRecorded` with no in-memory trace of the claim.
    ///
    /// # Errors
    /// Storage I/O.
    #[allow(clippy::result_large_err)]
    pub fn proxy_recorded_resp(&self, port: u16, sig_hash: &str) -> StorageResult<Option<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_PROXY_RECORDED_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(table
            .get((port, sig_hash))
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|v| v.value().to_owned()))
    }

    /// The desired engine state as of now, read from an open (possibly
    /// mid-transaction) view of `sm_configs`: every applied config,
    /// parsed — disabled ones included (a paused imposter stays bound, #817).
    ///
    /// `Ok(Err((port, reason)))` means a stored record failed to parse. That
    /// must abort the sync, not shrink it: `apply_config` deletes every live
    /// imposter missing from the desired set, so silently skipping a broken
    /// record would tear down a healthy imposter and report it as an
    /// operator-issued delete. The caller refuses the sync and records the
    /// failure instead — the engine keeps serving its last-known state.
    fn desired_configs(
        table: &impl ReadableTable<u16, &'static str>,
    ) -> Result<Result<Vec<ImposterConfig>, (u16, String)>, redb::StorageError> {
        let mut desired = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let port = key.value();
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
    fn sync_action(configs: &Table<'_, u16, &'static str>) -> StorageResult<EngineAction> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        Ok(match Self::desired_configs(configs).map_err(io)? {
            Ok(desired) => EngineAction::Sync(desired),
            Err((port, error)) => EngineAction::RefuseSync { port, error },
        })
    }

    /// The desired route table as of now, read from an open (possibly
    /// mid-transaction) view of `sm_routes`.
    ///
    /// **Every stored route is compiled in.** Before #550 this filtered to the default
    /// tenant's routes, because the front door is a single listener with no tenant
    /// discriminator and a unioned table would have let any tenant publish a catch-all
    /// that captured the whole fleet's traffic (D-68). With one fleet-wide table there is
    /// nothing to filter and nobody to shadow: what is stored is what dispatches, which is
    /// why D-68's `installed` field went with tenancy.
    ///
    /// `Ok(Err((id, reason)))` means a stored record failed to parse — this
    /// crate is the only writer of `sm_routes`, so it should never happen in
    /// practice, but the read path stays defensive rather than trusting that.
    fn desired_routes(
        table: &impl ReadableTable<&'static str, &'static str>,
    ) -> Result<Result<RouteTable, (String, String)>, redb::StorageError> {
        let mut routes = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let id = key.value();
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
        routes: &Table<'_, &'static str, &'static str>,
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
        configs: &Table<'_, u16, &'static str>,
        routes_revision: &Table<'_, &'static str, u64>,
        op: &ControlOp,
        expected: u64,
    ) -> StorageResult<Result<(), String>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let Some(target) = control::precondition_target(op) else {
            return Ok(Err(
                "revision conflict: expected-revision preconditions apply to single-imposter and \
                 route-table operations only"
                    .to_owned(),
            ));
        };
        match target {
            PreconditionTarget::Imposter(port) => match configs.get(port).map_err(io)? {
                None => Ok(Err(format!(
                    "revision conflict: expected revision {expected} but no imposter on port \
                         {port}"
                ))),
                Some(guard) => match serde_json::from_str::<StoredImposter>(guard.value()) {
                    Ok(record) if record.revision == expected => Ok(Ok(())),
                    Ok(record) => Ok(Err(format!(
                        "revision conflict: expected revision {expected}, stored revision \
                             {actual} on port {port}",
                        actual = record.revision
                    ))),
                    Err(e) => {
                        tracing::error!(port, error = %e, "corrupt stored record");
                        Ok(Err(format!("corrupt stored record for port {port}: {e}")))
                    }
                },
            },
            // No row means the table has never been written: revision 0 (issue
            // #210). Absence is a real revision here, not a missing record —
            // unlike the imposter arm above, where there is nothing to condition
            // on at all — so it compares rather than refuses, and a client that
            // conditions on 0 and writes first legitimately wins.
            PreconditionTarget::RouteTable => {
                let actual = routes_revision
                    .get(ROUTES_REVISION_ROW)
                    .map_err(io)?
                    .map_or(0, |v| v.value());
                if actual == expected {
                    Ok(Ok(()))
                } else {
                    Ok(Err(format!(
                        "revision conflict: expected revision {expected}, stored revision \
                         {actual} for the route table"
                    )))
                }
            }
        }
    }

    // Six table handles plus the journal, the op and the index: every one is a distinct piece
    // of the apply transaction, and grouping them into a struct would only move the arity.
    #[allow(clippy::too_many_arguments)]
    // `StorageError` is openraft's, carried here because this is the apply path openraft's
    // storage contract expects. Same reason as the other sites in this file.
    #[allow(clippy::result_large_err)]
    fn mutate_tables(
        configs: &mut Table<'_, u16, &'static str>,
        routes: &mut Table<'_, &'static str, &'static str>,
        routes_revision: &mut Table<'_, &'static str, u64>,
        session_key: &mut Table<'_, &'static str, &'static str>,
        fleet_name: &mut Table<'_, &'static str, &'static str>,
        journal_gens: &mut Table<'_, (u16, &'static str), u64>,
        proxy_recorded: &mut Table<'_, (u16, &'static str), &'static str>,
        // The local journal to push a committed generation into (issue #224), resolved once by
        // `apply` rather than upgraded per op — `None` in storage tests, on an embedder that
        // never wires one, or when a shutdown race has already dropped it (see the `journal`
        // field's doc on `RedbStateMachine`).
        journal: Option<&ClusterJournal>,
        op: &ControlOp,
        index: u64,
    ) -> StorageResult<Result<Vec<EngineAction>, String>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        match op {
            ControlOp::PutImposter { config } => {
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
                configs.insert(port, value.as_str()).map_err(io)?;
                // A replace is a new imposter in the recording sense (#226): the old
                // config's recorded stubs are gone from the stub list this put installs,
                // and a surviving marker row would answer `AlreadyRecorded` with the *old*
                // upstream's response — so the markers die with the config they described,
                // exactly as they do on `DeleteImposter`.
                proxy_recorded.retain(|(p, _), _| p != port).map_err(io)?;
                crate::metrics::config_applied(port, index);
                Ok(Ok(vec![Self::sync_action(configs)?]))
            }
            ControlOp::PatchStubs { port, edit } => {
                // Block-scoped so the read guard's borrow of `configs` ends
                // before the insert below.
                let mut record: StoredImposter = {
                    match configs.get(*port).map_err(io)? {
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
                configs.insert(*port, value.as_str()).map_err(io)?;
                crate::metrics::config_applied(*port, index);
                Ok(Ok(vec![EngineAction::Patch {
                    port: *port,
                    edit: edit.clone(),
                }]))
            }
            ControlOp::DeleteImposter { port } => {
                // Removing an absent port is a no-op, not a failure: deletes are
                // idempotent at the state-machine level (the admin-surface 404
                // for a missing imposter is the write path's concern).
                configs.remove(*port).map_err(io)?;
                // Recordings die with their imposter (#226) — the clustered mirror of the
                // manager's own port-reclaim `clear`, and atomic with the delete here.
                proxy_recorded.retain(|(p, _), _| p != *port).map_err(io)?;
                crate::metrics::config_removed(*port);
                Ok(Ok(vec![Self::sync_action(configs)?]))
            }
            ControlOp::DeleteAll => {
                let removed: Vec<u16> = {
                    let mut removed = Vec::new();
                    for item in configs.iter().map_err(io)? {
                        let (key, _value) = item.map_err(io)?;
                        removed.push(key.value());
                    }
                    removed
                };
                configs.retain(|_, _| false).map_err(io)?;
                proxy_recorded.retain(|_, _| false).map_err(io)?;
                for port in removed {
                    crate::metrics::config_removed(port);
                }
                Ok(Ok(vec![Self::sync_action(configs)?]))
            }
            ControlOp::SetEnabled { port, enabled } => {
                let mut record: StoredImposter = {
                    match configs.get(*port).map_err(io)? {
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
                configs.insert(*port, value.as_str()).map_err(io)?;
                crate::metrics::config_applied(*port, index);
                Ok(Ok(vec![EngineAction::SetEnabled {
                    port: *port,
                    enabled: *enabled,
                }]))
            }
            ControlOp::PutRoutes { table } => {
                // Whole-table replace: clear every row, then insert
                // the validated set. `validate` already confirmed the table
                // as a unit, so there is nothing left to check here — only to
                // store, deterministically, on every replica.
                routes.retain(|_, _| false).map_err(io)?;
                for route in &table.routes {
                    let value = serde_json::to_string(route)
                        .map_err(|e| StorageIOError::write_state_machine(&e))?;
                    routes
                        .insert(route.id.as_str(), value.as_str())
                        .map_err(io)?;
                }
                // The applying log index, so the stamp is the same on every
                // replica and is exactly the revision the front reports back to
                // the client that caused it (issue #210).
                routes_revision
                    .insert(ROUTES_REVISION_ROW, index)
                    .map_err(io)?;
                Ok(Ok(vec![Self::sync_routes_action(routes)?]))
            }
            ControlOp::DeleteRoute { id } => {
                // Idempotent no-op if absent, like `DeleteImposter` — the
                // admin-surface 404 for a missing route (if the operator
                // wants one) is the write path's concern, not apply's.
                routes.remove(id.as_str()).map_err(io)?;
                // Stamped even when the remove found nothing: the revision is
                // the table's, and this op *committed* against that table, so
                // an outstanding precondition must not survive it (issue #210).
                // Making the stamp conditional on the row's existence would
                // make the revision depend on state the client cannot see.
                routes_revision
                    .insert(ROUTES_REVISION_ROW, index)
                    .map_err(io)?;
                Ok(Ok(vec![Self::sync_routes_action(routes)?]))
            }
            ControlOp::SessionKeyPut { key } => {
                // Overwrites unconditionally: minting the *first* key and rotating an existing
                // one are the same op (RFC-006 §5.3), and the record's `revision` — stamped from
                // this apply's log index, not carried in the op — is what `session::verify`
                // binds into every token it mints. A rotation therefore invalidates every
                // outstanding session the instant this commits, on every replica, with no table
                // of live sessions to sweep.
                let record = SessionKey {
                    key: key.clone(),
                    revision: index,
                };
                let value = serde_json::to_string(&record)
                    .map_err(|e| StorageError::from(StorageIOError::write_state_machine(&e)))?;
                session_key
                    .insert(SESSION_KEY_ROW, value.as_str())
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::FleetNamePut { name } => {
                // Overwrites unconditionally, same reasoning as `SessionKeyPut` above: setting
                // the first name and renaming are one op, so the second write must replace the
                // first outright rather than accumulate — a fleet with two names is exactly the
                // confusion this feature exists to make impossible.
                fleet_name
                    .insert(FLEET_NAME_ROW, name.as_str())
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::JournalClearGen { port, space } => {
                // Whether an imposter exists on `port` is deliberately not checked here — see
                // `validate`'s doc for this op. A clear is a convergence primitive, not a config
                // write: it must succeed even against a port nothing has configured yet, the
                // same way `ClusterJournal::set_clear_gen` creates the shard on first touch
                // rather than refusing an unknown one.
                let space_key = journal_gen_space_key(space.as_deref());
                let key = (*port, space_key.as_str());
                // Apply *increments* rather than storing a value the submitter chose (see the
                // op's own doc on `ControlOp::JournalClearGen`): two clears racing from two
                // different leaders both take effect, composing to +2 — harmlessly stronger than
                // either alone, since both mean "ignore everything before me" — rather than the
                // second silently overwriting the first with the identical number.
                let current = journal_gens.get(key).map_err(io)?.map_or(0, |v| v.value());
                let next = current + 1;
                journal_gens.insert(key, next).map_err(io)?;
                // Pushed into this replica's own local shard(s) now, not deferred to
                // `drive_engine`: unlike an engine bind, `ClusterJournal::set_clear_gen` is an
                // infallible in-memory `fetch_max` with nothing to retry or report a failure
                // for, so there is no reason to give it the async, failure-tracked treatment the
                // engine gets. A missing handle (see the field's doc) is a benign no-op — the
                // generation this fleet agrees on is durable in `sm_journal_gens` regardless, and
                // any journal that binds or catches up later reads it from there (a snapshot
                // install replays every row; a late `bind` finds the redb table already correct
                // the next time this op's effect is asked about through it).
                if let Some(journal) = journal {
                    journal.set_clear_gen(*port, space.as_deref(), next);
                    // Blocker 1 (issue #224): a *port-wide* clear used to reach the engine
                    // directly (`DELETE savedRequests` -> `ClusterJournal::clear`), which is
                    // what zeroed `numberOfRequests`. Now that the same clear is a generation
                    // bump committed through Raft, nothing else zeroes it — so this node zeros
                    // its own count slot right here, on apply. A space-scoped bump must NOT do
                    // this: `clear_flow`/`retain` deliberately preserve the count for a scoped
                    // deletion, and a scoped `JournalClearGen` has to match that.
                    if space.is_none() {
                        journal.zero_count(*port);
                    }
                }
                Ok(Ok(Vec::new()))
            }
            ControlOp::ProxyRecorded {
                port,
                sig_hash,
                resp,
                stub,
            } => {
                // First-wins, idempotent — but only where "once" is the semantics. A
                // duplicate commit for the same proxyOnce signature (two owners racing a
                // membership handoff, or an op replayed past dedup's TTL) must not clobber
                // the recording replayers have already served. A `proxyAlways` merge
                // (`AfterProxyMerging`) is the opposite contract: the same signature
                // commits once per proxied request, and the merge below is the point.
                let already_recorded = proxy_recorded
                    .get((*port, sig_hash.as_str()))
                    .map_err(io)?
                    .is_some();
                let merging = stub.as_ref().is_some_and(|recorded| {
                    recorded.placement == control::RecordedStubPlacement::AfterProxyMerging
                });
                if already_recorded && !merging {
                    return Ok(Ok(Vec::new()));
                }
                let resp_json = serde_json::to_string(resp)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                // Checked for the stub-less path too, not just the config mutation below: a
                // recording racing a concurrent `DeleteImposter` must not re-insert a marker
                // after the delete's purge, or a later imposter on the same port would
                // wrongly answer `AlreadyRecorded` with the dead imposter's response.
                if configs.get(*port).map_err(io)?.is_none() {
                    return Ok(Err(format!("no imposter on port {port}")));
                }

                let Some(recorded) = stub else {
                    // Stub-less recording (no predicate generators): the row alone is the
                    // durable replay source `lookup()` answers from.
                    proxy_recorded
                        .insert((*port, sig_hash.as_str()), resp_json.as_str())
                        .map_err(io)?;
                    return Ok(Ok(Vec::new()));
                };

                let mut record: StoredImposter = {
                    match configs.get(*port).map_err(io)? {
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
                let mut stub_value = (*recorded.stub).clone();
                if stub_value.id.is_none() {
                    // Addressable identity: the engine drive replaces a merged stub by id, and
                    // a later proxyAlways merge into this stub needs the same handle. Derived
                    // from the sig-hash so every replica assigns the identical id.
                    stub_value.id = Some(format!("proxy-recorded-{sig_hash}"));
                }
                let placed = place_recorded_stub(
                    &mut config.stubs,
                    stub_value,
                    recorded.placement,
                    &recorded.proxy_to,
                );
                record.config_json = serde_json::to_string(&config)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                record.revision = index;
                let value = serde_json::to_string(&record)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                configs.insert(*port, value.as_str()).map_err(io)?;
                // Both facts land in this one apply transaction — the marker row is written
                // only alongside the stub mutation, so "recorded but stub-less" is
                // unrepresentable by construction (#226).
                proxy_recorded
                    .insert((*port, sig_hash.as_str()), resp_json.as_str())
                    .map_err(io)?;
                crate::metrics::config_applied(*port, index);
                let action = match placed {
                    PlacedRecording::Inserted { index } => EngineAction::Patch {
                        port: *port,
                        edit: control::StubEditScript(vec![control::StubEdit::Add {
                            stub: config.stubs[index].clone(),
                            index: Some(index),
                        }]),
                    },
                    PlacedRecording::MergedInto { index, id } => EngineAction::Patch {
                        port: *port,
                        edit: control::StubEditScript(vec![control::StubEdit::ReplaceById {
                            id,
                            stub: config.stubs[index].clone(),
                        }]),
                    },
                    // The merge target was a user-authored stub with no id: nothing addresses
                    // it in a patch script, so fall back to the full-sync drive — rare, and
                    // always correct.
                    PlacedRecording::MergedAnonymous => Self::sync_action(configs)?,
                };
                Ok(Ok(vec![action]))
            }
            ControlOp::ProxyRecordedClear { port } => {
                // Clearing an empty table is a no-op, not a failure — idempotent like every
                // delete here. Recorded *stubs* stay: they are imposter config, deleted
                // through the stub-edit surfaces (#226's documented split).
                proxy_recorded.retain(|(p, _), _| p != *port).map_err(io)?;
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
    async fn drive_engine(&self, actions: Vec<AttributedAction>) {
        for AttributedAction { principal, action } in actions {
            // U-10: re-open the attribution scope the admin request task could
            // not carry across the task boundary, so the listener upstream
            // invokes sees `EventContext::principal`. Wrapping each action
            // individually is deliberate — one `apply` batch can hold entries
            // from different principals, and attributing the whole batch to
            // whichever one happened to be first would be worse than the `None`
            // it replaces: wrong attribution is not a smaller error than missing
            // attribution.
            rift_cluster_base::seams::with_principal_scope(principal, async {
                self.drive_one(action).await;
            })
            .await;
        }
    }

    /// One action against the engine / route table. Runs inside the caller's
    /// principal scope; see [`AttributedAction`].
    async fn drive_one(&self, action: EngineAction) {
        match action {
            EngineAction::Sync(desired) => {
                // The whole-config level of D-5: upstream's `apply_config` (U-6)
                // diffs on stable stub keys, so a replicated write never resets
                // an untouched imposter's runtime state.
                let Some(engine) = &self.engine else { return };
                let desired_ports: std::collections::BTreeSet<u16> =
                    desired.iter().filter_map(|c| c.port).collect();
                // Before the engine call, and from the same set: the sequencer
                // must not answer for a config the engine has accepted while
                // this map still describes the previous one.
                if let Some(sequencing) = &self.sequencing {
                    sequencing.apply(&desired);
                }
                match engine.apply_config(desired).await {
                    Ok(report) => {
                        // Read before `record_report` reaps the map, because the
                        // ports this needs are exactly the ones it is about to
                        // drop (#573 review). See the clear below.
                        let previously_failed = self.previously_failed_ports();
                        self.record_report(&report, &desired_ports);
                        // The delete-path half of D-5 (#565): the ports the
                        // engine actually removed — `deleted`, never
                        // `replaced`/`stub_patched`, which are config changes
                        // that keep their runtime state — lose their flow
                        // state on this node. After the engine call, so the
                        // clear follows the removal it belongs to.
                        //
                        // Filtered against the desired set, because `deleted`
                        // answers a narrower question than "was this imposter
                        // removed" (#567 review): `replace_imposter` tears the
                        // old imposter down and then re-creates it, and when
                        // the re-create fails it reports the port as `deleted`
                        // *and* `failed` — truthfully, the engine is serving
                        // nothing there. But the config set still names that
                        // port: it is a failed **edit**, not a removal, the
                        // next successful sync re-creates it, and nothing —
                        // not even the reconcile sweep, which measures the same
                        // desired set — would ever put the flow state back. So
                        // a port the fleet still wants keeps its state, and one
                        // node's staging failure cannot make it disagree with
                        // its peers about the flows it owns.
                        //
                        // Unioned with the ports carrying a recorded apply
                        // failure, for the other half of that filter (#573
                        // review): a port whose re-create was refused has left
                        // the engine's map, so when the operator then *deletes*
                        // it, `apply_config` computes `removed_ports` from a
                        // map that no longer names it, reports nothing deleted,
                        // and the state the filter above deliberately kept
                        // would survive for the life of the process — the #565
                        // bug, reached through a failed edit. `record_report`
                        // reaps its own stale entries for exactly this reason
                        // ("a bind-failed port that is later deleted keeps its
                        // stale entry forever"); the flow state has to leave
                        // with them. The `desired_ports` filter still gates
                        // both halves, so a port that is merely failing stays
                        // untouched — only one the applied set has stopped
                        // naming is cleared.
                        self.clear_imposter_state(
                            report
                                .deleted
                                .iter()
                                .copied()
                                .chain(previously_failed)
                                .filter(|port| !desired_ports.contains(port)),
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "engine refused the applied config set");
                        self.apply_failures.lock().insert(0, e.to_string());
                    }
                }
            }
            EngineAction::RefuseSync { port, error } => {
                if self.engine.is_none() {
                    return;
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
                let Some(engine) = &self.engine else { return };
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
                let Some(engine) = &self.engine else { return };
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
                // D-69: the engine half of a space teardown's stub delete.
                StubEdit::DeleteBySpace { space } => {
                    let imposter = engine.get_imposter(port)?;
                    let doomed: Vec<usize> = imposter
                        .get_stubs()
                        .iter()
                        .enumerate()
                        .filter(|(_, stub)| stub.space.as_deref() == Some(space.as_str()))
                        .map(|(index, _)| index)
                        .collect();
                    // Descending: each delete re-indexes everything after it, so ascending order
                    // would delete the wrong stubs after the first removal.
                    //
                    // Per-index rather than `replace_stubs(kept)`: `delete_stub` resets the
                    // sequencer scope for *that stub only*, while `replace_stubs` resets the whole
                    // port's — which would discard sequencing state belonging to stubs that have
                    // nothing to do with the space being torn down.
                    for index in doomed.into_iter().rev() {
                        engine.delete_stub(port, index).await?;
                    }
                }
                StubEdit::Move { from, to } => {
                    engine.move_stub(port, *from, *to).await?;
                }
            }
        }
        Ok(())
    }

    /// Drop each port's imposter-scoped flow state on this node (#565, the D-5
    /// amendment). A no-op without a bound flow net, and per port a no-op when
    /// the shard holds nothing under it.
    ///
    /// The whole set goes down in one call, not one call per port: this runs
    /// inside the Raft apply loop, and a `DeleteAll` names every imposter the
    /// fleet has — see [`FlowNet::clear_imposter_scopes`] for what per-port
    /// cost.
    async fn clear_imposter_state(&self, ports: impl IntoIterator<Item = u16>) {
        let Some(flow_net) = &self.flow_net else {
            return;
        };
        let ports: BTreeSet<u16> = ports.into_iter().collect();
        for (port, flows) in flow_net.clear_imposter_scopes(&ports).await {
            tracing::info!(port, flows, "dropped a deleted imposter's flow state");
        }
    }

    /// The reconcile-time half of #565: drop the imposter-scoped namespaces
    /// this node's shard holds for ports `sm_configs` no longer names, both
    /// sets read *after* [`Self::reconcile_engine`]'s sync.
    ///
    /// The live path clears from the engine's `deleted` report, which needs the
    /// engine to have *had* the imposter. On a cold start it did not: the engine
    /// is process-local and empty until `reconcile_engine`, and `compose` runs
    /// that only once this node has caught up to the leader's applied index —
    /// so a delete committed while this node was down is applied against an
    /// empty engine, reports nothing deleted, and the shard reopened from disk
    /// still holds the port's state. The same holds for a snapshot installed
    /// before the first reconcile. Here the comparison that the engine could
    /// not make is made against the tables instead.
    ///
    /// **Against the tables as they stand after the sync, not the snapshot the
    /// sync was driven from (#567 review).** Those two reads are a whole
    /// `apply_config` apart — seconds on a cold start with listeners to bind —
    /// and the apply loop runs concurrently on its own handle with no barrier
    /// between them. An imposter committed inside that interval is absent from
    /// the earlier snapshot and perfectly alive, and sweeping against the
    /// snapshot drops its flow state. Re-reading closes that window: apply
    /// writes `sm_configs` before it drives the engine, so a port the fleet
    /// committed before this read is in the set.
    ///
    /// **The held set is read first and the desired set second — that order,
    /// not the other one (#573 review).** Neither read is instantaneous (one
    /// clones every key in the shard, the other walks the whole config table)
    /// and the apply loop is running between them, so one of the two is
    /// necessarily the older observation. Held-then-desired makes the older
    /// one the *accusation*: every port judged here was held at the earlier
    /// instant and is acquitted by a strictly fresher `sm_configs`. The
    /// reverse order re-opens #567 in miniature — an apply committing
    /// `PutImposter{P}` between the reads writes `sm_configs` before it drives
    /// the engine, so P is durably applied and served fleet-wide, this node is
    /// an HRW owner throughout, and a desired set read before that commit
    /// would convict a live imposter on a held set read after it.
    ///
    /// The tables, not the engine's `list_imposters`, deliberately: the two
    /// differ exactly on the ports the engine could not stage — a bind failure,
    /// a config `create_imposter_staged` refuses — and those are ports the fleet
    /// still wants. Their state is kept for the same reason the live path
    /// filters `deleted` against the desired set: a failed apply on one node is
    /// not a removal, and nothing would put the state back.
    ///
    /// Deliberately **not** run on every live sync. A live sync runs at this
    /// node's applied index, and a replica push for an imposter created at a
    /// later index — from an owner that has already applied it — can land in
    /// this shard before this node applies that entry; sweeping then would drop
    /// that copy, which nothing repairs until takeover.
    ///
    /// The residual, stated as it is, and it is exactly one thing: a flow that
    /// lands for an imposter this node has **not yet applied**, before the
    /// desired-set read below. The ring is Raft membership, so a restarted
    /// voter is an HRW owner the whole time it is catching up, and
    /// `flow_net.bind` runs in `compose` long before `spawn_reconciler`: such a
    /// flow *can* land in this shard as the authoritative copy, not merely a
    /// replica, and it is then held with no config to acquit it. Readiness does
    /// not gate that; what bounds it is that `compose` reconciles only once
    /// `last_applied` has reached the leader's applied index, so the exposure is
    /// the entries committed during one apply round-trip — not the seconds a
    /// pre-sync snapshot spanned. Nothing else remains: with the read order
    /// above, an imposter this node *has* applied by the desired-set read is in
    /// that set and is kept, whenever its flow arrived.
    #[allow(clippy::result_large_err)]
    async fn sweep_orphaned_imposter_state(
        &self,
        between_sweep_reads: impl Future<Output = ()>,
    ) -> StorageResult<()> {
        let Some(flow_net) = &self.flow_net else {
            between_sweep_reads.await;
            return Ok(());
        };
        // The accusation, read first — see the read-order paragraph above.
        let held = flow_net.imposter_ports_held();
        between_sweep_reads.await;
        if held.is_empty() {
            return Ok(());
        }
        // The acquittal, read second, and therefore never staler than the set
        // it is judging.
        let desired_ports: BTreeSet<u16> = self.configured_ports()?.into_iter().collect();
        let orphaned: Vec<u16> = held
            .into_iter()
            .filter(|port| !desired_ports.contains(port))
            .collect();
        if orphaned.is_empty() {
            return Ok(());
        }
        tracing::info!(
            ports = ?orphaned,
            "reconcile found flow state for imposters that no longer exist; dropping it"
        );
        self.clear_imposter_state(orphaned).await;
        Ok(())
    }

    /// The ports carrying an apply failure recorded *before* the report now
    /// being folded in — the second half of the live clear's port set (#573
    /// review), read while [`Self::record_report`] has not yet reaped them.
    ///
    /// Port `0` is excluded: it is the set-level slot (a whole `apply_config`
    /// the engine refused), not a port, and there is no `i0:` namespace.
    fn previously_failed_ports(&self) -> Vec<u16> {
        self.apply_failures
            .lock()
            .keys()
            .copied()
            .filter(|port| *port != 0)
            .collect()
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
    /// Builds on the blocking pool, never on a runtime worker.
    ///
    /// The body below walks every state-machine table and encodes the result — hundreds of lines
    /// with no `.await` in them. tokio cannot preempt that, so running it on a worker stops the
    /// timer wheel and the replication tasks sharing that worker for as long as it takes. On a
    /// two-core runner that is long enough for a follower's election timeout to fire and for this
    /// node to lose leadership while doing nothing but snapshotting (#444).
    #[allow(clippy::result_large_err)]
    async fn build_snapshot(&mut self) -> StorageResult<Snapshot<TypeConfig>> {
        let sm = self.clone();
        tokio::task::spawn_blocking(move || sm.build_snapshot_blocking())
            .await
            // A `JoinError` here is a panic inside the closure, or the runtime shutting down with
            // the task still queued. Both are storage faults from openraft's point of view; the
            // alternative — unwrapping the join — turns a recoverable one into a process abort.
            .map_err(|e| StorageIOError::write_snapshot(None, &std::io::Error::other(e)))?
    }
}

impl RedbStateMachine {
    /// [`RaftSnapshotBuilder::build_snapshot`]'s body, off the runtime.
    ///
    /// `&self` rather than `&mut self`: it mutates no field, which is what makes the `self.clone()`
    /// above sound — a clone shares the redb handle and the engine/journal handles, so a mutation
    /// here would be lost, and there is none to lose.
    #[allow(clippy::result_large_err)]
    fn build_snapshot_blocking(&self) -> StorageResult<Snapshot<TypeConfig>> {
        let applied = self.read_applied()?;

        let (
            configs,
            routes,
            routes_revision,
            session_key,
            fleet_name,
            journal_gens,
            proxy_recorded,
            dedup,
        ) = {
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
                configs.push((key.value(), value.value().to_owned()));
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
                routes.push((key.value().to_owned(), value.value().to_owned()));
            }
            // Travels with the routes themselves (issue #210). Omitting it
            // would not lose data, but it would silently reset the table to
            // revision 0 on the joining node — see the field's doc.
            let routes_revision = read_txn
                .open_table(SM_ROUTES_REVISION_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .get(ROUTES_REVISION_ROW)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .map(|v| v.value());
            // Travels with the snapshot like every other replicated table, with a sharp
            // failure if it did not: a follower that installs without it and then wins an
            // election either has no key to sign a login with (if none had ever been minted) or,
            // worse, mints its own on first login — silently rotating out from under every
            // session issued by every other node, with nothing reporting that a fleet-wide logout
            // just happened.
            let session_key = read_txn
                .open_table(SM_SESSION_KEY_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .get(SESSION_KEY_ROW)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .map(|v| v.value().to_owned());
            // Travels with the snapshot for the same #134/#137 reason. The failure if forgotten
            // is quieter than most of its siblings but still real: a node that joins by snapshot
            // would silently forget the fleet's name until the next rename.
            let fleet_name = read_txn
                .open_table(SM_FLEET_NAME_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .get(FLEET_NAME_ROW)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .map(|v| v.value().to_owned());
            // Travels with the snapshot for the #134/#137 reason every table above does, with the
            // #224-specific failure if it is ever forgotten here: a node that joins by snapshot
            // and reads every generation as `0` would resurrect entries its peers have already
            // agreed are cleared.
            let journal_gens_table = read_txn
                .open_table(SM_JOURNAL_GENS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut journal_gens = Vec::new();
            for item in journal_gens_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let (port, space_key) = key.value();
                journal_gens.push((port, decode_journal_gen_space_key(space_key), value.value()));
            }
            // Travels with the snapshot for the #134/#137 reason every table above does. The
            // #226-specific failure if forgotten: a snapshot-joined node answers `Claimed`
            // for signatures the fleet already recorded — a duplicate upstream call.
            let proxy_recorded_table = read_txn
                .open_table(SM_PROXY_RECORDED_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut proxy_recorded = Vec::new();
            for item in proxy_recorded_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let (port, sig_hash) = key.value();
                proxy_recorded.push((port, sig_hash.to_owned(), value.value().to_owned()));
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
                configs,
                routes,
                routes_revision,
                session_key,
                fleet_name,
                journal_gens,
                proxy_recorded,
                dedup,
            )
        };

        let payload = SnapshotPayload {
            configs,
            routes,
            routes_revision,
            session_key,
            fleet_name,
            journal_gens,
            proxy_recorded,
            dedup,
            last_applied_log: applied.last_applied_log,
            last_membership: applied.last_membership.clone(),
            logical_clock_secs: applied.logical_clock_secs,
        };
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

        // Streamed, never collected: `to_vec` here would rebuild the entire snapshot as one
        // `Vec<u8>` purely to hand it to the writer (#436 AC2).
        self.write_snapshot_file(&meta, |file| {
            // `to_writer` serialises and returns; it never flushes. Dropping the `BufWriter` here
            // would discard the flush error by design, so an ENOSPC in the last 8 KiB would look
            // like success and this node would commit a row naming a truncated snapshot — which
            // every joiner then fails to parse, for ever, with nothing on this side to say why.
            let mut writer = std::io::BufWriter::new(file);
            serde_json::to_writer(&mut writer, &payload).map_err(std::io::Error::other)?;
            std::io::Write::flush(&mut writer)
        })?;
        self.commit_snapshot_row(&meta)?;
        let file = self.open_snapshot_file(&meta.snapshot_id)?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(file),
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
            let mut routes_revision = write_txn
                .open_table(SM_ROUTES_REVISION_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut session_key = write_txn
                .open_table(SM_SESSION_KEY_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut fleet_name = write_txn
                .open_table(SM_FLEET_NAME_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut journal_gens = write_txn
                .open_table(SM_JOURNAL_GENS_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut proxy_recorded = write_txn
                .open_table(SM_PROXY_RECORDED_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut dedup = write_txn
                .open_table(SM_DEDUP_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            // Resolved once for the whole batch, not per entry: `Weak::upgrade` is cheap but
            // there is still no reason to pay it once per op when every op in this apply call
            // pushes into the very same journal (issue #224).
            let journal = self.journal.get().and_then(Weak::upgrade);
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
                                    &routes_revision,
                                    &request.op,
                                    expected,
                                )? {
                                    Err(reason) => Err(reason),
                                    Ok(()) => Self::mutate_tables(
                                        &mut configs,
                                        &mut routes,
                                        &mut routes_revision,
                                        &mut session_key,
                                        &mut fleet_name,
                                        &mut journal_gens,
                                        &mut proxy_recorded,
                                        journal.as_deref(),
                                        &request.op,
                                        log_id.index,
                                    )?,
                                },
                                None => Self::mutate_tables(
                                    &mut configs,
                                    &mut routes,
                                    &mut routes_revision,
                                    &mut session_key,
                                    &mut fleet_name,
                                    &mut journal_gens,
                                    &mut proxy_recorded,
                                    journal.as_deref(),
                                    &request.op,
                                    log_id.index,
                                )?,
                            },
                        };
                        let response = match outcome {
                            Ok(actions) => {
                                // U-10: each action remembers who caused it, so
                                // the engine drive below can re-open the
                                // attribution scope per action rather than per
                                // batch (see `AttributedAction`).
                                engine_actions.extend(actions.into_iter().map(|action| {
                                    AttributedAction {
                                        principal: request.principal.clone(),
                                        action,
                                    }
                                }));
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

    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<tokio::fs::File>> {
        // A fresh temp file per receive: openraft writes chunks into it and hands the same handle
        // back to `install_snapshot`, so it must be writable, seekable and private to this transfer.
        let path = self.snapshot_dir.join(format!(
            "receiving-{}",
            self.snapshot_idx.fetch_add(1, Ordering::Relaxed)
        ));
        let file = tokio::fs::File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| StorageIOError::write_snapshot(None, &e))?;
        Ok(Box::new(file))
    }

    #[allow(clippy::result_large_err)]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<tokio::fs::File>,
    ) -> StorageResult<()> {
        // Parsed straight off the received file (#436). Before this, install did `from_slice` on a
        // `Vec<u8>`, then `data.clone()`, then `to_vec` of the whole thing again to store it —
        // roughly 3x the snapshot in allocation for one install. The payload struct still lands in
        // memory (it has to; it is applied field by field), but the *bytes* now stream.
        //
        // `meta.snapshot_id` arrives from a peer and becomes a path component below. Nothing else
        // validates it, and the traversal that a `../` id would otherwise attempt is blocked today
        // only by the incidental `tmp-` prefix on the temp name. Make the defence deliberate.
        if meta.snapshot_id.is_empty()
            || !meta
                .snapshot_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(StorageIOError::read_snapshot(
                Some(meta.signature()),
                &std::io::Error::other(format!(
                    "peer sent an unusable snapshot id {:?}",
                    meta.snapshot_id
                )),
            )
            .into());
        }
        let received = snapshot.into_std().await;

        // Two steps: parse the payload off the runtime (redb-free, but a JSON parse of a bounded
        // buffer), then write the durable state off the runtime. The engine drive stays the
        // caller's, after the durable write.
        let received_for_parse = received;
        let (payload, received) = {
            let meta_owned = meta.clone();
            tokio::task::spawn_blocking(move || {
                Self::parse_snapshot(meta_owned, received_for_parse)
            })
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "snapshot parse task did not complete");
                StorageIOError::read_snapshot(Some(meta.signature()), &std::io::Error::other(e))
            })??
        };

        let sm = self.clone();
        let meta_owned = meta.clone();
        let actions = tokio::task::spawn_blocking(move || {
            sm.install_snapshot_blocking(meta_owned, payload, received)
        })
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "snapshot install task did not complete");
            StorageIOError::read_snapshot(Some(meta.signature()), &std::io::Error::other(e))
        })??;

        self.drive_engine(actions).await;

        Ok(())
    }

    /// Reads on the blocking pool, for [`RaftSnapshotBuilder::build_snapshot`]'s reason.
    ///
    /// Cheaper than a build since #436 made the payload a file rather than a JSON row to parse, but
    /// still a redb read with no `.await` in it, and openraft calls it on every send attempt.
    #[allow(clippy::result_large_err)]
    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<TypeConfig>>> {
        let sm = self.clone();
        tokio::task::spawn_blocking(move || sm.get_current_snapshot_blocking())
            .await
            .map_err(|e| StorageIOError::read_snapshot(None, &std::io::Error::other(e)))?
    }
}

impl RedbStateMachine {
    /// Parse a received snapshot file into its payload, off the runtime, returning the (rewound)
    /// file so the blocking install below can still copy it verbatim.
    ///
    /// Split out of [`Self::install_snapshot_blocking`] so the JSON parse and the redb write are
    /// separate `spawn_blocking` steps.
    #[allow(clippy::result_large_err)]
    fn parse_snapshot(
        meta: SnapshotMeta<u64, BasicNode>,
        mut received: std::fs::File,
    ) -> StorageResult<(SnapshotPayload, std::fs::File)> {
        use std::io::Seek as _;
        // Rewind before reading. openraft streams the transfer in by writing chunk after chunk into
        // this handle, so it arrives positioned at EOF — parsing from where it sits reads nothing
        // and the install fails with an empty-input error, which looks exactly like a peer that
        // sent a corrupt snapshot. The in-process tests never saw it: their handles come straight
        // from `build_snapshot` or a freshly opened file, both already at 0.
        received
            .seek(std::io::SeekFrom::Start(0))
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        // `try_clone` shares the file *offset* on Unix, so parsing through the clone advances this
        // handle too — which is why the copy in the blocking install must seek back to 0 rather
        // than assuming it is still where the rewind above left it.
        let received_for_parse = received
            .try_clone()
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let payload: SnapshotPayload =
            serde_json::from_reader(std::io::BufReader::new(received_for_parse)).map_err(|e| {
                StorageError::from(StorageIOError::read_snapshot(Some(meta.signature()), &e))
            })?;
        Ok((payload, received))
    }

    /// [`RaftStateMachine::install_snapshot`]'s synchronous middle, off the runtime.
    ///
    /// Takes the payload already parsed by [`Self::parse_snapshot`], so its own body is pure
    /// redb: no parse, no `.await`. Returns the engine actions rather than driving them — driving is the one
    /// `.await` in the original body, and returning it is what lets the whole durable write run on
    /// the blocking pool without changing the order those actions are applied in.
    #[allow(clippy::result_large_err)]
    fn install_snapshot_blocking(
        &self,
        meta: SnapshotMeta<u64, BasicNode>,
        payload: SnapshotPayload,
        received: std::fs::File,
    ) -> StorageResult<Vec<AttributedAction>> {
        use std::io::Seek as _;
        let meta = &meta;
        // The received bytes are copied verbatim rather than re-encoded from `payload`: what the
        // peer sent is what this node should serve on, and a re-encode would be a second full pass.
        let mut source = received;
        source
            .seek(std::io::SeekFrom::Start(0))
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        self.write_snapshot_file(meta, |file| std::io::copy(&mut source, file).map(|_| ()))?;
        let row = serde_json::to_vec(&StoredSnapshot {
            meta: meta.clone(),
            file: meta.snapshot_id.clone(),
        })
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
                .insert((), row.as_slice())
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

            let mut configs_table = write_txn
                .open_table(SM_CONFIGS_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            configs_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (port, value) in &payload.configs {
                configs_table
                    .insert(*port, value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }
            let mut routes_table = write_txn
                .open_table(SM_ROUTES_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            routes_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (id, value) in &payload.routes {
                routes_table
                    .insert(id.as_str(), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }
            let routes_action = match Self::desired_routes(&routes_table)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?
            {
                Ok(table) => EngineAction::SyncRoutes(table),
                Err((id, error)) => EngineAction::RefuseRoutesSync { id, error },
            };

            // Cleared before repopulating, like every table above: a payload
            // carrying no revision means the fleet holds none, and leaving this
            // node's stale row would let a token this node minted before the
            // install keep passing against a table it no longer has.
            let mut routes_revision_table = write_txn
                .open_table(SM_ROUTES_REVISION_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            routes_revision_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            if let Some(revision) = payload.routes_revision {
                routes_revision_table
                    .insert(ROUTES_REVISION_ROW, revision)
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            let config_action = match Self::desired_configs(&configs_table)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?
            {
                Ok(desired) => EngineAction::Sync(desired),
                Err((port, error)) => EngineAction::RefuseSync { port, error },
            };

            // Cleared before it is repopulated, exactly like the tables above: a payload carrying
            // no key means no console login has ever minted one on the leader, and leaving a stale local
            // key in place would let this node keep verifying cookies against a revision the
            // fleet no longer agrees is current.
            let mut session_key_table = write_txn
                .open_table(SM_SESSION_KEY_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            session_key_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            if let Some(value) = &payload.session_key {
                session_key_table
                    .insert(SESSION_KEY_ROW, value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            // Cleared before it is repopulated, like the sink and the session key above: a
            // payload carrying no name means the leader's fleet is unnamed, and leaving a stale
            // local name in place would make this node keep reporting a name the fleet has since
            // cleared or renamed away from.
            let mut fleet_name_table = write_txn
                .open_table(SM_FLEET_NAME_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            fleet_name_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            if let Some(value) = &payload.fleet_name {
                fleet_name_table
                    .insert(FLEET_NAME_ROW, value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            // Cleared before it is repopulated, like every table above — and the #224 reason to
            // do it this way rather than leave stale rows behind is the sharpest of the lot: a
            // generation this node still held from before the install could be *higher* than
            // what the payload carries (a stale leader that clears, is partitioned, and rejoins
            // by snapshot from a peer that never saw it), and leaving it in place would make a
            // clear this fleet has since forgotten win over the one it actually agrees on.
            let mut journal_gens_table = write_txn
                .open_table(SM_JOURNAL_GENS_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            journal_gens_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (port, space, generation) in &payload.journal_gens {
                let space_key = journal_gen_space_key(space.as_deref());
                journal_gens_table
                    .insert((*port, space_key.as_str()), *generation)
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            // Cleared before repopulating for the same stale-row reason as `journal_gens`
            // above: a marker this node held from before the install may name a signature
            // the fleet has since cleared, and leaving it would resurrect `AlreadyRecorded`
            // for it.
            let mut proxy_recorded_table = write_txn
                .open_table(SM_PROXY_RECORDED_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            proxy_recorded_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (port, sig_hash, resp) in &payload.proxy_recorded {
                proxy_recorded_table
                    .insert((*port, sig_hash.as_str()), resp.as_str())
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

        // Pushed into the local journal after the durable write, like the engine/routes
        // convergence just below — and for the identical #134/#137 reason `journal_gens_table`
        // above is cleared-then-repopulated rather than merged: a node joining (or rejoining
        // after a partition) by snapshot must come back agreeing with the fleet on every
        // generation, not just the ones its own log had already applied. A missing/dropped
        // handle is the same benign no-op it is in `mutate_tables`'s `JournalClearGen` arm — the
        // generations are durable in `sm_journal_gens` regardless of whether anything is
        // listening on the other end right now.
        if let Some(journal) = self.journal.get().and_then(Weak::upgrade) {
            // `ClusterJournal` keys entries on `(node_id, seq, clear_gen)` — see its own module
            // doc — so
            // `reset_clear_gen`, not `set_clear_gen` (Non-blocker 1): the durable table was just
            // cleared and reinserted from this exact payload, unconditionally, because a
            // generation this node still held could be higher than what the fleet now agrees on
            // — `set_clear_gen`'s `fetch_max` cannot lower to match, and leaving it high would
            // let one node's forgotten-but-not-really clear silently win the fleet-wide max a
            // merge computes, dropping every other node's entries.
            for (port, space, generation) in &payload.journal_gens {
                journal.reset_clear_gen(*port, space.as_deref(), *generation);
            }
        }

        // A snapshot replaces the whole applied state, so the engine and the
        // front door's compiled table both converge on it the same way apply
        // does — after the durable write, best-effort. Unattributed: a snapshot
        // is the sum of many principals' writes, so naming any one of them
        // would be a lie.
        // Same ordering rule as `commit_snapshot_row`: sweep only once the row naming the new
        // payload is durable. A follower that only ever installs would otherwise never sweep at all.
        self.gc_snapshot_files(&meta.snapshot_id);

        // Counted here — after the durable write — so the metric means "a peer's snapshot really
        // installed", the wire-path outcome #183 distinguishes from catch-up-by-replication. A
        // metric bumped at parse time would count installs that never landed.
        crate::metrics::snapshot_installed();

        Ok(vec![
            AttributedAction::unattributed(config_action),
            AttributedAction::unattributed(routes_action),
        ])
    }

    /// [`RaftStateMachine::get_current_snapshot`]'s body, off the runtime.
    #[allow(clippy::result_large_err)]
    fn get_current_snapshot_blocking(&self) -> StorageResult<Option<Snapshot<TypeConfig>>> {
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
        let path = self.snapshot_dir.join(&stored.file);
        let file = match std::fs::File::open(&path) {
            Ok(file) => file,
            // The row survived but its payload did not — a manual deletion, a half-restored backup,
            // a filesystem that lost it. openraft reads `None` as "this node has no snapshot" and
            // builds a fresh one, which is the correct outcome; erroring here would instead take a
            // healthy node out of service over derived state it can regenerate. Logged rather than
            // silent, because it is still a fact about the disk that an operator should see.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!(
                    path = %path.display(),
                    snapshot_id = %stored.meta.snapshot_id,
                    "snapshot row names a file that is gone; reporting no snapshot so one is rebuilt"
                );
                return Ok(None);
            }
            Err(e) => return Err(StorageIOError::read_snapshot(None, &e).into()),
        };
        Ok(Some(Snapshot {
            meta: stored.meta,
            snapshot: Box::new(tokio::fs::File::from_std(file)),
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
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};

    use super::{SESSION_KEY_ROW, SM_SESSION_KEY_TABLE};
    use rift_cluster_base::seams::{
        CompiledRoutes, ImposterConfig, ImposterManager, RecordedRequest, RequestJournal,
        ResponseMode, Route, RouteMatch, RouteTable, RouteTarget,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::{DEDUP_TTL_SECS, DedupEntry, RedbLogStore, RedbStateMachine, SM_DEDUP_TABLE, new};
    use crate::control::{
        ControlOp, ControlOutcome, ControlRequest, ControlResponse, StubEdit, StubEditScript,
    };
    use crate::raft::TypeConfig;
    use crate::stores::flow::FlowNet;
    use crate::stores::journal::ClusterJournal;
    use crate::stores::shard::{Durability, FlowShard, ShardConfig, Versioned};

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

    /// Drain a snapshot handle to bytes. Since #436 the handle is a `tokio::fs::File` whose cursor
    /// may sit at the end of a just-written payload, so it is rewound first — `Cursor::into_inner`,
    /// which these tests used before, had no such state to undo.
    async fn read_snapshot_bytes(mut snapshot: Box<tokio::fs::File>) -> Vec<u8> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        snapshot
            .seek(std::io::SeekFrom::Start(0))
            .await
            .expect("rewind snapshot");
        let mut bytes = Vec::new();
        snapshot
            .read_to_end(&mut bytes)
            .await
            .expect("read snapshot");
        bytes
    }

    /// Wrap `bytes` in a snapshot handle, for the tests that install a deliberately older-shaped
    /// payload. Replaces the `Box::new(Cursor::new(bytes))` those tests used before #436 made the
    /// snapshot a file; their assertions are unchanged.
    async fn snapshot_handle_from(dir: &std::path::Path, bytes: &[u8]) -> Box<tokio::fs::File> {
        use tokio::io::AsyncSeekExt;
        let path = dir.join(format!("older-{}", bytes.len()));
        tokio::fs::write(&path, bytes)
            .await
            .expect("write older payload");
        let mut file = tokio::fs::File::open(&path)
            .await
            .expect("open older payload");
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .expect("rewind older payload");
        Box::new(file)
    }

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
                config: Box::new(config(port, stubs)),
            },
        )
    }

    /// A `PutImposter` whose **imposter-level** config differs: `recordRequests`
    /// is not a stub, so upstream's diff
    /// (`imposter_level_differs_ignoring_enabled`) takes the wholesale-replace
    /// branch and reports the port under `ApplyReport::replaced`. D-5 names that
    /// branch specifically; a stubs-only edit reaches `stub_patched` instead and
    /// leaves it unexercised.
    fn put_recording(op_id: u128, port: u16, stubs: serde_json::Value) -> ControlRequest {
        let mut config = config(port, stubs);
        config.record_requests = true;
        request(
            op_id,
            ControlOp::PutImposter {
                config: Box::new(config),
            },
        )
    }

    /// A `PutImposter` upstream accepts into the desired set and then refuses at
    /// staging: `mutualAuth` is an imposter-level field (so the diff is a
    /// wholesale replace) that `validate_config_set` does not look at, and
    /// `create_imposter_staged`'s `client_auth_for` rejects on a cleartext
    /// listener. The teardown half of the replace therefore succeeds and the
    /// re-create fails — the shape that lands a still-desired port in
    /// `ApplyReport::deleted`.
    fn put_unstageable(op_id: u128, port: u16) -> ControlRequest {
        let mut config = config(port, json!([]));
        config.mutual_auth = true;
        request(
            op_id,
            ControlOp::PutImposter {
                config: Box::new(config),
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

    /// An imposter whose one stub carries roughly `target_bytes` of literal response body — the
    /// cheapest way to put a known quantity of bytes into `sm_configs`, which is what the two
    /// snapshot-size gates below need. `tag` keeps two ports' bytes distinct, so a test that
    /// wants N ports' worth of state actually gets it.
    fn bulky_config(port: u16, tag: &str, target_bytes: usize) -> ImposterConfig {
        let mut body = String::with_capacity(target_bytes + 32);
        let mut row = 0u64;
        while body.len() < target_bytes {
            body.push_str(&format!("{row},{tag}-{}\n", "x".repeat(1_000)));
            row += 1;
        }
        config(
            port,
            json!([{ "id": format!("{tag}-bulk"), "responses": [{ "is": { "body": body } }] }]),
        )
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

    /// Pins D-5: a committed `Move` reorders both the stored config and the
    /// live engine's stub list in place — the reconcile is order-aware, so a
    /// reorder replicates as a move rather than as a delete+add that would
    /// reset the slot.
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
            request(3, ControlOp::DeleteImposter { port: 18084 }),
        )])
        .await
        .expect("apply delete");
        assert_eq!(engine.count(), 1);
        assert_eq!(sm.configured_ports().expect("ports"), vec![18085]);

        sm.apply(vec![entry(4, request(4, ControlOp::DeleteAll))])
            .await
            .expect("apply delete-all");
        assert_eq!(engine.count(), 0);
        assert!(sm.configured_ports().expect("ports").is_empty());

        engine.shutdown().await;
    }

    /// A state machine with an engine AND a flow shard attached, plus a clone
    /// of the shard to seed and inspect through (the net owns the other).
    async fn fresh_sm_with_flow_net(
        engine: Arc<ImposterManager>,
    ) -> (TempDir, RedbStateMachine, FlowShard) {
        let td = TempDir::new().expect("tempdir");
        let (_, sm) = new(td.path().join("raft.redb")).await.expect("open store");
        let shard = FlowShard::in_memory(ShardConfig::default());
        let net = FlowNet::new(shard.clone());
        let sm = sm.with_engine(engine).with_flow_net(net);
        (td, sm, shard)
    }

    /// One live entry under `flow_id`, as an owner would have written it.
    async fn seed_flow(shard: &FlowShard, flow_id: &str) {
        shard
            .set(
                flow_id,
                "checkout",
                Versioned {
                    m_idx: 1,
                    v: 1,
                    origin: 1,
                    expires_at: 0,
                    value: json!("paid"),
                    deleted: false,
                },
                Durability::None,
            )
            .await
            .expect("seed");
    }

    /// #565 / the D-5 amendment: a committed `DeleteImposter` drops the deleted
    /// port's imposter-scoped flow state on this node — and nothing else. A
    /// sibling port's `i<port>:` state, a fleet-scoped `f:` flow and a
    /// flow under any other prefix are not the deleted imposter's to lose.
    /// A `PutImposter` over the same port is a config change, not a delete,
    /// and keeps the state (D-5) — pinned on the **wholesale-replace** branch,
    /// the one D-5 names, which only an imposter-level change reaches (a
    /// stubs-only edit is patched in place and never tears the imposter down).
    /// `DeleteAll` clears every deleted port the same way.
    #[tokio::test]
    async fn a_committed_delete_drops_only_that_ports_imposter_scoped_flow_state() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm, shard) = fresh_sm_with_flow_net(engine.clone()).await;
        sm.apply(vec![
            entry(1, put(1, 18094, json!([{ "id": "a" }]))),
            entry(2, put(2, 18095, json!([]))),
        ])
        .await
        .expect("apply puts");
        assert_eq!(engine.count(), 2);

        // What a scenario on each imposter, a fleet-scoped context and a
        // flow under some other prefix leave in this node's shard.
        for flow in [
            "i18094:checkout",
            "i18095:checkout",
            "f:checkout",
            "tacme:checkout",
        ] {
            seed_flow(&shard, flow).await;
        }
        assert_eq!(shard.flow_count(), 4);

        // A config change on the port is not a delete: its state stays (D-5).
        // `recordRequests` flips, so upstream replaces the imposter wholesale
        // (`ApplyReport::replaced`) rather than patching stubs in place.
        sm.apply(vec![entry(
            3,
            put_recording(3, 18094, json!([{ "id": "a" }])),
        )])
        .await
        .expect("apply replace");
        assert!(
            engine
                .get_imposter(18094)
                .expect("replaced imposter is served")
                .config
                .record_requests,
            "the replace reached the engine: this is the wholesale-replace branch"
        );
        assert!(
            shard.get("i18094:checkout", "checkout").is_some(),
            "a replaced imposter keeps its scenario state"
        );

        sm.apply(vec![entry(
            4,
            request(4, ControlOp::DeleteImposter { port: 18094 }),
        )])
        .await
        .expect("apply delete");
        assert_eq!(engine.count(), 1);
        assert!(
            shard.get("i18094:checkout", "checkout").is_none(),
            "the deleted imposter's namespace is dropped on this node"
        );
        for kept in ["i18095:checkout", "f:checkout", "tacme:checkout"] {
            assert!(
                shard.get(kept, "checkout").is_some(),
                "{kept} is not the deleted imposter's state and must survive"
            );
        }

        // Re-creating the port starts from nothing, and deleting it again with
        // nothing held is a no-op, not a failure.
        sm.apply(vec![entry(5, put(5, 18094, json!([{ "id": "a" }])))])
            .await
            .expect("apply re-create");
        assert!(shard.get("i18094:checkout", "checkout").is_none());
        sm.apply(vec![entry(6, request(6, ControlOp::DeleteAll))])
            .await
            .expect("apply delete-all");
        assert_eq!(engine.count(), 0);
        assert!(
            shard.get("i18095:checkout", "checkout").is_none(),
            "delete-all drops every deleted port's namespace"
        );
        assert!(shard.get("f:checkout", "checkout").is_some());
        assert!(shard.get("tacme:checkout", "checkout").is_some());
        assert!(
            sm.apply_failures().is_empty(),
            "the clears are not engine failures: {:?}",
            sm.apply_failures()
        );

        engine.shutdown().await;
    }

    /// #565, the cold-start half: a delete this node never applied against a
    /// live engine — it was down, or the entry arrived before the engine was
    /// rebuilt — leaves the port's flow state on disk with no `deleted` report
    /// to clear it. `reconcile_engine` compares the shard against the tables
    /// and drops what no committed imposter names; what the tables do name is
    /// kept, and so are the shared namespaces.
    #[tokio::test]
    async fn reconcile_drops_flow_state_of_imposters_the_tables_no_longer_name() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm, shard) = fresh_sm_with_flow_net(engine.clone()).await;
        // Applied against an engine that is about to be "restarted": the
        // tables keep 18096 and lose 18097 without the engine ever having
        // bound 18097 at the time of its delete.
        sm.apply(vec![entry(1, put(1, 18096, json!([])))])
            .await
            .expect("apply put");
        for flow in ["i18096:checkout", "i18097:checkout", "f:checkout"] {
            seed_flow(&shard, flow).await;
        }

        sm.reconcile_engine().await.expect("reconcile");
        assert_eq!(engine.count(), 1, "the engine is rebuilt from the tables");
        assert!(
            shard.get("i18097:checkout", "checkout").is_none(),
            "a namespace no committed imposter names is dropped at reconcile"
        );
        assert!(
            shard.get("i18096:checkout", "checkout").is_some(),
            "a live imposter's state is untouched by the sweep"
        );
        assert!(
            shard.get("f:checkout", "checkout").is_some(),
            "the fleet namespace names no port and is never swept"
        );

        // Idempotent: a second reconcile finds nothing orphaned.
        sm.reconcile_engine().await.expect("reconcile again");
        assert!(shard.get("i18096:checkout", "checkout").is_some());

        engine.shutdown().await;
    }

    /// #567 / #573 review — the sweep's bound, pinned at its worst case. The
    /// reconcile reads the config set, drives the engine to it (seconds, on a
    /// cold start), then sweeps; the apply loop keeps running on its own handle
    /// the whole time, and this node is a ring member throughout. An imposter
    /// committed while that is going on is alive on every node, and a flow
    /// written for it can already be in this shard. The sweep must not take
    /// that flow.
    ///
    /// The interleaving point is the one that decides it: **between the sweep's
    /// two reads**, which is where a concurrent apply can be seen by one read
    /// and not the other. It kills both mutations of this code — reverting the
    /// desired set to the pre-sync snapshot (#567), and reading the desired set
    /// before the held set instead of after (#573). Either one drops
    /// `i18099:checkout`, whose imposter is committed here and served fleet-wide.
    #[tokio::test]
    async fn reconcile_keeps_flow_state_of_an_imposter_committed_inside_its_sweep() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm, shard) = fresh_sm_with_flow_net(engine.clone()).await;
        sm.apply(vec![entry(1, put(1, 18098, json!([])))])
            .await
            .expect("apply put");
        // 18099 is not committed yet; 18100 was deleted while this node was
        // down and is the one namespace the sweep is for.
        for flow in ["i18098:checkout", "i18099:checkout", "i18100:checkout"] {
            seed_flow(&shard, flow).await;
        }

        // The apply loop's handle: `compose` gives openraft its own clone.
        let mut applier = sm.clone();
        sm.reconcile_engine_interleaved(async move {
            applier
                .apply(vec![entry(2, put(2, 18099, json!([])))])
                .await
                .expect("apply put during reconcile");
        })
        .await
        .expect("reconcile");

        assert_eq!(engine.count(), 2, "both committed imposters are served");
        assert!(
            shard.get("i18099:checkout", "checkout").is_some(),
            "an imposter committed between the sweep's two reads is alive; its state stays"
        );
        assert!(
            shard.get("i18098:checkout", "checkout").is_some(),
            "a live imposter's state is untouched by the sweep"
        );
        assert!(
            shard.get("i18100:checkout", "checkout").is_none(),
            "the namespace of a delete this node missed is still swept"
        );

        engine.shutdown().await;
    }

    /// #567 review — `deleted` is filtered against the desired set. Upstream's
    /// `replace_imposter` tears the old imposter down and re-creates it; when
    /// the re-create is refused at staging the port is reported `deleted`
    /// *and* `failed`. The fleet still wants that port — it is a failed edit,
    /// not a removal, and the next successful sync re-creates it — so its flow
    /// state must survive on this node exactly as it does on every node whose
    /// engine did not fail. Without the filter, the state was gone for good:
    /// the re-create that follows starts the imposter from nothing.
    #[tokio::test]
    async fn a_failed_re_create_of_a_still_desired_port_keeps_its_flow_state() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm, shard) = fresh_sm_with_flow_net(engine.clone()).await;
        sm.apply(vec![entry(1, put(1, 18101, json!([])))])
            .await
            .expect("apply put");
        seed_flow(&shard, "i18101:checkout").await;

        // `mutualAuth` on a cleartext listener: the desired set validates, the
        // diff is imposter-level (a replace), the teardown succeeds and the
        // staged re-create is refused.
        sm.apply(vec![entry(2, put_unstageable(2, 18101))])
            .await
            .expect("apply the unstageable edit");
        assert_eq!(engine.count(), 0, "the engine serves nothing on the port");
        assert!(
            sm.apply_failures().contains_key(&18101),
            "the failed edit is an apply failure: {:?}",
            sm.apply_failures()
        );
        assert!(
            shard.get("i18101:checkout", "checkout").is_some(),
            "a port the config set still names keeps its state through a failed re-create"
        );

        // The next good edit re-creates the imposter with its state intact.
        sm.apply(vec![entry(3, put(3, 18101, json!([])))])
            .await
            .expect("apply the repair");
        assert_eq!(engine.count(), 1);
        assert!(sm.apply_failures().is_empty(), "{:?}", sm.apply_failures());
        assert!(shard.get("i18101:checkout", "checkout").is_some());

        // A real delete still clears it.
        sm.apply(vec![entry(
            4,
            request(4, ControlOp::DeleteImposter { port: 18101 }),
        )])
        .await
        .expect("apply delete");
        assert!(shard.get("i18101:checkout", "checkout").is_none());

        engine.shutdown().await;
    }

    /// #573 review — the same failed re-create, with **no repair** before the
    /// delete. `replace_imposter`'s teardown has already dropped the port from
    /// the engine's map, so when the operator gives up and deletes it,
    /// `apply_config` computes its removal set (`map ∖ desired`) from a map
    /// that no longer names the port: `report.deleted` is empty, and a clear
    /// filtered on `deleted` alone finds nothing to do. The state the previous
    /// test deliberately keeps would then outlive the imposter for the life of
    /// the process, and an identically re-created imposter would meet
    /// yesterday's scenario — the #565 bug, reached through a failed edit. So
    /// the clear also names the ports carrying a recorded apply failure. The
    /// still-desired failing port here is the other half of the claim: the
    /// union is gated by the same desired-set filter, so a port that is merely
    /// failing keeps everything.
    #[tokio::test]
    async fn a_delete_after_a_failed_re_create_still_drops_the_flow_state() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm, shard) = fresh_sm_with_flow_net(engine.clone()).await;
        sm.apply(vec![
            entry(1, put(1, 18104, json!([]))),
            entry(2, put(2, 18105, json!([]))),
        ])
        .await
        .expect("apply puts");
        for flow in ["i18104:checkout", "i18105:checkout", "f:checkout"] {
            seed_flow(&shard, flow).await;
        }

        // Both ports take an edit the engine accepts into the desired set and
        // then refuses at staging: each leaves the engine's map and each lands
        // in `apply_failures`, with its flow state kept (the filter above).
        sm.apply(vec![
            entry(3, put_unstageable(3, 18104)),
            entry(4, put_unstageable(4, 18105)),
        ])
        .await
        .expect("apply the unstageable edits");
        assert_eq!(engine.count(), 0, "the engine serves neither port");
        assert_eq!(
            sm.apply_failures().keys().copied().collect::<Vec<_>>(),
            vec![18104, 18105],
            "both edits are recorded apply failures"
        );
        assert!(shard.get("i18104:checkout", "checkout").is_some());
        assert!(shard.get("i18105:checkout", "checkout").is_some());

        // The operator gives up on 18104 and deletes it. The engine has nothing
        // to remove, so this reports no deletion at all.
        sm.apply(vec![entry(
            5,
            request(5, ControlOp::DeleteImposter { port: 18104 }),
        )])
        .await
        .expect("apply delete");

        assert!(
            shard.get("i18104:checkout", "checkout").is_none(),
            "a deleted port loses its flow state even though the failed edit had \
             already taken it out of the engine's map"
        );
        assert!(
            shard.get("i18105:checkout", "checkout").is_some(),
            "a port the config set still names keeps its state, failing or not"
        );
        assert!(
            shard.get("f:checkout", "checkout").is_some(),
            "the fleet namespace names no port and is never cleared"
        );

        engine.shutdown().await;
    }

    /// #565 by way of a snapshot: an installed snapshot that omits an imposter
    /// this node's engine is serving syncs the engine to the snapshot, which
    /// reports the port `deleted` — and the clear follows, exactly as it does
    /// for an applied `DeleteImposter`. What the snapshot does carry keeps its
    /// state, and so does the fleet namespace.
    #[tokio::test]
    async fn an_installed_snapshot_clears_flow_state_of_imposters_it_omits() {
        let (_td, mut leader_sm) = fresh_sm(None).await;
        leader_sm
            .apply(vec![entry(1, put(1, 18102, json!([])))])
            .await
            .expect("apply on the leader");
        let mut builder = leader_sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let engine = Arc::new(ImposterManager::new());
        let (_td2, mut follower, shard) = fresh_sm_with_flow_net(engine.clone()).await;
        follower
            .apply(vec![
                entry(1, put(1, 18102, json!([]))),
                entry(2, put(2, 18103, json!([]))),
            ])
            .await
            .expect("apply on the follower");
        for flow in ["i18102:checkout", "i18103:checkout", "f:checkout"] {
            seed_flow(&shard, flow).await;
        }

        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");
        assert_eq!(engine.count(), 1, "the engine is synced to the snapshot");
        assert!(
            shard.get("i18103:checkout", "checkout").is_none(),
            "an imposter the installed snapshot omits loses its flow state"
        );
        assert!(
            shard.get("i18102:checkout", "checkout").is_some(),
            "an imposter the snapshot carries keeps its flow state"
        );
        assert!(shard.get("f:checkout", "checkout").is_some());

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
                request(1, ControlOp::DeleteImposter { port: 5555 }),
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
                ControlOp::DeleteImposter { port: 9999 },
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

        sm.inject_raw_config(18088, "not json");

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
            request(2, ControlOp::DeleteImposter { port }),
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
                conditioned(4, 5, ControlOp::DeleteImposter { port: 9999 }),
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
            .apply(vec![entry(5, conditioned(5, 1, ControlOp::DeleteAll))])
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

    // -- issue #224: journal clear generations ---------------------------------

    fn journal_clear(op_id: u128, port: u16, space: Option<&str>) -> ControlRequest {
        request(
            op_id,
            ControlOp::JournalClearGen {
                port,
                space: space.map(str::to_owned),
            },
        )
    }

    #[tokio::test]
    async fn applying_a_journal_clear_increments_the_generation() {
        let (_td, mut sm) = fresh_sm(None).await;
        let response = apply_one(&mut sm, 1, journal_clear(1, 8080, None)).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert_eq!(sm.journal_gen(8080, None).expect("read gen"), 1);

        apply_one(&mut sm, 2, journal_clear(2, 8080, None)).await;
        assert_eq!(
            sm.journal_gen(8080, None).expect("read gen"),
            2,
            "a second clear on the same port bumps again"
        );
    }

    /// Two clears for the same port, applied in log order (as every replica
    /// applies them), both succeed and compose to +2 — never one silently overwriting the
    /// other with the identical value. This is the entire reason
    /// `ControlOp::JournalClearGen` carries no number of its own: a submitted value would let
    /// the second of two racing clears collapse onto the first instead of composing with it.
    #[tokio::test]
    async fn racing_journal_clears_compose_rather_than_overwrite() {
        let (_td, mut sm) = fresh_sm(None).await;
        let first = apply_one(&mut sm, 1, journal_clear(1, 8080, None)).await;
        let second = apply_one(&mut sm, 2, journal_clear(2, 8080, None)).await;
        assert_eq!(first.outcome, ControlOutcome::Applied);
        assert_eq!(second.outcome, ControlOutcome::Applied);
        assert_eq!(
            sm.journal_gen(8080, None).expect("read gen"),
            2,
            "two racing clears must compose to +2, not collapse to the same value twice"
        );
    }

    #[tokio::test]
    async fn a_space_clear_leaves_the_port_generation_untouched() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, journal_clear(1, 8080, Some("f"))).await;
        assert_eq!(sm.journal_gen(8080, Some("f")).expect("read gen"), 1);
        assert_eq!(
            sm.journal_gen(8080, None).expect("read gen"),
            0,
            "a space-scoped clear must not bump the port-wide generation"
        );
        assert_eq!(
            sm.journal_gen(8080, Some("g")).expect("read gen"),
            0,
            "a space-scoped clear must not bump a sibling space's generation"
        );
    }

    #[tokio::test]
    async fn applying_a_journal_clear_pushes_the_generation_into_the_bound_local_journal() {
        let journal = ClusterJournal::new(1);
        let (_td, sm) = fresh_sm(None).await;
        let mut sm = sm.with_journal(&journal);
        apply_one(&mut sm, 1, journal_clear(1, 8080, None)).await;
        assert_eq!(
            journal.read_shard_since(8080, 0).clear_gen,
            1,
            "apply must push the bumped generation into this node's own local journal, not \
             just the durable table"
        );
    }

    /// A node joining by snapshot must come back holding the same generations its peers do —
    /// the #134/#137 lesson (a node reading a cleared entry back as if it never cleared)
    /// applied a third time to a third table.
    #[tokio::test]
    async fn journal_generations_survive_a_snapshot_install() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, journal_clear(1, 8080, None)).await;
        apply_one(&mut sm, 2, journal_clear(2, 8080, Some("f"))).await;

        let snapshot: Snapshot<TypeConfig> = sm.build_snapshot().await.expect("build snapshot");
        let (_td2, mut restored) = fresh_sm(None).await;
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        assert_eq!(
            restored.journal_gen(8080, None).expect("read gen"),
            1,
            "a node joining by snapshot must not read a cleared port back as 0 — that would \
             resurrect entries its peers already agree are cleared"
        );
        assert_eq!(restored.journal_gen(8080, Some("f")).expect("read gen"), 1);
    }

    /// A snapshot payload serialized before issue #224 still installs — `journal_gens` defaults
    /// to empty, which is what "this fleet has never committed a clear" is. Mirrors
    /// `a_pre_sources_snapshot_still_installs`/`a_pre_tenancy_snapshot_still_installs`: the same
    /// #134/#137 lesson, paid down with the same `#[serde(default)]` discipline a third time.
    #[tokio::test]
    async fn a_pre_journal_gens_snapshot_still_installs() {
        let (_td, sm) = fresh_sm(None).await;
        let legacy = json!({
            "configs": [],
            "dedup": [],
            "last_applied_log": null,
            "last_membership": { "log_id": null, "membership": { "configs": [], "nodes": {} } },
        });
        let payload: super::SnapshotPayload =
            serde_json::from_value(legacy).expect("a pre-#224 snapshot payload still decodes");
        assert!(payload.journal_gens.is_empty());
        assert_eq!(sm.journal_gen(8080, None).expect("read gen"), 0);
    }

    // -- ProxyRecorded / ProxyRecordedClear (#226) ---------------------------------

    fn proxy_imposter_config(port: u16) -> ImposterConfig {
        config(
            port,
            json!([{
                "responses": [{
                    "proxy": { "to": "http://u.example", "mode": "proxyOnce" }
                }]
            }]),
        )
    }

    fn recorded_stub(
        body: &str,
        placement: crate::control::RecordedStubPlacement,
    ) -> crate::control::RecordedStub {
        crate::control::RecordedStub {
            stub: Box::new(
                serde_json::from_value(json!({
                    "predicates": [{ "equals": { "path": "/r" } }],
                    "responses": [{ "is": { "statusCode": 200, "body": body } }],
                }))
                .expect("stub parses"),
            ),
            placement,
            proxy_to: "http://u.example".to_owned(),
        }
    }

    fn proxy_recorded(
        op_id: u128,
        port: u16,
        sig_hash: &str,
        body: &str,
        stub: Option<crate::control::RecordedStub>,
    ) -> ControlRequest {
        request(
            op_id,
            ControlOp::ProxyRecorded {
                port,
                sig_hash: sig_hash.to_owned(),
                resp: rift_cluster_base::seams::RecordedResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: body.as_bytes().to_vec(),
                    latency_ms: None,
                    timestamp_secs: 0,
                },
                stub,
            },
        )
    }

    fn marker(sm: &RedbStateMachine, port: u16, sig_hash: &str) -> Option<String> {
        sm.proxy_recorded_resp(port, sig_hash).expect("read marker")
    }

    /// The failure the snapshot field's own doc names: a snapshot built before #226 must
    /// still install, and the empty table it decodes to must answer "never recorded" —
    /// not fail — so a rolling upgrade cannot turn joins into duplicate upstream calls.
    #[tokio::test]
    async fn a_pre_proxy_recorded_snapshot_still_installs() {
        let (_td, sm) = fresh_sm(None).await;
        let legacy = json!({
            "configs": [],
            "dedup": [],
            "last_applied_log": null,
            "last_membership": { "log_id": null, "membership": { "configs": [], "nodes": {} } },
        });
        let payload: super::SnapshotPayload =
            serde_json::from_value(legacy).expect("a pre-#226 snapshot payload still decodes");
        assert!(payload.proxy_recorded.is_empty());
        assert!(marker(&sm, 8080, "aa11").is_none());
    }

    /// Recordings die with their imposter — the purge `ClusterProxyStore::clear`'s doc
    /// leans on. A regression here makes a deleted-and-recreated imposter answer
    /// `AlreadyRecorded` with the dead imposter's response, forever.
    #[tokio::test]
    async fn deleting_an_imposter_purges_its_proxy_markers() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            request(
                1,
                ControlOp::PutImposter {
                    config: Box::new(proxy_imposter_config(8080)),
                },
            ),
        )
        .await;
        let recorded = apply_one(&mut sm, 2, proxy_recorded(2, 8080, "aa11", "kept", None)).await;
        assert_eq!(recorded.outcome, ControlOutcome::Applied);
        assert!(marker(&sm, 8080, "aa11").is_some());

        apply_one(
            &mut sm,
            3,
            request(3, ControlOp::DeleteImposter { port: 8080 }),
        )
        .await;
        assert!(
            marker(&sm, 8080, "aa11").is_none(),
            "the delete purges the port's markers atomically"
        );
    }

    /// The apply-level first-wins guard: a duplicate proxyOnce commit for the same
    /// signature (a submit retried past dedup, or racing owners across a handoff) is a
    /// no-op — the first recording keeps both the row and the stub list it produced.
    #[tokio::test]
    async fn a_duplicate_proxy_once_recording_is_a_no_op() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            request(
                1,
                ControlOp::PutImposter {
                    config: Box::new(proxy_imposter_config(8080)),
                },
            ),
        )
        .await;
        let first = apply_one(
            &mut sm,
            2,
            proxy_recorded(
                2,
                8080,
                "cc33",
                "first",
                Some(recorded_stub(
                    "first",
                    crate::control::RecordedStubPlacement::BeforeProxy,
                )),
            ),
        )
        .await;
        assert_eq!(first.outcome, ControlOutcome::Applied);

        let duplicate = apply_one(
            &mut sm,
            3,
            proxy_recorded(
                3,
                8080,
                "cc33",
                "second",
                Some(recorded_stub(
                    "second",
                    crate::control::RecordedStubPlacement::BeforeProxy,
                )),
            ),
        )
        .await;
        assert_eq!(duplicate.outcome, ControlOutcome::Applied);

        let row = marker(&sm, 8080, "cc33").expect("row survives");
        assert!(
            row.contains("Zmlyc3Q=") || row.contains("first") || row.contains("102"),
            "the first recording wins the row: {row}"
        );
        let config_json = sm
            .read_config(8080)
            .expect("read config")
            .expect("imposter present");
        let config: ImposterConfig = serde_json::from_str(&config_json).expect("config parses");
        assert_eq!(
            config.stubs.len(),
            2,
            "the duplicate inserted no second recorded stub"
        );
    }

    /// A recording racing a concurrent delete is refused — including the stub-less path,
    /// which must not re-insert a marker after `DeleteImposter`'s purge (a later imposter
    /// on the same port would wrongly replay the dead one's response).
    #[tokio::test]
    async fn a_recording_for_a_missing_imposter_is_refused() {
        let (_td, mut sm) = fresh_sm(None).await;
        for (index, stub) in [
            None,
            Some(recorded_stub(
                "late",
                crate::control::RecordedStubPlacement::BeforeProxy,
            )),
        ]
        .into_iter()
        .enumerate()
        {
            let refused = apply_one(
                &mut sm,
                (index + 1) as u64,
                proxy_recorded((index + 1) as u128, 9999, "dd44", "late", stub),
            )
            .await;
            let ControlOutcome::Failed { reason } = &refused.outcome else {
                panic!("recording an absent imposter must be refused: {refused:?}");
            };
            assert!(reason.contains("no imposter"), "names the cause: {reason}");
        }
        assert!(marker(&sm, 9999, "dd44").is_none());
    }

    /// Blocker 1: before this feature, an unscoped `DELETE savedRequests` proxied to the
    /// engine's `ClusterJournal::clear`, which zeroed the count slot behind `numberOfRequests`.
    /// The op now commits as a generation bump instead, so nothing else zeroes it — this pins
    /// that the apply path does the zeroing itself, for a port-wide clear.
    #[tokio::test]
    async fn a_port_wide_clear_resets_the_fleet_count() {
        let journal = ClusterJournal::new(1);
        let (_td, sm) = fresh_sm(None).await;
        let mut sm = sm.with_journal(&journal);
        journal.note_request(8080);
        journal.note_request(8080);
        assert_eq!(
            journal.read_shard_since(8080, 0).count_slot,
            2,
            "counted before the clear"
        );

        apply_one(&mut sm, 1, journal_clear(1, 8080, None)).await;

        assert_eq!(
            journal.read_shard_since(8080, 0).count_slot,
            0,
            "a port-wide clear applying through Raft must zero this node's own count slot"
        );
    }

    /// Blocker 1's other half: a space-scoped bump must leave the count alone, matching
    /// `clear_flow`/`retain`'s existing contract that a scoped deletion never resets the total.
    #[tokio::test]
    async fn a_space_scoped_clear_leaves_the_count_alone() {
        let journal = ClusterJournal::new(1);
        let (_td, sm) = fresh_sm(None).await;
        let mut sm = sm.with_journal(&journal);
        journal.note_request(8080);
        journal.note_request(8080);

        apply_one(&mut sm, 1, journal_clear(1, 8080, Some("f"))).await;

        assert_eq!(
            journal.read_shard_since(8080, 0).count_slot,
            2,
            "a space-scoped bump must not touch the count slot"
        );
    }

    /// Blocker 2: the generation lives in `sm_journal_gens` (durable) and in the process-local
    /// journal (rebuilt from scratch on every restart). Simulates a cold start — apply clears
    /// against an sm with no journal bound (as if this were a previous process's commits, now
    /// only durable), then bind a *fresh* journal the way a restarted process would and run the
    /// same reconcile the compose cold-start loop calls once caught up to the leader.
    #[tokio::test]
    async fn clear_generations_are_rehydrated_after_a_restart() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, journal_clear(1, 8080, None)).await;
        apply_one(&mut sm, 2, journal_clear(2, 8080, Some("f"))).await;

        let journal = ClusterJournal::new(1);
        let sm = sm.with_journal(&journal);
        assert_eq!(
            journal.read_shard_since(8080, 0).clear_gen,
            0,
            "a fresh journal starts at generation 0, exactly the dangerous default this test \
             must not observe after reconcile"
        );

        sm.reconcile_engine().await.expect("reconcile");

        let shard = journal.read_shard_since(8080, 0);
        assert_eq!(
            shard.clear_gen, 1,
            "the port-wide generation must be rehydrated from sm_journal_gens on cold start"
        );
        assert_eq!(
            shard.space_gens,
            vec![("f".to_owned(), 1)],
            "a space-scoped generation must be rehydrated too"
        );

        // And a fresh append is stamped with the rehydrated generation, not 0 — the whole
        // point of rehydrating before anything else can record.
        journal.record_indexed(
            8080,
            "f",
            RecordedRequest {
                mode: ResponseMode::Text,
                request_from: "t".into(),
                method: "GET".into(),
                path: "/after-restart".into(),
                query: Default::default(),
                headers: Default::default(),
                body: None,
                timestamp: "t".into(),
                match_outcome: None,
                status: None,
                latency_ms: None,
                node: None,
            },
        );
        let stamped = journal.read_shard_since(8080, 0).entries;
        assert_eq!(stamped.len(), 1);
        assert_eq!(
            (stamped[0].clear_gen, stamped[0].space_gen),
            (1, 1),
            "a post-restart append must be stamped with the rehydrated generations, not 0"
        );
    }

    /// Non-blocker 1: `install_snapshot` clears and reinserts `sm_journal_gens` from the
    /// payload precisely because a generation this node still holds can be *higher* than what
    /// the fleet now agrees on — a stale leader that cleared, was partitioned, and rejoined by
    /// snapshot from a peer that never saw it. The live journal must follow the durable table
    /// down too, or this node's stuck-high generation silently wins the fleet-wide max a merge
    /// computes and drops every other node's entries.
    #[tokio::test]
    async fn installing_a_snapshot_lowers_a_live_generation_that_is_ahead() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, journal_clear(1, 8080, None)).await;
        let snapshot: Snapshot<TypeConfig> = sm.build_snapshot().await.expect("build snapshot");

        let journal = ClusterJournal::new(1);
        journal.set_clear_gen(8080, None, 99);
        assert_eq!(journal.read_shard_since(8080, 0).clear_gen, 99);

        let (_td2, restored) = fresh_sm(None).await;
        let mut restored = restored.with_journal(&journal);
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        assert_eq!(
            journal.read_shard_since(8080, 0).clear_gen,
            1,
            "install_snapshot must be able to LOWER a live generation that outran the fleet's \
             agreed value — set_clear_gen's fetch_max cannot do this"
        );
    }

    /// Non-blocker 2: `journal_generations_survive_a_snapshot_install` above never binds a
    /// journal (`fresh_sm(None)` both sides), so it only ever exercised the durable table — the
    /// loop that pushes into the *live* journal never ran in any test. This sibling binds one on
    /// both the source and the installing state machine and asserts the live journal actually
    /// received the generations, port-wide and space-scoped.
    #[tokio::test]
    async fn a_snapshot_install_pushes_generations_into_the_bound_live_journal() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, journal_clear(1, 8080, None)).await;
        apply_one(&mut sm, 2, journal_clear(2, 8080, Some("f"))).await;
        let snapshot: Snapshot<TypeConfig> = sm.build_snapshot().await.expect("build snapshot");

        let journal = ClusterJournal::new(1);
        let (_td2, restored) = fresh_sm(None).await;
        let mut restored = restored.with_journal(&journal);
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        let shard = journal.read_shard_since(8080, 0);
        assert_eq!(
            shard.clear_gen, 1,
            "install_snapshot must push the port-wide generation into the live journal, not \
             just the durable table"
        );
        assert_eq!(
            shard.space_gens,
            vec![("f".to_owned(), 1)],
            "and the space-scoped generation too"
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

        let delete = |op_id: u128| request(op_id, ControlOp::DeleteRoute { id: "a".to_owned() });
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

    // -- issue #373: the fleet's operator-set name ----------------------------

    fn set_fleet_name(op_id: u128, name: &str) -> ControlRequest {
        request(
            op_id,
            ControlOp::FleetNamePut {
                name: name.to_owned(),
            },
        )
    }

    #[tokio::test]
    async fn an_unset_fleet_name_reads_as_absent() {
        let (_td, sm) = fresh_sm(None).await;
        assert_eq!(
            sm.fleet_name().expect("read fleet name"),
            None,
            "a fleet nobody has named reads as absent, not as an empty string and not as an error"
        );
    }

    #[tokio::test]
    async fn fleet_name_reads_back_what_was_applied() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "rift-prod-eu"))])
            .await
            .expect("apply");
        assert_eq!(
            sm.fleet_name().expect("read fleet name"),
            Some("rift-prod-eu".to_owned())
        );
    }

    #[tokio::test]
    async fn a_second_fleet_name_write_renames_rather_than_appending() {
        // Setting the first name and renaming are one op, so the second write must replace the
        // first outright — a fleet with two names is the state this whole feature exists to
        // make impossible.
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![
            entry(1, set_fleet_name(1, "rift-prod-eu")),
            entry(2, set_fleet_name(2, "rift-prod-us")),
        ])
        .await
        .expect("apply");
        assert_eq!(
            sm.fleet_name().expect("read fleet name"),
            Some("rift-prod-us".to_owned())
        );
    }

    // -- snapshot payloads on disk (#436) --------------------------------------

    /// The snapshot payload is a file beside redb, and `raft_snapshot` holds only the metadata
    /// and the file name.
    ///
    /// The row assertion is the load-bearing half: before #436 the row *was* the payload,
    /// re-encoded as a JSON integer array (~3.7x). A row that stays small is the only direct
    /// evidence the bytes moved rather than being copied.
    ///
    /// Pins D-16 (amendment): redb keeps the snapshot's metadata and the file name only; the
    /// payload bytes live in a file beside it.
    #[tokio::test]
    async fn the_stored_snapshot_is_a_file_beside_redb_not_a_row() {
        let (td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "rift-prod-eu"))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, .. } = builder.build_snapshot().await.expect("build snapshot");

        let file = td.path().join("snapshot").join(&meta.snapshot_id);
        let on_disk = std::fs::read(&file)
            .unwrap_or_else(|e| panic!("the snapshot must exist at {file:?}: {e}"));
        let payload: super::SnapshotPayload =
            serde_json::from_slice(&on_disk).expect("the file is the payload verbatim");
        assert_eq!(payload.fleet_name.as_deref(), Some("rift-prod-eu"));

        // redb allows one open handle per file, and `sm`/`builder` still hold this one.
        drop(builder);
        drop(sm);
        let db = super::Database::create(td.path().join("raft.redb")).expect("reopen");
        let read = db.begin_read().expect("read txn");
        let table = read
            .open_table(super::SNAPSHOT_TABLE)
            .expect("snapshot table");
        let row = table.get(()).expect("get row").expect("a row exists");
        // Size alone is a weak claim: a *small* payload inlined the old way would also fit under
        // any threshold. Assert the row's shape instead — it must parse as `{meta, file}` naming
        // this snapshot, and must NOT parse as the pre-#436 `{meta, data}`.
        let parsed: super::StoredSnapshot =
            serde_json::from_slice(row.value()).expect("the row is the current shape");
        assert_eq!(parsed.file, meta.snapshot_id);
        assert!(
            serde_json::from_slice::<super::LegacyStoredSnapshot>(row.value()).is_err(),
            "the row must not carry an inlined payload"
        );
        assert!(
            row.value().len() < 4096,
            "the row must carry only meta + file name, not the payload — got {} bytes",
            row.value().len()
        );
    }

    /// #436 AC1: the stored artifact is within 1.1x the raw bytes it carries (was ~3.7x).
    ///
    /// Measured at 4 MiB rather than the AC's 64 MiB because the ratio is scale-invariant by
    /// construction — the file *is* the payload encoding, so nothing about it varies with size —
    /// and a 64 MiB in-process build costs tens of seconds. The literal 64 MiB figure is recorded
    /// as a measurement in `09-durability-failure.md` (AC4), not as a gate.
    /// #444's gate: the snapshot trio must not run its synchronous body on a runtime worker.
    ///
    /// Pinned to **one** worker on purpose. That is what makes the failure deterministic on any
    /// machine rather than only on a 2-vCPU CI runner: with a single worker, a synchronous body
    /// inside an `async fn` stops the timer wheel outright, so the ticker below simply does not run
    /// for the duration of the build. On a 10-core laptop the same defect hides, which is exactly
    /// why the original report could not be reproduced locally.
    ///
    /// The threshold is `election_timeout_min` (150 ms, `node.rs::raft_config`) because that is the
    /// figure with consequences: a leader that stops servicing its runtime for longer than a
    /// follower's election timeout loses leadership while doing nothing wrong. Measuring the gap
    /// with **no joiner present** is deliberate — it isolates the leader-side cost of *producing* a
    /// snapshot from anything on the install path, which is what made this hard to see when it was
    /// only observable through a catch-up scenario under CI load.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_snapshot_build_does_not_starve_the_runtime_worker() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        let (_td, mut sm) = fresh_sm(None).await;

        // >= 16 MiB of state machine, spread over four imposters. Anything much smaller stops
        // discriminating: the point is a build long enough to outrun an election timeout, and a
        // snapshot that fits comfortably inside one would pass against the unfixed code too.
        //
        // Each imposter gets **distinct** bytes, so four ports really are four ports' worth of
        // payload rather than the same string compressing into one.
        for (i, tag) in ["alpha", "beta", "gamma", "delta"].iter().enumerate() {
            let op = i as u128 + 1;
            let port = 19_100 + i as u16;
            let applied = sm
                .apply(vec![entry(
                    i as u64 + 1,
                    request(
                        op,
                        ControlOp::PutImposter {
                            config: Box::new(bulky_config(port, tag, 4 * 1024 * 1024)),
                        },
                    ),
                )])
                .await
                .expect("apply imposter");
            // Without this the whole test passes vacuously if the writes were refused — a tiny
            // snapshot builds fast enough to clear the threshold on the unfixed code.
            assert_eq!(
                applied.first().map(|r| &r.outcome),
                Some(&ControlOutcome::Applied),
                "imposter {tag} must actually be stored, or the gap below measures nothing"
            );
        }
        let sm = sm;

        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let max_gap_ms = std::sync::Arc::new(AtomicU64::new(0));
        let ticker = tokio::spawn({
            let stop = std::sync::Arc::clone(&stop);
            let max_gap_ms = std::sync::Arc::clone(&max_gap_ms);
            async move {
                let mut last = std::time::Instant::now();
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    let now = std::time::Instant::now();
                    let gap = now.duration_since(last).as_millis() as u64;
                    max_gap_ms.fetch_max(gap, Ordering::Relaxed);
                    last = now;
                }
            }
        });

        // Let the ticker reach its cadence, then discard the warm-up: the first gaps include this
        // task's own scheduling and the tail of the population above, neither of which is what is
        // being measured.
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        max_gap_ms.store(0, Ordering::Relaxed);

        // Run the snapshot work in a *spawned task*, not in the test future. On a `multi_thread`
        // runtime `block_on` drives the test future on the calling thread, while spawned tasks get
        // the worker pool — so doing the build inline would put it on a different thread from the
        // ticker and observe nothing, however long it blocked. Production runs `build_snapshot` on
        // openraft's state-machine task, which is a spawned task on the same runtime, and that is
        // the arrangement being reproduced: one worker, the ticker and the snapshot work competing
        // for it.
        let worked = tokio::spawn({
            let mut builder = sm.clone();
            let mut sm = sm.clone();
            async move {
                let Snapshot { meta, snapshot } =
                    builder.build_snapshot().await.expect("build snapshot");
                let current = sm
                    .get_current_snapshot()
                    .await
                    .expect("get current snapshot");
                assert!(
                    current.is_some(),
                    "the build must leave a readable current snapshot, or the read measured nothing"
                );
                sm.install_snapshot(&meta, snapshot)
                    .await
                    .expect("install snapshot");
            }
        });
        worked.await.expect("snapshot task");

        stop.store(true, Ordering::Relaxed);
        ticker.await.expect("ticker task");

        let observed = max_gap_ms.load(Ordering::Relaxed);
        assert!(
            observed < 150,
            "the runtime worker stalled {observed} ms across build/read/install; \
             anything at or past 150 ms (election_timeout_min) is long enough for a follower to \
             call an election and take leadership from a leader that is merely snapshotting"
        );
    }

    #[tokio::test]
    async fn a_stored_snapshot_is_within_one_point_one_times_the_raw_bytes() {
        let (td, mut sm) = fresh_sm(None).await;
        let big = bulky_config(19_200, "big", 4 * 1024 * 1024);
        let raw = serde_json::to_string(&big).expect("encode config").len();
        let applied = sm
            .apply(vec![entry(
                1,
                request(
                    1,
                    ControlOp::PutImposter {
                        config: Box::new(big),
                    },
                ),
            )])
            .await
            .expect("apply");
        // Without this the whole test passes vacuously if the write were refused: a near-empty
        // snapshot trivially satisfies a "<= 1.1x raw" bound.
        assert_eq!(
            applied.first().map(|r| &r.outcome),
            Some(&ControlOutcome::Applied),
            "the imposter must actually be stored, or the ratio below measures nothing"
        );
        let mut builder = sm.clone();
        let Snapshot { meta, .. } = builder.build_snapshot().await.expect("build snapshot");

        let stored = std::fs::metadata(td.path().join("snapshot").join(&meta.snapshot_id))
            .expect("snapshot file")
            .len() as usize;
        assert!(
            stored <= raw * 11 / 10,
            "stored {stored} must be <= 1.1x raw {raw} (ratio {:.2})",
            stored as f64 / raw as f64
        );
    }

    /// An installed snapshot must be readable back as the current one.
    ///
    /// This pins the regression that openraft's conformance suite caught and none of my own tests
    /// did: `build_snapshot` wrote the payload file and never committed the `SNAPSHOT_TABLE` row,
    /// so the file existed and nothing pointed at it. Every other install assertion reads the
    /// *other* tables written in the same transaction, so deleting the row write left them all
    /// green — the state was right, the snapshot was simply lost.
    #[tokio::test]
    async fn an_installed_snapshot_is_readable_back_as_the_current_snapshot() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "readable-back"))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");
        assert_eq!(
            builder
                .get_current_snapshot()
                .await
                .expect("read back")
                .expect("a built snapshot must be the current one")
                .meta
                .snapshot_id,
            meta.snapshot_id,
            "build must commit the row that names its payload, not only write the file"
        );

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");
        assert_eq!(
            follower
                .get_current_snapshot()
                .await
                .expect("read back")
                .expect("an installed snapshot must be the current one")
                .meta
                .snapshot_id,
            meta.snapshot_id,
            "install must commit the row too — a follower that cannot serve on what it just \
             installed will rebuild from nothing"
        );
    }

    /// A handle that arrives positioned at EOF must still install.
    ///
    /// This is how openraft actually delivers a transfer: it writes chunk after chunk into the
    /// handle from `begin_receiving_snapshot` and hands that same handle to `install_snapshot`,
    /// so it arrives at the end of the payload rather than the start. Every other unit test here
    /// supplies a handle already at 0, which is exactly why this bug reached the cluster tests
    /// before anything in this file noticed.
    #[tokio::test]
    async fn a_received_snapshot_positioned_at_eof_still_installs() {
        use tokio::io::AsyncWriteExt as _;

        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "written-at-eof"))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");
        let payload = read_snapshot_bytes(snapshot).await;

        let (_td2, mut follower) = fresh_sm(None).await;
        let mut handle = follower
            .begin_receiving_snapshot()
            .await
            .expect("begin receiving");
        // Deliberately NOT rewound afterwards — that is the whole point.
        handle
            .write_all(&payload)
            .await
            .expect("stream the payload");
        handle.flush().await.expect("flush");

        follower
            .install_snapshot(&meta, handle)
            .await
            .expect("a handle left at EOF by the transport must still install");
        assert_eq!(
            follower.fleet_name().expect("read fleet name"),
            Some("written-at-eof".to_owned())
        );
    }

    /// A build's GC must not delete a transfer that is still arriving.
    ///
    /// openraft spawns `build_snapshot` as a detached task (`sm::worker`) and its worker loop moves
    /// straight on to the next command, so a build finishing while an install streams into the same
    /// directory is ordinary scheduling, not a rare interleaving.
    #[tokio::test]
    async fn a_build_does_not_gc_a_transfer_that_is_still_arriving() {
        let (td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "concurrent"))])
            .await
            .expect("apply");

        // An install in flight: the handle exists and its file is being written to.
        let _receiving = sm
            .begin_receiving_snapshot()
            .await
            .expect("begin receiving");
        let before: Vec<_> = std::fs::read_dir(td.path().join("snapshot"))
            .expect("scan")
            .flatten()
            .map(|e| e.file_name())
            .collect();
        assert!(
            before
                .iter()
                .any(|n| n.to_string_lossy().starts_with("receiving-")),
            "the in-flight transfer must have a file to protect"
        );

        let mut builder = sm.clone();
        builder.build_snapshot().await.expect("build snapshot");

        let after: Vec<_> = std::fs::read_dir(td.path().join("snapshot"))
            .expect("scan")
            .flatten()
            .map(|e| e.file_name())
            .collect();
        for name in &before {
            assert!(
                after.contains(name),
                "a concurrent build's GC deleted {name:?}, which an in-flight transfer is using"
            );
        }
    }

    /// #436 AC3: a row written in the pre-#436 format is migrated to a file on first open.
    ///
    /// Migration rather than rebuild: a node part-way through catching a peer up must not lose the
    /// snapshot it already holds just because it restarted onto a new binary.
    #[tokio::test]
    async fn a_legacy_json_snapshot_row_is_migrated_on_open() {
        let (td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "legacy-fleet"))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");
        let payload_bytes = read_snapshot_bytes(snapshot).await;
        drop(sm);
        drop(builder);

        // Rewrite the row in the OLD shape and delete the file, so the only way to answer is to
        // migrate what the row carries.
        let legacy = serde_json::json!({ "meta": meta, "data": payload_bytes });
        let path = td.path().join("raft.redb");
        {
            let db = super::Database::create(&path).expect("reopen");
            let write = db.begin_write().expect("write txn");
            {
                let mut table = write
                    .open_table(super::SNAPSHOT_TABLE)
                    .expect("snapshot table");
                table
                    .insert(
                        (),
                        serde_json::to_vec(&legacy)
                            .expect("encode legacy")
                            .as_slice(),
                    )
                    .expect("insert legacy row");
            }
            write.commit().expect("commit");
        }
        std::fs::remove_dir_all(td.path().join("snapshot")).ok();

        let (_, mut reopened) = new(&path).await.expect("reopen store");
        let current = reopened
            .get_current_snapshot()
            .await
            .expect("read current snapshot")
            .expect("the migrated snapshot must still be there");
        assert_eq!(current.meta.snapshot_id, meta.snapshot_id);
        let migrated = read_snapshot_bytes(current.snapshot).await;
        assert_eq!(
            migrated, payload_bytes,
            "migration must preserve the payload byte-for-byte"
        );
        assert!(
            td.path().join("snapshot").join(&meta.snapshot_id).exists(),
            "the migrated payload must now live in a file"
        );
    }

    /// A row naming a file that is gone reads as "no snapshot" so openraft rebuilds, rather than
    /// erroring the node out of service. The one deliberate fallback in #436 — it must be a
    /// *correct* answer, not a silenced failure.
    #[tokio::test]
    async fn a_snapshot_row_whose_file_vanished_reports_no_snapshot() {
        let (td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "vanishing"))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, .. } = builder.build_snapshot().await.expect("build snapshot");
        std::fs::remove_file(td.path().join("snapshot").join(&meta.snapshot_id))
            .expect("remove the snapshot file");

        assert!(
            builder
                .get_current_snapshot()
                .await
                .expect("a missing file is not an error")
                .is_none(),
            "a row whose file is gone must read as no snapshot, so one gets rebuilt"
        );
    }

    /// Superseded payloads are swept once they are old enough to be unambiguously dead — and a
    /// recent one is deliberately left alone.
    ///
    /// Both halves matter. Sweeping is what stops every build leaking a full copy of the state
    /// machine; the age guard is what stops a sweep deleting a file another in-flight operation is
    /// still writing or has renamed but not yet committed a row for. A GC that only did the first
    /// half was the shape this change originally shipped, and it could unlink a live transfer.
    #[tokio::test]
    async fn a_superseded_snapshot_file_is_swept_once_it_is_old_but_not_before() {
        let (td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "first"))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let first = builder.build_snapshot().await.expect("build 1").meta;

        let dir = td.path().join("snapshot");
        // A file old enough that no in-flight operation could own it.
        let stale = dir.join("stale-leftover");
        std::fs::write(&stale, b"orphan").expect("plant a stale leftover");
        std::fs::File::open(&stale)
            .expect("open stale")
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(3600))
            .expect("age the stale leftover");

        sm.apply(vec![entry(2, set_fleet_name(2, "second"))])
            .await
            .expect("apply");
        let second = builder.build_snapshot().await.expect("build 2").meta;

        assert_ne!(first.snapshot_id, second.snapshot_id);
        assert!(
            dir.join(&second.snapshot_id).exists(),
            "the current snapshot must be on disk"
        );
        assert!(
            !stale.exists(),
            "a payload old enough to be unambiguously dead must be swept"
        );
        assert!(
            dir.join(&first.snapshot_id).exists(),
            "a payload this recent must be left alone — it is indistinguishable from one an \
             in-flight build has renamed but not yet committed a row for"
        );
    }

    /// A received snapshot that does not parse is an error, never a silently empty state machine.
    #[tokio::test]
    async fn an_unparseable_received_snapshot_is_an_error_not_a_default() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "keep-me"))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, .. } = builder.build_snapshot().await.expect("build snapshot");

        let (td2, mut follower) = fresh_sm(None).await;
        let junk = td2.path().join("not-a-snapshot");
        std::fs::write(&junk, b"{ this is not json").expect("write junk");
        let handle = Box::new(
            tokio::fs::File::open(&junk)
                .await
                .expect("open junk snapshot"),
        );
        follower
            .install_snapshot(&meta, handle)
            .await
            .expect_err("an unparseable snapshot must fail loudly");
    }

    #[tokio::test]
    async fn snapshot_round_trips_the_fleet_name() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "rift-prod-eu"))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");

        assert_eq!(
            follower.fleet_name().expect("read fleet name"),
            Some("rift-prod-eu".to_owned()),
            "a node that joins by snapshot must come back knowing which fleet it is in"
        );
    }

    #[tokio::test]
    async fn a_snapshot_without_a_fleet_name_installs_and_reads_absent() {
        // The #134/#137 lesson, applied before it can bite again: a snapshot built before this
        // field existed must still install. Simulated faithfully by stripping the key from a
        // real snapshot's JSON rather than by trusting `#[serde(default)]` in the abstract.
        let (td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, set_fleet_name(1, "rift-prod-eu"))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let mut payload: serde_json::Value =
            serde_json::from_slice(&read_snapshot_bytes(snapshot).await).expect("snapshot is json");
        let removed = payload
            .as_object_mut()
            .expect("snapshot payload is an object")
            .remove("fleet_name");
        assert!(
            removed.is_some(),
            "the field must be present in a current snapshot, or this test proves nothing"
        );
        let older =
            snapshot_handle_from(td.path(), &serde_json::to_vec(&payload).expect("re-encode"))
                .await;

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, older)
            .await
            .expect("a snapshot predating the fleet name must still install");
        assert_eq!(
            follower.fleet_name().expect("read fleet name"),
            None,
            "an older snapshot carries no name, which reads as absent — the same as a fleet \
             nobody has named"
        );
    }

    // -- issue #210: the route table's revision precondition ------------------

    fn delete_route(op_id: u128, id: &str) -> ControlRequest {
        request(op_id, ControlOp::DeleteRoute { id: id.to_owned() })
    }

    fn routes_revision(sm: &RedbStateMachine) -> u64 {
        sm.route_table_with_revision().expect("read route table").1
    }

    fn route_ids(sm: &RedbStateMachine) -> Vec<String> {
        let mut ids: Vec<String> = sm
            .route_table()
            .expect("read")
            .routes
            .into_iter()
            .map(|r| r.id)
            .collect();
        ids.sort();
        ids
    }

    /// #210 gate: a whole-table replace can be conditioned on the revision the
    /// reader saw, and a stale one is refused inside apply — deterministically,
    /// on every replica — instead of silently clobbering a concurrent edit.
    #[tokio::test]
    async fn route_table_preconditions_gate_apply_deterministically() {
        let conditioned = |op_id: u128, expected: u64, op: ControlOp| ControlRequest {
            expected_revision: Some(expected),
            ..request(op_id, op)
        };

        let (_td, mut sm, _routes) = fresh_sm_with_routes().await;
        let (_td2, mut sm2, _routes2) = fresh_sm_with_routes().await;

        // A table nobody has written is revision 0, and conditioning on 0 is
        // how a first writer wins the race to create one.
        assert_eq!(routes_revision(&sm), 0);
        for s in [&mut sm, &mut sm2] {
            let response = s
                .apply(vec![entry(
                    1,
                    conditioned(
                        1,
                        0,
                        ControlOp::PutRoutes {
                            table: RouteTable {
                                routes: vec![test_route("a", 1)],
                            },
                        },
                    ),
                )])
                .await
                .expect("apply");
            assert_eq!(response, vec![ControlResponse::applied(1)]);
        }
        assert_eq!(
            routes_revision(&sm),
            1,
            "the stamp is the applying log index"
        );

        // A stale expectation refuses — same committed `Failed` on both
        // replicas, and (the whole point) the table is untouched.
        let refused = sm
            .apply(vec![entry(
                2,
                conditioned(
                    2,
                    0,
                    ControlOp::PutRoutes {
                        table: RouteTable {
                            routes: vec![test_route("clobber", 2)],
                        },
                    },
                ),
            )])
            .await
            .expect("apply");
        let refused2 = sm2
            .apply(vec![entry(
                2,
                conditioned(
                    2,
                    0,
                    ControlOp::PutRoutes {
                        table: RouteTable {
                            routes: vec![test_route("clobber", 2)],
                        },
                    },
                ),
            )])
            .await
            .expect("apply");
        assert_eq!(refused, refused2, "replicas must agree");
        match &refused[0].outcome {
            ControlOutcome::Failed { reason } => {
                assert!(
                    reason.starts_with("revision conflict"),
                    "the front dispatches a 409 off this exact prefix: {reason}"
                );
                assert!(reason.contains("route table"), "{reason}");
            }
            other => panic!("expected a committed refusal, got {other:?}"),
        }
        assert_eq!(
            route_ids(&sm),
            vec!["a".to_owned()],
            "a refused precondition must not have replaced the table"
        );
        assert_eq!(
            routes_revision(&sm),
            1,
            "a refusal does not advance the revision either"
        );

        // A delete mutates the table, so it stamps too — an outstanding
        // precondition must not survive one.
        let applied = sm
            .apply(vec![entry(3, conditioned(3, 1, delete_route(3, "a").op))])
            .await
            .expect("apply");
        assert_eq!(applied, vec![ControlResponse::applied(3)]);
        assert_eq!(routes_revision(&sm), 3);

        // Even a delete that removed nothing: the op committed against the
        // table, so the revision it leaves behind must reflect that.
        sm.apply(vec![entry(4, delete_route(4, "never-existed"))])
            .await
            .expect("apply");
        assert_eq!(
            routes_revision(&sm),
            4,
            "an idempotent delete still committed against this table"
        );
    }

    /// #210: the revision travels with the snapshot. A follower that joined by
    /// snapshot install must refuse the same stale tokens the leader does —
    /// otherwise routing a conditioned write to the fresh node is a way around
    /// the precondition.
    #[tokio::test]
    async fn snapshot_round_trips_the_route_table_revision() {
        let (_td, mut sm, _routes) = fresh_sm_with_routes().await;
        sm.apply(vec![entry(1, put_routes(1, vec![test_route("a", 1)]))])
            .await
            .expect("apply");
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower, _follower_routes) = fresh_sm_with_routes().await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");

        assert_eq!(routes_revision(&follower), 1);

        // And it is a live precondition on the follower, not just a stored
        // number: a stale token is refused there too.
        let refused = follower
            .apply(vec![entry(
                3,
                ControlRequest {
                    expected_revision: Some(0),
                    ..put_routes(3, vec![test_route("clobber", 9)])
                },
            )])
            .await
            .expect("apply");
        assert!(
            matches!(&refused[0].outcome, ControlOutcome::Failed { reason }
                if reason.starts_with("revision conflict")),
            "{refused:?}"
        );
    }

    /// A snapshot built before #210 carries no revision at all. It must still
    /// install, and the table must read as revision 0 — which *fails* a
    /// stale precondition rather than passing one. The dangerous alternative
    /// (inheriting the last applied index) would let a token minted before the
    /// join silently pass.
    #[tokio::test]
    async fn a_pre_route_revision_snapshot_installs_and_reads_zero() {
        let (_td, sm, _routes) = fresh_sm_with_routes().await;
        let legacy = json!({
            "configs": [],
            "routes": [["a", "{}"]],
            "dedup": [],
            "last_applied_log": null,
            "last_membership": { "log_id": null, "membership": { "configs": [], "nodes": {} } },
        });
        let payload: super::SnapshotPayload =
            serde_json::from_value(legacy).expect("a pre-#210 snapshot payload still decodes");
        assert!(
            payload.routes_revision.is_none(),
            "the missing field defaults to absent, not a parse failure"
        );
        assert_eq!(
            routes_revision(&sm),
            0,
            "a table with no stored revision reads 0"
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

    /// An older snapshot, written before #185 existed, must still install —
    /// same `#[serde(default)]` contract every table added since #134 carries.
    #[tokio::test]
    async fn a_pre_session_key_snapshot_still_installs() {
        let (td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, put(1, 8080, json!([{ "id": "a" }]))).await;
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        // Strip #185's `session_key`, standing in for a payload serialized by a binary that
        // predates it.
        let mut payload: serde_json::Value =
            serde_json::from_slice(&read_snapshot_bytes(snapshot).await)
                .expect("snapshot payload is JSON");
        payload
            .as_object_mut()
            .expect("payload is an object")
            .remove("session_key");
        let stripped =
            snapshot_handle_from(td.path(), &serde_json::to_vec(&payload).expect("re-encode"))
                .await;

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, stripped)
            .await
            .expect("a pre-#185 snapshot must still install");
        assert_eq!(follower.session_key().expect("read session key"), None);
    }

    /// Overwrite a one-row table's value with something that is not JSON, simulating on-disk
    /// corruption or a forward-incompatible record written by a newer binary.
    ///
    /// These assertions stop at the accessor on purpose. An end-to-end version — corrupt the file,
    /// restart the node, assert the admin front answers `500` — was written and **withdrawn**: a
    /// restarted node rebuilds its state before serving, so the corruption is gone by the time the
    /// first request arrives, and the test passed locally while failing in CI. A test that
    /// green-lights a security invariant only on some machines is worse than none, because the
    /// failures read as flakes. Making it deterministic would need log surgery invasive enough to
    /// stop resembling the scenario it models.
    ///
    /// The accessor is the linchpin regardless: every caller reaches `should_bypass` only after an
    /// `Ok(None)`, so an `Err` here cannot become an authorization decision anywhere upstream.
    fn corrupt_row(sm: &RedbStateMachine, table: TableDefinition<&str, &str>, key: &str) {
        let write = sm.db.begin_write().expect("write txn");
        {
            let mut t = write.open_table(table).expect("open table");
            t.insert(key, "{ this is not a record }")
                .expect("overwrite row");
        }
        write.commit().expect("commit");
    }

    /// A corrupt session-key row is an **error**, never `None`.
    ///
    /// The distinction is the whole point. `None` means "no console login has ever minted a key",
    /// which `ensure_session_key` answers by minting a fresh one — so if corruption read back as
    /// `None`, the next login would quietly mint a *second* key, invalidating every outstanding
    /// session fleet-wide, and the node would look perfectly healthy while doing it. An error
    /// surfaces as a 500 on the paths that need the key and leaves the record alone.
    #[tokio::test]
    async fn a_corrupt_session_key_row_is_an_error_not_an_absent_key() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            request_at(
                1,
                1_000,
                ControlOp::SessionKeyPut {
                    key: "42".repeat(32),
                },
            ),
        )
        .await;
        assert!(sm.session_key().expect("read").is_some(), "key was minted");

        corrupt_row(&sm, SM_SESSION_KEY_TABLE, SESSION_KEY_ROW);

        assert!(
            sm.session_key().is_err(),
            "a corrupt session-key row read back as an absent key — the next login would mint a \
             second key and silently invalidate every live session"
        );
    }

    /// RFC-006 §5.3, issue #185: the session-signing key must travel through a snapshot install
    /// like every other replicated table — miss this and a node that joins by snapshot cannot
    /// verify cookies the rest of the fleet accepts, and if it is the one that later serves a
    /// login, it silently mints a second key that invalidates every outstanding session fleet-wide.
    #[tokio::test]
    async fn session_key_survives_a_snapshot_install() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            request_at(
                1,
                1_000,
                ControlOp::SessionKeyPut {
                    key: "42".repeat(32),
                },
            ),
        )
        .await;

        let key_before = sm.session_key().expect("read session key");
        assert!(key_before.is_some(), "a session key was minted");

        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");

        assert_eq!(
            follower.session_key().expect("read session key"),
            key_before,
            "a node joining by snapshot install must inherit the fleet's session-signing key"
        );
    }

    /// U-10 (upstream #855). The admin request task's `with_principal_scope` does
    /// not survive the hop to openraft's state-machine task, so without the
    /// re-opened scope in `drive_engine` every clustered change event reaches
    /// M3's SSE unattributed. The log entry being correctly attributed does not
    /// help it — it is on the event path, not the log path.
    #[tokio::test]
    async fn a_clustered_change_event_carries_the_principal_from_the_log() {
        use rift_cluster_base::seams::{EventContext, ImposterEvent, ImposterEventListener};

        #[derive(Default)]
        struct Recorder(parking_lot::Mutex<Vec<Option<String>>>);
        impl ImposterEventListener for Recorder {
            fn on_event(&self, _event: &ImposterEvent, ctx: &EventContext) {
                self.0.lock().push(ctx.principal.clone());
            }
        }

        let recorder = Arc::new(Recorder::default());
        let engine = Arc::new(
            ImposterManager::new()
                .with_event_listener(Arc::clone(&recorder) as Arc<dyn ImposterEventListener>),
        );
        let (_td, mut sm) = fresh_sm(Some(engine)).await;

        let mut req = put(7, 18080, json!([{ "id": "a" }]));
        req.principal = Some("acme/alice".to_owned());
        apply_one(&mut sm, 1, req).await;

        let seen = recorder.0.lock().clone();
        assert!(
            !seen.is_empty(),
            "the apply must have driven the engine and emitted at least one event"
        );
        assert!(
            seen.iter().all(|p| p.as_deref() == Some("acme/alice")),
            "every event from this apply must carry the committing principal, \
             not None: {seen:?}"
        );
    }

    /// The other half of the same contract: a drive with no request behind it —
    /// a restart replay — reports absent attribution as absent rather than
    /// borrowing whoever wrote the record originally.
    #[tokio::test]
    async fn a_restart_replay_emits_unattributed_events() {
        use rift_cluster_base::seams::{EventContext, ImposterEvent, ImposterEventListener};

        #[derive(Default)]
        struct Recorder(parking_lot::Mutex<Vec<Option<String>>>);
        impl ImposterEventListener for Recorder {
            fn on_event(&self, _event: &ImposterEvent, ctx: &EventContext) {
                self.0.lock().push(ctx.principal.clone());
            }
        }

        let td = TempDir::new().expect("tempdir");
        let path = td.path().join("raft.redb");
        {
            let (_, mut sm) = new(&path).await.expect("open store");
            let mut req = put(7, 18081, json!([{ "id": "a" }]));
            req.principal = Some("acme/alice".to_owned());
            apply_one(&mut sm, 1, req).await;
        }

        let recorder = Arc::new(Recorder::default());
        let engine = Arc::new(
            ImposterManager::new()
                .with_event_listener(Arc::clone(&recorder) as Arc<dyn ImposterEventListener>),
        );
        let (_, sm) = new(&path).await.expect("reopen store");
        let sm = sm.with_engine(engine);
        sm.reconcile_engine().await.expect("reconcile");

        let seen = recorder.0.lock().clone();
        assert!(
            seen.iter().all(Option::is_none),
            "a restart materializes a table, not one caller's write — absent \
             attribution is reported as absent, never guessed: {seen:?}"
        );
    }

    /// The snapshot-install call site of `AttributedAction::unattributed`. The
    /// restart path is covered above; this is the other one, and it is the one a
    /// joining follower takes — where inventing attribution would be worst,
    /// because a snapshot is the sum of many principals' writes.
    #[tokio::test]
    async fn a_snapshot_install_emits_unattributed_events() {
        use rift_cluster_base::seams::{EventContext, ImposterEvent, ImposterEventListener};

        #[derive(Default)]
        struct Recorder(parking_lot::Mutex<Vec<Option<String>>>);
        impl ImposterEventListener for Recorder {
            fn on_event(&self, _event: &ImposterEvent, ctx: &EventContext) {
                self.0.lock().push(ctx.principal.clone());
            }
        }

        let (_td, mut leader) = fresh_sm(None).await;
        let mut req = put(7, 18085, json!([{ "id": "a" }]));
        req.principal = Some("acme/alice".to_owned());
        apply_one(&mut leader, 1, req).await;

        let mut builder = leader.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let recorder = Arc::new(Recorder::default());
        let engine = Arc::new(
            ImposterManager::new()
                .with_event_listener(Arc::clone(&recorder) as Arc<dyn ImposterEventListener>),
        );
        let (_td2, mut follower) = fresh_sm(Some(engine)).await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");

        let seen = recorder.0.lock().clone();
        assert!(
            seen.iter().all(Option::is_none),
            "a snapshot is the sum of many principals' writes, so naming any one \
             of them would be a lie: {seen:?}"
        );
    }
}
