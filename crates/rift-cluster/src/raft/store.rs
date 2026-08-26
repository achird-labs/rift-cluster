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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    self, AuditRow, AuditSink, ControlOp, ControlRequest, ControlResponse, DEFAULT_TENANT,
    DatasetRecord, Digest, FLEET_SCOPE, OnDrift, PreconditionTarget, Principal, Quotas, Role,
    SessionKey, SourceMode, SourceProvenance, SpecFormat, SpecMeta, SpecProvenance, SpecSource,
    StubEdit, StubEditScript, Tenant, TenantConfigUsage, TenantId, routes_installed_for,
};
use crate::stores::journal::ClusterJournal;
use crate::stores::sequencer::SequencingRegistry;

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
/// `tenant -> revision`: the log index at which `tenant`'s route table was last
/// mutated (issue #210). A missing row reads as `0`.
///
/// A separate table rather than a field on each `sm_routes` row, because the
/// thing a client conditions a whole-table replace on is the *set*, not any one
/// route — and a per-row copy would have no answer at all for a tenant whose
/// last mutation was a delete that emptied the table.
///
/// `0` for "never written" is load-bearing in the safe direction: a tenant that
/// has never had a route reads `0`, so a client that conditions on `0` and
/// writes first wins, and every later stale token fails. It is never a value
/// that makes a stale precondition pass, because a real mutation stamps a log
/// index and log indices start above zero.
const SM_ROUTES_REVISION_TABLE: TableDefinition<&str, u64> =
    TableDefinition::new("sm_routes_revision");
/// `(tenant, source id) -> StoredSource` (JSON): imposter sources as durable
/// control-plane objects (issue #134). One row per source, like `sm_routes` —
/// a delete is a single-key removal rather than a read-modify-write of a set.
const SM_SOURCES_TABLE: TableDefinition<(&str, &str), &str> = TableDefinition::new("sm_sources");
/// `(tenant, spec id) -> StoredSpec` (JSON): spec records as durable control-plane objects
/// (RFC-004 S2, #278). One row per spec, like `sm_sources` — a delete is a single-key removal
/// rather than a read-modify-write of a set.
const SM_SPECS_TABLE: TableDefinition<(&str, &str), &str> = TableDefinition::new("sm_specs");
/// `digest hex -> document text` (#278): the content-addressed blob store a [`StoredSpec`]'s
/// `meta.digest` points into. Separate from `sm_specs` because the same bytes are commonly held
/// under more than one spec id (two tenants importing the same upstream OpenAPI document, or one
/// tenant re-declaring an id it already holds) — storing the document inline per record would
/// duplicate it once per reference instead of once per distinct byte string.
///
/// "Referenced" has no count column: [`RedbStateMachine::ports_of_spec`]-style logic instead
/// scans `sm_specs` for any row whose digest matches, on every delete/replace — specs are few
/// (an operator-authored set, not a per-request table), so the scan this trades for a maintained
/// refcount is cheap, and a maintained counter is one more value every writer must keep in step
/// or the GC silently drifts from the truth the scan always gets right.
const SM_SPEC_BLOBS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_spec_blobs");
/// `(tenant, dataset name, version) -> StoredDataset` (JSON): dataset records as durable
/// control-plane objects (RFC-005 D1, #285). Versioned rather than one row per name, like
/// `sm_specs` is not: a re-put does not replace a dataset, it adds a new live version and
/// leaves the old one live (D2 binds a stub to a name, not to one version, and RFC-005 §3.2
/// requires the old version keep answering lookups until the tenant deletes the name) — so the
/// version has to be part of the key, or a re-put would have nowhere to put the new row without
/// overwriting the one it must not touch.
const SM_DATASETS_TABLE: TableDefinition<(&str, &str, u64), &str> =
    TableDefinition::new("sm_datasets");
/// `digest hex -> csv text` (#285): the content-addressed blob store a [`StoredDataset`]'s
/// `record.digest` points into — the dataset-table counterpart of [`SM_SPEC_BLOBS_TABLE`], and
/// for the identical reason (see that table's doc): the same bytes are commonly held under more
/// than one name, version, or tenant, and "referenced" is answered by a scan over `sm_datasets`
/// rather than a maintained refcount, for the same trade that table's doc explains.
const SM_DATASET_BLOBS_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("sm_dataset_blobs");
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
/// `(revision, op_id) -> AuditRow` (JSON): the RFC-002 §9 audit projection
/// (issue #163), journal-style.
///
/// Keyed **revision-major** so a `?since=` read is a range scan from that
/// revision rather than a full-table filter, and so the natural key order is
/// the order things happened. `op_id` is the tiebreaker component and exists
/// only because redb keys must be unique — one revision applies exactly one
/// op, so it never actually disambiguates anything today; it is there so that
/// a future batched apply cannot silently overwrite a row.
const SM_AUDIT_TABLE: TableDefinition<(u64, &str), &str> = TableDefinition::new("sm_audit");
/// The fleet's audit export sink as JSON, under [`AUDIT_SINK_KEY`] (issue
/// #164). A one-row table rather than a field on some metadata blob, so it
/// snapshots, installs and gets cleared through exactly the same code shape as
/// every other replicated table.
const SM_AUDIT_SINK_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_audit_sink");
/// The last revision the leader has shipped to the sink, under
/// [`AUDIT_SINK_KEY`] (issue #164). Separate from the sink record because the
/// two have different lifetimes: removing a sink must not lose the checkpoint,
/// or re-declaring one would re-ship the entire retained history.
const SM_AUDIT_CHECKPOINT_TABLE: TableDefinition<&str, u64> =
    TableDefinition::new("sm_audit_checkpoint");
/// The single key both one-row audit-export tables use. Named rather than `()`
/// so the tables read the same as the rest and a second sink, if one is ever
/// wanted, is a key change and not a schema migration.
/// The highest revision retention GC has ever *actually removed* from
/// `sm_audit`, under [`AUDIT_SINK_KEY`] (issue #164).
///
/// Exists because the gap the exporter must report is not derivable from the
/// audit table alone. "The next surviving row is not at `checkpoint + 1`" does
/// **not** mean retention deleted something: `EntryPayload::Blank` (every
/// election), `Membership` entries, and the exporter's own unaudited
/// `AuditCheckpointPut` all consume a revision without producing a row — so
/// that test fires on a perfectly healthy fleet, and would turn the one alarm
/// for permanent audit loss into a rising false positive.
///
/// Written at apply, from the replicated clock, so every replica records the
/// same watermark.
const SM_AUDIT_GC_WATERMARK_TABLE: TableDefinition<&str, u64> =
    TableDefinition::new("sm_audit_gc_watermark");
const AUDIT_SINK_KEY: &str = "sink";
/// The fleet's session-signing key as JSON, under [`SESSION_KEY_ROW`] (RFC-006 §5.3, issue
/// #185). A one-row table, same shape as `sm_audit_sink` and for the same reason: it snapshots,
/// installs and gets cleared through exactly the same code path as every other replicated
/// table, rather than through a hand-written special case that is one commit away from missing
/// the snapshot path and silently logging every console user out after a compaction.
const SM_SESSION_KEY_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_session_key");
/// The single key `sm_session_key` uses, named rather than `()` for the same reason
/// [`AUDIT_SINK_KEY`] is: it reads like the rest of the schema and a second signing key, if one
/// is ever wanted, is a key change and not a schema migration.
const SESSION_KEY_ROW: &str = "key";
/// The fleet's operator-set name as a plain string, under [`FLEET_NAME_ROW`] (issue #373). A
/// one-row table, same shape as `sm_session_key` and for the same reason: it snapshots, installs
/// and gets cleared through exactly the same code path as every other replicated table, rather
/// than through a hand-written special case.
const SM_FLEET_NAME_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_fleet_name");
/// The single key `sm_fleet_name` uses, named rather than `()` for the same reason
/// [`SESSION_KEY_ROW`] is: it reads like the rest of the schema.
const FLEET_NAME_ROW: &str = "name";
/// `(tenant, port, space-tag) -> generation` (issue #224): the applied clear-generation
/// counters `ControlOp::JournalClearGen` bumps. Modelled on `sm_audit_checkpoint` — a small,
/// monotone, per-key counter table — except the key is three-part: `space-tag` is
/// [`journal_gen_space_key`]'s own encoding of `Option<&str>`, not a bare `&str`, because a
/// port-wide clear (`None`) must never be representable the same way as a space-scoped one
/// (`Some`) no matter what the space is named.
const SM_JOURNAL_GENS_TABLE: TableDefinition<(&str, u16, &str), u64> =
    TableDefinition::new("sm_journal_gens");

/// `(tenant, port, sig-hash) -> recorded-response JSON` (#226): the applied proxy-recording
/// markers `ControlOp::ProxyRecorded` writes. A row is both facts at once: *this signature is
/// Recorded* (the claim table any owner — including one elected after a handoff — answers
/// `AlreadyRecorded` from), and *this is the replayable response* (`lookup()`'s durable source
/// for a stub-less proxyOnce recording, which has no recorded stub in config to replay from).
/// Keyed like `sm_journal_gens` minus the space tag: the sig-hash is already a fixed-alphabet
/// hex string, so no encoding is needed to keep key families apart.
const SM_PROXY_RECORDED_TABLE: TableDefinition<(&str, u16, &str), &str> =
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

/// Materialise a dataset blob's bytes at `<dir>/<digest>.csv` (RFC-005 D1, #285) — the file
/// [`RedbStateMachine::spool_path`] names for `digest`. Called from three places that must all
/// converge on the same on-disk truth: a live `DatasetPut` apply, a snapshot install, and
/// `reconcile_engine`'s repair pass.
///
/// Write-then-rename, never a direct write: a reader must never observe a partial file, and a
/// crash mid-write must never leave a corrupt one under the final name. `rename` within one
/// directory is atomic on every filesystem this crate targets, so `<digest>.csv` is always
/// either the complete previous contents or the complete new ones. A file already present at
/// the final path is left untouched and no temp file is even created — the bytes a digest names
/// never change (it is the sha256 of them), so a second write under the same digest is
/// redundant by construction, never a real change.
fn write_spool(dir: &Path, digest: &str, csv: &str) -> std::io::Result<()> {
    let final_path = dir.join(format!("{digest}.csv"));
    if final_path.exists() {
        return Ok(());
    }
    if !dir.exists() {
        std::fs::create_dir_all(dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // The directory itself is private to this node, not merely the files inside it —
            // set once, when this code creates it, so an operator's own mode on a pre-existing
            // directory is respected rather than reset on every write.
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    // A unique temp name per call: a live apply's put and `reconcile_engine`'s repair can both
    // materialise the same digest around the same time (a restart racing a fresh put of bytes
    // this node already lost), and two writers must never share one temp file.
    static SPOOL_TMP_SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SPOOL_TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp_path = dir.join(format!("{digest}.csv.tmp-{}-{seq}", std::process::id()));
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp_path)?
    };
    #[cfg(not(unix))]
    let mut file = std::fs::File::create(&tmp_path)?;
    {
        use std::io::Write;
        file.write_all(csv.as_bytes())?;
        file.sync_all()?;
    }
    // If the rename fails, the temp file is left behind rather than silently deleted — an
    // operator investigating a spool-directory anomaly should find the evidence, not a clean
    // directory that lies about what happened.
    std::fs::rename(&tmp_path, &final_path)?;
    // The bytes are durable (`sync_all` above); the directory entry that names them is not until
    // the directory itself is synced. Without this a crash right after the rename can leave a
    // committed row whose file is gone — the one shape the design says cannot occur (startup
    // repair would heal it, but "cannot occur" should not lean on the repair).
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

const SM_DEDUP_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_op_dedup");
const SM_APPLIED_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("sm_applied");
/// Node-local durable intents (issue #9 R4): ops this node accepted but has
/// not yet seen commit. NOT replicated state — never in snapshots, never
/// touched by apply; each node parks and replays only what it accepted.
const PENDING_INTENTS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("pending_intents");

/// How long audit rows are kept when nothing says otherwise: 30 days
/// (RFC-002 §9, issue #163). Long enough to answer "who changed this last
/// month", short enough that the table does not grow without bound on a busy
/// fleet. Overridden by `--cluster-audit-retention`.
pub const DEFAULT_AUDIT_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;

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
    /// Which spec this imposter was deployed from, when one was (RFC-004 S2, #278). Defaulted so
    /// a record written before specs existed still parses — it is simply not spec-bound, which
    /// is exactly what `None` means.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    spec: Option<SpecProvenance>,
    /// Whether a config-mutating write has touched this imposter since it was last bound (or
    /// re-bound) to `spec`. Meaningless — and always `false` — when `spec` is `None`: drift is a
    /// property of the relationship between a stored imposter and the spec it was deployed from,
    /// not of the imposter alone.
    #[serde(default, skip_serializing_if = "is_false")]
    drifted: bool,
}

/// `#[serde(skip_serializing_if)]` predicate for a plain `bool`: omit the field when it is at
/// its default, the same way `Option::is_none` does for an `Option`.
fn is_false(b: &bool) -> bool {
    !*b
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

/// One row of the fleet-wide source scan: its identity, and its value *or* the
/// reason the value would not decode (issue #243).
///
/// The split exists because the two halves fail independently. A redb key is a
/// `(tenant, id)` pair of strings and decodes even when the JSON value beside it
/// is garbage — so a corrupt row can still be *named*. That is what lets the
/// poll scheduler hold exactly that source's poller instead of either dropping
/// it silently (which stops a live poller and says nothing) or failing the whole
/// scan (which parks reconciliation for every tenant at once).
#[derive(Debug, Clone)]
pub struct SourceRow {
    pub tenant: String,
    pub id: String,
    /// `Err` carries the decode failure, for the caller to report.
    pub record: Result<SourceRecord, String>,
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

/// What `sm_specs` stores per `(tenant, id)` (RFC-004 S2, #278).
///
/// Carries no ports: which imposters are bound to this spec is derived, at read time, by
/// scanning `sm_configs` for a `StoredImposter::spec` naming this id — the same relationship
/// `sm_sources`' ports take from `sm_configs` provenance rather than storing their own copy, and
/// for the identical reason: two tables agreeing about one fact is a table that can disagree
/// with itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSpec {
    meta: SpecMeta,
    revision: u64,
}

/// One spec as reported by the read paths — the shape `GET /specs` and
/// `GET /specs/:id` render (RFC-004 S2, #278).
///
/// A crate-owned type rather than the stored record itself, for the same reason [`SourceRecord`]
/// is: the stored shape is free to gain fields the operator surface should not render, and this
/// crosses the public API boundary where `openraft` types may not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpecRecord {
    pub id: String,
    pub format: SpecFormat,
    pub digest: String,
    pub source: SpecSource,
    /// Every port, ascending, whose `StoredImposter::spec` names this id.
    pub ports: Vec<u16>,
    /// Whether any bound port has drifted from this spec since its last deploy.
    pub drifted: bool,
    /// The log index that last wrote this spec record. Unaffected by binding or unbinding a
    /// port — see [`ControlOp::SpecBind`]'s doc — so it changes only when the document itself
    /// (or its metadata) is rewritten.
    pub revision: u64,
}

/// One port's spec provenance and drift state (RFC-004 S2, #278) — what the front reads before
/// deciding whether a config write needs edit-time spec warnings. Narrower than [`SpecRecord`]
/// because a binding answers "what is this port deployed from", not "what does the spec look like".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpecBinding {
    pub spec_id: String,
    pub digest: String,
    pub drifted: bool,
}

/// What `sm_datasets` stores per `(tenant, name, version)` (RFC-005 D1, #285).
///
/// Carries the declared [`DatasetRecord`] verbatim — `validate` already proved it true of the
/// blob it names — plus the bookkeeping apply itself owns: `version` mirrors the key's own
/// third component (kept alongside it so a reader never has to destructure the key to get it),
/// `deleted` is this version's tombstone bit (RFC-005 §3.2: a delete leaves every version's row
/// behind, marked, the same reason [`Tenant::deleted`] does), and `revision` is the log index
/// that last wrote *this* row — put or delete, whichever happened most recently.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredDataset {
    record: DatasetRecord,
    version: u64,
    created_at_secs: u64,
    revision: u64,
    deleted: bool,
}

/// One dataset version as reported by the read paths — the shape `GET /datasets` and
/// `GET /datasets/:name` render (RFC-005 D1, #285).
///
/// A crate-owned type rather than the stored record itself, for the same reason [`SpecRecord`]
/// is: the stored shape is free to gain fields the operator surface should not render.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetSummary {
    pub name: String,
    pub version: u64,
    pub digest: String,
    pub key_columns: Vec<String>,
    pub delimiter: char,
    pub columns: Vec<String>,
    pub rows: u64,
    pub bytes: u64,
    pub created_at_secs: u64,
    pub revision: u64,
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
    /// `(tenant, revision)` rows of `sm_routes_revision` (issue #210).
    ///
    /// Defaulted for the same reason `routes` is, and the failure it prevents
    /// is the one #210 exists to close: a node that installs a snapshot without
    /// these rows reads every tenant's table as revision 0, so a client holding
    /// a real token would be *refused* until the next write re-stamps it.
    /// Annoying, and deliberately the safe direction — a pre-#210 snapshot
    /// carries no revisions at all, and 0 fails every stale precondition rather
    /// than passing one. The opposite default (inherit the last applied index)
    /// would let a token minted before the join silently pass here.
    #[serde(default)]
    routes_revisions: Vec<(String, u64)>,
    /// `(tenant, source id, stored-source JSON)` rows of `sm_sources`. Defaulted
    /// for the same reason `routes` is: a snapshot built before issue #134 still
    /// installs cleanly, carrying no sources — the same as a fleet that declared
    /// none.
    #[serde(default)]
    sources: Vec<(String, String, String)>,
    /// `(tenant, spec id, stored-spec JSON)` rows of `sm_specs` (RFC-004 S2, #278). Defaulted for
    /// the #134/#137 reason every table above carries it: a snapshot built before specs existed
    /// must still install, and the empty vec it decodes to means exactly what a pre-#278 fleet's
    /// history actually is — no spec has ever been written.
    #[serde(default)]
    specs: Vec<(String, String, String)>,
    /// `(digest hex, document text)` rows of `sm_spec_blobs` (#278). Travels with `specs` rather
    /// than being reconstructed from them: a joining node must hold the exact bytes a bound
    /// spec's digest names, and the blob table's key order carries no tenant to derive it from
    /// alone if a row here were ever missing.
    #[serde(default)]
    spec_blobs: Vec<(String, String)>,
    /// `(tenant, dataset name, version, stored-dataset JSON)` rows of `sm_datasets` (RFC-005 D1,
    /// #285). Defaulted for the #134/#137 reason every table above carries it: a snapshot built
    /// before datasets existed must still install, and the empty vec it decodes to means exactly
    /// what a pre-#285 fleet's history actually is — no dataset has ever been written.
    #[serde(default)]
    datasets: Vec<(String, String, u64, String)>,
    /// `(digest hex, csv text)` rows of `sm_dataset_blobs` (#285). Travels with `datasets`, and
    /// for the identical reason `spec_blobs` travels with `specs`: a joining node must hold the
    /// exact bytes a live dataset's digest names, and installing them here is also what
    /// materialises every blob's spool file (see `install_snapshot`) — a node that joined by
    /// snapshot must be able to serve a lookup against this dataset immediately, not after a
    /// separate catch-up step.
    #[serde(default)]
    dataset_blobs: Vec<(String, String)>,
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
    /// `(revision, op_id, AuditRow JSON)` rows of `sm_audit` (issue #163).
    ///
    /// Defaulted for the same reason `routes`/`sources`/`tenants` are, and the
    /// reason is worth restating because this crate has already been bitten by
    /// it once (#134/#137): **a table omitted from this payload is a table that
    /// vanishes the next time a follower catches up by snapshot.** For an audit
    /// stream that failure is silent and permanent — the node comes back with
    /// an empty history and nothing reports a gap.
    #[serde(default)]
    audit: Vec<(u64, String, String)>,
    /// The `sm_audit_sink` row, if one is declared (issue #164). Same
    /// `#[serde(default)]` reasoning as `audit` above — and the same failure if
    /// it is forgotten: a node catching up by snapshot would come back with no
    /// sink, stop exporting the moment it won an election, and report nothing.
    #[serde(default)]
    audit_sink: Option<String>,
    /// The `sm_audit_checkpoint` row (issue #164). A node that installs a
    /// snapshot without it and then wins an election resumes from revision 0
    /// and re-ships the entire retained history to the customer's bucket.
    #[serde(default)]
    audit_checkpoint: Option<u64>,
    /// The `sm_audit_gc_watermark` row (issue #164). A node that installs a
    /// snapshot without it forgets that retention ever deleted anything, and
    /// its exporter then reports a clean stream over a window that is gone.
    #[serde(default)]
    audit_gc_watermark: Option<u64>,
    /// The `sm_session_key` row, if a console login has ever minted one (RFC-006 §5.3, issue
    /// #185). `#[serde(default)]` for the same reason `audit_sink` is, and the same failure
    /// shape if it is ever forgotten here: a node that installs a snapshot without it and then
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
    /// `(tenant, port, space, generation)` rows of `sm_journal_gens` (issue #224).
    /// `#[serde(default)]` for the #134/#137 reason every table above carries it: a snapshot
    /// built before this field existed must still install, and the empty vec it decodes to means
    /// exactly what an upgrading fleet's history actually is — no clear has ever committed. The
    /// sharper failure than most of this table's siblings if it were ever forgotten here: a node
    /// that joins by snapshot and reads every generation as `0` would silently resurrect entries
    /// its peers have already agreed are cleared, the very inversion issue #224 exists to close.
    #[serde(default)]
    journal_gens: Vec<(String, u16, Option<String>, u64)>,
    /// `(tenant, port, sig-hash, recorded-response JSON)` rows of `sm_proxy_recorded` (#226).
    /// `#[serde(default)]` for the #134/#137 reason every table above carries it. The failure
    /// if it were forgotten: a node that joins by snapshot answers `Claimed` for signatures
    /// the fleet already recorded, and the engine calls the real upstream a second time — the
    /// exact duplicate `proxyOnce` exists to prevent.
    #[serde(default)]
    proxy_recorded: Vec<(String, u16, String, String)>,
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
    // node's data directory — the same place a dataset's spool file already goes. Taken before
    // `Database::create` consumes `path`.
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
        write_txn.open_table(SM_SOURCES_TABLE).map_err(io)?;
        write_txn.open_table(SM_SPECS_TABLE).map_err(io)?;
        write_txn.open_table(SM_SPEC_BLOBS_TABLE).map_err(io)?;
        write_txn.open_table(SM_DATASETS_TABLE).map_err(io)?;
        write_txn.open_table(SM_DATASET_BLOBS_TABLE).map_err(io)?;
        write_txn.open_table(SM_TENANTS_TABLE).map_err(io)?;
        write_txn.open_table(SM_PRINCIPALS_TABLE).map_err(io)?;
        write_txn.open_table(SM_BINDINGS_TABLE).map_err(io)?;
        write_txn.open_table(SM_AUDIT_TABLE).map_err(io)?;
        write_txn.open_table(SM_AUDIT_SINK_TABLE).map_err(io)?;
        write_txn
            .open_table(SM_AUDIT_CHECKPOINT_TABLE)
            .map_err(io)?;
        write_txn
            .open_table(SM_AUDIT_GC_WATERMARK_TABLE)
            .map_err(io)?;
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
    /// Remove a dataset blob's spool file after the txn that GC'd its `sm_dataset_blobs` row has
    /// committed (RFC-005 D1, #285). Needs no engine — `drive_one` handles it with only the
    /// node's own spool directory — and always runs after the row is already gone durably: a
    /// crash between commit and this removal leaves a harmless orphan file behind (repair only
    /// ever *adds* a missing file, never removes one, so the orphan simply persists until
    /// noticed). The alternative ordering — deleting the file before the row commits — could
    /// delete bytes a concurrent read still names, which is the worse failure of the two.
    UnspoolDataset {
        digest: String,
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
/// Without this, every clustered change event reaches M3's SSE and the #164
/// export sink with `EventContext::principal == None`, and the fact that the
/// audit table is correctly attributed does not help them: they are on the event
/// path, not the log path.
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
    /// The front door's hot-swappable compiled table (issue #131). `None` in
    /// storage tests and on a node that never binds a front door — routes are
    /// still replicated and readable from `sm_routes` either way, this is only
    /// the dispatch-side handle. Attach before `Raft::new` for the same reason
    /// as `engine`: replay during join must drive it too, not just live
    /// commits.
    routes: Option<Arc<ArcSwap<CompiledRoutes>>>,
    /// Where snapshot payload files live: `<redb's parent>/snapshot/` (#436, the D-16 amendment).
    ///
    /// Not an `Option` and not a builder, unlike `spool_dir`: a snapshot has nowhere else it could
    /// go, so "no directory" is not a representable state. Derived from the path `new` already
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
    /// How long audit rows are kept, in seconds; `0` = forever (issue #163).
    ///
    /// **Every node in a fleet must be configured identically.** This value
    /// feeds `gc_audit`, which runs inside `apply` — so two nodes with
    /// different retention would drop different rows from the same log and
    /// their audit tables would diverge, which is exactly the property the
    /// replicated clock exists to protect. It is node configuration rather than
    /// replicated state because it is an operator's storage-budget decision,
    /// not a tenant's; `docs/rift-cluster-server.md` says so where the flag is
    /// documented.
    audit_retention_secs: u64,
    /// Where a dataset's csv bytes are materialised on this node's local disk, one file per
    /// digest (RFC-005 D1, #285) — `None` in storage tests and on an embedder that never wires
    /// one, exactly like `engine`. Node-local derived state, not replicated: every replica keeps
    /// its own copy of the file under its own data directory, the same way every replica keeps
    /// its own `redb` file for the tables the files sit beside.
    spool_dir: Option<PathBuf>,
}

impl std::fmt::Debug for RedbStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbStateMachine")
            .field("engine", &self.engine.is_some())
            .field("routes", &self.routes.is_some())
            .field("journal", &self.journal.get().is_some())
            .field("snapshot_dir", &self.snapshot_dir)
            .field("spool_dir", &self.spool_dir)
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
            routes: None,
            apply_failures: Arc::new(Mutex::new(BTreeMap::new())),
            journal: OnceLock::new(),
            audit_retention_secs: DEFAULT_AUDIT_RETENTION_SECS,
            spool_dir: None,
        }
    }

    /// Set the audit retention window. Same before-`Raft::new` contract as
    /// [`Self::with_engine`] — and see `audit_retention_secs`' doc for why every
    /// node in a fleet must be given the same value.
    #[must_use]
    pub fn with_audit_retention_secs(mut self, secs: u64) -> Self {
        self.audit_retention_secs = secs;
        self
    }

    /// Attach the directory dataset blobs are materialised into (RFC-005 D1, #285). Same
    /// before-`Raft::new` contract as [`Self::with_engine`] — apply writes a dataset's spool
    /// file inside the same transaction that commits its row, so every handle must agree on
    /// where that file goes from the very first entry it applies.
    #[must_use]
    pub fn with_spool_dir(mut self, dir: PathBuf) -> Self {
        self.spool_dir = Some(dir);
        self
    }

    /// The path a dataset blob's csv bytes are (or would be) materialised at, or `None` when
    /// this node has no spool directory attached (RFC-005 D1, #285). Answers regardless of
    /// whether `digest` names anything this node currently holds — a caller with a
    /// [`DatasetSummary`] already knows that; this is purely the path function.
    #[must_use]
    pub fn spool_path(&self, digest: &str) -> Option<PathBuf> {
        self.spool_dir
            .as_ref()
            .map(|dir| dir.join(format!("{digest}.csv")))
    }

    /// Whether the committed `sm_dataset_blobs` currently holds `digest` — read fresh, for the
    /// post-commit unspool to decide against (see `EngineAction::UnspoolDataset`).
    #[allow(clippy::result_large_err)]
    fn dataset_blob_present(&self, digest: &str) -> StorageResult<bool> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let blobs = read_txn
            .open_table(SM_DATASET_BLOBS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(blobs
            .get(digest)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .is_some())
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
            // 0o600, matching `write_spool`: a snapshot payload carries `session_key` and every
            // `principals` row, so it is strictly more sensitive than the CSVs that mode was
            // chosen for.
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

    /// Read the applied config JSON for `tenant`'s `port`, or `None` if no
    /// config has been applied for it.
    ///
    /// This is the node's read path: reads answer from the applied state machine
    /// directly and never go through Raft, so a follower or a restarted node can
    /// serve committed config without waiting to become leader. Openraft owns the
    /// state machine as `&mut self`, so the node keeps a cheap `Clone` of this
    /// handle (both share one `Arc<Database>`) purely for reads.
    #[allow(clippy::result_large_err)]
    pub fn read_config(&self, tenant: &str, port: u16) -> StorageResult<Option<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        table
            .get((tenant, port))
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|g| {
                serde_json::from_str::<StoredImposter>(g.value())
                    .map(|stored| stored.config_json)
                    .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))
            })
            .transpose()
    }

    /// `(tenant, port)` for every port fleet-wide that currently has an applied
    /// config, ascending — `redb` iterates `sm_configs` key-ordered
    /// `(tenant, port)`, so this is tenant-major then port-ascending within
    /// each tenant.
    ///
    /// Fleet-wide and not tenant-scoped on purpose: this backs the operator
    /// surface `GET /_cluster/config`, which reports what the whole node has
    /// converged on, not one tenant's view of it. Ports rather than bodies:
    /// the operator endpoints report *what* the node has converged on, and a
    /// fleet's full config set is far larger than the answer to that question.
    #[allow(clippy::result_large_err)]
    pub fn configured_ports(&self) -> StorageResult<Vec<(TenantId, u16)>> {
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
            ports.push((TenantId::new(tenant), port));
        }
        Ok(ports)
    }

    /// `tenant`'s route table, as currently applied. Like [`Self::read_config`],
    /// this is the node's own read path — it answers from local durable state
    /// without a Raft round trip. It is also the *only* read path for routes:
    /// upstream has no `GET /front-door/routes` to proxy to (U-11's admin CRUD
    /// was deferred), so `GET /front-door/routes` in the clustered admin front
    /// calls straight through to this.
    #[allow(clippy::result_large_err)]
    pub fn route_table(&self, tenant: &str) -> StorageResult<RouteTable> {
        Ok(self.route_table_with_revision(tenant)?.0)
    }

    /// `tenant`'s route table and the revision it is at, read in **one** redb
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
    pub fn route_table_with_revision(&self, tenant: &str) -> StorageResult<(RouteTable, u64)> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let revision = read_txn
            .open_table(SM_ROUTES_REVISION_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .get(tenant)
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
            let (row_tenant, id) = key.value();
            if row_tenant != tenant {
                continue;
            }
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
    /// holds no record for `(tenant, port)`.
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
    /// with no token merely leaves that imposter unconditionable — the same
    /// posture `stored_imposter` takes for provenance.
    #[allow(clippy::result_large_err)]
    pub fn imposter_revision(&self, tenant: &str, port: u16) -> StorageResult<Option<u64>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let Some(guard) = table
            .get((tenant, port))
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        else {
            return Ok(None);
        };
        match serde_json::from_str::<StoredImposter>(guard.value()) {
            Ok(stored) => Ok(Some(stored.revision)),
            Err(e) => {
                tracing::error!(tenant, port, error = %e, "corrupt stored imposter; read carries no revision token");
                Ok(None)
            }
        }
    }

    /// Every declared source for `tenant`, id-ascending (issue #134).
    ///
    /// Like [`Self::read_config`], this answers from local applied state with no
    /// Raft round trip, so comparing two nodes' answers is what tells an
    /// operator whether a `SourcePut` has converged.
    #[allow(clippy::result_large_err)]
    pub fn sources(&self, tenant: &str) -> StorageResult<Vec<SourceRecord>> {
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
        let owned = Self::ports_by_source(&configs, tenant)
            .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?;

        let mut records = Vec::new();
        for item in sources
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (row_tenant, id) = key.value();
            if row_tenant != tenant {
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

    /// Every declared source in the fleet paired with the tenant that owns it,
    /// `(tenant, id)`-ascending — the table's own key order (issue #241).
    ///
    /// One scan, rather than [`Self::sources`] looped over a tenant list. A
    /// tenant list is the wrong driver twice over: it carries tombstones
    /// ([`Tenant::deleted`]) and omits the always-present implicit default
    /// tenant, and each per-tenant call re-scans the whole table anyway.
    ///
    /// This is what the poll scheduler reconciles against, which is also what
    /// makes `TenantDelete` correct for free: the cascade drops that tenant's
    /// `sm_sources` rows in the same committed op, so they simply stop
    /// appearing here and the next reconcile stops their pollers.
    ///
    /// **A row that will not decode is reported in band** as
    /// [`SourceRow::record`] `= Err`, not skipped and not fatal to the scan
    /// (issue #243). The two rejected alternatives are why: skipping shrinks
    /// the list, which stops a live source's poller and says nothing; failing
    /// the call parks reconciliation for *every* tenant over one tenant's bad
    /// row. [`Self::sources`] and [`Self::source`] keep the strict behaviour —
    /// they answer a tenant about its own state, where a loud error is exactly
    /// right, and this method's caller is a fleet-wide control loop instead.
    ///
    /// Table and transaction failures still fail the whole call. Those are
    /// transient and say nothing about any individual row, so the scheduler's
    /// keep-what-is-running-and-retry response remains the correct one.
    #[allow(clippy::result_large_err)]
    pub fn sources_all(&self) -> StorageResult<Vec<SourceRow>> {
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
        let owned = Self::ports_by_source_all(&configs)
            .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?;

        let mut rows = Vec::new();
        for item in sources
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (tenant, id) = key.value();
            // Not logged here. This runs on every reconcile tick (1 Hz), and a
            // corrupt row stays corrupt until someone rewrites it, so logging
            // per read would emit the same line forever. The detail travels to
            // the scheduler, which logs the transition instead.
            let record = serde_json::from_str::<StoredSource>(value.value())
                .map(|stored| {
                    let ports = owned.get(tenant).and_then(|by_source| by_source.get(id));
                    Self::render_source(id, &stored, ports)
                })
                .map_err(|e| e.to_string());
            rows.push(SourceRow {
                tenant: tenant.to_owned(),
                id: id.to_owned(),
                record,
            });
        }
        Ok(rows)
    }

    /// One source by id, or `None` if `tenant` has no such source.
    #[allow(clippy::result_large_err)]
    pub fn source(&self, tenant: &str, id: &str) -> StorageResult<Option<SourceRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let sources = read_txn
            .open_table(SM_SOURCES_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let Some(guard) = sources
            .get((tenant, id))
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
        let owned = Self::ports_by_source(&configs, tenant)
            .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?;
        Ok(Some(Self::render_source(id, &stored, owned.get(id))))
    }

    /// Every declared spec for `tenant`, id-ascending (RFC-004 S2, #278).
    ///
    /// Like [`Self::sources`], this answers from local applied state with no Raft round trip.
    #[allow(clippy::result_large_err)]
    pub fn specs(&self, tenant: &str) -> StorageResult<Vec<SpecRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let specs = read_txn
            .open_table(SM_SPECS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let configs = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let owned = Self::ports_by_spec(&configs, tenant)
            .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?;

        let mut records = Vec::new();
        for item in specs
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (row_tenant, id) = key.value();
            if row_tenant != tenant {
                continue;
            }
            // A spec row that will not parse is committed-state corruption. Reported as an
            // error rather than skipped, for the same reason `sources` reports one loudly: a
            // silently shrinking list would tell an operator their spec is gone when it is not.
            let stored: StoredSpec = serde_json::from_str(value.value()).map_err(|e| {
                tracing::error!(spec_id = %id, error = %e, "corrupt stored spec");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            records.push(Self::render_spec(id, &stored, owned.get(id)));
        }
        Ok(records)
    }

    /// One spec by id, or `None` if `tenant` has no such spec (RFC-004 S2, #278).
    #[allow(clippy::result_large_err)]
    pub fn spec(&self, tenant: &str, id: &str) -> StorageResult<Option<SpecRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let specs = read_txn
            .open_table(SM_SPECS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let Some(guard) = specs
            .get((tenant, id))
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        else {
            return Ok(None);
        };
        let stored: StoredSpec = serde_json::from_str(guard.value()).map_err(|e| {
            tracing::error!(spec_id = %id, error = %e, "corrupt stored spec");
            StorageError::from(StorageIOError::read_state_machine(&e))
        })?;
        let configs = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let owned = Self::ports_by_spec(&configs, tenant)
            .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?;
        Ok(Some(Self::render_spec(id, &stored, owned.get(id))))
    }

    /// The document stored under `digest`, or `None` if no spec currently holds it (RFC-004 S2,
    /// #278). Addressed by digest, not by `(tenant, id)`: this is the blob table's own key, and
    /// the same document answers for every spec that shares its bytes.
    #[allow(clippy::result_large_err)]
    pub fn spec_document(&self, digest: &str) -> StorageResult<Option<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let blobs = read_txn
            .open_table(SM_SPEC_BLOBS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(blobs
            .get(digest)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|g| g.value().to_owned()))
    }

    /// `tenant`'s port `port`'s spec provenance, or `None` when the port holds no imposter or
    /// the imposter it holds is not spec-bound (RFC-004 S2, #278).
    ///
    /// A stored imposter that will not parse answers `None`, like [`Self::imposter_revision`]
    /// does for the same corruption: the read itself still succeeds, and this merely reports the
    /// port as unbound rather than failing the whole read.
    #[allow(clippy::result_large_err)]
    pub fn spec_binding(&self, tenant: &str, port: u16) -> StorageResult<Option<SpecBinding>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let configs = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let Some(guard) = configs
            .get((tenant, port))
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        else {
            return Ok(None);
        };
        let stored: StoredImposter = match serde_json::from_str(guard.value()) {
            Ok(stored) => stored,
            Err(e) => {
                tracing::error!(
                    tenant, port, error = %e,
                    "corrupt stored imposter; read carries no spec binding"
                );
                return Ok(None);
            }
        };
        Ok(stored.spec.map(|spec| SpecBinding {
            spec_id: spec.spec_id,
            digest: spec.digest.as_str().to_owned(),
            drifted: stored.drifted,
        }))
    }

    /// How many distinct spec documents are currently held, fleet-wide (RFC-004 S2, #278) — the
    /// blob table's row count, so a caller can assert on GC (a delete or a digest swap freeing
    /// the last reference) without knowing which digests exist.
    #[allow(clippy::result_large_err)]
    pub fn spec_blob_count(&self) -> StorageResult<usize> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let blobs = read_txn
            .open_table(SM_SPEC_BLOBS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let len = blobs
            .len()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(usize::try_from(len).unwrap_or(usize::MAX))
    }

    /// Every live dataset version `tenant` holds, name-ascending then version-ascending
    /// (RFC-005 D1, #285) — `sm_datasets`' own key order, so this is a single ordered scan
    /// with no sort step. Like [`Self::specs`], this answers from local applied state with no
    /// Raft round trip.
    ///
    /// A row that will not parse is committed-state corruption, reported as an error rather
    /// than skipped — the same reason [`Self::specs`] does: a silently shrinking list would
    /// tell an operator their dataset is gone when it is not.
    #[allow(clippy::result_large_err)]
    pub fn datasets(&self, tenant: &str) -> StorageResult<Vec<DatasetSummary>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let datasets = read_txn
            .open_table(SM_DATASETS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut out = Vec::new();
        for item in datasets
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (row_tenant, name, version) = key.value();
            if row_tenant != tenant {
                continue;
            }
            let stored: StoredDataset = serde_json::from_str(value.value()).map_err(|e| {
                tracing::error!(tenant, name, version, error = %e, "corrupt stored dataset");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            if stored.deleted {
                continue;
            }
            out.push(Self::render_dataset(name, version, &stored));
        }
        Ok(out)
    }

    /// The latest live version of `tenant`'s dataset `name`, or `None` when `tenant` holds no
    /// live version of it (RFC-005 D1, #285).
    #[allow(clippy::result_large_err)]
    pub fn dataset(&self, tenant: &str, name: &str) -> StorageResult<Option<DatasetSummary>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let datasets = read_txn
            .open_table(SM_DATASETS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut latest: Option<(u64, StoredDataset)> = None;
        for item in datasets
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (row_tenant, row_name, version) = key.value();
            if row_tenant != tenant || row_name != name {
                continue;
            }
            let stored: StoredDataset = serde_json::from_str(value.value()).map_err(|e| {
                tracing::error!(tenant, name, version, error = %e, "corrupt stored dataset");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            if stored.deleted {
                continue;
            }
            if latest.as_ref().is_none_or(|(v, _)| version > *v) {
                latest = Some((version, stored));
            }
        }
        Ok(latest.map(|(version, stored)| Self::render_dataset(name, version, &stored)))
    }

    /// The CSV bytes behind `digest`, or `None` when this node holds no such blob.
    ///
    /// Answers from the replicated blob table rather than the node-local spool file: the spool is
    /// a materialisation for the engine to read, and a node that has lost its `datasets/`
    /// directory still owes an honest answer here (`reconcile_engine` rebuilds the files from
    /// exactly this table).
    #[allow(clippy::result_large_err)]
    pub fn dataset_blob(&self, digest: &str) -> StorageResult<Option<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let blobs = read_txn
            .open_table(SM_DATASET_BLOBS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(blobs
            .get(digest)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|guard| guard.value().to_owned()))
    }

    /// How many live stubs bind each of `tenant`'s datasets, by dataset name, plus whether the
    /// tally is **complete**.
    ///
    /// **One** scan of the config table, tallying every name at once. The listing route needs a
    /// count per dataset, and a scan per dataset would make rendering one page
    /// O(datasets x configs) — the same reason `desired_configs` builds its whole set from a
    /// single pass. A name absent from the map has no bindings.
    ///
    /// A config that will not parse is skipped and the tally is reported **incomplete**, matching
    /// how per-tenant usage handles the same corruption (`TenantConfigUsage::incomplete`) rather
    /// than how `desired_configs` does. The difference is what the number is *for*: the engine's
    /// desired set drives teardown, so a short one is destructive and must abort; this count is
    /// advisory. But it must not be quietly short either — it is what tells an operator whether a
    /// delete will be refused, so under-reporting it would promise a delete that then 409s. The
    /// caller renders the flag as `Rift-Cluster-Partial`.
    #[allow(clippy::result_large_err)]
    pub fn dataset_binding_counts(
        &self,
        tenant: &str,
    ) -> StorageResult<(std::collections::HashMap<String, usize>, bool)> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let configs = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut incomplete = false;
        for item in configs
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (row_tenant, _) = key.value();
            if row_tenant != tenant {
                continue;
            }
            // Read as generic JSON, like the delete-while-bound check: a stored config that will
            // not parse must not make a *binding* invisible, which is what silently skipping it
            // would do — and this count is what tells an operator a delete will be refused.
            let stored: StoredImposter = match serde_json::from_str(value.value()) {
                Ok(stored) => stored,
                Err(e) => {
                    tracing::error!(tenant, error = %e, "corrupt stored imposter; binding count incomplete");
                    incomplete = true;
                    continue;
                }
            };
            let config: serde_json::Value = match serde_json::from_str(&stored.config_json) {
                Ok(config) => config,
                Err(e) => {
                    tracing::error!(tenant, error = %e, "corrupt stored config; binding count incomplete");
                    incomplete = true;
                    continue;
                }
            };
            for name in crate::datasets::bound_dataset_names(&config) {
                *counts.entry(name).or_default() += 1;
            }
        }
        Ok((counts, incomplete))
    }

    /// How many distinct dataset documents are currently held, fleet-wide (RFC-005 D1, #285) —
    /// the blob table's row count, the dataset-table counterpart of [`Self::spec_blob_count`].
    #[allow(clippy::result_large_err)]
    pub fn dataset_blob_count(&self) -> StorageResult<usize> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let blobs = read_txn
            .open_table(SM_DATASET_BLOBS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let len = blobs
            .len()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(usize::try_from(len).unwrap_or(usize::MAX))
    }

    fn render_dataset(name: &str, version: u64, stored: &StoredDataset) -> DatasetSummary {
        DatasetSummary {
            name: name.to_owned(),
            version,
            digest: stored.record.digest.as_str().to_owned(),
            key_columns: stored.record.key_columns.clone(),
            delimiter: stored.record.delimiter,
            columns: stored.record.columns.clone(),
            rows: stored.record.rows,
            bytes: stored.record.bytes,
            created_at_secs: stored.created_at_secs,
            revision: stored.revision,
        }
    }

    /// The tenant that owns `port`'s applied config, or `None` if no tenant
    /// has one.
    ///
    /// **Not O(1).** `sm_configs` is keyed `(tenant, port)` — tenant-major —
    /// so there is no index that seeks directly to a port; this is a full
    /// scan of every applied config fleet-wide, `O(configured ports)`. Ports
    /// are fleet-unique (RFC-002 §3.2, enforced by
    /// [`Self::port_claimed_by_another_tenant`]), so at most one row can ever
    /// match — this stops at the first hit rather than scanning to confirm
    /// uniqueness.
    #[allow(clippy::result_large_err)]
    pub fn owning_tenant(&self, port: u16) -> StorageResult<Option<TenantId>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        for item in table
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, _) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (tenant, row_port) = key.value();
            if row_port == port {
                return Ok(Some(TenantId::new(tenant)));
            }
        }
        Ok(None)
    }

    /// The principal record for `id`, or `None` if no such principal exists
    /// (issue #161). Like [`Self::read_config`], this answers from local
    /// applied state with no Raft round trip — authenticating a request must
    /// not require this node to be leader.
    #[allow(clippy::result_large_err)]
    pub fn principal(&self, id: &str) -> StorageResult<Option<Principal>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_PRINCIPALS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        table
            .get(id)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|g| {
                serde_json::from_str::<Principal>(g.value()).map_err(|e| {
                    tracing::error!(principal_id = %id, error = %e, "corrupt stored principal");
                    StorageError::from(StorageIOError::read_state_machine(&e))
                })
            })
            .transpose()
    }

    /// Every tenant `id` is bound in, with the role for each (RFC-002 §4,
    /// issue #161) — the whole-of-request read: authenticate, then load
    /// *this* principal's bindings, then intersect with what was requested.
    ///
    /// `sm_bindings` is keyed principal-major exactly so this can be a single
    /// seek (see the `TableDefinition`'s doc); this reads the whole table and
    /// filters instead, matching [`Self::sources`]'s tenant-filtered scan in
    /// this same file. A fleet's principal/binding count is nowhere near what
    /// would make that choice matter — simplicity over the seek this key
    /// order enables but does not require.
    #[allow(clippy::result_large_err)]
    pub fn principal_bindings(&self, id: &str) -> StorageResult<Vec<(TenantId, Role)>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_BINDINGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut bindings = Vec::new();
        for item in table
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (principal_id, tenant) = key.value();
            if principal_id != id {
                continue;
            }
            // A row that will not parse is committed-state corruption, not an
            // absent binding: reported as an error rather than skipped, or a
            // principal with a broken row would silently lose access instead
            // of the operator learning their state is corrupt.
            let role: Role = serde_json::from_str(value.value()).map_err(|e| {
                tracing::error!(principal_id = %id, tenant, error = %e, "corrupt stored binding");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            bindings.push((TenantId::new(tenant), role));
        }
        Ok(bindings)
    }

    /// One tenant record by id, or `None` when no row exists (issue #162).
    ///
    /// Tombstones are returned rather than hidden: `GET /admin/tenants/:id`
    /// reporting `deleted: true` is how an operator learns an id is spent
    /// rather than free, and hiding it here would make a deleted tenant
    /// indistinguishable from one that never existed on the one surface whose
    /// job is to tell them apart.
    #[allow(clippy::result_large_err)]
    pub fn tenant(&self, id: &str) -> StorageResult<Option<Tenant>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_TENANTS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        table
            .get(id)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|g| {
                serde_json::from_str::<Tenant>(g.value()).map_err(|e| {
                    tracing::error!(tenant = %id, error = %e, "corrupt stored tenant");
                    StorageError::from(StorageIOError::read_state_machine(&e))
                })
            })
            .transpose()
    }

    /// Every tenant record, id-ascending, tombstones included (issue #162) —
    /// what `GET /admin/tenants` reports. Like [`Self::principal`], this
    /// answers from local applied state and needs no leadership.
    #[allow(clippy::result_large_err)]
    pub fn tenants(&self) -> StorageResult<Vec<Tenant>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_TENANTS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut out = Vec::new();
        for item in table
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            // Corruption is an error, not a skipped row, for the same reason
            // `principal_bindings` gives: silently omitting a tenant from the
            // listing is how an operator concludes it was already deleted.
            let tenant: Tenant = serde_json::from_str(value.value()).map_err(|e| {
                tracing::error!(tenant = key.value(), error = %e, "corrupt stored tenant");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            out.push(tenant);
        }
        Ok(out)
    }

    /// Every tenant's config-table usage (issue #372), in **one** scan of
    /// `sm_configs` — not one scan per tenant. `GET /admin/tenants` needs every
    /// listed tenant's usage in the same response, and a per-tenant scan there
    /// would turn a page of N tenants into N full-table scans (the listing's
    /// AC7); this builds the whole map from a single pass instead, the same way
    /// [`Self::desired_configs`] builds its whole engine-desired set from one.
    ///
    /// A row that will not parse is skipped for this tenant's usage rather than
    /// aborting the whole map: unlike [`Self::desired_configs`] (which drives
    /// the engine and must not silently shrink what it tears down), a usage
    /// figure is advisory, and one corrupt imposter must not blank out every
    /// other tenant's numbers. It is still loud — logged at `error` — so the
    /// corruption itself is not silently lost, and [`TenantConfigUsage::incomplete`]
    /// carries the fact forward into the response as `Rift-Cluster-Partial`
    /// rather than letting the skip quietly shrink `imposters`/`max_stubs`
    /// with nothing to say so. The skip has a second-order effect worth
    /// naming too: a skipped row's port never reaches `ports`, so that
    /// imposter's flow entries also vanish from the flow-entry fan-out
    /// (`FlowNet::fleet_entry_counts` is only ever asked about ports this map
    /// reports) — one corrupt row understates two figures, not one.
    #[allow(clippy::result_large_err)]
    pub fn tenant_config_usage(&self) -> StorageResult<HashMap<String, TenantConfigUsage>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut usage: HashMap<String, TenantConfigUsage> = HashMap::new();
        for item in table
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (tenant, port) = key.value();
            let stored: StoredImposter = match serde_json::from_str(value.value()) {
                Ok(stored) => stored,
                Err(e) => {
                    tracing::error!(tenant, port, error = %e, "corrupt stored imposter; excluded from usage");
                    usage.entry(tenant.to_owned()).or_default().incomplete = true;
                    continue;
                }
            };
            let config: ImposterConfig = match serde_json::from_str(&stored.config_json) {
                Ok(config) => config,
                Err(e) => {
                    tracing::error!(tenant, port, error = %e, "corrupt stored config; excluded from usage");
                    usage.entry(tenant.to_owned()).or_default().incomplete = true;
                    continue;
                }
            };
            let entry = usage.entry(tenant.to_owned()).or_default();
            entry.imposters = entry.imposters.saturating_add(1);
            let stubs = u32::try_from(config.stubs.len()).unwrap_or(u32::MAX);
            entry.max_stubs = entry.max_stubs.max(stubs);
            entry.ports.push(port);
        }
        Ok(usage)
    }

    /// Every principal bound to `tenant`, with the role each holds there,
    /// principal-id-ascending (issue #162) — what
    /// `GET /admin/tenants/:id/principals` reports.
    ///
    /// This is the listing `sm_bindings`' principal-major key order pays for
    /// with a full scan (see its `TableDefinition`'s doc): the trade is
    /// deliberate, because the per-request direction — one principal's
    /// bindings — is the one that had to stay a single seek.
    ///
    /// A binding naming a principal with no `sm_principals` row is impossible
    /// through the ops (`PrincipalDelete` cascades its bindings away, and
    /// `PrincipalCreate` writes both rows in one revision), so one found here
    /// is committed-state corruption and is reported as an error rather than
    /// skipped — a silently-dropped row would hide exactly the inconsistency
    /// worth knowing about.
    #[allow(clippy::result_large_err)]
    pub fn tenant_principals(&self, tenant: &str) -> StorageResult<Vec<(Principal, Role)>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let bindings = read_txn
            .open_table(SM_BINDINGS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let principals = read_txn
            .open_table(SM_PRINCIPALS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut out = Vec::new();
        for item in bindings
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (principal_id, bound_tenant) = key.value();
            if bound_tenant != tenant {
                continue;
            }
            let role: Role = serde_json::from_str(value.value()).map_err(|e| {
                tracing::error!(principal_id, tenant, error = %e, "corrupt stored binding");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            let guard = principals
                .get(principal_id)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .ok_or_else(|| {
                    tracing::error!(principal_id, tenant, "binding names an absent principal");
                    StorageError::from(StorageIOError::read_state_machine(&std::io::Error::other(
                        format!("binding names principal {principal_id:?}, which has no record"),
                    )))
                })?;
            let principal: Principal = serde_json::from_str(guard.value()).map_err(|e| {
                tracing::error!(principal_id, error = %e, "corrupt stored principal");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            out.push((principal, role));
        }
        Ok(out)
    }

    /// Whether the fleet has any principal defined at all (RFC-002 §3.4).
    /// Governs the legacy-admin-plane bypass and the `rift_cluster_no_principals`
    /// gauge: presence is presence regardless of whether a given row happens
    /// to parse, so a corrupt row still counts — the wrong answer here is
    /// "false" (it would silently reopen the pre-#161 open admin plane on a
    /// fleet that in fact has principals).
    #[allow(clippy::result_large_err)]
    pub fn has_any_principals(&self) -> StorageResult<bool> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_PRINCIPALS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut iter = table
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(iter.next().is_some())
    }

    /// `(tenant, port, provenance)` for every source-owned config fleet-wide,
    /// tenant-major then port-ascending (the `sm_configs` key order) — what
    /// `GET /_cluster/config` reports so an operator can see which imposters a
    /// source owns and at which version.
    ///
    /// Fleet-wide and not tenant-scoped on purpose, like [`Self::configured_ports`]:
    /// this backs the same operator surface, not a tenant-facing one.
    #[allow(clippy::result_large_err)]
    pub fn config_provenance(&self) -> StorageResult<Vec<(TenantId, u16, SourceProvenance)>> {
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
            let stored: StoredImposter = serde_json::from_str(value.value())
                .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?;
            if let Some(provenance) = stored.source {
                owned.push((TenantId::new(tenant), port, provenance));
            }
        }
        Ok(owned)
    }

    /// Source id -> the ports it owns for `tenant`, from `sm_configs`
    /// provenance. Read from an open (possibly mid-transaction) view so both
    /// the read paths and apply can use it.
    fn ports_by_source(
        table: &impl ReadableTable<(&'static str, u16), &'static str>,
        tenant: &str,
    ) -> Result<BTreeMap<String, Vec<u16>>, redb::StorageError> {
        let mut owned: BTreeMap<String, Vec<u16>> = BTreeMap::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let (row_tenant, port) = key.value();
            if row_tenant != tenant {
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

    /// tenant -> source id -> the ports it owns, across every tenant — the
    /// all-tenants counterpart of [`Self::ports_by_source`], for the one reader
    /// that takes the whole source table at once.
    ///
    /// Nested rather than keyed by a `(tenant, id)` pair so the caller can look
    /// a row up by borrowing both halves: this runs on every reconcile tick, and
    /// a tuple key would mean allocating a throwaway `String` pair per source
    /// row just to probe the map.
    ///
    /// Unparsable rows are ignored for the same reason as there: as far as
    /// *provenance* goes such a record owns no port, and the config read paths
    /// are what report the corruption. Ignoring one can only under-report
    /// ownership, never delete anything.
    fn ports_by_source_all(
        table: &impl ReadableTable<(&'static str, u16), &'static str>,
    ) -> Result<BTreeMap<String, BTreeMap<String, Vec<u16>>>, redb::StorageError> {
        let mut owned: BTreeMap<String, BTreeMap<String, Vec<u16>>> = BTreeMap::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let (tenant, port) = key.value();
            if let Ok(stored) = serde_json::from_str::<StoredImposter>(value.value())
                && let Some(provenance) = stored.source
            {
                owned
                    .entry(tenant.to_owned())
                    .or_default()
                    .entry(provenance.id)
                    .or_default()
                    .push(port);
            }
        }
        for by_source in owned.values_mut() {
            for ports in by_source.values_mut() {
                ports.sort_unstable();
            }
        }
        Ok(owned)
    }

    /// Spec id -> (the ports bound to it, whether any of those ports has drifted), from
    /// `sm_configs` provenance — the spec-table counterpart of [`Self::ports_by_source`]. Read
    /// from an open (possibly mid-transaction) view so both the read paths and apply can use it.
    fn ports_by_spec(
        table: &impl ReadableTable<(&'static str, u16), &'static str>,
        tenant: &str,
    ) -> Result<BTreeMap<String, (Vec<u16>, bool)>, redb::StorageError> {
        let mut owned: BTreeMap<String, (Vec<u16>, bool)> = BTreeMap::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let (row_tenant, port) = key.value();
            if row_tenant != tenant {
                continue;
            }
            // A record that will not parse binds no spec *as far as provenance goes*, and every
            // replica holds the same bytes, so skipping it is deterministic. It can only
            // under-report a binding, never fabricate one — but under-reporting is what lets a
            // `SpecDelete` past a real binding, so it is logged rather than silently dropped.
            match serde_json::from_str::<StoredImposter>(value.value()) {
                Ok(stored) => {
                    if let Some(spec) = stored.spec {
                        let entry = owned.entry(spec.spec_id).or_default();
                        entry.0.push(port);
                        entry.1 |= stored.drifted;
                    }
                }
                Err(e) => {
                    tracing::error!(tenant, port, error = %e, "corrupt stored imposter; its spec binding is invisible");
                }
            }
        }
        for entry in owned.values_mut() {
            entry.0.sort_unstable();
        }
        Ok(owned)
    }

    fn render_spec(id: &str, stored: &StoredSpec, bound: Option<&(Vec<u16>, bool)>) -> SpecRecord {
        let (ports, drifted) = bound.cloned().unwrap_or_default();
        SpecRecord {
            id: id.to_owned(),
            format: stored.meta.format,
            digest: stored.meta.digest.as_str().to_owned(),
            source: stored.meta.source,
            ports,
            drifted,
            revision: stored.revision,
        }
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
    /// the replicated log. So a committed config proves the *record* is this tenant's and proves
    /// nothing whatever about who is listening on that port.
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
    /// `configured_ports`, a redb read transaction scanning the fleet-wide `SM_CONFIGS` table and
    /// allocating a `TenantId` per row — turning a 5-second console poll, fanned out to every peer,
    /// into an O(all imposters in the fleet) table scan on every node. The engine already holds
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
            let sm_datasets = read_txn
                .open_table(SM_DATASETS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let config_action =
                match Self::desired_configs(&configs, &sm_datasets, self.spool_dir.as_deref())
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
        // Repaired **before** the engine drive below, not after: a D2 dataset binding compiles
        // to a `lookup` naming a spool file, and the engine loading one that is not there yet
        // does not fail loudly — upstream's `CsvCache` warns and substitutes nothing, so the
        // response is served with its `${row}` tokens intact under a 200. `install_snapshot`
        // orders these the same way, and for the same reason.
        // Repair the spool directory (RFC-005 D1, #285): a restart with a missing or wiped
        // `datasets/` directory must not leave a dataset row pointing at a file this node no
        // longer has, since a restart replays no `DatasetPut` entries (they are already
        // reflected in `sm_dataset_blobs`) — nothing else re-materialises the files. A no-op
        // without a spool dir, like the engine drive above. Only ever *adds* a missing file:
        // an orphan already on disk is left alone, both because it is harmless and because
        // this pass has no way to know it is unreferenced without a second full table scan on
        // every restart.
        if let Some(dir) = &self.spool_dir {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let blobs = read_txn
                .open_table(SM_DATASET_BLOBS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            for item in blobs
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let digest = key.value();
                // Presence, not content: `write_spool` never leaves a partial file under the
                // final name, so a present file is a complete one unless the disk itself lied —
                // and re-hashing every blob on every restart to guard against that would make
                // startup cost proportional to the tenant byte quota. A deliberate trade.
                if self.spool_path(digest).is_some_and(|p| p.exists()) {
                    continue;
                }
                write_spool(dir, digest, value.value())
                    .map_err(|e| StorageError::from(StorageIOError::read_state_machine(&e)))?;
            }
        }

        // Unattributed: this materializes a whole table on restart, not one
        // caller's write, so there is no principal to name.
        self.drive_engine(vec![
            AttributedAction::unattributed(config_action),
            AttributedAction::unattributed(routes_action),
        ])
        .await;

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
                let (_tenant, port, space_key) = key.value();
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

    /// Test-only: overwrite a raw `sm_sources` row, bypassing validation — like
    /// [`Self::inject_raw_config`], the corrupt-record path is unreachable
    /// through the public API.
    #[cfg(test)]
    fn inject_raw_source(&self, tenant: &str, id: &str, value: &str) {
        let txn = self.db.begin_write().expect("test txn");
        {
            let mut table = txn.open_table(SM_SOURCES_TABLE).expect("test table");
            table.insert((tenant, id), value).expect("test insert");
        }
        txn.commit().expect("test commit");
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

    /// Drop audit rows older than `retention_secs` relative to `now_secs` — the
    /// replicated logical clock, exactly as [`Self::gc_dedup`] takes it, and
    /// never a local `SystemTime::now()`.
    ///
    /// `retention_secs == 0` means keep everything: an operator who turns
    /// retention off must not silently lose their history to a zero that reads
    /// as "expire immediately".
    ///
    /// Rows are expired on `ts_secs`, the applying entry's `issued_at_secs`,
    /// so "how old is this row" is answered with the same replicated clock that
    /// wrote it.
    ///
    /// Called once per `apply` against the clock as it stood *before* that
    /// batch, exactly as [`Self::gc_dedup`] is — so expiry lags the write that
    /// crosses the boundary by one apply. That is deliberate and harmless here:
    /// the lag is identical on every replica (they run the same GC at the same
    /// log point with the same clock), and a retention window is a floor on how
    /// long rows are kept, not a promise to delete them the instant it passes.
    ///
    /// A row whose JSON will not parse is dropped and logged rather
    /// than kept forever: it is committed-state corruption, it cannot be served
    /// to anyone, and it would otherwise pin the table's growth with something
    /// no reader can use.
    /// Returns the highest revision it removed, or `None` if it removed
    /// nothing — the caller folds that into `sm_audit_gc_watermark`, which is
    /// the exporter's only trustworthy evidence that rows were lost rather than
    /// simply never written (see [`SM_AUDIT_GC_WATERMARK_TABLE`]).
    fn gc_audit(
        table: &mut Table<'_, (u64, &'static str), &'static str>,
        now_secs: u64,
        retention_secs: u64,
    ) -> Result<Option<u64>, redb::StorageError> {
        if retention_secs == 0 {
            return Ok(None);
        }
        let cutoff = now_secs.saturating_sub(retention_secs);
        let mut removed_through: Option<u64> = None;
        table.retain(|(revision, op_id), value| {
            let keep = match serde_json::from_str::<AuditRow>(value) {
                Ok(row) => row.ts_secs >= cutoff,
                Err(e) => {
                    tracing::error!(revision, op_id, error = %e, "dropping unparseable sm_audit row");
                    false
                }
            };
            if !keep {
                removed_through = Some(removed_through.map_or(revision, |seen: u64| seen.max(revision)));
            }
            keep
        })?;
        Ok(removed_through)
    }

    /// Audit rows at or after `since`, ascending by revision, optionally
    /// narrowed to one tenant (issue #163).
    ///
    /// `tenant: Some(..)` is what a `TenantAdmin` gets — the filter is applied
    /// **here, server-side**, not by the caller. Handing a tenant admin the
    /// fleet's rows and trusting a client to narrow them would mean the server
    /// had already sent another tenant's audit history, which is the same
    /// mistake RFC-002 §4.3 warns about for the event stream.
    ///
    /// Answers from local applied state and needs no leadership or fan-out:
    /// every replica derived the same rows from the same log.
    #[allow(clippy::result_large_err)]
    pub fn audit_since(
        &self,
        since: u64,
        tenant: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<AuditRow>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_AUDIT_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut out = Vec::new();
        // Revision-major keys, so this is a range scan from `since` rather than
        // a scan of everything followed by a filter.
        for item in table
            .range((since, "")..)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            if out.len() >= limit {
                break;
            }
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (revision, op_id) = key.value();
            // A row that will not parse is committed-state corruption, not an
            // absent row — surfaced as an error rather than skipped, for the
            // same reason `principal_bindings` does it: an audit stream that
            // quietly omits what it cannot read is worse than one that admits
            // it is broken.
            let row: AuditRow = serde_json::from_str(value.value()).map_err(|e| {
                tracing::error!(revision, op_id, error = %e, "corrupt stored audit row");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            if let Some(tenant) = tenant
                && row.tenant.as_str() != tenant
            {
                continue;
            }
            out.push(row);
        }
        Ok(out)
    }

    /// The fleet's declared audit export sink, or `None` when none is declared
    /// (issue #164). Answered from local applied state, like `audit_since`.
    ///
    /// # Errors
    /// Storage I/O, or a stored record that will not parse.
    #[allow(clippy::result_large_err)]
    pub fn audit_sink(&self) -> StorageResult<Option<AuditSink>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_AUDIT_SINK_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let Some(value) = table
            .get(AUDIT_SINK_KEY)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        else {
            return Ok(None);
        };
        // Surfaced, never defaulted away: an unparseable sink record must not
        // read as "no sink configured", which would silently stop exporting.
        let record: AuditSink = serde_json::from_str(value.value()).map_err(|e| {
            tracing::error!(error = %e, "corrupt stored audit sink record");
            StorageError::from(StorageIOError::read_state_machine(&e))
        })?;
        Ok(Some(record))
    }

    /// The fleet's session-signing key, or `None` when no console login has minted one yet
    /// (RFC-006 §5.3, issue #185). Answered from local applied state, like `audit_sink`.
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
        // under whoever was relying on the first — same reasoning as `audit_sink`.
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

    /// The last revision shipped to the sink; `0` when nothing has shipped
    /// (issue #164).
    ///
    /// # Errors
    /// Storage I/O.
    #[allow(clippy::result_large_err)]
    pub fn audit_checkpoint(&self) -> StorageResult<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_AUDIT_CHECKPOINT_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(table
            .get(AUDIT_SINK_KEY)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map_or(0, |v| v.value()))
    }

    /// The highest revision retention GC has removed from `sm_audit`; `0` if it
    /// has never removed anything (issue #164).
    ///
    /// # Errors
    /// Storage I/O.
    #[allow(clippy::result_large_err)]
    pub fn audit_gc_watermark(&self) -> StorageResult<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_AUDIT_GC_WATERMARK_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(table
            .get(AUDIT_SINK_KEY)
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map_or(0, |v| v.value()))
    }

    /// The applied clear generation for `port` (or `port`'s `space`, when given); `0` if
    /// `ControlOp::JournalClearGen` has never committed for that key (issue #224).
    ///
    /// # Errors
    /// Storage I/O.
    #[allow(clippy::result_large_err)]
    pub fn journal_gen(&self, tenant: &str, port: u16, space: Option<&str>) -> StorageResult<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_JOURNAL_GENS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let space_key = journal_gen_space_key(space);
        Ok(table
            .get((tenant, port, space_key.as_str()))
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map_or(0, |v| v.value()))
    }

    /// The applied proxy-recording marker for `(tenant, port, sig_hash)` (#226): the
    /// recorded-response JSON `ControlOp::ProxyRecorded` committed, or `None` when the
    /// signature has never been recorded (or was cleared). Local durable state — any node
    /// answers without leadership, which is what lets a post-handoff owner say
    /// `AlreadyRecorded` with no in-memory trace of the claim.
    ///
    /// # Errors
    /// Storage I/O.
    #[allow(clippy::result_large_err)]
    pub fn proxy_recorded_resp(
        &self,
        tenant: &str,
        port: u16,
        sig_hash: &str,
    ) -> StorageResult<Option<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let table = read_txn
            .open_table(SM_PROXY_RECORDED_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        Ok(table
            .get((tenant, port, sig_hash))
            .map_err(|e| StorageIOError::read_state_machine(&e))?
            .map(|v| v.value().to_owned()))
    }

    /// The desired engine state as of now, read from an open (possibly
    /// mid-transaction) view of `sm_configs`: every tenant's config, unioned —
    /// parsed — disabled ones included (a paused imposter stays bound, #817).
    ///
    /// Union rather than default-tenant-only: ports are fleet-unique across
    /// tenants (RFC-002 §3.2, enforced by [`Self::port_claimed_by_another_tenant`]),
    /// so one shared `ImposterManager` can bind every tenant's imposters with
    /// no collision — there is nothing tenant-specific for the engine to key
    /// on.
    ///
    /// `Ok(Err((port, reason)))` means a stored record failed to parse. That
    /// must abort the sync, not shrink it: `apply_config` deletes every live
    /// imposter missing from the desired set, so silently skipping a broken
    /// record would tear down a healthy imposter and report it as an
    /// operator-issued delete. The caller refuses the sync and records the
    /// failure instead — the engine keeps serving its last-known state. This
    /// still holds with the union: a broken record in *any* tenant aborts the
    /// whole sync, exactly as it did when only the default tenant was read.
    ///
    /// This is also where a `_rift.dataset` binding becomes something the engine can execute
    /// (RFC-005 D2, #286): the stored config keeps the operator's declarative block, and the
    /// engine-facing copy gains the compiled `lookup` pointing at this node's spool file. It
    /// happens here and nowhere else — the other stored-config readers either edit and write back
    /// (and so must keep the declarative form) or only count usage.
    fn desired_configs(
        table: &impl ReadableTable<(&'static str, u16), &'static str>,
        datasets: &impl ReadableTable<(&'static str, &'static str, u64), &'static str>,
        spool_dir: Option<&Path>,
    ) -> Result<Result<Vec<ImposterConfig>, (u16, String)>, redb::StorageError> {
        let mut desired = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let (tenant, port) = key.value();
            let stored = match serde_json::from_str::<StoredImposter>(value.value()) {
                Ok(stored) => stored,
                Err(e) => {
                    return Ok(Err((port, format!("stored record will not parse: {e}"))));
                }
            };
            // Compiled as JSON, before the parse: upstream precomputes `_behaviors` once at
            // construction (#479), so a lookup added to an already-parsed config would change the
            // field and not what the engine runs.
            let mut config_json =
                match serde_json::from_str::<serde_json::Value>(&stored.config_json) {
                    Ok(value) => value,
                    Err(e) => {
                        return Ok(Err((port, format!("stored config will not parse: {e}"))));
                    }
                };
            if let Err(e) = crate::datasets::compile_bindings(
                &mut config_json,
                |name, version| Self::resolve_dataset(datasets, tenant, name, version),
                spool_dir,
            ) {
                // Refused rather than dropped, for the same reason an unparseable record is: a
                // binding that silently fails to compile serves the response with its `${row}`
                // tokens unsubstituted, under a 200.
                return Ok(Err((
                    port,
                    format!("dataset binding will not compile: {e}"),
                )));
            }
            // Disabled configs stay in the desired set: upstream keeps a
            // paused imposter bound (serving 503) — dropping it here would
            // read as "delete it" to apply_config (#817).
            match serde_json::from_value::<ImposterConfig>(config_json) {
                Ok(config) => desired.push(config),
                Err(e) => {
                    return Ok(Err((port, format!("stored config will not parse: {e}"))));
                }
            }
        }
        Ok(Ok(desired))
    }

    /// One dataset version as [`crate::datasets::compile_bindings`] needs it (RFC-005 D2, #286).
    ///
    /// `Ok(None)` means the row is genuinely absent or tombstoned — a binding to either is
    /// refused, never quietly compiled against whatever version happens to be live now.
    ///
    /// A storage failure or a row that will not parse is an `Err`, **not** an `Ok(None)**, for the
    /// same reason [`Self::datasets`] refuses rather than skipping one: folding corruption into
    /// "absent" tells an operator their dataset is gone when it is not. Here it would do that on
    /// every node at once, for a dataset they can still list — and the real cause, a corrupt row,
    /// would appear in no log at all.
    fn resolve_dataset(
        datasets: &impl ReadableTable<(&'static str, &'static str, u64), &'static str>,
        tenant: &str,
        name: &str,
        version: u64,
    ) -> Result<Option<crate::datasets::ResolvedDataset>, String> {
        let Some(guard) = datasets
            .get((tenant, name, version))
            .map_err(|e| format!("reading dataset \"{name}\" version {version}: {e}"))?
        else {
            return Ok(None);
        };
        let stored: StoredDataset = serde_json::from_str(guard.value()).map_err(|e| {
            tracing::error!(tenant, name, version, error = %e, "corrupt stored dataset");
            format!("stored dataset \"{name}\" version {version} will not parse: {e}")
        })?;
        if stored.deleted {
            return Ok(None);
        }
        Ok(Some(crate::datasets::ResolvedDataset {
            version: stored.version,
            digest: stored.record.digest.to_string(),
            delimiter: stored.record.delimiter,
            key_columns: stored.record.key_columns,
        }))
    }

    /// Build the engine action for a config op: a full sync when every stored
    /// record parses, a recorded refusal when one does not.
    #[allow(clippy::result_large_err)]
    fn sync_action(
        configs: &Table<'_, (&'static str, u16), &'static str>,
        datasets: &Table<'_, (&'static str, &'static str, u64), &'static str>,
        spool_dir: Option<&Path>,
    ) -> StorageResult<EngineAction> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        Ok(
            match Self::desired_configs(configs, datasets, spool_dir).map_err(io)? {
                Ok(desired) => EngineAction::Sync(desired),
                Err((port, error)) => EngineAction::RefuseSync { port, error },
            },
        )
    }

    /// The desired route table as of now, read from an open (possibly
    /// mid-transaction) view of `sm_routes`: the **default tenant's** routes only.
    ///
    /// Deliberately NOT the union, unlike [`Self::desired_configs`] — and the asymmetry is the
    /// point, so it is spelled out here rather than left to be "fixed" later.
    ///
    /// Imposters can be unioned safely because a request names the resource it wants: a port is
    /// fleet-unique (RFC-002 §3.2), so binding every tenant's ports into one engine is collision-
    /// free and each request resolves to exactly one owner. **Front-door routes have no such
    /// discriminator.** The front door is a single listener and an arriving data-plane request
    /// carries no tenant identity — RFC-002 §7 keeps that plane open and anonymous on purpose — so
    /// a unioned table is one shared *matching* namespace that every tenant writes into.
    ///
    /// Concretely, unioning here would let any principal holding `imposter.write` in any tenant
    /// publish `{"match": {}, "priority": i32::MAX}` — an empty match is an explicitly legal
    /// catch-all and the priority is unbounded — and capture **100% of front-door traffic
    /// fleet-wide**. That is a denial of service against every other tenant, and where the target
    /// names another tenant's now-bound port, a public read of its mocks. Constraining
    /// `RouteTarget.port` to the writing tenant's own ports does not fix it: the shadowing lives in
    /// the match, not the target.
    ///
    /// So routes stay default-only until the front door has a tenant dimension to route on (a host
    /// mapping, a listener per tenant, or an explicit per-tenant prefix). Tenanted routes are still
    /// *stored*, and [`Self::route_table`] reads them back per tenant so a tenant sees what it
    /// wrote — they are simply not compiled into the shared front door.
    ///
    /// `Ok(Err((id, reason)))` means a stored record failed to parse — this
    /// crate is the only writer of `sm_routes`, so it should never happen in
    /// practice, but the read path stays defensive rather than trusting that.
    fn desired_routes(
        table: &impl ReadableTable<(&'static str, &'static str), &'static str>,
    ) -> Result<Result<RouteTable, (String, String)>, redb::StorageError> {
        let mut routes = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            let (tenant, id) = key.value();
            // The rule itself lives in `routes_installed_for`, which the admin plane's hit read
            // also calls — so what is compiled here and what the console reports as installed are
            // the same decision, not two copies of it (issue #368).
            if !routes_installed_for(tenant) {
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
            PreconditionTarget::Imposter(tenant, port) => {
                match configs.get((tenant.as_str(), port)).map_err(io)? {
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
                }
            }
            // A tenant with no row has never had its table written: revision 0
            // (issue #210). Absence is a real revision here, not a missing
            // record — unlike the imposter arm above, where there is nothing to
            // condition on at all — so it compares rather than refuses, and a
            // client that conditions on 0 and writes first legitimately wins.
            PreconditionTarget::RouteTable(tenant) => {
                let actual = routes_revision
                    .get(tenant.as_str())
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

    /// `(tenant, port)`'s current spec provenance, or `None` when it holds no imposter or the
    /// imposter is not spec-bound (RFC-004 S2, #278) — the spec-table counterpart of
    /// [`Self::provenance_of`], read the same way so a config-mutating write can preserve it
    /// across the rewrite.
    #[allow(clippy::result_large_err)]
    fn spec_of(
        configs: &Table<'_, (&'static str, u16), &'static str>,
        tenant: &str,
        port: u16,
    ) -> StorageResult<Option<SpecProvenance>> {
        Ok(Self::stored_imposter(configs, tenant, port)?.and_then(|stored| stored.spec))
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

    /// The stored spec at `(tenant, id)`, or `None` when there is none (RFC-004 S2, #278).
    ///
    /// A record that will not parse is treated as `None`, like [`Self::stored_source`]: apply
    /// takes its "unknown spec" path deterministically, since every replica holds the same bad
    /// bytes.
    #[allow(clippy::result_large_err)]
    fn stored_spec(
        specs: &Table<'_, (&'static str, &'static str), &'static str>,
        tenant: &str,
        id: &str,
    ) -> StorageResult<Option<StoredSpec>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let Some(guard) = specs.get((tenant, id)).map_err(io)? else {
            return Ok(None);
        };
        match serde_json::from_str::<StoredSpec>(guard.value()) {
            Ok(stored) => Ok(Some(stored)),
            Err(e) => {
                tracing::error!(spec_id = %id, error = %e, "corrupt stored spec");
                Ok(None)
            }
        }
    }

    /// Every port of `tenant` currently bound to spec `id`, ascending (RFC-004 S2, #278) — the
    /// spec-table counterpart of [`Self::ports_of_source`].
    #[allow(clippy::result_large_err)]
    fn ports_of_spec(
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
            match serde_json::from_str::<StoredImposter>(value.value()) {
                Ok(stored) if stored.spec.as_ref().is_some_and(|s| s.spec_id == id) => {
                    ports.push(port);
                }
                Ok(_) => {}
                // Same reasoning as `ports_by_spec`: deterministic to skip, wrong to skip quietly.
                Err(e) => {
                    tracing::error!(tenant, port, error = %e, "corrupt stored imposter; its spec binding is invisible");
                }
            }
        }
        ports.sort_unstable();
        Ok(ports)
    }

    /// Whether any `sm_specs` row, in any tenant, still names `digest` (RFC-004 S2, #278) — the
    /// blob table's "refcount", computed by scan rather than maintained as a counter (see
    /// [`SM_SPEC_BLOBS_TABLE`]'s doc for why that trade is the right one here).
    #[allow(clippy::result_large_err)]
    fn spec_digest_referenced(
        specs: &Table<'_, (&'static str, &'static str), &'static str>,
        digest: &str,
    ) -> StorageResult<bool> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        for item in specs.iter().map_err(io)? {
            let (_, value) = item.map_err(io)?;
            if let Ok(stored) = serde_json::from_str::<StoredSpec>(value.value())
                && stored.meta.digest.as_str() == digest
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Remove `digest`'s blob if [`Self::spec_digest_referenced`] says nothing holds it any
    /// more. Called after a spec row is deleted or repointed to a different digest — always
    /// *after* the `sm_specs` write that could have removed the last reference, so the scan
    /// sees the post-write truth.
    #[allow(clippy::result_large_err)]
    fn gc_spec_blob_if_unreferenced(
        spec_blobs: &mut Table<'_, &'static str, &'static str>,
        specs: &Table<'_, (&'static str, &'static str), &'static str>,
        digest: &str,
    ) -> StorageResult<()> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        if !Self::spec_digest_referenced(specs, digest)? {
            spec_blobs.remove(digest).map_err(io)?;
        }
        Ok(())
    }

    /// Every stored row (live or tombstoned) for `(tenant, name)` in `sm_datasets`,
    /// version-ascending (RFC-005 D1, #285) — the source both `DatasetPut`'s version numbering
    /// and `DatasetDelete`'s tombstone sweep scan. A row that will not parse is logged and
    /// skipped, like [`Self::stored_spec`]: every replica holds the same bad bytes, so skipping
    /// it deterministically never diverges two replicas' apply of the same committed op.
    #[allow(clippy::result_large_err)]
    fn dataset_rows(
        datasets: &Table<'_, (&'static str, &'static str, u64), &'static str>,
        tenant: &str,
        name: &str,
    ) -> StorageResult<Vec<(u64, StoredDataset)>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let mut rows = Vec::new();
        for item in datasets.iter().map_err(io)? {
            let (key, value) = item.map_err(io)?;
            let (row_tenant, row_name, version) = key.value();
            if row_tenant != tenant || row_name != name {
                continue;
            }
            match serde_json::from_str::<StoredDataset>(value.value()) {
                Ok(stored) => rows.push((version, stored)),
                Err(e) => {
                    tracing::error!(tenant, name, version, error = %e, "corrupt stored dataset");
                }
            }
        }
        rows.sort_by_key(|(version, _)| *version);
        Ok(rows)
    }

    /// `(distinct live dataset names `tenant` holds, excluding `exclude_name` / sum of bytes
    /// across every live version of every dataset `tenant` holds, `exclude_name` included)` —
    /// what `DatasetPut`'s count and total-bytes quotas are checked against (RFC-005 D1, #285).
    /// One scan for both, like [`Self::quota_refusal_for_config`]'s single scan for imposter
    /// count. `exclude_name` is excluded only from the name count, never the byte sum: a
    /// re-versioned name is not a *new* dataset, but its already-live bytes still count toward
    /// the total the tenant holds.
    #[allow(clippy::result_large_err)]
    fn dataset_usage(
        datasets: &Table<'_, (&'static str, &'static str, u64), &'static str>,
        tenant: &str,
        exclude_name: &str,
    ) -> StorageResult<(u32, u64)> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let mut names: BTreeSet<String> = BTreeSet::new();
        let mut total_bytes: u64 = 0;
        for item in datasets.iter().map_err(io)? {
            let (key, value) = item.map_err(io)?;
            let (row_tenant, row_name, _version) = key.value();
            if row_tenant != tenant {
                continue;
            }
            let stored: StoredDataset = match serde_json::from_str(value.value()) {
                Ok(stored) => stored,
                Err(e) => {
                    tracing::error!(tenant, name = row_name, error = %e, "corrupt stored dataset");
                    continue;
                }
            };
            if stored.deleted {
                continue;
            }
            if row_name != exclude_name {
                names.insert(row_name.to_owned());
            }
            total_bytes = total_bytes.saturating_add(stored.record.bytes);
        }
        Ok((u32::try_from(names.len()).unwrap_or(u32::MAX), total_bytes))
    }

    /// Whether any *live* `sm_datasets` row, in any tenant, still names `digest` (RFC-005 D1,
    /// #285) — the dataset blob table's "refcount", by scan, the same trade
    /// [`Self::spec_digest_referenced`] makes and for the identical reason. Tombstoned rows do
    /// not count: a deleted version no longer entitles its digest to a file.
    #[allow(clippy::result_large_err)]
    fn dataset_digest_referenced(
        datasets: &Table<'_, (&'static str, &'static str, u64), &'static str>,
        digest: &str,
    ) -> StorageResult<bool> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        for item in datasets.iter().map_err(io)? {
            let (key, value) = item.map_err(io)?;
            match serde_json::from_str::<StoredDataset>(value.value()) {
                Ok(stored) if !stored.deleted && stored.record.digest.as_str() == digest => {
                    return Ok(true);
                }
                Ok(_) => {}
                // A row that will not parse might be a live reference to exactly this digest.
                // The safe reading for a garbage collector is "still referenced": a leaked
                // blob costs disk, a reclaimed one under a live row costs the dataset. Loud,
                // and deterministic — every replica holds the same bytes.
                Err(e) => {
                    let (t, name, version) = key.value();
                    tracing::error!(
                        tenant = t, name, version, error = %e,
                        "corrupt stored dataset; treating its digest as still referenced"
                    );
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// The union of every digest a live `sm_specs` or `sm_datasets` row still names (RFC-004 S2 /
    /// RFC-005 D1) — what the per-node blob transfer store's GC (#437) is allowed to reclaim
    /// around. A different question from [`Self::spec_digest_referenced`] and
    /// [`Self::dataset_digest_referenced`]: those gate the state machine's own replicated blob
    /// tables (`sm_spec_blobs`, the spooled dataset CSVs), which every replica agrees on. The blob
    /// transfer store (`blobs::BlobStore`) is node-local and unreplicated — nothing about it
    /// enters the state machine — so its GC needs its own, independently computed answer to "what
    /// does *this node's* applied state still point at", read straight off the tables.
    ///
    /// Read-only, no Raft round trip, the same shape as [`Self::specs`]/[`Self::sources`]: a row
    /// that will not parse fails the whole scan rather than being silently dropped from the set.
    /// Dropping it would read as "nothing references this digest" and let the sweep reclaim a
    /// blob a live (if corrupt) row still names — the same conservatism
    /// [`Self::dataset_digest_referenced`] applies per-row (a row that will not parse is treated
    /// as still referencing whatever digest is being asked about), generalised to a whole-table
    /// union: there is no single digest here to spare, so the entire scan fails instead of
    /// guessing which one to protect. The caller (`RaftNode`'s GC tick) is required to propagate
    /// this with `?` and skip that sweep, never to substitute an empty set.
    #[allow(clippy::result_large_err)]
    pub fn referenced_digests(&self) -> StorageResult<HashSet<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let mut digests = HashSet::new();

        let specs = read_txn
            .open_table(SM_SPECS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        for item in specs
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (_, id) = key.value();
            let stored: StoredSpec = serde_json::from_str(value.value()).map_err(|e| {
                tracing::error!(spec_id = %id, error = %e, "corrupt stored spec");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            digests.insert(stored.meta.digest.as_str().to_owned());
        }

        let datasets = read_txn
            .open_table(SM_DATASETS_TABLE)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        for item in datasets
            .iter()
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
            let (tenant, name, version) = key.value();
            let stored: StoredDataset = serde_json::from_str(value.value()).map_err(|e| {
                tracing::error!(tenant, name, version, error = %e, "corrupt stored dataset");
                StorageError::from(StorageIOError::read_state_machine(&e))
            })?;
            if !stored.deleted {
                digests.insert(stored.record.digest.as_str().to_owned());
            }
        }

        Ok(digests)
    }

    /// Every port of `tenant` whose applied config binds a stub response to dataset `name`
    /// (RFC-005 §3.3, #285) — what `DatasetDelete` refuses against. The binding lives in a
    /// response's `_rift.dataset.name` extension (D2 compiles it into the engine's lookup
    /// wiring; D1 only needs to know a binding exists, never what it does), so this reads the
    /// stored config as generic JSON rather than through the typed `ImposterConfig` — the same
    /// reason [`Self::ports_of_spec`] reads `StoredImposter::spec` instead of re-parsing a
    /// document: the fact this needs is provenance, not behavior.
    #[allow(clippy::result_large_err)]
    fn ports_binding_dataset(
        configs: &Table<'_, (&'static str, u16), &'static str>,
        tenant: &str,
        name: &str,
    ) -> StorageResult<Result<Vec<u16>, String>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        let mut ports = Vec::new();
        for item in configs.iter().map_err(io)? {
            let (key, value) = item.map_err(io)?;
            let (t, port) = key.value();
            if t != tenant {
                continue;
            }
            // This is a delete guard, so it fails *closed*: a config row it cannot read might be
            // the very stub that binds the dataset, and "unreadable" must refuse the delete
            // rather than let it through — the opposite of `ports_of_spec`, where under-reporting
            // only loses a provenance fact. Deterministic: every replica holds the same bytes.
            let stored = match serde_json::from_str::<StoredImposter>(value.value()) {
                Ok(stored) => stored,
                Err(e) => {
                    tracing::error!(tenant, port, error = %e, "corrupt stored imposter; refusing the dataset delete");
                    return Ok(Err(format!(
                        "cannot tell whether dataset {name:?} is bound: port {port}'s stored \
                         imposter is unreadable"
                    )));
                }
            };
            let config = match serde_json::from_str::<serde_json::Value>(&stored.config_json) {
                Ok(config) => config,
                Err(e) => {
                    tracing::error!(tenant, port, error = %e, "corrupt stored config; refusing the dataset delete");
                    return Ok(Err(format!(
                        "cannot tell whether dataset {name:?} is bound: port {port}'s stored \
                         config is unreadable"
                    )));
                }
            };
            let binds = config["stubs"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|stub| stub["responses"].as_array())
                .flatten()
                .any(|resp| resp["_rift"]["dataset"]["name"].as_str() == Some(name));
            if binds {
                ports.push(port);
            }
        }
        ports.sort_unstable();
        Ok(Ok(ports))
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

    /// The tenant's quotas: [`Quotas::default`] when it has **no** stored
    /// record, and a committed refusal when the record exists but will not parse
    /// (issue #163).
    ///
    /// The two cases are deliberately not the same, and conflating them is a
    /// fail-open:
    ///
    /// - **No record** is a domain value, not an error. `default` has no stored
    ///   row on a fresh cluster (nothing writes one), so a fleet that never
    ///   configured tenancy must not find every write refused because an absent
    ///   record read as a quota of nothing. The *generous* default is correct.
    /// - **A corrupt record** is a gate that cannot read what it is gating.
    ///   Answering `Quotas::default()` there would hand the tenant the generous
    ///   ceiling precisely when the operator's real — possibly much tighter —
    ///   one became unreadable, and nothing downstream would notice: a quota is
    ///   a resource gate, and a gate that cannot classify its input must treat
    ///   it as the dangerous class. So it refuses, the same way `TenantDelete`
    ///   already refuses against an unreadable row.
    ///
    /// An earlier version of this defended the fail-open by claiming
    /// `require_live_tenant` had already refused corruption upstream. It had
    /// not: that helper is called only from the `PrincipalPut` /
    /// `PrincipalCreate` / `BindingPut` arms, and *neither* caller of this
    /// function — `PutImposter` via [`Self::quota_refusal_for_config`], or
    /// `PatchStubs` directly — goes through it.
    #[allow(clippy::result_large_err)]
    fn quotas_for(
        tenants: &Table<'_, &'static str, &'static str>,
        tenant: &str,
    ) -> StorageResult<Result<Quotas, String>> {
        Ok(match Self::stored_tenant(tenants, tenant)? {
            Err(reason) => Err(format!(
                "tenant {tenant:?} has an unreadable record, so its quota cannot \
                 be checked: {reason}"
            )),
            Ok(None) => Ok(Quotas::default()),
            Ok(Some(record)) => Ok(record.quotas),
        })
    }

    /// `Some(reason)` when committing `config` on `port` would put `tenant`
    /// over a ceiling (RFC-002 §4.4, issue #163).
    ///
    /// Two ceilings apply. `max_stubs_per_imposter` is a property of the
    /// payload alone. `max_imposters` counts what the tenant already holds —
    /// and counts it **excluding `port`**, because replacing an existing
    /// imposter does not add one; without that, a tenant sitting exactly at its
    /// limit could never update anything it already owned.
    #[allow(clippy::result_large_err)]
    fn quota_refusal_for_config(
        configs: &Table<'_, (&'static str, u16), &'static str>,
        tenants: &Table<'_, &'static str, &'static str>,
        tenant: &str,
        port: u16,
        config: &ImposterConfig,
    ) -> StorageResult<Option<String>> {
        let quotas = match Self::quotas_for(tenants, tenant)? {
            Ok(quotas) => quotas,
            Err(reason) => return Ok(Some(reason)),
        };
        let stubs = config.stubs.len();
        if stubs > quotas.max_stubs_per_imposter as usize {
            return Ok(Some(format!(
                "tenant {tenant:?} allows at most {} stubs per imposter; this config carries {stubs}",
                quotas.max_stubs_per_imposter
            )));
        }
        let mut held = 0u32;
        for item in configs
            .iter()
            .map_err(|e| StorageError::from(StorageIOError::write_state_machine(&e)))?
        {
            let (key, _) =
                item.map_err(|e| StorageError::from(StorageIOError::write_state_machine(&e)))?;
            let (stored_tenant, stored_port) = key.value();
            if stored_tenant == tenant && stored_port != port {
                held = held.saturating_add(1);
            }
        }
        if held >= quotas.max_imposters {
            return Ok(Some(format!(
                "tenant {tenant:?} is at its ceiling of {} imposters",
                quotas.max_imposters
            )));
        }
        Ok(None)
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
        routes_revision: &mut Table<'_, &'static str, u64>,
        sources: &mut Table<'_, (&'static str, &'static str), &'static str>,
        specs: &mut Table<'_, (&'static str, &'static str), &'static str>,
        spec_blobs: &mut Table<'_, &'static str, &'static str>,
        datasets: &mut Table<'_, (&'static str, &'static str, u64), &'static str>,
        dataset_blobs: &mut Table<'_, &'static str, &'static str>,
        tenants: &mut Table<'_, &'static str, &'static str>,
        principals: &mut Table<'_, &'static str, &'static str>,
        bindings: &mut Table<'_, (&'static str, &'static str), &'static str>,
        audit_sink: &mut Table<'_, &'static str, &'static str>,
        audit_checkpoint: &mut Table<'_, &'static str, u64>,
        session_key: &mut Table<'_, &'static str, &'static str>,
        fleet_name: &mut Table<'_, &'static str, &'static str>,
        journal_gens: &mut Table<'_, (&'static str, u16, &'static str), u64>,
        proxy_recorded: &mut Table<'_, (&'static str, u16, &'static str), &'static str>,
        // The local journal to push a committed generation into (issue #224), resolved once by
        // `apply` rather than upgraded per op — `None` in storage tests, on an embedder that
        // never wires one, or when a shutdown race has already dropped it (see the `journal`
        // field's doc on `RedbStateMachine`).
        journal: Option<&ClusterJournal>,
        // Where a `DatasetPut`'s spool file is written, inside this same apply transaction
        // (RFC-005 D1, #285) — `None` in storage tests and on an embedder that never attached
        // one, matching every other node-local handle threaded through here.
        spool_dir: Option<&Path>,
        op: &ControlOp,
        index: u64,
        issued_at_secs: u64,
    ) -> StorageResult<Result<Vec<EngineAction>, String>> {
        let io =
            |e: redb::StorageError| StorageError::from(StorageIOError::write_state_machine(&e));
        match op {
            // Deliberately mutates nothing (RFC-002 §9's audit exception, issue #287). The op
            // exists so the export is recorded, and the recording happens in the audit arm that
            // runs for every committed op — not here. An empty arm someone had to write, rather
            // than an absent branch: the "changes nothing" claim is the whole contract.
            ControlOp::DatasetContentRead { .. } => Ok(Ok(Vec::new())),
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
                // Quotas (RFC-002 §4.4, issue #163). Checked here, at apply, on
                // every replica — not pre-commit at the leader.
                //
                // The issue says "enforced at the Raft leader", meaning: in the
                // one place that sees the tenant's whole write stream, rather
                // than in a handler counting its own node's view (which
                // over-commits under concurrent writes). Apply satisfies that
                // and one thing leader-side validation cannot: a refusal here
                // is a *committed* decision, so all three nodes record the same
                // `Failed` outcome at the same revision — which is what the
                // acceptance criteria actually demand, and what makes the
                // refusal discoverable through `op_status` after a parked
                // replay.
                if let Some(reason) =
                    Self::quota_refusal_for_config(configs, tenants, tenant.as_str(), port, config)?
                {
                    return Ok(Err(reason));
                }
                // Provenance survives a manual replace: the source still owns
                // this port, it just no longer holds what the source declares.
                // Clearing it instead would orphan the port, and the next
                // `overwrite` pull would recreate it as a second imposter.
                let provenance = Self::provenance_of(configs, tenant.as_str(), port)?;
                // A manual PutImposter against a spec-bound port is drift (RFC-004 S2, #278):
                // the provenance survives — a redeploy is still tracked to the spec it named —
                // but the bytes no longer match what that spec last produced.
                let spec = Self::spec_of(configs, tenant.as_str(), port)?;
                let drifted = spec.is_some();
                let config_json = serde_json::to_string(config)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                let stored = StoredImposter {
                    config_json,
                    enabled: config.enabled,
                    revision: index,
                    source: provenance.clone(),
                    spec,
                    drifted,
                };
                let value = serde_json::to_string(&stored)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                configs
                    .insert((tenant.as_str(), port), value.as_str())
                    .map_err(io)?;
                // A replace is a new imposter in the recording sense (#226): the old
                // config's recorded stubs are gone from the stub list this put installs,
                // and a surviving marker row would answer `AlreadyRecorded` with the *old*
                // upstream's response — so the markers die with the config they described,
                // exactly as they do on `DeleteImposter`.
                proxy_recorded
                    .retain(|(t, p, _), _| !(t == tenant.as_str() && p == port))
                    .map_err(io)?;
                Self::mark_drifted(sources, tenant.as_str(), provenance.as_ref(), index)?;
                crate::metrics::config_applied(port, index);
                Ok(Ok(vec![Self::sync_action(configs, datasets, spool_dir)?]))
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
                // The per-imposter stub ceiling, checked on the *result* of the
                // edit rather than on the edit script: a script is a sequence of
                // adds, moves and deletes, so only the config it produces knows
                // how many stubs the imposter ends up with. `max_imposters` is
                // not re-checked — a patch edits an imposter that already
                // exists and cannot add one.
                let quotas = match Self::quotas_for(tenants, tenant.as_str())? {
                    Ok(quotas) => quotas,
                    Err(reason) => return Ok(Err(reason)),
                };
                if config.stubs.len() > quotas.max_stubs_per_imposter as usize {
                    return Ok(Err(format!(
                        "tenant {:?} allows at most {} stubs per imposter; this edit would leave {}",
                        tenant.as_str(),
                        quotas.max_stubs_per_imposter,
                        config.stubs.len()
                    )));
                }
                record.config_json = serde_json::to_string(&config)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                record.revision = index;
                // A stub patch against a spec-bound port is drift (RFC-004 S2, #278), same
                // reasoning as `PutImposter`'s.
                if record.spec.is_some() {
                    record.drifted = true;
                }
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
                // Recordings die with their imposter (#226) — the clustered mirror of the
                // manager's own port-reclaim `clear`, and atomic with the delete here.
                proxy_recorded
                    .retain(|(t, p, _), _| !(t == tenant.as_str() && p == *port))
                    .map_err(io)?;
                Self::mark_drifted(sources, tenant.as_str(), provenance.as_ref(), index)?;
                crate::metrics::config_removed(*port);
                Ok(Ok(vec![Self::sync_action(configs, datasets, spool_dir)?]))
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
                proxy_recorded
                    .retain(|(t, _, _), _| t != tenant)
                    .map_err(io)?;
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
                Ok(Ok(vec![Self::sync_action(configs, datasets, spool_dir)?]))
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
                // The applying log index, so the stamp is the same on every
                // replica and is exactly the revision the front reports back to
                // the client that caused it (issue #210).
                routes_revision.insert(tenant_str, index).map_err(io)?;
                Ok(Ok(vec![Self::sync_routes_action(routes)?]))
            }
            ControlOp::DeleteRoute { tenant, id } => {
                // Idempotent no-op if absent, like `DeleteImposter` — the
                // admin-surface 404 for a missing route (if the operator
                // wants one) is the write path's concern, not apply's.
                routes.remove((tenant.as_str(), id.as_str())).map_err(io)?;
                // Stamped even when the remove found nothing: the revision is
                // the table's, and this op *committed* against that table, so
                // an outstanding precondition must not survive it (issue #210).
                // Making the stamp conditional on the row's existence would
                // make the revision depend on state the client cannot see.
                routes_revision.insert(tenant.as_str(), index).map_err(io)?;
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
                // "tear down live traffic" (D-29: orphan, never cascade) — but
                // nothing may still point at a source that no longer exists,
                // so their provenance is cleared.
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
                    // Every refusal lives in this pre-pass so the arm below is
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
                    // A `_rift.dataset` binding is pinned at *admission* (RFC-005 D2, #286), and a
                    // source pull is not an admission — nothing here resolves a version to a
                    // digest. An unpinned block that reached storage would be refused at apply,
                    // and that refusal aborts the whole engine sync, so a document fetched from a
                    // remote endpoint could freeze this node's entire config plane. Refused here
                    // instead, scoped to the pull that carried it.
                    let rendered = match serde_json::to_value(config) {
                        Ok(rendered) => rendered,
                        Err(e) => {
                            return Ok(Err(format!(
                                "pull result for port {port} could not be inspected: {e}"
                            )));
                        }
                    };
                    let bound = crate::datasets::bound_dataset_names(&rendered);
                    if !bound.is_empty() {
                        return Ok(Err(format!(
                            "port {port} binds dataset(s) {}: a `_rift.dataset` block is resolved \
                             and pinned when it is written to the admin API, so a source pull \
                             cannot carry one",
                            bound.join(", ")
                        )));
                    }
                }

                let provenance = SourceProvenance {
                    id: id.clone(),
                    version: version.clone(),
                };
                let previously_owned = Self::ports_of_source(configs, tenant_str, id)?;
                let declared: BTreeSet<u16> = fetched.iter().filter_map(|c| c.port).collect();

                // Quotas bind the pull path too (RFC-002 §4.4, issue #163).
                //
                // Enforcing them only on `PutImposter`/`PatchStubs` is exactly
                // the drift the comment above warns about, and the quieter half
                // of it: a tenant capped at five imposters could hold five
                // hundred by pointing a source at a document that declares them,
                // without anyone typing a port. A ceiling that one write path
                // ignores is not a ceiling.
                //
                // In the pre-pass, and for the same load-bearing reason as the
                // port checks: `Ok(Err(_))` is a committed `Failed`, not a
                // rollback, so a refusal raised after the de-declared-port
                // removals below would leave a half-pull reported as a clean
                // failure.
                //
                // Counted as *the whole post-pull set*, not incrementally: a
                // pull replaces this source's ports outright, so what matters is
                // what the tenant ends up holding — everything it holds that
                // this source does not own, plus everything this pull declares.
                {
                    let quotas = match Self::quotas_for(tenants, tenant_str)? {
                        Ok(quotas) => quotas,
                        Err(reason) => return Ok(Err(reason)),
                    };
                    for config in fetched {
                        let stubs = config.stubs.len();
                        if stubs > quotas.max_stubs_per_imposter as usize {
                            let port = config.port.unwrap_or_default();
                            return Ok(Err(format!(
                                "tenant {tenant_str:?} allows at most {} stubs per imposter; \
                                 the pulled config for port {port} carries {stubs}",
                                quotas.max_stubs_per_imposter
                            )));
                        }
                    }
                    let mut retained = 0u32;
                    for item in configs.iter().map_err(io)? {
                        let (key, _) = item.map_err(io)?;
                        let (stored_tenant, stored_port) = key.value();
                        if stored_tenant == tenant_str && !previously_owned.contains(&stored_port) {
                            retained = retained.saturating_add(1);
                        }
                    }
                    let total =
                        retained.saturating_add(u32::try_from(declared.len()).unwrap_or(u32::MAX));
                    if total > quotas.max_imposters {
                        return Ok(Err(format!(
                            "tenant {tenant_str:?} is limited to {} imposters; this pull would \
                             leave it holding {total}",
                            quotas.max_imposters
                        )));
                    }
                }

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
                    // A source-driven replace against a spec-bound port is drift (RFC-004 S2,
                    // #278), same reasoning as `PutImposter`'s: the provenance survives, the
                    // bytes no longer match what the spec last produced.
                    let spec = existing.as_ref().and_then(|e| e.spec.clone());
                    let drifted = spec.is_some();
                    let stored = StoredImposter {
                        config_json,
                        enabled: config.enabled,
                        revision: index,
                        source: Some(provenance.clone()),
                        spec,
                        drifted,
                    };
                    let value = serde_json::to_string(&stored)
                        .map_err(|e| StorageIOError::write_state_machine(&e))?;
                    configs
                        .insert((tenant_str, port), value.as_str())
                        .map_err(io)?;
                    // Same reasoning as the `PutImposter` purge (#226): a source-driven
                    // replace installs a new stub list, so the old config's recording
                    // markers must not survive to replay a dead upstream's responses. The
                    // byte-identical short-circuit above never reaches here, which is what
                    // keeps still-valid recordings across a no-op pull.
                    proxy_recorded
                        .retain(|(t, p, _), _| !(t == tenant_str && p == port))
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

                Ok(Ok(vec![Self::sync_action(configs, datasets, spool_dir)?]))
            }
            ControlOp::TenantPut {
                tenant,
                display_name,
                quotas,
                journal_retention_secs,
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
                    journal_retention_secs: *journal_retention_secs,
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
                // Specs cascade too (RFC-004 S2, #278), the same shape as sources above: the
                // tenant's records go, and any blob nothing else still references — including a
                // *different* tenant's spec that happened to share the same bytes — is reclaimed.
                // Collected before the removal so the digests to check are the ones this tenant
                // is about to stop referencing, not whatever happens to remain after.
                let mut freed_digests = Vec::new();
                for item in specs.iter().map_err(io)? {
                    let (key, value) = item.map_err(io)?;
                    let (t, _id) = key.value();
                    if t != tenant_str {
                        continue;
                    }
                    if let Ok(stored) = serde_json::from_str::<StoredSpec>(value.value()) {
                        freed_digests.push(stored.meta.digest.as_str().to_owned());
                    }
                }
                specs.retain(|(t, _), _| t != tenant_str).map_err(io)?;
                for digest in freed_digests {
                    Self::gc_spec_blob_if_unreferenced(spec_blobs, specs, &digest)?;
                }
                // Datasets cascade too (RFC-005 D1, #285), the same shape as specs above: the
                // tenant's rows go — every version, live or already tombstoned, since a deleted
                // tenant has no "old version stays visible" a live `DatasetDelete`'s tombstone
                // exists to protect — and any blob nothing else still references, including a
                // *different* tenant's dataset that happened to share the same bytes, is
                // reclaimed. Removal happens after this txn commits (`UnspoolDataset`), so the
                // digests to free are collected from the live rows before that removal.
                let mut freed_dataset_digests = Vec::new();
                for item in datasets.iter().map_err(io)? {
                    let (key, value) = item.map_err(io)?;
                    let (t, _name, _version) = key.value();
                    if t != tenant_str {
                        continue;
                    }
                    if let Ok(stored) = serde_json::from_str::<StoredDataset>(value.value())
                        && !stored.deleted
                    {
                        freed_dataset_digests.push(stored.record.digest.as_str().to_owned());
                    }
                }
                datasets
                    .retain(|(t, _, _), _| t != tenant_str)
                    .map_err(io)?;
                freed_dataset_digests.sort_unstable();
                freed_dataset_digests.dedup();
                let mut dataset_actions = Vec::new();
                for digest in freed_dataset_digests {
                    if !Self::dataset_digest_referenced(datasets, &digest)? {
                        dataset_blobs.remove(digest.as_str()).map_err(io)?;
                        dataset_actions.push(EngineAction::UnspoolDataset { digest });
                    }
                }
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
                let mut actions = vec![
                    Self::sync_action(configs, datasets, spool_dir)?,
                    Self::sync_routes_action(routes)?,
                ];
                actions.extend(dataset_actions);
                Ok(Ok(actions))
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
            // Both rows, one transaction, one revision (issue #162). Every
            // arm in this match already runs inside the apply's single write
            // txn, so writing the principal and its binding here is atomic by
            // construction — that is precisely why this is one op rather than
            // the caller submitting two.
            ControlOp::PrincipalCreate {
                tenant,
                principal,
                role,
            } => {
                if let Err(reason) = Self::require_live_tenant(tenants, tenant.as_str())? {
                    return Ok(Err(reason));
                }
                let principal_value = serde_json::to_string(principal)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                let role_value = serde_json::to_string(role)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                principals
                    .insert(principal.id.as_str(), principal_value.as_str())
                    .map_err(io)?;
                bindings
                    .insert(
                        (principal.id.as_str(), tenant.as_str()),
                        role_value.as_str(),
                    )
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
                // The principal must exist. Before #162 this op was reachable
                // only through `RaftNode::submit`, so a binding naming nothing
                // was a fixture mistake; the admin surface now exposes it to
                // any tenant admin, where a mistyped id would durably and
                // replicatedly commit a binding with no principal behind it.
                //
                // That is not merely untidy: `tenant_principals` resolves every
                // binding to its principal to answer
                // `GET /admin/tenants/:id/principals`, and a row it cannot
                // resolve is committed-state corruption it reports as an error
                // — so one typo would permanently 500 the very listing an
                // operator would use to find and remove it.
                //
                // Enforced here, at the write, rather than tolerated at the
                // read: skipping an unresolvable row in the listing would hide
                // a binding that really does grant access, which is the worse
                // failure of the two.
                if principals.get(principal_id.as_str()).map_err(io)?.is_none() {
                    return Ok(Err(format!(
                        "unknown principal {:?}: a binding must name a principal that exists",
                        principal_id.as_str()
                    )));
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
            ControlOp::AuditSinkPut {
                uri,
                auth_ref,
                batch_max_rows,
                ..
            } => {
                let record = AuditSink {
                    // Trimmed here, once, so the stored record is canonical.
                    // `validate` trims before every one of its checks, so a
                    // pasted " https://…" is admitted — and `transport_for`
                    // does not trim, so storing it verbatim produced a sink
                    // that committed fleet-wide, read back as configured, and
                    // failed "unsupported scheme" on every pass forever.
                    uri: uri.trim().to_owned(),
                    auth_ref: auth_ref.clone(),
                    batch_max_rows: *batch_max_rows,
                    revision: index,
                };
                let value = serde_json::to_string(&record)
                    .map_err(|e| StorageError::from(StorageIOError::write_state_machine(&e)))?;
                audit_sink
                    .insert(AUDIT_SINK_KEY, value.as_str())
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::AuditSinkDelete { .. } => {
                // The checkpoint is deliberately left behind. Removing a sink
                // and re-declaring it must resume, not re-ship every retained
                // row to the customer's bucket a second time.
                audit_sink.remove(AUDIT_SINK_KEY).map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::AuditCheckpointPut { revision, .. } => {
                // Monotonic, and enforced here rather than at the submitter:
                // apply is the only place that sees every write in one order on
                // every replica. A leader deposed mid-batch can still have a
                // checkpoint in flight; committing it after the new leader has
                // moved ahead would rewind the stream and re-ship a window that
                // was already delivered. `max` makes that late write a no-op
                // instead — deterministically, on all three replicas.
                let current = audit_checkpoint
                    .get(AUDIT_SINK_KEY)
                    .map_err(io)?
                    .map_or(0, |v| v.value());
                if *revision > current {
                    audit_checkpoint
                        .insert(AUDIT_SINK_KEY, *revision)
                        .map_err(io)?;
                }
                Ok(Ok(Vec::new()))
            }
            ControlOp::SessionKeyPut { key, .. } => {
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
            ControlOp::FleetNamePut { name, .. } => {
                // Overwrites unconditionally, same reasoning as `SessionKeyPut` above: setting
                // the first name and renaming are one op, so the second write must replace the
                // first outright rather than accumulate — a fleet with two names is exactly the
                // confusion this feature exists to make impossible.
                fleet_name
                    .insert(FLEET_NAME_ROW, name.as_str())
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::JournalClearGen {
                tenant,
                port,
                space,
            } => {
                // Tenant-exists / port-ownership are deliberately not checked here — see
                // `validate`'s doc for this op. A clear is a convergence primitive, not a config
                // write: it must succeed even against a port nothing has configured yet, the
                // same way `ClusterJournal::set_clear_gen` creates the shard on first touch
                // rather than refusing an unknown one.
                let space_key = journal_gen_space_key(space.as_deref());
                let key = (tenant.as_str(), *port, space_key.as_str());
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
                tenant,
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
                    .get((tenant.as_str(), *port, sig_hash.as_str()))
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
                if configs.get((tenant.as_str(), *port)).map_err(io)?.is_none() {
                    return Ok(Err(format!("no imposter on port {port}")));
                }

                let Some(recorded) = stub else {
                    // Stub-less recording (no predicate generators): the row alone is the
                    // durable replay source `lookup()` answers from.
                    proxy_recorded
                        .insert(
                            (tenant.as_str(), *port, sig_hash.as_str()),
                            resp_json.as_str(),
                        )
                        .map_err(io)?;
                    return Ok(Ok(Vec::new()));
                };

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
                let quotas = match Self::quotas_for(tenants, tenant.as_str())? {
                    Ok(quotas) => quotas,
                    Err(reason) => return Ok(Err(reason)),
                };
                if config.stubs.len() > quotas.max_stubs_per_imposter as usize {
                    return Ok(Err(format!(
                        "tenant {:?} allows at most {} stubs per imposter; this recording would \
                         leave {}",
                        tenant.as_str(),
                        quotas.max_stubs_per_imposter,
                        config.stubs.len()
                    )));
                }
                record.config_json = serde_json::to_string(&config)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                record.revision = index;
                let value = serde_json::to_string(&record)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                configs
                    .insert((tenant.as_str(), *port), value.as_str())
                    .map_err(io)?;
                // Both facts land in this one apply transaction — the marker row is written
                // only alongside the stub mutation, so "recorded but stub-less" is
                // unrepresentable by construction (#226).
                proxy_recorded
                    .insert(
                        (tenant.as_str(), *port, sig_hash.as_str()),
                        resp_json.as_str(),
                    )
                    .map_err(io)?;
                Self::mark_drifted(sources, tenant.as_str(), record.source.as_ref(), index)?;
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
                    PlacedRecording::MergedAnonymous => {
                        Self::sync_action(configs, datasets, spool_dir)?
                    }
                };
                Ok(Ok(vec![action]))
            }
            ControlOp::ProxyRecordedClear { tenant, port } => {
                // Clearing an empty table is a no-op, not a failure — idempotent like every
                // delete here. Recorded *stubs* stay: they are imposter config, deleted
                // through the stub-edit surfaces (#226's documented split).
                proxy_recorded
                    .retain(|(t, p, _), _| !(t == tenant.as_str() && p == *port))
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::SpecPut {
                tenant,
                id,
                meta,
                document,
            } => {
                // Read before the row is overwritten: this is the digest the record is about to
                // stop naming, which is exactly what the GC pass below needs to check.
                let previous = Self::stored_spec(specs, tenant.as_str(), id)?;
                let stored = StoredSpec {
                    meta: meta.clone(),
                    revision: index,
                };
                let value = serde_json::to_string(&stored)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                specs
                    .insert((tenant.as_str(), id.as_str()), value.as_str())
                    .map_err(io)?;
                // Bytes still ride the log here until D-23 (#439) lands: `document` is the
                // in-log copy, and the blob store already holds the same bytes from the
                // pre-propose fan-out (#438).
                // Content-addressed (D-4): insert the blob only on its first reference. Re-putting
                // the same bytes under the same id, or under a different id or tenant, is then a
                // no-op here — `validate` already proved `meta.digest` is this document's own
                // sha256, so a hit means the bytes already stored are these bytes.
                if spec_blobs.get(meta.digest.as_str()).map_err(io)?.is_none() {
                    spec_blobs
                        .insert(meta.digest.as_str(), document.as_str())
                        .map_err(io)?;
                }
                // A digest change may have orphaned the old blob. Checked *after* the row above
                // is rewritten, so the reference scan sees the post-write truth — this record
                // now names the new digest, not the old one.
                if let Some(previous) = previous
                    && previous.meta.digest.as_str() != meta.digest.as_str()
                {
                    Self::gc_spec_blob_if_unreferenced(
                        spec_blobs,
                        specs,
                        previous.meta.digest.as_str(),
                    )?;
                }
                Ok(Ok(Vec::new()))
            }
            ControlOp::SpecDelete { tenant, id } => {
                let Some(stored) = Self::stored_spec(specs, tenant.as_str(), id)? else {
                    return Ok(Err(format!("no spec {id:?}")));
                };
                let bound_ports = Self::ports_of_spec(configs, tenant.as_str(), id)?;
                if !bound_ports.is_empty() {
                    let ports = bound_ports
                        .iter()
                        .map(u16::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Ok(Err(format!(
                        "spec {id:?} is bound to port(s) {ports}; DELETE with ?force to unbind \
                         first"
                    )));
                }
                specs.remove((tenant.as_str(), id.as_str())).map_err(io)?;
                Self::gc_spec_blob_if_unreferenced(spec_blobs, specs, stored.meta.digest.as_str())?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::SpecBind { tenant, id, port } => {
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
                let Some(spec) = Self::stored_spec(specs, tenant.as_str(), id)? else {
                    return Ok(Err(format!("no spec {id:?}")));
                };
                // The drift baseline: a bind (first deploy, or a redeploy after drift) declares
                // this the last-known-good state, so whatever drifted before is resolved.
                record.spec = Some(SpecProvenance {
                    spec_id: id.clone(),
                    digest: spec.meta.digest.clone(),
                });
                record.drifted = false;
                record.revision = index;
                let value = serde_json::to_string(&record)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                configs
                    .insert((tenant.as_str(), *port), value.as_str())
                    .map_err(io)?;
                // Config bytes are unchanged — a bind only stamps provenance — so there is
                // nothing for the engine to do.
                Ok(Ok(Vec::new()))
            }
            ControlOp::SpecUnbind { tenant, port } => {
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
                record.spec = None;
                record.drifted = false;
                record.revision = index;
                let value = serde_json::to_string(&record)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                configs
                    .insert((tenant.as_str(), *port), value.as_str())
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::DatasetPut {
                tenant,
                record,
                csv,
            } => {
                let quotas = match Self::quotas_for(tenants, tenant.as_str())? {
                    Ok(quotas) => quotas,
                    Err(reason) => return Ok(Err(reason)),
                };
                if record.bytes > quotas.max_dataset_bytes {
                    return Ok(Err(format!(
                        "dataset {:?} is {} bytes; tenant {:?} allows at most {} per dataset",
                        record.name,
                        record.bytes,
                        tenant.as_str(),
                        quotas.max_dataset_bytes
                    )));
                }
                let (live_names_excluding, live_bytes) =
                    Self::dataset_usage(datasets, tenant.as_str(), &record.name)?;
                if live_names_excluding >= quotas.max_datasets {
                    return Ok(Err(format!(
                        "tenant {:?} is at its ceiling of {} datasets",
                        tenant.as_str(),
                        quotas.max_datasets
                    )));
                }
                let would_hold = live_bytes.saturating_add(record.bytes);
                if would_hold > quotas.max_dataset_total_bytes {
                    return Ok(Err(format!(
                        "tenant {:?} would hold {would_hold} dataset bytes; its ceiling is {}",
                        tenant.as_str(),
                        quotas.max_dataset_total_bytes
                    )));
                }

                // Version = one past the highest version this name has ever held, tombstones
                // included: a delete must never free up a version number a stale client
                // reference could still name.
                let existing = Self::dataset_rows(datasets, tenant.as_str(), &record.name)?;
                let version = existing
                    .iter()
                    .map(|(v, _)| *v)
                    .max()
                    .map_or(1, |max| max + 1);

                // The spool file is written before the row that names it, inside this same
                // transaction: a reader that observes the committed row must be able to find
                // the bytes it names immediately, never in a window where the row exists but
                // the file does not. An I/O failure here is a real storage error (propagated,
                // like a redb failure) rather than a committed refusal — a refusal here would
                // let replicas silently diverge on whether the put "worked".
                if let Some(dir) = spool_dir {
                    write_spool(dir, record.digest.as_str(), csv)
                        .map_err(|e| StorageError::from(StorageIOError::write_state_machine(&e)))?;
                }
                // Bytes still ride the log here until D-23 (#439) lands: `csv` is the in-log
                // copy, and the blob store already holds the same bytes from the pre-propose
                // fan-out (#438).
                // Content-addressed (D-4): insert the blob only on its first reference, exactly
                // like `SpecPut` — `validate` already proved `record.digest` is `csv`'s own sha256.
                if dataset_blobs
                    .get(record.digest.as_str())
                    .map_err(io)?
                    .is_none()
                {
                    dataset_blobs
                        .insert(record.digest.as_str(), csv.as_str())
                        .map_err(io)?;
                }
                let stored = StoredDataset {
                    record: record.clone(),
                    version,
                    created_at_secs: issued_at_secs,
                    revision: index,
                    deleted: false,
                };
                let value = serde_json::to_string(&stored)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?;
                datasets
                    .insert(
                        (tenant.as_str(), record.name.as_str(), version),
                        value.as_str(),
                    )
                    .map_err(io)?;
                Ok(Ok(Vec::new()))
            }
            ControlOp::DatasetDelete { tenant, name } => {
                let rows = Self::dataset_rows(datasets, tenant.as_str(), name)?;
                if rows.iter().all(|(_, stored)| stored.deleted) {
                    return Ok(Err(format!("no dataset {name:?}")));
                }
                let bound_ports = match Self::ports_binding_dataset(configs, tenant.as_str(), name)?
                {
                    Ok(ports) => ports,
                    Err(reason) => return Ok(Err(reason)),
                };
                if !bound_ports.is_empty() {
                    let ports = bound_ports
                        .iter()
                        .map(u16::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    // "bound **to** port(s)", matching the spec refusal above word for word: the front maps
                    // that phrasing to `409`, and two spellings of the same fact would mean two
                    // status rules that can drift apart (issue #287 renders this one).
                    return Ok(Err(format!("dataset {name:?} is bound to port(s) {ports}")));
                }
                // Tombstone every live version — a delete addresses the whole name, not one
                // version, and D2's binding is by name (RFC-005 §3.2).
                let mut freed_digests = Vec::new();
                for (version, mut stored) in rows {
                    if stored.deleted {
                        continue;
                    }
                    freed_digests.push(stored.record.digest.as_str().to_owned());
                    stored.deleted = true;
                    stored.revision = index;
                    let value = serde_json::to_string(&stored)
                        .map_err(|e| StorageIOError::write_state_machine(&e))?;
                    datasets
                        .insert((tenant.as_str(), name.as_str(), version), value.as_str())
                        .map_err(io)?;
                }
                freed_digests.sort_unstable();
                freed_digests.dedup();
                let mut actions = Vec::new();
                for digest in freed_digests {
                    // Checked *after* every tombstone write above, so this scan sees the
                    // post-write truth — nothing this delete just tombstoned still counts.
                    if !Self::dataset_digest_referenced(datasets, &digest)? {
                        dataset_blobs.remove(digest.as_str()).map_err(io)?;
                        actions.push(EngineAction::UnspoolDataset { digest });
                    }
                }
                Ok(Ok(actions))
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
            // it replaces: wrong attribution in an audit-adjacent stream is not
            // a smaller error than missing attribution.
            rift_cluster_base::seams::with_principal_scope(principal, async {
                self.drive_one(action).await;
            })
            .await;
        }
        if self.engine.is_some() {
            crate::metrics::observe_apply_failures(&self.apply_failures.lock());
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
                    Ok(report) => self.record_report(&report, &desired_ports),
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
            EngineAction::UnspoolDataset { digest } => {
                let Some(path) = self.spool_path(&digest) else {
                    return;
                };
                // Re-check the *committed* blob table before touching the file. `apply` runs a
                // whole batch of entries in one transaction and drives the actions only after it
                // commits, so a `DatasetDelete` and a `DatasetPut` of the same bytes in one batch
                // would otherwise queue an unspool that lands after the put re-created the
                // reference — deleting a live dataset's file with nothing left to repair it. The
                // table is the truth; the action is only a hint that a removal *may* be due.
                match self.dataset_blob_present(&digest) {
                    Ok(false) => {}
                    Ok(true) => return,
                    Err(e) => {
                        tracing::error!(
                            digest = %digest,
                            error = %e,
                            "could not re-check the dataset blob table; keeping the spool file"
                        );
                        return;
                    }
                }
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    // Already gone is success, not failure — the outcome this call wants
                    // (no file at `path`) already holds.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        tracing::error!(
                            digest = %digest,
                            path = %path.display(),
                            error = %e,
                            "failed to remove an unreferenced dataset spool file"
                        );
                    }
                }
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
            routes_revisions,
            sources,
            specs,
            spec_blobs,
            datasets,
            dataset_blobs,
            tenants,
            principals,
            bindings,
            audit,
            audit_sink,
            audit_checkpoint,
            audit_gc_watermark,
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
            // Travels with the routes themselves (issue #210). Omitting it
            // would not lose data, but it would silently reset every tenant's
            // table to revision 0 on the joining node — see the field's doc.
            let routes_revision_table = read_txn
                .open_table(SM_ROUTES_REVISION_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut routes_revisions = Vec::new();
            for item in routes_revision_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                routes_revisions.push((key.value().to_owned(), value.value()));
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
            // Travels with the snapshot for the #134/#137 reason every table above does: a node
            // joining by snapshot must come back holding the same specs and blobs as its peers.
            let specs_table = read_txn
                .open_table(SM_SPECS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut specs = Vec::new();
            for item in specs_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let (tenant, id) = key.value();
                specs.push((tenant.to_owned(), id.to_owned(), value.value().to_owned()));
            }
            let spec_blobs_table = read_txn
                .open_table(SM_SPEC_BLOBS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut spec_blobs = Vec::new();
            for item in spec_blobs_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                spec_blobs.push((key.value().to_owned(), value.value().to_owned()));
            }
            // Travels with the snapshot for the identical #134/#137 reason `specs`/`spec_blobs`
            // do (RFC-005 D1, #285): a node joining by snapshot must come back holding the same
            // datasets and blobs as its peers.
            let datasets_table = read_txn
                .open_table(SM_DATASETS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut datasets = Vec::new();
            for item in datasets_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let (tenant, name, version) = key.value();
                datasets.push((
                    tenant.to_owned(),
                    name.to_owned(),
                    version,
                    value.value().to_owned(),
                ));
            }
            let dataset_blobs_table = read_txn
                .open_table(SM_DATASET_BLOBS_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut dataset_blobs = Vec::new();
            for item in dataset_blobs_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                dataset_blobs.push((key.value().to_owned(), value.value().to_owned()));
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
            let audit_table = read_txn
                .open_table(SM_AUDIT_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let mut audit = Vec::new();
            for item in audit_table
                .iter()
                .map_err(|e| StorageIOError::read_state_machine(&e))?
            {
                let (key, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
                let (revision, op_id) = key.value();
                audit.push((revision, op_id.to_owned(), value.value().to_owned()));
            }
            // The sink and its checkpoint travel too, for the same reason and
            // with a sharper failure: a follower that installs without them and
            // then wins an election either stops exporting (no sink) or resumes
            // from zero and re-ships the whole retained history to the
            // customer's bucket (no checkpoint).
            let audit_sink = read_txn
                .open_table(SM_AUDIT_SINK_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .get(AUDIT_SINK_KEY)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .map(|v| v.value().to_owned());
            let audit_checkpoint = read_txn
                .open_table(SM_AUDIT_CHECKPOINT_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .get(AUDIT_SINK_KEY)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .map(|v| v.value());
            let audit_gc_watermark = read_txn
                .open_table(SM_AUDIT_GC_WATERMARK_TABLE)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .get(AUDIT_SINK_KEY)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
                .map(|v| v.value());
            // Travels with the snapshot for the same reason the sink does, with a sharper
            // failure than a missed export: a follower that installs without it and then wins an
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
                let (tenant, port, space_key) = key.value();
                journal_gens.push((
                    tenant.to_owned(),
                    port,
                    decode_journal_gen_space_key(space_key),
                    value.value(),
                ));
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
                let (tenant, port, sig_hash) = key.value();
                proxy_recorded.push((
                    tenant.to_owned(),
                    port,
                    sig_hash.to_owned(),
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
                configs,
                routes,
                routes_revisions,
                sources,
                specs,
                spec_blobs,
                datasets,
                dataset_blobs,
                tenants,
                principals,
                bindings,
                audit,
                audit_sink,
                audit_checkpoint,
                audit_gc_watermark,
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
            routes_revisions,
            sources,
            specs,
            spec_blobs,
            datasets,
            dataset_blobs,
            tenants,
            principals,
            bindings,
            audit,
            audit_sink,
            audit_checkpoint,
            audit_gc_watermark,
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
            let mut sources = write_txn
                .open_table(SM_SOURCES_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut specs = write_txn
                .open_table(SM_SPECS_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut spec_blobs = write_txn
                .open_table(SM_SPEC_BLOBS_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut datasets = write_txn
                .open_table(SM_DATASETS_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut dataset_blobs = write_txn
                .open_table(SM_DATASET_BLOBS_TABLE)
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
            let mut audit = write_txn
                .open_table(SM_AUDIT_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut audit_sink = write_txn
                .open_table(SM_AUDIT_SINK_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut audit_checkpoint = write_txn
                .open_table(SM_AUDIT_CHECKPOINT_TABLE)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let mut audit_gc_watermark = write_txn
                .open_table(SM_AUDIT_GC_WATERMARK_TABLE)
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
            // Audit retention runs on the same replicated clock, for a reason
            // that is the same in kind and worse in consequence: two replicas
            // disagreeing about which rows have expired would leave their audit
            // tables permanently different, and the whole claim of this feature
            // is that any node can answer because every node holds the same
            // rows. A node whose local wall clock is a week fast must GC
            // exactly what its peers do — so the local clock is never read.
            let gc_removed_through = Self::gc_audit(
                &mut audit,
                applied.logical_clock_secs,
                self.audit_retention_secs,
            )
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
            if let Some(removed_through) = gc_removed_through {
                // Monotonic, like the export checkpoint: the watermark records
                // how far retention has ever reached, so it can only advance.
                let current = audit_gc_watermark
                    .get(AUDIT_SINK_KEY)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?
                    .map_or(0, |v| v.value());
                if removed_through > current {
                    audit_gc_watermark
                        .insert(AUDIT_SINK_KEY, removed_through)
                        .map_err(|e| StorageIOError::write_state_machine(&e))?;
                }
            }

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
                                        &mut sources,
                                        &mut specs,
                                        &mut spec_blobs,
                                        &mut datasets,
                                        &mut dataset_blobs,
                                        &mut tenants,
                                        &mut principals,
                                        &mut bindings,
                                        &mut audit_sink,
                                        &mut audit_checkpoint,
                                        &mut session_key,
                                        &mut fleet_name,
                                        &mut journal_gens,
                                        &mut proxy_recorded,
                                        journal.as_deref(),
                                        self.spool_dir.as_deref(),
                                        &request.op,
                                        log_id.index,
                                        applied.logical_clock_secs,
                                    )?,
                                },
                                None => Self::mutate_tables(
                                    &mut configs,
                                    &mut routes,
                                    &mut routes_revision,
                                    &mut sources,
                                    &mut specs,
                                    &mut spec_blobs,
                                    &mut datasets,
                                    &mut dataset_blobs,
                                    &mut tenants,
                                    &mut principals,
                                    &mut bindings,
                                    &mut audit_sink,
                                    &mut audit_checkpoint,
                                    &mut session_key,
                                    &mut fleet_name,
                                    &mut journal_gens,
                                    &mut proxy_recorded,
                                    journal.as_deref(),
                                    self.spool_dir.as_deref(),
                                    &request.op,
                                    log_id.index,
                                    applied.logical_clock_secs,
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

                        // The audit projection (RFC-002 §9, issue #163).
                        //
                        // Here, and only here. Every replica runs this same
                        // arm for the same committed entry with the same
                        // inputs, so all three derive a byte-identical row —
                        // which is what lets `GET /admin/audit` answer from
                        // local state with no fan-out.
                        //
                        // Below the dedup short-circuit above, deliberately: a
                        // replayed `op_id` returns the original response and
                        // changes nothing, so it must not append a second row
                        // for the same write. "Exactly once per write" is an
                        // acceptance criterion, and this ordering is what
                        // provides it.
                        //
                        // Refusals are recorded too. A `Failed` outcome is a
                        // committed decision, and "who tried to do what and was
                        // refused" is the half of an audit log that matters
                        // most.
                        //
                        // An op with no action slug opts out (today: only the
                        // exporter's own checkpoint, whose audit row would feed
                        // the exporter that wrote it). The opt-out is a `None`
                        // arm someone had to write, not a missing branch.
                        if let Some(action) = request.op.audit_action() {
                            let audit_row = AuditRow {
                                ts_secs: request.issued_at_secs,
                                principal: request.principal.clone(),
                                tenant: request.op.tenant().clone(),
                                action: action.to_owned(),
                                resource: request.op.audit_resource(),
                                op_id: request.op_id,
                                revision: log_id.index,
                                outcome: response.outcome.clone(),
                            };
                            let audit_value = serde_json::to_string(&audit_row)
                                .map_err(|e| StorageIOError::write_state_machine(&e))?;
                            audit
                                .insert((log_id.index, op_key.as_str()), audit_value.as_str())
                                .map_err(|e| StorageIOError::write_state_machine(&e))?;
                        }

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

        // Everything from the rewind to the durable commit is one contiguous synchronous block —
        // parse, full table clear-and-reinsert, `Durability::Immediate` commit. Off the runtime for
        // #444's reason; the engine drive that used to close this function is returned instead and
        // awaited by the caller, which keeps the documented order (durable write first, engine
        // after) true by construction rather than by comment.
        let sm = self.clone();
        let meta_owned = meta.clone();
        let actions =
            tokio::task::spawn_blocking(move || sm.install_snapshot_blocking(meta_owned, received))
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
    /// [`RaftStateMachine::install_snapshot`]'s synchronous middle, off the runtime.
    ///
    /// Returns the engine actions rather than driving them: driving is the one `.await` in the
    /// original body, and returning it is what lets the whole rest of the install run on the
    /// blocking pool without changing the order those actions are applied in.
    #[allow(clippy::result_large_err)]
    fn install_snapshot_blocking(
        &self,
        meta: SnapshotMeta<u64, BasicNode>,
        mut received: std::fs::File,
    ) -> StorageResult<Vec<AttributedAction>> {
        use std::io::Seek as _;
        let meta = &meta;
        // Rewind before reading. openraft streams the transfer in by writing chunk after chunk into
        // this handle, so it arrives positioned at EOF — parsing from where it sits reads nothing
        // and the install fails with an empty-input error, which looks exactly like a peer that
        // sent a corrupt snapshot. The in-process tests never saw it: their handles come straight
        // from `build_snapshot` or a freshly opened file, both already at 0.
        received
            .seek(std::io::SeekFrom::Start(0))
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        // `try_clone` shares the file *offset* on Unix, so parsing through the clone advances this
        // handle too — which is why the copy below must seek back to 0 rather than assuming it is
        // still where the rewind above left it.
        let received_for_parse = received
            .try_clone()
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let payload: SnapshotPayload =
            serde_json::from_reader(std::io::BufReader::new(received_for_parse)).map_err(|e| {
                StorageError::from(StorageIOError::read_snapshot(Some(meta.signature()), &e))
            })?;
        // Counted here — after the payload parses, before it is applied — so the metric means "a
        // peer's snapshot really arrived and was readable", which is what a scenario asserting the
        // wire path needs to distinguish from catch-up-by-replication (issue #183).
        crate::metrics::snapshot_installed();

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
            for (tenant, port, value) in &payload.configs {
                configs_table
                    .insert((tenant.as_str(), *port), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }
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

            // Cleared before repopulating, like every table above: a payload
            // carrying no revision for a tenant means the fleet holds none, and
            // leaving this node's stale row would let a token this node minted
            // before the install keep passing against a table it no longer has.
            let mut routes_revision_table = write_txn
                .open_table(SM_ROUTES_REVISION_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            routes_revision_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (tenant, revision) in &payload.routes_revisions {
                routes_revision_table
                    .insert(tenant.as_str(), *revision)
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

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

            // Cleared before repopulating, like every table above: a payload carrying no specs
            // means the fleet holds none, and leaving this node's stale rows in place would let
            // a spec (or a binding to one) the fleet has since deleted keep answering here.
            let mut specs_table = write_txn
                .open_table(SM_SPECS_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            specs_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (tenant, id, value) in &payload.specs {
                specs_table
                    .insert((tenant.as_str(), id.as_str()), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            let mut spec_blobs_table = write_txn
                .open_table(SM_SPEC_BLOBS_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            spec_blobs_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (digest, document) in &payload.spec_blobs {
                spec_blobs_table
                    .insert(digest.as_str(), document.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            // Cleared before repopulating, like `specs` above (RFC-005 D1, #285): a payload
            // carrying no datasets means the fleet holds none, and leaving this node's stale
            // rows in place would let a dataset (or a binding to one) the fleet has since
            // deleted keep answering here.
            let mut datasets_table = write_txn
                .open_table(SM_DATASETS_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            datasets_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (tenant, name, version, value) in &payload.datasets {
                datasets_table
                    .insert((tenant.as_str(), name.as_str(), *version), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            // Computed *after* `datasets_table` is repopulated, not alongside the configs above:
            // compiling a `_rift.dataset` binding reads this table, and against the emptied one it
            // would refuse every bound imposter on the node — turning a snapshot install into a
            // fleet-wide outage for exactly the configs the snapshot was carrying.
            let config_action = match Self::desired_configs(
                &configs_table,
                &datasets_table,
                self.spool_dir.as_deref(),
            )
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?
            {
                Ok(desired) => EngineAction::Sync(desired),
                Err((port, error)) => EngineAction::RefuseSync { port, error },
            };

            let mut dataset_blobs_table = write_txn
                .open_table(SM_DATASET_BLOBS_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            dataset_blobs_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (digest, csv) in &payload.dataset_blobs {
                dataset_blobs_table
                    .insert(digest.as_str(), csv.as_str())
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

            // The audit table travels with the snapshot, like every other
            // replicated table. A node joining by snapshot install must come
            // back holding the same history as its peers — omitting this is how
            // an audit stream silently loses everything before the join, with
            // nothing reporting a gap (#134/#137 taught this crate the same
            // lesson about sources).
            let mut audit_table = write_txn
                .open_table(SM_AUDIT_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            audit_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            for (revision, op_id, value) in &payload.audit {
                audit_table
                    .insert((*revision, op_id.as_str()), value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            // Cleared before it is repopulated, exactly like the tables above:
            // a payload carrying no sink means the fleet has no sink, and
            // leaving this node's stale one in place would have it keep
            // shipping to an endpoint the fleet has retired.
            let mut audit_sink_table = write_txn
                .open_table(SM_AUDIT_SINK_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            audit_sink_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            if let Some(value) = &payload.audit_sink {
                audit_sink_table
                    .insert(AUDIT_SINK_KEY, value.as_str())
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            let mut audit_checkpoint_table = write_txn
                .open_table(SM_AUDIT_CHECKPOINT_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            audit_checkpoint_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            if let Some(revision) = payload.audit_checkpoint {
                audit_checkpoint_table
                    .insert(AUDIT_SINK_KEY, revision)
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            let mut audit_gc_watermark_table = write_txn
                .open_table(SM_AUDIT_GC_WATERMARK_TABLE)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            audit_gc_watermark_table
                .retain(|_, _| false)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            if let Some(revision) = payload.audit_gc_watermark {
                audit_gc_watermark_table
                    .insert(AUDIT_SINK_KEY, revision)
                    .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
            }

            // Cleared before it is repopulated, like the sink above: a payload carrying no key
            // means no console login has ever minted one on the leader, and leaving a stale local
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
            for (tenant, port, space, generation) in &payload.journal_gens {
                let space_key = journal_gen_space_key(space.as_deref());
                journal_gens_table
                    .insert((tenant.as_str(), *port, space_key.as_str()), *generation)
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
            for (tenant, port, sig_hash, resp) in &payload.proxy_recorded {
                proxy_recorded_table
                    .insert((tenant.as_str(), *port, sig_hash.as_str()), resp.as_str())
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

        // Materialise every dataset blob's spool file (RFC-005 D1, #285), after the durable
        // table write: a node that joins by snapshot must be able to serve a lookup against a
        // bound dataset immediately, not after a separate catch-up step, and `write_spool`
        // already skips a digest whose final file is present — so this is a no-op for every
        // blob this node already had. A no-op with no `spool_dir` attached, like the engine
        // drive below.
        if let Some(dir) = &self.spool_dir {
            for (digest, csv) in &payload.dataset_blobs {
                write_spool(dir, digest, csv).map_err(|e| {
                    StorageError::from(StorageIOError::write_snapshot(Some(meta.signature()), &e))
                })?;
            }
        }

        // Pushed into the local journal after the durable write, like the engine/routes
        // convergence just below — and for the identical #134/#137 reason `journal_gens_table`
        // above is cleared-then-repopulated rather than merged: a node joining (or rejoining
        // after a partition) by snapshot must come back agreeing with the fleet on every
        // generation, not just the ones its own log had already applied. A missing/dropped
        // handle is the same benign no-op it is in `mutate_tables`'s `JournalClearGen` arm — the
        // generations are durable in `sm_journal_gens` regardless of whether anything is
        // listening on the other end right now.
        if let Some(journal) = self.journal.get().and_then(Weak::upgrade) {
            // `ClusterJournal` itself is tenant-oblivious (it always has been — see its own
            // module doc: entries key on `(node_id, seq, clear_gen)`, nothing tenant-shaped), so
            // only `port`/`space`/`generation` travel across this boundary; `tenant` stays behind
            // in `sm_journal_gens`, which is the source of truth `journal_gen` reads back from.
            //
            // `reset_clear_gen`, not `set_clear_gen` (Non-blocker 1): the durable table was just
            // cleared and reinserted from this exact payload, unconditionally, because a
            // generation this node still held could be higher than what the fleet now agrees on
            // — `set_clear_gen`'s `fetch_max` cannot lower to match, and leaving it high would
            // let one node's forgotten-but-not-really clear silently win the fleet-wide max a
            // merge computes, dropping every other node's entries.
            for (_tenant, port, space, generation) in &payload.journal_gens {
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

    use super::{SESSION_KEY_ROW, SM_PRINCIPALS_TABLE, SM_SESSION_KEY_TABLE};
    use crate::control::hash_api_key;
    use rift_cluster_base::seams::{
        CompiledRoutes, ImposterConfig, ImposterManager, RecordedRequest, RequestJournal,
        ResponseMode, Route, RouteMatch, RouteTable, RouteTarget,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::{
        DEDUP_TTL_SECS, DatasetSummary, DedupEntry, PullOutcome, RedbLogStore, RedbStateMachine,
        SM_CONFIGS_TABLE, SM_DATASETS_TABLE, SM_DEDUP_TABLE, SM_SPECS_TABLE, SM_TENANTS_TABLE,
        SourceRecord, SourceRow, SpecRecord, StoredDataset, StoredImposter, StoredSpec, new,
    };
    use crate::control::{
        AUDIT_RESOURCE_ALL, AuditRow, AuthSource, ControlOp, ControlOutcome, ControlRequest,
        ControlResponse, DEFAULT_TENANT, DatasetRecord, Digest, FLEET_SCOPE, OnDrift, Principal,
        PrincipalId, Quotas, Role, SourceMode, SpecFormat, SpecMeta, SpecSource, StubEdit,
        StubEditScript, TenantId,
    };
    use crate::raft::TypeConfig;
    use crate::stores::journal::ClusterJournal;

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

    /// Write one `sm_specs` row directly, so a test can build an exact reference
    /// set (including a deliberately corrupt row) without driving apply.
    fn seed_spec(sm: &RedbStateMachine, tenant: &str, id: &str, value: &str) {
        let txn = sm.db.begin_write().expect("begin write");
        {
            let mut t = txn.open_table(SM_SPECS_TABLE).expect("open sm_specs");
            t.insert((tenant, id), value).expect("insert spec");
        }
        txn.commit().expect("commit");
    }

    fn seed_dataset(sm: &RedbStateMachine, tenant: &str, name: &str, version: u64, value: &str) {
        let txn = sm.db.begin_write().expect("begin write");
        {
            let mut t = txn.open_table(SM_DATASETS_TABLE).expect("open sm_datasets");
            t.insert((tenant, name, version), value)
                .expect("insert dataset");
        }
        txn.commit().expect("commit");
    }

    fn spec_json(digest: &str) -> String {
        serde_json::to_string(&StoredSpec {
            meta: SpecMeta {
                format: SpecFormat::Json,
                digest: Digest::new(digest.to_owned()),
                source: SpecSource::Inline,
            },
            revision: 1,
        })
        .expect("encode spec")
    }

    fn dataset_json(name: &str, digest: &str, deleted: bool) -> String {
        serde_json::to_string(&StoredDataset {
            record: DatasetRecord {
                name: name.to_owned(),
                digest: Digest::new(digest.to_owned()),
                key_columns: vec!["id".to_owned()],
                delimiter: ',',
                columns: vec!["id".to_owned()],
                rows: 1,
                bytes: 4,
            },
            version: 1,
            created_at_secs: 0,
            revision: 1,
            deleted,
        })
        .expect("encode dataset")
    }

    /// #437: what the blob GC actually asks in production. Every `BlobStore::gc`
    /// unit test passes a hand-built set, so without this nothing proves the
    /// real answer is right — and a wrong one here reclaims a referenced blob,
    /// which costs the dataset rather than the disk.
    #[tokio::test]
    async fn referenced_digests_unions_specs_and_live_datasets_only() {
        let (_td, sm) = fresh_sm(None).await;
        let spec_digest = "a".repeat(64);
        let live_digest = "b".repeat(64);
        let shared_digest = "c".repeat(64);
        let deleted_digest = "d".repeat(64);

        seed_spec(&sm, "acme", "s1", &spec_json(&spec_digest));
        // The same digest named by both a spec and a dataset must appear once.
        seed_spec(&sm, "acme", "s2", &spec_json(&shared_digest));
        seed_dataset(
            &sm,
            "acme",
            "shared",
            1,
            &dataset_json("shared", &shared_digest, false),
        );
        seed_dataset(
            &sm,
            "acme",
            "live",
            1,
            &dataset_json("live", &live_digest, false),
        );
        // A tombstoned version no longer entitles its digest to a file.
        seed_dataset(
            &sm,
            "acme",
            "gone",
            1,
            &dataset_json("gone", &deleted_digest, true),
        );

        let referenced = sm.referenced_digests().expect("scan");

        let expected: std::collections::HashSet<String> = [spec_digest, live_digest, shared_digest]
            .into_iter()
            .collect();
        assert_eq!(referenced, expected);
    }

    /// #437 edge 15. A scan that cannot read a row must FAIL, not quietly drop
    /// it: the caller reclaims everything the returned set does not mention, so
    /// a dropped row is a deleted blob. Fail-closed, like
    /// `dataset_digest_referenced`'s own unparseable-row rule.
    #[tokio::test]
    async fn referenced_digests_fails_closed_on_a_row_it_cannot_parse() {
        let (_td, sm) = fresh_sm(None).await;
        let good = "e".repeat(64);
        seed_dataset(&sm, "acme", "good", 1, &dataset_json("good", &good, false));
        seed_dataset(&sm, "acme", "corrupt", 1, "{ this is not a dataset");

        let scanned = sm.referenced_digests();
        assert!(
            scanned.is_err(),
            "an unparseable row must fail the scan, not silently shrink the referenced set"
        );
    }

    /// The same rule for a corrupt spec row.
    #[tokio::test]
    async fn referenced_digests_fails_closed_on_a_corrupt_spec_row() {
        let (_td, sm) = fresh_sm(None).await;
        seed_spec(&sm, "acme", "s1", "not json at all");
        assert!(sm.referenced_digests().is_err());
    }

    /// An empty state machine references nothing — the honest answer, and the
    /// one the GC tick is allowed to act on.
    #[tokio::test]
    async fn referenced_digests_of_an_empty_state_machine_is_empty() {
        let (_td, sm) = fresh_sm(None).await;
        assert!(sm.referenced_digests().expect("scan").is_empty());
    }

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
            .read_config(DEFAULT_TENANT, port)
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

    /// Every row's identity, readable whether or not its value decoded — which
    /// is the property that lets the scheduler hold a corrupt source's poller
    /// instead of losing track of it (#243).
    fn tenant_and_id(all: &[SourceRow]) -> Vec<(&str, &str)> {
        all.iter()
            .map(|row| (row.tenant.as_str(), row.id.as_str()))
            .collect()
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
        sm.source(DEFAULT_TENANT, id)
            .expect("read source")
            .expect("source present")
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
        assert_eq!(sm.sources(DEFAULT_TENANT).expect("list").len(), 1);

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
        assert!(sm.source(DEFAULT_TENANT, "mocks").expect("read").is_none());
        assert!(sm.sources(DEFAULT_TENANT).expect("list").is_empty());
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
        for (tenant, port, source) in provenance {
            assert_eq!(tenant, TenantId::default());
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
        assert!(
            sm.read_config(DEFAULT_TENANT, 8080)
                .expect("read")
                .is_none()
        );
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
        let sibling_before = sm
            .read_config(DEFAULT_TENANT, 8081)
            .expect("read")
            .expect("present");

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
            sm.read_config(DEFAULT_TENANT, 8081)
                .expect("read")
                .is_none(),
            "a port the document dropped is removed from the source's set"
        );
        assert!(
            sm.read_config(DEFAULT_TENANT, 9000)
                .expect("read")
                .is_some(),
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
        let untouched_before = sm
            .read_config(DEFAULT_TENANT, 8081)
            .expect("read")
            .expect("present");

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
            sm.read_config(DEFAULT_TENANT, 8081)
                .expect("read")
                .expect("present"),
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
    ///
    /// Pins D-29: deleting a source orphans its imposters — the config survives
    /// the delete and only its provenance is cleared; a cascade would have
    /// removed it.
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
            sm.read_config(DEFAULT_TENANT, 8080)
                .expect("read")
                .is_some(),
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
            .source(DEFAULT_TENANT, "mocks")
            .expect("read source")
            .expect("source survived the snapshot");
        assert_eq!(record.uri, "https://h/i.json");
        assert_eq!(record.on_drift, OnDrift::Skip);
        assert_eq!(record.last_version.as_deref(), Some("v7"));
        assert!(record.drifted, "the drift verdict must survive too");
        assert_eq!(record.ports, vec![8080]);

        let provenance = restored.config_provenance().expect("provenance");
        assert_eq!(provenance.len(), 1);
        assert_eq!(provenance[0].0, TenantId::default());
        assert_eq!(provenance[0].1, 8080);
        assert_eq!(provenance[0].2.id, "mocks");
        assert_eq!(provenance[0].2.version.as_deref(), Some("v7"));
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
        assert!(sm.sources(DEFAULT_TENANT).expect("list").is_empty());
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
        let body = sm
            .read_config(DEFAULT_TENANT, 8080)
            .expect("read")
            .expect("present");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("parses");
        assert_eq!(parsed["port"], 8080);
        assert_eq!(
            sm.configured_ports().expect("ports"),
            vec![(TenantId::default(), 8080)]
        );
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
        assert_eq!(
            sm.read_config(DEFAULT_TENANT, 1).expect("read"),
            None,
            "nothing mutated"
        );
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
            sm.read_config(DEFAULT_TENANT, port)
                .expect("read")
                .is_some(),
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
        assert_eq!(
            sm.configured_ports().expect("ports"),
            vec![(TenantId::default(), 18085)]
        );

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
            follower
                .read_config(DEFAULT_TENANT, 8080)
                .expect("read")
                .is_some(),
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

        let body = sm
            .read_config(DEFAULT_TENANT, 18090)
            .expect("read")
            .expect("present");
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

    // -- issue #241: the poll scheduler's whole-table view ---------------------

    /// The scheduler drives off one scan of the source table rather than a
    /// per-tenant read looped over a tenant list — that list carries tombstones
    /// and omits the always-present implicit default tenant, and N tenants
    /// would mean N full scans of the same table.
    #[tokio::test]
    async fn sources_all_spans_every_tenant_in_key_order() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(
            &mut sm,
            2,
            source_put_in(2, "acme", "mocks", "https://acme/mocks.json"),
        )
        .await;
        apply_one(
            &mut sm,
            3,
            source_put_in(3, DEFAULT_TENANT, "mocks", "https://default/mocks.json"),
        )
        .await;
        apply_one(
            &mut sm,
            4,
            source_put_in(4, "acme", "billing", "https://acme/billing.json"),
        )
        .await;

        // A corrupt row must not displace the rest of the scan, nor its own
        // place in it: key order is the table's, and the key is readable even
        // when the value is not (#243).
        sm.inject_raw_source("acme", "billing", "{not json");

        let all = sm.sources_all().expect("read every tenant's sources");
        assert_eq!(
            tenant_and_id(&all),
            vec![
                ("acme", "billing"),
                ("acme", "mocks"),
                (DEFAULT_TENANT, "mocks"),
            ],
            "(tenant, id)-ascending, which is the table's own key order"
        );

        // A source id is unique only within its tenant, so the same name in two
        // tenants must come back as two records — not one shadowing the other.
        assert_eq!(
            all.iter()
                .filter_map(|row| row.record.as_ref().ok())
                .filter(|record| record.id == "mocks")
                .map(|record| record.uri.as_str())
                .collect::<Vec<_>>(),
            vec!["https://acme/mocks.json", "https://default/mocks.json"]
        );
    }

    /// Port provenance must not cross tenants either. Both tenants own a source
    /// called `mocks`, so a provenance map keyed by bare source id would hand
    /// each of them the other's ports — the same shadowing bug as above, one
    /// level down, and invisible in a fixture where no source owns anything.
    #[tokio::test]
    async fn sources_all_keeps_each_tenants_port_provenance_to_itself() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(
            &mut sm,
            2,
            source_put_in(2, "acme", "mocks", "https://acme/mocks.json"),
        )
        .await;
        apply_one(
            &mut sm,
            3,
            source_put_in(3, DEFAULT_TENANT, "mocks", "https://default/mocks.json"),
        )
        .await;
        apply_one(&mut sm, 4, pull_in(4, "acme", "mocks", &[19401], 1)).await;
        apply_one(&mut sm, 5, pull_in(5, DEFAULT_TENANT, "mocks", &[19402], 1)).await;

        let all = sm.sources_all().expect("read");
        let ports: Vec<(&str, &[u16])> = all
            .iter()
            .map(|row| {
                let record = row.record.as_ref().expect("these rows decode");
                (row.tenant.as_str(), record.ports.as_slice())
            })
            .collect();
        assert_eq!(
            ports,
            vec![("acme", &[19401u16][..]), (DEFAULT_TENANT, &[19402u16][..]),],
            "each same-named source keeps only the ports its own tenant pulled"
        );
    }

    /// A corrupt row is reported, not skipped — and reported **in band**, so it
    /// costs only itself (#243).
    ///
    /// Skipping it would stop a live source's poller and say nothing. Failing
    /// the whole scan — what #241 did — says something, but it says it about
    /// every tenant at once: this projection drives the fleet's poll scheduler,
    /// so one unreadable row would park reconciliation everywhere. The row's
    /// key decodes even when its value does not, which is what makes the third
    /// option available.
    #[tokio::test]
    async fn sources_all_reports_a_corrupt_row_rather_than_skipping_it() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(
            &mut sm,
            2,
            source_put_in(2, "acme", "mocks", "https://acme/mocks.json"),
        )
        .await;
        apply_one(
            &mut sm,
            3,
            source_put_in(3, DEFAULT_TENANT, "healthy", "https://default/h.json"),
        )
        .await;

        sm.inject_raw_source("acme", "mocks", "{not json");

        let all = sm
            .sources_all()
            .expect("a corrupt row must not fail the scan");
        assert_eq!(
            tenant_and_id(&all),
            vec![("acme", "mocks"), (DEFAULT_TENANT, "healthy")],
            "the corrupt row keeps its place in the list — it is reported, not dropped"
        );

        let corrupt = &all[0];
        let detail = corrupt
            .record
            .as_ref()
            .expect_err("the unparsable row must carry its decode failure");
        assert!(
            !detail.is_empty(),
            "the failure must say something a human can act on"
        );

        assert!(
            all[1].record.is_ok(),
            "another tenant's readable row must be unaffected: one bad row costs only itself"
        );
    }

    /// The per-row tolerance is `sources_all`'s alone. A tenant asking about
    /// its own sources still gets a hard error, because that is the surface
    /// where the corruption is actionable — and because the two must not drift
    /// together: someone "fixing" these to match `sources_all`'s new leniency
    /// would turn a tenant's committed-state corruption into an empty list.
    #[tokio::test]
    async fn a_corrupt_row_still_fails_the_strict_per_tenant_reads() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(
            &mut sm,
            2,
            source_put_in(2, "acme", "mocks", "https://acme/mocks.json"),
        )
        .await;
        apply_one(
            &mut sm,
            3,
            source_put_in(3, DEFAULT_TENANT, "healthy", "https://default/h.json"),
        )
        .await;

        sm.inject_raw_source("acme", "mocks", "{not json");

        assert!(
            sm.sources("acme").is_err(),
            "the owning tenant's list must not silently lose the source"
        );
        assert!(
            sm.source("acme", "mocks").is_err(),
            "nor may reading it directly answer `None`, which reads as 'no such source'"
        );
        assert!(
            sm.sources(DEFAULT_TENANT).is_ok(),
            "another tenant's read is unaffected — it filters before it parses"
        );
    }

    /// Deleting a tenant takes its sources out of the scheduler's view in the
    /// same committed op, so the next reconcile simply stops their pollers —
    /// no tombstone check and no tenant-list staleness anywhere in the
    /// scheduler.
    #[tokio::test]
    async fn sources_all_drops_a_deleted_tenants_rows() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(
            &mut sm,
            2,
            source_put_in(2, "acme", "mocks", "https://acme/mocks.json"),
        )
        .await;
        apply_one(
            &mut sm,
            3,
            source_put_in(3, DEFAULT_TENANT, "mocks", "https://default/mocks.json"),
        )
        .await;
        assert_eq!(sm.sources_all().expect("read").len(), 2);

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
            tenant_and_id(&sm.sources_all().expect("read")),
            vec![(DEFAULT_TENANT, "mocks")],
            "a deleted tenant's sources must stop being polled"
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
                journal_retention_secs: 0,
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

    // -- issue #161: the production read path (`principal`, `principal_bindings`,
    // `has_any_principals`) that authentication and authorization are built on --

    #[tokio::test]
    async fn has_any_principals_reflects_the_fleet_before_and_after_a_put() {
        let (_td, mut sm) = fresh_sm(None).await;
        assert!(!sm.has_any_principals().expect("read"));

        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme Corp")).await;
        apply_one(
            &mut sm,
            2,
            principal_put_req(2, "acme", test_principal("alice")),
        )
        .await;
        assert!(sm.has_any_principals().expect("read"));
    }

    #[tokio::test]
    async fn principal_reads_back_the_stored_record_and_none_for_an_unknown_id() {
        let (_td, mut sm) = fresh_sm(None).await;
        assert_eq!(sm.principal("alice").expect("read"), None);

        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme Corp")).await;
        apply_one(
            &mut sm,
            2,
            principal_put_req(2, "acme", test_principal("alice")),
        )
        .await;

        let principal = sm.principal("alice").expect("read").expect("stored");
        assert_eq!(principal.id, PrincipalId::new("alice"));
        assert_eq!(principal.display_name, "alice");
        assert_eq!(sm.principal("bob").expect("read"), None);
    }

    /// `sm_bindings` is principal-major (`(principal id, tenant)`), so this is
    /// the read that proves a lookup by principal id actually collects every
    /// tenant it names, not just the first — and that a different principal's
    /// rows never leak in.
    #[tokio::test]
    async fn principal_bindings_collects_every_tenant_and_nothing_belonging_to_another_principal() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme Corp")).await;
        apply_one(&mut sm, 2, tenant_put_req(2, "globex", "Globex Inc")).await;
        apply_one(
            &mut sm,
            3,
            principal_put_req(3, "acme", test_principal("alice")),
        )
        .await;
        apply_one(
            &mut sm,
            4,
            principal_put_req(4, "acme", test_principal("bob")),
        )
        .await;

        apply_one(
            &mut sm,
            5,
            binding_put_req(5, "acme", "alice", Role::Editor),
        )
        .await;
        apply_one(
            &mut sm,
            6,
            binding_put_req(6, "globex", "alice", Role::Viewer),
        )
        .await;
        // Bob's own binding must never appear in Alice's read.
        apply_one(
            &mut sm,
            7,
            binding_put_req(7, "acme", "bob", Role::TenantAdmin),
        )
        .await;

        let mut alice = sm.principal_bindings("alice").expect("read");
        alice.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        assert_eq!(
            alice,
            vec![
                (TenantId::new("acme"), Role::Editor),
                (TenantId::new("globex"), Role::Viewer),
            ]
        );

        assert_eq!(
            sm.principal_bindings("bob").expect("read"),
            vec![(TenantId::new("acme"), Role::TenantAdmin)]
        );

        assert_eq!(
            sm.principal_bindings("nobody").expect("read"),
            Vec::new(),
            "an unbound (or unknown) principal has no bindings, not an error"
        );
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
            sm.read_config(DEFAULT_TENANT, 8080)
                .expect("read")
                .is_some(),
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
            sm.read_config(DEFAULT_TENANT, 8080)
                .expect("read")
                .is_some(),
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

    // -- issue #224: journal clear generations ---------------------------------

    fn journal_clear(op_id: u128, port: u16, space: Option<&str>) -> ControlRequest {
        request(
            op_id,
            ControlOp::JournalClearGen {
                tenant: TenantId::default(),
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
        assert_eq!(
            sm.journal_gen(DEFAULT_TENANT, 8080, None)
                .expect("read gen"),
            1
        );

        apply_one(&mut sm, 2, journal_clear(2, 8080, None)).await;
        assert_eq!(
            sm.journal_gen(DEFAULT_TENANT, 8080, None)
                .expect("read gen"),
            2,
            "a second clear on the same port bumps again"
        );
    }

    /// Two clears for the same `(tenant, port)`, applied in log order (as every replica
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
            sm.journal_gen(DEFAULT_TENANT, 8080, None)
                .expect("read gen"),
            2,
            "two racing clears must compose to +2, not collapse to the same value twice"
        );
    }

    #[tokio::test]
    async fn a_space_clear_leaves_the_port_generation_untouched() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, journal_clear(1, 8080, Some("f"))).await;
        assert_eq!(
            sm.journal_gen(DEFAULT_TENANT, 8080, Some("f"))
                .expect("read gen"),
            1
        );
        assert_eq!(
            sm.journal_gen(DEFAULT_TENANT, 8080, None)
                .expect("read gen"),
            0,
            "a space-scoped clear must not bump the port-wide generation"
        );
        assert_eq!(
            sm.journal_gen(DEFAULT_TENANT, 8080, Some("g"))
                .expect("read gen"),
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
            restored
                .journal_gen(DEFAULT_TENANT, 8080, None)
                .expect("read gen"),
            1,
            "a node joining by snapshot must not read a cleared port back as 0 — that would \
             resurrect entries its peers already agree are cleared"
        );
        assert_eq!(
            restored
                .journal_gen(DEFAULT_TENANT, 8080, Some("f"))
                .expect("read gen"),
            1
        );
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
        assert_eq!(
            sm.journal_gen(DEFAULT_TENANT, 8080, None)
                .expect("read gen"),
            0
        );
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
                tenant: TenantId::default(),
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
        sm.proxy_recorded_resp(DEFAULT_TENANT, port, sig_hash)
            .expect("read marker")
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
                    tenant: TenantId::default(),
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
            request(
                3,
                ControlOp::DeleteImposter {
                    tenant: TenantId::default(),
                    port: 8080,
                },
            ),
        )
        .await;
        assert!(
            marker(&sm, 8080, "aa11").is_none(),
            "the delete purges the port's markers atomically"
        );
    }

    /// A recording that would blow the tenant's stub ceiling is a committed refusal that
    /// names the ceiling — and writes NO marker row, or the claim would settle Recorded
    /// against a stub that never landed (the atomicity #226 exists to guarantee).
    #[tokio::test]
    async fn a_recording_over_the_stub_ceiling_is_refused_and_writes_no_marker() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            tenant_put_with_quotas(
                1,
                "acme",
                Quotas {
                    max_imposters: 4,
                    max_stubs_per_imposter: 1,
                    max_flow_entries: 100,
                    ..Quotas::default()
                },
                0,
            ),
        )
        .await;
        apply_one(
            &mut sm,
            2,
            request(
                2,
                ControlOp::PutImposter {
                    tenant: TenantId::new("acme"),
                    config: Box::new(proxy_imposter_config(8081)),
                },
            ),
        )
        .await;
        let refused = apply_one(
            &mut sm,
            3,
            request(
                3,
                ControlOp::ProxyRecorded {
                    tenant: TenantId::new("acme"),
                    port: 8081,
                    sig_hash: "bb22".to_owned(),
                    resp: rift_cluster_base::seams::RecordedResponse {
                        status: 200,
                        headers: Vec::new(),
                        body: b"over".to_vec(),
                        latency_ms: None,
                        timestamp_secs: 0,
                    },
                    stub: Some(recorded_stub(
                        "over",
                        crate::control::RecordedStubPlacement::BeforeProxy,
                    )),
                },
            ),
        )
        .await;
        let ControlOutcome::Failed { reason } = &refused.outcome else {
            panic!("a recording over the ceiling must be refused: {refused:?}");
        };
        assert!(
            reason.contains("at most 1 stubs"),
            "names the ceiling: {reason}"
        );
        assert!(
            sm.proxy_recorded_resp("acme", 8081, "bb22")
                .expect("read marker")
                .is_none(),
            "a refused recording writes no marker"
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
                    tenant: TenantId::default(),
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
            .read_config(DEFAULT_TENANT, 8080)
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
            sm.read_config(DEFAULT_TENANT, 9300)
                .expect("read")
                .is_some(),
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
        let table = sm.route_table(DEFAULT_TENANT).expect("read route table");
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
        let table = sm.route_table(DEFAULT_TENANT).expect("read route table");
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
        assert!(
            sm.route_table(DEFAULT_TENANT)
                .expect("read")
                .routes
                .is_empty()
        );

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
            sm.route_table(DEFAULT_TENANT).expect("read").routes[0].id,
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
            .route_table(DEFAULT_TENANT)
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
                tenant: TenantId::new(crate::FLEET_SCOPE),
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

    // -- specs (#278) ------------------------------------------------------------------

    /// sha256 hex of `document`, stated by the test rather than computed by
    /// the code under test.
    fn spec_digest(document: &str) -> Digest {
        use sha2::{Digest as _, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(document.as_bytes());
        Digest::new(format!("{:x}", hasher.finalize()))
    }

    fn spec_put_as(op_id: u128, tenant: &str, id: &str, document: &str) -> ControlRequest {
        request(
            op_id,
            ControlOp::SpecPut {
                tenant: TenantId::new(tenant),
                id: id.to_owned(),
                meta: SpecMeta {
                    format: SpecFormat::Json,
                    digest: spec_digest(document),
                    source: SpecSource::Inline,
                },
                document: document.to_owned(),
            },
        )
    }

    fn spec_put(op_id: u128, id: &str, document: &str) -> ControlRequest {
        spec_put_as(op_id, DEFAULT_TENANT, id, document)
    }

    fn spec_delete(op_id: u128, id: &str) -> ControlRequest {
        request(
            op_id,
            ControlOp::SpecDelete {
                tenant: TenantId::default(),
                id: id.to_owned(),
            },
        )
    }

    fn spec_bind(op_id: u128, id: &str, port: u16) -> ControlRequest {
        request(
            op_id,
            ControlOp::SpecBind {
                tenant: TenantId::default(),
                id: id.to_owned(),
                port,
            },
        )
    }

    fn spec_unbind(op_id: u128, port: u16) -> ControlRequest {
        request(
            op_id,
            ControlOp::SpecUnbind {
                tenant: TenantId::default(),
                port,
            },
        )
    }

    fn one_spec(sm: &RedbStateMachine, id: &str) -> SpecRecord {
        sm.spec(DEFAULT_TENANT, id)
            .expect("read spec")
            .expect("spec present")
    }

    const PETSTORE: &str =
        r#"{"openapi":"3.0.0","info":{"title":"pets","version":"1"},"paths":{}}"#;
    const ORDERS: &str =
        r#"{"openapi":"3.0.0","info":{"title":"orders","version":"1"},"paths":{}}"#;

    #[tokio::test]
    async fn spec_put_stores_the_record_and_its_content_addressed_blob() {
        let (_td, mut sm) = fresh_sm(None).await;
        let response = apply_one(&mut sm, 1, spec_put(1, "petstore", PETSTORE)).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);

        let record = one_spec(&sm, "petstore");
        assert_eq!(record.id, "petstore");
        assert_eq!(record.digest, spec_digest(PETSTORE).as_str());
        assert_eq!(record.format, SpecFormat::Json);
        assert_eq!(record.source, SpecSource::Inline);
        assert!(record.ports.is_empty(), "nothing bound yet");
        assert!(!record.drifted);
        assert_eq!(record.revision, 1);
        assert_eq!(
            sm.spec_document(spec_digest(PETSTORE).as_str())
                .expect("read blob")
                .as_deref(),
            Some(PETSTORE),
            "the blob is addressed by digest and holds the exact bytes"
        );
        assert_eq!(
            sm.specs(DEFAULT_TENANT)
                .expect("list")
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["petstore"]
        );
        assert!(
            sm.spec("other", "petstore").expect("read").is_none(),
            "specs are tenant-owned: another tenant does not see it"
        );
    }

    /// Pins D-4: a blob's identity is its content digest — two spec ids with
    /// identical bytes share one `sm_spec_blobs` row, which lives until the last
    /// reference goes.
    #[tokio::test]
    async fn two_specs_sharing_bytes_share_one_blob_that_outlives_either_alone() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, spec_put(1, "a", PETSTORE)).await;
        apply_one(&mut sm, 2, spec_put_as(2, "acme", "b", PETSTORE)).await;
        let digest = spec_digest(PETSTORE);
        assert!(sm.spec_document(digest.as_str()).expect("read").is_some());
        assert_eq!(
            sm.spec_blob_count().expect("count"),
            1,
            "identical bytes under two ids — even across tenants — are one blob"
        );

        apply_one(&mut sm, 3, spec_delete(3, "a")).await;
        assert!(
            sm.spec_document(digest.as_str()).expect("read").is_some(),
            "deleting one referencing spec keeps the blob"
        );
        assert!(sm.spec(DEFAULT_TENANT, "a").expect("read").is_none());
        assert!(sm.spec("acme", "b").expect("read").is_some());

        let response = apply_one(
            &mut sm,
            4,
            request(
                4,
                ControlOp::SpecDelete {
                    tenant: TenantId::new("acme"),
                    id: "b".to_owned(),
                },
            ),
        )
        .await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert!(
            sm.spec_document(digest.as_str()).expect("read").is_none(),
            "deleting the last referencing spec removes the blob"
        );
        assert_eq!(sm.spec_blob_count().expect("count"), 0);
    }

    #[tokio::test]
    async fn re_putting_a_spec_with_new_bytes_swaps_the_blob_and_bumps_the_revision() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, spec_put(1, "petstore", PETSTORE)).await;
        apply_one(&mut sm, 2, spec_put(2, "petstore", ORDERS)).await;
        let record = one_spec(&sm, "petstore");
        assert_eq!(record.digest, spec_digest(ORDERS).as_str());
        assert_eq!(record.revision, 2);
        assert!(
            sm.spec_document(spec_digest(PETSTORE).as_str())
                .expect("read")
                .is_none(),
            "the superseded bytes are unreferenced and gone"
        );
        assert_eq!(
            sm.spec_document(spec_digest(ORDERS).as_str())
                .expect("read")
                .as_deref(),
            Some(ORDERS)
        );
        assert_eq!(sm.spec_blob_count().expect("count"), 1);
    }

    #[tokio::test]
    async fn spec_delete_of_an_unknown_spec_is_a_committed_refusal() {
        let (_td, mut sm) = fresh_sm(None).await;
        let response = apply_one(&mut sm, 1, spec_delete(1, "ghost")).await;
        assert_eq!(
            response.outcome,
            ControlOutcome::Failed {
                reason: "no spec \"ghost\"".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn spec_bind_stamps_provenance_and_a_manual_edit_marks_the_port_drifted() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, spec_put(1, "petstore", PETSTORE)).await;
        apply_one(
            &mut sm,
            2,
            put(2, 8080, json!([{ "id": "spec:listPets:200" }])),
        )
        .await;
        let response = apply_one(&mut sm, 3, spec_bind(3, "petstore", 8080)).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);

        let binding = sm
            .spec_binding(DEFAULT_TENANT, 8080)
            .expect("read binding")
            .expect("bound");
        assert_eq!(binding.spec_id, "petstore");
        assert_eq!(binding.digest, spec_digest(PETSTORE).as_str());
        assert!(!binding.drifted);
        let record = one_spec(&sm, "petstore");
        assert_eq!(record.ports, [8080]);
        assert!(!record.drifted);
        assert_eq!(
            record.revision, 1,
            "binding a port does not rewrite the spec record"
        );
        assert_eq!(
            sm.read_config(DEFAULT_TENANT, 8080)
                .expect("read")
                .map(|json| json.contains("spec:listPets:200")),
            Some(true),
            "the bind leaves the config bytes exactly as they were"
        );

        // A hand edit of a spec-bound port is drift, on the port and hence on the spec.
        apply_one(&mut sm, 4, put(4, 8080, json!([{ "id": "edited" }]))).await;
        let binding = sm
            .spec_binding(DEFAULT_TENANT, 8080)
            .expect("read")
            .expect("still bound after an edit — provenance survives");
        assert!(binding.drifted, "a manual PutImposter flips drift");
        assert!(one_spec(&sm, "petstore").drifted);

        // Re-binding (a redeploy) resets the baseline.
        apply_one(&mut sm, 5, spec_bind(5, "petstore", 8080)).await;
        assert!(
            !sm.spec_binding(DEFAULT_TENANT, 8080)
                .expect("read")
                .expect("bound")
                .drifted,
            "a bind is the drift baseline"
        );

        // A stub patch is a config mutation too.
        apply_one(
            &mut sm,
            6,
            request(
                6,
                ControlOp::PatchStubs {
                    tenant: TenantId::default(),
                    port: 8080,
                    edit: StubEditScript(vec![StubEdit::Add {
                        stub: serde_json::from_value(json!({ "id": "added" })).expect("stub"),
                        index: None,
                    }]),
                },
            ),
        )
        .await;
        assert!(
            sm.spec_binding(DEFAULT_TENANT, 8080)
                .expect("read")
                .expect("bound")
                .drifted,
            "PatchStubs flips drift"
        );

        // Toggling is not a config edit.
        apply_one(&mut sm, 7, spec_bind(7, "petstore", 8080)).await;
        apply_one(
            &mut sm,
            8,
            request(
                8,
                ControlOp::SetEnabled {
                    tenant: TenantId::default(),
                    port: 8080,
                    enabled: false,
                },
            ),
        )
        .await;
        let binding = sm
            .spec_binding(DEFAULT_TENANT, 8080)
            .expect("read")
            .expect("SetEnabled keeps the provenance");
        assert!(!binding.drifted, "SetEnabled is not drift");
    }

    #[tokio::test]
    async fn spec_bind_refuses_an_unknown_imposter_or_spec() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, spec_put(1, "petstore", PETSTORE)).await;
        let response = apply_one(&mut sm, 2, spec_bind(2, "petstore", 8080)).await;
        assert_eq!(
            response.outcome,
            ControlOutcome::Failed {
                reason: "no imposter on port 8080".to_owned()
            }
        );
        apply_one(&mut sm, 3, put(3, 8080, json!([]))).await;
        let response = apply_one(&mut sm, 4, spec_bind(4, "ghost", 8080)).await;
        assert_eq!(
            response.outcome,
            ControlOutcome::Failed {
                reason: "no spec \"ghost\"".to_owned()
            }
        );
        assert!(
            sm.spec_binding(DEFAULT_TENANT, 8080)
                .expect("read")
                .is_none(),
            "a refused bind stamps nothing"
        );
        let response = apply_one(&mut sm, 5, spec_unbind(5, 9090)).await;
        assert_eq!(
            response.outcome,
            ControlOutcome::Failed {
                reason: "no imposter on port 9090".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn spec_delete_refuses_while_bound_and_unbind_clears_the_port() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, spec_put(1, "petstore", PETSTORE)).await;
        apply_one(&mut sm, 2, put(2, 8080, json!([]))).await;
        apply_one(&mut sm, 3, spec_bind(3, "petstore", 8080)).await;

        let response = apply_one(&mut sm, 4, spec_delete(4, "petstore")).await;
        match response.outcome {
            ControlOutcome::Failed { reason } => {
                assert!(
                    reason.contains("bound") && reason.contains("8080"),
                    "the refusal names the binding: {reason}"
                );
            }
            other => panic!("a bound spec must not be deleted: {other:?}"),
        }
        assert!(sm.spec(DEFAULT_TENANT, "petstore").expect("read").is_some());

        let response = apply_one(&mut sm, 5, spec_unbind(5, 8080)).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert!(
            sm.spec_binding(DEFAULT_TENANT, 8080)
                .expect("read")
                .is_none(),
            "unbind clears the provenance"
        );
        assert!(
            sm.read_config(DEFAULT_TENANT, 8080)
                .expect("read")
                .is_some(),
            "unbind never tears the imposter down"
        );
        assert!(one_spec(&sm, "petstore").ports.is_empty());

        let response = apply_one(&mut sm, 6, spec_delete(6, "petstore")).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert!(sm.spec(DEFAULT_TENANT, "petstore").expect("read").is_none());
        assert!(
            sm.spec_document(spec_digest(PETSTORE).as_str())
                .expect("read")
                .is_none()
        );
    }

    #[tokio::test]
    async fn deleting_a_bound_imposter_takes_its_spec_binding_with_it() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, spec_put(1, "petstore", PETSTORE)).await;
        apply_one(&mut sm, 2, put(2, 8080, json!([]))).await;
        apply_one(&mut sm, 3, spec_bind(3, "petstore", 8080)).await;
        apply_one(
            &mut sm,
            4,
            request(
                4,
                ControlOp::DeleteImposter {
                    tenant: TenantId::default(),
                    port: 8080,
                },
            ),
        )
        .await;
        assert!(one_spec(&sm, "petstore").ports.is_empty());
        let response = apply_one(&mut sm, 5, spec_delete(5, "petstore")).await;
        assert_eq!(
            response.outcome,
            ControlOutcome::Applied,
            "nothing binds it any more"
        );
    }

    #[tokio::test]
    async fn snapshot_round_trips_specs_blobs_and_bindings() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, spec_put(1, "petstore", PETSTORE)).await;
        apply_one(&mut sm, 2, put(2, 8080, json!([]))).await;
        apply_one(&mut sm, 3, spec_bind(3, "petstore", 8080)).await;
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");

        let record = follower
            .spec(DEFAULT_TENANT, "petstore")
            .expect("read")
            .expect("spec travels in the snapshot");
        assert_eq!(record.digest, spec_digest(PETSTORE).as_str());
        assert_eq!(record.ports, [8080]);
        assert_eq!(
            follower
                .spec_document(spec_digest(PETSTORE).as_str())
                .expect("read")
                .as_deref(),
            Some(PETSTORE),
            "the blob travels too — a joining node must hold the same bytes"
        );
        assert_eq!(
            follower
                .spec_binding(DEFAULT_TENANT, 8080)
                .expect("read")
                .map(|b| b.spec_id),
            Some("petstore".to_owned())
        );
    }

    #[tokio::test]
    async fn a_snapshot_without_specs_installs_and_reads_empty() {
        let (td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, spec_put(1, "petstore", PETSTORE)).await;
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");
        let mut payload: serde_json::Value =
            serde_json::from_slice(&read_snapshot_bytes(snapshot).await).expect("snapshot is json");
        let object = payload.as_object_mut().expect("object");
        assert!(
            object.remove("specs").is_some(),
            "specs travel in a current snapshot"
        );
        assert!(
            object.remove("spec_blobs").is_some(),
            "blobs travel in a current snapshot"
        );
        let older =
            snapshot_handle_from(td.path(), &serde_json::to_vec(&payload).expect("re-encode"))
                .await;
        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, older)
            .await
            .expect("a pre-#278 snapshot still installs");
        assert!(follower.specs(DEFAULT_TENANT).expect("list").is_empty());
        assert_eq!(follower.spec_blob_count().expect("count"), 0);
    }

    #[tokio::test]
    async fn tenant_delete_cascades_specs_and_gcs_their_blobs() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            request(
                1,
                ControlOp::TenantPut {
                    tenant: TenantId::new("acme"),
                    display_name: "Acme".to_owned(),
                    quotas: Quotas::default(),
                    journal_retention_secs: 0,
                },
            ),
        )
        .await;
        apply_one(&mut sm, 2, spec_put_as(2, "acme", "petstore", PETSTORE)).await;
        apply_one(&mut sm, 3, spec_put(3, "shared", PETSTORE)).await;
        apply_one(&mut sm, 4, spec_put_as(4, "acme", "orders", ORDERS)).await;
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
        assert!(
            sm.specs("acme").expect("list").is_empty(),
            "the tenant's specs go with it"
        );
        assert!(
            sm.spec_document(spec_digest(ORDERS).as_str())
                .expect("read")
                .is_none(),
            "a blob only the deleted tenant referenced is gone"
        );
        assert!(
            sm.spec_document(spec_digest(PETSTORE).as_str())
                .expect("read")
                .is_some(),
            "a blob another tenant still references stays"
        );
    }

    // -- datasets (#285) --------------------------------------------------------------

    fn dataset_digest(csv: &str) -> Digest {
        use sha2::{Digest as _, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(csv.as_bytes());
        Digest::new(format!("{:x}", hasher.finalize()))
    }

    fn dataset_put_as(op_id: u128, tenant: &str, name: &str, csv: &str) -> ControlRequest {
        let mut lines = csv.lines();
        let columns: Vec<String> = lines
            .next()
            .unwrap_or_default()
            .split(',')
            .map(|c| c.trim().to_owned())
            .collect();
        let rows = lines.count() as u64;
        request(
            op_id,
            ControlOp::DatasetPut {
                tenant: TenantId::new(tenant),
                record: DatasetRecord {
                    name: name.to_owned(),
                    digest: dataset_digest(csv),
                    key_columns: vec![columns[0].clone()],
                    delimiter: ',',
                    columns,
                    rows,
                    bytes: csv.len() as u64,
                },
                csv: csv.to_owned(),
            },
        )
    }

    fn dataset_put(op_id: u128, name: &str, csv: &str) -> ControlRequest {
        dataset_put_as(op_id, DEFAULT_TENANT, name, csv)
    }

    fn dataset_delete_as(op_id: u128, tenant: &str, name: &str) -> ControlRequest {
        request(
            op_id,
            ControlOp::DatasetDelete {
                tenant: TenantId::new(tenant),
                name: name.to_owned(),
            },
        )
    }

    fn dataset_delete(op_id: u128, name: &str) -> ControlRequest {
        dataset_delete_as(op_id, DEFAULT_TENANT, name)
    }

    /// A state machine with a spool directory, the way `RaftNode::start` builds one.
    async fn fresh_sm_with_spool() -> (TempDir, RedbStateMachine, std::path::PathBuf) {
        let td = TempDir::new().expect("tempdir");
        let (_, sm) = new(td.path().join("raft.redb")).await.expect("open store");
        let spool = td.path().join("datasets");
        let sm = sm.with_spool_dir(spool.clone());
        (td, sm, spool)
    }

    fn spool_file(spool: &std::path::Path, csv: &str) -> std::path::PathBuf {
        spool.join(format!("{}.csv", dataset_digest(csv).as_str()))
    }

    fn one_dataset(sm: &RedbStateMachine, name: &str) -> DatasetSummary {
        sm.dataset(DEFAULT_TENANT, name)
            .expect("read dataset")
            .expect("dataset present")
    }

    const CUSTOMERS: &str = "id,name,tier\n1,ada,gold\n2,bob,silver\n";
    const ORDERS_CSV: &str = "order,customer\n100,1\n101,2\n102,1\n";

    #[tokio::test]
    async fn dataset_put_stores_the_record_the_blob_and_the_spool_file() {
        let (_td, mut sm, spool) = fresh_sm_with_spool().await;
        let response = apply_one(
            &mut sm,
            1,
            request_at(1, 1_700_000_000, dataset_put_op("customers", CUSTOMERS)),
        )
        .await;
        assert_eq!(response.outcome, ControlOutcome::Applied);

        let record = one_dataset(&sm, "customers");
        assert_eq!(record.name, "customers");
        assert_eq!(record.version, 1, "the first version of a name is 1");
        assert_eq!(record.digest, dataset_digest(CUSTOMERS).as_str());
        assert_eq!(record.key_columns, ["id"]);
        assert_eq!(record.columns, ["id", "name", "tier"]);
        assert_eq!(record.rows, 2);
        assert_eq!(record.bytes, CUSTOMERS.len() as u64);
        assert_eq!(
            record.created_at_secs, 1_700_000_000,
            "the replicated clock, never a local one"
        );
        assert_eq!(record.revision, 1);
        assert_eq!(sm.dataset_blob_count().expect("count"), 1);
        assert_eq!(
            sm.spool_path(dataset_digest(CUSTOMERS).as_str()),
            Some(spool_file(&spool, CUSTOMERS))
        );
        let on_disk = std::fs::read(spool_file(&spool, CUSTOMERS)).expect("spool file exists");
        assert_eq!(
            on_disk,
            CUSTOMERS.as_bytes(),
            "byte-identical to the upload"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(spool_file(&spool, CUSTOMERS))
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "spool files are private to the node");
        }
        assert!(
            std::fs::read_dir(&spool).expect("spool dir").all(|e| !e
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .contains(".tmp")),
            "no temp file is left behind"
        );
        assert!(
            sm.dataset("other", "customers").expect("read").is_none(),
            "datasets are tenant-owned"
        );
    }

    fn dataset_put_op(name: &str, csv: &str) -> ControlOp {
        match dataset_put(0, name, csv).op {
            op @ ControlOp::DatasetPut { .. } => op,
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn re_putting_a_name_appends_a_version_and_keeps_the_old_one_live() {
        let (_td, mut sm, spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS)).await;
        let v2 = "id,name,tier\n1,ada,platinum\n";
        apply_one(&mut sm, 2, dataset_put(2, "customers", v2)).await;
        let latest = one_dataset(&sm, "customers");
        assert_eq!(latest.version, 2);
        assert_eq!(latest.digest, dataset_digest(v2).as_str());
        let all = sm.datasets(DEFAULT_TENANT).expect("list");
        assert_eq!(
            all.iter()
                .map(|d| (d.name.as_str(), d.version))
                .collect::<Vec<_>>(),
            [("customers", 1), ("customers", 2)],
            "every live version is listed, name then version ascending"
        );
        assert!(
            spool_file(&spool, CUSTOMERS).exists(),
            "v1's file is still referenced"
        );
        assert!(spool_file(&spool, v2).exists());
        assert_eq!(sm.dataset_blob_count().expect("count"), 2);
    }

    /// Pins D-4: content-addressed identity — identical bytes under different
    /// names and tenants share one blob row and one spool file.
    #[tokio::test]
    async fn identical_bytes_share_one_blob_and_one_spool_file_across_names_and_tenants() {
        let (_td, mut sm, spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "a", CUSTOMERS)).await;
        apply_one(&mut sm, 2, dataset_put(2, "b", CUSTOMERS)).await;
        apply_one(&mut sm, 3, dataset_put_as(3, "acme", "c", CUSTOMERS)).await;
        assert_eq!(sm.dataset_blob_count().expect("count"), 1);
        assert_eq!(
            std::fs::read_dir(&spool).expect("dir").count(),
            1,
            "one file for one byte string"
        );

        apply_one(&mut sm, 4, dataset_delete(4, "a")).await;
        apply_one(&mut sm, 5, dataset_delete(5, "b")).await;
        assert!(
            spool_file(&spool, CUSTOMERS).exists(),
            "acme still references it"
        );
        assert_eq!(sm.dataset_blob_count().expect("count"), 1);

        let response = apply_one(&mut sm, 6, dataset_delete_as(6, "acme", "c")).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert_eq!(sm.dataset_blob_count().expect("count"), 0);
        assert!(
            !spool_file(&spool, CUSTOMERS).exists(),
            "the last live reference takes the file with it"
        );
    }

    #[tokio::test]
    async fn dataset_delete_tombstones_every_live_version_and_a_re_put_continues_the_numbering() {
        let (_td, mut sm, spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS)).await;
        apply_one(&mut sm, 2, dataset_put(2, "customers", ORDERS_CSV)).await;
        let response = apply_one(&mut sm, 3, dataset_delete(3, "customers")).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert!(
            sm.dataset(DEFAULT_TENANT, "customers")
                .expect("read")
                .is_none()
        );
        assert!(sm.datasets(DEFAULT_TENANT).expect("list").is_empty());
        assert!(!spool_file(&spool, CUSTOMERS).exists());
        assert!(!spool_file(&spool, ORDERS_CSV).exists());

        let response = apply_one(&mut sm, 4, dataset_delete(4, "customers")).await;
        assert_eq!(
            response.outcome,
            ControlOutcome::Failed {
                reason: "no dataset \"customers\"".to_owned()
            },
            "deleting what is already gone is a committed refusal"
        );

        apply_one(&mut sm, 5, dataset_put(5, "customers", CUSTOMERS)).await;
        assert_eq!(
            one_dataset(&sm, "customers").version,
            3,
            "versions are monotonic per name across a delete: tombstones count"
        );
        assert!(spool_file(&spool, CUSTOMERS).exists(), "the file is back");
    }

    /// A binding tally that had to skip a corrupt config says so.
    ///
    /// The count is what tells an operator whether a delete will be refused, so a silently short
    /// one promises a delete that then `409`s. The flag is what the front renders as
    /// `Rift-Cluster-Partial` — without it, "0 bindings" from a corrupt table is indistinguishable
    /// from "0 bindings" from an empty one.
    #[tokio::test]
    async fn a_binding_tally_that_skipped_a_corrupt_config_reports_itself_incomplete() {
        let (_td, mut sm, _spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS)).await;

        let bound = json!({
            "config_json": json!({
                "port": 8080,
                "protocol": "http",
                "stubs": [{
                    "id": "b",
                    "responses": [{
                        "is": {},
                        "_rift": { "dataset": {
                            "name": "customers", "keyColumn": "id", "into": "${row}"
                        } }
                    }]
                }]
            })
            .to_string(),
            "enabled": true,
            "revision": 2,
        })
        .to_string();
        sm.inject_raw_config(DEFAULT_TENANT, 8080, &bound);

        let (counts, incomplete) = sm
            .dataset_binding_counts(DEFAULT_TENANT)
            .expect("counts read");
        assert_eq!(counts.get("customers"), Some(&1));
        assert!(!incomplete, "a readable table is a complete tally");

        // Now a row that will not parse at all.
        sm.inject_raw_config(DEFAULT_TENANT, 8081, "{not json");
        let (counts, incomplete) = sm
            .dataset_binding_counts(DEFAULT_TENANT)
            .expect("counts read");
        assert_eq!(
            counts.get("customers"),
            Some(&1),
            "the readable bindings are still counted"
        );
        assert!(
            incomplete,
            "but the tally must admit it is short, or a 0 from corruption reads as a real 0"
        );
    }

    #[tokio::test]
    async fn dataset_delete_refuses_while_a_stub_binds_it() {
        let (_td, mut sm, _spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS)).await;
        // The RFC-005 §3.3 binding block on a stub response. D2 is what makes the engine's
        // config schema carry `_rift.dataset` — today the vendored `RiftResponseExtension` drops
        // it on parse, so a `PutImposter` cannot store one yet. The row is injected as D2 will
        // store it, so the refusal D1 wires is exercised now rather than trusted.
        let stored = json!({
            "config_json": json!({
                "port": 8080,
                "protocol": "http",
                "host": "127.0.0.1",
                "stubs": [{
                    "id": "lookup",
                    "responses": [{
                        "is": { "statusCode": 200, "body": "${row}[name]" },
                        "_rift": { "dataset": { "name": "customers", "keyColumn": "id", "into": "${row}" } }
                    }]
                }]
            }).to_string(),
            "enabled": true,
            "revision": 2,
        })
        .to_string();
        sm.inject_raw_config(DEFAULT_TENANT, 8080, &stored);
        let response = apply_one(&mut sm, 3, dataset_delete(3, "customers")).await;
        match response.outcome {
            ControlOutcome::Failed { reason } => assert!(
                reason.contains("bound") && reason.contains("8080"),
                "names the binding port: {reason}"
            ),
            other => panic!("a bound dataset must not be deleted: {other:?}"),
        }
        assert!(
            sm.dataset(DEFAULT_TENANT, "customers")
                .expect("read")
                .is_some()
        );
        // A different tenant's identically-named dataset is not what this stub binds.
        apply_one(
            &mut sm,
            4,
            dataset_put_as(4, "acme", "customers", ORDERS_CSV),
        )
        .await;
        assert_eq!(
            apply_one(&mut sm, 5, dataset_delete_as(5, "acme", "customers"))
                .await
                .outcome,
            ControlOutcome::Applied
        );
    }

    #[tokio::test]
    async fn dataset_quotas_are_enforced_at_apply_at_their_exact_ceilings() {
        let (_td, mut sm, _spool) = fresh_sm_with_spool().await;
        apply_one(
            &mut sm,
            1,
            request(
                1,
                ControlOp::TenantPut {
                    tenant: TenantId::new("acme"),
                    display_name: "Acme".to_owned(),
                    quotas: Quotas {
                        max_datasets: 2,
                        max_dataset_bytes: 40,
                        max_dataset_total_bytes: 100,
                        ..Quotas::default()
                    },
                    journal_retention_secs: 0,
                },
            ),
        )
        .await;
        let small = "id,v\n1,a\n"; // 9 bytes
        assert_eq!(small.len(), 9);
        // "id,v\n1," is 7 bytes and the trailing newline 1, so 32 payload bytes make exactly 40.
        let forty = format!("id,v\n1,{}\n", "a".repeat(32));
        assert_eq!(forty.len(), 40);
        let forty_one = format!("id,v\n1,{}\n", "a".repeat(33));
        assert_eq!(forty_one.len(), 41);
        let (forty, forty_one) = (forty.as_str(), forty_one.as_str());

        // Per-dataset bytes: at the ceiling is fine, one over is refused, nothing stored.
        assert_eq!(
            apply_one(&mut sm, 2, dataset_put_as(2, "acme", "d1", forty))
                .await
                .outcome,
            ControlOutcome::Applied
        );
        let response = apply_one(&mut sm, 3, dataset_put_as(3, "acme", "d2", forty_one)).await;
        match response.outcome {
            ControlOutcome::Failed { reason } => assert!(
                reason.contains("40") && reason.contains("\"d2\""),
                "names the ceiling and the dataset: {reason}"
            ),
            other => panic!("over the per-dataset ceiling: {other:?}"),
        }
        assert!(sm.dataset("acme", "d2").expect("read").is_none());
        assert_eq!(
            sm.dataset_blob_count().expect("count"),
            1,
            "a refused put stores no blob"
        );

        // Count: the second distinct name is fine, a third is refused — but a new version of an
        // existing name is not a new dataset.
        assert_eq!(
            apply_one(&mut sm, 4, dataset_put_as(4, "acme", "d2", small))
                .await
                .outcome,
            ControlOutcome::Applied
        );
        let response = apply_one(&mut sm, 5, dataset_put_as(5, "acme", "d3", small)).await;
        match response.outcome {
            ControlOutcome::Failed { reason } => {
                assert!(
                    reason.contains("ceiling") && reason.contains("2"),
                    "{reason}"
                );
            }
            other => panic!("over the dataset-count ceiling: {other:?}"),
        }
        assert_eq!(
            apply_one(&mut sm, 6, dataset_put_as(6, "acme", "d1", "id,v\n1,b\n"))
                .await
                .outcome,
            ControlOutcome::Applied,
            "re-versioning a name the tenant already holds is not a new dataset"
        );

        // Total bytes: 40 + 9 + 9 = 58 held (every live version counts). Two 21-byte uploads
        // land exactly on the ceiling (58 + 21 + 21 = 100); the next byte is refused.
        let twenty_one = format!("id,v\n1,{}\n", "a".repeat(13));
        assert_eq!(twenty_one.len(), 21);
        let twenty_one = twenty_one.as_str();
        assert_eq!(
            apply_one(&mut sm, 7, dataset_put_as(7, "acme", "d1", twenty_one))
                .await
                .outcome,
            ControlOutcome::Applied
        );
        let twenty_one_b = format!("id,v\n1,{}\n", "b".repeat(13));
        let twenty_one_b = twenty_one_b.as_str();
        assert_eq!(
            apply_one(&mut sm, 8, dataset_put_as(8, "acme", "d2", twenty_one_b))
                .await
                .outcome,
            ControlOutcome::Applied,
            "exactly at the total ceiling (100) is allowed"
        );
        let response = apply_one(&mut sm, 9, dataset_put_as(9, "acme", "d1", small)).await;
        match response.outcome {
            ControlOutcome::Failed { reason } => {
                assert!(reason.contains("100"), "names the total ceiling: {reason}");
            }
            other => panic!("over the total-bytes ceiling: {other:?}"),
        }
        // Deleting frees the total.
        apply_one(&mut sm, 10, dataset_delete_as(10, "acme", "d2")).await;
        assert_eq!(
            apply_one(&mut sm, 11, dataset_put_as(11, "acme", "d1", small))
                .await
                .outcome,
            ControlOutcome::Applied
        );
    }

    #[tokio::test]
    async fn a_default_tenant_gets_the_default_dataset_quotas() {
        let (_td, mut sm, _spool) = fresh_sm_with_spool().await;
        // 8 MiB + 1 byte: over the default per-dataset ceiling for a tenant with no record.
        let mut csv = String::from("id,v\n");
        let mut i = 0u64;
        while csv.len() <= 8 * 1024 * 1024 {
            csv.push_str(&format!("{i},{}\n", "x".repeat(1000)));
            i += 1;
        }
        let response = apply_one(&mut sm, 1, dataset_put(1, "big", &csv)).await;
        match response.outcome {
            ControlOutcome::Failed { reason } => {
                assert!(
                    reason.contains("8388608"),
                    "names the default ceiling: {reason}"
                );
            }
            other => panic!("over the default per-dataset ceiling: {other:?}"),
        }
    }

    #[tokio::test]
    async fn snapshot_round_trips_datasets_and_materialises_their_spool_files() {
        let (_td, mut sm, _spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS)).await;
        apply_one(&mut sm, 2, dataset_put(2, "orders", ORDERS_CSV)).await;
        apply_one(&mut sm, 3, dataset_delete(3, "orders")).await;
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower, follower_spool) = fresh_sm_with_spool().await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");
        let record = follower
            .dataset(DEFAULT_TENANT, "customers")
            .expect("read")
            .expect("dataset travels in the snapshot");
        assert_eq!(record.digest, dataset_digest(CUSTOMERS).as_str());
        assert!(
            follower
                .dataset(DEFAULT_TENANT, "orders")
                .expect("read")
                .is_none(),
            "a tombstone travels as a tombstone"
        );
        assert_eq!(
            std::fs::read(spool_file(&follower_spool, CUSTOMERS)).expect("materialised on install"),
            CUSTOMERS.as_bytes(),
            "a node that joins by snapshot holds the same bytes on disk"
        );
        assert!(
            !spool_file(&follower_spool, ORDERS_CSV).exists(),
            "no file for a tombstoned digest"
        );
        assert_eq!(follower.dataset_blob_count().expect("count"), 1);
    }

    #[tokio::test]
    async fn a_snapshot_without_datasets_installs_and_reads_empty() {
        let (td, mut sm, _spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS)).await;
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");
        let mut payload: serde_json::Value =
            serde_json::from_slice(&read_snapshot_bytes(snapshot).await).expect("snapshot is json");
        let object = payload.as_object_mut().expect("object");
        assert!(object.remove("datasets").is_some());
        assert!(object.remove("dataset_blobs").is_some());
        let older =
            snapshot_handle_from(td.path(), &serde_json::to_vec(&payload).expect("re-encode"))
                .await;
        let (_td2, mut follower, _) = fresh_sm_with_spool().await;
        follower
            .install_snapshot(&meta, older)
            .await
            .expect("a pre-#285 snapshot still installs");
        assert!(follower.datasets(DEFAULT_TENANT).expect("list").is_empty());
        assert_eq!(follower.dataset_blob_count().expect("count"), 0);
    }

    #[tokio::test]
    async fn a_missing_spool_file_is_repaired_from_the_state_machine_on_reconcile() {
        let (_td, mut sm, spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS)).await;
        std::fs::remove_file(spool_file(&spool, CUSTOMERS)).expect("simulate a lost file");
        std::fs::write(spool.join("orphan.csv"), b"junk")
            .expect("an orphan the repair must not touch");
        sm.reconcile_engine().await.expect("reconcile");
        assert_eq!(
            std::fs::read(spool_file(&spool, CUSTOMERS)).expect("repaired"),
            CUSTOMERS.as_bytes()
        );
        assert!(spool.join("orphan.csv").exists(), "repair only ever adds");
    }

    #[tokio::test]
    async fn tenant_delete_cascades_datasets_and_their_files() {
        let (_td, mut sm, spool) = fresh_sm_with_spool().await;
        apply_one(
            &mut sm,
            1,
            request(
                1,
                ControlOp::TenantPut {
                    tenant: TenantId::new("acme"),
                    display_name: "Acme".to_owned(),
                    quotas: Quotas::default(),
                    journal_retention_secs: 0,
                },
            ),
        )
        .await;
        apply_one(
            &mut sm,
            2,
            dataset_put_as(2, "acme", "customers", CUSTOMERS),
        )
        .await;
        apply_one(&mut sm, 3, dataset_put_as(3, "acme", "orders", ORDERS_CSV)).await;
        apply_one(&mut sm, 4, dataset_put(4, "shared", CUSTOMERS)).await;
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
        assert!(sm.datasets("acme").expect("list").is_empty());
        assert!(
            !spool_file(&spool, ORDERS_CSV).exists(),
            "only acme held these bytes"
        );
        assert!(
            spool_file(&spool, CUSTOMERS).exists(),
            "default still holds these"
        );
        assert_eq!(sm.dataset_blob_count().expect("count"), 1);
    }

    /// The batch shape `apply` actually runs: several entries in one transaction, actions
    /// driven after the commit. A delete and a re-put of the same bytes in one batch must not
    /// leave the live row pointing at a file the deferred unspool removed.
    #[tokio::test]
    async fn a_delete_and_re_put_of_the_same_bytes_in_one_batch_keeps_the_spool_file() {
        let (_td, mut sm, spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS)).await;
        let responses = sm
            .apply([
                entry(2, dataset_delete(2, "customers")),
                entry(3, dataset_put(3, "customers", CUSTOMERS)),
            ])
            .await
            .expect("apply");
        assert!(
            responses
                .iter()
                .all(|r| r.outcome == ControlOutcome::Applied),
            "{responses:?}"
        );
        assert_eq!(one_dataset(&sm, "customers").version, 2);
        assert_eq!(sm.dataset_blob_count().expect("count"), 1);
        assert_eq!(
            std::fs::read(spool_file(&spool, CUSTOMERS)).expect("the file survives the batch"),
            CUSTOMERS.as_bytes()
        );
    }

    #[tokio::test]
    async fn deleting_a_dataset_whose_spool_file_is_already_gone_still_applies() {
        let (_td, mut sm, spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS)).await;
        std::fs::remove_file(spool_file(&spool, CUSTOMERS)).expect("simulate a lost file");
        let response = apply_one(&mut sm, 2, dataset_delete(2, "customers")).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);
        assert_eq!(sm.dataset_blob_count().expect("count"), 0);
    }

    /// The delete guard fails closed: a config row it cannot read might be the binding stub.
    #[tokio::test]
    async fn dataset_delete_refuses_when_a_stored_config_is_unreadable() {
        let (_td, mut sm, _spool) = fresh_sm_with_spool().await;
        apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS)).await;
        sm.inject_raw_config(DEFAULT_TENANT, 8080, "{not json");
        let response = apply_one(&mut sm, 2, dataset_delete(2, "customers")).await;
        match response.outcome {
            ControlOutcome::Failed { reason } => assert!(
                reason.contains("cannot tell") && reason.contains("8080"),
                "{reason}"
            ),
            other => panic!("an unreadable config must refuse the delete: {other:?}"),
        }
        assert!(
            sm.dataset(DEFAULT_TENANT, "customers")
                .expect("read")
                .is_some()
        );
    }

    #[tokio::test]
    async fn without_a_spool_dir_the_state_machine_still_applies_datasets() {
        // An embedder that never attached a spool dir (or a storage-only test) still gets the
        // replicated tables; only the on-disk materialisation is skipped.
        let (_td, mut sm) = fresh_sm(None).await;
        assert_eq!(
            apply_one(&mut sm, 1, dataset_put(1, "customers", CUSTOMERS))
                .await
                .outcome,
            ControlOutcome::Applied
        );
        assert_eq!(one_dataset(&sm, "customers").version, 1);
        assert_eq!(sm.spool_path("abc"), None);
    }

    /// #436: the snapshot payload lives as a **file** beside redb, and the redb row keeps only
    /// `{meta, file}`.
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

        // >= 16 MiB of state machine, spread over four datasets because the per-dataset quota is
        // 8 MiB. Anything much smaller stops discriminating: the point is a build long enough to
        // outrun an election timeout, and a snapshot that fits comfortably inside one would pass
        // against the unfixed code too.
        //
        // Each dataset gets **distinct** bytes, and that is load-bearing rather than incidental:
        // dataset blobs are content-addressed — `SM_DATASET_BLOBS_TABLE` is keyed by digest and
        // the `DatasetPut` arm inserts only on a digest's first reference — so four datasets
        // sharing one CSV would store a single blob, and this would quietly measure a quarter of
        // what it claims to.
        for (i, name) in ["alpha", "beta", "gamma", "delta"].iter().enumerate() {
            let mut csv = String::from("id,payload\n");
            let mut row = 0u64;
            while csv.len() < 4 * 1024 * 1024 {
                csv.push_str(&format!("{row},{name}-{}\n", "x".repeat(1_000)));
                row += 1;
            }
            let op = i as u128 + 1;
            let applied = sm
                .apply(vec![entry(i as u64 + 1, dataset_put(op, name, &csv))])
                .await
                .expect("apply dataset");
            // Without this the whole test passes vacuously if the writes were refused — a tiny
            // snapshot builds fast enough to clear the threshold on the unfixed code.
            assert_eq!(
                applied.first().map(|r| &r.outcome),
                Some(&ControlOutcome::Applied),
                "dataset {name} must actually be stored, or the gap below measures nothing"
            );
        }

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
        let mut csv = String::from("id,payload\n");
        let mut i = 0u64;
        while csv.len() < 4 * 1024 * 1024 {
            csv.push_str(&format!("{i},{}\n", "x".repeat(1_000)));
            i += 1;
        }
        let raw = csv.len();
        let applied = sm
            .apply(vec![entry(1, dataset_put(1, "big", &csv))])
            .await
            .expect("apply");
        // Without this the whole test passes vacuously if the write were refused: a near-empty
        // snapshot trivially satisfies a "<= 1.1x raw" bound.
        assert_eq!(
            applied.first().map(|r| &r.outcome),
            Some(&ControlOutcome::Applied),
            "the dataset must actually be stored, or the ratio below measures nothing"
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
        request(
            op_id,
            ControlOp::DeleteRoute {
                tenant: TenantId::default(),
                id: id.to_owned(),
            },
        )
    }

    fn routes_revision(sm: &RedbStateMachine, tenant: &str) -> u64 {
        sm.route_table_with_revision(tenant)
            .expect("read route table")
            .1
    }

    fn route_ids(sm: &RedbStateMachine, tenant: &str) -> Vec<String> {
        let mut ids: Vec<String> = sm
            .route_table(tenant)
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
        assert_eq!(routes_revision(&sm, DEFAULT_TENANT), 0);
        for s in [&mut sm, &mut sm2] {
            let response = s
                .apply(vec![entry(
                    1,
                    conditioned(
                        1,
                        0,
                        ControlOp::PutRoutes {
                            tenant: TenantId::default(),
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
            routes_revision(&sm, DEFAULT_TENANT),
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
                        tenant: TenantId::default(),
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
                        tenant: TenantId::default(),
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
            route_ids(&sm, DEFAULT_TENANT),
            vec!["a".to_owned()],
            "a refused precondition must not have replaced the table"
        );
        assert_eq!(
            routes_revision(&sm, DEFAULT_TENANT),
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
        assert_eq!(routes_revision(&sm, DEFAULT_TENANT), 3);

        // Even a delete that removed nothing: the op committed against the
        // table, so the revision it leaves behind must reflect that.
        sm.apply(vec![entry(4, delete_route(4, "never-existed"))])
            .await
            .expect("apply");
        assert_eq!(
            routes_revision(&sm, DEFAULT_TENANT),
            4,
            "an idempotent delete still committed against this table"
        );

        // Per tenant, not fleet-wide: writing one tenant's table must not
        // invalidate another's outstanding precondition.
        assert_eq!(routes_revision(&sm, "acme"), 0);
        sm.apply(vec![entry(
            5,
            put_routes_in(5, "acme", vec![test_route("z", 3)]),
        )])
        .await
        .expect("apply");
        assert_eq!(routes_revision(&sm, "acme"), 5);
        assert_eq!(
            routes_revision(&sm, DEFAULT_TENANT),
            4,
            "another tenant's write must not touch this tenant's revision"
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
        sm.apply(vec![entry(
            2,
            put_routes_in(2, "acme", vec![test_route("z", 2)]),
        )])
        .await
        .expect("apply");

        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower, _follower_routes) = fresh_sm_with_routes().await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");

        assert_eq!(routes_revision(&follower, DEFAULT_TENANT), 1);
        assert_eq!(routes_revision(&follower, "acme"), 2);

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

    /// A snapshot built before #210 carries no revisions at all. It must still
    /// install, and every tenant must read as revision 0 — which *fails* a
    /// stale precondition rather than passing one. The dangerous alternative
    /// (inheriting the last applied index) would let a token minted before the
    /// join silently pass.
    #[tokio::test]
    async fn a_pre_route_revision_snapshot_installs_and_reads_zero() {
        let (_td, sm, _routes) = fresh_sm_with_routes().await;
        let legacy = json!({
            "configs": [],
            "routes": [["default", "a", "{}"]],
            "dedup": [],
            "last_applied_log": null,
            "last_membership": { "log_id": null, "membership": { "configs": [], "nodes": {} } },
        });
        let payload: super::SnapshotPayload =
            serde_json::from_value(legacy).expect("a pre-#210 snapshot payload still decodes");
        assert!(
            payload.routes_revisions.is_empty(),
            "the missing field defaults to no rows, not a parse failure"
        );
        assert_eq!(
            routes_revision(&sm, DEFAULT_TENANT),
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

    // -- issue #163: the audit projection and leader-side quotas --------------
    //
    // RFC-002 §9/§4.4. These are the state-machine half of the gate; the
    // every-node claims are asserted across a real three-node cluster in
    // `tests/cluster.rs`, and the RBAC-visibility claims over HTTP in
    // `rift-cluster-server/tests/rbac.rs`.

    fn audit_rows(sm: &RedbStateMachine) -> Vec<AuditRow> {
        sm.audit_since(0, None, 10_000).expect("read audit")
    }

    /// The ports a tenant currently holds, ascending. `configured_ports` is
    /// fleet-wide (it backs the operator surface, not a tenant-scoped read),
    /// and a quota test needs a real tenant record — which `default` cannot
    /// have — so this reads the table directly instead of filtering
    /// `configured_ports`'s output.
    fn ports_in_tenant(sm: &RedbStateMachine, tenant: &str) -> Vec<u16> {
        let read_txn = sm.db.begin_read().expect("read txn");
        let table = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .expect("open sm_configs");
        let mut ports: Vec<u16> = Vec::new();
        for item in table.iter().expect("iter configs") {
            let (key, _) = item.expect("config row");
            let (stored_tenant, port) = key.value();
            if stored_tenant == tenant {
                ports.push(port);
            }
        }
        ports.sort_unstable();
        ports
    }

    /// A `PutImposter` in a named tenant — the existing `put` helper is
    /// default-tenant only (issue #182).
    fn put_in(op_id: u128, tenant: &str, port: u16, stubs: serde_json::Value) -> ControlRequest {
        request(
            op_id,
            ControlOp::PutImposter {
                tenant: TenantId::new(tenant),
                config: Box::new(config(port, stubs)),
            },
        )
    }

    /// A `PutRoutes` in a named tenant — the existing `put_routes` helper is
    /// default-tenant only (issue #182).
    fn put_routes_in(op_id: u128, tenant: &str, routes: Vec<Route>) -> ControlRequest {
        request(
            op_id,
            ControlOp::PutRoutes {
                tenant: TenantId::new(tenant),
                table: RouteTable { routes },
            },
        )
    }

    /// A `SourcePut` in a named tenant — the existing `source_put` helper is
    /// default-tenant only. `uri` is explicit because the multi-tenant
    /// projection tests (#241) distinguish records by it.
    fn source_put_in(op_id: u128, tenant: &str, id: &str, uri: &str) -> ControlRequest {
        request(
            op_id,
            ControlOp::SourcePut {
                tenant: TenantId::new(tenant),
                id: id.to_owned(),
                uri: uri.to_owned(),
                mode: SourceMode::Pinned,
                auth_ref: None,
                on_drift: OnDrift::Overwrite,
                poll_secs: None,
            },
        )
    }

    /// A `SourcePullResult` in a named tenant, with `stubs_each` stubs per port.
    fn pull_in(
        op_id: u128,
        tenant: &str,
        id: &str,
        ports: &[u16],
        stubs_each: usize,
    ) -> ControlRequest {
        let configs: Vec<ImposterConfig> = ports
            .iter()
            .map(|port| {
                let stubs: Vec<serde_json::Value> = (0..stubs_each)
                    .map(|i| json!({ "id": format!("s{port}-{i}") }))
                    .collect();
                config(*port, serde_json::Value::Array(stubs))
            })
            .collect();
        request(
            op_id,
            ControlOp::SourcePullResult {
                tenant: TenantId::new(tenant),
                id: id.to_owned(),
                version: Some("v1".to_owned()),
                digest: Digest::new("digest-v1"),
                configs,
            },
        )
    }

    /// Like [`RedbStateMachine::read_config`], but parses straight to the
    /// stub ids a quota test wants to assert on, instead of the raw config
    /// JSON.
    fn stub_ids_in_tenant(sm: &RedbStateMachine, tenant: &str, port: u16) -> Vec<String> {
        let read_txn = sm.db.begin_read().expect("read txn");
        let table = read_txn
            .open_table(SM_CONFIGS_TABLE)
            .expect("open sm_configs");
        let guard = table
            .get((tenant, port))
            .expect("get config")
            .expect("config present");
        let stored: StoredImposter =
            serde_json::from_str(guard.value()).expect("stored imposter parses");
        let config: serde_json::Value =
            serde_json::from_str(&stored.config_json).expect("config parses");
        config["stubs"]
            .as_array()
            .expect("stubs array")
            .iter()
            .map(|s| s["id"].as_str().expect("test stubs carry ids").to_owned())
            .collect()
    }

    fn tenant_put_with_quotas(
        op_id: u128,
        tenant: &str,
        quotas: Quotas,
        journal_retention_secs: u64,
    ) -> ControlRequest {
        request(
            op_id,
            ControlOp::TenantPut {
                tenant: TenantId::new(tenant),
                display_name: tenant.to_owned(),
                quotas,
                journal_retention_secs,
            },
        )
    }

    fn put_in_tenant(op_id: u128, tenant: &str, port: u16) -> ControlRequest {
        request(
            op_id,
            ControlOp::PutImposter {
                tenant: TenantId::new(tenant),
                config: Box::new(config(port, json!([{ "id": "a" }]))),
            },
        )
    }

    /// A write is audited once, with every field taken from the committed entry
    /// rather than from anything a handler decided.
    #[tokio::test]
    async fn a_committed_write_is_projected_into_exactly_one_audit_row() {
        let (_td, mut sm) = fresh_sm(None).await;
        let mut req = put(7, 8080, json!([{ "id": "a" }]));
        req.principal = Some("default/alice".to_owned());
        req.issued_at_secs = 1_700_000_000;

        let response = apply_one(&mut sm, 5, req).await;
        assert_eq!(response.outcome, ControlOutcome::Applied);

        let rows = audit_rows(&sm);
        assert_eq!(rows.len(), 1, "one write, one row: {rows:?}");
        let row = &rows[0];
        assert_eq!(row.principal.as_deref(), Some("default/alice"));
        assert_eq!(row.tenant, TenantId::default());
        assert_eq!(row.action, "imposter.write");
        assert_eq!(row.resource, "8080");
        assert_eq!(row.op_id, Uuid::from_u128(7));
        assert_eq!(
            row.revision, 5,
            "the row carries the applying log index, which is the same revision \
             the write's own response returned"
        );
        assert_eq!(row.outcome, ControlOutcome::Applied);
        assert_eq!(
            row.ts_secs, 1_700_000_000,
            "the timestamp is the entry's replicated issued_at_secs, so every \
             replica derives the same one — never a local SystemTime::now()"
        );
    }

    /// The exactly-once claim, against the one thing that can break it: a
    /// replayed `op_id`. The projection sits below the dedup short-circuit, so a
    /// replay returns the original response and appends nothing.
    #[tokio::test]
    async fn a_replayed_op_id_does_not_append_a_second_audit_row() {
        let (_td, mut sm) = fresh_sm(None).await;
        let first = apply_one(&mut sm, 1, put(7, 8080, json!([{ "id": "a" }]))).await;
        let replay = apply_one(&mut sm, 2, put(7, 8080, json!([{ "id": "a" }]))).await;
        assert_eq!(replay, first, "the replay returns the original response");

        let rows = audit_rows(&sm);
        assert_eq!(
            rows.len(),
            1,
            "a replay is not a second write and must not be a second row: {rows:?}"
        );
        assert_eq!(rows[0].revision, 1, "the row keeps the original revision");
    }

    /// A refusal is a committed decision, and the audit stream records it. "Who
    /// tried to do what and was refused" is the half of an audit log that
    /// matters most.
    #[tokio::test]
    async fn a_refusal_is_audited_as_a_committed_failed_decision() {
        let (_td, mut sm) = fresh_sm(None).await;
        let mut req = request(
            9,
            ControlOp::PatchStubs {
                tenant: TenantId::default(),
                port: 4444,
                edit: StubEditScript(vec![StubEdit::DeleteById {
                    id: "nope".to_owned(),
                }]),
            },
        );
        req.principal = Some("default/mallory".to_owned());

        let response = apply_one(&mut sm, 3, req).await;
        let ControlOutcome::Failed { .. } = &response.outcome else {
            panic!(
                "patching an absent port must fail, got {:?}",
                response.outcome
            );
        };

        let rows = audit_rows(&sm);
        assert_eq!(rows.len(), 1, "a refusal is still a row: {rows:?}");
        assert_eq!(rows[0].principal.as_deref(), Some("default/mallory"));
        assert_eq!(rows[0].revision, 3);
        assert_eq!(
            rows[0].outcome, response.outcome,
            "the audited outcome is the committed one, verbatim"
        );
    }

    /// §11 open question 1, as a test: a quota refusal is not an error the
    /// submitter sees at submit time — it is a committed `Failed` at a revision,
    /// which is exactly what makes it discoverable through `op_status` after a
    /// parked write replays.
    #[tokio::test]
    async fn a_quota_refusal_is_a_committed_failed_decision_naming_the_ceiling() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            tenant_put_with_quotas(
                1,
                "acme",
                Quotas {
                    max_imposters: 1,
                    ..Quotas::default()
                },
                0,
            ),
        )
        .await;

        let first = apply_one(&mut sm, 2, put_in_tenant(2, "acme", 8080)).await;
        assert_eq!(
            first.outcome,
            ControlOutcome::Applied,
            "the first imposter is inside the ceiling"
        );

        let refused = apply_one(&mut sm, 3, put_in_tenant(3, "acme", 8081)).await;
        let ControlOutcome::Failed { reason } = &refused.outcome else {
            panic!("the second imposter is over the ceiling: {refused:?}");
        };
        assert!(
            reason.contains("ceiling") && reason.contains('1'),
            "the refusal must name the ceiling it hit, not just fail: {reason}"
        );
        assert_eq!(refused.revision, 3, "the refusal has a revision of its own");

        assert!(
            sm.read_config(DEFAULT_TENANT, 8081)
                .expect("read")
                .is_none(),
            "a refused write must not land"
        );

        let refusal_row = audit_rows(&sm)
            .into_iter()
            .find(|r| r.revision == 3)
            .expect("the refusal is audited");
        assert_eq!(refusal_row.outcome, refused.outcome);
        assert_eq!(refusal_row.tenant, TenantId::new("acme"));
        assert_eq!(refusal_row.resource, "8081");
    }

    /// The `max_imposters` count excludes the port being written, so a tenant
    /// sitting exactly at its ceiling can still update what it already owns.
    /// Without this, a full tenant would be frozen rather than merely full.
    #[tokio::test]
    async fn a_tenant_at_its_ceiling_can_still_replace_an_imposter_it_owns() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            tenant_put_with_quotas(
                1,
                "acme",
                Quotas {
                    max_imposters: 1,
                    ..Quotas::default()
                },
                0,
            ),
        )
        .await;
        apply_one(&mut sm, 2, put_in_tenant(2, "acme", 8080)).await;

        let replace = apply_one(
            &mut sm,
            3,
            request(
                3,
                ControlOp::PutImposter {
                    tenant: TenantId::new("acme"),
                    config: Box::new(config(8080, json!([{ "id": "b" }]))),
                },
            ),
        )
        .await;
        assert_eq!(
            replace.outcome,
            ControlOutcome::Applied,
            "replacing an owned imposter adds none, so the ceiling is not reached"
        );
    }

    /// The per-imposter stub ceiling applies to a `PutImposter` payload and to
    /// the *result* of a `PatchStubs` — a script is a sequence of edits, so only
    /// the config it produces knows how many stubs the imposter ends up with.
    #[tokio::test]
    async fn the_stub_ceiling_refuses_both_a_config_and_an_edit_that_would_exceed_it() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            tenant_put_with_quotas(
                1,
                "acme",
                Quotas {
                    max_stubs_per_imposter: 2,
                    ..Quotas::default()
                },
                0,
            ),
        )
        .await;

        let too_many = apply_one(
            &mut sm,
            2,
            request(
                2,
                ControlOp::PutImposter {
                    tenant: TenantId::new("acme"),
                    config: Box::new(config(
                        8080,
                        json!([{ "id": "a" }, { "id": "b" }, { "id": "c" }]),
                    )),
                },
            ),
        )
        .await;
        let ControlOutcome::Failed { reason } = &too_many.outcome else {
            panic!("three stubs against a ceiling of two must fail: {too_many:?}");
        };
        assert!(reason.contains('2'), "{reason}");

        // At the ceiling: accepted.
        let at_ceiling = apply_one(
            &mut sm,
            3,
            request(
                3,
                ControlOp::PutImposter {
                    tenant: TenantId::new("acme"),
                    config: Box::new(config(8080, json!([{ "id": "a" }, { "id": "b" }]))),
                },
            ),
        )
        .await;
        assert_eq!(at_ceiling.outcome, ControlOutcome::Applied);

        // And an edit that would push it over is refused on its result.
        let over_by_edit = apply_one(
            &mut sm,
            4,
            request(
                4,
                ControlOp::PatchStubs {
                    tenant: TenantId::new("acme"),
                    port: 8080,
                    edit: StubEditScript(vec![StubEdit::Add {
                        stub: serde_json::from_value(json!({ "id": "c" }))
                            .expect("test stub parses"),
                        index: None,
                    }]),
                },
            ),
        )
        .await;
        let ControlOutcome::Failed { reason } = &over_by_edit.outcome else {
            panic!("an edit taking the imposter to three stubs must fail: {over_by_edit:?}");
        };
        assert!(reason.contains('2'), "{reason}");
        assert_eq!(
            stub_ids_in_tenant(&sm, "acme", 8080),
            vec!["a".to_owned(), "b".to_owned()],
            "the refused edit left the stored config untouched"
        );
    }

    /// A fleet with no tenant records configured must not find every write
    /// refused because an absent record read as a quota of nothing.
    #[tokio::test]
    async fn a_tenant_with_no_stored_record_gets_the_generous_default_quota() {
        let (_td, mut sm) = fresh_sm(None).await;
        for (index, port) in (8080u16..8090).enumerate() {
            let response = apply_one(
                &mut sm,
                index as u64 + 1,
                put(index as u128 + 1, port, json!([{ "id": "a" }])),
            )
            .await;
            assert_eq!(
                response.outcome,
                ControlOutcome::Applied,
                "an unconfigured default tenant must not be capacity-locked"
            );
        }
    }

    /// AC3. Retention is measured on the replicated logical clock, and this test
    /// is built so that a local `SystemTime::now()` could not possibly pass it:
    /// every timestamp here is decades in the past, so a GC reading the wall
    /// clock would find *everything* expired and drop the row that must survive.
    #[tokio::test]
    async fn audit_retention_gc_runs_on_the_replicated_clock_not_the_local_one() {
        let (_td, sm) = fresh_sm(None).await;
        let mut sm = sm.with_audit_retention_secs(100);

        // Three writes on a logical clock that has nothing to do with now.
        for (index, ts) in [(1u64, 1_000u64), (2, 1_050), (3, 1_090)] {
            let mut req = put(
                u128::from(index),
                8080 + index as u16,
                json!([{ "id": "a" }]),
            );
            req.issued_at_secs = ts;
            apply_one(&mut sm, index, req).await;
        }
        assert_eq!(audit_rows(&sm).len(), 3, "nothing has expired yet");

        // A fourth write advances the replicated clock to 1_200. GC runs against
        // the clock as it stood *before* its batch (see `gc_audit`), so this
        // apply does not yet sweep — the fifth one does, with a cutoff of 1_100.
        let mut advance = put(4, 8099, json!([{ "id": "a" }]));
        advance.issued_at_secs = 1_200;
        apply_one(&mut sm, 4, advance).await;
        assert_eq!(
            audit_rows(&sm).len(),
            4,
            "expiry lags the clock-advancing write by one apply, identically on \
             every replica"
        );

        let mut sweep = put(5, 8098, json!([{ "id": "a" }]));
        sweep.issued_at_secs = 1_200;
        apply_one(&mut sm, 5, sweep).await;

        let rows = audit_rows(&sm);
        assert_eq!(
            rows.iter().map(|r| r.revision).collect::<Vec<_>>(),
            vec![4, 5],
            "GC must expire on the replicated clock's cutoff of 1_100, keeping \
             only the rows inside the window: {rows:?}"
        );
        assert!(
            rows.iter().all(|r| r.ts_secs == 1_200),
            "a wall-clock GC would have dropped these too — every timestamp in \
             this test is decades old, so this assertion is what distinguishes \
             the replicated clock from SystemTime::now(): {rows:?}"
        );
    }

    /// `0` means keep everything. An operator who turns retention off must not
    /// lose their history to a zero that reads as "expire immediately".
    #[tokio::test]
    async fn audit_retention_of_zero_keeps_every_row() {
        let (_td, sm) = fresh_sm(None).await;
        let mut sm = sm.with_audit_retention_secs(0);
        for index in 1u64..=3 {
            let mut req = put(
                u128::from(index),
                8080 + index as u16,
                json!([{ "id": "a" }]),
            );
            req.issued_at_secs = index * 1_000_000;
            apply_one(&mut sm, index, req).await;
        }
        assert_eq!(
            audit_rows(&sm).len(),
            3,
            "retention 0 is 'forever', not 'expire immediately'"
        );
    }

    /// AC4, snapshot half — and the #134/#137 mutant #165 keeps standing: drop
    /// `audit` from `SnapshotPayload` and a follower that joins by snapshot
    /// install comes back with an empty history and nothing reports a gap.
    #[tokio::test]
    async fn audit_rows_survive_a_snapshot_build_and_install() {
        let (_td, mut sm) = fresh_sm(None).await;
        let mut req = put(7, 8080, json!([{ "id": "a" }]));
        req.principal = Some("default/alice".to_owned());
        apply_one(&mut sm, 1, req).await;
        let before = audit_rows(&sm);
        assert_eq!(before.len(), 1);

        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");

        assert_eq!(
            audit_rows(&follower),
            before,
            "a node that joins by snapshot install must hold the same audit \
             history as the node it joined from"
        );
    }

    /// The #164 tables must ride the snapshot too — and this test exists
    /// because the one that was supposed to cover them did not.
    ///
    /// `sink_and_checkpoint_survive_a_restart_and_snapshot_install` in
    /// `tests/cluster.rs` restarts a node in a 3-node fleet. openraft here runs
    /// the default `LogEntries(5000)` snapshot policy, and that fleet commits a
    /// few dozen entries, so **no snapshot is ever built**: the node restores
    /// from its own redb and the test proves restart-and-replay, not
    /// `install_snapshot`. The chaos README already records the same correction
    /// for C18 and C22. So the snapshot round trip is gated here, in process,
    /// by driving `build_snapshot`/`install_snapshot` directly — exactly as
    /// `audit_rows_survive_a_snapshot_build_and_install` does for #163's rows.
    ///
    /// The standing mutant (#134/#137): drop any of these three from
    /// `SnapshotPayload` and a node joining by snapshot comes back either not
    /// exporting at all, re-shipping the whole retained history to the
    /// customer's bucket, or reporting a clean stream over a window retention
    /// has already deleted.
    #[tokio::test]
    async fn the_audit_export_sink_checkpoint_and_gc_watermark_survive_a_snapshot_install() {
        let (_td, sm) = fresh_sm(None).await;
        // Retention short enough that GC actually runs below, so the watermark
        // under test is a real one rather than a zero that would pass whether
        // or not it was carried.
        let mut sm = sm.with_audit_retention_secs(100);

        apply_one(
            &mut sm,
            1,
            request_at(
                1,
                1_000,
                ControlOp::AuditSinkPut {
                    tenant: TenantId::new(FLEET_SCOPE),
                    uri: "s3://acme-audit/rift/".to_owned(),
                    auth_ref: Some("prod-collector".to_owned()),
                    batch_max_rows: 250,
                },
            ),
        )
        .await;
        apply_one(
            &mut sm,
            2,
            request_at(
                2,
                1_000,
                ControlOp::AuditCheckpointPut {
                    tenant: TenantId::new(FLEET_SCOPE),
                    revision: 1,
                },
            ),
        )
        .await;

        // Age the sink-declaration row out so GC records a watermark, the same
        // clock-advance-then-sweep shape
        // `audit_retention_gc_runs_on_the_replicated_clock_not_the_local_one`
        // uses (expiry lags the advancing write by one apply).
        for (index, ts) in [(3u64, 1_200u64), (4, 1_200)] {
            let mut advance = put(
                u128::from(index),
                8080 + index as u16,
                json!([{ "id": "a" }]),
            );
            advance.issued_at_secs = ts;
            apply_one(&mut sm, index, advance).await;
        }

        let sink_before = sm.audit_sink().expect("read sink");
        let checkpoint_before = sm.audit_checkpoint().expect("read checkpoint");
        let watermark_before = sm.audit_gc_watermark().expect("read watermark");
        assert!(sink_before.is_some(), "a sink was declared");
        assert_eq!(checkpoint_before, 1);
        assert!(
            watermark_before > 0,
            "GC must have run for the watermark to be worth carrying; without              this the assertion below would pass on a payload that dropped it"
        );

        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, snapshot)
            .await
            .expect("install");

        assert_eq!(
            follower.audit_sink().expect("read sink"),
            sink_before,
            "a node joining by snapshot install must inherit the fleet's sink;              without it, it stops exporting the moment it wins an election"
        );
        assert_eq!(
            follower.audit_checkpoint().expect("read checkpoint"),
            checkpoint_before,
            "without the checkpoint it resumes from zero and re-ships the whole              retained history to the customer's bucket"
        );
        assert_eq!(
            follower.audit_gc_watermark().expect("read watermark"),
            watermark_before,
            "without the watermark it forgets retention ever deleted anything              and reports a clean stream over a permanent hole"
        );
    }

    /// An older snapshot, written before #164 existed, must still install —
    /// same `#[serde(default)]` contract every table added since #134 carries.
    #[tokio::test]
    async fn a_pre_audit_export_snapshot_still_installs() {
        let (td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, put(1, 8080, json!([{ "id": "a" }]))).await;
        let mut builder = sm.clone();
        let Snapshot { meta, snapshot } = builder.build_snapshot().await.expect("build snapshot");

        // Strip the #164 fields plus #185's `session_key`, standing in for a payload serialized
        // by a binary that predates all of them.
        let mut payload: serde_json::Value =
            serde_json::from_slice(&read_snapshot_bytes(snapshot).await)
                .expect("snapshot payload is JSON");
        for field in [
            "audit_sink",
            "audit_checkpoint",
            "audit_gc_watermark",
            "session_key",
        ] {
            payload
                .as_object_mut()
                .expect("payload is an object")
                .remove(field);
        }
        let stripped =
            snapshot_handle_from(td.path(), &serde_json::to_vec(&payload).expect("re-encode"))
                .await;

        let (_td2, mut follower) = fresh_sm(None).await;
        follower
            .install_snapshot(&meta, stripped)
            .await
            .expect("a pre-#164/#185 snapshot must still install");
        assert_eq!(follower.audit_sink().expect("read sink"), None);
        assert_eq!(follower.audit_checkpoint().expect("read checkpoint"), 0);
        assert_eq!(follower.audit_gc_watermark().expect("read watermark"), 0);
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
                    tenant: TenantId::new(FLEET_SCOPE),
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

    /// A corrupt principal row is an **error**, never `None`.
    ///
    /// `None` means "no such principal", which every authentication path treats as "this credential
    /// resolves to nobody" — and on a fleet with no `--api-key` and no principals, `should_bypass`
    /// turns that into an *open admin plane*. So a corrupt row reading back as `None` would convert
    /// disk corruption into an authorization bypass. It must fail closed instead.
    #[tokio::test]
    async fn a_corrupt_principal_row_is_an_error_not_an_absent_principal() {
        let (_td, mut sm) = fresh_sm(None).await;
        let principal = Principal {
            id: PrincipalId::new("p-corrupt"),
            display_name: "corrupt".to_owned(),
            auth: AuthSource::ApiKey {
                hash: hash_api_key("some-key"),
            },
            disabled: false,
        };
        apply_one(
            &mut sm,
            1,
            request_at(
                1,
                1_000,
                ControlOp::PrincipalPut {
                    tenant: TenantId::default(),
                    principal,
                },
            ),
        )
        .await;
        assert!(
            sm.principal("p-corrupt").expect("read").is_some(),
            "principal was stored"
        );

        corrupt_row(&sm, SM_PRINCIPALS_TABLE, "p-corrupt");

        assert!(
            sm.principal("p-corrupt").is_err(),
            "a corrupt principal row read back as an absent principal — on a fleet with no \
             --api-key that is an open admin plane, not a 401"
        );
    }

    /// RFC-006 §5.3, issue #185: the session-signing key must travel through a snapshot install
    /// exactly like `sm_audit_sink` does — miss this and a node that joins by snapshot cannot
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
                    tenant: TenantId::new(FLEET_SCOPE),
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

    /// AC4, restart half: the rows are in redb, not in memory, so reopening the
    /// same database file serves them.
    #[tokio::test]
    async fn audit_rows_survive_reopening_the_database() {
        let td = TempDir::new().expect("tempdir");
        let path = td.path().join("raft.redb");
        let before = {
            let (_, mut sm) = new(&path).await.expect("open store");
            apply_one(&mut sm, 1, put(7, 8080, json!([{ "id": "a" }]))).await;
            audit_rows(&sm)
        };
        let (_, sm) = new(&path).await.expect("reopen store");
        assert_eq!(
            audit_rows(&sm),
            before,
            "audit history must survive a restart"
        );
        assert_eq!(before.len(), 1);
    }

    /// `since` is a range scan from a revision, and `limit` bounds the response.
    /// The endpoint is reachable by any tenant admin, so an unbounded read would
    /// be an unbounded response.
    #[tokio::test]
    async fn audit_since_scans_from_a_revision_and_respects_its_limit() {
        let (_td, mut sm) = fresh_sm(None).await;
        for index in 1u64..=5 {
            apply_one(
                &mut sm,
                index,
                put(
                    u128::from(index),
                    8080 + index as u16,
                    json!([{ "id": "a" }]),
                ),
            )
            .await;
        }

        let from_three = sm.audit_since(3, None, 10_000).expect("read");
        assert_eq!(
            from_three.iter().map(|r| r.revision).collect::<Vec<_>>(),
            vec![3, 4, 5],
            "since is inclusive and ascending by revision"
        );

        let limited = sm.audit_since(0, None, 2).expect("read");
        assert_eq!(limited.len(), 2, "limit bounds the response");
        assert_eq!(limited[0].revision, 1, "and takes the oldest first");
    }

    /// The tenant filter is applied in the store, not by the caller: handing a
    /// tenant admin the fleet's rows and trusting a client to narrow them would
    /// mean the server had already sent another tenant's audit history.
    #[tokio::test]
    async fn audit_since_filters_by_tenant_in_the_store() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(&mut sm, 2, tenant_put_req(2, "globex", "Globex")).await;
        apply_one(&mut sm, 3, put_in_tenant(3, "acme", 8080)).await;
        apply_one(&mut sm, 4, put_in_tenant(4, "globex", 8081)).await;

        let acme = sm.audit_since(0, Some("acme"), 10_000).expect("read");
        assert!(
            acme.iter().all(|r| r.tenant.as_str() == "acme"),
            "a tenant-scoped read must not leak another tenant's rows: {acme:?}"
        );
        assert!(
            acme.iter().any(|r| r.resource == "8080"),
            "and must still contain its own: {acme:?}"
        );

        let fleet = sm.audit_since(0, None, 10_000).expect("read");
        assert!(
            fleet.len() > acme.len(),
            "an unfiltered read is the fleet's"
        );
    }

    /// AC6, as deviated. RFC-002 §6 reasons that a fleet-wide delete "carries no
    /// port, therefore no tenant" and asks for `tenant: null`. #159 has since put
    /// an explicit tenant on every op, so the delete knows exactly whose
    /// imposters it destroyed — and recording `null` would hide that in the row
    /// describing the most destructive operation in the set. The wildcard
    /// belongs in `resource`, and this asserts it lands there, so the choice is
    /// pinned rather than left to be re-litigated as a bug.
    #[tokio::test]
    async fn delete_all_is_audited_with_the_real_tenant_and_a_wildcard_resource() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(&mut sm, 1, tenant_put_req(1, "acme", "Acme")).await;
        apply_one(&mut sm, 2, put_in_tenant(2, "acme", 8080)).await;
        apply_one(
            &mut sm,
            3,
            request(
                3,
                ControlOp::DeleteAll {
                    tenant: TenantId::new("acme"),
                },
            ),
        )
        .await;

        let row = audit_rows(&sm)
            .into_iter()
            .find(|r| r.revision == 3)
            .expect("the delete is audited");
        assert_eq!(
            row.tenant,
            TenantId::new("acme"),
            "the row names whose imposters were destroyed"
        );
        assert_eq!(row.resource, AUDIT_RESOURCE_ALL);
        assert_eq!(row.action, "imposter.delete");
    }

    /// U-10 (upstream #855). The admin request task's `with_principal_scope` does
    /// not survive the hop to openraft's state-machine task, so without the
    /// re-opened scope in `drive_engine` every clustered change event reaches
    /// M3's SSE and the #164 export sink unattributed. The audit table being
    /// correctly attributed does not help them — they are on the event path, not
    /// the log path.
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

    /// A zero ceiling is refused at validation rather than stored. It is
    /// representable and almost never intended: it makes the tenant permanently
    /// unable to hold a single imposter, and the operator finds out later from a
    /// write that fails for a reason they will not connect to a quota they set.
    #[tokio::test]
    async fn a_zero_quota_ceiling_is_refused_rather_than_stored() {
        let (_td, mut sm) = fresh_sm(None).await;
        for quotas in [
            Quotas {
                max_imposters: 0,
                ..Quotas::default()
            },
            Quotas {
                max_stubs_per_imposter: 0,
                ..Quotas::default()
            },
        ] {
            let response =
                apply_one(&mut sm, 1, tenant_put_with_quotas(1, "acme", quotas, 0)).await;
            let ControlOutcome::Failed { reason } = &response.outcome else {
                panic!("a zero ceiling must be refused: {response:?}");
            };
            assert!(reason.contains('0'), "{reason}");
        }
    }

    /// A quota is a resource gate, and a gate that cannot read what it is
    /// gating must treat it as the dangerous class. A corrupt tenant record used
    /// to yield `Quotas::default()` — the *generous* ceiling — on the strength
    /// of a comment claiming `require_live_tenant` had already refused upstream.
    /// It had not: that helper is wired only to the principal/binding arms, and
    /// neither `PutImposter` nor `PatchStubs` goes through it. So the operator's
    /// real ceiling silently became 1000 exactly when it became unreadable.
    #[tokio::test]
    async fn a_corrupt_tenant_record_refuses_a_write_rather_than_granting_the_default_quota() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            tenant_put_with_quotas(
                1,
                "acme",
                Quotas {
                    max_imposters: 1,
                    ..Quotas::default()
                },
                0,
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

        let refused = apply_one(&mut sm, 2, put_in_tenant(2, "acme", 8080)).await;
        let ControlOutcome::Failed { reason } = &refused.outcome else {
            panic!("an unreadable quota must refuse, never fall open: {refused:?}");
        };
        assert!(
            reason.contains("unreadable") || reason.contains("corrupt"),
            "the refusal must say the quota could not be read: {reason}"
        );
        assert!(
            sm.read_config(DEFAULT_TENANT, 8080)
                .expect("read")
                .is_none(),
            "nothing may land while the ceiling is unknown"
        );

        // And the same for the edit path, which reaches `quotas_for` directly.
        let refused = apply_one(
            &mut sm,
            3,
            request(
                3,
                ControlOp::PatchStubs {
                    tenant: TenantId::new("acme"),
                    port: 8080,
                    edit: StubEditScript(vec![StubEdit::Add {
                        stub: serde_json::from_value(json!({ "id": "a" }))
                            .expect("test stub parses"),
                        index: None,
                    }]),
                },
            ),
        )
        .await;
        assert!(
            matches!(refused.outcome, ControlOutcome::Failed { .. }),
            "the edit path must fail closed too: {refused:?}"
        );
    }

    /// A ceiling that one write path ignores is not a ceiling. Enforcing quotas
    /// only on `PutImposter`/`PatchStubs` let a tenant capped at N imposters hold
    /// far more by pointing a **source** at a document declaring them — the
    /// quieter half of the drift `validate_replicable_config` exists to prevent,
    /// since nobody typed the port.
    #[tokio::test]
    async fn a_source_pull_cannot_carry_a_tenant_past_its_imposter_ceiling() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            tenant_put_with_quotas(
                1,
                "acme",
                Quotas {
                    max_imposters: 2,
                    ..Quotas::default()
                },
                0,
            ),
        )
        .await;
        apply_one(
            &mut sm,
            2,
            source_put_in(2, "acme", "mocks", "https://h/i.json"),
        )
        .await;

        let refused = apply_one(
            &mut sm,
            3,
            pull_in(3, "acme", "mocks", &[9001, 9002, 9003], 1),
        )
        .await;
        let ControlOutcome::Failed { reason } = &refused.outcome else {
            panic!("three imposters against a ceiling of two must refuse: {refused:?}");
        };
        assert!(reason.contains('2'), "{reason}");
        assert_eq!(
            ports_in_tenant(&sm, "acme"),
            Vec::<u16>::new(),
            "a refused pull must be a *whole* refusal — no half-applied set"
        );

        // Inside the ceiling: applies.
        let applied = apply_one(&mut sm, 4, pull_in(4, "acme", "mocks", &[9001, 9002], 1)).await;
        assert_eq!(applied.outcome, ControlOutcome::Applied);
        assert_eq!(ports_in_tenant(&sm, "acme"), vec![9001, 9002]);
    }

    /// The stub ceiling binds the pull path too.
    #[tokio::test]
    async fn a_source_pull_cannot_carry_an_imposter_past_the_stub_ceiling() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            tenant_put_with_quotas(
                1,
                "acme",
                Quotas {
                    max_stubs_per_imposter: 1,
                    ..Quotas::default()
                },
                0,
            ),
        )
        .await;
        apply_one(
            &mut sm,
            2,
            source_put_in(2, "acme", "mocks", "https://h/i.json"),
        )
        .await;

        let refused = apply_one(&mut sm, 3, pull_in(3, "acme", "mocks", &[9010], 2)).await;
        let ControlOutcome::Failed { reason } = &refused.outcome else {
            panic!("two stubs against a ceiling of one must refuse: {refused:?}");
        };
        assert!(reason.contains("9010"), "{reason}");
        assert_eq!(
            ports_in_tenant(&sm, "acme"),
            Vec::<u16>::new(),
            "nothing lands from a refused pull"
        );
    }

    /// The audit projection lives in the `Normal` arm, so entries that carry no
    /// `ControlOp` must produce no rows at all. This is the other half of
    /// exactly-once: not just "no duplicates", but "nothing invented".
    #[tokio::test]
    async fn a_blank_entry_produces_no_audit_row() {
        let (_td, mut sm) = fresh_sm(None).await;
        let blank = Entry::<TypeConfig> {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Blank,
        };
        sm.apply(vec![blank, entry(2, put(7, 8080, json!([{ "id": "a" }])))])
            .await
            .expect("apply");

        let rows = audit_rows(&sm);
        assert_eq!(
            rows.len(),
            1,
            "only the write is audited; a Blank entry is not a write: {rows:?}"
        );
        assert_eq!(rows[0].revision, 2);
    }

    /// Every other test here applies one entry at a time. A committed batch is
    /// the shape that would break a projection keyed on anything batch-scoped,
    /// and `gc_audit`'s clock genuinely *is* batch-scoped — so the batch case is
    /// worth its own assertion rather than an assumption.
    #[tokio::test]
    async fn every_entry_in_one_committed_batch_gets_its_own_audit_row() {
        let (_td, mut sm) = fresh_sm(None).await;
        let batch: Vec<_> = (1u64..=3)
            .map(|i| {
                entry(
                    i,
                    put(u128::from(i), 8080 + i as u16, json!([{ "id": "a" }])),
                )
            })
            .collect();
        let responses = sm.apply(batch).await.expect("apply");
        assert_eq!(responses.len(), 3);

        let rows = audit_rows(&sm);
        assert_eq!(
            rows.iter().map(|r| r.revision).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "one row per entry, at its own revision: {rows:?}"
        );
        assert_eq!(
            rows.iter().map(|r| r.resource.as_str()).collect::<Vec<_>>(),
            vec!["8081", "8082", "8083"]
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

    /// §11 open question 2, pinned: `journal_retention_secs` is a per-tenant
    /// policy on the tenant record, not a field of `Quotas`. Stored now, applied
    /// by the M3 shards (#147) — so this asserts it round-trips, which is the
    /// whole of what this slice owes it.
    #[tokio::test]
    async fn journal_retention_round_trips_on_the_tenant_record_not_in_quotas() {
        let (_td, mut sm) = fresh_sm(None).await;
        apply_one(
            &mut sm,
            1,
            tenant_put_with_quotas(1, "acme", Quotas::default(), 3_600),
        )
        .await;

        let tenant = sm
            .tenant("acme")
            .expect("read tenant")
            .expect("tenant present");
        assert_eq!(tenant.journal_retention_secs, 3_600);
    }

    // -- issue #182: tenant-aware reads and engine sync ------------------------

    /// Two tenants, one imposter each on distinct ports: each tenant's
    /// `read_config` sees only its own port, never the other's.
    #[tokio::test]
    async fn read_config_is_isolated_per_tenant() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(
            1,
            put_in(1, "acme", 19001, json!([{ "id": "a" }])),
        )])
        .await
        .expect("apply acme");
        sm.apply(vec![entry(
            2,
            put_in(2, "globex", 19002, json!([{ "id": "b" }])),
        )])
        .await
        .expect("apply globex");

        assert!(
            sm.read_config("acme", 19001).expect("read").is_some(),
            "acme sees its own port"
        );
        assert!(
            sm.read_config("acme", 19002).expect("read").is_none(),
            "acme must not see globex's port"
        );
        assert!(
            sm.read_config("globex", 19002).expect("read").is_some(),
            "globex sees its own port"
        );
        assert!(
            sm.read_config("globex", 19001).expect("read").is_none(),
            "globex must not see acme's port"
        );
    }

    /// `desired_configs` (the engine-sync read) is the union of every tenant's
    /// configs, not just the default tenant's — one shared `ImposterManager`
    /// binds them all because ports are fleet-unique across tenants
    /// (RFC-002 §3.2).
    #[tokio::test]
    async fn engine_sync_binds_the_union_of_every_tenant() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(engine.clone())).await;

        sm.apply(vec![
            entry(1, put_in(1, "acme", 19011, json!([{ "id": "a" }]))),
            entry(2, put_in(2, "globex", 19012, json!([{ "id": "b" }]))),
        ])
        .await
        .expect("apply");

        assert_eq!(
            engine.count(),
            2,
            "the engine must bind both tenants' imposters from one sync"
        );
        assert_eq!(engine_stub_ids(&engine, 19011), vec!["a"]);
        assert_eq!(engine_stub_ids(&engine, 19012), vec!["b"]);

        engine.shutdown().await;
    }

    /// `desired_routes` compiles the **default tenant's routes only** — unlike `desired_configs`,
    /// which unions.
    ///
    /// This asymmetry is a security property, not an oversight, so it is pinned rather than left
    /// to a comment. A front-door request carries no tenant identity (RFC-002 §7 keeps the data
    /// plane anonymous), so a unioned table would be one shared matching namespace: an empty match
    /// is a legal catch-all and `priority` is an unbounded `i32`, so any tenant could publish
    /// `{match: {}, priority: i32::MAX}` and swallow every other tenant's front-door traffic
    /// fleet-wide. Flip `desired_routes` back to a union and the second half of this test goes red.
    #[tokio::test]
    async fn route_sync_compiles_only_the_default_tenants_routes() {
        let (_td, mut sm, routes) = fresh_sm_with_routes().await;

        sm.apply(vec![entry(
            1,
            put_routes_in(1, DEFAULT_TENANT, vec![test_route("default-a", 19021)]),
        )])
        .await
        .expect("apply default routes");
        sm.apply(vec![entry(
            2,
            put_routes_in(2, "globex", vec![test_route("globex-a", 19022)]),
        )])
        .await
        .expect("apply globex routes");

        let loaded = routes.load();
        let default_route = loaded
            .resolve(
                None,
                &hyper::Method::GET,
                "/default-a",
                &hyper::HeaderMap::new(),
            )
            .expect("the default tenant's route compiles");
        assert_eq!(default_route.target.port, 19021);
        assert!(
            loaded
                .resolve(
                    None,
                    &hyper::Method::GET,
                    "/globex-a",
                    &hyper::HeaderMap::new(),
                )
                .is_none(),
            "a non-default tenant's route must NOT reach the shared front door — it would be \
             matching in a namespace every other tenant also routes through"
        );

        // Stored, though — `route_table` reads it back, so a tenant still sees what it wrote.
        let stored = sm.route_table("globex").expect("read globex's table");
        assert_eq!(
            stored.routes.len(),
            1,
            "globex's route is stored, just not compiled"
        );
    }

    /// `owning_tenant` resolves a fleet-unique port to the tenant that holds
    /// it, and `None` for a port nobody has configured.
    #[tokio::test]
    async fn owning_tenant_resolves_the_right_tenant_and_none_when_unconfigured() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(1, put_in(1, "acme", 19031, json!([])))])
            .await
            .expect("apply acme");
        sm.apply(vec![entry(2, put_in(2, "globex", 19032, json!([])))])
            .await
            .expect("apply globex");

        assert_eq!(
            sm.owning_tenant(19031).expect("read"),
            Some(TenantId::new("acme"))
        );
        assert_eq!(
            sm.owning_tenant(19032).expect("read"),
            Some(TenantId::new("globex"))
        );
        assert_eq!(
            sm.owning_tenant(19099).expect("read"),
            None,
            "an unconfigured port owns nothing"
        );
    }

    /// `tenant_config_usage` builds every tenant's usage from **one** call —
    /// not one scan per tenant (issue #372, AC7). A single `sm.tenant_config_usage()`
    /// here must already carry the right imposter count, the right ports, and
    /// the right worst-single-imposter stub count for every tenant at once;
    /// there is no second call this test could make that a per-tenant-scan
    /// implementation would need and a single-scan one would not.
    #[tokio::test]
    async fn the_listing_scans_the_config_table_once() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(
            1,
            put_in(1, "acme", 19051, json!([{"id": "s1"}, {"id": "s2"}])),
        )])
        .await
        .expect("apply acme imposter 1");
        sm.apply(vec![entry(
            2,
            put_in(
                2,
                "acme",
                19052,
                json!([{"id": "s1"}, {"id": "s2"}, {"id": "s3"}, {"id": "s4"}, {"id": "s5"},
                       {"id": "s6"}, {"id": "s7"}]),
            ),
        )])
        .await
        .expect("apply acme imposter 2");
        sm.apply(vec![entry(
            3,
            put_in(3, "globex", 19053, json!([{"id": "only"}])),
        )])
        .await
        .expect("apply globex imposter");

        let usage = sm.tenant_config_usage().expect("one-scan usage");

        let acme = usage.get("acme").expect("acme present");
        assert_eq!(acme.imposters, 2, "acme holds two imposters");
        assert_eq!(
            acme.max_stubs, 7,
            "the worst imposter (7 stubs), not the sum (9)"
        );
        let mut acme_ports = acme.ports.clone();
        acme_ports.sort_unstable();
        assert_eq!(acme_ports, vec![19051, 19052]);

        let globex = usage.get("globex").expect("globex present, same call");
        assert_eq!(globex.imposters, 1);
        assert_eq!(globex.max_stubs, 1);
        assert_eq!(globex.ports, vec![19053]);

        assert!(
            !usage.contains_key("unconfigured-tenant"),
            "a tenant with no config rows has no entry, not a zeroed one"
        );
    }

    /// Issue #372: a corrupt `sm_configs` row must not silently shrink
    /// `imposters`/`max_stubs`/`ports` with nothing to say so — the skip has
    /// to leave a mark `dispatch` can turn into `Rift-Cluster-Partial`.
    #[tokio::test]
    async fn a_corrupt_config_row_marks_the_tenant_usage_partial() {
        let (_td, mut sm) = fresh_sm(None).await;
        sm.apply(vec![entry(
            1,
            put_in(1, "acme", 19061, json!([{"id": "a"}, {"id": "b"}])),
        )])
        .await
        .expect("apply acme's readable imposter");
        // A second, corrupt row for acme — bypassing validation, since the
        // broken-record path is unreachable through the public API.
        sm.inject_raw_config("acme", 19062, "not json");
        // A wholly separate tenant, readable end to end, to prove one
        // tenant's corruption does not spill onto another's figures.
        sm.apply(vec![entry(
            2,
            put_in(2, "globex", 19063, json!([{"id": "only"}])),
        )])
        .await
        .expect("apply globex's readable imposter");

        let usage = sm
            .tenant_config_usage()
            .expect("a corrupt row must not fail the scan");

        let acme = usage.get("acme").expect("acme present");
        assert_eq!(
            acme.imposters, 1,
            "the corrupt row is excluded from the count, not counted as one"
        );
        assert_eq!(
            acme.ports,
            vec![19061],
            "the corrupt row's port never reaches `ports`"
        );
        assert!(
            acme.incomplete,
            "a skipped row must mark this tenant's usage incomplete, not just log and move on"
        );

        let globex = usage.get("globex").expect("globex present");
        assert_eq!(globex.imposters, 1);
        assert!(
            !globex.incomplete,
            "a tenant with no corrupt rows of its own must not inherit another tenant's flag"
        );
    }

    /// The union must preserve `desired_configs`'s existing abort semantics:
    /// one broken record — in *any* tenant, not just the one being written —
    /// still refuses the whole engine sync rather than silently shrinking it.
    #[tokio::test]
    async fn a_broken_record_in_one_tenant_still_aborts_the_whole_sync() {
        let engine = Arc::new(ImposterManager::new());
        let (_td, mut sm) = fresh_sm(Some(engine.clone())).await;
        sm.apply(vec![entry(
            1,
            put_in(1, "acme", 19041, json!([{ "id": "a" }])),
        )])
        .await
        .expect("apply acme");
        assert_eq!(engine.count(), 1);

        // Corrupt a *different* tenant's record directly, bypassing
        // validation (the broken-record path is unreachable through the
        // public API, like `a_broken_stored_record_refuses_sync_instead_of_deleting`).
        sm.inject_raw_config("globex", 19042, "not json");

        let responses = sm
            .apply(vec![entry(2, put_in(2, "acme", 19043, json!([])))])
            .await
            .expect("apply still succeeds — the refusal is engine status");
        assert_eq!(responses, vec![ControlResponse::applied(2)]);
        assert_eq!(
            engine.count(),
            1,
            "globex's broken record must abort the union sync fleet-wide: \
             acme's new imposter must not be created by a partial sync, and \
             acme's live one must not be torn down"
        );
        assert_eq!(engine_stub_ids(&engine, 19041), vec!["a"]);
        assert!(
            sm.apply_failures().contains_key(&19042),
            "the broken record is surfaced as node status: {:?}",
            sm.apply_failures()
        );

        engine.shutdown().await;
    }
}
