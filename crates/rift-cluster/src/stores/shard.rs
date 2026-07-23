//! The durable flow-state tier: one redb file per node, three durability modes.
//!
//! # Why redb, honestly
//!
//! The control plane already depends on it (ADR-001), so persisting flow state
//! here costs no new dependency and no second storage engine to operate. The
//! design originally argued something stronger — that redb's per-commit
//! `Durability::{None, Eventual, Immediate}` mapped 1:1 onto the knob below —
//! but `Eventual` does not exist at the pinned version (redb 4.1 has `None` and
//! `Immediate`, and dropped `Eventual` several majors ago). See the correction
//! on issue #119.
//!
//! # The three modes
//!
//! | mode | commit | loses |
//! |---|---|---|
//! | [`Durability::Sync`] | `Immediate` **before the ack returns** | nothing |
//! | [`Durability::Async`] | `None`, made durable by the ticker | ≤ one fsync interval, and only if every holder dies inside it |
//! | [`Durability::None`] | redb is never opened | everything, by choice |
//!
//! `Async` works because a redb `None` commit is explicitly *not durable until a
//! later `Immediate` commit* — so a background ticker issuing one `Immediate`
//! per interval is what makes a batch of `None` commits durable, all at once.
//! That is group commit, spelled with the two levels redb actually has.
//!
//! # A separate file from the control plane
//!
//! `flow.redb` sits beside the ADR-001 control tables in `--cluster-state-dir`,
//! never inside the same file. Control commits are always `Immediate`; flow
//! fsync is tunable and, in `Async`, deliberately lazy. One file would put both
//! on one commit lock, so the control plane would pay flow state's write volume
//! and flow state would pay the control plane's fsync policy.
//!
//! # Eviction sheds whole flows
//!
//! Never single keys. A scenario whose `step` key survived while its `cart` key
//! was evicted is worse than one that is plainly gone: the first produces a test
//! failure that looks like a product bug, the second looks like what it is.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::metrics;

/// `(flow_id, key)` → the versioned entry, JSON-encoded.
const FLOW_KV: TableDefinition<(&str, &str), &str> = TableDefinition::new("flow_kv");
/// `flow_id` → `{ last_touch, entry_count }`, JSON-encoded. Drives TTL and LRU
/// without scanning `FLOW_KV`.
const FLOW_META: TableDefinition<&str, &str> = TableDefinition::new("flow_meta");

/// How many queued writes one transaction may absorb. Bounds the worst-case
/// latency a `Sync` op can be made to wait behind other people's work.
const MAX_BATCH: usize = 256;

/// Per-imposter durability for flow-state writes.
///
/// `Async` is the default: it keeps the hot path off the disk while bounding
/// loss to one fsync interval, which is the right trade for state that is
/// valuable but reconstructible by re-running a test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Durability {
    /// In memory only. redb is never opened; a restart loses everything.
    None,
    /// Committed without fsync; the ticker makes it durable within one interval.
    #[default]
    Async,
    /// fsynced before the write is acknowledged.
    Sync,
}

/// What a shard needs to know about the file it owns.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// How often the writer makes accumulated `Async` commits durable.
    pub fsync_interval: Duration,
    /// Flows held before the least recently touched are shed. Whole flows.
    pub max_flows: usize,
    /// How long a flow survives without being touched. `None` disables expiry.
    pub ttl: Option<Duration>,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            fsync_interval: Duration::from_millis(50),
            max_flows: 100_000,
            ttl: Some(Duration::from_secs(300)),
        }
    }
}

/// A value plus the version triple ownership changes are resolved by.
///
/// `(m_idx, v, origin)` is compared lexicographically and highest wins: `m_idx`
/// is the Raft-applied membership index (so an op minted under a stale
/// membership loses to one minted after the change), `v` is the per-key counter,
/// and `origin` breaks ties between two nodes that minted the same `v` — which
/// can only happen across an ownership change, and is why the tuple is ordered
/// this way rather than by `v` alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Versioned {
    pub m_idx: u64,
    pub v: u64,
    pub origin: u64,
    /// Unix millis. `0` means no expiry.
    pub expires_at: u64,
    pub value: serde_json::Value,
    /// A **tombstone** (#126): the versioned record that this key was deleted.
    /// Readers ([`FlowShard::get`]) treat it as absence; replication and
    /// adoption ([`FlowShard::flow`]) carry it, so a delayed `Put` push loses
    /// to it by the ordinary version comparison instead of resurrecting the
    /// key. Tombstones carry a finite `expires_at`, so the sweep reaps them.
    ///
    /// `serde(default)` is what keeps #119's disk rows readable: an absent
    /// field is "not deleted", so a shard written before tombstones existed
    /// recovers unchanged.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deleted: bool,
}

impl Versioned {
    /// Whether `self` should be replaced by `other` on adoption or replication.
    #[must_use]
    pub fn superseded_by(&self, other: &Self) -> bool {
        (other.m_idx, other.v, other.origin) > (self.m_idx, self.v, self.origin)
    }
}

/// One flow's live keys plus when it was last written.
///
/// `last_touch` is what makes eviction actual LRU. It has to be tracked
/// separately from the entries' `expires_at`: two flows can share a TTL yet have
/// been touched minutes apart, and a no-TTL flow (`expires_at == 0`) has no
/// expiry to sort by at all — ordering by `expires_at` would evict exactly the
/// permanent flows first.
#[derive(Default)]
struct FlowMem {
    keys: HashMap<String, Versioned>,
    last_touch: u64,
}

/// The `flow_meta` row, so `last_touch` survives a restart and recovery restores
/// LRU order rather than resetting every flow to "just loaded".
#[derive(Debug, Serialize, Deserialize)]
struct FlowMeta {
    last_touch: u64,
}

/// The in-memory mirror, shared between the read-serving handle and the writer
/// thread's expiry sweep.
type Memory = Arc<parking_lot::RwLock<HashMap<String, FlowMem>>>;

/// Anything that can go wrong that is not a bug in this module.
#[derive(Debug, thiserror::Error)]
pub enum ShardError {
    #[error("flow shard storage: {0}")]
    Storage(String),
    #[error("flow shard encoding: {0}")]
    Encoding(String),
    /// The writer task is gone, which only happens on shutdown.
    #[error("flow shard writer stopped")]
    WriterStopped,
}

/// One mutation, as handed to the writer.
#[derive(Debug)]
enum Mutation {
    Set {
        flow_id: String,
        key: String,
        entry: Versioned,
    },
    Delete {
        flow_id: String,
        key: String,
    },
    ClearFlow {
        flow_id: String,
    },
}

struct WriteRequest {
    mutation: Mutation,
    durability: Durability,
    /// Present only for a `Sync` write, which is the only caller that waits.
    /// `Async` allocates no channel — it is fire-and-forget by construction, and
    /// a per-write oneshot it never reads is pure overhead on the hot path.
    ack: Option<oneshot::Sender<Result<(), ShardError>>>,
}

/// This node's flow-state slice.
///
/// Cloneable and shared: the handle is thin, the state lives behind the writer
/// task and (for reads) the database itself.
#[derive(Clone)]
pub struct FlowShard {
    inner: Arc<Inner>,
}

struct Inner {
    /// In-memory mirror. Serves every read, so a read never contends with the
    /// writer's transaction — and it is the *only* reader: the redb `Database`
    /// itself is owned solely by the writer thread, so joining that thread
    /// (via [`FlowShard::close`] or drop) is what releases the file lock. A copy
    /// of the handle here would hold the lock open past `close`, so there is not
    /// one.
    memory: Memory,
    /// Behind a lock so [`FlowShard::close`] can drop it — dropping the last
    /// sender is what ends the writer loop. `None` for an in-memory shard, and
    /// after `close`.
    writer: parking_lot::Mutex<Option<mpsc::UnboundedSender<WriteRequest>>>,
    /// The writer thread's handle, taken by [`FlowShard::close`] to join it —
    /// which is also what releases the redb file lock. Behind a `Mutex<Option>`
    /// because `close` takes `&self` (the handle is shared) and must run once.
    writer_thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    config: ShardConfig,
}

impl FlowShard {
    /// Open (or create) the shard at `dir/flow.redb` and start its writer.
    ///
    /// Recovery happens here: expired flows are dropped before the shard is
    /// handed back, so a node never serves state it would have expired had it
    /// stayed up, and never offers it to a peer pulling a range.
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be opened or its contents cannot be decoded.
    pub fn open(dir: &Path, config: ShardConfig) -> Result<Self, ShardError> {
        let path = dir.join("flow.redb");
        let db = Database::create(&path).map_err(|e| ShardError::Storage(e.to_string()))?;

        let memory: Memory = Arc::new(parking_lot::RwLock::new(recover(&db, &config)?));

        let (tx, rx) = mpsc::unbounded_channel();

        // The writer owns the `Database` outright — no shared handle — so joining
        // it is the single point at which the file lock is released. It also owns
        // its own single-thread runtime rather than `tokio::spawn`-ing onto the
        // caller's: `open` is called from `RaftStateMachine::apply` and from
        // plain reopen paths that are not inside a runtime at all, and a
        // dedicated thread keeps the fsync ticker off the shared worker pool
        // where a stall would be one imposter starving every other task.
        let interval = config.fsync_interval;
        let writer_memory = Arc::clone(&memory);
        let handle = std::thread::Builder::new()
            .name("flow-shard-writer".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .expect("flow shard writer runtime");
                rt.block_on(writer_loop(db, rx, interval, writer_memory));
            })
            .map_err(|e| ShardError::Storage(e.to_string()))?;

        let inner = Arc::new(Inner {
            memory,
            writer: parking_lot::Mutex::new(Some(tx)),
            writer_thread: std::sync::Mutex::new(Some(handle)),
            config,
        });

        Ok(Self { inner })
    }

    /// A shard that never touches disk. Restarting loses everything, which is
    /// the point: this is the mode for imposters that live and die with one CI
    /// run.
    #[must_use]
    pub fn in_memory(config: ShardConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                memory: Arc::new(parking_lot::RwLock::new(HashMap::new())),
                writer: parking_lot::Mutex::new(None),
                writer_thread: std::sync::Mutex::new(None),
                config,
            }),
        }
    }

    /// Stop the writer and wait for it to release the file.
    ///
    /// redb holds an exclusive lock for the life of the `Database`, and the
    /// writer thread holds the last handle to it. Dropping the shard *asks* the
    /// thread to stop but does not wait, so a caller that reopens the same path
    /// immediately — recovery after a controlled restart, or a test — can race
    /// the lock. `close` makes the release synchronous: drop the sender so the
    /// loop ends, then join the thread, which drops its `Database` handle.
    ///
    /// Idempotent, and a plain drop without it is still safe — the thread exits
    /// on its own, just not before this returns.
    pub fn close(&self) {
        // Drop the sender first: the writer's `rx.recv()` then returns `None`
        // and the loop breaks.
        *self.inner.writer.lock() = None;
        if let Some(handle) = self
            .inner
            .writer_thread
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
        {
            let _ = handle.join();
        }
    }

    /// Read a key. Never blocks on the writer.
    ///
    /// A tombstone reads as absence: the deletion *record* is replication's
    /// business ([`Self::flow`]), never the reader's.
    #[must_use]
    pub fn get(&self, flow_id: &str, key: &str) -> Option<Versioned> {
        let now = now_millis();
        let guard = self.inner.memory.read();
        guard
            .get(flow_id)?
            .keys
            .get(key)
            .filter(|entry| is_live(entry, now) && !entry.deleted)
            .cloned()
    }

    /// Read a key *including* a live tombstone — the version-comparison view
    /// merges need, where "deleted at v5" must beat "set at v4".
    #[must_use]
    pub fn get_versioned(&self, flow_id: &str, key: &str) -> Option<Versioned> {
        let now = now_millis();
        let guard = self.inner.memory.read();
        guard
            .get(flow_id)?
            .keys
            .get(key)
            .filter(|entry| is_live(entry, now))
            .cloned()
    }

    /// Every live key of a flow, for a replica pull or an adoption range read.
    #[must_use]
    pub fn flow(&self, flow_id: &str) -> Vec<(String, Versioned)> {
        let now = now_millis();
        let guard = self.inner.memory.read();
        guard
            .get(flow_id)
            .map(|flow| {
                flow.keys
                    .iter()
                    .filter(|(_, e)| is_live(e, now))
                    .map(|(k, e)| (k.clone(), e.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// How many flows are held. Exposed for the eviction assertions.
    #[must_use]
    pub fn flow_count(&self) -> usize {
        self.inner.memory.read().len()
    }

    /// Every flow id this shard holds — the anti-entropy loop's worklist
    /// (#126). A snapshot, not a view: the loop iterates it while writes land.
    #[must_use]
    pub fn flow_ids(&self) -> Vec<String> {
        self.inner.memory.read().keys().cloned().collect()
    }

    /// Write a key at `durability`, returning once that level is satisfied.
    ///
    /// The in-memory mirror is updated before the durable write is awaited, so a
    /// concurrent read on this node sees the write immediately regardless of
    /// mode. That is deliberate and is what makes `Async` cheap: durability is
    /// about surviving a restart, not about when a local reader observes it.
    ///
    /// # Errors
    ///
    /// Propagates a storage failure. A failed durable write leaves the memory
    /// mirror updated and the caller informed — it does **not** silently succeed.
    pub async fn set(
        &self,
        flow_id: &str,
        key: &str,
        entry: Versioned,
        durability: Durability,
    ) -> Result<(), ShardError> {
        {
            let mut guard = self.inner.memory.write();
            let flow = guard.entry(flow_id.to_owned()).or_default();
            flow.keys.insert(key.to_owned(), entry.clone());
            flow.last_touch = now_millis();
        }
        self.evict_if_needed();

        self.submit(
            Mutation::Set {
                flow_id: flow_id.to_owned(),
                key: key.to_owned(),
                entry,
            },
            durability,
        )
        .await
    }

    /// Remove a key.
    ///
    /// # Errors
    ///
    /// Propagates a storage failure.
    pub async fn delete(
        &self,
        flow_id: &str,
        key: &str,
        durability: Durability,
    ) -> Result<(), ShardError> {
        {
            let mut guard = self.inner.memory.write();
            if let Some(flow) = guard.get_mut(flow_id) {
                flow.keys.remove(key);
                flow.last_touch = now_millis();
                if flow.keys.is_empty() {
                    guard.remove(flow_id);
                }
            }
        }
        self.submit(
            Mutation::Delete {
                flow_id: flow_id.to_owned(),
                key: key.to_owned(),
            },
            durability,
        )
        .await
    }

    /// Drop a whole flow — the only granularity eviction ever uses.
    ///
    /// # Errors
    ///
    /// Propagates a storage failure.
    pub async fn clear_flow(
        &self,
        flow_id: &str,
        durability: Durability,
    ) -> Result<(), ShardError> {
        self.inner.memory.write().remove(flow_id);
        self.submit(
            Mutation::ClearFlow {
                flow_id: flow_id.to_owned(),
            },
            durability,
        )
        .await
    }

    async fn submit(&self, mutation: Mutation, durability: Durability) -> Result<(), ShardError> {
        // `Durability::None` and an in-memory shard are the same thing to the
        // writer: there is nothing to persist, so there is nothing to wait for.
        if durability == Durability::None {
            return Ok(());
        }

        // Only `Sync` waits, so only `Sync` pays for a channel.
        let wait = match durability {
            Durability::Sync => {
                let (ack, wait) = oneshot::channel();
                self.send(WriteRequest {
                    mutation,
                    durability,
                    ack: Some(ack),
                })?;
                Some(wait)
            }
            Durability::Async | Durability::None => {
                self.send(WriteRequest {
                    mutation,
                    durability,
                    ack: None,
                })?;
                None
            }
        };

        match wait {
            // The ack is the whole contract: it returns only after the fsync.
            Some(wait) => wait.await.map_err(|_| ShardError::WriterStopped)?,
            // Queued is enough. Waiting would make `Async` synchronous with the
            // batch it landed in, which is the cost the mode exists to avoid.
            None => Ok(()),
        }
    }

    /// Hand one request to the writer, or report it gone. `Ok` on an in-memory
    /// shard: memory already holds the write.
    fn send(&self, request: WriteRequest) -> Result<(), ShardError> {
        let guard = self.inner.writer.lock();
        match guard.as_ref() {
            Some(writer) => writer.send(request).map_err(|_| ShardError::WriterStopped),
            None => Ok(()),
        }
    }

    /// Shed whole flows, least-recently-touched first, until the cap holds.
    fn evict_if_needed(&self) {
        // Select and remove under one write lock. Computing victims under a read
        // lock and dropping it before removing them lets a concurrent `set` land
        // a flow that is then evicted from under it — the victim list must be
        // decided against the same map it is applied to.
        let victims: Vec<String> = {
            let mut guard = self.inner.memory.write();
            let over = guard.len().saturating_sub(self.inner.config.max_flows);
            if over == 0 {
                return;
            }
            let mut by_age: Vec<(String, u64)> = guard
                .iter()
                .map(|(id, flow)| (id.clone(), flow.last_touch))
                .collect();
            // Oldest touch first — genuine LRU, unaffected by whether a flow has
            // a TTL at all.
            by_age.sort_by_key(|(_, touched)| *touched);

            by_age
                .into_iter()
                .take(over)
                .map(|(id, _)| {
                    guard.remove(&id);
                    id
                })
                .collect()
        };

        metrics::flow_flows_evicted(victims.len());

        // The durable copy is cleared best-effort in the background: eviction is
        // a capacity decision, and blocking a write on it would make the shard
        // slowest exactly when it is fullest. A crash before these land means
        // recovery re-reads flows that were over the cap, and the next write
        // evicts them again.
        let guard = self.inner.writer.lock();
        if let Some(writer) = guard.as_ref() {
            for id in victims {
                let _ = writer.send(WriteRequest {
                    mutation: Mutation::ClearFlow { flow_id: id },
                    durability: Durability::Async,
                    ack: None,
                });
            }
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // The last handle is going away, so nothing can reopen this path from
        // *this* process until the writer's `Database` is dropped. Drop the
        // sender to end the loop and join, so a same-process reopen after the
        // shard is gone does not race the file lock. This runs only on the final
        // `Arc` drop; a caller that wants a *guaranteed* release before its own
        // reopen calls [`FlowShard::close`] explicitly.
        let sender = self.writer.lock().take();
        drop(sender);
        if let Some(handle) = self
            .writer_thread
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
        {
            let _ = handle.join();
        }
    }
}

/// Whether an entry is still live at `now`. `expires_at == 0` means no expiry.
fn is_live(entry: &Versioned, now: u64) -> bool {
    entry.expires_at == 0 || entry.expires_at > now
}

/// Read the file back into memory, dropping anything already expired.
fn recover(db: &Database, config: &ShardConfig) -> Result<HashMap<String, FlowMem>, ShardError> {
    let now = now_millis();
    let mut memory: HashMap<String, FlowMem> = HashMap::new();
    let mut replayed = 0usize;

    let txn = db
        .begin_read()
        .map_err(|e| ShardError::Storage(e.to_string()))?;
    let table = match txn.open_table(FLOW_KV) {
        Ok(table) => table,
        // A shard that has never been written has no table yet. That is an empty
        // shard, not a failure — every other table error is propagated.
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(memory),
        Err(e) => return Err(ShardError::Storage(e.to_string())),
    };

    for row in table
        .iter()
        .map_err(|e| ShardError::Storage(e.to_string()))?
    {
        let (k, v) = row.map_err(|e| ShardError::Storage(e.to_string()))?;
        let (flow_id, key) = k.value();
        let entry: Versioned =
            serde_json::from_str(v.value()).map_err(|e| ShardError::Encoding(e.to_string()))?;

        if !is_live(&entry, now) {
            continue;
        }
        memory
            .entry(flow_id.to_owned())
            .or_default()
            .keys
            .insert(key.to_owned(), entry);
        replayed += 1;
    }

    // Restore each flow's last-touch from `flow_meta`, so recovery comes up with
    // the LRU order it had rather than resetting everything to "just loaded". A
    // flow with no meta row (older data, or a write that never made it) falls
    // back to `now`.
    if let Ok(meta_table) = txn.open_table(FLOW_META) {
        for (flow_id, flow) in &mut memory {
            if let Ok(Some(row)) = meta_table.get(flow_id.as_str())
                && let Ok(meta) = serde_json::from_str::<FlowMeta>(row.value())
            {
                flow.last_touch = meta.last_touch;
            } else {
                flow.last_touch = now;
            }
        }
    } else {
        for flow in memory.values_mut() {
            flow.last_touch = now;
        }
    }

    // Bring an over-cap file back under the cap, oldest-touch first — the same
    // LRU rule the live path uses, so a shard cannot come up holding more than
    // it is allowed to.
    while memory.len() > config.max_flows {
        let oldest = memory
            .iter()
            .min_by_key(|(_, flow)| flow.last_touch)
            .map(|(id, _)| id.clone());
        match oldest {
            Some(id) => {
                memory.remove(&id);
            }
            None => break,
        }
    }

    metrics::flow_replayed(replayed);
    Ok(memory)
}

/// Drain mutations, batch them into one transaction, and make `Async` work
/// durable on a timer.
async fn writer_loop(
    db: Database,
    mut rx: mpsc::UnboundedReceiver<WriteRequest>,
    fsync_interval: Duration,
    memory: Memory,
) {
    let mut ticker = tokio::time::interval(fsync_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Set by any batch committed with `None`, cleared by the next `Immediate`.
    // This is the "acked but not yet fsynced" depth the WAL-lag gauge reports.
    let mut unsynced: usize = 0;

    loop {
        tokio::select! {
            first = rx.recv() => {
                let Some(first) = first else { break };
                let mut batch = vec![first];
                while batch.len() < MAX_BATCH {
                    match rx.try_recv() {
                        Ok(next) => batch.push(next),
                        Err(_) => break,
                    }
                }

                // One `Sync` op in the batch makes the whole batch durable.
                // Everyone in it gets the stronger guarantee they did not ask
                // for, which is free, and the `Sync` caller gets the one it did.
                let any_sync = batch.iter().any(|r| r.durability == Durability::Sync);
                let level = if any_sync {
                    redb::Durability::Immediate
                } else {
                    redb::Durability::None
                };

                let result = commit_batch(&db, &batch, level);
                match &result {
                    // An Immediate commit made every prior `None` op durable too,
                    // so the lag clears — but only if it actually succeeded.
                    Ok(()) if any_sync => unsynced = 0,
                    Ok(()) => unsynced += batch.len(),
                    // A failed commit reached no disk. `Async` writes carry no ack,
                    // so without this line the failure would vanish entirely while
                    // the memory mirror still served the write — a silent
                    // divergence on the next restart. Loud is the least this owes.
                    Err(e) => tracing::error!(
                        error = %e,
                        durable = any_sync,
                        batch = batch.len(),
                        "flow shard commit failed; these writes are not on disk"
                    ),
                }
                metrics::flow_wal_lag(unsynced);

                for request in batch {
                    // Only `Sync` writes carry an ack; `Async` is fire-and-forget.
                    if let Some(ack) = request.ack {
                        let _ = ack.send(match &result {
                            Ok(()) => Ok(()),
                            Err(e) => Err(ShardError::Storage(e.clone())),
                        });
                    }
                }
            }
            _ = ticker.tick() => {
                // The sweep drops expired entries from memory and hands their
                // keys back to be deleted from disk in this same tick. Without
                // it, expired entries are only *filtered* on read — they linger
                // in memory, count toward the cap, and can push live flows out.
                let expired = sweep_expired(&memory);

                // Nothing to fsync and nothing swept: the common idle tick.
                if unsynced == 0 && expired.is_empty() {
                    continue;
                }

                // One `Immediate` transaction does both jobs: it deletes the
                // expired keys AND, being Immediate, makes every preceding `None`
                // commit durable — redb's documented contract and the whole
                // mechanism behind `Async`.
                let started = std::time::Instant::now();
                match commit_deletes(&db, &expired) {
                    Ok(()) => {
                        metrics::flow_fsync_observed(started.elapsed());
                        unsynced = 0;
                        metrics::flow_wal_lag(0);
                    }
                    Err(e) => tracing::error!(error = %e, "flow shard fsync/sweep tick failed"),
                }
            }
        }
    }
}

/// Apply a batch inside one transaction. Returns the error text so every waiter
/// in the batch can be told the same thing.
fn commit_batch(
    db: &Database,
    batch: &[WriteRequest],
    level: redb::Durability,
) -> Result<(), String> {
    let started = std::time::Instant::now();
    let mut txn = db.begin_write().map_err(|e| e.to_string())?;
    txn.set_durability(level).map_err(|e| e.to_string())?;

    {
        let mut kv = txn.open_table(FLOW_KV).map_err(|e| e.to_string())?;
        let mut meta = txn.open_table(FLOW_META).map_err(|e| e.to_string())?;

        for request in batch {
            match &request.mutation {
                Mutation::Set {
                    flow_id,
                    key,
                    entry,
                } => {
                    let encoded = serde_json::to_string(entry).map_err(|e| e.to_string())?;
                    kv.insert((flow_id.as_str(), key.as_str()), encoded.as_str())
                        .map_err(|e| e.to_string())?;
                    // Persist the touch time so recovery restores LRU order. Read
                    // back in `recover`; not consulted on the live path (that
                    // uses the in-memory `last_touch`).
                    let encoded_meta = serde_json::to_string(&FlowMeta {
                        last_touch: now_millis(),
                    })
                    .map_err(|e| e.to_string())?;
                    meta.insert(flow_id.as_str(), encoded_meta.as_str())
                        .map_err(|e| e.to_string())?;
                }
                Mutation::Delete { flow_id, key } => {
                    kv.remove((flow_id.as_str(), key.as_str()))
                        .map_err(|e| e.to_string())?;
                }
                Mutation::ClearFlow { flow_id } => {
                    // `drain_filter` over a range would be tidier, but redb's
                    // range API borrows the table; collecting the keys first is
                    // the version that compiles and is still one transaction.
                    let doomed: Vec<String> = kv
                        .iter()
                        .map_err(|e| e.to_string())?
                        .filter_map(|row| row.ok())
                        .filter(|(k, _)| k.value().0 == flow_id.as_str())
                        .map(|(k, _)| k.value().1.to_owned())
                        .collect();
                    for key in doomed {
                        kv.remove((flow_id.as_str(), key.as_str()))
                            .map_err(|e| e.to_string())?;
                    }
                    meta.remove(flow_id.as_str()).map_err(|e| e.to_string())?;
                }
            }
        }
    }

    txn.commit().map_err(|e| e.to_string())?;
    // `redb::Durability` is not `PartialEq`, so the caller's intent is carried
    // rather than re-derived from the value.
    if matches!(level, redb::Durability::Immediate) {
        metrics::flow_fsync_observed(started.elapsed());
    }
    Ok(())
}

/// Remove expired entries from the memory mirror and return their keys so the
/// caller can delete them from disk. Empties whose last key expired are dropped
/// whole.
fn sweep_expired(memory: &Memory) -> Vec<(String, String)> {
    let now = now_millis();
    let mut deletes = Vec::new();
    let mut guard = memory.write();

    guard.retain(|flow_id, flow| {
        flow.keys.retain(|key, entry| {
            let live = is_live(entry, now);
            if !live {
                deletes.push((flow_id.clone(), key.clone()));
            }
            live
        });
        !flow.keys.is_empty()
    });

    deletes
}

/// Delete a set of keys in one `Immediate` transaction. An empty set still opens
/// and commits an Immediate transaction, because that is the fsync tick — the
/// empty commit is what makes prior `None` commits durable.
fn commit_deletes(db: &Database, deletes: &[(String, String)]) -> Result<(), String> {
    let mut txn = db.begin_write().map_err(|e| e.to_string())?;
    txn.set_durability(redb::Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut kv = txn.open_table(FLOW_KV).map_err(|e| e.to_string())?;
        for (flow_id, key) in deletes {
            kv.remove((flow_id.as_str(), key.as_str()))
                .map_err(|e| e.to_string())?;
        }
    }
    txn.commit().map_err(|e| e.to_string())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Millis since the epoch, `ttl` from now. `0` when there is no TTL.
#[must_use]
pub fn expiry_from(ttl: Option<Duration>) -> u64 {
    match ttl {
        Some(ttl) => {
            now_millis().saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
        }
        None => 0,
    }
}
