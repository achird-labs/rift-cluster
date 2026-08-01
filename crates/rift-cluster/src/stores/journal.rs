//! This node's writer shard of the fleet request journal (RFC-001 Ch.7, issue #222).
//!
//! Recorded requests are the one piece of cluster state that never conflicts: entries
//! from different nodes interleave rather than disagree. That buys a design with no
//! owners, no consensus and no coordination on the write side — every node appends to
//! **its own shard** and reads merge (issue #223).
//!
//! What lives here is only the local half: the shard, its writer-local caps, and the
//! read surface a peer merge consumes. There is no RPC, no peer pull and no disk —
//! journal entries are test-run-scoped and volatile by design (Ch.7, Ch.9's matrix).
//!
//! Entries are keyed `(node_id, seq, clear_gen)`:
//!
//! * `node_id` — the Raft node id of the writer, stable across restarts, and what a
//!   vector cursor (issue #225) addresses a shard position by.
//! * `seq` — this node's per-port monotone counter. It is also the index handed back
//!   to the engine, so the upstream `?since=` cursor and SSE `index` contracts hold
//!   unchanged on a single node.
//! * `clear_gen` — the port's clear generation at append time. Pinned at 0 here and
//!   bumped by issue #224; reserving the field now means generations change no storage
//!   shape later, and a merge can ignore-older-generation from the day it is written.
//!
//! Single-node fidelity is the exit gate: with one voter this is behaviourally identical
//! to the upstream `LocalJournal` it replaces, down to which reads report truncation —
//! with exactly one deliberate exception, the age cap. Upstream retains an entry until
//! the size cap evicts it; here an entry older than [`JournalConfig::max_age`] is dropped
//! and, like any retention eviction, advances the watermark. A single-node `?since=`
//! spanning that eviction therefore reports `truncated: true` where upstream would report
//! `false`. That is Ch.7's "plus an age cap", not an accident, and it is the one place
//! the parity claim is narrowed rather than absolute.

use crate::raft::NodeId;
use crate::raft::node::RaftNode;
use parking_lot::RwLock;
use prometheus::Gauge;
use rift_cluster_base::seams::{
    JournalEntry, JournalRead, JournalReadSince, MAX_RECORDED_REQUESTS, MatchOutcome,
    RecordedRequest, RequestJournal,
};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

/// Floor under the per-shard cap. Ch.7: `max(500, 10_000 / N)` — a large fleet must not
/// divide each writer's shard down to uselessness, because a shard too small to hold one
/// test's traffic makes the merged read lossy for everyone.
pub const MIN_SHARD_CAP: usize = 500;

/// How long an entry may sit in a shard before retention drops it. Journal entries back
/// in-run assertions, so an hours-old entry is memory held for nobody.
pub const DEFAULT_MAX_AGE: Duration = Duration::from_secs(600);

/// Where the shard cap's divisor comes from.
///
/// Late-bound, like `FlowNet`'s node slot: the imposter manager — and therefore the
/// journal — is built before the Raft node exists. Only the *divisor* is late; the
/// writer's identity is not, and is required at construction so no entry can ever be
/// stamped with a placeholder node id.
enum Voters {
    /// Applied Raft membership. `Weak` for the same reason `FlowNet` holds one: the
    /// journal must never keep the node (which owns ports and the redb lock) alive past
    /// shutdown.
    Node(Weak<RaftNode>),
    /// A fixed count, so the cap formula is testable without standing a Raft node up.
    #[cfg(test)]
    Fixed(usize),
}

impl Voters {
    fn count(&self) -> usize {
        match self {
            // Upgrade failing means the node was dropped while traffic is still being
            // recorded — a shutdown race, expected and benign. Falling back to one voter
            // only ever makes the cap *larger*, so a teardown over-retains rather than
            // evicting entries an in-flight assertion still wanted. It cannot corrupt the
            // merge key: the writer's node id is a plain field, not read from here.
            Self::Node(node) => node.upgrade().map_or(1, |node| node.voter_count().max(1)),
            #[cfg(test)]
            Self::Fixed(voters) => (*voters).max(1),
        }
    }
}

/// Monotonic time for the age cap, injectable so retention is testable without sleeping.
pub trait Clock: Send + Sync {
    fn now_millis(&self) -> u64;
}

/// Process-monotonic clock. Deliberately not wall-clock: retention must not be
/// steerable by clock skew, which is the same reason clears are generations (Ch.7).
pub struct MonotonicClock {
    base: Instant,
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self {
            base: Instant::now(),
        }
    }
}

impl Clock for MonotonicClock {
    fn now_millis(&self) -> u64 {
        u64::try_from(self.base.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

/// Retention policy for this node's shards.
#[derive(Debug, Clone)]
pub struct JournalConfig {
    /// Fleet-wide entries per port, divided among writers. Defaults to the upstream
    /// per-port cap so a single-voter cluster caps exactly where `LocalJournal` does.
    pub fleet_capacity: usize,
    /// Floor under the divided cap, so a large fleet cannot shrink a shard to uselessness.
    pub min_shard_cap: usize,
    /// How long an entry may sit in a shard. Values beyond `u64::MAX` milliseconds
    /// saturate, which disables the age cap rather than erroring — reachable only by an
    /// embedder passing a near-`Duration::MAX` value, which reads as "never expire".
    pub max_age: Duration,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            fleet_capacity: MAX_RECORDED_REQUESTS,
            min_shard_cap: MIN_SHARD_CAP,
            max_age: DEFAULT_MAX_AGE,
        }
    }
}

/// One recorded request in this node's shard, carrying the merge key issue #223 needs.
#[derive(Debug, Clone)]
pub struct ShardEntry {
    pub node_id: NodeId,
    pub seq: u64,
    pub clear_gen: u64,
    /// Resolved at record time so scoped clears need not re-derive it from stored headers.
    pub flow_id: String,
    pub request: RecordedRequest,
    /// Monotonic arrival time, for the age cap only. Not part of the merge key and not
    /// the ordering key a merge uses — that is the request's own recorded timestamp.
    recorded_at_millis: u64,
}

/// This node's shard of one port, as a peer merge consumes it (issue #223).
#[derive(Debug, Clone)]
pub struct ShardRead {
    pub entries: Vec<ShardEntry>,
    /// Highest seq dropped by retention pressure — **inclusive**: that seq itself is
    /// gone. So a reader at or above this value has seen everything eviction removed,
    /// and one below it has a hole. (Ch.7 names this field; upstream calls the identical
    /// quantity `evicted_through`, which is the less ambiguous reading of the two.)
    pub evicted_below_seq: u64,
    pub clear_gen: u64,
    /// This node's G-counter slot — summed across shards to answer `numberOfRequests`.
    pub count_slot: u64,
}

#[derive(Debug)]
struct PortShard {
    /// Ordered by `seq`, which is assigned under this same write lock — so deque order
    /// always matches seq order and [`ClusterJournal::attach_match`]'s binary search is
    /// exact rather than merely a heuristic.
    entries: RwLock<VecDeque<ShardEntry>>,
    /// This node's G-counter slot. Counts every request, recorded body or not.
    count: AtomicU64,
    /// Last seq handed out; 1-based, so 0 reads as "nothing recorded yet". Never reset —
    /// not by eviction, not by `clear`/`clear_flow`/`retain` — which is what keeps a
    /// cursor held across any of them valid.
    seq: AtomicU64,
    /// Highest seq dropped by *retention pressure* (cap or age). Deliberate deletions
    /// never touch it: losing entries you asked to delete is not a hole in your view.
    evicted_below_seq: AtomicU64,
    /// The port's clear generation, stamped into every entry appended under it. Pinned
    /// at 0 until issue #224 bumps it.
    clear_gen: AtomicU64,
    /// Whether the cap warning already fired for the current fill-up. A full shard evicts
    /// on every record, and warning per eviction serializes the recording path on the
    /// tracing writer (upstream issue #718). Deliberate deletions re-arm it.
    cap_warned: AtomicBool,
    /// Resolved once per shard, not per append: `with_label_values` allocates the label
    /// string and hashes it under the registry lock, which is not something the recording
    /// path should pay for every request.
    entries_gauge: Gauge,
}

impl PortShard {
    fn new(port: u16) -> Self {
        Self {
            entries: RwLock::new(VecDeque::new()),
            count: AtomicU64::new(0),
            seq: AtomicU64::new(0),
            evicted_below_seq: AtomicU64::new(0),
            clear_gen: AtomicU64::new(0),
            cap_warned: AtomicBool::new(false),
            entries_gauge: crate::metrics::journal_entries_gauge(port),
        }
    }
}

/// How long a computed shard cap is reused before the voter count is consulted again.
///
/// The cap is a retention heuristic, not a correctness boundary, and membership changes
/// only through a committed log entry — so a second of staleness costs nothing, while
/// recomputing per append would pay openraft's voter-set allocation on every recorded
/// request.
const CAP_REFRESH_MILLIS: u64 = 1_000;

/// This node's writer shards, keyed by port.
pub struct ClusterJournal {
    /// This writer's identity, stamped into every entry. Required at construction rather
    /// than bound later, because the manager starts serving during node startup — an
    /// entry recorded in that window would otherwise carry a placeholder id into
    /// `(node_id, seq, clear_gen)`, the key issue #223 merges on.
    node_id: NodeId,
    ports: RwLock<HashMap<u16, Arc<PortShard>>>,
    voters: OnceLock<Voters>,
    /// Last computed cap, and when. `0` means "not yet computed" — a real cap is always
    /// at least 1, so the sentinel cannot collide with a legitimate value.
    cap_cache: AtomicUsize,
    cap_refreshed_at: AtomicU64,
    clock: Arc<dyn Clock>,
    config: JournalConfig,
}

impl ClusterJournal {
    #[must_use]
    pub fn new(node_id: NodeId) -> Arc<Self> {
        Self::with_parts(
            node_id,
            JournalConfig::default(),
            Arc::new(MonotonicClock::default()),
        )
    }

    #[must_use]
    pub fn with_parts(node_id: NodeId, config: JournalConfig, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            ports: RwLock::new(HashMap::new()),
            voters: OnceLock::new(),
            cap_cache: AtomicUsize::new(0),
            cap_refreshed_at: AtomicU64::new(0),
            clock,
            config,
        })
    }

    /// Attach the node whose applied membership sizes the shard cap.
    ///
    /// Binding twice is a no-op — the second caller wanted what the first one got — but
    /// a second bind naming a *different* node means one journal was wired across two
    /// nodes, and shards would keep being sized from the first. Only the divisor is bound
    /// here, so that is a retention-accuracy bug rather than a data one; it is logged
    /// rather than enforced, because there is no correct recovery at this point in
    /// startup and refusing to serve would be worse than retaining slightly too much.
    pub fn bind(&self, node: &Arc<RaftNode>) {
        if node.id() != self.node_id {
            tracing::error!(
                journal_node = self.node_id,
                bound_node = node.id(),
                "journal bound to a node other than the one it stamps entries with"
            );
        }
        if self.voters.set(Voters::Node(Arc::downgrade(node))).is_err() {
            tracing::warn!("journal voter source already bound; second bind ignored");
            return;
        }
        // The cached cap was computed while unbound (one voter). Drop it so the next
        // append re-reads the real membership instead of waiting out the refresh window.
        self.cap_cache.store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn bind_fixed_voters(&self, voters: usize) {
        let _ = self.voters.set(Voters::Fixed(voters));
        self.cap_cache.store(0, Ordering::Relaxed);
    }

    /// Entries this shard may hold for one port: `max(min, fleet_capacity / voters)`.
    ///
    /// Unbound — pre-bind, or `--cluster` off — reads as one voter, so the cap is the
    /// upstream per-port cap and single-node behaviour is unchanged.
    ///
    /// Cached for [`CAP_REFRESH_MILLIS`]: this is consulted on every recorded request and
    /// openraft's voter-set accessor allocates.
    #[must_use]
    pub fn shard_cap(&self) -> usize {
        let now = self.clock.now_millis();
        let cached = self.cap_cache.load(Ordering::Relaxed);
        if cached != 0
            && now.saturating_sub(self.cap_refreshed_at.load(Ordering::Relaxed))
                < CAP_REFRESH_MILLIS
        {
            return cached;
        }

        let voters = self.voters.get().map_or(1, Voters::count).max(1);
        // At least one, so a pathological config cannot ask a shard to hold nothing and
        // spin evicting the entry it is about to append.
        let cap = (self.config.fleet_capacity / voters)
            .max(self.config.min_shard_cap)
            .max(1);
        self.cap_cache.store(cap, Ordering::Relaxed);
        self.cap_refreshed_at.store(now, Ordering::Relaxed);
        cap
    }

    /// This node's shard of `port`, from `since_seq` exclusive (0 for everything).
    #[must_use]
    pub fn read_shard_since(&self, port: u16, since_seq: u64) -> ShardRead {
        let shard = self.shard(port);
        let entries = shard.entries.read();
        ShardRead {
            entries: entries
                .iter()
                .filter(|entry| entry.seq > since_seq)
                .cloned()
                .collect(),
            evicted_below_seq: shard.evicted_below_seq.load(Ordering::SeqCst),
            clear_gen: shard.clear_gen.load(Ordering::SeqCst),
            count_slot: shard.count.load(Ordering::SeqCst),
        }
    }

    fn shard(&self, port: u16) -> Arc<PortShard> {
        if let Some(shard) = self.ports.read().get(&port) {
            return Arc::clone(shard);
        }
        Arc::clone(
            self.ports
                .write()
                .entry(port)
                .or_insert_with(|| Arc::new(PortShard::new(port))),
        )
    }

    /// Move entries the retention policy no longer admits out of the deque, oldest first,
    /// advancing the watermark past everything removed.
    ///
    /// Caller holds the write lock: eviction and the seq assignment that follows it have
    /// to be one critical section, or a reader could observe a deque whose front is
    /// already gone while the watermark still says it is retained.
    ///
    /// Evicted entries are moved into `drained` rather than dropped here. Each owns a
    /// `String` and a `RecordedRequest` with its header and query maps, and a membership
    /// growth can evict thousands in a single pass (1→5 voters takes the cap from 10_000
    /// to 2_000) — running those destructors under the port's write lock would stall
    /// every concurrent recorder for that port. The caller drops them after releasing it.
    ///
    /// `cap` is passed in rather than read here so [`Self::shard_cap`] is consulted
    /// *before* the lock is taken: the cap is a retention heuristic and does not need to
    /// be consistent with the critical section.
    fn evict(
        &self,
        port: u16,
        shard: &PortShard,
        entries: &mut VecDeque<ShardEntry>,
        now: u64,
        cap: usize,
        drained: &mut Vec<ShardEntry>,
    ) -> Evicted {
        let mut evicted = Evicted::default();
        // Saturating: only an embedder passing a near-`Duration::MAX` max_age reaches it,
        // and the resulting "never expires" is what that value already means.
        let max_age = u64::try_from(self.config.max_age.as_millis()).unwrap_or(u64::MAX);

        while entries
            .front()
            .is_some_and(|oldest| now.saturating_sub(oldest.recorded_at_millis) >= max_age)
        {
            let Some(oldest) = entries.pop_front() else {
                break;
            };
            shard
                .evicted_below_seq
                .fetch_max(oldest.seq, Ordering::SeqCst);
            drained.push(oldest);
            evicted.age += 1;
        }

        // `while`, not `if`: a membership change shrinks the cap, so a shard can start an
        // append already several entries over the new limit and must converge in one pass.
        while entries.len() >= cap {
            if !shard.cap_warned.swap(true, Ordering::SeqCst) {
                tracing::warn!(
                    port,
                    cap,
                    "Journal shard cap reached; evicting oldest entries (warned once per fill-up)"
                );
            }
            let Some(oldest) = entries.pop_front() else {
                break;
            };
            shard
                .evicted_below_seq
                .fetch_max(oldest.seq, Ordering::SeqCst);
            drained.push(oldest);
            evicted.cap += 1;
        }
        evicted
    }
}

/// What one append's retention pass removed, reported to metrics after the lock drops.
#[derive(Debug, Default)]
struct Evicted {
    cap: usize,
    age: usize,
}

/// The tail every deliberate deletion shares. Caller holds the entries write lock.
///
/// Two things have to happen after `clear` / `retain` / `clear_flow`, and neither is
/// obvious from the deletion itself:
///
/// * The cap warning re-arms, because a deliberate deletion starts a new fill-up and each
///   fill-up warns once. Re-armed under the write lock so a racing recorder cannot observe
///   the stale flag and skip its fill-up's warning.
/// * The depth gauge is republished, because it is otherwise only written by
///   `record_indexed` — a port cleared and never written again would report its pre-clear
///   depth forever.
///
/// What deliberately does *not* happen: the watermark and `seq` are untouched. Losing
/// entries you asked to delete is not a hole in your view, and a cursor held across a
/// clear has to stay valid.
fn finish_deletion(shard: &PortShard, retained: usize) {
    shard.cap_warned.store(false, Ordering::SeqCst);
    shard.entries_gauge.set(retained as f64);
}

impl RequestJournal for ClusterJournal {
    fn note_request(&self, port: u16) {
        // Every request, recorded body or not — this is the G-counter slot behind
        // `numberOfRequests`, and the engine calls it before the recording gate.
        self.shard(port).count.fetch_add(1, Ordering::SeqCst);
    }

    fn record(&self, port: u16, flow_id: &str, req: RecordedRequest) {
        self.record_indexed(port, flow_id, req);
    }

    fn record_indexed(&self, port: u16, flow_id: &str, req: RecordedRequest) -> Option<u64> {
        let shard = self.shard(port);
        let now = self.clock.now_millis();
        // Both resolved before the lock, to keep the critical section to the deque
        // mutation and the seq assignment that must be atomic with it.
        let cap = self.shard_cap();
        let flow = flow_id.to_string();
        // Outlives the guard below, so evicted entries' destructors run after it drops.
        let mut drained = Vec::new();

        let (seq, evicted, retained) = {
            let mut entries = shard.entries.write();
            let evicted = self.evict(port, &shard, &mut entries, now, cap, &mut drained);
            // Assigned under the write lock: a fetch_add outside it could interleave with
            // a concurrent recorder and push entries in a different order than their seqs,
            // which would make the cursor cut skip entries and the binary search in
            // `attach_match` unsound.
            let seq = shard.seq.fetch_add(1, Ordering::SeqCst) + 1;
            entries.push_back(ShardEntry {
                node_id: self.node_id,
                seq,
                clear_gen: shard.clear_gen.load(Ordering::SeqCst),
                flow_id: flow,
                request: req,
                recorded_at_millis: now,
            });
            (seq, evicted, entries.len())
        };

        shard.entries_gauge.set(retained as f64);
        crate::metrics::note_journal_evictions(evicted.cap, evicted.age);
        Some(seq)
    }

    fn read(&self, port: u16) -> JournalRead {
        JournalRead {
            entries: self
                .shard(port)
                .entries
                .read()
                .iter()
                .map(|entry| entry.request.clone())
                .collect(),
            complete: true,
        }
    }

    fn read_filtered(&self, port: u16, keep: &dyn Fn(&RecordedRequest) -> bool) -> JournalRead {
        // Filter over references under the read lock so only matches are cloned.
        JournalRead {
            entries: self
                .shard(port)
                .entries
                .read()
                .iter()
                .filter(|entry| keep(&entry.request))
                .map(|entry| entry.request.clone())
                .collect(),
            complete: true,
        }
    }

    fn read_since(
        &self,
        port: u16,
        since: Option<u64>,
        keep: &dyn Fn(&RecordedRequest) -> bool,
    ) -> Option<JournalReadSince> {
        let shard = self.shard(port);
        let entries = shard.entries.read();
        // 0 admits every entry, since seqs are 1-based — a baseline read needs no case.
        let cut = since.unwrap_or(0);
        Some(JournalReadSince {
            entries: entries
                .iter()
                .filter(|entry| entry.seq > cut)
                .filter(|entry| keep(&entry.request))
                .map(|entry| JournalEntry {
                    index: entry.seq,
                    request: entry.request.clone(),
                })
                .collect(),
            next: shard.seq.load(Ordering::SeqCst),
            // A baseline read sees everything retained, so it cannot have a hole. A
            // reader at the watermark has already seen everything eviction removed.
            truncated: since
                .is_some_and(|seen| shard.evicted_below_seq.load(Ordering::SeqCst) > seen),
            complete: true,
        })
    }

    fn clear(&self, port: u16) -> anyhow::Result<()> {
        let shard = self.shard(port);
        let mut entries = shard.entries.write();
        entries.clear();
        shard.count.store(0, Ordering::SeqCst);
        finish_deletion(&shard, 0);
        Ok(())
    }

    fn retain(&self, port: u16, keep: &dyn Fn(&RecordedRequest) -> bool) {
        let shard = self.shard(port);
        let mut entries = shard.entries.write();
        entries.retain(|entry| keep(&entry.request));
        finish_deletion(&shard, entries.len());
    }

    fn clear_flow(&self, port: u16, flow_id: &str) -> anyhow::Result<()> {
        let shard = self.shard(port);
        let mut entries = shard.entries.write();
        entries.retain(|entry| entry.flow_id != flow_id);
        finish_deletion(&shard, entries.len());
        Ok(())
    }

    fn count(&self, port: u16) -> u64 {
        // This node's slot only. The fleet sum is the merge's job (issue #223).
        self.shard(port).count.load(Ordering::SeqCst)
    }

    fn attach_match(&self, port: u16, index: u64, outcome: MatchOutcome) {
        let shard = self.shard(port);
        let mut entries = shard.entries.write();
        // Seqs are assigned under this same write lock, so the deque is always in
        // strictly ascending seq order — the binary search is exact, not a heuristic.
        // The whole design leans on that invariant, and violating it would not fail
        // loudly: the search would silently annotate the *wrong* entry. So it is asserted
        // in debug builds, where a future change to eviction or a scoped clear that
        // reorders the deque turns a silent mis-attribution into a test failure.
        debug_assert!(
            entries.iter().is_sorted_by_key(|entry| entry.seq),
            "journal deque must stay ordered by seq for attach_match to address entries"
        );
        // An entry that is gone (evicted, or cleared between record and match) is not an
        // error: a diagnostic annotation must never be able to fail a request.
        if let Ok(position) = entries.binary_search_by(|entry| entry.seq.cmp(&index)) {
            entries[position].request.match_outcome = Some(outcome);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rift_cluster_base::seams::ResponseMode;

    #[derive(Default)]
    struct ManualClock(AtomicU64);

    impl ManualClock {
        fn advance(&self, millis: u64) {
            self.0.fetch_add(millis, Ordering::SeqCst);
        }
    }

    impl Clock for ManualClock {
        fn now_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    /// Unbound journal: one voter, upstream cap — the single-node fidelity baseline.
    fn journal() -> Arc<ClusterJournal> {
        ClusterJournal::new(1)
    }

    fn bound(voters: usize, node_id: NodeId) -> Arc<ClusterJournal> {
        let j = ClusterJournal::new(node_id);
        j.bind_fixed_voters(voters);
        j
    }

    fn req(path: &str) -> RecordedRequest {
        RecordedRequest {
            mode: ResponseMode::Text,
            request_from: "t".into(),
            method: "GET".into(),
            path: path.into(),
            query: Default::default(),
            headers: Default::default(),
            body: None,
            timestamp: "t".into(),
            match_outcome: None,
        }
    }

    fn outcome() -> MatchOutcome {
        MatchOutcome {
            matched: false,
            stub_index: None,
            stub_id: None,
            tried: Vec::new(),
            tried_omitted: 0,
        }
    }

    fn cursor(j: &ClusterJournal, port: u16, since: Option<u64>) -> JournalReadSince {
        j.read_since(port, since, &|_| true)
            .expect("ClusterJournal supports cursors")
    }

    fn indices(read: &JournalReadSince) -> Vec<u64> {
        read.entries.iter().map(|e| e.index).collect()
    }

    fn paths(read: &JournalReadSince) -> Vec<&str> {
        read.entries
            .iter()
            .map(|e| e.request.path.as_str())
            .collect()
    }

    // ---- AC1: upstream semantics, preserved ------------------------------------------

    // The unbound cap is the upstream cap, and eviction is oldest-first.
    #[test]
    fn caps_and_evicts_oldest_first() {
        let j = journal();
        for i in 0..(MAX_RECORDED_REQUESTS + 10) {
            j.record(1, "f", req(&format!("/{i}")));
        }
        let read = j.read(1);
        assert_eq!(read.entries.len(), MAX_RECORDED_REQUESTS);
        assert_eq!(read.entries[0].path, "/10", "oldest entries evicted first");
        assert!(read.complete);
    }

    // numberOfRequests counts even when body recording is off.
    #[test]
    fn counts_without_recording() {
        let j = journal();
        j.note_request(1);
        j.note_request(1);
        assert_eq!(j.count(1), 2);
        assert!(j.read(1).entries.is_empty());
    }

    #[test]
    fn clear_resets_count() {
        let j = journal();
        j.note_request(1);
        j.record(1, "f", req("/a"));
        assert_eq!(j.count(1), 1, "counted before the clear");
        assert_eq!(j.read(1).entries.len(), 1, "recorded before the clear");

        j.clear(1).expect("clear");
        assert_eq!(j.count(1), 0);
        assert!(j.read(1).entries.is_empty());
    }

    #[test]
    fn retain_preserves_count() {
        let j = journal();
        j.note_request(1);
        j.note_request(1);
        j.record(1, "f", req("/a"));
        j.record(1, "f", req("/b"));
        j.retain(1, &|r| r.path == "/b");
        assert_eq!(j.read(1).entries.len(), 1);
        assert_eq!(j.count(1), 2, "retain never resets the count");
    }

    #[test]
    fn clear_flow_removes_one_slice() {
        let j = journal();
        j.note_request(1);
        j.record(1, "flow-a", req("/a"));
        j.record(1, "flow-b", req("/b"));
        j.clear_flow(1, "flow-a").expect("clear_flow");
        let read = j.read(1);
        assert_eq!(read.entries.len(), 1);
        assert_eq!(read.entries[0].path, "/b");
        assert_eq!(j.count(1), 1, "scoped clear keeps the total count");
    }

    #[test]
    fn clears_are_ok_and_actually_delete() {
        let j = journal();
        j.record(1, "flow-a", req("/a"));
        assert!(j.clear_flow(1, "flow-a").is_ok());
        assert!(
            j.read(1).entries.is_empty(),
            "a clear that reports Ok must have deleted — #330's whole point"
        );

        j.record(1, "flow-b", req("/b"));
        assert!(j.clear(1).is_ok());
        assert!(j.read(1).entries.is_empty());
    }

    #[test]
    fn read_filtered_keeps_only_matches() {
        let j = journal();
        j.record(1, "f", req("/keep/1"));
        j.record(1, "f", req("/drop/1"));
        j.record(1, "f", req("/keep/2"));

        let read = j.read_filtered(1, &|r| r.path.starts_with("/keep"));
        let got: Vec<&str> = read.entries.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(got, vec!["/keep/1", "/keep/2"]);
        assert!(j.read_filtered(1, &|_| false).entries.is_empty());
        assert_eq!(j.read_filtered(1, &|_| true).entries.len(), 3);
    }

    #[test]
    fn ports_are_isolated() {
        let j = journal();
        j.record(1, "f", req("/a"));
        j.note_request(2);
        assert_eq!(j.read(1).entries.len(), 1);
        assert!(j.read(2).entries.is_empty());
        assert_eq!(j.count(1), 0);
        assert_eq!(j.count(2), 1);
    }

    #[test]
    fn cursor_assigns_1based_indices_and_reports_next() {
        let j = journal();
        assert_eq!(cursor(&j, 1, None).next, 0, "nothing assigned yet");

        assert_eq!(j.record_indexed(1, "f", req("/a")), Some(1), "1-based");
        assert_eq!(j.record_indexed(1, "f", req("/b")), Some(2));
        assert_eq!(j.record_indexed(1, "f", req("/c")), Some(3));

        let read = cursor(&j, 1, None);
        assert_eq!(indices(&read), vec![1, 2, 3]);
        assert_eq!(paths(&read), vec!["/a", "/b", "/c"]);
        assert_eq!(read.next, 3);
        assert!(!read.truncated, "a baseline read can never be truncated");

        // `record` shares the counter — an unindexed write still advances the cursor.
        j.record(1, "f", req("/d"));
        assert_eq!(cursor(&j, 1, None).next, 4);

        // seqs are per-port, not global.
        assert_eq!(j.record_indexed(2, "f", req("/x")), Some(1));
    }

    #[test]
    fn cursor_since_returns_strictly_newer() {
        let j = journal();
        for p in ["/a", "/b", "/c"] {
            j.record_indexed(1, "f", req(p));
        }
        assert_eq!(indices(&cursor(&j, 1, Some(1))), vec![2, 3]);
        assert_eq!(indices(&cursor(&j, 1, Some(2))), vec![3]);

        let caught_up = cursor(&j, 1, Some(3));
        assert!(caught_up.entries.is_empty());
        assert_eq!(caught_up.next, 3);

        let beyond = cursor(&j, 1, Some(99));
        assert!(beyond.entries.is_empty());
        assert_eq!(beyond.next, 3);
    }

    #[test]
    fn cursor_keep_composes_after_cut() {
        let j = journal();
        j.record_indexed(1, "f", req("/keep/1")); // 1
        j.record_indexed(1, "f", req("/drop/1")); // 2
        j.record_indexed(1, "f", req("/keep/2")); // 3
        j.record_indexed(1, "f", req("/drop/2")); // 4

        let keep_only = |r: &RecordedRequest| r.path.starts_with("/keep");

        let all = j.read_since(1, None, &keep_only).expect("cursor");
        assert_eq!(indices(&all), vec![1, 3]);
        assert_eq!(all.next, 4, "next spans scanned entries, not returned ones");

        let after = j.read_since(1, Some(1), &keep_only).expect("cursor");
        assert_eq!(indices(&after), vec![3], "cut first, then filter");

        // An all-rejected window must still advance, or a filtered tail re-scans forever.
        let empty = j
            .read_since(1, Some(3), &|r| r.path == "/nothing")
            .expect("cursor");
        assert!(empty.entries.is_empty());
        assert_eq!(empty.next, 4);
    }

    #[test]
    fn cursor_survives_clear_without_truncation() {
        let j = journal();
        j.record_indexed(1, "f", req("/a"));
        j.record_indexed(1, "f", req("/b"));
        j.clear(1).expect("clear");

        let after_clear = cursor(&j, 1, Some(2));
        assert!(after_clear.entries.is_empty());
        assert_eq!(after_clear.next, 2, "next never regresses over a clear");
        assert!(
            !after_clear.truncated,
            "a clear is deliberate, not retention pressure"
        );

        j.record_indexed(1, "f", req("/c"));
        let resumed = cursor(&j, 1, Some(2));
        assert_eq!(
            indices(&resumed),
            vec![3],
            "post-clear seqs keep counting up"
        );
        assert_eq!(paths(&resumed), vec!["/c"]);
        assert!(!resumed.truncated);

        j.record_indexed(1, "flow-x", req("/x"));
        j.clear_flow(1, "flow-x").expect("clear_flow");
        assert!(!cursor(&j, 1, Some(1)).truncated);
        j.retain(1, &|_| false);
        assert!(!cursor(&j, 1, Some(1)).truncated);
    }

    #[test]
    fn cursor_since_zero_differs_from_baseline_only_in_truncation() {
        let j = journal();
        for i in 0..(MAX_RECORDED_REQUESTS + 3) {
            j.record_indexed(1, "f", req(&format!("/{i}")));
        }
        let baseline = cursor(&j, 1, None);
        let from_zero = cursor(&j, 1, Some(0));
        assert_eq!(indices(&baseline), indices(&from_zero));
        assert_eq!(baseline.next, from_zero.next);
        assert!(!baseline.truncated, "a snapshot cannot have a hole");
        assert!(
            from_zero.truncated,
            "a replay lost entries 1..=3 to the cap"
        );

        let fresh = journal();
        fresh.record_indexed(1, "f", req("/a"));
        assert!(!cursor(&fresh, 1, None).truncated);
        assert!(!cursor(&fresh, 1, Some(0)).truncated);
    }

    #[test]
    fn attach_match_on_an_absent_entry_is_a_no_op() {
        let j = journal();
        for i in 0..(MAX_RECORDED_REQUESTS + 5) {
            j.record_indexed(1, "f", req(&format!("/{i}")));
        }
        // seqs 1..=5 fell off the front; 6 is the oldest retained.
        j.attach_match(1, 1, outcome());
        j.attach_match(9999, 1, outcome());

        let entries = j.read(1).entries;
        assert_eq!(entries.len(), MAX_RECORDED_REQUESTS);
        assert!(
            entries.iter().all(|r| r.match_outcome.is_none()),
            "attaching to an evicted seq must not land on a surviving entry"
        );

        j.attach_match(1, 6, outcome());
        let entries = j.read(1).entries;
        assert!(entries[0].match_outcome.is_some(), "the addressed entry");
        assert!(
            entries[1..].iter().all(|r| r.match_outcome.is_none()),
            "and only that entry"
        );
    }

    #[test]
    fn cursor_indices_stay_ordered_under_concurrent_recorders() {
        use std::sync::Barrier;

        const RECORDERS: usize = 8;
        const PER_RECORDER: usize = 64;

        let j = journal();
        let barrier = Arc::new(Barrier::new(RECORDERS));
        let mut handles = Vec::new();
        for r in 0..RECORDERS {
            let j = Arc::clone(&j);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                (0..PER_RECORDER)
                    .filter_map(|i| j.record_indexed(1, "f", req(&format!("/{r}-{i}"))))
                    .collect::<Vec<_>>()
            }));
        }
        let mut assigned: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().expect("recorder thread"))
            .collect();

        let total = (RECORDERS * PER_RECORDER) as u64;
        assigned.sort_unstable();
        assert_eq!(
            assigned,
            (1..=total).collect::<Vec<_>>(),
            "every seq handed out exactly once, with no gaps"
        );

        let read = cursor(&j, 1, None);
        let seen = indices(&read);
        assert_eq!(seen.len(), total as usize);
        assert!(
            seen.windows(2).all(|w| w[0] < w[1]),
            "deque order must match seq order, or the cursor cut skips entries"
        );
        assert_eq!(read.next, total);
    }

    // ---- AC2: the merge key and the watermark ----------------------------------------

    #[test]
    fn every_entry_carries_the_merge_key() {
        let j = bound(3, 7);
        j.record_indexed(1, "flow-a", req("/a"));
        j.record_indexed(1, "flow-b", req("/b"));

        let shard = j.read_shard_since(1, 0);
        assert_eq!(shard.entries.len(), 2);
        assert!(
            shard.entries.iter().all(|e| e.node_id == 7),
            "every entry is stamped with this writer's node id"
        );
        assert_eq!(
            shard.entries.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1, 2],
            "seq is per-port monotone"
        );
        assert!(
            shard.entries.iter().all(|e| e.clear_gen == 0),
            "generations are pinned at 0 until #224 bumps them"
        );
        assert_eq!(shard.entries[0].flow_id, "flow-a");
        assert_eq!(shard.entries[1].request.path, "/b");
    }

    #[test]
    fn evicted_below_seq_advances_under_cap_pressure() {
        let j = bound(1, 1);
        assert_eq!(
            j.read_shard_since(1, 0).evicted_below_seq,
            0,
            "nothing lost"
        );

        for i in 0..(MAX_RECORDED_REQUESTS + 10) {
            j.record_indexed(1, "f", req(&format!("/{i}")));
        }
        let shard = j.read_shard_since(1, 0);
        assert_eq!(
            shard.evicted_below_seq, 10,
            "the highest seq retention dropped"
        );
        assert_eq!(shard.entries.len(), MAX_RECORDED_REQUESTS);
        assert_eq!(shard.entries[0].seq, 11, "oldest surviving seq");

        // Deliberate deletion is not retention pressure and must not move the watermark.
        j.clear(1).expect("clear");
        assert_eq!(j.read_shard_since(1, 0).evicted_below_seq, 10);
    }

    #[test]
    fn truncated_iff_since_below_watermark() {
        let j = journal();
        for i in 0..(MAX_RECORDED_REQUESTS + 10) {
            j.record_indexed(1, "f", req(&format!("/{i}")));
        }
        // seqs 1..=10 evicted; watermark = 10.
        assert!(cursor(&j, 1, Some(5)).truncated, "lost 6..=10");
        assert!(
            cursor(&j, 1, Some(9)).truncated,
            "never received seq 10 before the cap took it"
        );
        assert!(
            !cursor(&j, 1, Some(10)).truncated,
            "a reader at the watermark has seen everything eviction removed"
        );
        assert!(!cursor(&j, 1, Some(50)).truncated, "well ahead");
        assert!(!cursor(&j, 1, None).truncated, "baseline reads everything");
    }

    // ---- AC3: the cap formula and the age cap ----------------------------------------

    #[test]
    fn shard_cap_divides_fleet_capacity_by_voters() {
        assert_eq!(
            journal().shard_cap(),
            MAX_RECORDED_REQUESTS,
            "unbound reads as one voter, so single-node fidelity holds"
        );
        assert_eq!(bound(1, 1).shard_cap(), 10_000);
        assert_eq!(bound(3, 1).shard_cap(), 3_333);
        assert_eq!(
            bound(25, 1).shard_cap(),
            MIN_SHARD_CAP,
            "the floor stops a large fleet dividing shards into uselessness"
        );
        assert_eq!(bound(0, 1).shard_cap(), 10_000, "0 voters cannot divide");
    }

    #[test]
    fn a_three_voter_shard_caps_at_a_third() {
        let j = bound(3, 1);
        for i in 0..3_400 {
            j.record_indexed(1, "f", req(&format!("/{i}")));
        }
        assert_eq!(j.read(1).entries.len(), 3_333);
        assert_eq!(
            j.read_shard_since(1, 0).evicted_below_seq,
            3_400 - 3_333,
            "the watermark tracks what the tighter cap dropped"
        );
    }

    #[test]
    fn age_cap_evicts_and_advances_watermark() {
        let clock = Arc::new(ManualClock::default());
        let j = ClusterJournal::with_parts(
            1,
            JournalConfig {
                max_age: Duration::from_secs(60),
                ..JournalConfig::default()
            },
            Arc::clone(&clock) as Arc<dyn Clock>,
        );

        j.record_indexed(1, "f", req("/old-1"));
        j.record_indexed(1, "f", req("/old-2"));
        clock.advance(61_000);
        j.record_indexed(1, "f", req("/fresh"));

        let read = j.read(1);
        assert_eq!(
            read.entries.len(),
            1,
            "entries past max_age are dropped on append"
        );
        assert_eq!(read.entries[0].path, "/fresh");

        let shard = j.read_shard_since(1, 0);
        assert_eq!(
            shard.evicted_below_seq, 2,
            "the age cap advances the same watermark the size cap does"
        );
        assert!(
            cursor(&j, 1, Some(1)).truncated,
            "a reader below the watermark has a hole, whichever cap made it"
        );
    }

    // ---- The read surface issue #223 merges ------------------------------------------

    #[test]
    fn read_shard_since_serves_the_merge_payload() {
        let j = bound(3, 42);
        j.note_request(1);
        j.record_indexed(1, "f", req("/a"));
        j.record_indexed(1, "f", req("/b"));
        j.record_indexed(1, "f", req("/c"));

        let all = j.read_shard_since(1, 0);
        assert_eq!(all.entries.len(), 3, "0 means everything retained");
        assert_eq!(all.clear_gen, 0);
        assert_eq!(
            all.count_slot, 1,
            "the slot tracks noted requests, not recordings: the engine calls note_request \
             for every request and record only when body recording is on, so recording must \
             not double-count"
        );

        let delta = j.read_shard_since(1, 1);
        assert_eq!(
            delta.entries.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![2, 3],
            "since_seq is exclusive, so a peer never re-fetches what it has"
        );

        assert!(
            j.read_shard_since(1, 99).entries.is_empty(),
            "a peer ahead of this shard pulls nothing"
        );

        let unknown = j.read_shard_since(9999, 0);
        assert!(
            unknown.entries.is_empty(),
            "an unknown port is empty, not an error"
        );
        assert_eq!(
            (
                unknown.evicted_below_seq,
                unknown.clear_gen,
                unknown.count_slot
            ),
            (0, 0, 0),
            "a never-touched port reports a zeroed shard, so a merge reads no phantom \
             watermark or count from it"
        );
    }

    // ---- Retention interactions and the cap warning ----------------------------------

    // The two caps share one `evict` pass and one watermark. A shard that is both full
    // and holding aged entries must converge in a single append, and the watermark has to
    // end up past whichever eviction reached further.
    #[test]
    fn age_and_size_caps_evict_in_the_same_pass() {
        let clock = Arc::new(ManualClock::default());
        let j = ClusterJournal::with_parts(
            1,
            JournalConfig {
                fleet_capacity: 10,
                min_shard_cap: 1,
                max_age: Duration::from_secs(60),
            },
            Arc::clone(&clock) as Arc<dyn Clock>,
        );

        // Fill to the cap with entries that will also be aged out.
        for i in 0..10 {
            j.record_indexed(1, "f", req(&format!("/old-{i}")));
        }
        clock.advance(61_000);

        // One append: every retained entry is both over-age and over-cap.
        j.record_indexed(1, "f", req("/fresh"));

        let read = j.read(1);
        assert_eq!(read.entries.len(), 1, "only the new entry survives");
        assert_eq!(read.entries[0].path, "/fresh");
        assert_eq!(
            j.read_shard_since(1, 0).evicted_below_seq,
            10,
            "the watermark reaches the highest seq either cap dropped"
        );
        assert!(
            cursor(&j, 1, Some(3)).truncated,
            "a reader below the watermark has a hole regardless of which cap made it"
        );
    }

    // A shrinking cap (a fleet growing from 1 to 5 voters) must converge in one append,
    // not shed a single entry per request while staying permanently over the limit.
    #[test]
    fn a_shrinking_cap_converges_in_one_append() {
        let j = bound(1, 1);
        for i in 0..600 {
            j.record_indexed(1, "f", req(&format!("/{i}")));
        }
        assert_eq!(j.read(1).entries.len(), 600);

        // 10_000 / 20 = 500, the floor.
        let tighter = bound(20, 1);
        for i in 0..600 {
            tighter.record_indexed(1, "f", req(&format!("/{i}")));
        }
        assert_eq!(
            tighter.read(1).entries.len(),
            MIN_SHARD_CAP,
            "the shard settles at the tighter cap rather than drifting above it"
        );
    }

    // Upstream issue #718: warning per eviction serialized the whole recording path on
    // the tracing writer (−29% RPS at c=200, −55% in the #702 sweep). This journal ports
    // that mechanism, so it ports the guard too.
    #[test]
    #[tracing_test::traced_test]
    fn cap_warns_once_per_fill_not_per_eviction() {
        let j = bound(20, 1); // 500-entry shard, so the fill is cheap
        for i in 0..(MIN_SHARD_CAP + 100) {
            j.record(1, "f", req(&format!("/{i}")));
        }
        logs_assert(|lines: &[&str]| {
            let n = lines.iter().filter(|l| l.contains("cap reached")).count();
            if n == 1 {
                Ok(())
            } else {
                Err(format!("expected exactly one cap warning, saw {n}"))
            }
        });
    }

    // Deliberate deletions start a new fill-up, so each one re-arms the warning.
    #[test]
    #[tracing_test::traced_test]
    fn cap_warning_rearms_after_deliberate_deletions() {
        let j = bound(20, 1);
        let fill = |j: &ClusterJournal| {
            for i in 0..(MIN_SHARD_CAP + 5) {
                j.record(1, "f", req(&format!("/{i}")));
            }
        };
        fill(&j);
        j.clear(1).expect("clear");
        fill(&j);
        j.retain(1, &|_| false);
        fill(&j);
        j.clear_flow(1, "f").expect("clear_flow");
        fill(&j);
        logs_assert(|lines: &[&str]| {
            let n = lines.iter().filter(|l| l.contains("cap reached")).count();
            if n == 4 {
                Ok(())
            } else {
                Err(format!(
                    "expected 4 cap warnings (one per fill-up), saw {n}"
                ))
            }
        });
    }

    // The flag lives on the port shard, and the warning names the port — one journal
    // serves every imposter on the node, so an operator must be able to tell which one
    // is shedding entries.
    #[test]
    #[tracing_test::traced_test]
    fn cap_warning_is_per_port_and_names_the_port() {
        let j = bound(20, 1);
        for port in [1u16, 2] {
            for i in 0..(MIN_SHARD_CAP + 5) {
                j.record(port, "f", req(&format!("/{i}")));
            }
        }
        logs_assert(|lines: &[&str]| {
            let warnings: Vec<&&str> = lines.iter().filter(|l| l.contains("cap reached")).collect();
            if warnings.len() != 2 {
                return Err(format!("expected 2 cap warnings, saw {}", warnings.len()));
            }
            if !warnings.iter().any(|l| l.contains("port=1"))
                || !warnings.iter().any(|l| l.contains("port=2"))
            {
                return Err(format!("warnings do not name both ports: {warnings:?}"));
            }
            Ok(())
        });
    }
}
