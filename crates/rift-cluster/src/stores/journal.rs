//! This node's writer shard of the fleet request journal (RFC-001 Ch.7, issue #222).
//!
//! Recorded requests are the one piece of cluster state that never conflicts: entries
//! from different nodes interleave rather than disagree. That buys a design with no
//! owners, no consensus and no coordination on the write side — every node appends to
//! **its own shard** and reads merge (issue #223).
//!
//! What lives here is only the local half: the shard, its writer-local caps, and the
//! read surface a peer merge consumes. There is no RPC and no peer pull, and journal
//! entries are test-run-scoped and volatile by design (Ch.7, Ch.9's matrix).
//!
//! There is one small piece of disk, and only one: the per-port **seq floor**
//! ([`super::journal_seq`], issue #351). Entries are not persisted and never will be;
//! the counter that numbers them is, because `node_id` is durable and so `(node_id,
//! seq)` is an identity the rest of the fleet keeps referring to — in replica caches
//! and in live cursors — after a crash. Reissuing those keys is wrong data rather than
//! lost data, which is a different thing from the volatility decision and is why it
//! gets a different answer.
//!
//! Entries are keyed `(node_id, seq, clear_gen)`:
//!
//! * `node_id` — the Raft node id of the writer, stable across restarts, and what a
//!   vector cursor (issue #225) addresses a shard position by.
//! * `seq` — this node's per-port monotone counter. It is also the index handed back
//!   to the engine, so the upstream `?since=` cursor and SSE `index` contracts hold
//!   unchanged on a single node.
//! * `clear_gen` — the port's clear generation at append time, alongside each entry's own
//!   `space_gen` (issue #224): monotone counters the Raft state machine bumps via
//!   [`ClusterJournal::set_clear_gen`] when a fleet-wide clear commits, so a merge can drop
//!   everything older without clock sync or coordination on the write side either.
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
use crate::stores::journal_seq::SeqFloors;
use parking_lot::RwLock;
use prometheus::Gauge;
use rift_cluster_base::seams::{
    JournalEntry, JournalRead, JournalReadSince, MAX_RECORDED_REQUESTS, MatchOutcome,
    RecordedRequest, RequestJournal,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
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
    /// The entry's space generation at append time — `clear_gen`'s sibling, scoped to
    /// `flow_id` instead of the whole port. Kept as its own component rather than folded into
    /// `clear_gen` (e.g. `max(port_gen, space_gen)`): a port-wide clear must drop an entry
    /// whose space generation is numerically *higher* than the new port generation, and a
    /// merged stamp cannot tell that case apart from a legitimately post-clear one.
    pub space_gen: u64,
    /// Resolved at record time so scoped clears need not re-derive it from stored headers.
    pub flow_id: String,
    pub request: RecordedRequest,
    /// Monotonic arrival time, for the age cap only. Not part of the merge key and not
    /// the ordering key a merge uses — that is the request's own recorded timestamp.
    ///
    /// `pub(crate)` so [`super::journal_net`] can build entries for the merge tests. Deliberately
    /// not `pub`: the value is a *local* clock reading, meaningless on any other node, which is
    /// why the wire form omits it entirely.
    pub(crate) recorded_at_millis: u64,
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
    /// Current per-space generations — only spaces that have actually been bumped occupy a
    /// row, so a never-cleared space costs nothing here. Sorted by space name: this crosses
    /// into the merge's fleet-wide max and onto the wire, and both need a deterministic order
    /// to agree with themselves across nodes and across repeated calls.
    pub space_gens: Vec<(String, u64)>,
    /// This node's G-counter slot — summed across shards to answer `numberOfRequests`.
    pub count_slot: u64,
}

/// A reader's position across every writer's shard of one port (issue #225) — what a merged
/// `?since=` read hands back and takes in, instead of one scalar index.
///
/// A scalar cursor cannot address a multi-writer merge: "index 500" names a position in
/// *whose* shard? So this carries a position **per shard**, keyed by the writer's [`NodeId`],
/// plus the clear generation (issue #224) the cursor was issued under — a merge needs both to
/// know which entries a reader has already consumed and which generation's worth of clears it
/// has already accounted for.
///
/// `pos` is a [`BTreeMap`], not a `HashMap`: the same cursor must [`Self::encode`] to the same
/// bytes every time, and a `HashMap`'s iteration order is not stable across processes (or even
/// two runs of the same process, under the default hasher) — an unstable order would make the
/// token an unreliable cache key and a flaky value to assert on in a test.
///
/// The token [`Self::encode`] produces is opaque **by contract**, not merely by convention: a
/// client must round-trip it, never parse it. That is what leaves this format free to change
/// shape behind the version tag it carries — [`Self::decode`] rejects any version it does not
/// recognize — without breaking a client that only ever passed the string back unmodified.
///
/// A malformed or unrecognized token is always an error, never a defaulted position:
/// defaulting to `0` would silently replay the entire journal, and defaulting to "current"
/// would silently skip every entry recorded since the token went stale. Both are
/// wrong-but-quiet, so [`Self::decode`] and [`Self::decode_or_legacy`] refuse instead of
/// guessing — see [`CursorError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalCursor {
    /// The port's clear generation (issue #224) at the moment this cursor was issued — not at
    /// the moment it is read back.
    ///
    /// **Carried, not yet acted on.** The walk's clear-safety comes entirely from the per-shard
    /// positions plus the fact that a clear never resets `seq`: entries from a superseded
    /// generation are dropped by the merge itself, and the positions step over the seq range
    /// they occupied, so nothing here needs to filter on this value. It is part of the `v1`
    /// wire format because a reader that *can* see which generation a token was minted under is
    /// the cheap enabling condition for anything that later needs to (telling "your cursor
    /// predates a clear" apart from "nothing new", say), and adding a field to a versioned
    /// token afterwards costs a version bump. Kept deliberately, described honestly: no code
    /// path reads it except to keep it monotone.
    pub generation: u64,
    /// `node_id -> that shard's seq the reader has consumed`, exclusive — the same convention
    /// [`ClusterJournal::read_shard_since`]'s `since_seq` uses. A shard absent from the map
    /// has contributed nothing to this cursor yet, which reads identically to a position of 0.
    pub pos: BTreeMap<NodeId, u64>,
}

/// On-wire shape of an encoded [`JournalCursor`]. Kept as its own type rather than deriving
/// (de)serialization on [`JournalCursor`] directly: the version tag belongs to the *encoding*,
/// not the cursor itself, so a future format change is a change to this struct (or a
/// replacement for it behind a new [`CURSOR_TOKEN_VERSION`]) and never touches what callers of
/// [`JournalCursor`] hold.
#[derive(Debug, Serialize, Deserialize)]
struct CursorPayload {
    v: u32,
    #[serde(rename = "gen")]
    generation: u64,
    pos: BTreeMap<NodeId, u64>,
}

/// The only payload version this build knows how to read. [`JournalCursor::decode`] rejects
/// every other value rather than guessing at what an unfamiliar shape means — the gate this
/// buys is exercised directly by issue #225's malformed-token test.
const CURSOR_TOKEN_VERSION: u32 = 1;

/// Why a vector cursor token was refused (issue #225).
///
/// Each variant is a distinct, named reason rather than one catch-all string: the front door
/// (a later issue) maps this to a typed 400, and an operator chasing a client's "cursor
/// rejected" report needs to know whether the token was mangled in transit, was never one of
/// ours, or names a format version this node does not speak — three different fixes.
#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    /// Not valid unpadded base64url — truncated, edited by hand, or never one of ours.
    #[error("cursor token is not valid base64url: {0}")]
    Encoding(#[from] base64::DecodeError),
    /// Valid base64, but the decoded bytes are not a [`CursorPayload`].
    #[error("cursor token does not decode to a cursor payload: {0}")]
    Payload(#[from] serde_json::Error),
    /// A well-formed payload this node does not know how to read: it decoded, but its
    /// version is not [`CURSOR_TOKEN_VERSION`]. Rejected rather than guessed at — a version
    /// this build has never seen could mean anything about the shape that follows it.
    #[error("cursor token version {0} is not supported by this node")]
    UnsupportedVersion(u32),
}

impl JournalCursor {
    /// An explicit position at the very beginning: no shard positions at all, which reads
    /// identically to every shard at 0.
    ///
    /// This is **not** what an absent `?since=` means. A baseline read is a snapshot of
    /// everything retained and so can never be truncated, whereas this is a reader asserting it
    /// has consumed nothing — which, against a shard that has evicted, means it has already
    /// missed something. The merged read keeps the two apart by passing `Option<&JournalCursor>`
    /// rather than substituting this value for absence; see `merge_shards_since`.
    #[must_use]
    pub fn start() -> Self {
        Self {
            generation: 0,
            pos: BTreeMap::new(),
        }
    }

    /// Encode this cursor as a versioned, opaque, unpadded base64url token — safe inside a
    /// query string and an SSE `id:` line, both of which forbid `+`, `/`, `=`, and whitespace.
    #[must_use]
    pub fn encode(&self) -> String {
        use base64::Engine as _;

        let payload = CursorPayload {
            v: CURSOR_TOKEN_VERSION,
            generation: self.generation,
            pos: self.pos.clone(),
        };
        // `CursorPayload`'s fields are all JSON primitives — two integers and a u64-keyed
        // map, which serde_json renders as string keys — so serialization cannot fail here.
        // The fallback exists only so this infallible, `String`-returning method never needs
        // `.unwrap()`/`.expect()`: if it were ever somehow hit, the result is a token that
        // fails to decode, not one that decodes to a wrong-but-plausible cursor.
        let bytes = serde_json::to_vec(&payload).unwrap_or_default();
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }

    /// Decode a token [`Self::encode`] produced. Rejects malformed base64, bytes that are not
    /// a [`CursorPayload`], and a payload whose version this build does not recognize — see
    /// [`CursorError`] and this type's doc comment for why none of these fall back to a
    /// default position.
    pub fn decode(token: &str) -> Result<Self, CursorError> {
        use base64::Engine as _;

        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token)?;
        let payload: CursorPayload = serde_json::from_slice(&bytes)?;
        if payload.v != CURSOR_TOKEN_VERSION {
            return Err(CursorError::UnsupportedVersion(payload.v));
        }
        Ok(Self {
            generation: payload.generation,
            pos: payload.pos,
        })
    }

    /// Decode `token`, accepting one legacy shape alongside [`Self::decode`]'s: a bare `u64`.
    /// That can only have come from a pre-#223 client, proxied straight to `this_node`'s own
    /// scalar `?since=` — the merged read did not exist yet to have issued anything else. It
    /// is read as `{this_node: that_seq}` at generation 0 (clear generations postdate every such
    /// client); every other shard starts at 0 because a client holding a bare scalar has
    /// provably never seen any of them.
    pub fn decode_or_legacy(token: &str, this_node: NodeId) -> Result<Self, CursorError> {
        if let Ok(seq) = token.parse::<u64>() {
            let mut pos = BTreeMap::new();
            pos.insert(this_node, seq);
            return Ok(Self { generation: 0, pos });
        }
        Self::decode(token)
    }
}

#[derive(Debug)]
struct PortShard {
    /// Ordered by `seq`, which is assigned under this same write lock — so deque order
    /// always matches seq order and [`ClusterJournal::attach_match`]'s binary search is
    /// exact rather than merely a heuristic.
    entries: RwLock<VecDeque<ShardEntry>>,
    /// This node's G-counter slot. Counts every request, recorded body or not.
    count: AtomicU64,
    /// Last seq handed out; 1-based, so 0 reads as "nothing recorded yet" on a first
    /// boot. Never reset — not by eviction, not by `clear`/`clear_flow`/`retain` — which
    /// is what keeps a cursor held across any of them valid.
    ///
    /// Across a *restart* it is not reset either, but nor does it start from 0: it starts
    /// at [`Self::boot_floor`], the durable floor this port had reached before the crash
    /// (issue #351). `node_id` is stable across restarts, so a counter that restarted at 0
    /// would re-issue `(node_id, seq)` keys the fleet still holds in its replica caches and
    /// still addresses with live cursors — silently replacing entries in merge dedup and
    /// silently withholding them from any walker positioned above the reused seq.
    seq: AtomicU64,
    /// The highest seq this shard may hand out before another durable reservation is
    /// required. Always at or above `seq`; see [`super::journal_seq::SeqFloors`] for why
    /// this is block-allocated rather than written per append.
    durable_floor: AtomicU64,
    /// The value `durable_floor` held at shard creation, i.e. the boundary between "seqs
    /// a previous boot could have used" and "seqs only this boot can use".
    ///
    /// Read-only after construction. Issue #349 needs exactly this to tell a peer's cached
    /// entry of ours apart from one we still hold: a cached seq at or below the boot floor
    /// is, by construction, an entry this node lost to the crash.
    boot_floor: u64,
    /// Highest seq dropped by *retention pressure* (cap or age). Deliberate deletions
    /// never touch it: losing entries you asked to delete is not a hole in your view.
    evicted_below_seq: AtomicU64,
    /// The port's clear generation, stamped into every entry appended under it and raised
    /// only by [`ClusterJournal::set_clear_gen`] (issue #224).
    clear_gen: AtomicU64,
    /// Per-space generations, populated only for spaces [`ClusterJournal::set_clear_gen`] has
    /// bumped. A separate `parking_lot::RwLock` from `entries`, not folded into it: the append
    /// path only ever reads one space's entry from here, so it should not have to fight a
    /// scoped clear (or another append) for the whole deque's lock to do it.
    space_gens: RwLock<HashMap<String, u64>>,
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
    fn new(port: u16, boot_floor: u64) -> Self {
        Self {
            entries: RwLock::new(VecDeque::new()),
            count: AtomicU64::new(0),
            seq: AtomicU64::new(boot_floor),
            durable_floor: AtomicU64::new(boot_floor),
            boot_floor,
            evicted_below_seq: AtomicU64::new(0),
            clear_gen: AtomicU64::new(0),
            space_gens: RwLock::new(HashMap::new()),
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
    /// Durable per-port seq high-waters (issue #351). Ephemeral unless the journal was
    /// built with a state directory, in which case a crash-restarted writer resumes
    /// strictly above every seq the previous boot could have handed out.
    seq_floors: SeqFloors,
}

impl ClusterJournal {
    /// A journal with **no** durable seq floors.
    ///
    /// Correct for embedders and tests, which have no state directory and no restart to
    /// survive. A clustered node wants [`Self::with_state_dir`] instead — without it a
    /// crash-restart re-issues `(node_id, seq)` keys the fleet still holds (issue #351).
    #[must_use]
    pub fn new(node_id: NodeId) -> Arc<Self> {
        Self::with_parts(
            node_id,
            JournalConfig::default(),
            Arc::new(MonotonicClock::default()),
        )
    }

    /// A journal whose seq floors persist under `state_dir`, so cursor identity survives
    /// a crash-restart (issue #351).
    ///
    /// Fails if the floor file exists but does not parse. That is deliberately fatal:
    /// see [`SeqFloors::load`] — starting from 0 over a damaged floor file is exactly the
    /// seq reuse the floors exist to prevent.
    pub fn with_state_dir(
        node_id: NodeId,
        state_dir: &std::path::Path,
    ) -> std::io::Result<Arc<Self>> {
        Ok(Self::assemble(
            node_id,
            JournalConfig::default(),
            Arc::new(MonotonicClock::default()),
            SeqFloors::load(state_dir)?,
        ))
    }

    #[must_use]
    pub fn with_parts(node_id: NodeId, config: JournalConfig, clock: Arc<dyn Clock>) -> Arc<Self> {
        Self::assemble(node_id, config, clock, SeqFloors::ephemeral())
    }

    fn assemble(
        node_id: NodeId,
        config: JournalConfig,
        clock: Arc<dyn Clock>,
        seq_floors: SeqFloors,
    ) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            ports: RwLock::new(HashMap::new()),
            voters: OnceLock::new(),
            cap_cache: AtomicUsize::new(0),
            cap_refreshed_at: AtomicU64::new(0),
            clock,
            config,
            seq_floors,
        })
    }

    /// The seq boundary between this boot and any previous one for `port`.
    ///
    /// Every seq at or below this value was handed out (or reserved) before the current
    /// process started; everything above it belongs to this boot.
    ///
    /// The value has to be captured at shard construction, because `durable_floor` advances
    /// during the run and nothing later can recover where this boot began.
    ///
    /// [`super::journal_net::JournalNet::pull_since_budgeted`] is the consumer (issue #349):
    /// a peer reporting that it caches an entry of ours at or below this floor is reporting,
    /// exactly, an entry the crash took from us — which is what lets a restarted writer stamp
    /// `Rift-Cluster-Partial` instead of answering short in silence.
    #[must_use]
    pub(crate) fn boot_floor(&self, port: u16) -> u64 {
        // Deliberately NOT `self.shard(port).boot_floor`: #349 asks this about ports named
        // by a *peer's* cached entries, which this node may never have served, and
        // `shard()` inserts on a miss — so the obvious spelling would allocate a
        // `PortShard` (RwLock, HashMap, Prometheus gauge) per probe. For a live shard the
        // two agree; for an untouched port the persisted floor IS its boot floor.
        self.ports
            .read()
            .get(&port)
            .map_or_else(|| self.seq_floors.floor(port), |shard| shard.boot_floor)
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

    /// This writer's own node id — the one every entry this journal appends is
    /// stamped with. Fixed at construction (see the struct doc), so this reads
    /// the same before and after [`Self::bind`]; issue #223's network layer
    /// needs it to build its own [`super::journal_net::ShardSlice`] without
    /// waiting on a `RaftNode` that may not exist yet.
    #[must_use]
    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Every port this node has ever recorded to or noted a request for.
    ///
    /// Issue #223's anti-entropy pull needs this to know which ports to ask
    /// peers about; nothing before it needed a port registry, since each port
    /// is otherwise addressed directly by callers who already know it.
    #[must_use]
    pub(crate) fn known_ports(&self) -> Vec<u16> {
        self.ports.read().keys().copied().collect()
    }

    /// This node's shard of `port`, from `since_seq` exclusive (0 for everything).
    #[must_use]
    pub fn read_shard_since(&self, port: u16, since_seq: u64) -> ShardRead {
        let shard = self.shard(port);
        let entries = shard.entries.read();
        // Sorted here rather than left in `HashMap` iteration order: this is the payload a
        // merge and the wire both consume, and both need every node to agree on the order.
        let mut space_gens: Vec<(String, u64)> = shard
            .space_gens
            .read()
            .iter()
            .map(|(space, generation)| (space.clone(), *generation))
            .collect();
        space_gens.sort_by(|a, b| a.0.cmp(&b.0));
        ShardRead {
            entries: entries
                .iter()
                .filter(|entry| entry.seq > since_seq)
                .cloned()
                .collect(),
            evicted_below_seq: shard.evicted_below_seq.load(Ordering::SeqCst),
            clear_gen: shard.clear_gen.load(Ordering::SeqCst),
            space_gens,
            count_slot: shard.count.load(Ordering::SeqCst),
        }
    }

    /// Apply a clear generation bump for `port` (issue #224): `space: None` raises the port's
    /// own generation, which every subsequent append is stamped with regardless of space;
    /// `Some(space)` raises only that space's. This is *applied* Raft state — the state
    /// machine calls it from the apply path when `ControlOp::JournalClearGen` commits, never a
    /// caller working ahead of consensus — so it must be monotone per key: a late or
    /// re-delivered apply carrying a lower generation than what is already stamped is ignored
    /// rather than un-clearing a port a newer entry already committed. Apply is strictly
    /// increasing per key by construction (each commit stores `current + 1`), so this guard
    /// only ever fires against a duplicate or out-of-order redelivery, never a legitimate one.
    ///
    /// This monotonicity is specific to the apply path. [`Self::reset_clear_gen`] is the other
    /// caller of this generation — snapshot install — and deliberately does *not* go through
    /// here: a fleet's agreed-on generation can be *lower* than what this node still holds, and
    /// `fetch_max` structurally cannot express that.
    ///
    /// Creates the port's shard if this is the first thing ever recorded against it — a clear
    /// can commit before any append does. Touches nothing else: `seq`, `evicted_below_seq`,
    /// the entries and the count slot stay exactly as `record_indexed` and the deletion
    /// methods left them, because losing entries is not this method's job. See
    /// [`Self::zero_count`] for the one apply-path case that *does* touch the count slot.
    pub fn set_clear_gen(&self, port: u16, space: Option<&str>, generation: u64) {
        let shard = self.shard(port);
        match space {
            None => {
                shard.clear_gen.fetch_max(generation, Ordering::SeqCst);
            }
            Some(space) => {
                let mut space_gens = shard.space_gens.write();
                space_gens
                    .entry(space.to_string())
                    .and_modify(|current| *current = (*current).max(generation))
                    .or_insert(generation);
            }
        }
    }

    /// Set `port`'s clear generation (or `port`'s `space`) to exactly `generation`, regardless
    /// of what is currently stamped (issue #224, Non-blocker 1). The one caller allowed to
    /// *lower* it: `install_snapshot` clears and reinserts `sm_journal_gens` from the payload
    /// because a generation this node still holds could be higher than what the fleet has since
    /// agreed on (a stale leader that cleared, was partitioned, and rejoined by snapshot from a
    /// peer that never saw that clear) — and the live journal must follow the durable table
    /// down, not just up, or this one node's stuck-high generation would silently win the
    /// fleet-wide max a merge computes, dropping every other node's entries from the read.
    ///
    /// Never call this from the apply path — [`Self::set_clear_gen`]'s `fetch_max` is what keeps
    /// a duplicate or re-delivered apply from un-clearing a port a newer entry already
    /// committed, and this method has no such guard.
    pub fn reset_clear_gen(&self, port: u16, space: Option<&str>, generation: u64) {
        let shard = self.shard(port);
        match space {
            None => shard.clear_gen.store(generation, Ordering::SeqCst),
            Some(space) => {
                shard
                    .space_gens
                    .write()
                    .insert(space.to_string(), generation);
            }
        }
    }

    /// Zero this node's G-counter slot for `port` (issue #224, Blocker 1). A **port-wide**
    /// clear now commits through Raft as a generation bump rather than a call into the local
    /// engine, so nothing else zeroes `numberOfRequests` — before this feature, the unscoped
    /// `DELETE savedRequests` proxied straight to [`Self::clear`], which does. The apply path
    /// calls this only for `space: None`; a space-scoped bump must leave the count alone,
    /// matching [`Self::clear_flow`]'s and `retain`'s existing contract that a scoped deletion
    /// never resets the total.
    ///
    /// Deliberately a separate operation from [`Self::set_clear_gen`], not folded into it:
    /// `set_clear_gen` is also called by cold-start rehydration and (via
    /// [`Self::reset_clear_gen`]) snapshot install, and re-zeroing the count on either of those
    /// would erase real, already-committed traffic the fleet never asked to forget — only a
    /// *newly applied* port-wide clear should ever zero it.
    ///
    /// Each node zeros its own slot as it applies, independently — a fleet sum taken mid-apply
    /// can therefore differ transiently across nodes by apply lag, same as any other
    /// per-node-on-apply projection in this state machine.
    pub fn zero_count(&self, port: u16) {
        self.shard(port).count.store(0, Ordering::SeqCst);
    }

    /// This node's currently-live **port** clear generation for `port` (issue #224) — never
    /// a space generation, which has no single value to report here. Reads the atomic
    /// directly rather than going through [`Self::read_shard_since`], which would pay for
    /// walking (and cloning) the deque to build a `ShardRead` this caller only wants one
    /// field of; [`super::journal_net::journal_routes`]'s counts handler is the caller, and
    /// it needs this alongside [`RequestJournal::count`] to answer `numberOfRequests`
    /// generation-aware (see [`super::journal_net::fleet_count`]).
    #[must_use]
    pub(crate) fn clear_gen(&self, port: u16) -> u64 {
        self.shard(port).clear_gen.load(Ordering::SeqCst)
    }

    fn shard(&self, port: u16) -> Arc<PortShard> {
        if let Some(shard) = self.ports.read().get(&port) {
            return Arc::clone(shard);
        }
        // The floor is read here, at first touch of the port, rather than eagerly for
        // every persisted port at startup: shards are created lazily and a node that
        // never serves a port has no counter to protect.
        let boot_floor = self.seq_floors.floor(port);
        Arc::clone(
            self.ports
                .write()
                .entry(port)
                .or_insert_with(|| Arc::new(PortShard::new(port, boot_floor))),
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
        // Resolved before the lock, to keep the critical section to the deque mutation and
        // the seq assignment that must be atomic with it. `space_gens` is a separate lock
        // from `entries` and is only ever read here — `set_clear_gen` never touches
        // `entries` — so taking it first and releasing it before the write lock below
        // introduces no ordering between the two that could deadlock.
        let cap = self.shard_cap();
        let flow = flow_id.to_string();
        let space_gen = shard.space_gens.read().get(flow_id).copied().unwrap_or(0);
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
            // The durability invariant (issue #351): never hand out a seq above the floor
            // already on disk, so a crash-restart resumes strictly above everything this
            // boot could have used. Inside the lock on purpose — a reservation racing the
            // assignment could let a seq escape above an unpersisted floor, which is the
            // one ordering that reintroduces the collision. The cost lands once per
            // `SEQ_FLOOR_SLACK` appends, so blocking the deque for that write is a
            // per-2^20-requests event, not a hot-path one.
            if seq > shard.durable_floor.load(Ordering::SeqCst) {
                let (reserved, persisted) = self.seq_floors.reserve_through(port, seq);
                shard.durable_floor.store(reserved, Ordering::SeqCst);
                if let Err(e) = persisted {
                    // Not swallowed, and not fatal. The entries this numbers are volatile
                    // by design, so refusing to record would turn a durability failure
                    // into a total outage of the thing the node exists to do. What is
                    // lost is crash-safety of the counter until the disk recovers, which
                    // is worth an error-level line every time it happens — and it can
                    // only happen once per reserved block, so this cannot become a log
                    // storm on a persistently bad disk.
                    tracing::error!(
                        port,
                        seq,
                        error = %e,
                        "could not persist the journal seq floor; a crash before it \
                         recovers may reuse seqs and corrupt cursor identity"
                    );
                }
            }
            entries.push_back(ShardEntry {
                node_id: self.node_id,
                seq,
                clear_gen: shard.clear_gen.load(Ordering::SeqCst),
                space_gen,
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

    // ---- Vector cursors (issue #225) --------------------------------------------------
    //
    // A scalar cursor cannot address a multi-writer merge: "index 500" names a position in
    // whose shard? The token carries a position per shard plus the clear generation it was
    // issued under, so a walk is gapless and duplicate-free *per shard* across membership
    // changes and clears alike.

    #[test]
    fn a_cursor_round_trips_through_its_token() {
        let cursor = JournalCursor {
            generation: 7,
            pos: [(1u64, 10u64), (2, 0), (9, u64::MAX)].into_iter().collect(),
        };
        let token = cursor.encode();
        assert_eq!(
            JournalCursor::decode(&token).expect("round trip"),
            cursor,
            "a token must decode to exactly the cursor that issued it"
        );
    }

    #[test]
    fn a_token_is_opaque_and_url_safe() {
        // Opaque is a contract, not an aesthetic: clients must not parse it, and it travels in a
        // query string and an SSE `id:` line, so it cannot carry `+`, `/` or `=`.
        let token = JournalCursor {
            generation: u64::MAX,
            pos: [(u64::MAX, u64::MAX)].into_iter().collect(),
        }
        .encode();
        assert!(
            token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "token must be unpadded base64url: {token}"
        );
        assert!(
            !token.contains(char::is_whitespace),
            "a token with whitespace would not survive an SSE id: line"
        );
    }

    #[test]
    fn an_empty_cursor_round_trips() {
        // The first read of a port issues a cursor with no shard positions at all.
        let empty = JournalCursor {
            generation: 0,
            pos: BTreeMap::new(),
        };
        assert_eq!(
            JournalCursor::decode(&empty.encode()).expect("round trip"),
            empty
        );
    }

    #[test]
    fn a_malformed_token_is_rejected_rather_than_defaulted() {
        // Defaulting a bad cursor to "start from 0" would silently replay the whole journal;
        // defaulting it to "current" would silently skip entries. Both are worse than a 400.
        for bad in [
            "not base64!!",
            "",
            "Zm9v",                                // valid base64url, not our JSON
            "eyJ2Ijo5OTksImdlbiI6MCwicG9zIjp7fX0", // well-formed but version 999
        ] {
            assert!(
                JournalCursor::decode(bad).is_err(),
                "must reject {bad:?} rather than silently choosing a position"
            );
        }
    }

    #[test]
    fn a_legacy_scalar_cursor_is_read_as_this_nodes_position() {
        // Any bare u64 a client holds predates the merged read (which has issued no cursor at
        // all until now), so it can only have come from a proxied per-node read of THIS node.
        let cursor = JournalCursor::decode_or_legacy("42", 7).expect("a scalar is accepted");
        assert_eq!(cursor.pos.get(&7).copied(), Some(42));
        assert_eq!(
            cursor.pos.len(),
            1,
            "every other shard starts at 0 — the client has provably seen none of them"
        );
        assert_eq!(
            cursor.generation, 0,
            "a legacy cursor predates clear generations"
        );
    }

    #[test]
    fn decode_or_legacy_still_rejects_what_is_neither() {
        assert!(JournalCursor::decode_or_legacy("-1", 7).is_err());
        assert!(JournalCursor::decode_or_legacy("not base64!!", 7).is_err());
    }

    proptest::proptest! {
        #[test]
        fn any_cursor_survives_a_round_trip(
            generation in proptest::prelude::any::<u64>(),
            pairs in proptest::collection::vec(
                (proptest::prelude::any::<u64>(), proptest::prelude::any::<u64>()),
                0..8,
            ),
        ) {
            let cursor = JournalCursor { generation, pos: pairs.into_iter().collect() };
            let decoded = JournalCursor::decode(&cursor.encode())
                .expect("an encoded cursor always decodes");
            proptest::prop_assert_eq!(decoded, cursor);
        }
    }

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
            "a port that has never been cleared stamps generation 0"
        );
        assert_eq!(shard.entries[0].flow_id, "flow-a");
        assert_eq!(shard.entries[1].request.path, "/b");
    }

    // ---- Clear generations (issue #224) ----------------------------------------------
    //
    // The generation is *applied* state: `set_clear_gen` is what the Raft state machine calls
    // when `ControlOp::JournalClearGen` commits. Nothing here reaches consensus; these pin the
    // local half — that a bump changes what subsequent appends are stamped with, and that it
    // touches nothing else.

    #[test]
    fn a_port_generation_bump_stamps_every_subsequent_append() {
        let j = bound(3, 7);
        j.record_indexed(1, "flow-a", req("/before"));

        j.set_clear_gen(1, None, 1);
        j.record_indexed(1, "flow-a", req("/after"));

        let shard = j.read_shard_since(1, 0);
        assert_eq!(
            shard
                .entries
                .iter()
                .map(|e| (e.request.path.as_str(), e.clear_gen))
                .collect::<Vec<_>>(),
            vec![("/before", 0), ("/after", 1)],
            "the bump stamps appends after it and rewrites nothing before it — the older \
             entries are dropped by the merge, not deleted here"
        );
        assert_eq!(
            shard.clear_gen, 1,
            "the shard publishes its current generation so a merge can compute the threshold"
        );
    }

    #[test]
    fn a_space_generation_bump_stamps_only_its_own_spaces_appends() {
        let j = bound(3, 7);
        j.set_clear_gen(1, Some("flow-a"), 4);

        j.record_indexed(1, "flow-a", req("/a"));
        j.record_indexed(1, "flow-b", req("/b"));

        let shard = j.read_shard_since(1, 0);
        let stamped: Vec<_> = shard
            .entries
            .iter()
            .map(|e| (e.flow_id.as_str(), e.clear_gen, e.space_gen))
            .collect();
        assert_eq!(
            stamped,
            vec![("flow-a", 0, 4), ("flow-b", 0, 0)],
            "a space bump raises only its own space's stamp; a sibling space and the \
             port-wide generation are untouched"
        );
        assert_eq!(
            shard.space_gens,
            vec![("flow-a".to_owned(), 4)],
            "only spaces that have ever been bumped occupy a row"
        );
    }

    #[test]
    fn a_generation_bump_never_moves_seq_or_the_eviction_watermark() {
        // Cursor validity across clears (#225 depends on this). A clear deletes nothing
        // locally, so there is no hole to report and no counter to rewind.
        let j = bound(3, 7);
        j.record_indexed(1, "f", req("/a"));
        j.record_indexed(1, "f", req("/b"));
        let before = j.read_shard_since(1, 0);

        j.set_clear_gen(1, None, 1);
        j.set_clear_gen(1, Some("f"), 1);
        j.record_indexed(1, "f", req("/c"));

        let after = j.read_shard_since(1, 0);
        assert_eq!(
            after.evicted_below_seq, before.evicted_below_seq,
            "a deliberate clear is not retention pressure, so the watermark must not move"
        );
        assert_eq!(
            after.entries.last().expect("appended").seq,
            3,
            "seq keeps counting across a clear rather than restarting at 1"
        );
    }

    #[test]
    fn a_stale_generation_never_moves_the_stamp_backwards() {
        // Apply is monotone per key, but a late/duplicate delivery must not un-clear a port.
        let j = bound(3, 7);
        j.set_clear_gen(1, None, 5);
        j.set_clear_gen(1, None, 2);

        j.record_indexed(1, "f", req("/a"));
        assert_eq!(
            j.read_shard_since(1, 0).entries[0].clear_gen,
            5,
            "an out-of-order lower generation is ignored, not applied"
        );
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

    // -- Durable seq floors across a crash-restart (issue #351) ---------------
    //
    // "Restart" here means constructing a second `ClusterJournal` over the same state
    // directory while the first is simply dropped. That is the right model: a SIGKILL
    // runs no destructor and flushes nothing, so a clean-shutdown path would be testing
    // a code path the crash case never takes.

    fn restarted(dir: &std::path::Path, node_id: NodeId) -> Arc<ClusterJournal> {
        ClusterJournal::with_state_dir(node_id, dir).expect("journal over state dir")
    }

    #[test]
    fn a_first_boot_starts_at_one_and_persists_its_floor() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let j = restarted(dir.path(), 1);
        assert_eq!(j.boot_floor(8080), 0, "nothing persisted yet");
        assert_eq!(j.record_indexed(8080, "f", req("/a")), Some(1));

        // The floor is on disk now, so a restart cannot re-issue seq 1.
        let after = restarted(dir.path(), 1);
        assert!(
            after.boot_floor(8080) >= 1,
            "boot floor {} must cover the seq already handed out",
            after.boot_floor(8080)
        );
    }

    #[test]
    fn a_restarted_writer_never_reuses_a_seq() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let first = restarted(dir.path(), 1);
        let pre: Vec<u64> = (0..5)
            .map(|i| {
                first
                    .record_indexed(8080, "f", req(&format!("/pre{i}")))
                    .expect("recorded")
            })
            .collect();
        drop(first);

        let second = restarted(dir.path(), 1);
        let post: Vec<u64> = (0..5)
            .map(|i| {
                second
                    .record_indexed(8080, "f", req(&format!("/post{i}")))
                    .expect("recorded")
            })
            .collect();

        // Positive control first, per this module's convention: prove the pre-crash seqs
        // were real before asserting anything about their absence from the post set.
        assert_eq!(pre, vec![1, 2, 3, 4, 5]);
        for seq in &post {
            assert!(
                *seq > *pre.last().expect("pre is non-empty"),
                "post-restart seq {seq} collides with the pre-crash range {pre:?}"
            );
        }
    }

    #[test]
    fn each_port_keeps_its_own_floor_across_a_restart() {
        // A single shared counter would be correct-but-wasteful here; a per-port floor
        // that leaked across ports would be silently wrong. Pin the per-port shape.
        let dir = tempfile::TempDir::new().expect("tempdir");
        let first = restarted(dir.path(), 1);
        assert_eq!(first.record_indexed(8080, "f", req("/a")), Some(1));
        assert_eq!(first.record_indexed(9090, "f", req("/a")), Some(1));
        drop(first);

        let second = restarted(dir.path(), 1);
        let a = second.record_indexed(8080, "f", req("/b")).expect("8080");
        let b = second.record_indexed(9090, "f", req("/b")).expect("9090");
        assert!(
            a > 1 && b > 1,
            "both ports resume above their own pre-crash seq"
        );
        // 8081 was never touched before the crash, so it is a first boot for that port.
        assert_eq!(second.record_indexed(8081, "f", req("/c")), Some(1));
    }

    #[test]
    fn a_cursor_held_across_a_restart_sees_every_new_entry_exactly_once() {
        // The cursor contract is what this issue is really about: a walker positioned at
        // the pre-crash high-water must receive every post-restart entry, none withheld
        // by `entry.seq > position` and none delivered twice.
        let dir = tempfile::TempDir::new().expect("tempdir");
        let first = restarted(dir.path(), 1);
        for i in 0..3 {
            first.record_indexed(8080, "f", req(&format!("/pre{i}")));
        }
        let cursor_position = first
            .read_shard_since(8080, 0)
            .entries
            .last()
            .expect("entries")
            .seq;
        drop(first);

        let second = restarted(dir.path(), 1);
        for i in 0..4 {
            second.record_indexed(8080, "f", req(&format!("/post{i}")));
        }

        let delivered: Vec<u64> = second
            .read_shard_since(8080, 0)
            .entries
            .iter()
            .filter(|e| e.seq > cursor_position)
            .map(|e| e.seq)
            .collect();
        assert_eq!(
            delivered.len(),
            4,
            "every post-restart entry reaches the walker"
        );
        let unique: std::collections::BTreeSet<u64> = delivered.iter().copied().collect();
        assert_eq!(unique.len(), delivered.len(), "no seq delivered twice");
    }

    #[test]
    fn merging_a_pre_crash_read_with_the_post_restart_shard_collides_on_nothing() {
        // The other corruption site: a survivor's replica cache still holds the pre-crash
        // entries under `(node_id, seq)`. If the reborn writer reuses those keys, merge
        // dedup drops one of the two by identity and an entry silently vanishes.
        let dir = tempfile::TempDir::new().expect("tempdir");
        let first = restarted(dir.path(), 7);
        for i in 0..3 {
            first.record_indexed(8080, "f", req(&format!("/pre{i}")));
        }
        let cached = first.read_shard_since(8080, 0).entries;
        drop(first);

        let second = restarted(dir.path(), 7);
        for i in 0..3 {
            second.record_indexed(8080, "f", req(&format!("/post{i}")));
        }
        let live = second.read_shard_since(8080, 0).entries;

        assert_eq!(
            cached.len(),
            3,
            "positive control: the cache really holds three"
        );
        assert_eq!(live.len(), 3);
        let keys: std::collections::BTreeSet<(NodeId, u64)> = cached
            .iter()
            .chain(live.iter())
            .map(|e| (e.node_id, e.seq))
            .collect();
        assert_eq!(
            keys.len(),
            6,
            "all six entries must have distinct (node_id, seq) merge keys"
        );
    }

    #[test]
    fn allocation_never_outruns_what_is_durable() {
        // The invariant, stated directly: whatever seq has been handed out, a restart
        // right now resumes strictly above it.
        let dir = tempfile::TempDir::new().expect("tempdir");
        let j = restarted(dir.path(), 1);
        let mut highest = 0;
        for i in 0..50 {
            highest = j
                .record_indexed(8080, "f", req(&format!("/{i}")))
                .expect("recorded");
            let observer = restarted(dir.path(), 1);
            assert!(
                observer.boot_floor(8080) >= highest,
                "durable floor {} trails the handed-out seq {highest}",
                observer.boot_floor(8080)
            );
        }
        assert_eq!(highest, 50, "positive control: 50 appends really happened");
    }

    #[test]
    fn a_journal_without_a_state_dir_behaves_exactly_as_before() {
        // Embedders and the whole existing test suite take this path. It must keep
        // starting at 1 -- the pre-#351 behaviour -- rather than acquiring a floor.
        let j = journal();
        assert_eq!(j.record_indexed(8080, "f", req("/a")), Some(1));
        assert_eq!(j.record_indexed(8080, "f", req("/b")), Some(2));
        assert_eq!(j.boot_floor(8080), 0);
    }

    #[test]
    fn a_corrupt_floor_file_stops_the_journal_from_starting() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        std::fs::write(dir.path().join("journal-seq-floors"), "8080=garbage\n").expect("write");
        // Matched rather than `expect_err`ed: the Ok arm carries an `Arc<ClusterJournal>`,
        // which is not `Debug`, and deriving Debug on the whole journal to satisfy a test
        // assertion would be the tail wagging the dog.
        match ClusterJournal::with_state_dir(1, dir.path()) {
            Ok(_) => panic!("a corrupt floor file must not silently start from zero"),
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidData),
        }
    }
}
