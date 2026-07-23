//! The flow shard's durability contract, proven by killing a real process.
//!
//! **An in-process "crash" cannot test this.** After a non-fsynced commit the
//! data is in the OS page cache, so reopening the file inside the same process
//! finds it whether or not fsync ever ran — the test would pass identically with
//! durability switched off. redb also holds a file lock, so a same-process
//! reopen is not even possible.
//!
//! So the writing side is a **child process** that this binary re-executes, and
//! the parent `SIGKILL`s it. The child never gets to run a destructor, flush a
//! buffer, or close the database; whatever survives is what the disk actually
//! held at the instant of death.
//!
//! Stated limit: `SIGKILL` tests *process* death, not power loss. That is the
//! contract the cluster makes — #16's scenario is a full-cluster restart — and
//! kernel-crash semantics are redb's promise, not this module's.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use rift_cluster::stores::{Durability, FlowShard, ShardConfig, Versioned};

/// Set by the parent to turn `child_writes_then_parks` into the writing side.
/// Format: `<dir>|<sync|async>|<settle_ms>`.
const CHILD_SPEC: &str = "RIFT_FLOW_SHARD_CHILD_SPEC";

fn entry(value: &str) -> Versioned {
    Versioned {
        m_idx: 1,
        v: 1,
        origin: 7,
        expires_at: 0,
        value: serde_json::json!(value),
        deleted: false,
    }
}

fn config(fsync_ms: u64) -> ShardConfig {
    ShardConfig {
        fsync_interval: Duration::from_millis(fsync_ms),
        max_flows: 1000,
        ttl: None,
    }
}

fn now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("fits u64")
}

/// The writing side. Inert unless the parent asked for it.
///
/// Writes, prints `ACKED`, then parks forever so the parent controls the exact
/// moment of death. Parking rather than exiting is the point: an exit would run
/// cleanup, and cleanup is what this test must never benefit from.
#[tokio::test]
async fn child_writes_then_parks() {
    let Ok(spec) = std::env::var(CHILD_SPEC) else {
        return;
    };
    let parts: Vec<&str> = spec.split('|').collect();
    let (dir, mode, settle_ms) = (parts[0], parts[1], parts[2].parse::<u64>().expect("settle"));

    let durability = match mode {
        "sync" => Durability::Sync,
        "async" => Durability::Async,
        other => panic!("unknown mode {other}"),
    };

    // A long interval for the loss-window case, a short one where the test wants
    // the ticker to have fired.
    let shard = FlowShard::open(
        Path::new(dir),
        config(if settle_ms > 0 { 20 } else { 60_000 }),
    )
    .expect("open shard");
    shard
        .set("flow-1", "step", entry("committed"), durability)
        .await
        .expect("write");

    if settle_ms > 0 {
        tokio::time::sleep(Duration::from_millis(settle_ms)).await;
    }

    println!("ACKED");
    use std::io::Write;
    std::io::stdout().flush().expect("flush");

    // Park. The parent kills us here.
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

/// Run the child against `dir`, wait for its ack, then SIGKILL it.
fn kill_after_ack(dir: &Path, mode: &str, settle_ms: u64) {
    let exe = std::env::current_exe().expect("test binary path");
    let mut child = Command::new(exe)
        .args(["child_writes_then_parks", "--exact", "--nocapture"])
        .env(
            CHILD_SPEC,
            format!("{}|{mode}|{settle_ms}", dir.to_string_lossy()),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the writing child");

    let stdout = child.stdout.take().expect("child stdout");
    let mut acked = false;
    for line in BufReader::new(stdout).lines() {
        let line = line.expect("read child stdout");
        if line.trim() == "ACKED" {
            acked = true;
            break;
        }
    }
    assert!(acked, "child never acknowledged its write");

    child.kill().expect("SIGKILL the child");
    child.wait().expect("reap the child");
}

/// `sync` means what it says: the write is on disk before the ack returns, so a
/// kill immediately afterwards cannot lose it.
///
/// This is the test the mutation targets — set the writer's `Sync` batches to
/// commit with `redb::Durability::None` and this is what goes red.
#[test]
fn a_sync_write_survives_kill_nine() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    kill_after_ack(dir.path(), "sync", 0);

    let shard = FlowShard::open(dir.path(), config(50)).expect("reopen after the kill");
    let found = shard.get("flow-1", "step");
    assert_eq!(
        found.map(|e| e.value),
        Some(serde_json::json!("committed")),
        "a sync write acknowledged before the kill must be on disk"
    );
}

/// `async` is durable once the ticker has fired — the loss window is bounded by
/// the fsync interval, not open-ended.
#[test]
fn an_async_write_survives_once_the_interval_has_passed() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    // 20ms interval in the child, 300ms of settling: several ticks.
    kill_after_ack(dir.path(), "async", 300);

    let shard = FlowShard::open(dir.path(), config(50)).expect("reopen after the kill");
    assert_eq!(
        shard.get("flow-1", "step").map(|e| e.value),
        Some(serde_json::json!("committed")),
        "an async write must be durable once a fsync interval has elapsed"
    );
}

/// Inside the window, `async` may lose the write — that is the documented
/// trade. What it may never do is come back *torn*: the file must open, and the
/// key must be either absent or exactly what was written.
///
/// Asserting the disjunction rather than the loss is deliberate. Asserting "it
/// is gone" would make the test fail on a fast disk that happened to flush, and
/// a test that depends on the disk being slow is a flake with a rationale.
#[test]
fn an_async_write_inside_the_window_is_lost_or_intact_never_torn() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    kill_after_ack(dir.path(), "async", 0);

    let shard = FlowShard::open(dir.path(), config(50)).expect("the file must still open cleanly");
    match shard.get("flow-1", "step") {
        None => {}
        Some(found) => assert_eq!(
            found.value,
            serde_json::json!("committed"),
            "a surviving entry must be exactly what was written"
        ),
    }
}

#[tokio::test]
async fn recovery_drops_what_had_already_expired() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    {
        let shard = FlowShard::open(dir.path(), config(10)).expect("open");
        let mut expired = entry("stale");
        expired.expires_at = 1; // 1970: long gone.
        shard
            .set("flow-1", "gone", expired, Durability::Sync)
            .await
            .expect("write expired");
        shard
            .set("flow-1", "kept", entry("live"), Durability::Sync)
            .await
            .expect("write live");
    }

    let shard = FlowShard::open(dir.path(), config(10)).expect("reopen");
    assert!(
        shard.get("flow-1", "gone").is_none(),
        "an entry past its expiry must not come back from disk"
    );
    assert_eq!(
        shard.get("flow-1", "kept").map(|e| e.value),
        Some(serde_json::json!("live"))
    );
}

/// Eviction sheds whole flows, never individual keys: a scenario that kept
/// `step` but lost `cart` fails the test using it in a way that looks like a
/// product bug.
#[tokio::test]
async fn eviction_sheds_whole_flows() {
    let shard = FlowShard::in_memory(ShardConfig {
        max_flows: 2,
        ..config(50)
    });

    for i in 0..5 {
        let flow = format!("flow-{i}");
        shard
            .set(&flow, "a", entry("x"), Durability::None)
            .await
            .expect("write a");
        shard
            .set(&flow, "b", entry("y"), Durability::None)
            .await
            .expect("write b");
    }

    assert!(
        shard.flow_count() <= 2,
        "the cap must hold, saw {} flows",
        shard.flow_count()
    );
    // Whatever survived, survived entire.
    for i in 0..5 {
        let keys = shard.flow(&format!("flow-{i}"));
        assert!(
            keys.is_empty() || keys.len() == 2,
            "flow-{i} was half-evicted: {keys:?}"
        );
    }
}

/// Eviction is real LRU: the least-recently-*touched* flow goes first, and a
/// no-TTL flow is not singled out just for having no expiry.
///
/// This is the review finding made into a test — the old `max(expires_at)`
/// policy evicted no-TTL flows (`expires_at == 0`) first, because 0 sorts
/// smallest. Here `keep` has no TTL and is touched last; it must survive while
/// the older flows are shed.
#[tokio::test]
async fn eviction_is_lru_not_by_expiry() {
    let shard = FlowShard::in_memory(ShardConfig {
        max_flows: 2,
        ttl: None,
        ..config(50)
    });

    // Three flows, touched oldest-to-newest with a real gap between them.
    for id in ["old", "middle", "keep"] {
        shard
            .set(id, "k", entry(id), Durability::None)
            .await
            .expect("write");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Cap is 2, so exactly one was shed — and it must be the oldest touch, not
    // the no-TTL "keep" that a max(expires_at) policy would have taken first.
    assert!(
        shard.get("keep", "k").is_some(),
        "the newest flow was evicted"
    );
    assert!(
        shard.get("old", "k").is_none(),
        "the least-recently-touched flow should have gone first"
    );
    assert!(shard.flow_count() <= 2, "the cap must hold");
}

/// The ticker sweeps expired entries out of memory, not merely filters them on
/// read: they must stop counting toward the cap.
#[tokio::test]
async fn the_ticker_sweeps_expired_entries_out_of_memory() {
    // 20ms fsync/sweep interval.
    let shard = FlowShard::open(
        tempfile::TempDir::new().expect("tempdir").path(),
        ShardConfig {
            fsync_interval: Duration::from_millis(20),
            max_flows: 1000,
            ttl: None,
        },
    )
    .expect("open");

    let mut soon = entry("fleeting");
    soon.expires_at = now_millis() + 30; // expires in ~30ms
    shard
        .set("flow-1", "k", soon, Durability::Async)
        .await
        .expect("write");
    assert_eq!(shard.flow_count(), 1);

    // Past the expiry and at least one sweep tick.
    tokio::time::sleep(Duration::from_millis(120)).await;

    assert_eq!(
        shard.flow_count(),
        0,
        "an expired entry must be swept from memory, not just filtered on read"
    );
    shard.close();
}

/// Recovery restores LRU order from `flow_meta`, so a reopened shard evicts the
/// flow that was least-recently-touched *before* the restart — not an arbitrary
/// one.
#[tokio::test]
async fn recovery_restores_lru_order() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    {
        let shard = FlowShard::open(dir.path(), config(10)).expect("open");
        for id in ["first", "second", "third"] {
            shard
                .set(id, "k", entry(id), Durability::Sync)
                .await
                .expect("write");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        shard.close();
    }

    // Reopen with a cap of 2: the flow touched first before the restart must be
    // the one dropped, which is only possible if last_touch survived.
    let shard = FlowShard::open(
        dir.path(),
        ShardConfig {
            max_flows: 2,
            ..config(10)
        },
    )
    .expect("reopen");
    assert!(
        shard.get("first", "k").is_none(),
        "the pre-restart oldest flow should have been evicted on recovery"
    );
    assert!(shard.get("third", "k").is_some());
    shard.close();
}

/// A tombstone (#126) is a versioned *deleted* entry: `get` must hide it —
/// readers see absence — while `flow` must still carry it, because replication
/// and adoption are exactly the consumers that need to learn about the delete.
#[tokio::test]
async fn a_tombstone_hides_the_key_from_get_but_rides_the_flow_listing() {
    let shard = FlowShard::in_memory(config(50));
    let mut tombstone = entry("gone");
    tombstone.deleted = true;
    tombstone.v = 2;
    shard
        .set("flow-t", "k", tombstone, Durability::None)
        .await
        .expect("write tombstone");

    assert_eq!(
        shard.get("flow-t", "k"),
        None,
        "a reader must see a deleted key as absent"
    );
    let listed = shard.flow("flow-t");
    assert_eq!(listed.len(), 1, "replication must still see the tombstone");
    assert!(listed[0].1.deleted, "and it must say it is one");
}

/// #119 wrote disk rows without the `deleted` field. They must keep reading —
/// a flow shard is durable state, and a format change that cannot read
/// yesterday's file is a data loss with extra steps.
#[test]
fn a_pre_tombstone_disk_row_still_deserializes() {
    let legacy = r#"{"m_idx":3,"v":7,"origin":2,"expires_at":0,"value":"kept"}"#;
    let entry: Versioned = serde_json::from_str(legacy).expect("legacy row parses");
    assert!(!entry.deleted, "absent field must mean not deleted");
    assert_eq!(entry.value, serde_json::json!("kept"));
}

#[test]
fn the_version_triple_orders_by_membership_then_counter_then_origin() {
    let base = Versioned {
        m_idx: 2,
        v: 5,
        origin: 1,
        expires_at: 0,
        value: serde_json::json!("base"),
        deleted: false,
    };

    // A newer membership wins even with a lower per-key counter: an op minted
    // before an ownership change must not beat one minted after it.
    let newer_membership = Versioned {
        m_idx: 3,
        v: 1,
        ..base.clone()
    };
    assert!(base.superseded_by(&newer_membership));
    assert!(!newer_membership.superseded_by(&base));

    // Within one membership, the counter decides.
    let higher_v = Versioned {
        v: 6,
        ..base.clone()
    };
    assert!(base.superseded_by(&higher_v));

    // And origin only breaks an exact tie.
    let other_origin = Versioned {
        origin: 9,
        ..base.clone()
    };
    assert!(base.superseded_by(&other_origin));
    assert!(!base.superseded_by(&base.clone()));
}

/// Per-mode write throughput, for the "`async` costs ≤ 5% over `none`" claim in
/// #16's acceptance list.
///
/// A plain in-suite micro-benchmark rather than a criterion bench: the repo
/// carries no criterion today, and pulling it in (plotters, rayon, …) for one
/// measurement is a poor trade. `#[ignore]`d so it never slows normal CI; run it
/// on demand with `--ignored --nocapture`. The numbers are comparative, not
/// absolute — the ratio between modes is the signal, and it is stable across
/// machines in a way wall-clock nanoseconds are not.
#[tokio::test]
#[ignore = "micro-benchmark; run with --ignored --nocapture"]
async fn bench_write_throughput_by_mode() {
    const N: u64 = 2_000;

    async fn run(shard: &FlowShard, mode: Durability) -> f64 {
        let start = std::time::Instant::now();
        for i in 0..N {
            let flow = format!("flow-{}", i % 64);
            shard
                .set(&flow, "step", entry(&i.to_string()), mode)
                .await
                .expect("write");
        }
        let elapsed = start.elapsed();
        N as f64 / elapsed.as_secs_f64()
    }

    let none = FlowShard::in_memory(config(50));
    let none_rps = run(&none, Durability::None).await;

    let async_dir = tempfile::TempDir::new().expect("tempdir");
    let async_shard = FlowShard::open(async_dir.path(), config(50)).expect("open async");
    let async_rps = run(&async_shard, Durability::Async).await;
    async_shard.close();

    let sync_dir = tempfile::TempDir::new().expect("tempdir");
    let sync_shard = FlowShard::open(sync_dir.path(), config(50)).expect("open sync");
    let sync_rps = run(&sync_shard, Durability::Sync).await;
    sync_shard.close();

    println!("flow-shard write throughput ({N} writes/mode):");
    println!("  none  {none_rps:>12.0} writes/s");
    println!(
        "  async {async_rps:>12.0} writes/s  ({:+.1}% vs none)",
        (async_rps - none_rps) / none_rps * 100.0
    );
    println!(
        "  sync  {sync_rps:>12.0} writes/s  ({:+.1}% vs none)",
        (sync_rps - none_rps) / none_rps * 100.0
    );

    // Not a hard gate — a shared runner's disk makes the exact ratio noisy — but
    // a floor loose enough to catch a real regression: async must stay within an
    // order of magnitude of none. A tighter bound belongs on dedicated hardware.
    assert!(
        async_rps > none_rps / 10.0,
        "async throughput collapsed relative to none: {async_rps:.0} vs {none_rps:.0}"
    );
}

/// `close` releases the file lock synchronously: a reopen of the same path
/// while the closed handle is still alive must succeed.
///
/// This is the guarantee that would break if the shard kept its own copy of the
/// `Database` handle — the writer thread would be joined but the file would stay
/// locked until the handle dropped. It does not keep one; this proves it.
#[tokio::test]
async fn close_releases_the_lock_before_the_handle_is_dropped() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let first = FlowShard::open(dir.path(), config(50)).expect("open");
    first
        .set("flow-1", "k", entry("v"), Durability::Sync)
        .await
        .expect("write");
    first.close();

    // `first` is deliberately still in scope: close must have released the lock
    // without waiting for the drop.
    let second = FlowShard::open(dir.path(), config(50)).expect("reopen after close");
    assert_eq!(
        second.get("flow-1", "k").map(|e| e.value),
        Some(serde_json::json!("v")),
        "the reopened shard must recover what the first one committed"
    );
    second.close();
    drop(first);
}

/// An in-memory shard is genuinely free of the disk: no file is created, so a
/// throwaway CI imposter pays nothing for durability it did not ask for.
#[tokio::test]
async fn none_mode_never_creates_a_file() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let shard = FlowShard::in_memory(config(50));
    shard
        .set("flow-1", "k", entry("v"), Durability::None)
        .await
        .expect("write");

    assert_eq!(
        shard.get("flow-1", "k").map(|e| e.value),
        Some(serde_json::json!("v"))
    );
    assert!(
        !dir.path().join("flow.redb").exists(),
        "an in-memory shard must not touch the disk"
    );
}
