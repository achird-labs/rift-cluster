//! The clustered response sequencer's acceptance gate (#466, D-47): real
//! `RaftNode`s over real localhost TCP, driven through upstream's own
//! `ResponseSequencer` seam — the exact surface response building reaches it
//! through.
//!
//! What each test buys:
//! - `owner` mode cycles **once fleet-wide**, which is the whole feature;
//! - the default stays per-node, so an imposter that opted into nothing is
//!   byte-identical to a single-node rift (D-10);
//! - an owner that cannot answer degrades to a local cursor and *says so*,
//!   rather than failing the request — the one stateful op where availability
//!   wins;
//! - a membership change restarts the cursor, which is D-8's documented price
//!   for not replicating every advance.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rift_cluster::stores::{ClusteredSequencer, SequencingMode, SequencingRegistry, seq_routes};
use rift_cluster::{Authority, BridgeConfig, NodeConfig, NodeId, RaftNode};
use rift_cluster_base::seams::{ImposterConfig, ResponseSequencer, SequenceKey};
use tempfile::TempDir;

const SECRET: &str = "sequencer-test-secret";
const CONVERGE: Duration = Duration::from_secs(10);
const TEST_PORT: u16 = 4919;

/// One cluster at a time: scarce localhost ports, plus the process-global
/// Prometheus counters the fallback assertion reads.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn reserve_ports(n: usize) -> Vec<u16> {
    let held: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("reserve"))
        .collect();
    held.iter()
        .map(|l| l.local_addr().expect("addr").port())
        .collect()
}

struct Member {
    node: Arc<RaftNode>,
    seq: Arc<ClusteredSequencer>,
    registry: Arc<SequencingRegistry>,
    _dir: TempDir,
}

/// An imposter config carrying `_rift.sequencing.mode` — the block upstream
/// rift#978 declared, without which it would be dropped on parse.
fn imposter_with_mode(port: u16, mode: &str) -> ImposterConfig {
    serde_json::from_value(serde_json::json!({
        "port": port,
        "protocol": "http",
        "_rift": { "sequencing": { "mode": mode } },
    }))
    .expect("a config this crate's own seam declares must parse")
}

async fn spawn_member(id: NodeId, addr: SocketAddr, dir: &Path) -> Member {
    let registry = SequencingRegistry::new();
    let seq = ClusteredSequencer::new(Arc::clone(&registry));
    let node = RaftNode::start(NodeConfig {
        node_id: id,
        bind: addr,
        advertise: Some(Authority::from(addr)),
        data_dir: dir.to_path_buf(),
        secret: Some(SECRET.to_owned()),
        routes: seq_routes(Arc::clone(&seq)),
        engine: None,
        audit_retention_secs: rift_cluster::DEFAULT_AUDIT_RETENTION_SECS,
        snapshot_log_entries: None,
        advertise_as_digest_only_incapable: false,
    })
    .await
    .unwrap_or_else(|e| panic!("start node {id}: {e}"));
    let node = Arc::new(node);
    seq.bind(&node, BridgeConfig::default())
        .expect("sequencer bridge");
    Member {
        node,
        seq,
        registry,
        _dir: TempDir::new().expect("placeholder"),
    }
}

/// `n` voters, all sequencing `TEST_PORT` in `mode`.
async fn cluster_of(n: usize, mode: &str) -> (Vec<Member>, Vec<TempDir>) {
    let ports = reserve_ports(n);
    let mut dirs = Vec::new();
    let mut members = Vec::new();

    for (i, port) in ports.iter().enumerate() {
        let dir = TempDir::new().expect("tempdir");
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
        let member = spawn_member((i + 1) as NodeId, addr, dir.path()).await;
        if i == 0 {
            member.node.cluster_init().await.expect("bootstrap");
        } else {
            let seed = Authority::from(
                format!("127.0.0.1:{}", ports[0])
                    .parse::<SocketAddr>()
                    .expect("addr"),
            );
            member.node.join_via(&seed).await.expect("join");
        }
        member
            .registry
            .apply(&[imposter_with_mode(TEST_PORT, mode)]);
        members.push(member);
        dirs.push(dir);
    }

    let deadline = Instant::now() + CONVERGE;
    while Instant::now() < deadline {
        if members
            .iter()
            .all(|m| m.node.ring().members().len() == n && !m.node.ring().is_empty())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (members, dirs)
}

fn key<'a>(stub_key: &'a str) -> SequenceKey<'a> {
    SequenceKey {
        port: TEST_PORT,
        // Node-local by definition, and deliberately *different* per member below:
        // the cluster key must not depend on it (RFC-001 §8.3).
        slot: 7,
        stub_key,
        scope: "",
    }
}

/// One routed decision, off the test's runtime.
///
/// A routed decision parks the calling thread on the bridge while the owner
/// answers over TCP. `#[tokio::test]` gives a current-thread runtime, and the
/// node's own RPC server runs on it — so blocking that thread stops the server
/// that has to reply, every decision degrades to the local cursor, and the
/// feature looks absent rather than broken. The engine has the same shape and
/// the flow-store tests hop the same way.
async fn decide(
    seq: &Arc<ClusteredSequencer>,
    stub: &'static str,
    response_count: usize,
    repeats: Vec<u32>,
    advance: bool,
) -> usize {
    let seq = Arc::clone(seq);
    tokio::task::spawn_blocking(move || {
        if advance {
            seq.next(key(stub), response_count, &repeats)
        } else {
            seq.peek(key(stub), response_count, &repeats)
        }
    })
    .await
    .expect("blocking decision")
    .expect("a cursor decision degrades, it does not fail")
}

async fn next_on(
    seq: &Arc<ClusteredSequencer>,
    stub: &'static str,
    count: usize,
    repeats: &[u32],
) -> usize {
    decide(seq, stub, count, repeats.to_vec(), true).await
}

async fn peek_on(
    seq: &Arc<ClusteredSequencer>,
    stub: &'static str,
    count: usize,
    repeats: &[u32],
) -> usize {
    decide(seq, stub, count, repeats.to_vec(), false).await
}

fn counter(name: &str) -> u64 {
    prometheus::gather()
        .into_iter()
        .filter(|family| family.get_name() == name)
        .flat_map(|family| family.get_metric().to_owned())
        .map(|metric| metric.get_counter().get_value() as u64)
        .sum()
}

/// The feature itself: three nodes advancing the same stub in turn see one
/// cursor, not three. Before #466 each node cycled its own copy, so a
/// round-robin LB served `A, A, A` for `responses: [A, B, C]`.
#[tokio::test]
async fn owner_mode_cycles_once_fleet_wide() {
    let _lock = TEST_LOCK.lock().await;
    let (members, _dirs) = cluster_of(3, "owner").await;

    // Advance from a different member each time. Whichever node owns the key,
    // every one of these routes to it, so the indices form one sequence.
    let fallbacks_before = counter("rift_cluster_sequence_fallbacks_total");
    let mut seen = Vec::new();
    for round in 0..6 {
        seen.push(next_on(&members[round % members.len()].seq, "stub-a", 3, &[1, 1, 1]).await);
    }
    assert_eq!(
        seen,
        vec![0, 1, 2, 0, 1, 2],
        "three nodes sharing one owned cursor must produce one cycle, not three"
    );
    // Load-bearing: every cluster failure in this design degrades to the local
    // cursor, so without this a wiring bug is indistinguishable from a healthy
    // fleet — the sequence above would read 0,0,0,1,1,1 and only this catches it.
    assert_eq!(
        counter("rift_cluster_sequence_fallbacks_total"),
        fallbacks_before,
        "a healthy fleet must answer every decision; any fallback here means the \
         indices above were cycled locally rather than fleet-wide"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// The default is untouched: an imposter that never opts in keeps per-process
/// cursors, exactly as a single-node rift does (D-10). If this ever fails, the
/// clustered sequencer has silently changed behaviour for every imposter that
/// asked for nothing.
#[tokio::test]
async fn local_mode_keeps_per_node_cursors() {
    let _lock = TEST_LOCK.lock().await;
    let (members, _dirs) = cluster_of(3, "local").await;

    let mut first = Vec::new();
    for member in &members {
        first.push(next_on(&member.seq, "stub-b", 3, &[1, 1, 1]).await);
    }
    assert_eq!(
        first,
        vec![0, 0, 0],
        "each node must start its own cursor when the imposter did not opt in"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// D-10's rule, made concrete: when the fleet cannot answer, the request still
/// gets an index. A `503` here would block every cyclic response during a blip,
/// which is worse than a possible duplicate — but the degradation is *flagged*,
/// via the counter and the response annotation, so it is never silent.
#[tokio::test]
async fn an_unreachable_owner_falls_back_locally_rather_than_failing() {
    let _lock = TEST_LOCK.lock().await;
    let (members, _dirs) = cluster_of(3, "owner").await;

    // Find a node that is not the owner, so its decisions have to leave the box.
    let stub = "stub-c";
    let _ = next_on(&members[0].seq, stub, 3, &[1, 1, 1]).await;

    // Kill the other two: whoever owned the key is now unreachable from at
    // least one survivor, and the survivor is isolated besides.
    for member in members.iter().skip(1) {
        member.node.shutdown().await.expect("shutdown");
    }
    let deadline = Instant::now() + CONVERGE;
    while !members[0].node.is_isolated() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let before = counter("rift_cluster_sequence_fallbacks_total");
    let index = next_on(&members[0].seq, stub, 3, &[1, 1, 1]).await;
    assert!(
        index < 3,
        "the fallback must still honour the seam's `< response_count` contract"
    );
    assert!(
        counter("rift_cluster_sequence_fallbacks_total") > before,
        "a locally-served decision must be counted — it is the only signal that a \
         response was not fleet-ordered"
    );

    members[0].node.shutdown().await.expect("shutdown");
}

/// D-8's price, asserted rather than assumed: cursors are not replicated, so the
/// node that takes over a key starts at zero. Documented as a reset, not a bug —
/// but it has to actually be a reset, and only for the key that moved.
#[tokio::test]
async fn a_cursor_is_not_replicated_so_a_fresh_owner_starts_at_zero() {
    let _lock = TEST_LOCK.lock().await;
    let (members, _dirs) = cluster_of(3, "owner").await;

    let stub = "stub-d";
    for _ in 0..2 {
        next_on(&members[0].seq, stub, 3, &[1, 1, 1]).await;
    }

    // A member that never owned this key holds no copy of its cursor: ask it
    // directly, bypassing the routing, and it must be untouched.
    let mut untouched = Vec::new();
    for m in &members {
        m.registry.apply(&[imposter_with_mode(TEST_PORT, "local")]);
        untouched.push(peek_on(&m.seq, stub, 3, &[1, 1, 1]).await);
        m.registry.apply(&[imposter_with_mode(TEST_PORT, "owner")]);
    }
    assert_eq!(
        untouched.iter().filter(|index| **index == 0).count(),
        2,
        "exactly one node holds the cursor; the other two start from zero, which is \
         what a takeover inherits: {untouched:?}"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// Every member's *own* cursor for `stub`, read without routing.
///
/// Reading through `peek_on` on one member proves nothing about "every member": in owner mode
/// that call is routed to whichever member the HRW ring picked, so it reports one cursor three
/// times — and a routed read whose owner hop fails silently falls back to the caller's local
/// cursor (D-10), which can read `0` for a reset that never landed anywhere. Flipping each
/// member to `local` for the duration of the read is the recipe
/// `a_cursor_is_not_replicated_so_a_fresh_owner_starts_at_zero` already uses to ask a member
/// about its own copy.
async fn local_cursors(members: &[Member], stub: &'static str) -> Vec<usize> {
    let mut cursors = Vec::new();
    for m in members {
        m.registry.apply(&[imposter_with_mode(TEST_PORT, "local")]);
        cursors.push(peek_on(&m.seq, stub, 3, &[1, 1, 1]).await);
        m.registry.apply(&[imposter_with_mode(TEST_PORT, "owner")]);
    }
    cursors
}

/// `reset_scope` is the engine's GC hook — stub delete, bulk replace, imposter
/// teardown. It has to clear the *fleet's* cursor, not just this node's, or a
/// redeployed stub keeps cycling from wherever the owner left off.
///
/// Pins D-57 (#514). The assertion reads **every member's own cursor**, not a routed peek: the
/// cursor that matters lives on the ring owner, which is not necessarily `members[0]`, and a
/// routed read can answer `0` from a local fallback while the owner still holds `2`. That is how
/// this test used to fail intermittently under load — the fan-out was lost, nothing retried it,
/// and the poll exited on a fallback.
#[tokio::test]
async fn reset_scope_clears_the_cursor_on_every_member() {
    let _lock = TEST_LOCK.lock().await;
    let (members, _dirs) = cluster_of(3, "owner").await;

    let stub = "stub-e";
    for _ in 0..2 {
        next_on(&members[0].seq, stub, 3, &[1, 1, 1]).await;
    }
    assert!(
        local_cursors(&members, stub).await.iter().any(|c| *c != 0),
        "the cursor has advanced somewhere, so a reset has something to undo"
    );

    members[0].seq.reset_scope(TEST_PORT, Some(stub));

    // The fan-out is asynchronous and retried; the caller's own copy is cleared synchronously.
    let deadline = Instant::now() + CONVERGE;
    let mut cursors = local_cursors(&members, stub).await;
    while cursors.iter().any(|c| *c != 0) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cursors = local_cursors(&members, stub).await;
    }
    assert_eq!(
        cursors,
        vec![0, 0, 0],
        "a reset stub must cycle from the start again on every member, including the ring owner"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// Repeats are the part most easily lost across a wire: the owner must apply
/// them, not the caller, or `repeat: 2` degrades to `repeat: 1` the moment the
/// decision is routed.
#[tokio::test]
async fn repeats_are_honoured_through_the_owner_hop() {
    let _lock = TEST_LOCK.lock().await;
    let (members, _dirs) = cluster_of(3, "owner").await;

    let mut seen = Vec::new();
    for round in 0..4 {
        seen.push(next_on(&members[round % members.len()].seq, "stub-f", 2, &[2, 1]).await);
    }
    assert_eq!(
        seen,
        vec![0, 0, 1, 0],
        "the first response repeats twice before the cursor moves on, wherever the \
         decision was made"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// The registry is the sequencer's only route to per-imposter config, and it is
/// rebuilt from the applied set rather than merged into. Both halves matter: an
/// in-place mode change must be observed (the case that ruled out hooking
/// `FlowStoreProvider::provide`, which is not re-called for one), and a port
/// that leaves the set must stop being sequenced.
#[test]
fn the_registry_mirrors_the_applied_config_set() {
    let registry = SequencingRegistry::new();
    assert_eq!(
        registry.mode(TEST_PORT),
        SequencingMode::Local,
        "a port nobody has applied must not be cycled through a cursor nobody owns"
    );

    registry.apply(&[imposter_with_mode(TEST_PORT, "owner")]);
    assert_eq!(registry.mode(TEST_PORT), SequencingMode::Owner);

    registry.apply(&[imposter_with_mode(TEST_PORT, "local")]);
    assert_eq!(
        registry.mode(TEST_PORT),
        SequencingMode::Local,
        "an in-place mode change must be observed"
    );

    registry.apply(&[imposter_with_mode(TEST_PORT, "owner")]);
    registry.apply(&[]);
    assert_eq!(
        registry.mode(TEST_PORT),
        SequencingMode::Local,
        "a port that left the applied set must drop out, not linger at its old mode"
    );
}
