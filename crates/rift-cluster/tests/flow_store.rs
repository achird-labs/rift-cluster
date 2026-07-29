//! The clustered flow store's acceptance gate (#120): real `RaftNode`s over
//! real localhost TCP, the store driven through upstream's own `FlowStore`
//! trait — the exact seam a script's `ctx.state` call reaches it through.
//!
//! What each test buys, in the issue's words:
//! - write on node A, immediately read on node B with `strong` → observes it;
//! - `readConsistency: "local"` restores the replica read;
//! - a stale `m_idx` is rejected and counted, never applied;
//! - a `strong` read costs at most one RPC, asserted via
//!   `flow_reads_total{path}` deltas;
//! - before the cluster is bound the store fails loud — no silent builtin.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rift_cluster::stores::{
    ClusteredFlowStoreProvider, FlowNet, FlowShard, ShardConfig, flow_routes,
};
use rift_cluster::{Authority, NodeConfig, NodeId, RaftNode};
use rift_ee::seams::{CasOutcome, FlowStore, FlowStoreProvider, ImposterConfig};
use tempfile::TempDir;

const SECRET: &str = "flow-store-test-secret";
const CONVERGE: Duration = Duration::from_secs(10);

/// One cluster at a time: scarce localhost ports, plus process-global
/// Prometheus counters whose deltas the assertions below read.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn reserve_ports(n: usize) -> Vec<u16> {
    let held: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("reserve"))
        .collect();
    held.iter()
        .map(|l| l.local_addr().expect("addr").port())
        .collect()
}

struct FlowMember {
    node: Arc<RaftNode>,
    net: Arc<FlowNet>,
    /// A clone of the shard the net owns — same `Arc<Inner>`, so tests can
    /// inspect (and sabotage) a member's local state directly.
    shard: FlowShard,
    _dir: TempDir,
}

async fn spawn_member(
    id: NodeId,
    addr: SocketAddr,
    dir: &Path,
) -> (Arc<RaftNode>, Arc<FlowNet>, FlowShard) {
    let shard = FlowShard::open(dir, ShardConfig::default()).expect("open flow shard");
    let handle = shard.clone();
    let net = FlowNet::new(shard);
    let node = RaftNode::start(NodeConfig {
        node_id: id,
        bind: addr,
        advertise: Some(Authority::from(addr)),
        data_dir: dir.to_path_buf(),
        secret: Some(SECRET.to_owned()),
        routes: flow_routes(Arc::clone(&net)),
        engine: None,
        audit_retention_secs: rift_cluster::DEFAULT_AUDIT_RETENTION_SECS,
    })
    .await
    .unwrap_or_else(|e| panic!("start node {id}: {e}"));
    (Arc::new(node), net, handle)
}

/// A converged 2-voter cluster with the flow subsystem bound on both nodes,
/// anti-entropy at test cadence.
async fn flow_cluster() -> Vec<FlowMember> {
    flow_cluster_of(2, Duration::from_millis(200)).await
}

/// `n` voters, with the repair cadence chosen per test: short where a test
/// waits for the loop, effectively-off where the loop would mask what the test
/// isolates (adoption).
async fn flow_cluster_of(n: usize, anti_entropy: Duration) -> Vec<FlowMember> {
    let ports = reserve_ports(n);
    let mut members = Vec::new();

    for (i, port) in ports.iter().enumerate() {
        let dir = TempDir::new().expect("tempdir");
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
        let (node, net, shard) = spawn_member((i + 1) as NodeId, addr, dir.path()).await;
        if i == 0 {
            node.cluster_init().await.expect("bootstrap");
        } else {
            let seed = Authority::from(
                format!("127.0.0.1:{}", ports[0])
                    .parse::<SocketAddr>()
                    .expect("addr"),
            );
            node.join_via(&seed).await.expect("join");
        }
        members.push(FlowMember {
            node,
            net,
            shard,
            _dir: dir,
        });
    }

    // Wait for everyone to see the full voter set, then bind the flow nets.
    let deadline = Instant::now() + CONVERGE;
    loop {
        let converged = members.iter().all(|m| m.node.ring().members().len() == n);
        if converged {
            break;
        }
        assert!(Instant::now() < deadline, "cluster never converged");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for member in &members {
        member
            .net
            .bind(
                &member.node,
                rift_cluster::stores::FlowBindConfig {
                    bridge: rift_cluster::BridgeConfig::for_workers(2),
                    anti_entropy_interval: anti_entropy,
                },
            )
            .expect("bind flow net");
    }
    members
}

/// The port is a parameter because it *is* the imposter scope (#152): two
/// configs differing only in port are two imposters, and that is the boundary
/// the scope tests below assert.
fn imposter_on(port: u16, flow_state: serde_json::Value) -> ImposterConfig {
    serde_json::from_value(serde_json::json!({
        "port": port,
        "protocol": "http",
        "_rift": { "flowState": flow_state },
    }))
    .expect("imposter parses")
}

/// The port `store_on` and `stored` agree on. Shared so the two cannot drift —
/// a mismatch would show up as a confusing runtime absence, not a compile error.
const TEST_PORT: u16 = 4545;

fn imposter(flow_state: serde_json::Value) -> ImposterConfig {
    imposter_on(TEST_PORT, flow_state)
}

fn store_on_port(
    member: &FlowMember,
    port: u16,
    flow_state: serde_json::Value,
) -> Arc<dyn FlowStore> {
    ClusteredFlowStoreProvider::new(Arc::clone(&member.net))
        .provide(&imposter_on(port, flow_state))
        .expect("the clustered provider always provides")
}

fn store_on(member: &FlowMember, flow_state: serde_json::Value) -> Arc<dyn FlowStore> {
    store_on_port(member, TEST_PORT, flow_state)
}

/// The id a flow written through [`store_on`] is actually **stored** under.
///
/// The store scopes at its face (#152), so everything below it — shards, the
/// ownership ring, replication — sees the prefixed id. Tests that reach past
/// the store to a `FlowShard`, or that predict an owner from the ring, are
/// asking questions about that layer and must use its vocabulary. Rendered by
/// the production function rather than a hand-written literal, so a test can
/// never disagree with the store about what the prefix is.
///
/// This applies to **hand-built wire bodies** as much as to `FlowShard` calls:
/// a `/_cluster/flow/write` or `/_cluster/flow/replicate` body carries the id
/// verbatim, so a bare id there addresses a namespace no store ever writes to,
/// and any assertion made through the store face against it is vacuously true.
fn stored(flow_id: &str) -> String {
    format!(
        "{}{flow_id}",
        rift_cluster::stores::ContextScope::Imposter.prefix_for(Some(TEST_PORT))
    )
}

/// The store face is synchronous and parks its thread on the bridge; calling it
/// from a tokio worker would be exactly the head-of-line blocking `is_blocking`
/// exists to route around, so the tests hop through `spawn_blocking` like the
/// engine does.
async fn blocking<T: Send + 'static>(op: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(op).await.expect("blocking op")
}

/// A gauge's value summed across this process's registry — both nodes' shards
/// report into the same one, which is what makes "did *any* writer see this
/// write" answerable.
fn gauge(name: &str) -> u64 {
    prometheus::gather()
        .into_iter()
        .filter(|family| family.get_name() == name)
        .flat_map(|family| family.get_metric().to_owned())
        .map(|metric| metric.get_gauge().get_value() as u64)
        .sum()
}

fn counter(name: &str, label: (&str, &str)) -> u64 {
    prometheus::gather()
        .into_iter()
        .filter(|family| family.get_name() == name)
        .flat_map(|family| family.get_metric().to_owned())
        .find(|metric| {
            metric
                .get_label()
                .iter()
                .any(|l| l.get_name() == label.0 && l.get_value() == label.1)
        })
        .map_or(0, |metric| metric.get_counter().get_value() as u64)
}

/// The heart of Gap C: a write made through one node is observed by a `strong`
/// read through the *other* node, immediately — no replication lag in the
/// contract, because both operations are answered by the owner.
///
/// Also pins the cost claim: the two cross-checking strong reads below cost at
/// most one forwarded RPC each.
#[tokio::test(flavor = "multi_thread")]
async fn a_strong_read_anywhere_observes_a_write_made_anywhere() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let store_a = store_on(&members[0], serde_json::json!({}));
    let store_b = store_on(&members[1], serde_json::json!({}));

    let forwards_before = counter("rift_cluster_flow_reads_total", ("path", "forward"));
    let owner_before = counter("rift_cluster_flow_reads_total", ("path", "owner"));

    blocking(move || store_a.set("flow-x", "step", serde_json::json!("two")))
        .await
        .expect("write through node A");

    let seen = blocking(move || store_b.get("flow-x", "step"))
        .await
        .expect("strong read through node B");
    assert_eq!(
        seen,
        Some(serde_json::json!("two")),
        "a strong read through the other node must observe the write immediately"
    );

    // Exactly one read happened; it was answered by the owner either locally or
    // via exactly one forward. Both nodes' counters are in this process, so the
    // sum of the two paths is the total number of strong reads: 1.
    let forwards = counter("rift_cluster_flow_reads_total", ("path", "forward")) - forwards_before;
    let owner = counter("rift_cluster_flow_reads_total", ("path", "owner")) - owner_before;
    assert_eq!(
        forwards + owner,
        1,
        "one strong read must cost exactly one answered read (≤ 1 RPC)"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// `readConsistency: "local"` restores the replica read: served from the local
/// shard, no forward — and the replica *has* the data, because the owner pushes
/// every applied write to its successors.
#[tokio::test(flavor = "multi_thread")]
async fn a_local_read_stays_on_the_replica_and_sees_replicated_state() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let strong_a = store_on(&members[0], serde_json::json!({}));
    let local_b = store_on(
        &members[1],
        serde_json::json!({ "readConsistency": "local" }),
    );

    blocking(move || strong_a.set("flow-y", "cart", serde_json::json!(3)))
        .await
        .expect("write");

    // A 2-node ring replicates every flow to the other node, so B holds a copy
    // wherever the owner is. Replication is async — poll, bounded.
    let forwards_before = counter("rift_cluster_flow_reads_total", ("path", "forward"));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let local_b = Arc::clone(&local_b);
        let seen = blocking(move || local_b.get("flow-y", "cart"))
            .await
            .expect("local read");
        if seen == Some(serde_json::json!(3)) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replication push never reached the replica; local read still sees {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        counter("rift_cluster_flow_reads_total", ("path", "forward")),
        forwards_before,
        "a local read must never forward"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// The G5 parity proof (#152, RFC-005 §3.5): two imposters that happen to
/// resolve the same flow id — the ordinary outcome of both using
/// `flowIdSource: "header:X-Session"` — must not share state. OSS isolates them
/// because each imposter builds its own `InMemoryFlowStore`; the clustered
/// store has to reproduce that boundary itself, since one `FlowNet` backs every
/// imposter on the node.
///
/// Both read paths are asserted, because the prefix has to sit above the read
/// consistency choice, not inside one branch of it. The `local` half is written
/// so its absence assertion means something: it first waits until the *same*
/// imposter's local read observes the write, which proves replication landed —
/// only then is the other imposter's empty read attributable to scoping rather
/// than to a push still in flight.
#[tokio::test(flavor = "multi_thread")]
async fn imposter_scope_isolates_two_imposters_sharing_one_flow_id() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    const SHARED: &str = "x-session-abc";

    let writer = store_on_port(&members[0], 6400, serde_json::json!({}));
    let same_imposter_strong = store_on_port(&members[1], 6400, serde_json::json!({}));
    let other_imposter_strong = store_on_port(&members[1], 6401, serde_json::json!({}));
    let same_imposter_local = store_on_port(
        &members[1],
        6400,
        serde_json::json!({ "readConsistency": "local" }),
    );
    let other_imposter_local = store_on_port(
        &members[1],
        6401,
        serde_json::json!({ "readConsistency": "local" }),
    );

    blocking(move || writer.set(SHARED, "step", serde_json::json!("checkout")))
        .await
        .expect("write through imposter 6400 on node A");

    // Control: the same imposter, through the other node, does see it. Without
    // this the isolation assertion below could pass on a store that lost the
    // write entirely.
    let seen = blocking(move || same_imposter_strong.get(SHARED, "step"))
        .await
        .expect("strong read");
    assert_eq!(
        seen,
        Some(serde_json::json!("checkout")),
        "the writing imposter must still observe its own flow across nodes"
    );

    let seen = blocking(move || other_imposter_strong.get(SHARED, "step"))
        .await
        .expect("strong read");
    assert_eq!(
        seen, None,
        "a different imposter sharing the flow-id value must not observe the write (strong path)"
    );

    // Replication is async; poll the same imposter's local read until the push
    // lands, so the cross-imposter local read that follows is a scoping result.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let probe = Arc::clone(&same_imposter_local);
        let seen = blocking(move || probe.get(SHARED, "step"))
            .await
            .expect("local read");
        if seen == Some(serde_json::json!("checkout")) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replication push never reached the replica; local read still sees {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let seen = blocking(move || other_imposter_local.get(SHARED, "step"))
        .await
        .expect("local read");
    assert_eq!(
        seen, None,
        "a different imposter must not observe the write on the local path either"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// `contextScope: "fleet"` is the opt-back-in to pre-#152 sharing: two
/// imposters deliberately share one context. Asserted alongside the disjointness
/// of the two namespaces — a `fleet` store must not pick up an `imposter`-scoped
/// write either, which is why `fleet` carries its own `f:` prefix instead of
/// passing the flow id through bare.
#[tokio::test(flavor = "multi_thread")]
async fn fleet_scope_shares_across_imposters_and_stays_disjoint_from_imposter_scope() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    const SHARED: &str = "x-session-def";
    let fleet = || serde_json::json!({ "contextScope": "fleet" });

    let fleet_writer = store_on_port(&members[0], 6500, fleet());
    let fleet_reader = store_on_port(&members[1], 6501, fleet());

    blocking(move || fleet_writer.set(SHARED, "step", serde_json::json!("shared")))
        .await
        .expect("fleet-scoped write");

    let seen = blocking(move || fleet_reader.get(SHARED, "step"))
        .await
        .expect("strong read");
    assert_eq!(
        seen,
        Some(serde_json::json!("shared")),
        "two fleet-scoped imposters must share one context"
    );

    // Disjointness, both directions.
    let imposter_reader = store_on_port(&members[1], 6501, serde_json::json!({}));
    let seen = blocking(move || imposter_reader.get(SHARED, "step"))
        .await
        .expect("strong read");
    assert_eq!(
        seen, None,
        "an imposter-scoped read must not observe a fleet-scoped write"
    );

    let imposter_writer = store_on_port(&members[0], 6502, serde_json::json!({}));
    blocking(move || imposter_writer.set(SHARED, "own", serde_json::json!("mine")))
        .await
        .expect("imposter-scoped write");
    let fleet_probe = store_on_port(&members[1], 6502, fleet());
    let seen = blocking(move || fleet_probe.get(SHARED, "own"))
        .await
        .expect("strong read");
    assert_eq!(
        seen, None,
        "a fleet-scoped read must not observe an imposter-scoped write"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// The boundary holds for every write op, not just `set` — and for flow ids
/// deliberately shaped like another imposter's prefix.
///
/// Today all eleven `FlowStore` methods funnel through `write`/`get`, so the
/// scoping is structural rather than per-method. This test is what keeps that
/// true: a future bespoke fast path for `clear_flow` or `increment_by` that
/// forgot to scope would fail here instead of silently reopening #152.
#[tokio::test(flavor = "multi_thread")]
async fn every_write_op_is_scoped_including_prefix_shaped_flow_ids() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let a = store_on_port(&members[0], 6400, serde_json::json!({}));
    let b = store_on_port(&members[1], 6401, serde_json::json!({}));

    // `increment_by`: two imposters counting under one id keep separate counts.
    let first = {
        let a = Arc::clone(&a);
        blocking(move || a.increment_by("ctr", "n", 5))
            .await
            .expect("increment through imposter 6400")
    };
    assert_eq!(first, 5);
    let second = {
        let b = Arc::clone(&b);
        blocking(move || b.increment_by("ctr", "n", 1))
            .await
            .expect("increment through imposter 6401")
    };
    assert_eq!(
        second, 1,
        "a second imposter's counter must start at zero, not inherit the first's"
    );

    // `clear_flow`: wiping one imposter's flow must not reach the other's.
    for (store, mark) in [(&a, "a"), (&b, "b")] {
        let store = Arc::clone(store);
        let mark = serde_json::json!(mark);
        blocking(move || store.set("wipe", "k", mark))
            .await
            .expect("seed");
    }
    {
        let a = Arc::clone(&a);
        blocking(move || a.clear_flow("wipe"))
            .await
            .expect("clear through imposter 6400");
    }
    let cleared = {
        let a = Arc::clone(&a);
        blocking(move || a.get("wipe", "k")).await.expect("read")
    };
    assert_eq!(cleared, None, "the clear must take effect in its own scope");
    let survivor = {
        let b = Arc::clone(&b);
        blocking(move || b.get("wipe", "k")).await.expect("read")
    };
    assert_eq!(
        survivor,
        Some(serde_json::json!("b")),
        "clear_flow must not reach across the scope boundary"
    );

    // A flow id shaped like imposter 6401's prefix cannot address imposter
    // 6401: `6401:evil` on port 6400 keys `i6400:6401:evil`, which is not
    // `i6401:evil`. The decimal port always terminates in a `:`, so the scheme
    // stays uniquely decodable.
    {
        let a = Arc::clone(&a);
        blocking(move || a.set("6401:evil", "k", serde_json::json!("spoofed")))
            .await
            .expect("write a prefix-shaped id");
    }
    let spoofed = {
        let b = Arc::clone(&b);
        blocking(move || b.get("evil", "k")).await.expect("read")
    };
    assert_eq!(
        spoofed, None,
        "a caller-chosen flow id must not be able to impersonate another imposter's prefix"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// RFC-001 §7.6: an op minted under a stale membership view is rejected and
/// counted, never applied under an ownership the sender no longer holds. Driven
/// over the real wire with a hand-built body, which pins the wire contract too.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_fencing_token_is_rejected_and_counted() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let fences_before = counter("rift_cluster_cas_conflicts_total", ("reason", "fence"));

    let target = members[1].node.id();
    let stale = serde_json::json!({
        "flow_id": stored("flow-z"),
        "key": "step",
        "op": { "Set": { "value": "stolen" } },
        "ttl_seconds": null,
        "durability": "async",
        "m_idx": 999_999,
    });
    let reply = members[0]
        .node
        .call_member(
            target,
            "POST",
            "/_cluster/flow/write",
            serde_json::to_vec(&stale).expect("encode"),
        )
        .await
        .expect("the transport call itself succeeds");
    let reply: serde_json::Value = serde_json::from_slice(&reply).expect("json reply");

    assert!(
        reply.get("Fenced").is_some(),
        "a stale m_idx must be fenced, got {reply}"
    );
    assert_eq!(
        counter("rift_cluster_cas_conflicts_total", ("reason", "fence")) - fences_before,
        1,
        "the fencing rejection must be counted"
    );

    // And the write must not have landed: a strong read finds nothing.
    let store = store_on(&members[0], serde_json::json!({}));
    let seen = blocking(move || store.get("flow-z", "step"))
        .await
        .expect("read");
    assert_eq!(seen, None, "a fenced write must not be applied");

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// A write carrying a *valid* fencing token but sent to the wrong node is
/// refused, not applied: only a buggy member misroutes (correct members compute
/// the same HRW owner from the same membership), and applying it would pollute
/// a replica's shard with owner-versioned entries. Exactly one of the two nodes
/// owns the flow; the other must answer `NotOwner`.
#[tokio::test(flavor = "multi_thread")]
async fn a_misrouted_write_with_a_valid_token_is_refused() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let m_idx = members[0].node.ring().m_idx();
    let body = |val: &str| {
        serde_json::to_vec(&serde_json::json!({
            "flow_id": stored("flow-misroute"),
            "key": "k",
            "op": { "Set": { "value": val } },
            "ttl_seconds": null,
            "durability": "async",
            "m_idx": m_idx,
        }))
        .expect("encode")
    };

    let mut applied = 0;
    let mut refused = 0;
    for target in [members[0].node.id(), members[1].node.id()] {
        let reply = members[0]
            .node
            .call_member(target, "POST", "/_cluster/flow/write", body("probe"))
            .await
            .expect("transport");
        let reply: serde_json::Value = serde_json::from_slice(&reply).expect("json");
        if reply.get("Applied").is_some() {
            applied += 1;
        } else if reply.get("NotOwner").is_some() {
            refused += 1;
        } else {
            panic!("unexpected reply: {reply}");
        }
    }
    assert_eq!(
        (applied, refused),
        (1, 1),
        "exactly one node owns the flow; the other must refuse the misroute"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// CAS semantics across the cluster: conflicts return the winning value and are
/// counted; the increments they guard stay exact under owner serialization.
#[tokio::test(flavor = "multi_thread")]
async fn cas_reports_the_winning_value_and_increment_is_exact() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let store_a = store_on(&members[0], serde_json::json!({}));
    let store_b = store_on(&members[1], serde_json::json!({}));

    let cas_before = counter("rift_cluster_cas_conflicts_total", ("reason", "cas"));

    {
        let store_a = Arc::clone(&store_a);
        blocking(move || store_a.set("flow-c", "winner", serde_json::json!("a")))
            .await
            .expect("seed");
    }
    let outcome = {
        let store_b = Arc::clone(&store_b);
        blocking(move || store_b.compare_and_set("flow-c", "winner", None, serde_json::json!("b")))
            .await
            .expect("cas")
    };
    assert_eq!(
        outcome,
        CasOutcome::Conflict(Some(serde_json::json!("a"))),
        "a lost CAS must report the value that won"
    );
    assert_eq!(
        counter("rift_cluster_cas_conflicts_total", ("reason", "cas")) - cas_before,
        1
    );

    // Interleaved increments through both nodes land exactly once each.
    for _ in 0..5 {
        let store_a = Arc::clone(&store_a);
        blocking(move || store_a.increment("flow-c", "count"))
            .await
            .expect("incr via A");
        let store_b = Arc::clone(&store_b);
        blocking(move || store_b.increment("flow-c", "count"))
            .await
            .expect("incr via B");
    }
    let total = blocking(move || store_a.get("flow-c", "count"))
        .await
        .expect("read");
    assert_eq!(
        total,
        Some(serde_json::json!(10)),
        "10 owner-serialized increments must land exactly once each"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// #126, tombstones: a delayed replication `Put` arriving *after* a delete must
/// not resurrect the key on a replica. The delete is pushed as a versioned
/// tombstone, so the stale `Put` loses the ordinary version comparison — the
/// resurrect is structurally impossible, not guarded.
///
/// Driven over the real wire with hand-built pushes, which also pins the wire
/// shape a mixed-version peer would send.
#[tokio::test(flavor = "multi_thread")]
async fn a_delayed_put_cannot_resurrect_a_deleted_key() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    // Find the owner of this flow and the other node (the replica).
    let ring = members[0].node.ring();
    let owner_id = ring
        .owner(rift_cluster::OwnedKey::new(
            rift_cluster::KeyClass::FlowKv,
            &stored("flow-rz"),
        ))
        .expect("two members");
    let (owner, replica) = if members[0].node.id() == owner_id {
        (&members[0], &members[1])
    } else {
        (&members[1], &members[0])
    };

    // Seed (v1 lands on the owner, push reaches the replica), then delete
    // (tombstone v2, also pushed).
    let store = store_on(owner, serde_json::json!({}));
    {
        let store = Arc::clone(&store);
        blocking(move || store.set("flow-rz", "k", serde_json::json!("alive")))
            .await
            .expect("seed");
    }
    {
        let store = Arc::clone(&store);
        blocking(move || store.delete("flow-rz", "k"))
            .await
            .expect("delete");
    }

    // The delayed v1 push, replayed at the replica after the delete: exactly
    // what a slow network delivers. It must lose to the tombstone.
    let stale_put = serde_json::json!({
        "flow_id": stored("flow-rz"),
        "op": { "Put": { "key": "k", "entry": {
            "m_idx": ring.m_idx(),
            "v": 1,
            "origin": owner.node.id(),
            "expires_at": 0,
            "value": "alive",
        }}},
    });
    members[0]
        .node
        .call_member(
            replica.node.id(),
            "POST",
            "/_cluster/flow/replicate",
            serde_json::to_vec(&stale_put).expect("encode"),
        )
        .await
        .expect("replicate call");

    assert_eq!(
        replica.shard.get(&stored("flow-rz"), "k"),
        None,
        "a delayed Put must lose to the tombstone, not resurrect the key"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// #126: a key that is deleted and then *re-set* must converge on the replicas.
/// The re-set has to mint its version above the (hidden) tombstone — a mint
/// that restarts at v1 loses the replica merge to the v-higher tombstone, and
/// the acknowledged new value silently never replicates: divergent replicas
/// now, a lost write on the next takeover. Common shape: flow state that
/// clears a slot and reuses it.
/// #131 follow-up: the single-round version of this below is ~17% flaky, and the
/// failure it shows is not a slow push — it is a *lost update*.
///
/// A delete and the re-set that follows it are pushed to the replica as two
/// independent fire-and-forget RPCs, so the replica handles them concurrently.
/// Both read the same `current` before either writes, both conclude they
/// supersede it, and whichever writes last wins. When that is the tombstone, an
/// acknowledged write is silently reverted on the replica — and a later takeover
/// promotes exactly that copy.
///
/// Hammering the sequence turns a 1-in-6 coin flip into a near-certain failure,
/// which is what makes this a gate rather than a rumour.
#[tokio::test(flavor = "multi_thread")]
async fn a_reset_after_delete_never_loses_to_the_tombstone_under_repetition() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster_of(2, Duration::from_secs(60)).await;

    let ring = members[0].node.ring();
    let owner_id = ring
        .owner(rift_cluster::OwnedKey::new(
            rift_cluster::KeyClass::FlowKv,
            &stored("flow-hammer"),
        ))
        .expect("two members");
    let (owner, replica) = if members[0].node.id() == owner_id {
        (&members[0], &members[1])
    } else {
        (&members[1], &members[0])
    };
    let store = store_on(owner, serde_json::json!({}));

    for round in 0..25u32 {
        let want = serde_json::json!(format!("round-{round}"));
        for step in 0..3u8 {
            let store = Arc::clone(&store);
            let want = want.clone();
            match step {
                0 => blocking(move || store.set("flow-hammer", "slot", serde_json::json!("seed")))
                    .await
                    .expect("seed"),
                1 => blocking(move || store.delete("flow-hammer", "slot"))
                    .await
                    .expect("delete"),
                _ => blocking(move || store.set("flow-hammer", "slot", want))
                    .await
                    .expect("re-set"),
            }
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if replica
                .shard
                .get(&stored("flow-hammer"), "slot")
                .map(|e| e.value)
                == Some(want.clone())
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "round {round}: the replica lost the re-set. Its copy is {:?} — an older push \
                 (a tombstone, or an earlier value) overwrote a newer acknowledged write, which \
                 means the merge compare-and-install is not atomic against a concurrent push.",
                replica.shard.get_versioned(&stored("flow-hammer"), "slot")
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reset_after_delete_replicates_over_the_tombstone() {
    let _lock = TEST_LOCK.lock().await;
    // Anti-entropy effectively off: replication push alone must get this right.
    let members = flow_cluster_of(2, Duration::from_secs(60)).await;

    let ring = members[0].node.ring();
    let owner_id = ring
        .owner(rift_cluster::OwnedKey::new(
            rift_cluster::KeyClass::FlowKv,
            &stored("flow-reset"),
        ))
        .expect("two members");
    let (owner, replica) = if members[0].node.id() == owner_id {
        (&members[0], &members[1])
    } else {
        (&members[1], &members[0])
    };

    let store = store_on(owner, serde_json::json!({}));
    for step in [
        ("set", Some(serde_json::json!("A"))),
        ("delete", None),
        ("set", Some(serde_json::json!("B"))),
    ] {
        let store = Arc::clone(&store);
        match step {
            ("set", Some(value)) => blocking(move || store.set("flow-reset", "slot", value))
                .await
                .expect("set"),
            _ => blocking(move || store.delete("flow-reset", "slot"))
                .await
                .expect("delete"),
        }
    }

    // The replica must converge to B via the ordinary push — the re-set must
    // beat the tombstone it cannot see.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if replica
            .shard
            .get(&stored("flow-reset"), "slot")
            .map(|e| e.value)
            == Some(serde_json::json!("B"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the re-set never replicated: the replica still holds the tombstone \
             (its copy: {:?})",
            replica.shard.get_versioned(&stored("flow-reset"), "slot")
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// #126, anti-entropy: a replica that missed a push converges within one
/// repair tick. Sabotage stands in for the missed push — one key is removed
/// from the replica's shard directly — and the loop must pull it back from the
/// owner without any new write happening.
#[tokio::test(flavor = "multi_thread")]
async fn a_replica_that_missed_a_push_converges_within_one_tick() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await; // 200 ms anti-entropy

    let ring = members[0].node.ring();
    let owner_id = ring
        .owner(rift_cluster::OwnedKey::new(
            rift_cluster::KeyClass::FlowKv,
            &stored("flow-ae"),
        ))
        .expect("two members");
    let (owner, replica) = if members[0].node.id() == owner_id {
        (&members[0], &members[1])
    } else {
        (&members[1], &members[0])
    };

    // Two keys: the flow must survive the sabotage in the replica's listing,
    // or the loop would have nothing to pull (a replica cannot pull a flow it
    // never heard of — the documented residual).
    let store = store_on(owner, serde_json::json!({}));
    for key in ["kept", "lost"] {
        let store = Arc::clone(&store);
        blocking(move || store.set("flow-ae", key, serde_json::json!("v")))
            .await
            .expect("write");
    }

    // Wait until the push has landed, then knock one key out of the replica —
    // the moral equivalent of a push that never arrived.
    let deadline = Instant::now() + Duration::from_secs(5);
    while replica.shard.get(&stored("flow-ae"), "lost").is_none() {
        assert!(Instant::now() < deadline, "push never reached the replica");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    replica
        .shard
        .delete(
            &stored("flow-ae"),
            "lost",
            rift_cluster::stores::Durability::None,
        )
        .await
        .expect("sabotage");
    assert_eq!(
        replica.shard.get(&stored("flow-ae"), "lost"),
        None,
        "sabotage held"
    );

    // No new writes: only the loop can repair this.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if replica
            .shard
            .get(&stored("flow-ae"), "lost")
            .map(|e| e.value)
            == Some(serde_json::json!("v"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "anti-entropy never repaired the missing key"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// #126, adoption: after a membership change moves a flow's ownership, the new
/// owner verifies its copy against the surviving holders before serving it. The
/// new owner's replica is sabotaged with a stale value; without adoption it
/// would serve that value — with it, the pull from the intact survivor wins the
/// version merge and the read returns the truth.
///
/// Anti-entropy is effectively off (60 s) so the repair being observed is
/// adoption's, not the loop's.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_owner_adopts_from_the_surviving_replica_on_takeover() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster_of(3, Duration::from_secs(60)).await;

    // A flow owned by a non-leader (node 1 bootstrapped and leads), so the
    // membership change below does not also move leadership.
    let full_ring = members[0].node.ring();
    let leader_id = members[0].node.id();
    // Ownership is computed over the *stored* id, because that is the string
    // the ring sees once the store has scoped it (#152) — predicting from the
    // bare id would name a node that never owns this flow.
    let (flow_id, old_owner_id) = (0..64)
        .map(|i| format!("flow-adopt-{i}"))
        .find_map(|candidate| {
            let owner = full_ring.owner(rift_cluster::OwnedKey::new(
                rift_cluster::KeyClass::FlowKv,
                &stored(&candidate),
            ))?;
            (owner != leader_id).then_some((candidate, owner))
        })
        .expect("some flow is owned by a non-leader");
    let stored_id = stored(&flow_id);

    // Predict the post-removal owner: HRW depends only on the member set and
    // the key, so the test computes it the same way every node will.
    let survivor_ids: Vec<rift_cluster::NodeId> = members
        .iter()
        .map(|m| m.node.id())
        .filter(|&id| id != old_owner_id)
        .collect();
    let next_owner_id = rift_cluster::Ring::new(survivor_ids.iter().copied(), 0)
        .owner(rift_cluster::OwnedKey::new(
            rift_cluster::KeyClass::FlowKv,
            &stored_id,
        ))
        .expect("two survivors");
    let next_owner = members
        .iter()
        .find(|m| m.node.id() == next_owner_id)
        .expect("member");
    let reader = members
        .iter()
        .find(|m| m.node.id() != old_owner_id && m.node.id() != next_owner_id)
        .expect("the third survivor");

    // Write through the old owner; the push reaches every node (REPLICAS = 3).
    let store = store_on(&members[0], serde_json::json!({}));
    {
        let store = Arc::clone(&store);
        let flow = flow_id.clone();
        blocking(move || store.set(&flow, "k", serde_json::json!("truth")))
            .await
            .expect("write");
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while next_owner.shard.get(&stored_id, "k").is_none() {
        assert!(
            Instant::now() < deadline,
            "push never reached the successor"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Sabotage the successor's copy with a STALE (lower-versioned) value — the
    // stand-in for a push it missed.
    let stale = rift_cluster::stores::Versioned {
        m_idx: 0,
        v: 0,
        origin: 0,
        expires_at: 0,
        value: serde_json::json!("stale"),
        deleted: false,
    };
    next_owner
        .shard
        .set(
            &stored_id,
            "k",
            stale,
            rift_cluster::stores::Durability::None,
        )
        .await
        .expect("sabotage");

    // Remove the old owner from the membership; ownership moves to the
    // predicted successor.
    let voters: std::collections::BTreeSet<rift_cluster::NodeId> =
        survivor_ids.iter().copied().collect();
    members[0]
        .node
        .change_membership(voters)
        .await
        .expect("membership change");
    let old_m_idx = full_ring.m_idx();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let ring = reader.node.ring();
        if ring.members().len() == 2 && ring.m_idx() > old_m_idx {
            break;
        }
        assert!(Instant::now() < deadline, "membership change never applied");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A strong read via the third survivor forwards to the new owner, whose
    // first serve must adopt from the intact replica — and answer the truth,
    // not its sabotaged copy.
    let reader_store = store_on(reader, serde_json::json!({}));
    let flow = flow_id.clone();
    let seen = blocking(move || reader_store.get(&flow, "k"))
        .await
        .expect("strong read after takeover");
    assert_eq!(
        seen,
        Some(serde_json::json!("truth")),
        "the new owner must verify against the surviving replica before serving"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// #121: `durability: "none"` means none **fleet-wide**, not just at the owner.
///
/// The replication push carries the write's own durability, so a replica holds
/// a `none` flow in memory exactly like the owner does. Until #121 the push was
/// hardcoded `Async`, so both replicas persisted state the imposter had asked
/// to keep off disk — and after a full restart the new owner adopted it back,
/// which made `none` and `sync` indistinguishable. The chaos-tier C15 mutation
/// is what caught it; this pins it where it is cheap to run.
///
/// The observable is the shard's own view: `flow()` reads the in-memory mirror,
/// so it cannot tell disk from memory — the WAL-lag gauge can. A `none` write
/// never reaches the shard writer at all, so the replica's lag must not move.
#[tokio::test(flavor = "multi_thread")]
async fn a_none_durability_write_is_not_persisted_by_the_replica_either() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster_of(2, Duration::from_secs(60)).await;

    let ring = members[0].node.ring();
    let owner_id = ring
        .owner(rift_cluster::OwnedKey::new(
            rift_cluster::KeyClass::FlowKv,
            &stored("flow-none"),
        ))
        .expect("two members");
    let (owner, replica) = if members[0].node.id() == owner_id {
        (&members[0], &members[1])
    } else {
        (&members[1], &members[0])
    };

    let lag_before = gauge("rift_cluster_flow_wal_lag_ops");

    let store = store_on(owner, serde_json::json!({ "durability": "none" }));
    {
        let store = Arc::clone(&store);
        blocking(move || store.set("flow-none", "k", serde_json::json!("ephemeral")))
            .await
            .expect("write");
    }

    // The push still happens — a `local` reader on the replica must see it —
    // so wait for the value to arrive before judging what it cost.
    let deadline = Instant::now() + Duration::from_secs(5);
    while replica.shard.get(&stored("flow-none"), "k").is_none() {
        assert!(
            Instant::now() < deadline,
            "the push never reached the replica"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(
        gauge("rift_cluster_flow_wal_lag_ops"),
        lag_before,
        "a `none` write reached a shard writer: the replica is persisting state \
         the imposter asked to keep in memory"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// Before the node exists the store fails loud — it must never quietly hand an
/// imposter process-local semantics (the "silent builtin fallback" the design
/// forbids).
#[tokio::test(flavor = "multi_thread")]
async fn an_unbound_store_fails_loud_never_silently_local() {
    let shard = FlowShard::in_memory(ShardConfig::default());
    let net = FlowNet::new(shard);
    let store = ClusteredFlowStoreProvider::new(net)
        .provide(&imposter(serde_json::json!({})))
        .expect("provider always provides");

    let err = blocking(move || store.set("flow-early", "k", serde_json::json!(1)))
        .await
        .expect_err("a write before bind must refuse");
    assert!(
        err.to_string().contains("starting"),
        "the refusal must say the cluster is starting, got: {err}"
    );
}
