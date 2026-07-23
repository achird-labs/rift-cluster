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
    _dir: TempDir,
}

async fn spawn_member(id: NodeId, addr: SocketAddr, dir: &Path) -> (Arc<RaftNode>, Arc<FlowNet>) {
    let shard = FlowShard::open(dir, ShardConfig::default()).expect("open flow shard");
    let net = FlowNet::new(shard);
    let node = RaftNode::start(NodeConfig {
        node_id: id,
        bind: addr,
        advertise: Some(Authority::from(addr)),
        data_dir: dir.to_path_buf(),
        secret: Some(SECRET.to_owned()),
        routes: flow_routes(Arc::clone(&net)),
        engine: None,
    })
    .await
    .unwrap_or_else(|e| panic!("start node {id}: {e}"));
    (Arc::new(node), net)
}

/// A converged 2-voter cluster with the flow subsystem bound on both nodes.
async fn flow_cluster() -> Vec<FlowMember> {
    let ports = reserve_ports(2);
    let mut members = Vec::new();

    for (i, port) in ports.iter().enumerate() {
        let dir = TempDir::new().expect("tempdir");
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
        let (node, net) = spawn_member((i + 1) as NodeId, addr, dir.path()).await;
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
            _dir: dir,
        });
    }

    // Wait for both to see a 2-voter membership, then bind the flow nets.
    let deadline = Instant::now() + CONVERGE;
    loop {
        let converged = members.iter().all(|m| m.node.ring().members().len() == 2);
        if converged {
            break;
        }
        assert!(Instant::now() < deadline, "cluster never converged");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for member in &members {
        member
            .net
            .bind(&member.node, rift_cluster::BridgeConfig::for_workers(2))
            .expect("bind flow net");
    }
    members
}

fn imposter(flow_state: serde_json::Value) -> ImposterConfig {
    serde_json::from_value(serde_json::json!({
        "port": 4545,
        "protocol": "http",
        "_rift": { "flowState": flow_state },
    }))
    .expect("imposter parses")
}

fn store_on(member: &FlowMember, flow_state: serde_json::Value) -> Arc<dyn FlowStore> {
    ClusteredFlowStoreProvider::new(Arc::clone(&member.net))
        .provide(&imposter(flow_state))
        .expect("the clustered provider always provides")
}

/// The store face is synchronous and parks its thread on the bridge; calling it
/// from a tokio worker would be exactly the head-of-line blocking `is_blocking`
/// exists to route around, so the tests hop through `spawn_blocking` like the
/// engine does.
async fn blocking<T: Send + 'static>(op: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(op).await.expect("blocking op")
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
        "flow_id": "flow-z",
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
            "flow_id": "flow-misroute",
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
