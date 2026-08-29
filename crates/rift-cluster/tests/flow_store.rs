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
use rift_cluster::{Authority, KeyClass, NodeConfig, NodeId, OwnedKey, RaftNode};
use rift_cluster_base::seams::{
    BackendUnavailable, CasOutcome, FlowStore, FlowStoreProvider, ImposterConfig,
    backend_error_response,
};
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
        snapshot_log_entries: None,
        advertise_as_digest_only_incapable: false,
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
    stored_on(TEST_PORT, flow_id)
}

/// [`stored`], parameterized over the port — needed by the handful of tests (the fleet-wide
/// spaces listing among them) that write through a port other than [`TEST_PORT`] and still need
/// the identical scoping arithmetic the store itself uses.
fn stored_on(port: u16, flow_id: &str) -> String {
    format!(
        "{}{flow_id}",
        rift_cluster::stores::ContextScope::Imposter.prefix_for(Some(port), None)
    )
}

/// The store face is synchronous and parks its thread on the bridge; calling it
/// from a tokio worker would be exactly the head-of-line blocking `is_blocking`
/// exists to route around, so the tests hop through `spawn_blocking` like the
/// engine does.
async fn blocking<T: Send + 'static>(op: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(op).await.expect("blocking op")
}

/// Pins D-65 at the store face: a failure caused by the cluster's state carries
/// `BackendUnavailable` for the `flowState` feature, and the data plane answers it
/// with 503. The status is taken from `backend_error_response` itself — the
/// production mapping — so this asserts what a client sees, not a reading of it.
fn assert_backend_unavailable(err: &anyhow::Error, what: &str) {
    let typed = err
        .downcast_ref::<BackendUnavailable>()
        .unwrap_or_else(|| panic!("{what} must carry BackendUnavailable (D-65): {err:#}"));
    assert_eq!(typed.feature, "flowState", "{what}: {err:#}");
    assert_eq!(
        backend_error_response(err).status().as_u16(),
        503,
        "{what} must answer 503 on the data plane, not a generic 500: {err:#}"
    );
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
///
/// Pins D-10: the default read is owner-authoritative (`strong`) — no imposter
/// config, and the read through the non-owner still sees the write.
/// Pins D-13: correctness does not depend on the receiving node being the
/// owner — the budget is one forwarded LAN RPC, never an assumed co-location.
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
///
/// Pins D-10: replica reads are a per-imposter opt-in (`readConsistency`), not
/// a fleet-wide degraded mode.
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
///
/// Pins D-20: the ring key is scope-prefixed (`i{port}:` by default), so two
/// imposters' same-named flows are two flows — each with its own owner.
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
///
/// Pins D-20: under `fleet` two imposters' same-named flows are one flow with
/// one owner (`f:` prefix), and that namespace is disjoint from `i{port}:`.
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

/// RFC-005 S1 (#288): `contextScope: "tenant"` shares state **within** a tenant's imposters and
/// never **across** tenants — two tenants, the same caller-chosen flow id, disjoint keys — and it
/// stays disjoint from the imposter and fleet namespaces even for ids shaped like `t<tenant>:x`.
///
/// The tenant is not in the config (open-core rule): the provider learns it from the control
/// plane, keyed by port — ports are fleet-unique across tenants — so the imposters are committed
/// through the node first, exactly as `PutImposter` would leave them for the engine to provide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_scope_shares_within_a_tenant_and_never_across() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;
    let tenant_scoped = || serde_json::json!({ "contextScope": "tenant" });
    let commit = |port: u16, tenant: &str| {
        let leader = &members[0].node;
        let config = imposter_on(port, tenant_scoped());
        let tenant = tenant.to_owned();
        async move {
            let response = leader
                .submit(rift_cluster::ControlRequest {
                    op_id: uuid::Uuid::new_v4(),
                    principal: None,
                    issued_at_secs: 0,
                    expected_revision: None,
                    op: rift_cluster::ControlOp::PutImposter {
                        tenant: rift_cluster::TenantId::new(&tenant),
                        config: Box::new(config),
                    },
                })
                .await
                .expect("commit");
            assert_eq!(response.outcome, rift_cluster::ControlOutcome::Applied);
            response.revision
        }
    };
    let last = {
        commit(6600, "acme").await;
        commit(6601, "acme").await;
        commit(6602, "beta").await;
        commit(6603, "beta").await
    };
    assert!(
        members[0]
            .node
            .await_applied(last, Duration::from_secs(10))
            .await
            .is_empty(),
        "every member must hold the tenant rows before providing"
    );

    const SHARED: &str = "x-session-abc";
    let acme_writer = store_on_port(&members[0], 6600, tenant_scoped());
    let acme_reader = store_on_port(&members[1], 6601, tenant_scoped());
    let beta_reader = store_on_port(&members[1], 6602, tenant_scoped());
    let beta_writer = store_on_port(&members[0], 6603, tenant_scoped());

    blocking(move || acme_writer.set(SHARED, "step", serde_json::json!("acme")))
        .await
        .expect("acme write");
    let seen = blocking(move || acme_reader.get(SHARED, "step"))
        .await
        .expect("strong read");
    assert_eq!(
        seen,
        Some(serde_json::json!("acme")),
        "two of acme's imposters must share one tenant context"
    );
    let seen = blocking(move || beta_reader.get(SHARED, "step"))
        .await
        .expect("strong read");
    assert_eq!(
        seen, None,
        "beta must never observe acme's tenant-scoped state"
    );

    // The same id written by beta lands in beta's namespace only.
    blocking(move || beta_writer.set(SHARED, "step", serde_json::json!("beta")))
        .await
        .expect("beta write");
    let acme_probe = store_on_port(&members[1], 6600, tenant_scoped());
    let seen = blocking(move || acme_probe.get(SHARED, "step"))
        .await
        .expect("strong read");
    assert_eq!(
        seen,
        Some(serde_json::json!("acme")),
        "beta's write must not overwrite acme's"
    );

    // Disjoint from the other two scopes, including prefix-shaped ids: an imposter-scoped or
    // fleet-scoped write of `tacme:x-session-abc` is not acme's `x-session-abc`.
    let imposter_writer = store_on_port(&members[0], 6600, serde_json::json!({}));
    blocking(move || imposter_writer.set("tacme:x-session-abc", "step", serde_json::json!("imp")))
        .await
        .expect("imposter-scoped write");
    let fleet_writer = store_on_port(
        &members[0],
        6601,
        serde_json::json!({ "contextScope": "fleet" }),
    );
    blocking(move || fleet_writer.set("tacme:x-session-abc", "step", serde_json::json!("fleet")))
        .await
        .expect("fleet-scoped write");
    let acme_probe = store_on_port(&members[1], 6601, tenant_scoped());
    let seen = blocking(move || acme_probe.get(SHARED, "step"))
        .await
        .expect("strong read");
    assert_eq!(
        seen,
        Some(serde_json::json!("acme")),
        "a prefix-shaped id under another scope must not reach the tenant namespace"
    );
    // And what the tenant store wrote is stored under `tacme:` — the production rendering, so
    // the ring, replication and the admin front all agree on the key.
    let stored =
        rift_cluster::stores::ContextScope::Tenant.scoped_flow_id(Some(6600), Some("acme"), SHARED);
    assert_eq!(stored, "tacme:x-session-abc");
    let held = members
        .iter()
        .any(|m| m.shard.get(&stored, "step").is_some());
    assert!(
        held,
        "the tenant-scoped write is stored under the tenant prefix on some member"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// A tenant-scoped store built before its config row is applied — the one legitimate way the
/// owning tenant can be unresolvable at `provide` — must not fall into any real tenant's
/// namespace, nor the fleet's: it renders its own defensive `t??:` prefix. And it must not stay
/// there: the failure is retried per op, so once the row lands the same store writes under the
/// real tenant. A store that cached the failure would keep every one of this imposter's flows
/// in the placeholder namespace for its lifetime, with nothing but a provide-time log to say so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unresolved_tenant_falls_back_defensively_and_heals_once_the_row_lands() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;
    let tenant_scoped = serde_json::json!({ "contextScope": "tenant" });
    const PORT: u16 = 6700;

    // Nobody has committed a config for PORT: the provider cannot resolve its tenant.
    let store = store_on_port(&members[0], PORT, tenant_scoped.clone());
    let early = Arc::clone(&store);
    blocking(move || early.set("early", "k", serde_json::json!(1)))
        .await
        .expect("the store still functions");
    let placeholder =
        rift_cluster::stores::ContextScope::Tenant.scoped_flow_id(Some(PORT), None, "early");
    assert_eq!(placeholder, "t??:early");
    assert!(
        members
            .iter()
            .any(|m| m.shard.get(&placeholder, "k").is_some()),
        "an unresolved tenant writes under the defensive placeholder"
    );
    for wrong in ["tacme:early", "f:early", "i6700:early"] {
        assert!(
            members.iter().all(|m| m.shard.get(wrong, "k").is_none()),
            "never under a real namespace: {wrong}"
        );
    }

    // The row lands; the very same store now resolves and writes under `tacme:`.
    let response = members[0]
        .node
        .submit(rift_cluster::ControlRequest {
            op_id: uuid::Uuid::new_v4(),
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op: rift_cluster::ControlOp::PutImposter {
                tenant: rift_cluster::TenantId::new("acme"),
                config: Box::new(imposter_on(PORT, tenant_scoped)),
            },
        })
        .await
        .expect("commit");
    assert_eq!(response.outcome, rift_cluster::ControlOutcome::Applied);
    assert!(
        members[0]
            .node
            .await_applied(response.revision, Duration::from_secs(10))
            .await
            .is_empty()
    );
    let late = Arc::clone(&store);
    blocking(move || late.set("late", "k", serde_json::json!(2)))
        .await
        .expect("write after the row landed");
    assert!(
        members
            .iter()
            .any(|m| m.shard.get("tacme:late", "k").is_some()),
        "the same store heals onto the real tenant prefix"
    );
    assert!(
        members
            .iter()
            .all(|m| m.shard.get("t??:late", "k").is_none()),
        "and stops using the placeholder"
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
///
/// Pins D-17: ownership is derived from the *committed* membership — the
/// applied index is the fencing token, and a write minted under an older
/// membership is refused rather than applied under an ownership the sender no
/// longer holds.
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
///
/// Pins D-20: a flow has exactly one owner (HRW over the applied membership),
/// and a misrouted write answers `NotOwner{owner}` instead of being applied.
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

    // Wait for the tombstone to actually reach the replica before injecting anything.
    //
    // `set`/`delete` return once the **owner's** write is durable; the push to the replica is
    // asynchronous and neither call awaits it. Without this wait the test races that push, and it
    // loses often enough to be a recurring CI failure on unrelated branches: the replica is still
    // holding the seed at v1, the injected v1 `Put` ties rather than loses, and the assertion below
    // reports the *seed* (a non-zero `expires_at`, unlike the injected entry's `0`) as a
    // resurrection that never happened.
    //
    // `get_versioned` rather than `get`, because `get` hides tombstones — it answers `None` both
    // for "the delete arrived" and for "nothing ever arrived", and proceeding on the second would
    // let the injected `Put` land on an empty replica and genuinely resurrect the key. Waiting for
    // `deleted: true` is the only state that means what this test needs it to mean.
    let deadline = Instant::now() + CONVERGE;
    loop {
        if replica
            .shard
            .get_versioned(&stored("flow-rz"), "k")
            .is_some_and(|entry| entry.deleted)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the delete's tombstone never replicated to {}; the stale-Put injection below would \
             have been testing an empty replica rather than a tombstoned one",
            replica.node.id()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
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
///
/// Pins D-17: flow state is not on consensus — a membership change moves
/// ownership, and the new owner recovers the state from successor replicas
/// (adopting the highest version), not from the Raft log.
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
///
/// Pins D-7: the provider hands out a per-imposter face over the shared,
/// late-bound net — the store can be constructed before the node exists, and
/// that construction-time gap surfaces as a loud error, not a local store.
#[tokio::test(flavor = "multi_thread")]
async fn an_unbound_store_fails_loud_never_silently_local() {
    let shard = FlowShard::in_memory(ShardConfig::default());
    let net = FlowNet::new(shard);
    let store = ClusteredFlowStoreProvider::new(net)
        .provide(&imposter(serde_json::json!({})))
        .expect("provider always provides");

    let writer = Arc::clone(&store);
    let err = blocking(move || writer.set("flow-early", "k", serde_json::json!(1)))
        .await
        .expect_err("a write before bind must refuse");
    assert!(
        err.to_string().contains("starting"),
        "the refusal must say the cluster is starting, got: {err}"
    );
    assert_backend_unavailable(&err, "a write before bind");

    // The read side has its own pre-flight, and D-65 types it the same way.
    let err = blocking(move || store.get("flow-early", "k"))
        .await
        .expect_err("a strong read before bind must refuse, never answer `None`");
    assert!(
        err.to_string().contains("starting"),
        "the refusal must say the cluster is starting, got: {err}"
    );
    assert_backend_unavailable(&err, "a strong read before bind");
}

/// Issue #372: `FlowNet::fleet_entry_counts`'s partial flag, with an
/// unreachable peer. A full multi-node HTTP round trip through
/// `GET /admin/tenants` is not the practical place to inject "peer
/// unreachable" — this unit-tests the fan-out itself, the same layer
/// `a_delayed_put_cannot_resurrect_a_deleted_key` above reaches into for its
/// owner/replica split.
///
/// Node death **without** a membership change is the honest shape of
/// "unreachable": the ring still lists the dead node (nothing removed it), so
/// the fan-out must still try to reach it and fail — exactly what a real
/// network partition looks like from the surviving node's side, and exactly
/// why the answer must degrade to `partial: true` rather than silently
/// omitting that peer's share.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_peer_marks_the_flow_entry_usage_partial() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    // Pick whichever member owns this flow so the write — and the count this
    // test asserts on — survives the other member's death.
    let ring = members[0].node.ring();
    let owner_id = ring
        .owner(rift_cluster::OwnedKey::new(
            rift_cluster::KeyClass::FlowKv,
            &stored("flow-usage-partial"),
        ))
        .expect("two members");
    let (owner, other) = if members[0].node.id() == owner_id {
        (&members[0], &members[1])
    } else {
        (&members[1], &members[0])
    };

    let store = store_on(owner, serde_json::json!({}));
    blocking(move || store.set("flow-usage-partial", "k", serde_json::json!("v")))
        .await
        .expect("write through the owner");

    // Kill the other member's cluster port without a membership change: the
    // owner's own ring still lists it, so the fan-out has to try it.
    other.node.shutdown().await.expect("shutdown the peer");

    let (counts, partial) = owner
        .net
        .fleet_entry_counts(&[TEST_PORT], Duration::from_millis(500))
        .await;

    assert!(
        partial,
        "an unreachable ring member must mark the answer partial, never a \
         silently complete one"
    );
    assert_eq!(
        counts.get(&TEST_PORT).copied(),
        Some(1),
        "partial must not mean discarded: the reachable owner's own entry \
         must still be counted: {counts:?}"
    );

    owner.node.shutdown().await.expect("shutdown owner");
}

/// Issue #372: the ring-divergence half of the fan-out's `partial` contract,
/// isolated the same way `a_stale_fencing_token_is_rejected_and_counted`
/// isolates fencing above — a hand-built wire body straight at
/// `/_cluster/flow/counts`, because provoking *real* ring divergence needs an
/// in-flight membership change racing a fan-out, which is exactly the kind of
/// timing-dependent setup the neighbouring `an_unreachable_peer_...` test's
/// doc comment already rules out as impractical.
///
/// `owned_port_counts` decides ownership from the *serving* node's own ring,
/// so two nodes whose rings disagree (mid membership-change) could each
/// answer under a different view — one flow claimed by both (double count) or
/// by neither (undercount), and both would report `partial: false`. The fix
/// is the caller stamping its `m_idx` on the request and the callee refusing
/// on a mismatch, which routes into the same peer-failure path
/// `an_unreachable_peer_marks_the_flow_entry_usage_partial` proves turns into
/// `partial: true` — so this test only needs to pin the refusal itself: a
/// stale `m_idx` is rejected, and the current one is answered normally.
#[tokio::test(flavor = "multi_thread")]
async fn a_ring_divergence_marks_the_flow_entry_usage_partial() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let target = members[1].node.id();
    let current_m_idx = members[0].node.ring().m_idx();

    let stale = serde_json::to_vec(&serde_json::json!({
        "ports": [TEST_PORT],
        "m_idx": current_m_idx + 1,
    }))
    .expect("encode");
    let stale_reply = members[0]
        .node
        .call_member(target, "POST", "/_cluster/flow/counts", stale)
        .await;
    assert!(
        stale_reply.is_err(),
        "a caller `m_idx` the serving node's ring does not share must be \
         refused, not answered under whichever view the peer happens to \
         hold: {stale_reply:?}"
    );

    // Control: the current token is answered normally, so the refusal above
    // is really about the mismatch and not some other request-shape problem.
    let fresh = serde_json::to_vec(&serde_json::json!({
        "ports": [TEST_PORT],
        "m_idx": current_m_idx,
    }))
    .expect("encode");
    let fresh_reply = members[0]
        .node
        .call_member(target, "POST", "/_cluster/flow/counts", fresh)
        .await
        .expect("a matching m_idx must be answered");
    let fresh_reply: serde_json::Value = serde_json::from_slice(&fresh_reply).expect("json reply");
    assert!(
        fresh_reply.get("slots").is_some(),
        "a matching m_idx must get a real counts reply, got {fresh_reply}"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// Issue #374 — the property the whole listing rests on: it is **fleet-wide and
/// duplicate-free**, and it does not depend on which node you ask.
///
/// A flow keeps `REPLICAS` copies, so the two obvious implementations are both
/// wrong: reading the local shard lists whatever copies this node happens to
/// hold (incomplete, and different per node), while summing every node's copies
/// lists each space several times. Filtering to the ring **owner** is what makes
/// the union exactly the set of live flows, which is what this asserts — from
/// *both* nodes, so a listing that silently answered only from local state
/// fails on whichever node owns less.
#[tokio::test(flavor = "multi_thread")]
async fn spaces_listing_is_fleet_wide_duplicate_free_and_node_independent() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    const PORT: u16 = 6400;
    const SPACES: [&str; 6] = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];

    // Written through one node; ownership lands wherever HRW puts each flow, so
    // in a 2-voter cluster both nodes typically own some and each node's answer
    // needs the other's share to be complete.
    let writer = store_on_port(&members[0], PORT, serde_json::json!({}));
    for (i, space) in SPACES.iter().enumerate() {
        let store = Arc::clone(&writer);
        let owned = (*space).to_owned();
        blocking(move || store.set(&owned, "step", serde_json::json!(i)))
            .await
            .unwrap_or_else(|e| panic!("write {space}: {e}"));
    }

    let mut expected: Vec<String> = SPACES.iter().map(|s| (*s).to_owned()).collect();
    expected.sort_unstable();

    for (i, member) in members.iter().enumerate() {
        let (rows, partial) = member
            .net
            .fleet_spaces(PORT, "i6400:", Duration::from_secs(5))
            .await;

        assert!(
            !partial,
            "node {} answered partial with both voters up",
            i + 1
        );

        let mut listed: Vec<String> = rows.iter().map(|row| row.space.clone()).collect();
        listed.sort_unstable();
        assert_eq!(
            listed,
            expected,
            "node {} listed {listed:?}; every space exactly once was expected",
            i + 1
        );

        // Stated separately from the equality above so a regression that
        // duplicated rows names itself, rather than reading as a sort mismatch.
        let unique: std::collections::HashSet<&str> =
            rows.iter().map(|row| row.space.as_str()).collect();
        assert_eq!(
            unique.len(),
            rows.len(),
            "node {} listed a replicated space more than once",
            i + 1
        );

        for row in &rows {
            assert_eq!(
                row.entry_count, 1,
                "space {} was written exactly one key",
                row.space
            );
            // The real-wire peer branch stamps a row with the peer that reported it
            // (`merge_space_rows`); only the synthetic `merge_space_rows` unit test pinned that
            // before now, so a transposition bug (every row stamped `me` regardless of which
            // node actually owns it) sailed through this end-to-end test with green everywhere.
            let expected_owner = member
                .node
                .ring()
                .owner(rift_cluster::OwnedKey::new(
                    rift_cluster::KeyClass::FlowKv,
                    &stored_on(PORT, &row.space),
                ))
                .expect("two members");
            assert_eq!(
                row.owner,
                expected_owner,
                "node {} reported space {} as owned by {}, ring says {}",
                i + 1,
                row.space,
                row.owner,
                expected_owner
            );
        }
    }

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// Issue #374. An imposter with no flows has a **knowable** zero, and it must be
/// reported as one — `partial: false` — so the console can say "no spaces"
/// rather than "cannot tell". The unknowable zero (a peer that did not answer)
/// is the case that stamps `partial`, and collapsing the two would let the
/// screen state absence as fact.
#[tokio::test(flavor = "multi_thread")]
async fn an_imposter_with_no_spaces_reports_a_knowable_zero() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let (rows, partial) = members[0]
        .net
        .fleet_spaces(6599, "i6599:", Duration::from_secs(5))
        .await;

    assert!(rows.is_empty());
    assert!(!partial, "both voters answered; this zero is knowable");

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// Issue #374, the spaces-listing sibling of `an_unreachable_peer_marks_the_flow_entry_usage_partial`:
/// a dead peer must not make the listing look complete. `fleet_spaces` still owes the caller
/// whatever this node itself owns — `partial` is how the caller is told the *other* member's share
/// is missing, not a licence to answer nothing at all.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_peer_marks_the_spaces_listing_partial() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let ring = members[0].node.ring();
    let owner_id = ring
        .owner(rift_cluster::OwnedKey::new(
            rift_cluster::KeyClass::FlowKv,
            &stored("flow-spaces-partial"),
        ))
        .expect("two members");
    let (owner, other) = if members[0].node.id() == owner_id {
        (&members[0], &members[1])
    } else {
        (&members[1], &members[0])
    };

    let store = store_on(owner, serde_json::json!({}));
    blocking(move || store.set("flow-spaces-partial", "k", serde_json::json!("v")))
        .await
        .expect("write through the owner");

    // Same sabotage as the entry-usage test: kill the peer's cluster port without a membership
    // change, so the owner's ring still lists it and the fan-out has to try it.
    other.node.shutdown().await.expect("shutdown the peer");

    let (rows, partial) = owner
        .net
        .fleet_spaces(
            TEST_PORT,
            &rift_cluster::stores::ContextScope::Imposter.prefix_for(Some(TEST_PORT), None),
            Duration::from_millis(500),
        )
        .await;

    assert!(
        partial,
        "an unreachable ring member must mark the listing partial, never a silently complete one"
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.space.as_str())
            .collect::<Vec<_>>(),
        vec!["flow-spaces-partial"],
        "partial must not mean discarded: the reachable owner's own space must still be listed: \
         {rows:?}"
    );

    owner.node.shutdown().await.expect("shutdown owner");
}

/// Issue #374, isolating the spaces wire route's ring-divergence refusal the same way
/// `a_ring_divergence_marks_the_flow_entry_usage_partial` isolates the counts route's: a
/// hand-built body straight at `/_cluster/flow/spaces`, because provoking real ring divergence
/// needs an in-flight membership change racing a fan-out. `SpacesReq`'s fields (`port`, `prefix`,
/// `m_idx`) are read off the struct in `stores/flow.rs` rather than imported — it is
/// crate-private, and the wire body is exactly what an out-of-process peer would send.
#[tokio::test(flavor = "multi_thread")]
async fn a_ring_divergence_is_refused_by_the_spaces_wire_route() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let target = members[1].node.id();
    let current_m_idx = members[0].node.ring().m_idx();
    let prefix = rift_cluster::stores::ContextScope::Imposter.prefix_for(Some(TEST_PORT), None);

    let stale = serde_json::to_vec(&serde_json::json!({
        "port": TEST_PORT,
        "prefix": prefix,
        "m_idx": current_m_idx + 1,
    }))
    .expect("encode");
    let stale_reply = members[0]
        .node
        .call_member(target, "POST", "/_cluster/flow/spaces", stale)
        .await;
    assert!(
        stale_reply.is_err(),
        "a caller `m_idx` the serving node's ring does not share must be refused, not answered \
         under whichever view the peer happens to hold: {stale_reply:?}"
    );

    // Control: the current token is answered normally, so the refusal above is really about the
    // mismatch and not some other request-shape problem.
    let fresh = serde_json::to_vec(&serde_json::json!({
        "port": TEST_PORT,
        "prefix": prefix,
        "m_idx": current_m_idx,
    }))
    .expect("encode");
    let fresh_reply = members[0]
        .node
        .call_member(target, "POST", "/_cluster/flow/spaces", fresh)
        .await
        .expect("a matching m_idx must be answered");
    let fresh_reply: serde_json::Value = serde_json::from_slice(&fresh_reply).expect("json reply");
    assert!(
        fresh_reply.get("rows").is_some(),
        "a matching m_idx must get a real spaces reply, got {fresh_reply}"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// Issue #374: the listing's own isolation contract, alongside the store-level one
/// `imposter_scope_isolates_two_imposters_sharing_one_flow_id` already pins. Two imposters at the
/// default (`Imposter`) scope that both use the same caller-chosen flow id are still two separate
/// spaces once scoped (`i6400:x-session-abc` vs `i6401:x-session-abc`) — the listing must not let
/// one port's write show up under the other port's enumeration.
#[tokio::test(flavor = "multi_thread")]
async fn two_imposters_sharing_a_flow_id_do_not_list_each_others_spaces() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    const SHARED: &str = "x-session-abc";
    const PORT_A: u16 = 6400;
    const PORT_B: u16 = 6401;

    let store_a = store_on_port(&members[0], PORT_A, serde_json::json!({}));
    let store_b = store_on_port(&members[0], PORT_B, serde_json::json!({}));
    blocking(move || store_a.set(SHARED, "step", serde_json::json!("a")))
        .await
        .expect("write through imposter A");
    blocking(move || store_b.set(SHARED, "step", serde_json::json!("b")))
        .await
        .expect("write through imposter B");

    let (rows_a, partial_a) = members[0]
        .net
        .fleet_spaces(
            PORT_A,
            &rift_cluster::stores::ContextScope::Imposter.prefix_for(Some(PORT_A), None),
            Duration::from_secs(5),
        )
        .await;
    let (rows_b, partial_b) = members[0]
        .net
        .fleet_spaces(
            PORT_B,
            &rift_cluster::stores::ContextScope::Imposter.prefix_for(Some(PORT_B), None),
            Duration::from_secs(5),
        )
        .await;

    assert!(
        !partial_a && !partial_b,
        "both voters up: neither list should be partial"
    );
    assert_eq!(
        rows_a
            .iter()
            .map(|row| row.space.as_str())
            .collect::<Vec<_>>(),
        vec![SHARED],
        "imposter A's listing must show its own write: {rows_a:?}"
    );
    assert_eq!(
        rows_b
            .iter()
            .map(|row| row.space.as_str())
            .collect::<Vec<_>>(),
        vec![SHARED],
        "imposter B's listing must show its own write: {rows_b:?}"
    );

    for member in &members {
        member.node.shutdown().await.expect("shutdown");
    }
}

/// Pins D-17: a partitioned owner refuses flow writes and owner-answered strong
/// reads until it sees a quorum again. Before #465 the rule was enforced only
/// for proxyOnce claims, so a deposed owner on the minority side kept mutating
/// and serving keys a new owner on the majority side already held.
#[tokio::test]
async fn an_isolated_owner_refuses_writes_and_strong_reads_but_not_local_ones() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster_of(3, Duration::from_millis(200)).await;

    // Membership is agreed, so every member computes the same owner for this key.
    let flow = "flow-isolated";
    let owner_id = members[0]
        .node
        .ring()
        .owner(OwnedKey::new(KeyClass::FlowKv, &stored(flow)))
        .expect("a formed cluster owns every key");
    let owner_ix = members
        .iter()
        .position(|m| m.node.id() == owner_id)
        .expect("the owner is one of the three members");

    let m_idx = members[owner_ix].node.ring().m_idx();
    let write_body = |val: &str| {
        serde_json::to_vec(&serde_json::json!({
            "flow_id": stored(flow),
            "key": "k",
            "op": { "Set": { "value": val } },
            "ttl_seconds": null,
            "durability": "async",
            "m_idx": m_idx,
        }))
        .expect("encode")
    };
    let write_on_owner = |body: Vec<u8>| {
        let node = Arc::clone(&members[owner_ix].node);
        async move {
            let raw = node
                .call_member(owner_id, "POST", "/_cluster/flow/write", body)
                .await
                .expect("transport");
            serde_json::from_slice::<serde_json::Value>(&raw).expect("json")
        }
    };

    // Healthy first, through the very same entry point: the refusal below is
    // then attributable to isolation and not to a path that never worked.
    let applied = write_on_owner(write_body("healthy")).await;
    assert!(
        applied.get("Applied").is_some(),
        "a healthy owner must apply its own write: {applied}"
    );

    // Partition it. With both peers gone the owner cannot reach a quorum, which
    // is exactly what `is_isolated` reports (see
    // `node.rs::leader_becomes_isolated_when_it_loses_quorum`).
    for (ix, member) in members.iter().enumerate() {
        if ix != owner_ix {
            member.node.shutdown().await.expect("shutdown peer");
        }
    }
    let deadline = Instant::now() + CONVERGE;
    while !members[owner_ix].node.is_isolated() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        members[owner_ix].node.is_isolated(),
        "an owner that lost its quorum must report isolated"
    );

    let refusals_before = counter("rift_cluster_cas_conflicts_total", ("reason", "isolated"));
    let refused = write_on_owner(write_body("divergent")).await;
    let reason = refused
        .get("Error")
        .and_then(|e| e.get("reason"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("an isolated owner must refuse the write: {refused}"));
    assert!(
        reason.contains("owner is isolated"),
        "the refusal must name isolation rather than some other failure: {reason}"
    );
    assert_eq!(
        counter("rift_cluster_cas_conflicts_total", ("reason", "isolated")) - refusals_before,
        1,
        "the refusal is the runbook's signal and must be counted exactly once"
    );

    // The safety property itself: the divergent value never reached the shard,
    // so a healed majority has nothing wrong to reconcile against.
    assert_eq!(
        members[owner_ix]
            .shard
            .get(&stored(flow), "k")
            .map(|entry| entry.value),
        Some(serde_json::json!("healthy")),
        "an isolated owner must not mutate owned state"
    );

    // A forwarded owner-read landing here is the minority-side serve in its
    // purest form, and the route must refuse it rather than answer.
    let get_body = serde_json::to_vec(&serde_json::json!({ "flow_id": stored(flow), "key": "k" }))
        .expect("encode");
    let forwarded = members[owner_ix]
        .node
        .call_member(owner_id, "POST", "/_cluster/flow/get", get_body)
        .await
        .expect_err("an isolated node must refuse a forwarded owner-read, not answer it");
    assert!(
        forwarded.contains("owner is isolated"),
        "the route's refusal must carry the same discriminator as the other two \
         entries — a bare `is_err()` here would pass on a drifted literal, a \
         transport failure or a decode failure alike: {forwarded}"
    );

    // The store face: `strong` fails loudly; `local` still answers, because the
    // imposter opted into replica staleness (D-10) and this rule must not
    // silently revoke that contract.
    let strong = store_on(&members[owner_ix], serde_json::json!({}));
    let err = blocking(move || strong.get(flow, "k"))
        .await
        .expect_err("a strong read on an isolated owner must fail loudly");
    assert!(
        err.to_string().contains("owner is isolated"),
        "the strong read must name isolation: {err}"
    );
    assert_backend_unavailable(&err, "an owner-side strong read on an isolated owner");

    // The same refusal on the write side travels in band (`WriteReply::Error`),
    // and the store face must type it the same way (D-65).
    let writer = store_on(&members[owner_ix], serde_json::json!({}));
    let err = blocking(move || writer.set(flow, "k", serde_json::json!("divergent-face")))
        .await
        .expect_err("a write on an isolated owner must fail loudly at the store face");
    assert!(
        err.to_string().contains("owner is isolated"),
        "the refused write must name isolation: {err}"
    );
    assert_backend_unavailable(&err, "a write on an isolated owner");

    let local = store_on(
        &members[owner_ix],
        serde_json::json!({ "readConsistency": "local" }),
    );
    assert_eq!(
        blocking(move || local.get(flow, "k"))
            .await
            .expect("a local read never consults the owner"),
        Some(serde_json::json!("healthy")),
        "`local` is the imposter's opted-in replica read and stays available while isolated"
    );

    members[owner_ix]
        .node
        .shutdown()
        .await
        .expect("shutdown owner");
}

/// Pins D-61 (#471): the runbook signal survives the forwarding hop.
///
/// The sibling test above reads on the isolated owner itself. This one reads through a **live
/// non-owner**, which is the path an on-call actually hits, and the path that used to lie: the
/// owner's `Unavailable` was flattened into this hop's `RpcError::Transport`, so the client saw
/// `502 transport failure: <authority> unreachable (…)` for a peer that was up and had said
/// exactly what was wrong — sending triage at the network instead of at quorum state.
///
/// Four members, not three: the owner must be isolated *while another node is still alive to
/// forward through*, so the survivors have to be a minority. Two of four is; two of three is not.
#[tokio::test]
async fn a_forwarded_refusal_from_an_isolated_owner_is_unavailable_not_unreachable() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster_of(4, Duration::from_millis(200)).await;

    // `flow_cluster_of` returns as soon as the ring holds four members, but a joiner enters as a
    // learner — so wait for the promotion sweep before counting on a quorum. Without this the
    // effective membership can still be three voters plus a learner, where killing two leaves
    // 2 of 3 and the survivors keep their quorum; the test would then fail on arithmetic that
    // has nothing to do with what it pins.
    let voters_by = Instant::now() + CONVERGE;
    while members
        .iter()
        .any(|m| m.node.status().voters.len() != members.len())
        && Instant::now() < voters_by
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for member in &members {
        assert_eq!(
            member.node.status().voters.len(),
            4,
            "every member must be a voter, or `two of four is a minority` is not the arithmetic \
             this test is running"
        );
    }

    let flow = "flow-forwarded-refusal";
    let owner_id = members[0]
        .node
        .ring()
        .owner(OwnedKey::new(KeyClass::FlowKv, &stored(flow)))
        .expect("a formed cluster owns every key");
    let owner_ix = members
        .iter()
        .position(|m| m.node.id() == owner_id)
        .expect("the owner is one of the four members");
    // The forwarder must be a *follower*, and the leader must not survive. D-17 isolates a
    // follower only while it cannot see a leader, so keeping the leader alive as the forwarder
    // would leave the owner hearing heartbeats and never isolated — the survivors would be one
    // correctly-isolated leader and one unbothered follower, which is not the situation this
    // test is about. The exception is an owner that *is* the leader: it isolates on its own
    // quorum-ack age, and any surviving follower can forward to it.
    let leader_id = members[0]
        .node
        .status()
        .current_leader
        .expect("a converged cluster has a leader");
    let forwarder_ix = (0..members.len())
        .find(|ix| {
            *ix != owner_ix && (owner_id == leader_id || members[*ix].node.id() != leader_id)
        })
        .expect("a four-member cluster has a non-owner that is not the leader");

    // Two of four left alive: short of the three-voter majority, so the owner loses its quorum
    // while staying reachable over RPC from the forwarder.
    for (ix, member) in members.iter().enumerate() {
        if ix != owner_ix && ix != forwarder_ix {
            member.node.shutdown().await.expect("shutdown peer");
        }
    }

    let isolated_by = Instant::now() + CONVERGE;
    while !members[owner_ix].node.is_isolated() && Instant::now() < isolated_by {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        members[owner_ix].node.is_isolated(),
        "an owner that lost its quorum must report isolated"
    );

    let forwarded = store_on(&members[forwarder_ix], serde_json::json!({}));
    let err = blocking(move || forwarded.get(flow, "k"))
        .await
        .expect_err("a strong read forwarded to an isolated owner must fail loudly");
    let text = err.to_string();

    assert!(
        text.contains("owner is isolated"),
        "the owner's own reason must survive the hop: {text}"
    );
    assert!(
        !text.contains("unreachable"),
        "the owner answered, so the hop must not report it as unreachable: {text}"
    );
    assert!(
        !text.contains("transport failure"),
        "a peer-answered refusal is not this hop's transport failure (502): {text}"
    );
    assert!(
        text.contains("flowState: unavailable:"),
        "the refusal must keep the owner's `Unavailable` class inside the wrap — matched at the \
         detail position, since the wrapper's own `backend unavailable:` would match any typed \
         error: {text}"
    );
    assert_backend_unavailable(&err, "a strong read forwarded to an isolated owner");

    for ix in [owner_ix, forwarder_ix] {
        members[ix]
            .node
            .shutdown()
            .await
            .expect("shutdown survivor");
    }
}

/// Pins where the D-17 check sits in `owner_write`'s sequence: fence, then
/// ownership, then isolation. It does **not** prove the guard exists — delete the
/// guard entirely and this still passes, because both refusals below are reached
/// before it; that job belongs to
/// `an_isolated_owner_refuses_writes_and_strong_reads_but_not_local_ones`. What it
/// discriminates is a guard hoisted too early: an isolated non-owner would then
/// answer `Error{isolated}` instead of `NotOwner`, sending a correct caller off to
/// rebuild a ring that was already right and hiding the misroute the counter
/// exists to surface — and an isolated stale-token write would lose its `Fenced`
/// reply, which is the one that tells the caller to retry at all.
#[tokio::test]
async fn a_misroute_is_still_a_misroute_when_the_receiving_node_is_isolated() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster_of(3, Duration::from_millis(200)).await;

    let flow = "flow-misroute-isolated";
    let owner_id = members[0]
        .node
        .ring()
        .owner(OwnedKey::new(KeyClass::FlowKv, &stored(flow)))
        .expect("a formed cluster owns every key");
    let survivor_ix = members
        .iter()
        .position(|m| m.node.id() != owner_id)
        .expect("with three members at least one is not the owner");

    // Leave a single non-owner alive: it is isolated *and* it does not own the
    // key, which is the collision this test is about.
    for (ix, member) in members.iter().enumerate() {
        if ix != survivor_ix {
            member.node.shutdown().await.expect("shutdown");
        }
    }
    let deadline = Instant::now() + CONVERGE;
    while !members[survivor_ix].node.is_isolated() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        members[survivor_ix].node.is_isolated(),
        "a lone survivor of a three-voter cluster must report isolated"
    );

    // Membership cannot change without a quorum, so the ring — and therefore the
    // owner it names — is the same one the healthy cluster agreed on.
    let survivor = &members[survivor_ix];
    let body = serde_json::to_vec(&serde_json::json!({
        "flow_id": stored(flow),
        "key": "k",
        "op": { "Set": { "value": "misrouted" } },
        "ttl_seconds": null,
        "durability": "async",
        "m_idx": survivor.node.ring().m_idx(),
    }))
    .expect("encode");
    let raw = survivor
        .node
        .call_member(survivor.node.id(), "POST", "/_cluster/flow/write", body)
        .await
        .expect("transport");
    let reply: serde_json::Value = serde_json::from_slice(&raw).expect("json");
    assert_eq!(
        reply
            .get("NotOwner")
            .and_then(|n| n.get("owner"))
            .and_then(serde_json::Value::as_u64),
        Some(owner_id),
        "a misroute must be reported as a misroute naming the real owner, not as isolation: {reply}"
    );

    // The other half of the ordering: the fence check runs before both, so a
    // stale token on an isolated node is still `Fenced` — the reply that tells
    // the caller to rebuild its ring and retry, rather than one that reads as
    // "this node is out of the picture".
    let stale = serde_json::to_vec(&serde_json::json!({
        "flow_id": stored(flow),
        "key": "k",
        "op": { "Set": { "value": "stale" } },
        "ttl_seconds": null,
        "durability": "async",
        "m_idx": survivor.node.ring().m_idx() - 1,
    }))
    .expect("encode");
    let raw = survivor
        .node
        .call_member(survivor.node.id(), "POST", "/_cluster/flow/write", stale)
        .await
        .expect("transport");
    let reply: serde_json::Value = serde_json::from_slice(&raw).expect("json");
    assert!(
        reply.get("Fenced").is_some(),
        "a stale membership token must still be reported as `Fenced` on an isolated node: {reply}"
    );

    survivor.node.shutdown().await.expect("shutdown survivor");
}

/// Pins D-65 on the row RFC-001 §7.6 names outright — *owner-unreachable*. A `strong` read
/// through a survivor whose owner is dead fails with a liveness error at the store face, and
/// that error must carry `BackendUnavailable` for the same 503 the isolation refusal answers:
/// the client cannot tell the two apart by status, and should not have to. Node death without
/// a membership change, as in `an_unreachable_peer_marks_the_flow_entry_usage_partial`: the
/// ring still names the dead owner, so the survivor forwards to it and fails.
#[tokio::test(flavor = "multi_thread")]
async fn a_strong_read_through_a_survivor_of_a_dead_owner_is_backend_unavailable() {
    let _lock = TEST_LOCK.lock().await;
    let members = flow_cluster().await;

    let flow = "flow-dead-owner";
    let owner_id = members[0]
        .node
        .ring()
        .owner(OwnedKey::new(KeyClass::FlowKv, &stored(flow)))
        .expect("two members");
    let (owner, survivor) = if members[0].node.id() == owner_id {
        (&members[0], &members[1])
    } else {
        (&members[1], &members[0])
    };

    let store = store_on(owner, serde_json::json!({}));
    blocking(move || store.set(flow, "k", serde_json::json!("v")))
        .await
        .expect("write through the owner");

    owner.node.shutdown().await.expect("shutdown the owner");

    let store = store_on(survivor, serde_json::json!({}));
    let err = blocking(move || store.get(flow, "k"))
        .await
        .expect_err("a strong read whose owner is dead must fail, never answer from the replica");
    assert!(
        !err.to_string().contains("owner is isolated"),
        "a dead owner is unreachable, not isolated — the reason must say which: {err:#}"
    );
    assert_backend_unavailable(&err, "a strong read whose owner is unreachable");

    survivor.node.shutdown().await.expect("shutdown survivor");
}
