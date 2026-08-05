//! The clustered proxy-recording store's acceptance gate (#226): real
//! `RaftNode`s over real localhost TCP, the store driven through upstream's
//! own `ProxyRecordingStore` trait — the exact seam `handle_proxy_request`
//! reaches it through (U-16, rift#911).
//!
//! What each test buys, in the issue's words:
//! - concurrent first-hits across the fleet grant exactly one claim, and the
//!   recording lands as one committed op — stub in replicated config on every
//!   node, marker row behind `AlreadyRecorded`;
//! - upstream failure releases the claim; a failed publication releases it
//!   too (retryable, never wedged);
//! - a claim dies with its owner: after the owner leaves, the signature is
//!   re-claimable (duplicate-upstream bound = 1 + ownership changes);
//! - a stale token after deadline expiry cannot clobber the new claim;
//! - a node that joined after the recording answers `AlreadyRecorded` from
//!   the applied table alone;
//! - `clear` deletes the markers fleet-wide;
//! - a 1-voter cluster still records exactly once (single-node fidelity);
//! - before the cluster is bound the store fails loud — no silent builtin.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rift_cluster::stores::{
    ClusterProxyStore, ProxyBindConfig, ProxyNet, proxy_routes, proxy_sig_key,
};
use rift_cluster::{
    Authority, ControlRequest, KeyClass, NodeConfig, NodeId, OwnedKey, RaftNode, RecordedStub,
    RecordedStubPlacement, TenantId,
};
use rift_cluster_base::seams::{
    ClaimOutcome, ClaimToken, ImposterConfig, ProxyRecordingStore, ProxyStoreError,
    RecordedResponse, RequestSignature, Stub,
};
use tempfile::TempDir;

const SECRET: &str = "proxy-claims-test-secret";
const CONVERGE: Duration = Duration::from_secs(10);
const TEST_PORT: u16 = 4646;

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

struct ProxyMember {
    node: Arc<RaftNode>,
    net: Arc<ProxyNet>,
    _dir: TempDir,
}

async fn spawn_member(
    id: NodeId,
    addr: SocketAddr,
    dir: &Path,
    snapshot_log_entries: Option<u64>,
) -> (Arc<RaftNode>, Arc<ProxyNet>) {
    let net = ProxyNet::new();
    let node = RaftNode::start(NodeConfig {
        node_id: id,
        bind: addr,
        advertise: Some(Authority::from(addr)),
        data_dir: dir.to_path_buf(),
        secret: Some(SECRET.to_owned()),
        routes: proxy_routes(Arc::clone(&net)),
        engine: None,
        audit_retention_secs: rift_cluster::DEFAULT_AUDIT_RETENTION_SECS,
        snapshot_log_entries,
    })
    .await
    .unwrap_or_else(|e| panic!("start node {id}: {e}"));
    (Arc::new(node), net)
}

async fn proxy_cluster_of(n: usize, claim_ttl: Duration) -> Vec<ProxyMember> {
    proxy_cluster_with(n, claim_ttl, None).await
}

async fn proxy_cluster_with(
    n: usize,
    claim_ttl: Duration,
    snapshot_log_entries: Option<u64>,
) -> Vec<ProxyMember> {
    let ports = reserve_ports(n);
    let mut members = Vec::new();

    for (i, port) in ports.iter().enumerate() {
        let dir = TempDir::new().expect("tempdir");
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
        let (node, net) =
            spawn_member((i + 1) as NodeId, addr, dir.path(), snapshot_log_entries).await;
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
        members.push(ProxyMember {
            node,
            net,
            _dir: dir,
        });
    }

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
        bind_member(member, claim_ttl);
    }
    members
}

fn bind_member(member: &ProxyMember, claim_ttl: Duration) {
    member
        .net
        .bind(
            &member.node,
            ProxyBindConfig {
                bridge: rift_cluster::BridgeConfig::for_workers(2),
                claim_ttl,
            },
        )
        .expect("bind proxy net");
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

fn mint(op: rift_cluster::ControlOp) -> ControlRequest {
    ControlRequest {
        op_id: uuid::Uuid::new_v4(),
        principal: None,
        issued_at_secs: now_secs(),
        expected_revision: None,
        op,
    }
}

/// A proxyOnce imposter whose only stub is the proxy stub the recordings hang
/// off. The port is the imposter scope; `mode` parameterizes the two modes the
/// store dispatches on.
fn proxy_imposter(port: u16, mode: &str) -> ImposterConfig {
    serde_json::from_value(serde_json::json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "responses": [{
                "proxy": {
                    "to": "http://upstream.example",
                    "mode": mode,
                    "predicateGenerators": [{ "matches": { "path": true } }],
                }
            }]
        }],
    }))
    .expect("imposter parses")
}

/// Install the imposter and wait until every member's applied state can see
/// it — mode resolution reads local applied config, so a test that races the
/// apply would measure replication lag, not claim semantics.
async fn install_imposter(members: &[ProxyMember], port: u16, mode: &str) {
    members[0]
        .node
        .submit(mint(rift_cluster::ControlOp::PutImposter {
            tenant: TenantId::new("default"),
            config: Box::new(proxy_imposter(port, mode)),
        }))
        .await
        .expect("imposter commits");
    let deadline = Instant::now() + CONVERGE;
    loop {
        let applied = members.iter().all(|m| {
            m.node
                .get_imposter("default", port)
                .ok()
                .flatten()
                .is_some()
        });
        if applied {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "imposter never applied fleet-wide"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn sig(path: &str) -> RequestSignature {
    RequestSignature::new("GET", path, None, &[])
}

/// A labeled counter from this process's registry — every member's store reports into the
/// same one, so deltas measure the whole in-process fleet (flow_store.rs's pattern).
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

fn resp(body: &str) -> RecordedResponse {
    RecordedResponse {
        status: 200,
        headers: vec![("content-type".to_owned(), "text/plain".to_owned())],
        body: body.as_bytes().to_vec(),
        latency_ms: Some(5),
        timestamp_secs: now_secs(),
    }
}

fn generated_stub(path: &str, body: &str) -> Stub {
    serde_json::from_value(serde_json::json!({
        "predicates": [{ "equals": { "path": path } }],
        "responses": [{ "is": { "statusCode": 200, "body": body } }],
    }))
    .expect("stub parses")
}

fn store_on(member: &ProxyMember) -> Arc<ClusterProxyStore> {
    Arc::new(ClusterProxyStore::new(Arc::clone(&member.net)))
}

/// The store face is synchronous and parks its thread on the bridge; calling
/// it from a tokio worker would be head-of-line blocking, so tests hop
/// through `spawn_blocking` exactly as the engine's callers do.
async fn claim(store: &Arc<ClusterProxyStore>, port: u16, s: &RequestSignature) -> ClaimOutcome {
    let store = Arc::clone(store);
    let s = s.clone();
    tokio::task::spawn_blocking(move || store.try_claim(port, &s).expect("claim answers"))
        .await
        .expect("join")
}

async fn try_claim_raw(
    store: &Arc<ClusterProxyStore>,
    port: u16,
    s: &RequestSignature,
) -> Result<ClaimOutcome, ProxyStoreError> {
    let store = Arc::clone(store);
    let s = s.clone();
    tokio::task::spawn_blocking(move || store.try_claim(port, &s))
        .await
        .expect("join")
}

async fn complete_recorded(
    store: &Arc<ClusterProxyStore>,
    port: u16,
    s: &RequestSignature,
    token: ClaimToken,
    body: &str,
) -> Result<(), ProxyStoreError> {
    let store = Arc::clone(store);
    let s2 = s.clone();
    let stub = generated_stub(&s.path, body);
    let r = resp(body);
    tokio::task::spawn_blocking(move || {
        store.complete_recorded(
            port,
            s2,
            token,
            r,
            RecordedStub {
                stub: Box::new(stub),
                placement: RecordedStubPlacement::BeforeProxy,
                proxy_to: "http://upstream.example".to_owned(),
            },
        )
    })
    .await
    .expect("join")
}

async fn lookup(
    store: &Arc<ClusterProxyStore>,
    port: u16,
    s: &RequestSignature,
) -> Option<RecordedResponse> {
    let store = Arc::clone(store);
    let s = s.clone();
    tokio::task::spawn_blocking(move || store.lookup(port, &s))
        .await
        .expect("join")
}

/// The member index the HRW ring names as owner for `(port, sig)` — computed
/// through the production key renderer so tests can never disagree with the
/// store about what is hashed.
fn owner_index(members: &[ProxyMember], port: u16, s: &RequestSignature) -> usize {
    let key = proxy_sig_key(port, s);
    let owner = members[0]
        .node
        .ring()
        .owner(OwnedKey::new(KeyClass::Proxy, &key))
        .expect("ring names an owner");
    members
        .iter()
        .position(|m| m.node.id() == owner)
        .expect("owner is a member")
}

/// A signature whose ring owner is `want` — probed through the production key
/// renderer, so ownership placement is real, not assumed.
fn sig_owned_by(members: &[ProxyMember], port: u16, want: usize) -> RequestSignature {
    for i in 0..1000 {
        let candidate = sig(&format!("/owned/{i}"));
        if owner_index(members, port, &candidate) == want {
            return candidate;
        }
    }
    panic!("no signature landed on member {want} in 1000 probes");
}

/// Config stubs a member's applied state carries for `TEST_PORT`.
fn applied_stubs(member: &ProxyMember, port: u16) -> Vec<serde_json::Value> {
    let raw = member
        .node
        .get_imposter("default", port)
        .expect("read applied config")
        .expect("imposter present");
    let config: serde_json::Value = serde_json::from_str(&raw).expect("config parses");
    config["stubs"].as_array().cloned().unwrap_or_default()
}

async fn wait_stub_count(members: &[ProxyMember], port: u16, want: usize) {
    let deadline = Instant::now() + CONVERGE;
    loop {
        if members.iter().all(|m| applied_stubs(m, port).len() == want) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "stub count never reached {want} fleet-wide: {:?}",
            members
                .iter()
                .map(|m| applied_stubs(m, port).len())
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------
// AC2 — concurrent first-hits: exactly one claim, one recording, replicated.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_hits_record_exactly_once_fleet_wide() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(3, Duration::from_secs(30)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;
    let s = sig("/once");

    let granted_before = counter("rift_cluster_proxy_claims_total", ("outcome", "granted"));
    let inflight_before = counter("rift_cluster_proxy_claims_total", ("outcome", "inflight"));
    let replay_before = counter(
        "rift_cluster_proxy_claims_total",
        ("outcome", "already_recorded"),
    );

    // All three nodes race the same first hit — spawned onto the blocking pool
    // directly, so the three claims genuinely run concurrently.
    let handles: Vec<_> = members
        .iter()
        .map(|m| {
            let store = store_on(m);
            let s = s.clone();
            tokio::task::spawn_blocking(move || {
                store.try_claim(TEST_PORT, &s).expect("claim answers")
            })
        })
        .collect();
    let mut outcomes = Vec::new();
    for handle in handles {
        outcomes.push(handle.await.expect("join"));
    }

    let granted: Vec<ClaimToken> = outcomes
        .iter()
        .filter_map(|o| match o {
            ClaimOutcome::Claimed(t) => Some(*t),
            _ => None,
        })
        .collect();
    assert_eq!(granted.len(), 1, "exactly one winner: {outcomes:?}");
    assert!(
        outcomes
            .iter()
            .all(|o| matches!(o, ClaimOutcome::Claimed(_) | ClaimOutcome::InFlight)),
        "losers see InFlight, never AlreadyRecorded pre-record: {outcomes:?}"
    );

    // The winner completes: one committed op carries stub + marker.
    let winner = outcomes
        .iter()
        .position(|o| matches!(o, ClaimOutcome::Claimed(_)))
        .expect("a winner exists");
    complete_recorded(
        &store_on(&members[winner]),
        TEST_PORT,
        &s,
        granted[0],
        "recorded-once",
    )
    .await
    .expect("publication commits");

    // Recorded stub present in the applied config of every node (proxy stub + recorded stub).
    wait_stub_count(&members, TEST_PORT, 2).await;
    for member in &members {
        let stubs = applied_stubs(member, TEST_PORT);
        assert!(
            stubs[0]["responses"][0]["is"]["body"] == serde_json::json!("recorded-once"),
            "recorded stub sits BEFORE the proxy stub on {}: {stubs:?}",
            member.node.id()
        );
    }

    // Every node now answers AlreadyRecorded and replays from durable state.
    for member in &members {
        let store = store_on(member);
        assert!(
            matches!(
                claim(&store, TEST_PORT, &s).await,
                ClaimOutcome::AlreadyRecorded
            ),
            "node {} answers AlreadyRecorded",
            member.node.id()
        );
        let replay = lookup(&store, TEST_PORT, &s).await.expect("replayable");
        assert_eq!(replay.body, b"recorded-once".to_vec());
    }

    // AC9 — the claim families moved, with the right labels: one grant, two
    // concurrent losers, and a replay per member from the loop just above.
    assert_eq!(
        counter("rift_cluster_proxy_claims_total", ("outcome", "granted")) - granted_before,
        1,
        "exactly one grant"
    );
    assert_eq!(
        counter("rift_cluster_proxy_claims_total", ("outcome", "inflight")) - inflight_before,
        2,
        "both losers counted"
    );
    assert_eq!(
        counter(
            "rift_cluster_proxy_claims_total",
            ("outcome", "already_recorded"),
        ) - replay_before,
        3,
        "one replay per member"
    );
    let recordings = prometheus::gather()
        .into_iter()
        .filter(|family| family.get_name() == "rift_cluster_proxy_recordings_total")
        .flat_map(|family| family.get_metric().to_owned())
        .map(|metric| metric.get_counter().get_value() as u64)
        .sum::<u64>();
    assert!(recordings >= 1, "the recordings counter moved");
}

// ---------------------------------------------------------------------------
// AC2b — record() without a generated stub is durable: lookup replays
// fleet-wide from the applied table, not from anyone's memory.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_without_stub_replays_fleet_wide() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(2, Duration::from_secs(30)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;
    let s = sig("/no-generators");

    let store = store_on(&members[0]);
    let ClaimOutcome::Claimed(token) = claim(&store, TEST_PORT, &s).await else {
        panic!("first claim wins");
    };
    {
        let store = Arc::clone(&store);
        let s2 = s.clone();
        let r = resp("bare-recording");
        tokio::task::spawn_blocking(move || store.record(TEST_PORT, s2, token, r))
            .await
            .expect("join")
            .expect("record commits");
    }

    for member in &members {
        let store = store_on(member);
        assert!(
            matches!(
                claim(&store, TEST_PORT, &s).await,
                ClaimOutcome::AlreadyRecorded
            ),
            "recorded fact visible on node {}",
            member.node.id()
        );
        let replay = lookup(&store, TEST_PORT, &s).await.expect("replayable");
        assert_eq!(replay.body, b"bare-recording".to_vec());
    }
    // No stub was generated, so config still carries only the proxy stub.
    wait_stub_count(&members, TEST_PORT, 1).await;
}

// ---------------------------------------------------------------------------
// AC3a — upstream failure: release makes the signature immediately
// re-claimable, no wedge.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upstream_failure_releases_claim_and_signature_is_reclaimable() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(2, Duration::from_secs(30)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;
    let s = sig("/upstream-dies");

    let store_a = store_on(&members[0]);
    let store_b = store_on(&members[1]);

    let ClaimOutcome::Claimed(token) = claim(&store_a, TEST_PORT, &s).await else {
        panic!("first claim wins");
    };
    assert!(
        matches!(claim(&store_b, TEST_PORT, &s).await, ClaimOutcome::InFlight),
        "concurrent loser sees InFlight"
    );

    // The upstream call failed: the engine releases.
    {
        let store = Arc::clone(&store_a);
        let s2 = s.clone();
        tokio::task::spawn_blocking(move || store.release_claim(TEST_PORT, &s2, token))
            .await
            .expect("join");
    }

    // Immediately re-claimable — by the other node, which then records fine.
    let ClaimOutcome::Claimed(token_b) = claim(&store_b, TEST_PORT, &s).await else {
        panic!("released signature is re-claimable");
    };
    complete_recorded(&store_b, TEST_PORT, &s, token_b, "second-try")
        .await
        .expect("retry records");
    assert!(matches!(
        claim(&store_a, TEST_PORT, &s).await,
        ClaimOutcome::AlreadyRecorded
    ));
}

// ---------------------------------------------------------------------------
// AC3b — publication failure (no quorum): the claim is released, not wedged.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publication_failure_releases_claim_and_is_retryable() {
    let _lock = TEST_LOCK.lock().await;
    let mut members = proxy_cluster_of(2, Duration::from_secs(30)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;

    // A signature owned by member 0, so the claim survives member 1's death.
    let s = sig_owned_by(&members, TEST_PORT, 0);
    let store = store_on(&members[0]);
    let ClaimOutcome::Claimed(token) = claim(&store, TEST_PORT, &s).await else {
        panic!("first claim wins");
    };
    assert_eq!(members[0].net.pending_claims(TEST_PORT), 1);

    // Kill the peer: quorum is gone, the publication cannot commit.
    let peer = members.remove(1);
    peer.node.shutdown().await.ok();

    let err = complete_recorded(&store, TEST_PORT, &s, token, "never-commits")
        .await
        .expect_err("publication without quorum must fail loud");
    let _ = err;

    // The owner released the claim — retryable, never Recorded-but-stub-less.
    assert_eq!(
        members[0].net.pending_claims(TEST_PORT),
        0,
        "failed publication releases the pending claim"
    );
    assert!(
        lookup(&store, TEST_PORT, &s).await.is_none(),
        "nothing was recorded"
    );
}

// ---------------------------------------------------------------------------
// AC4 — the claim dies with its owner: after the owner leaves, re-claim
// succeeds once membership settles.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_death_while_pending_allows_reclaim_after_membership_settles() {
    let _lock = TEST_LOCK.lock().await;
    let mut members = proxy_cluster_of(3, Duration::from_secs(30)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;

    // Claim a signature owned by member 2, from member 0 (an RPC claim).
    let s = sig_owned_by(&members, TEST_PORT, 2);
    let store_a = store_on(&members[0]);
    let ClaimOutcome::Claimed(_pending_token) = claim(&store_a, TEST_PORT, &s).await else {
        panic!("first claim wins");
    };

    // The owner dies while the claim is Pending.
    let owner = members.remove(2);
    owner
        .node
        .leave(Duration::from_secs(10))
        .await
        .expect("owner leaves");
    owner.node.shutdown().await.ok();

    // Membership settles at 2 voters; the ring re-homes the key.
    let deadline = Instant::now() + CONVERGE;
    loop {
        if members.iter().all(|m| m.node.ring().members().len() == 2) {
            break;
        }
        assert!(Instant::now() < deadline, "membership never settled");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The signature is re-claimable on the surviving fleet, and records.
    let deadline = Instant::now() + CONVERGE;
    let token = loop {
        match try_claim_raw(&store_a, TEST_PORT, &s).await {
            Ok(ClaimOutcome::Claimed(token)) => break token,
            Ok(ClaimOutcome::AlreadyRecorded) => panic!("nothing was recorded yet"),
            Ok(ClaimOutcome::InFlight) | Err(_) => {
                // The new owner may briefly refuse while isolated/settling.
                assert!(Instant::now() < deadline, "signature never re-claimable");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    complete_recorded(&store_a, TEST_PORT, &s, token, "after-handoff")
        .await
        .expect("recording lands on the survivors");
    assert!(matches!(
        claim(&store_on(&members[1]), TEST_PORT, &s).await,
        ClaimOutcome::AlreadyRecorded
    ));
}

// ---------------------------------------------------------------------------
// AC5 — deadline expiry frees the claim; the expired token is a stale fence
// that cannot clobber the new claim's recording.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_token_after_deadline_expiry_cannot_clobber_new_claim() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(2, Duration::from_millis(300)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;
    let s = sig("/slow-winner");

    let store = store_on(&members[0]);
    let ClaimOutcome::Claimed(stale) = claim(&store, TEST_PORT, &s).await else {
        panic!("first claim wins");
    };

    // Let the claim deadline lapse; the signature must be re-claimable.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let ClaimOutcome::Claimed(fresh) = claim(&store, TEST_PORT, &s).await else {
        panic!("expired claim frees the signature");
    };
    assert_ne!(stale.value(), fresh.value(), "a re-claim mints a new token");

    // The slow first winner limps in with the expired token: rejected.
    complete_recorded(&store, TEST_PORT, &s, stale, "stale-write")
        .await
        .expect("a stale complete is dropped, not an error");
    assert!(
        lookup(&store, TEST_PORT, &s).await.is_none(),
        "the stale token recorded nothing"
    );

    // The fresh claim's recording wins.
    complete_recorded(&store, TEST_PORT, &s, fresh, "fresh-write")
        .await
        .expect("fresh recording lands");
    let replay = lookup(&store, TEST_PORT, &s).await.expect("replayable");
    assert_eq!(replay.body, b"fresh-write".to_vec());
}

// ---------------------------------------------------------------------------
// AC6 — a node that joined after the recording answers AlreadyRecorded from
// the applied table alone (snapshot-joined: small snapshot threshold forces
// the joiner through snapshot install, not log replay).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_owner_answers_already_recorded_from_applied_table() {
    let _lock = TEST_LOCK.lock().await;
    let claim_ttl = Duration::from_secs(30);
    let mut members = proxy_cluster_with(2, claim_ttl, Some(8)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;
    let s = sig("/pre-join");

    let store = store_on(&members[0]);
    let ClaimOutcome::Claimed(token) = claim(&store, TEST_PORT, &s).await else {
        panic!("first claim wins");
    };
    complete_recorded(&store, TEST_PORT, &s, token, "before-the-join")
        .await
        .expect("recording lands");

    // A third member joins after the fact.
    let port = reserve_ports(1)[0];
    let dir = TempDir::new().expect("tempdir");
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let (node, net) = spawn_member(3, addr, dir.path(), Some(8)).await;
    let seed_addr = members[0].node.advertise();
    node.join_via(seed_addr).await.expect("join");
    let joiner = ProxyMember {
        node,
        net,
        _dir: dir,
    };
    let deadline = Instant::now() + CONVERGE;
    loop {
        if joiner.node.ring().members().len() == 3
            && joiner
                .node
                .get_imposter("default", TEST_PORT)
                .ok()
                .flatten()
                .is_some()
        {
            break;
        }
        assert!(Instant::now() < deadline, "joiner never caught up");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bind_member(&joiner, claim_ttl);
    members.push(joiner);

    // Asked directly, the joiner's own applied state answers AlreadyRecorded —
    // it has no in-memory trace of the claim, only consensus state. Polled to a
    // deadline: right after a join the owner may transiently refuse (settling
    // membership, isolation check) — the same window the owner-death test
    // tolerates. A `Claimed` outcome stays fatal: that would mean the joined
    // fleet genuinely does not know the recording.
    let store_j = store_on(&members[2]);
    let deadline = Instant::now() + CONVERGE;
    loop {
        match try_claim_raw(&store_j, TEST_PORT, &s).await {
            Ok(ClaimOutcome::AlreadyRecorded) => break,
            Ok(ClaimOutcome::Claimed(_)) => {
                panic!("the joined fleet does not know the recording")
            }
            Ok(ClaimOutcome::InFlight) | Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "joiner never answered AlreadyRecorded"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    let replay = lookup(&store_j, TEST_PORT, &s).await.expect("replayable");
    assert_eq!(replay.body, b"before-the-join".to_vec());
}

// ---------------------------------------------------------------------------
// AC10 — the committed clear (`ControlOp::ProxyRecordedClear`, what
// `DELETE .../savedProxyResponses` terminates into at the front door) deletes
// the recorded markers fleet-wide — every node's caches retire against the
// applied state with no fan-out — and the signature records afresh.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clear_deletes_recorded_markers_fleet_wide() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(2, Duration::from_secs(30)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;
    let s = sig("/cleared");

    let store = store_on(&members[0]);
    let ClaimOutcome::Claimed(token) = claim(&store, TEST_PORT, &s).await else {
        panic!("first claim wins");
    };
    complete_recorded(&store, TEST_PORT, &s, token, "will-be-cleared")
        .await
        .expect("recording lands");

    // Submitted from the *other* member, like any front-door write would be —
    // the clear's effect must not depend on which node accepted it.
    members[1]
        .node
        .submit(mint(rift_cluster::ControlOp::ProxyRecordedClear {
            tenant: TenantId::new("default"),
            port: TEST_PORT,
        }))
        .await
        .expect("the clear commits");

    // Cleared fleet-wide: both nodes grant a fresh claim again.
    let deadline = Instant::now() + CONVERGE;
    loop {
        let mut cleared = true;
        for member in &members {
            cleared &= lookup(&store_on(member), TEST_PORT, &s).await.is_none();
        }
        if cleared {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "markers never cleared fleet-wide"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ClaimOutcome::Claimed(fresh) = claim(&store, TEST_PORT, &s).await else {
        panic!("a cleared signature grants a fresh claim");
    };
    // And the fresh claim records again — the cleared state is fully re-usable,
    // not a half-cleared wedge.
    complete_recorded(&store, TEST_PORT, &s, fresh, "recorded-again")
        .await
        .expect("re-recording after a clear lands");
    let replay = lookup(&store, TEST_PORT, &s).await.expect("replayable");
    assert_eq!(replay.body, b"recorded-again".to_vec());
}

// ---------------------------------------------------------------------------
// AC7 — single-node fidelity: a 1-voter cluster still records exactly once
// with the store engaged.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_voter_cluster_records_exactly_once() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(1, Duration::from_secs(30)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;
    let s = sig("/solo");

    let store = store_on(&members[0]);
    let ClaimOutcome::Claimed(token) = claim(&store, TEST_PORT, &s).await else {
        panic!("first claim wins");
    };
    assert!(matches!(
        claim(&store, TEST_PORT, &s).await,
        ClaimOutcome::InFlight
    ));
    complete_recorded(&store, TEST_PORT, &s, token, "solo-recording")
        .await
        .expect("recording lands");
    assert!(matches!(
        claim(&store, TEST_PORT, &s).await,
        ClaimOutcome::AlreadyRecorded
    ));
    let replay = lookup(&store, TEST_PORT, &s).await.expect("replayable");
    assert_eq!(replay.body, b"solo-recording".to_vec());
}

// ---------------------------------------------------------------------------
// proxyAlways — publication merges into the existing recorded stub at apply
// (the #611 semantics), replicated to every node.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_always_merges_responses_fleet_wide() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(2, Duration::from_secs(30)).await;
    install_imposter(&members, TEST_PORT, "proxyAlways").await;
    let s = sig("/always");

    let store_a = store_on(&members[0]);
    let store_b = store_on(&members[1]);

    // proxyAlways never gates: every claim is granted (a formality token).
    let ClaimOutcome::Claimed(t1) = claim(&store_a, TEST_PORT, &s).await else {
        panic!("proxyAlways always grants");
    };
    let ClaimOutcome::Claimed(t2) = claim(&store_b, TEST_PORT, &s).await else {
        panic!("proxyAlways always grants concurrently");
    };

    let always = |store: &Arc<ClusterProxyStore>, token: ClaimToken, body: &str| {
        let store = Arc::clone(store);
        let s2 = s.clone();
        let stub = generated_stub(&s.path, body);
        let r = resp(body);
        let body = body.to_owned();
        async move {
            tokio::task::spawn_blocking(move || {
                store.complete_recorded(
                    TEST_PORT,
                    s2,
                    token,
                    r,
                    RecordedStub {
                        stub: Box::new(stub),
                        placement: RecordedStubPlacement::AfterProxyMerging,
                        proxy_to: "http://upstream.example".to_owned(),
                    },
                )
            })
            .await
            .expect("join")
            .unwrap_or_else(|e| panic!("publication of {body} commits: {e}"))
        }
    };
    always(&store_a, t1, "first-response").await;
    always(&store_b, t2, "second-response").await;

    // One merged stub after the proxy stub, carrying both responses, on
    // every node. Commit-ack proves the *leader* applied; followers apply
    // asynchronously, so the merged shape is polled to a deadline like every
    // other replicated read in this file.
    wait_stub_count(&members, TEST_PORT, 2).await;
    let deadline = Instant::now() + CONVERGE;
    loop {
        let merged = members.iter().all(|member| {
            let stubs = applied_stubs(member, TEST_PORT);
            stubs
                .get(1)
                .and_then(|recorded| recorded["responses"].as_array())
                .is_some_and(|responses| responses.len() == 2)
        });
        if merged {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "responses never merged fleet-wide: {:?}",
            members
                .iter()
                .map(|m| applied_stubs(m, TEST_PORT))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------
// Before bind, the store fails loud — no silent local fallback.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unbound_store_fails_loud() {
    let net = ProxyNet::new();
    let store = Arc::new(ClusterProxyStore::new(net));
    let s = sig("/too-early");
    let outcome = tokio::task::spawn_blocking(move || store.try_claim(TEST_PORT, &s))
        .await
        .expect("join");
    assert!(
        outcome.is_err(),
        "an unbound store must answer Unavailable, not pretend: {outcome:?}"
    );
}
