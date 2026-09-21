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
    RecordedStubPlacement,
};
use rift_cluster_base::seams::{
    BackendUnavailable, ClaimOutcome, ClaimToken, ImposterConfig, ProxyRecordingStore,
    ProxyStoreError, RecordedResponse, RequestSignature, Stub,
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
            config: Box::new(proxy_imposter(port, mode)),
        }))
        .await
        .expect("imposter commits");
    let deadline = Instant::now() + CONVERGE;
    loop {
        let applied = members
            .iter()
            .all(|m| m.node.get_imposter(port).ok().flatten().is_some());
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
    sigs_owned_by(members, port, want, 1, "/owned/").remove(0)
}

/// `n` distinct signatures the ring homes on `want`, probed through the production key renderer
/// so a test can never disagree with the store about ownership. Callers that release from a
/// *different* node than `want` are exercising the RPC hop rather than the owner-local map, which
/// is where a bridge call could block or panic.
fn sigs_owned_by(
    members: &[ProxyMember],
    port: u16,
    want: usize,
    n: usize,
    prefix: &str,
) -> Vec<RequestSignature> {
    let mut found = Vec::new();
    for i in 0..5000 {
        let candidate = sig(&format!("{prefix}{i}"));
        if owner_index(members, port, &candidate) == want {
            found.push(candidate);
            if found.len() == n {
                return found;
            }
        }
    }
    panic!("fewer than {n} signature(s) landed on member {want} in 5000 probes");
}

/// Config stubs a member's applied state carries for `TEST_PORT`.
fn applied_stubs(member: &ProxyMember, port: u16) -> Vec<serde_json::Value> {
    let raw = member
        .node
        .get_imposter(port)
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
// Pins D-40: N concurrent first hits yield one claim, one upstream call and one committed
// `ProxyRecorded` — marker and stub together — replayed from every node.
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
// Pins D-40: Pending supports release — a failed upstream call frees the signature at once,
// which a grow-only replicated claim set could not express.
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
// Pins D-40: Pending is owner-local and dies with its owner; the signature is re-claimable at
// the new owner once membership settles.
// Pins D-66: while it settles, the new owner *refuses* — the re-claim loop below accepts only
// `Refused`, so a regression to the old degrade fails here rather than looking like patience.
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
            Ok(ClaimOutcome::InFlight) | Err(ProxyStoreError::Refused(_)) => {
                // The new owner may briefly refuse while isolated/settling — but only with the
                // refusing error (D-66). Any other `Err` shape falls through to the panic below.
                assert!(Instant::now() < deadline, "signature never re-claimable");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(other) => panic!("a settling owner must refuse, not degrade: {other:?}"),
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
// Pins D-40: the claim TTL is a fixed deadline; an expired token is a stale fence that cannot
// misattribute a recording after re-claim.
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
            && joiner.node.get_imposter(TEST_PORT).ok().flatten().is_some()
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
/// Pins D-66: an unbound store refuses, so a node that has not joined yet answers 503 rather
/// than proxying to the real upstream un-arbitrated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unbound_store_fails_loud() {
    let net = ProxyNet::new();
    let store = Arc::new(ClusterProxyStore::new(net));
    let s = sig("/too-early");
    let outcome = tokio::task::spawn_blocking(move || store.try_claim(TEST_PORT, &s))
        .await
        .expect("join");
    // Pins D-66: not merely "an error" — the *refusing* error. A bare `is_err()` would pass on
    // the old `Unavailable`, which the engine answers by forwarding to the upstream without a
    // claim; that is exactly the behaviour this issue exists to remove.
    assert!(
        matches!(outcome, Err(ProxyStoreError::Refused(_))),
        "an unbound store must refuse the claim, not degrade: {outcome:?}"
    );
}

// ---------------------------------------------------------------------------
// D-66 — the cluster cannot serialize the claim, so the request is refused
// (503 at the data plane), never forwarded.
// ---------------------------------------------------------------------------

/// Extract the refusal, asserting its shape. The `feature` is what the data plane puts in the
/// 503 body's `feature` field, so it is part of the contract, not a log string.
fn expect_refusal(
    outcome: Result<ClaimOutcome, ProxyStoreError>,
    what: &str,
) -> BackendUnavailable {
    match outcome {
        Err(ProxyStoreError::Refused(b)) => {
            assert_eq!(
                b.feature, "proxyOnce",
                "{what}: the 503 must name the feature that refused"
            );
            b
        }
        other => {
            panic!("{what}: must refuse the claim (D-66), not degrade into a forward: {other:?}")
        }
    }
}

/// Pins D-66 on the D-17 isolation refusal. `owner_claim` has always failed closed while
/// partitioned — it returns `ClaimReply::Error { "owner is isolated" }` — but that refusal was
/// flattened to `ProxyStoreError::Unavailable` at the store face, which upstream's engine answers
/// by **forwarding to the real upstream without a claim**. With nothing serializing claims, every
/// request for the duration of the partition would reach the upstream: the unbounded duplicate
/// Ch. 9's row exists to prevent. It must now carry `BackendUnavailable`, which the data plane
/// answers 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_owner_refuses_the_claim() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(3, Duration::from_secs(30)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;

    // A signature this node owns, so the claim is answered locally by `owner_claim` — the
    // isolation gate under test — rather than forwarded.
    let owner_ix = 0;
    let s = sig_owned_by(&members, TEST_PORT, owner_ix);

    // Partition it. Both peers go, so the owner loses quorum *and* has no leader left to hear:
    // a follower that can still see a leader never isolates.
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

    let before = counter("rift_cluster_proxy_claims_total", ("outcome", "refused"));
    let refusal = expect_refusal(
        try_claim_raw(&store_on(&members[owner_ix]), TEST_PORT, &s).await,
        "an isolated owner",
    );
    assert!(
        refusal.detail.contains("isolated"),
        "the refusal must name isolation rather than some other failure — a bare `Refused` \
         would pass on a timeout or a decode error alike: {}",
        refusal.detail
    );
    assert_eq!(
        counter("rift_cluster_proxy_claims_total", ("outcome", "refused")) - before,
        1,
        "every refusal is counted exactly once — the operator's reading of this condition"
    );
}

/// Pins D-66 on the row RFC-001 §7.6 names outright — *owner-unreachable* — and on Ch. 9's
/// "fast-fail". The owner dies without leaving, so the ring still names it and a survivor
/// forwards into a corpse; that liveness failure must refuse rather than degrade, and must do so
/// inside the claim deadline rather than hanging the data plane.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_through_a_survivor_of_a_dead_owner_is_refused_fast() {
    let _lock = TEST_LOCK.lock().await;
    let mut members = proxy_cluster_of(3, Duration::from_secs(30)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;

    // Owned by member 2, claimed from member 0: an RPC claim, not a local one.
    let s = sig_owned_by(&members, TEST_PORT, 2);
    let owner = members.remove(2);
    owner.node.shutdown().await.ok();

    // Two of three remain, so the survivors keep quorum and are *not* isolated: the only thing
    // wrong is that the owner is unreachable. That is the row under test.
    assert!(
        !members[0].node.is_isolated(),
        "the survivor must not be isolated — otherwise this test proves the isolation row again"
    );

    let started = Instant::now();
    let refusal = expect_refusal(
        try_claim_raw(&store_on(&members[0]), TEST_PORT, &s).await,
        "a survivor of a dead owner",
    );
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "Ch. 9 promises a *fast-fail* 503; this took {elapsed:?}"
    );
    assert!(
        !refusal.detail.is_empty(),
        "the refusal must carry the reason into the 503 body — the runbook signal D-61 chose"
    );
}

/// Pins D-66's scope: only `proxyOnce` arbitrates, so only `proxyOnce` refuses. `proxyAlways`
/// and `proxyTransparent` never gate — their claim is a formality — and making them fail closed
/// would take a whole imposter offline for a partition that costs them nothing. An
/// implementation that refused in `try_claim` before consulting the mode passes every other test
/// in this file and fails this one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_always_never_refuses_on_an_isolated_owner() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(3, Duration::from_secs(30)).await;
    let always_port = TEST_PORT + 1;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;
    install_imposter(&members, always_port, "proxyAlways").await;

    let owner_ix = 0;
    let once_sig = sig_owned_by(&members, TEST_PORT, owner_ix);
    for (ix, member) in members.iter().enumerate() {
        if ix != owner_ix {
            member.node.shutdown().await.expect("shutdown peer");
        }
    }
    let deadline = Instant::now() + CONVERGE;
    while !members[owner_ix].node.is_isolated() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(members[owner_ix].node.is_isolated(), "owner must isolate");

    let store = store_on(&members[owner_ix]);
    // The control: on the same node, in the same partition, proxyOnce refuses.
    expect_refusal(
        try_claim_raw(&store, TEST_PORT, &once_sig).await,
        "proxyOnce on the isolated owner",
    );
    // The claim under test: proxyAlways grants regardless.
    assert!(
        matches!(
            try_claim_raw(&store, always_port, &sig("/always-partitioned")).await,
            Ok(ClaimOutcome::Claimed(_))
        ),
        "proxyAlways gates nothing, so a partition must not stop it recording"
    );
}

// ---------------------------------------------------------------------------
// #629 — the release seam is reached from a destructor (rift#1193, PR #1197).
// ---------------------------------------------------------------------------

/// The guard rift#1197 wraps a won claim in, reproduced rather than imported: upstream's is
/// private to `rift-mock-core`, so a test has to own one. Upstream's borrows the request-scoped
/// signature; this one owns store and signature so it can move into a spawned task. The property
/// under test is shared by both: the claim is given back from `Drop`.
struct HeldClaim {
    store: Arc<ClusterProxyStore>,
    port: u16,
    sig: RequestSignature,
    /// Taken by `Drop`. Upstream's guard also clears it in `settle()`; this replica has no settle
    /// path, so the already-taken arm is unreachable here and exists to mirror the shape.
    token: Option<ClaimToken>,
}

impl HeldClaim {
    fn new(
        store: &Arc<ClusterProxyStore>,
        port: u16,
        sig: &RequestSignature,
        token: ClaimToken,
    ) -> Self {
        Self {
            store: Arc::clone(store),
            port,
            sig: sig.clone(),
            token: Some(token),
        }
    }
}

impl Drop for HeldClaim {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.store.release_claim(self.port, &self.sig, token);
        }
    }
}

/// Wait for the owner's claim table to drop to empty. The cluster's claim TTL is set far above
/// this bound, so an expiry cannot be what satisfies it — only a release can. Generous against a
/// loaded CI runner: `release_claim` makes a single un-retried bridge call, so one transient 2 s
/// timeout must not read as a failure to release.
async fn await_released(member: &ProxyMember, port: u16, phase: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if member.net.pending_claims(port) == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{phase}: the owner still holds the claim 5s after the guard was dropped"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Pins D-90: a claim released from a destructor really is freed, in each of the three shapes
/// rift#1197's guard produces — a task aborted mid-await (imposter stop), a panicking handler,
/// and a data-plane runtime torn down with the request still parked. What would falsify it: a
/// `Bridge::call` that waits the async way (`block_on`/`blocking_recv`), since the first two
/// drops run with a runtime entered, or any arm that panics while unwinding.
///
/// The claim TTL is 300 s against a 10 s window, so no expiry can stand in for a release, and the
/// owner is always the *other* node, so every release is a real RPC hop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_is_released_when_its_guard_is_dropped() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(2, Duration::from_secs(300)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;

    const OWNER: usize = 1;
    let store = store_on(&members[0]);
    let sigs = sigs_owned_by(&members, TEST_PORT, OWNER, 3, "/dropped/");

    // -- phase 1: the request task is aborted mid-await (an imposter stop) ---
    let ClaimOutcome::Claimed(token) = claim(&store, TEST_PORT, &sigs[0]).await else {
        panic!("phase 1: the first claim on a fresh signature must win");
    };
    assert_eq!(
        members[OWNER].net.pending_claims(TEST_PORT),
        1,
        "phase 1: the owner records the claim before the guard is dropped"
    );
    let guard = HeldClaim::new(&store, TEST_PORT, &sigs[0], token);
    let parked = tokio::spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    parked.abort();
    // Bounded, because the release runs inside the task's drop glue: a regression that made the
    // bridge park unbounded would hang here until CI's job timeout instead of failing legibly.
    let ended = tokio::time::timeout(Duration::from_secs(30), parked)
        .await
        .expect("phase 1: the aborted task's drop must not hang");
    assert!(
        ended.expect_err("the task was aborted").is_cancelled(),
        "phase 1: the task must end by abort, so the guard drops without a return"
    );
    await_released(&members[OWNER], TEST_PORT, "phase 1 (task abort)").await;

    // -- phase 2: the handler panics; the guard drops mid-unwind ------------
    // A panic inside `release_claim` here would be a *second* panic while unwinding, which
    // aborts the process — so this phase fails by killing the test binary, not by assertion.
    let ClaimOutcome::Claimed(token) = claim(&store, TEST_PORT, &sigs[1]).await else {
        panic!("phase 2: the first claim on a fresh signature must win");
    };
    let guard = HeldClaim::new(&store, TEST_PORT, &sigs[1], token);
    let panicked = tokio::spawn(async move {
        let _guard = guard;
        panic!("the handler panicked mid-request");
    });
    let ended = tokio::time::timeout(Duration::from_secs(30), panicked)
        .await
        .expect("phase 2: the panicking task's drop must not hang");
    assert!(
        ended.expect_err("the task panicked").is_panic(),
        "phase 2: the task must end by panic, so the guard drops while unwinding"
    );
    await_released(&members[OWNER], TEST_PORT, "phase 2 (panic unwind)").await;

    // -- phase 3: the data-plane runtime is dropped, request still parked ---
    // The guard's release rides the *bridge's* private cluster-io runtime, not the one being
    // torn down, which is why it can still reach the owner from inside this shutdown.
    let ClaimOutcome::Claimed(token) = claim(&store, TEST_PORT, &sigs[2]).await else {
        panic!("phase 3: the first claim on a fresh signature must win");
    };
    let guard = HeldClaim::new(&store, TEST_PORT, &sigs[2], token);
    // Two layers, each load-bearing: the inner `std::thread` is a pristine thread with no runtime
    // context, because dropping a runtime from inside an async context panics; the outer
    // `spawn_blocking` keeps the join off this test's async workers, as every other blocking
    // hand-off in this file does.
    tokio::task::spawn_blocking(move || {
        std::thread::spawn(move || {
            let data_plane = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("a data-plane runtime starts");
            data_plane.spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
            std::thread::sleep(Duration::from_millis(50));
            // Dropping the runtime drops the parked task, and with it the guard.
            drop(data_plane);
        })
        .join()
        .expect("the data-plane runtime shut down without panicking");
    })
    .await
    .expect("phase 3: the runtime teardown must not hang");
    await_released(&members[OWNER], TEST_PORT, "phase 3 (runtime shutdown)").await;

    // The releases were effective, not merely quiet: the first signature is claimable again, and
    // the claim that grants proves the owner's table really is free.
    assert!(
        matches!(
            claim(&store, TEST_PORT, &sigs[0]).await,
            ClaimOutcome::Claimed(_)
        ),
        "a released signature must be immediately re-claimable"
    );
}

/// The other half of #629's question: the release also runs when an imposter has been deleted or
/// the node is mid-shutdown, where the port's identity no longer resolves. That arm must warn and
/// return.
///
/// **The assertion is that the process survives.** A panic in that arm would be a second panic
/// while unwinding, which aborts — so this test fails by killing the test binary, and there is
/// deliberately no `pending_claims` check afterwards: the guard's port never held a claim, so any
/// such assertion would read as coverage while being unable to fail. Mutating the arm to `panic!`
/// turns this red (SIGABRT), which is what shows it discriminates.
///
/// A bogus token on a port with no imposter reaches the same arm as a deleted imposter does,
/// without waiting out `MODE_CACHE_TTL` for the mode cache to go cold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guard_drops_cleanly_when_the_port_identity_is_gone() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(2, Duration::from_secs(300)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;

    let store = store_on(&members[0]);
    let orphan = HeldClaim::new(
        &store,
        TEST_PORT + 7,
        &sig("/never-had-an-imposter"),
        ClaimToken::new(1),
    );
    let panicked = tokio::spawn(async move {
        let _orphan = orphan;
        panic!("the handler panicked with no applied imposter on the port");
    });
    let ended = tokio::time::timeout(Duration::from_secs(30), panicked)
        .await
        .expect("the orphan guard's drop must not hang");
    assert!(
        ended.expect_err("the task panicked").is_panic(),
        "the guard must drop while unwinding, with the port identity unresolvable"
    );
}

/// Pins the half of D-90 the phases above cannot reach: the release is **synchronous**. Every
/// assertion there polls, so a fire-and-forget implementation — the one D-90 rejects, and the one
/// upstream's SPI recommends for a store that releases over the network — would satisfy all of
/// them. Here the guard is dropped on a blocking thread and the owner's table is read the instant
/// `drop` returns, with no polling: a release handed to the bridge instead of waited on would
/// still be in flight at that point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_release_reaches_the_owner_before_the_drop_returns() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(2, Duration::from_secs(300)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;

    const OWNER: usize = 1;
    let store = store_on(&members[0]);
    let s = sigs_owned_by(&members, TEST_PORT, OWNER, 1, "/synchronous/").remove(0);

    let ClaimOutcome::Claimed(token) = claim(&store, TEST_PORT, &s).await else {
        panic!("the first claim on a fresh signature must win");
    };
    assert_eq!(members[OWNER].net.pending_claims(TEST_PORT), 1);

    let guard = HeldClaim::new(&store, TEST_PORT, &s, token);
    tokio::task::spawn_blocking(move || drop(guard))
        .await
        .expect("the guard dropped");

    assert_eq!(
        members[OWNER].net.pending_claims(TEST_PORT),
        0,
        "the owner's claim must already be gone when the drop returns — a handed-off release \
         would still be travelling to the owner here"
    );
}

/// The clustered counterpart of upstream's `a_stale_guard_does_not_release_a_newer_claim`: a
/// guard dropped after its claim expired and was re-taken must not free the new holder.
///
/// `owner_release`'s token check is what refuses it, and **nothing reached that check before**.
/// The file's only other `release_claim` call passes a live token, and the stale-token test
/// beside it drives `complete_recorded`, which is `owner_complete`'s separate guard. rift#1197
/// makes this reachable in production: a slow request whose claim ages out still drops its guard
/// eventually, by which time the signature may belong to someone else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_guard_drop_does_not_free_the_current_claim() {
    let _lock = TEST_LOCK.lock().await;
    let members = proxy_cluster_of(2, Duration::from_millis(300)).await;
    install_imposter(&members, TEST_PORT, "proxyOnce").await;

    const OWNER: usize = 1;
    let store = store_on(&members[0]);
    let s = sigs_owned_by(&members, TEST_PORT, OWNER, 1, "/stale-guard/").remove(0);

    // Win a claim, then let it age past the 300 ms TTL so the signature frees itself and the
    // first token goes stale — the abandoned winner still holds its guard.
    let ClaimOutcome::Claimed(stale) = claim(&store, TEST_PORT, &s).await else {
        panic!("the first claim on a fresh signature must win");
    };
    tokio::time::sleep(Duration::from_millis(600)).await;
    let ClaimOutcome::Claimed(fresh) = claim(&store, TEST_PORT, &s).await else {
        panic!("an expired claim leaves the signature re-claimable");
    };
    assert_eq!(members[OWNER].net.pending_claims(TEST_PORT), 1);

    // The abandoned request finally drops its guard, carrying the stale token.
    let stale_guard = HeldClaim::new(&store, TEST_PORT, &s, stale);
    tokio::task::spawn_blocking(move || drop(stale_guard))
        .await
        .expect("the stale guard dropped");

    assert_eq!(
        members[OWNER].net.pending_claims(TEST_PORT),
        1,
        "a stale guard must not free the claim that replaced it"
    );
    complete_recorded(&store, TEST_PORT, &s, fresh, "recorded-by-the-live-holder")
        .await
        .expect("the live claim still settles after a stale guard dropped");
}
