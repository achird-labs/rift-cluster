//! The clustered composition end to end (issue #10 AC2/AC3): bootstrap and
//! seed-join, the SIGTERM graceful leave, the post-start intercept refusal, and
//! the guarantee that a failed start leaves nothing bound.
//!
//! These drive `compose::start` itself rather than its parts, because every bug
//! they exist to catch lives in the wiring: probe-before-node ordering, the
//! drain-before-close sequence, and cleanup on the error paths.

use std::time::Duration;

use clap::Parser;
use rift_ee_server::cli::EeCli;
use rift_ee_server::compose;
use tempfile::TempDir;

const SECRET: &str = "clustered-test-secret";

/// A clustered invocation. Every port is ephemeral unless `bind` or `probe_bind`
/// pins one, so tests never collide.
fn cluster_on(state: &TempDir, bind: &str, probe_bind: &str, extra: &[&str]) -> EeCli {
    let mut args = vec![
        "rift-ee-server".to_owned(),
        "--port".to_owned(),
        "0".to_owned(),
        "--metrics-port".to_owned(),
        "0".to_owned(),
        "--cluster".to_owned(),
        "--cluster-bind".to_owned(),
        bind.to_owned(),
        "--cluster-probe-bind".to_owned(),
        probe_bind.to_owned(),
        "--cluster-secret".to_owned(),
        SECRET.to_owned(),
        "--cluster-state-dir".to_owned(),
        state.path().to_string_lossy().into_owned(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

/// The common case: every port ephemeral.
fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    cluster_on(state, "127.0.0.1:0", "127.0.0.1:0", extra)
}

/// An address that was bound and released — free right now, and concrete enough
/// to hand to a listener that has not started yet.
fn reserve_port() -> String {
    let held = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    held.local_addr().expect("addr").to_string()
}

/// Poll `node`'s membership until it holds exactly `want` voters, bounded.
async fn wait_voter_count(node: &rift_cluster::RaftNode, want: usize, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let voters = node.status().voters;
        if voters.len() == want {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: expected {want} voters, saw {voters:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `/readyz` until it answers 200, bounded.
async fn wait_ready(probes: &str, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = reqwest::get(format!("http://{probes}/readyz")).await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: /readyz never reached 200"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn probe(base: &str, path: &str) -> (u16, serde_json::Value) {
    let response = reqwest::get(format!("http://{base}{path}"))
        .await
        .expect("probe request");
    let status = response.status().as_u16();
    (status, response.json().await.expect("probe body is json"))
}

/// Poll `path` until the listener is accepting, bounded. The probe port comes up
/// concurrently with the caller, so a single early request would race it.
async fn poll_until_bound(base: &str, path: &str) -> (u16, serde_json::Value) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = reqwest::get(format!("http://{base}{path}")).await {
            let status = response.status().as_u16();
            return (status, response.json().await.expect("probe body is json"));
        }
        assert!(
            std::time::Instant::now() < deadline,
            "probe listener never came up on {base}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn allow_solo_bootstraps_a_single_node_cluster_and_opens_the_gate() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");

    let probes = server.probe_addr().expect("probes bound under --cluster");
    assert!(server.cluster_addr().is_some());

    // The reconcile gate opens asynchronously just after start, so poll.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = probe(&probes.to_string(), "/readyz").await;
        if status == 200 {
            assert_eq!(body["pending"], serde_json::json!([]));
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a bootstrapped solo node must become ready: {body}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    server.shutdown().await;
}

#[tokio::test]
async fn no_seeds_and_no_allow_solo_is_refused_rather_than_founding_a_second_cluster() {
    let state = TempDir::new().expect("tempdir");
    let err = match compose::start(cluster_cli(&state, &[])).await {
        Ok(_) => panic!("a seedless node without --cluster-allow-solo must not start"),
        Err(e) => format!("{e:#}"),
    };
    assert!(err.contains("--cluster-allow-solo"), "{err}");
}

#[tokio::test]
async fn unreachable_seeds_fail_startup_naming_every_seed_tried() {
    let state = TempDir::new().expect("tempdir");
    // Port 1 on loopback is reserved and never listening, so this exercises the
    // join-failure path without depending on anything in the environment.
    let cli = cluster_cli(&state, &["--cluster-seeds", "127.0.0.1:1,127.0.0.1:2"]);
    let err = match compose::start(cli).await {
        Ok(_) => panic!("unreachable seeds must not produce a running node"),
        Err(e) => format!("{e:#}"),
    };
    assert!(err.contains("127.0.0.1:1"), "{err}");
    assert!(err.contains("127.0.0.1:2"), "{err}");
}

/// The regression test for the `Arc` cycle: with a strong handle in the operator
/// surface's slot, `RaftNode::Drop` never ran, so a failed start left the cluster
/// port bound and the redb state dir locked — and the retry that `start` exists
/// to support failed with an error that hid the real cause.
#[tokio::test]
async fn a_failed_start_releases_the_cluster_port_and_the_state_dir() {
    let state = TempDir::new().expect("tempdir");
    // A fixed cluster port, so the retry provably rebinds the same address.
    let bind = reserve_port();

    let failing = cluster_on(
        &state,
        &bind,
        "127.0.0.1:0",
        &["--cluster-seeds", "127.0.0.1:1"],
    );
    assert!(
        compose::start(failing).await.is_err(),
        "the unreachable seed must fail this start"
    );

    // Same state dir, same cluster port: both must be free again.
    let server = compose::start(cluster_on(
        &state,
        &bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("a retry after a failed start must not be blocked by the failed one");
    server.shutdown().await;
}

/// The config-file spelling of intercept, which the CLI-level guard cannot see:
/// it is only known once the open-source builder has loaded the config.
#[tokio::test]
async fn an_intercept_config_block_is_refused_after_the_config_loads() {
    let state = TempDir::new().expect("tempdir");
    let configfile = state.path().join("imposters.json");
    std::fs::write(
        &configfile,
        serde_json::json!({
            "imposters": [],
            "intercept": { "port": 0 }
        })
        .to_string(),
    )
    .expect("write config");

    let cli = cluster_cli(
        &state,
        &[
            "--cluster-allow-solo",
            "--configfile",
            &configfile.to_string_lossy(),
        ],
    );
    let err = match compose::start(cli).await {
        Ok(_) => panic!("an intercept block must not start under --cluster"),
        Err(e) => format!("{e:#}"),
    };
    assert!(err.to_lowercase().contains("intercept"), "{err}");
}

/// AC3's ordering guarantee: readiness fails *first*, so the balancer sheds this
/// node before any socket closes. Closing first would turn every in-flight
/// request into a client-visible error.
#[tokio::test]
async fn sigterm_fails_readiness_before_closing_any_listener() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(
        &state,
        &["--cluster-allow-solo", "--cluster-leave-timeout", "2"],
    ))
    .await
    .expect("solo cluster starts");
    let probes = server.probe_addr().expect("probes bound").to_string();
    let admin = format!("http://{}", server.admin_addr());

    assert_eq!(probe(&probes, "/readyz").await.0, 200);

    let (leave_tx, leave_rx) = tokio::sync::oneshot::channel::<()>();
    let leaving = tokio::spawn(server.serve_until(async move {
        let _ = leave_rx.await;
    }));

    leave_tx.send(()).expect("trigger the leave");

    // Within the drain window: not-ready, but everything still serving.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (status, body) = probe(&probes, "/readyz").await;
    assert_eq!(status, 503);
    assert_eq!(body["status"], "draining");
    assert_eq!(
        probe(&probes, "/healthz").await.0,
        200,
        "liveness must stay up through the drain or the orchestrator kills it mid-leave"
    );
    assert!(
        reqwest::get(format!("{admin}/imposters"))
            .await
            .is_ok_and(|r| r.status().is_success()),
        "the admin plane must keep serving until the drain window elapses"
    );

    tokio::time::timeout(Duration::from_secs(15), leaving)
        .await
        .expect("the leave completes")
        .expect("the leave task did not panic")
        .expect("a signal-driven leave is not an admin-plane failure");

    // And only then are the listeners actually gone.
    assert!(
        reqwest::get(format!("http://{probes}/healthz"))
            .await
            .is_err(),
        "the probe port must be released once the leave completes"
    );
}

/// The probe listener must be up *before* the node joins, not after.
///
/// Binding it last made `/healthz` connection-refused for the whole convergence
/// window, so a Kubernetes `livenessProbe` would restart the pod mid-join — the
/// exact failure the liveness/readiness split exists to prevent. It also made
/// the "closed until proven open" latch unobservable, because the only gate was
/// already satisfied by the time the socket existed.
#[tokio::test]
async fn probes_answer_while_the_node_is_still_joining() {
    let state = TempDir::new().expect("tempdir");
    // A concrete probe address, so it can be polled while start() is still
    // running — an ephemeral one is only knowable after start() returns, which
    // is exactly the window under test.
    let probe_bind = reserve_port();

    // Seeds that never answer keep this node in its join window for the whole
    // test, which is precisely when the probes have to be reachable.
    let cli = cluster_on(
        &state,
        "127.0.0.1:0",
        &probe_bind,
        &["--cluster-seeds", "127.0.0.1:1"],
    );
    let starting = tokio::spawn(compose::start(cli));

    // Mid-join: liveness answers, and readiness correctly reports the gate that
    // has not reported in yet.
    let (status, body) = poll_until_bound(&probe_bind, "/healthz").await;
    assert_eq!(status, 200, "liveness during join: {body}");

    let (status, body) = probe(&probe_bind, "/readyz").await;
    assert_eq!(status, 503, "the node has not joined yet: {body}");
    assert_eq!(body["status"], "not-ready");
    assert_eq!(
        body["pending"],
        serde_json::json!(["cluster-joined", "cluster-reconciled"])
    );

    assert!(
        !starting.is_finished(),
        "the seed retry must still be running at this point"
    );

    let err = match tokio::time::timeout(Duration::from_secs(60), starting)
        .await
        .expect("start resolves")
        .expect("start task did not panic")
    {
        Ok(_) => panic!("unreachable seeds must not produce a running node"),
        Err(e) => format!("{e:#}"),
    };
    assert!(
        err.contains("127.0.0.1:1"),
        "the failure must be the seed, not the probe listener: {err}"
    );
}

/// Issue #6 and #69: the SIGTERM path must actually leave the Raft membership —
/// and must stop doing so once the fleet has no voter to spare.
///
/// Every other test here runs `--cluster-allow-solo`, and a sole voter cannot
/// leave (openraft refuses to empty the voter set), so `graceful_leave` skips
/// the departure entirely on those — a no-op regression in the wiring would
/// leave them all green. This is the only test that puts real peers behind the
/// leave and reads the survivor's membership afterwards.
///
/// Three nodes, so both halves are visible in one run: the first departure is
/// above the floor and lands, the second would drop the fleet to a single voter
/// and is refused. Two nodes could only ever show the refusal, and would leave
/// the wiring that performs a real departure untested.
#[tokio::test]
async fn graceful_leave_removes_this_node_until_the_voter_floor_stops_it() {
    let founder_state = TempDir::new().expect("tempdir");
    let first_state = TempDir::new().expect("tempdir");
    let second_state = TempDir::new().expect("tempdir");
    let founder_bind = reserve_port();

    let founder = compose::start(cluster_on(
        &founder_state,
        &founder_bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("founder starts");

    let joiner_args = [
        "--cluster-seeds",
        &founder_bind,
        "--cluster-leave-timeout",
        "5",
    ];
    let first = compose::start(cluster_on(
        &first_state,
        &reserve_port(),
        "127.0.0.1:0",
        &joiner_args,
    ))
    .await
    .expect("first joiner starts");
    let second = compose::start(cluster_on(
        &second_state,
        &reserve_port(),
        "127.0.0.1:0",
        &joiner_args,
    ))
    .await
    .expect("second joiner starts");

    let founder_node = founder.node().expect("founder is clustered").clone();
    let first_id = first.node().expect("first is clustered").id();
    let second_id = second.node().expect("second is clustered").id();
    wait_voter_count(
        &founder_node,
        3,
        "all three must be voters before the leave",
    )
    .await;

    // Above the floor: this one really departs.
    first.graceful_leave().await;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let voters = founder_node.status().voters;
        if !voters.contains(&first_id) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "SIGTERM did not remove the departing node from the membership: {voters:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // At the floor: this one drains and exits, but keeps its vote, so the
    // membership the survivors hold still names it and a cold start can form a
    // quorum without it having to come back first.
    second.graceful_leave().await;
    let voters = founder_node.status().voters;
    assert!(
        voters.contains(&second_id),
        "the voter floor must keep the last departing node in the membership: {voters:?}"
    );
    assert_eq!(
        voters.len(),
        2,
        "the fleet must stop at two voters, not walk to one: {voters:?}"
    );

    founder.shutdown().await;
}

/// Issue #72: a node that gracefully left must come back when it restarts on its
/// **retained** state directory — the rolling-restart shape, since a Docker
/// volume or a k8s PVC outlives the container.
///
/// Three nodes, not two, so the departure lands with a voter to spare: issue #69
/// adds a floor that refuses a leave which would drop the fleet below two
/// voters, and this test must keep proving the rejoin after that lands.
///
/// It also pins the marker's on-disk name, which
/// `a_departed_node_without_seeds_fails_startup_with_guidance` relies on.
#[tokio::test]
async fn departed_node_with_retained_state_dir_rejoins_on_restart() {
    let founder_state = TempDir::new().expect("tempdir");
    let keeper_state = TempDir::new().expect("tempdir");
    let roller_state = TempDir::new().expect("tempdir");
    let founder_bind = reserve_port();
    let roller_bind = reserve_port();

    let founder = compose::start(cluster_on(
        &founder_state,
        &founder_bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("founder starts");
    let keeper = compose::start(cluster_on(
        &keeper_state,
        &reserve_port(),
        "127.0.0.1:0",
        &["--cluster-seeds", &founder_bind],
    ))
    .await
    .expect("keeper starts");
    let roller = compose::start(cluster_on(
        &roller_state,
        &roller_bind,
        "127.0.0.1:0",
        &[
            "--cluster-seeds",
            &founder_bind,
            "--cluster-leave-timeout",
            "5",
        ],
    ))
    .await
    .expect("roller starts");

    let founder_node = founder.node().expect("founder is clustered").clone();
    let roller_id = roller.node().expect("roller is clustered").id();
    wait_voter_count(
        &founder_node,
        3,
        "all three must be voters before the leave",
    )
    .await;

    roller.graceful_leave().await;
    wait_voter_count(
        &founder_node,
        2,
        "the graceful leave must shrink the membership",
    )
    .await;

    let marker = roller_state.path().join("departed");
    assert!(
        marker.exists(),
        "a confirmed departure must leave a durable marker in the state dir"
    );

    // Same state directory, same address: the pod came back, its volume intact.
    let rejoined = compose::start(cluster_on(
        &roller_state,
        &roller_bind,
        "127.0.0.1:0",
        &["--cluster-seeds", &founder_bind],
    ))
    .await
    .expect("a departed node must start and rejoin on its retained state dir");

    // Nothing was wiped: the node kept its minted identity, so its parked
    // intents and applied state came back with it.
    assert_eq!(
        rejoined.node().expect("rejoined is clustered").id(),
        roller_id,
        "the rejoin must reuse the node's identity, not mint a new one"
    );

    let probes = rejoined.probe_addr().expect("probes bound").to_string();
    wait_ready(&probes, "the rejoined node").await;
    wait_voter_count(&founder_node, 3, "the rejoined node must be a voter again").await;
    assert!(
        !marker.exists(),
        "the marker must be cleared once the rejoin succeeds, or every later restart re-joins"
    );

    rejoined.shutdown().await;
    keeper.shutdown().await;
    founder.shutdown().await;
}

/// Issue #72 AC4: the fix must not break the path it is carving around.
///
/// A member that stopped **without** leaving still owns its place in the
/// membership, so it has to resume from its durable log — that is how a whole
/// fleet cold-starts. Restarting it with no seeds *and* no `--cluster-allow-solo`
/// makes the assertion sharp: the resume row is the only row that can start this
/// node at all, so a regression that sent it down the seed-join path would fail
/// here rather than pass quietly.
#[tokio::test]
async fn cold_start_resumes_from_durable_state_without_seed_joining() {
    let state = TempDir::new().expect("tempdir");
    let bind = reserve_port();

    let first = compose::start(cluster_on(
        &state,
        &bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("solo cluster starts");
    let probes = first.probe_addr().expect("probes bound").to_string();
    wait_ready(&probes, "the founding node").await;
    // No leave: this is a crash/stop, so no departure marker is written.
    first.shutdown().await;

    let resumed = compose::start(cluster_on(&state, &bind, "127.0.0.1:0", &[]))
        .await
        .expect("a node still in the membership must resume without seeds");
    let probes = resumed.probe_addr().expect("probes bound").to_string();
    wait_ready(&probes, "the resumed node").await;

    resumed.shutdown().await;
}

/// Issue #72 AC5: a node that is out of its cluster with nowhere at all to
/// rejoin fails loudly instead of resuming into the wedge.
///
/// The marker is placed by hand **on purpose**, and this is the one test where
/// that is legitimate: no ordinary path produces it. A node that genuinely
/// departs learns its survivors from its own log, so it has somewhere to go and
/// takes the rejoin row instead. This asserts the last-resort guard for a state
/// directory that has been hand-edited or half-restored — the marker's name is
/// pinned by `departed_node_with_retained_state_dir_rejoins_on_restart`, which
/// produces a real one.
#[tokio::test]
async fn a_node_marked_departed_with_nowhere_to_rejoin_fails_startup_with_guidance() {
    let state = TempDir::new().expect("tempdir");
    let bind = reserve_port();

    let first = compose::start(cluster_on(
        &state,
        &bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("solo cluster starts");
    first.shutdown().await;

    std::fs::write(state.path().join("departed"), b"").expect("place the departure marker");

    let err = match compose::start(cluster_on(&state, &bind, "127.0.0.1:0", &[])).await {
        Ok(_) => panic!("a departed node with nowhere to rejoin must not silently resume"),
        Err(e) => format!("{e:#}"),
    };
    assert!(
        err.contains("--cluster-seeds"),
        "the refusal must name the flag that recovers it: {err}"
    );
}

/// Issue #72 regression: a **solo** node must survive its own SIGTERM.
///
/// A sole voter cannot leave — openraft refuses to empty a voter set — so
/// `leave` declines and the node exits still a member. Recording that as a
/// departure would refuse its next start outright, which is a hard regression
/// on `--cluster-allow-solo`: before any of this it restarted fine. This is why
/// `leave` reports `Departed` vs `Retained` rather than just `Ok`.
#[tokio::test]
async fn a_solo_node_survives_a_graceful_leave_and_restarts() {
    let state = TempDir::new().expect("tempdir");
    let bind = reserve_port();

    let solo = compose::start(cluster_on(
        &state,
        &bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo", "--cluster-leave-timeout", "2"],
    ))
    .await
    .expect("solo cluster starts");
    let probes = solo.probe_addr().expect("probes bound").to_string();
    wait_ready(&probes, "the solo node").await;

    solo.graceful_leave().await;
    assert!(
        !state.path().join("departed").exists(),
        "a sole voter never leaves, so nothing may record it as departed"
    );

    let restarted = compose::start(cluster_on(
        &state,
        &bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("a solo node must come back after its own graceful stop");
    let probes = restarted.probe_addr().expect("probes bound").to_string();
    wait_ready(&probes, "the restarted solo node").await;
    restarted.shutdown().await;
}

/// Issue #72 / #69 shared invariant, in-process half: a graceful stop of the
/// **whole fleet** must leave a cluster that can start again.
///
/// This is the amplified form of the solo regression above. Nodes SIGTERM in
/// sequence, and if every node recorded itself as departed, none could resume
/// and there would be no one left to elect — the fleet would need its state
/// directories deleted to come back at all.
///
/// Two nodes, so the voter floor (#69) refuses **both** departures and the
/// membership survives the teardown whole. The three-node shape, where one node
/// really departs and the other two are floor-refused, needs real processes and
/// lives in the container tier as `whole_fleet_sigterm_then_cold_start_converges`.
#[tokio::test]
async fn a_graceful_stop_of_the_whole_fleet_can_cold_start_again() {
    let founder_state = TempDir::new().expect("tempdir");
    let joiner_state = TempDir::new().expect("tempdir");
    let founder_bind = reserve_port();
    let joiner_bind = reserve_port();

    let founder = compose::start(cluster_on(
        &founder_state,
        &founder_bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo", "--cluster-leave-timeout", "2"],
    ))
    .await
    .expect("founder starts");
    let joiner = compose::start(cluster_on(
        &joiner_state,
        &joiner_bind,
        "127.0.0.1:0",
        &[
            "--cluster-seeds",
            &founder_bind,
            "--cluster-leave-timeout",
            "2",
        ],
    ))
    .await
    .expect("joiner starts");

    let founder_node = founder.node().expect("founder is clustered").clone();
    wait_voter_count(
        &founder_node,
        2,
        "both nodes must be voters before the teardown",
    )
    .await;
    drop(founder_node);

    // Whole-fleet stop, one after the other, exactly as `compose stop` does.
    joiner.graceful_leave().await;
    founder.graceful_leave().await;

    // Cold start in the same order. Since the voter floor (#69) neither node of
    // a two-voter fleet may leave, so both take the resume row and the whole
    // membership survives the teardown intact — which is the point: nothing has
    // to rejoin before a quorum can form.
    let founder = compose::start(cluster_on(
        &founder_state,
        &founder_bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("the fleet must be able to cold-start after a graceful stop");
    let founder_probes = founder.probe_addr().expect("probes bound").to_string();
    wait_ready(&founder_probes, "the restarted founder").await;

    let joiner = compose::start(cluster_on(
        &joiner_state,
        &joiner_bind,
        "127.0.0.1:0",
        &["--cluster-seeds", &founder_bind],
    ))
    .await
    .expect("the joiner must come back after the fleet's graceful stop");
    let joiner_probes = joiner.probe_addr().expect("probes bound").to_string();
    wait_ready(&joiner_probes, "the restarted joiner").await;

    let founder_node = founder.node().expect("founder is clustered").clone();
    wait_voter_count(
        &founder_node,
        2,
        "the fleet must converge back to both voters",
    )
    .await;
    drop(founder_node);

    joiner.shutdown().await;
    founder.shutdown().await;
}

/// Issue #72: the **founder** must be able to come back too, and it is the one
/// node that cannot be given seeds.
///
/// It founded the cluster, so `--cluster-seeds` is empty by construction. After
/// a graceful leave its only route back is the peer list its own durable log
/// remembers. Without that it restarts into a permanent refusal — which is what
/// the container tier caught, and what this test guards in-process, since the
/// container tier does not run on every change.
#[tokio::test]
async fn a_departed_founder_rejoins_through_the_peers_its_log_remembers() {
    let founder_state = TempDir::new().expect("tempdir");
    let keeper_state = TempDir::new().expect("tempdir");
    let extra_state = TempDir::new().expect("tempdir");
    let founder_bind = reserve_port();

    let founder = compose::start(cluster_on(
        &founder_state,
        &founder_bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo", "--cluster-leave-timeout", "5"],
    ))
    .await
    .expect("founder starts");
    let keeper = compose::start(cluster_on(
        &keeper_state,
        &reserve_port(),
        "127.0.0.1:0",
        &["--cluster-seeds", &founder_bind],
    ))
    .await
    .expect("keeper starts");
    // A third node so the founder's departure keeps two voters behind it.
    let extra = compose::start(cluster_on(
        &extra_state,
        &reserve_port(),
        "127.0.0.1:0",
        &["--cluster-seeds", &founder_bind],
    ))
    .await
    .expect("third node starts");

    let keeper_node = keeper.node().expect("keeper is clustered").clone();
    wait_voter_count(
        &keeper_node,
        3,
        "all three must be voters before the founder leaves",
    )
    .await;

    founder.graceful_leave().await;
    wait_voter_count(&keeper_node, 2, "the founder's departure must land").await;
    assert!(
        founder_state.path().join("departed").exists(),
        "the founder really departed, so it must be recorded"
    );

    // No seeds, exactly as the founder is configured in the shipped topology.
    let founder = compose::start(cluster_on(
        &founder_state,
        &founder_bind,
        "127.0.0.1:0",
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("a departed founder must rejoin through the peers its log remembers");
    let probes = founder.probe_addr().expect("probes bound").to_string();
    wait_ready(&probes, "the returning founder").await;
    wait_voter_count(&keeper_node, 3, "the founder must be a voter again").await;
    drop(keeper_node);

    founder.shutdown().await;
    extra.shutdown().await;
    keeper.shutdown().await;
}
