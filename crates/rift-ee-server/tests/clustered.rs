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
