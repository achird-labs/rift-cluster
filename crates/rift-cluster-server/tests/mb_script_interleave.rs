//! D-121: a cluster node does not tune Mountebank-style script scheduling — pool size, imposter
//! affinity, the realm cap and `_rift.scriptEngine.timeoutMs` are the engine's, exactly as
//! standalone has them (rift#1678).
//!
//! The cluster form of upstream's `issue_1678_inject_timers_interleave.rs`: an `inject` waiting on a
//! timer holds no script worker, so a node full of sleeping injects still answers a synchronous one
//! at once. Its own binary, and its tests run one at a time: the script pool is process-wide.

use std::time::{Duration, Instant};

use clap::Parser;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

mod common;

use common::ports::reserve_port;

const SECRET: &str = "mb-script-interleave-test-secret";

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn cluster_cli(state: &TempDir) -> EeCli {
    let state = state.path().to_string_lossy();
    EeCli::try_parse_from([
        "rift-cluster-server",
        "--port",
        "0",
        "--metrics-port",
        "0",
        "--cluster",
        "--cluster-bind",
        "127.0.0.1:0",
        "--cluster-probe-bind",
        "127.0.0.1:0",
        "--cluster-secret",
        SECRET,
        "--cluster-state-dir",
        &state,
        "--cluster-allow-solo",
        "--allow-injection",
    ])
    .expect("parses")
}

async fn solo_node(state: &TempDir) -> ComposedServer {
    let server = compose::start(cluster_cli(state))
        .await
        .expect("solo cluster starts");
    let probes = server.probe_addr().expect("probes bound under --cluster");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = reqwest::get(format!("http://{probes}/readyz")).await
            && response.status().as_u16() == 200
        {
            return server;
        }
        assert!(Instant::now() < deadline, "node never became ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Create an imposter whose one stub answers with `inject`, with a script timeout long enough for
/// a 10 s timer; returns its port.
async fn inject_imposter(server: &ComposedServer, inject: &str) -> u16 {
    let port = reserve_port();
    let created = reqwest::Client::new()
        .post(format!("http://{}/imposters", server.admin_addr()))
        .json(&json!({
            "port": port,
            "protocol": "http",
            "_rift": { "scriptEngine": { "timeoutMs": 20_000 } },
            "stubs": [{ "responses": [{ "inject": inject }] }],
        }))
        .send()
        .await
        .expect("post");
    let status = created.status().as_u16();
    assert_eq!(status, 201, "{:?}", created.text().await);
    port
}

async fn get(server: &ComposedServer, port: u16) -> String {
    reqwest::Client::new()
        .get(format!("http://{}/__rift/{port}/", server.admin_addr()))
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("request")
        .text()
        .await
        .expect("body")
}

/// Pins D-121: script scheduling on a cluster node is the engine's, and since rift#1678 a sleeping
/// inject holds no worker — twenty runs waiting 10 s on five imposters, more than the 16 workers
/// the pool can have, leave a synchronous inject on another imposter answering at once. On an
/// engine before rift#1678 the sleepers hold every worker and this waits ~10 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sleeping_injects_on_one_imposter_do_not_starve_another_on_a_node() {
    let _serial = ONE_AT_A_TIME.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = solo_node(&state).await;
    let quick = inject_imposter(&server, "config => ({ body: 'quick' })").await;
    assert_eq!(get(&server, quick).await, "quick", "warm-up");

    let mut sleepers = Vec::new();
    for _ in 0..5 {
        let port = inject_imposter(
            &server,
            "config => new Promise(r => setTimeout(() => r({ body: 'slept' }), 10000))",
        )
        .await;
        for _ in 0..4 {
            let url = format!("http://{}/__rift/{port}/", server.admin_addr());
            sleepers.push(tokio::spawn(async move { reqwest::get(url).await }));
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = Instant::now();
    assert_eq!(get(&server, quick).await, "quick");
    let took = started.elapsed();
    for sleeper in sleepers {
        sleeper.abort();
    }
    assert!(
        took < Duration::from_millis(500),
        "a synchronous inject waited {took:?} behind other imposters' sleeping injects"
    );
    server.shutdown().await;
}

/// Pins D-121: the door does not cap `_rift.scriptEngine.timeoutMs` — it admits what standalone
/// admits, because with one administrator (D-73) there is no trust boundary for a cap to defend.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_door_admits_any_script_timeout_ms_as_standalone_does() {
    let _serial = ONE_AT_A_TIME.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = solo_node(&state).await;
    let port = reserve_port();
    let created = reqwest::Client::new()
        .post(format!("http://{}/imposters", server.admin_addr()))
        .json(&json!({
            "port": port,
            "protocol": "http",
            "_rift": { "scriptEngine": { "timeoutMs": 600_000 } },
            "stubs": [{ "responses": [{ "inject": "config => ({ body: 'ok' })" }] }],
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(created.status().as_u16(), 201);
    server.shutdown().await;
}
