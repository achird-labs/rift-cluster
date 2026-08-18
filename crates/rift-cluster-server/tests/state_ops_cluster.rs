//! Issue #290 (RFC-005 §3.7 / S3): declarative state operations (`_rift.stateOps`, upstream U-15)
//! under `--cluster` — the exit criterion verbatim: **a counter stub increments correctly behind a
//! round-robin LB with no script and `--allowInjection` off**, and the ops land scope-prefixed.
//!
//! Two nodes in one process, the imposter committed through one of them and applied on both; the
//! "load balancer" is the test alternating requests between the two nodes' `/__rift/{port}/`
//! gateways, so every other request is answered by a different node's engine. Whether the joiner
//! could bind the imposter's port itself is irrelevant to the proof (BSD lets both bind under
//! `SO_REUSEPORT`, Linux does not — the joiner then serves in-process, #143), which is exactly why
//! the gateway is used on both sides rather than the port.
//!
//! What makes the counter correct is nothing this crate adds for it: the ops run inside the engine
//! against the imposter's `FlowStore`, which under `--cluster` is the owner-routed, replicated,
//! scope-prefixed clustered store — zero cluster code on the write path.

use std::time::Duration;

use clap::Parser;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

mod common;

use common::ports::{reserve_addr, reserve_port};

const SECRET: &str = "state-ops-cluster-secret";

fn cluster_on(state: &TempDir, bind: &str, extra: &[&str]) -> EeCli {
    let mut args = vec![
        "rift-cluster-server".to_owned(),
        "--port".to_owned(),
        "0".to_owned(),
        "--metrics-port".to_owned(),
        "0".to_owned(),
        "--cluster".to_owned(),
        "--cluster-bind".to_owned(),
        bind.to_owned(),
        "--cluster-probe-bind".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--cluster-secret".to_owned(),
        SECRET.to_owned(),
        "--cluster-state-dir".to_owned(),
        state.path().to_string_lossy().into_owned(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

async fn wait_ready(server: &ComposedServer) {
    let probes = server.probe_addr().expect("probes bound under --cluster");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = reqwest::get(format!("http://{probes}/readyz")).await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node never became ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One data-plane request through `node`'s `/__rift/{port}/` gateway, carrying the session header
/// the imposter resolves its flow id from. Returns the body.
async fn hit(admin: &str, port: u16, session: &str) -> String {
    let response = reqwest::Client::new()
        .get(format!("http://{admin}/__rift/{port}/count"))
        .header("X-Session", session)
        .send()
        .await
        .expect("gateway request");
    assert_eq!(response.status().as_u16(), 200, "the imposter answered");
    response.text().await.expect("body")
}

/// Poll `node`'s gateway until it serves `port` (the joiner applies the config a replication
/// round after the founder committed it).
async fn wait_served(admin: &str, port: u16) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = reqwest::Client::new()
            .get(format!("http://{admin}/__rift/{port}/warm"))
            .header("X-Session", "warm-up")
            .send()
            .await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{admin} never served imposter {port} through its gateway"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn counter_imposter(port: u16) -> serde_json::Value {
    json!({
        "port": port,
        "protocol": "http",
        "_rift": { "flowState": { "flowIdSource": "header:X-Session" } },
        "stubs": [{
            "responses": [{
                "is": { "statusCode": 200, "body": "{{ state.hits }}" },
                "_rift": {
                    "templated": true,
                    "stateOps": [{ "op": "increment", "key": "hits" }]
                }
            }]
        }]
    })
}

/// The RFC-005 §3.7 exit criterion, verbatim, plus the scope-prefix half.
///
/// Multi-threaded runtime, deliberately: the body's `{{ state.hits }}` read runs inline on the
/// engine's worker (upstream renders templates synchronously), and on the node that does not own
/// the flow it is an RPC to the owner parked on the flow bridge. On a current-thread runtime that
/// park starves the very runtime that would serve the RPC, the read times out and renders empty —
/// a test-harness artefact, not the deployment shape, where the runtime is multi-threaded. (The
/// inline read itself is a head-of-line-blocking gap on any blocking store, filed upstream as
/// achird-labs/rift#971; the FSM and the state ops already offload.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_counter_stub_increments_behind_a_round_robin_lb_with_no_script_and_no_injection() {
    let founder_state = TempDir::new().expect("tempdir");
    let joiner_state = TempDir::new().expect("tempdir");
    let founder_bind = reserve_addr();

    // No `--allowInjection` on either node: that is the point.
    let founder = compose::start(cluster_on(
        &founder_state,
        &founder_bind,
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("founder starts");
    wait_ready(&founder).await;
    let joiner = compose::start(cluster_on(
        &joiner_state,
        &reserve_addr(),
        &["--cluster-seeds", &founder_bind],
    ))
    .await
    .expect("joiner starts");
    wait_ready(&joiner).await;

    let founder_admin = founder.admin_addr().to_string();
    let joiner_admin = joiner.admin_addr().to_string();
    let port = reserve_port();
    let other = reserve_port();

    // Admission: a `stateOps` config is not a scripted config. `config_uses_script_surface` must
    // not classify it as one, or this would be the `400 invalid injection` a stub with `inject`
    // gets — the whole reason the ops are engine vocabulary rather than generated scripts.
    let client = reqwest::Client::new();
    for p in [port, other] {
        let response = client
            .post(format!("http://{founder_admin}/imposters"))
            .json(&counter_imposter(p))
            .send()
            .await
            .expect("post imposter");
        assert_eq!(
            response.status().as_u16(),
            201,
            "a stateOps config is admitted with --allowInjection off: {}",
            response.text().await.unwrap_or_default()
        );
    }
    wait_served(&founder_admin, port).await;
    wait_served(&joiner_admin, port).await;
    wait_served(&founder_admin, other).await;

    // Round-robin: alternate nodes for one session. The body is `{{ state.hits }}` rendered
    // BEFORE this request's increment, so the sequence starts empty (no key yet — a template miss
    // renders empty outside debug) and then counts 1, 2, 3, ... regardless of which node answers.
    let mut seen = Vec::new();
    for i in 0..6 {
        let admin = if i % 2 == 0 {
            &founder_admin
        } else {
            &joiner_admin
        };
        seen.push(hit(admin, port, "session-a").await);
    }
    assert_eq!(
        seen,
        vec!["", "1", "2", "3", "4", "5"],
        "the counter increments once per request across both nodes"
    );

    // Scope-prefixed: the other imposter, same session header, sees nothing of this counter — its
    // first read is empty, its own increments start at 1.
    let other_first = hit(&founder_admin, other, "session-a").await;
    assert_eq!(
        other_first, "",
        "an imposter-scoped counter is not visible from another imposter under the same flow id"
    );
    let other_second = hit(&joiner_admin, other, "session-a").await;
    assert_eq!(other_second, "1");
    // And the first imposter is where it was left, unaffected by the other's ops.
    assert_eq!(hit(&joiner_admin, port, "session-a").await, "6");

    joiner.shutdown().await;
    founder.shutdown().await;
}
