//! Issue #143 gate (RFC-001 §7.4.6): a node that cannot bind an imposter's port must still
//! serve that imposter in-process.
//!
//! The premise the RFC states and the code did not honour: every node applies the same committed
//! config, so a port taken by an unrelated process on *one* node used to leave that node with no
//! map entry at all — and both in-process addressing routes (`/__rift/:port` on the gateway, and
//! the front door) resolve through that map, so the node answered `404` for an imposter the
//! cluster considers to exist.
//!
//! These tests are the in-process half of the gate. The container tier's
//! `c19_front_door_routes_around_bind_divergence` proves the same thing across a real three-node
//! stack with the squatter in one node's network namespace; here the squat is a plain listener in
//! this process, which is the same collision the node's bind actually loses.

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rift_cluster::rpc::{AlwaysHealthy, RpcClient, RpcClientConfig, Signer};
use rift_ee_server::cli::EeCli;
use rift_ee_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

mod common;

use common::ports::reserve_port;
use common::seen::Seen;

const SECRET: &str = "bind-divergence-test-secret";

fn cluster_cli(state: &TempDir) -> EeCli {
    cluster_cli_with(state, &[])
}

fn cluster_cli_with(state: &TempDir, extra: &[&str]) -> EeCli {
    let mut args = vec![
        "rift-ee-server".to_owned(),
        "--port".to_owned(),
        "0".to_owned(),
        "--metrics-port".to_owned(),
        "0".to_owned(),
        "--cluster".to_owned(),
        "--cluster-bind".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--cluster-probe-bind".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--cluster-secret".to_owned(),
        SECRET.to_owned(),
        "--cluster-state-dir".to_owned(),
        state.path().to_string_lossy().into_owned(),
        "--cluster-allow-solo".to_owned(),
        "--front-door".to_owned(),
        "127.0.0.1:0".to_owned(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

async fn wait_ready(server: &ComposedServer) {
    let probes = server.probe_addr().expect("probes bound under --cluster");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
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

fn imposter(port: u16, body: &str) -> serde_json::Value {
    json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "id": "a",
            "responses": [{ "is": { "statusCode": 200, "body": body } }],
        }],
    })
}

/// The squat a real deployment sees: an unrelated process holding the port with a plain
/// (non-SO_REUSEPORT) listener, so the node's reusable bind cannot join it.
///
/// `0.0.0.0`, not `127.0.0.1`: an imposter binds the wildcard address, and `SO_REUSEADDR` lets a
/// wildcard bind succeed *over* a loopback-specific one on macOS. Squatting loopback therefore
/// produces a node that binds cleanly and a test that passes without exercising anything.
async fn squat(port: u16) -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("squatter bind")
}

/// Whether the node reports a local engine failure for `port`.
///
/// Read off the node rather than scraped from `/metrics`: this is the exact state the
/// `rift_cluster_bind_failures` gauge and the read-side header are both derived from, so asserting
/// it pins the cause instead of one of its two projections.
fn reports_bind_failure(server: &ComposedServer, port: u16) -> bool {
    server
        .node()
        .expect("clustered")
        .apply_failures()
        .contains_key(&port)
}

/// `GET /_cluster/imposters` off the authenticated cluster port — the operator surface, which is
/// not on the admin port and needs the cluster credential.
async fn cluster_imposters(server: &ComposedServer) -> serde_json::Value {
    let addr: std::net::SocketAddr = server
        .cluster_addr()
        .expect("clustered")
        .as_str()
        .parse()
        .expect("advertise is a literal address in tests");
    let body = RpcClient::new(
        Some(Signer::new(SECRET)),
        Arc::new(AlwaysHealthy),
        RpcClientConfig::default(),
    )
    .call(addr, "GET", "/_cluster/imposters", Vec::new())
    .await
    .expect("GET /_cluster/imposters");
    serde_json::from_slice(&body).expect("json body")
}

/// The dividend itself: the imposter is readable and serves through the front door on the very
/// node whose bind failed, and the node says so rather than pretending it is healthy.
#[tokio::test]
async fn a_bind_failed_node_still_serves_its_imposter_through_the_front_door() {
    let port = reserve_port();
    let blocker = squat(port).await;

    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let front_door = server
        .front_door_addr()
        .expect("--front-door was given, must bind");

    // The write commits and is applied. It is a `201` — not the `404` the render downgrade used to
    // produce when the re-read found nothing — because the imposter is now genuinely in the map.
    let created = Seen::of(
        reqwest::Client::new()
            .post(format!("http://{admin}/imposters"))
            .json(&imposter(port, "served-while-unbound"))
            .send()
            .await
            .expect("post imposter"),
    )
    .await;
    assert_eq!(
        created.status, 201,
        "the imposter exists cluster-wide regardless of the local bind: {created}"
    );
    // A `201` must not be a silent `201`. This warning is what tells the client that the write
    // succeeded fleet-wide but this node could not realize it locally — it is the difference
    // between "degraded and saying so" and "degraded and indistinguishable from healthy".
    let warnings = created
        .header("rift-cluster-warnings")
        .expect("a write whose local apply failed carries a warning");
    assert!(
        warnings.contains("local-engine="),
        "the warning names the local engine failure: {warnings}"
    );

    // Route the front door at it and make a real request: this is the assertion that matters,
    // because it goes through the same `get_imposter` lookup the gateway uses.
    let routed = Seen::of(
        reqwest::Client::new()
            .put(format!("http://{admin}/front-door/routes"))
            .json(&json!({
                "routes": [{
                    "id": "svc",
                    "match": { "path_prefix": "/svc" },
                    "target": { "port": port },
                }],
            }))
            .send()
            .await
            .expect("put routes"),
    )
    .await;
    assert_eq!(routed.status, 200, "{routed}");

    let dispatched = reqwest::get(format!("http://{front_door}/svc/anything"))
        .await
        .expect("request through the front door");
    assert_eq!(
        dispatched.status().as_u16(),
        200,
        "the node serves the imposter it could not bind"
    );
    assert_eq!(
        dispatched.text().await.expect("body"),
        "served-while-unbound"
    );

    // The OTHER in-process route. §7.4.6 promises both, and they are different code paths into the
    // same `get_imposter` lookup — the front door resolves a route table first, the gateway
    // addresses the port directly — so proving one does not prove the other.
    let gateway = Seen::of(
        reqwest::get(format!("http://{admin}/__rift/{port}/anything"))
            .await
            .expect("request through the gateway prefix"),
    )
    .await;
    assert_eq!(
        gateway.status, 200,
        "the gateway route serves the imposter this node could not bind: {gateway}"
    );
    assert_eq!(gateway.body, "served-while-unbound");

    // Serving is not the same as pretending to be healthy: the failure is still reported.
    assert!(
        reports_bind_failure(&server, port),
        "the bind failure stays observable while the node serves unbound"
    );

    // And the read surface marks the imposter degraded rather than hiding it or 404ing it.
    let read = Seen::of(
        reqwest::get(format!("http://{admin}/imposters/{port}"))
            .await
            .expect("get imposter"),
    )
    .await;
    assert_eq!(read.status, 200, "the diverged node reports it: {read}");
    let marker = read
        .header("rift-cluster-bind-failures")
        .expect("the degraded marker is on the read");
    assert!(
        marker.contains(&port.to_string()),
        "the marker names the port that failed: {marker}"
    );

    // A port with no failure carries no marker — otherwise the header would be noise rather
    // than a signal.
    let healthy_port = reserve_port();
    Seen::of(
        reqwest::Client::new()
            .post(format!("http://{admin}/imposters"))
            .json(&imposter(healthy_port, "bound"))
            .send()
            .await
            .expect("post imposter"),
    )
    .await;
    let healthy = Seen::of(
        reqwest::get(format!("http://{admin}/imposters/{healthy_port}"))
            .await
            .expect("get imposter"),
    )
    .await;
    assert_eq!(healthy.status, 200, "{healthy}");
    assert!(
        healthy.header("rift-cluster-bind-failures").is_none(),
        "a healthy imposter is not marked degraded: {healthy}"
    );

    // The operator surface names the failure per port — the `(port, node)` view RFC-001 §7.4.6
    // asks for, answered by each node about itself because a bind outcome is a node-local
    // observation and cannot ride the deterministic raft apply.
    let listing = cluster_imposters(&server).await;
    let entry = listing["imposters"]
        .as_array()
        .expect("imposters is an array")
        .iter()
        .find(|e| e["port"] == port)
        .expect("the squatted port is listed");
    assert!(
        entry["bind_failure"]
            .as_str()
            .is_some_and(|r| r.contains("Address already in use")),
        "the operator listing names why the port could not be realized: {entry}"
    );
    let healthy_entry = listing["imposters"]
        .as_array()
        .expect("imposters is an array")
        .iter()
        .find(|e| e["port"] == healthy_port)
        .expect("the healthy port is listed");
    assert_eq!(
        healthy_entry["bind_failure"],
        serde_json::Value::Null,
        "a port this node did realize reports no failure: {healthy_entry}"
    );

    drop(blocker);
    server.shutdown().await;
}

/// A non-bind apply failure must NOT be reported as bind divergence.
///
/// The node tracks every kind of engine-side failure in one map, keyed by port, with the error
/// stringified — so the *kind* is gone by the time anything reads it. Sourcing the degraded marker
/// from that map would make an unreadable TLS cert, a rejected stub patch, or an unparseable stored
/// record all announce themselves as "bound elsewhere, still served here in-process". For those the
/// imposter is not in the local map at all and the read is a 404, so the marker would send an
/// operator looking at ports instead of at their cert.
#[tokio::test]
async fn a_non_bind_failure_is_not_reported_as_bind_divergence() {
    let state = TempDir::new().expect("tempdir");
    // `--no-self-signed-tls` is what makes the failure below a failure at all: with self-signed
    // generation on (the default) a missing cert silently falls back to a generated one and the
    // apply succeeds, so the test would prove nothing.
    let server = compose::start(cluster_cli_with(&state, &["--no-self-signed-tls"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();

    // An https imposter naming a cert that does not exist: the config commits (validation is not
    // the engine's cert check), then the local apply fails resolving the TLS acceptor — a
    // `Tls` error, not a `BindError`, so nothing is registered and nothing degrades.
    Seen::of(
        reqwest::Client::new()
            .post(format!("http://{admin}/imposters"))
            .json(&json!({
                "port": port,
                "protocol": "https",
                "certFile": "/nonexistent/cert.pem",
                "keyFile": "/nonexistent/key.pem",
                "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "never" } }] }],
            }))
            .send()
            .await
            .expect("post imposter"),
    )
    .await;

    // The node does record *a* failure for this port...
    assert!(
        reports_bind_failure(&server, port),
        "the TLS failure is tracked in the general apply-failure map"
    );
    // ...but it must not be dressed up as bind divergence on either surface.
    let read = Seen::of(
        reqwest::get(format!("http://{admin}/imposters/{port}"))
            .await
            .expect("get imposter"),
    )
    .await;
    assert!(
        read.header("rift-cluster-bind-failures").is_none(),
        "a TLS failure must not be reported as a bind failure: {read}"
    );

    let listing = cluster_imposters(&server).await;
    if let Some(entry) = listing["imposters"]
        .as_array()
        .expect("imposters is an array")
        .iter()
        .find(|e| e["port"] == port)
    {
        assert_eq!(
            entry["bind_failure"],
            serde_json::Value::Null,
            "the operator listing must not call a TLS failure a bind failure: {entry}"
        );
    }

    server.shutdown().await;
}

/// Healing: once the squatter exits, the node must rebind and start serving the port itself —
/// and stop reporting the failure. Without the rebind the map entry would make reconcile see
/// "exists, unchanged" and the node would stay degraded for the life of the process.
#[tokio::test]
async fn the_node_rebinds_and_clears_the_failure_once_the_port_frees_up() {
    let port = reserve_port();
    let blocker = squat(port).await;

    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();

    Seen::of(
        reqwest::Client::new()
            .post(format!("http://{admin}/imposters"))
            .json(&imposter(port, "healed"))
            .send()
            .await
            .expect("post imposter"),
    )
    .await;
    assert!(
        reports_bind_failure(&server, port),
        "degraded while the squatter holds the port"
    );

    drop(blocker);

    // Any subsequent apply re-attempts the bind. A stub edit is the cheapest way to provoke one
    // without changing what the imposter serves.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        Seen::of(
            reqwest::Client::new()
                .post(format!("http://{admin}/imposters"))
                .json(&imposter(port, "healed"))
                .send()
                .await
                .expect("post imposter"),
        )
        .await;
        if !reports_bind_failure(&server, port) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the node never rebound the port after the squatter exited"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Rebound for real: the port itself now answers, not just the in-process routes.
    let direct = reqwest::get(format!("http://127.0.0.1:{port}/anything"))
        .await
        .expect("request straight at the imposter's own port");
    assert_eq!(
        direct.status().as_u16(),
        200,
        "the rebound listener accepts on the port"
    );
    assert_eq!(direct.text().await.expect("body"), "healed");

    server.shutdown().await;
}
