//! Issue #131 gate: the front door's route table as a replicated control-plane
//! object, end to end. Same shape as `write_path.rs`'s imposter gate — plain
//! HTTP against a real solo cluster — because a route write is a control op
//! exactly like an imposter write is, and everything downstream of `build_mutation`
//! (validate, mint, park, submit, barrier, headers) is the identical generic
//! machinery.
//!
//! What distinguishes these tests from a lookup-API check is deliberate (see
//! issue #131's correction comment): there is no `resolve` endpoint to assert
//! against, so "the write took effect" is proven by making a real request
//! *through the bound front door* and observing which imposter answered it.

use std::time::Duration;

use clap::Parser;
use rift_ee_server::cli::EeCli;
use rift_ee_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

mod common;

use common::ports::reserve_port;
use common::seen::Seen;

const SECRET: &str = "front-door-test-secret";

/// `front_door` is a parameter, not a hardcoded `:0`, so the double-bind test
/// below can ask for a fixed address without depending on clap's
/// last-flag-wins behavior for a repeated `--front-door`.
fn cluster_cli_with_front_door(state: &TempDir, front_door: &str, extra: &[&str]) -> EeCli {
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
        front_door.to_owned(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    cluster_cli_with_front_door(state, "127.0.0.1:0", extra)
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
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn minimal_imposter(port: u16, body: &str) -> serde_json::Value {
    json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "id": "a",
            "responses": [{ "is": { "statusCode": 201, "body": body } }],
        }],
    })
}

async fn put_imposter(admin: std::net::SocketAddr, port: u16, body: &str) {
    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port, body))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(
        response.status().as_u16(),
        201,
        "imposter setup must succeed"
    );
}

/// A whole-table `PUT` naming one route, straight to `path_prefix`.
fn one_route(id: &str, path_prefix: &str, target_port: u16) -> serde_json::Value {
    json!({
        "routes": [{
            "id": id,
            "match": { "path_prefix": path_prefix },
            "target": { "port": target_port },
        }],
    })
}

#[tokio::test]
async fn put_routes_commits_and_dispatches_through_the_front_door() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let front_door = server
        .front_door_addr()
        .expect("--front-door was given, must bind");
    let port = reserve_port();
    put_imposter(admin, port, "from-a").await;

    let response = reqwest::Client::new()
        .put(format!("http://{admin}/front-door/routes"))
        .json(&one_route("svc", "/svc", port))
        .send()
        .await
        .expect("put routes");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "{seen}");

    // No per-record port to qualify the revision with — a whole-table
    // replace has no single stored record.
    let revision = seen
        .header("rift-cluster-revision")
        .expect("revision header present")
        .to_owned();
    assert!(
        revision.starts_with("default@"),
        "revision names the tenant and log index: {revision}"
    );
    let op_id = seen
        .header("rift-cluster-op-id")
        .expect("op-id header present");
    uuid::Uuid::parse_str(op_id).expect("op id is a uuid");

    // A real request through the front door, not a lookup API (issue #131's
    // correction: there is no `resolve` endpoint upstream to assert against).
    // The default barrier already waited for local apply, so no retry loop
    // is needed here — unlike the imposter *bind*, the ArcSwap swap happens
    // synchronously inside the same apply that produced the response above.
    let dispatched = reqwest::get(format!("http://{front_door}/svc/anything"))
        .await
        .expect("request through the front door");
    assert_eq!(dispatched.status().as_u16(), 201);
    assert_eq!(dispatched.text().await.expect("body"), "from-a");

    // A path the route does not claim still 404s with the "no route" marker,
    // proving the front door is actually consulting the table rather than
    // catching everything.
    let unmatched = reqwest::get(format!("http://{front_door}/unmatched"))
        .await
        .expect("request through the front door");
    assert_eq!(unmatched.status().as_u16(), 404);
    assert_eq!(
        unmatched
            .headers()
            .get("x-rift-front-door")
            .and_then(|v| v.to_str().ok()),
        Some("no-route")
    );

    server.shutdown().await;
}

#[tokio::test]
async fn get_front_door_routes_reads_the_replicated_table() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();

    let empty: serde_json::Value = reqwest::get(format!("http://{admin}/front-door/routes"))
        .await
        .expect("get routes")
        .json()
        .await
        .expect("json");
    assert_eq!(empty["routes"], json!([]), "starts empty: {empty}");

    reqwest::Client::new()
        .put(format!("http://{admin}/front-door/routes"))
        .json(&one_route("svc", "/svc", port))
        .send()
        .await
        .expect("put routes");

    let after: serde_json::Value = reqwest::get(format!("http://{admin}/front-door/routes"))
        .await
        .expect("get routes")
        .json()
        .await
        .expect("json");
    assert_eq!(after["routes"][0]["id"], "svc", "{after}");

    server.shutdown().await;
}

#[tokio::test]
async fn delete_route_removes_it_and_404s_when_missing() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();

    reqwest::Client::new()
        .put(format!("http://{admin}/front-door/routes"))
        .json(&one_route("svc", "/svc", port))
        .send()
        .await
        .expect("put routes");

    let response = reqwest::Client::new()
        .delete(format!("http://{admin}/front-door/routes/svc"))
        .send()
        .await
        .expect("delete route");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "{seen}");

    let after: serde_json::Value = reqwest::get(format!("http://{admin}/front-door/routes"))
        .await
        .expect("get routes")
        .json()
        .await
        .expect("json");
    assert_eq!(after["routes"], json!([]), "{after}");

    // Mirrors DeleteImposter: the admin surface 404s for a route that was
    // never there, even though the state machine's own DeleteRoute is
    // idempotent (see `admin_front::build_mutation`'s DeleteRoute arm).
    let response = reqwest::Client::new()
        .delete(format!("http://{admin}/front-door/routes/svc"))
        .send()
        .await
        .expect("delete absent route");
    assert_eq!(response.status().as_u16(), 404);

    server.shutdown().await;
}

#[tokio::test]
async fn put_routes_rejects_an_invalid_table_as_a_typed_400() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();

    // Duplicate ids: rejected by `RouteTable::validate` before anything is
    // parked or committed.
    let response = reqwest::Client::new()
        .put(format!("http://{admin}/front-door/routes"))
        .json(&json!({
            "routes": [
                { "id": "same", "target": { "port": 1 } },
                { "id": "same", "target": { "port": 2 } },
            ],
        }))
        .send()
        .await
        .expect("put invalid routes");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 400, "{seen}");
    assert!(seen.body.contains("same"), "{seen}");

    // Nothing committed: the table is still empty.
    let after: serde_json::Value = reqwest::get(format!("http://{admin}/front-door/routes"))
        .await
        .expect("get routes")
        .json()
        .await
        .expect("json");
    assert_eq!(after["routes"], json!([]), "{after}");

    server.shutdown().await;
}

#[tokio::test]
async fn put_routes_dedups_by_idempotency_key() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let key = "front-door-dedup-key";

    let first = reqwest::Client::new()
        .put(format!("http://{admin}/front-door/routes"))
        .header("idempotency-key", key)
        .json(&one_route("first", "/first", port))
        .send()
        .await
        .expect("first put");
    let first_seen = Seen::of(first).await;
    assert_eq!(first_seen.status, 200, "{first_seen}");
    let first_revision = first_seen
        .header("rift-cluster-revision")
        .expect("revision")
        .to_owned();

    // Same key, a *different* body: a genuine second op would replace the
    // table with `second`. The dedup contract says it must not — the replay
    // collapses to the original response.
    let replay = reqwest::Client::new()
        .put(format!("http://{admin}/front-door/routes"))
        .header("idempotency-key", key)
        .json(&one_route("second", "/second", port))
        .send()
        .await
        .expect("replayed put");
    let replay_seen = Seen::of(replay).await;
    assert_eq!(replay_seen.status, 200, "{replay_seen}");
    assert_eq!(
        replay_seen.header("rift-cluster-revision"),
        Some(first_revision.as_str()),
        "a replayed op_id must return the ORIGINAL revision: {replay_seen}"
    );

    let after: serde_json::Value = reqwest::get(format!("http://{admin}/front-door/routes"))
        .await
        .expect("get routes")
        .json()
        .await
        .expect("json");
    assert_eq!(
        after["routes"][0]["id"], "first",
        "the replayed op_id must not have applied a second time: {after}"
    );

    server.shutdown().await;
}

/// Under `--cluster`, `attach_data_plane` must clear `--front-door` before
/// handing the CLI to the open-source `ServerBuilder` and bind it itself
/// instead — leaving the flag in place would have both try to bind the same
/// fixed address, and the second bind fails loudly. A fixed (not `:0`) port
/// makes that failure observable: two ephemeral binds would each silently
/// succeed on different ports and this test would prove nothing.
#[tokio::test]
async fn front_door_binds_exactly_once_on_the_fixed_port() {
    let state = TempDir::new().expect("tempdir");
    let fixed_port = reserve_port();
    let server = compose::start(cluster_cli_with_front_door(
        &state,
        &format!("127.0.0.1:{fixed_port}"),
        &[],
    ))
    .await
    .expect("solo cluster starts (a double bind would fail this)");
    wait_ready(&server).await;

    assert_eq!(
        server.front_door_addr(),
        Some(format!("127.0.0.1:{fixed_port}").parse().unwrap()),
        "exactly the requested address, bound exactly once"
    );
    let response = reqwest::get(format!("http://127.0.0.1:{fixed_port}/anything"))
        .await
        .expect("the one listener answers");
    assert_eq!(response.status().as_u16(), 404); // no route matched — but *something* answered.

    server.shutdown().await;
}

/// `--cluster` off never reaches `attach_data_plane` at all — `compose::start`
/// hands `ServerBuilder` the CLI whole and returns. This is the `parity` job's
/// bar: nothing about `--front-door` changes when clustering is off.
#[tokio::test]
async fn cluster_off_front_door_is_upstream_unchanged() {
    let state = TempDir::new().expect("tempdir");
    let fixed_port = reserve_port();
    let args: Vec<String> = vec![
        "rift-ee-server".to_owned(),
        "--port".to_owned(),
        "0".to_owned(),
        "--metrics-port".to_owned(),
        "0".to_owned(),
        "--datadir".to_owned(),
        state.path().to_string_lossy().into_owned(),
        "--front-door".to_owned(),
        format!("127.0.0.1:{fixed_port}"),
    ];
    // No `--cluster*` flags at all, so `resolve_cluster` short-circuits
    // before anything cluster-shaped is read — `compose::start` hands the
    // whole CLI to `ServerBuilder` untouched.
    let cli = EeCli::try_parse_from(args).expect("parses");

    let server = compose::start(cli)
        .await
        .expect("un-clustered server starts");
    assert_eq!(
        server.front_door_addr(),
        Some(format!("127.0.0.1:{fixed_port}").parse().unwrap()),
        "upstream's own ServerBuilder bound it — nothing in this crate touched it"
    );
    server.shutdown().await;
}
