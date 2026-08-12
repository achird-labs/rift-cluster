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

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rift_cluster::rpc::{AlwaysHealthy, RpcClient, RpcClientConfig, Signer};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

mod common;

use common::ports::reserve_port;
use common::seen::Seen;

const SECRET: &str = "front-door-test-secret";

/// `front_door` is a parameter, not a hardcoded `:0`, so the double-bind test
/// below can ask for a fixed address without depending on clap's
/// last-flag-wins behavior for a repeated `--front-door`.
/// Everything a solo clustered node needs except the front door itself, which is what the three
/// wrappers below differ on.
fn base_args(state: &TempDir) -> Vec<String> {
    vec![
        "rift-cluster-server".to_owned(),
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
    ]
}

/// `front_door` is a parameter, not a hardcoded `:0`, so the double-bind test
/// below can ask for a fixed address without depending on clap's
/// last-flag-wins behavior for a repeated `--front-door`.
fn cluster_cli_with_front_door(state: &TempDir, front_door: &str, extra: &[&str]) -> EeCli {
    let mut args = base_args(state);
    args.push("--front-door".to_owned());
    args.push(front_door.to_owned());
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    cluster_cli_with_front_door(state, "127.0.0.1:0", extra)
}

/// The same node with **no** `--front-door` at all — it never binds a listener, so it can never
/// count a dispatch.
fn cluster_cli_without_front_door(state: &TempDir) -> EeCli {
    EeCli::try_parse_from(base_args(state)).expect("parses")
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

/// Issue #210 gate: the route table carries a revision a client can condition
/// on, and a write that names a stale one is refused rather than silently
/// replacing the whole table over the top of a concurrent edit.
///
/// The lost-update this pins is not hypothetical: `PUT /front-door/routes` is a
/// whole-table replace, so two consoles that both read, edited and wrote leave
/// only the second one's table behind with nothing reporting the loss.
#[tokio::test]
async fn a_stale_if_match_cannot_clobber_the_route_table() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    // The read path answers a revision, portless: a route table has no single
    // record to qualify the token with, so the tenant segment stands alone.
    let seen = Seen::of(
        reqwest::get(format!("http://{admin}/front-door/routes"))
            .await
            .expect("get routes"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    let empty_revision = seen
        .header("rift-cluster-revision")
        .expect("GET must answer a revision to condition on")
        .to_owned();
    assert!(
        empty_revision.starts_with("default@"),
        "portless token: {empty_revision}"
    );

    // A write carrying the revision the read answered applies. A table nobody
    // has ever written reads as revision 0, so this also pins that an absent
    // row is 0 rather than a refusal.
    let response = client
        .put(format!("http://{admin}/front-door/routes"))
        .header("if-match", &empty_revision)
        .json(&one_route("first-writer", "/svc", port))
        .send()
        .await
        .expect("conditioned put");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "{seen}");

    // ...and the read's revision advances with it.
    let seen = Seen::of(
        reqwest::get(format!("http://{admin}/front-door/routes"))
            .await
            .expect("get routes"),
    )
    .await;
    let after_write = seen
        .header("rift-cluster-revision")
        .expect("revision header")
        .to_owned();
    assert_ne!(
        empty_revision, after_write,
        "a committed write must advance the table's revision"
    );

    // The stale token is refused with the same 409 shape a single-imposter
    // conflict uses — not a 400, and not a silent unconditional replace.
    let response = client
        .put(format!("http://{admin}/front-door/routes"))
        .header("if-match", &empty_revision)
        .json(&one_route("late-writer", "/late", port))
        .send()
        .await
        .expect("stale conditioned put");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 409, "{seen}");
    let body = seen.json();
    assert_eq!(body["errors"][0]["type"], "resource conflict", "{seen}");
    assert!(
        body["errors"][0]["message"]
            .as_str()
            .unwrap_or_default()
            .starts_with("revision conflict"),
        "{seen}"
    );

    // The point of the whole feature: the refused write changed nothing.
    let table: serde_json::Value = reqwest::get(format!("http://{admin}/front-door/routes"))
        .await
        .expect("get routes")
        .json()
        .await
        .expect("json");
    assert_eq!(
        table["routes"][0]["id"], "first-writer",
        "a refused precondition must not have replaced the table: {table}"
    );
    assert_eq!(table["routes"].as_array().map(Vec::len), Some(1), "{table}");

    // A token this front cannot evaluate is still a 400, not a pass.
    let response = client
        .put(format!("http://{admin}/front-door/routes"))
        .header("if-match", "*")
        .json(&one_route("wildcard", "/w", port))
        .send()
        .await
        .expect("malformed conditioned put");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 400, "{seen}");
    assert_eq!(seen.json()["errors"][0]["type"], "bad data", "{seen}");

    server.shutdown().await;
}

/// Issue #210 gate: a single-route delete mutates the table, so it must
/// invalidate every outstanding precondition against it — otherwise a client
/// that read before the delete could still replace the table wholesale after.
#[tokio::test]
async fn deleting_a_route_advances_the_table_revision() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    client
        .put(format!("http://{admin}/front-door/routes"))
        .json(&json!({
            "routes": [
                { "id": "a", "match": { "path_prefix": "/a" }, "target": { "port": port } },
                { "id": "b", "match": { "path_prefix": "/b" }, "target": { "port": port } },
            ],
        }))
        .send()
        .await
        .expect("put routes");

    let seen = Seen::of(
        reqwest::get(format!("http://{admin}/front-door/routes"))
            .await
            .expect("get routes"),
    )
    .await;
    let before_delete = seen
        .header("rift-cluster-revision")
        .expect("revision header")
        .to_owned();

    let response = client
        .delete(format!("http://{admin}/front-door/routes/b"))
        .send()
        .await
        .expect("delete route");
    assert_eq!(response.status().as_u16(), 200);

    let seen = Seen::of(
        reqwest::get(format!("http://{admin}/front-door/routes"))
            .await
            .expect("get routes"),
    )
    .await;
    let after_delete = seen
        .header("rift-cluster-revision")
        .expect("revision header")
        .to_owned();
    assert_ne!(
        before_delete, after_delete,
        "a delete mutates the table, so it must advance its revision"
    );

    // The token captured before the delete is now stale for a whole-table
    // replace...
    let response = client
        .put(format!("http://{admin}/front-door/routes"))
        .header("if-match", &before_delete)
        .json(&one_route("c", "/c", port))
        .send()
        .await
        .expect("stale conditioned put");
    assert_eq!(response.status().as_u16(), 409);

    // ...and for a second delete conditioned on it.
    let response = client
        .delete(format!("http://{admin}/front-door/routes/a"))
        .header("if-match", &before_delete)
        .send()
        .await
        .expect("stale conditioned delete");
    assert_eq!(response.status().as_u16(), 409);

    // A delete carrying the current token applies.
    let response = client
        .delete(format!("http://{admin}/front-door/routes/a"))
        .header("if-match", &after_delete)
        .send()
        .await
        .expect("conditioned delete");
    assert_eq!(response.status().as_u16(), 200);

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
        "rift-cluster-server".to_owned(),
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

/// Issue #368: the HITS column's source. Counted on route *claim*, so a route whose target then
/// fails still counts — a route claiming traffic and failing is exactly what the column exists to
/// reveal — and a route that has taken nothing reports a `0`, never an absence.
#[tokio::test]
async fn front_door_dispatches_are_counted_per_route() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let front_door = server
        .front_door_addr()
        .expect("--front-door was given, must bind");
    let served = reserve_port();
    // Deliberately never given an imposter: dispatches to it 404, and must still count.
    let dead = reserve_port();
    put_imposter(admin, served, "from-a").await;

    let table = json!({
        "routes": [
            { "id": "busy", "match": { "path_prefix": "/busy" }, "target": { "port": served } },
            { "id": "idle", "match": { "path_prefix": "/idle" }, "target": { "port": dead } },
        ],
    });
    let put = reqwest::Client::new()
        .put(format!("http://{admin}/front-door/routes"))
        .json(&table)
        .send()
        .await
        .expect("put routes");
    assert_eq!(put.status().as_u16(), 200);

    let before: serde_json::Value = reqwest::get(format!("http://{admin}/front-door/route-hits"))
        .await
        .expect("get route hits")
        .json()
        .await
        .expect("json");
    assert_eq!(
        before,
        json!({ "installed": true, "hits": { "busy": 0, "idle": 0 }, "front_door": "bound" }),
        "an untouched table is all zeros, not an empty map: {before}"
    );

    for _ in 0..3 {
        let dispatched = reqwest::get(format!("http://{front_door}/busy/anything"))
            .await
            .expect("request through the front door");
        assert_eq!(dispatched.status().as_u16(), 201);
    }

    // A path no route claims counts against nothing.
    let unmatched = reqwest::get(format!("http://{front_door}/unclaimed"))
        .await
        .expect("request through the front door");
    assert_eq!(unmatched.status().as_u16(), 404);

    let after_traffic: serde_json::Value =
        reqwest::get(format!("http://{admin}/front-door/route-hits"))
            .await
            .expect("get route hits")
            .json()
            .await
            .expect("json");
    assert_eq!(
        after_traffic,
        json!({ "installed": true, "hits": { "busy": 3, "idle": 0 }, "front_door": "bound" }),
        "three claims for `busy`, and `idle` still an explicit zero: {after_traffic}"
    );

    // `idle` targets a port with no imposter, so this 404s — and still counts. "Hits" is what the
    // route claimed, not what succeeded.
    let failed = reqwest::get(format!("http://{front_door}/idle/x"))
        .await
        .expect("request through the front door");
    assert_eq!(failed.status().as_u16(), 404);

    let after_failure: serde_json::Value =
        reqwest::get(format!("http://{admin}/front-door/route-hits"))
            .await
            .expect("get route hits")
            .json()
            .await
            .expect("json");
    assert_eq!(
        after_failure,
        json!({ "installed": true, "hits": { "busy": 3, "idle": 1 }, "front_door": "bound" }),
        "a claimed-then-failed request counts: {after_failure}"
    );

    server.shutdown().await;
}

/// A node with no front door still answers, and answers zeros (issue #368, E12).
///
/// Routes are replicated control-plane objects, so `GET /front-door/route-hits` must answer
/// identically on every node — including one that binds no listener. Its honest contribution to
/// the fleet sum is zero, not an error and not an absence: it really has dispatched nothing.
#[tokio::test]
async fn a_node_with_no_front_door_reports_zeros_rather_than_failing() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli_without_front_door(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    assert!(
        server.front_door_addr().is_none(),
        "this fixture must not bind a front door, or it proves nothing"
    );

    let put = reqwest::Client::new()
        .put(format!("http://{admin}/front-door/routes"))
        .json(&one_route("svc", "/svc", reserve_port()))
        .send()
        .await
        .expect("put routes");
    assert_eq!(put.status().as_u16(), 200);

    let hits: serde_json::Value = reqwest::get(format!("http://{admin}/front-door/route-hits"))
        .await
        .expect("get route hits")
        .json()
        .await
        .expect("json");
    // What a PEER would read from this node. The admin assertion below cannot cover it: this is a
    // solo node, so its own flag is folded in locally and never crosses the cluster port. If
    // `compose` passed the wrong value to `cluster_api::routes` specifically, a multi-node fleet
    // would report `bound` over listener-less nodes and nothing else here would notice.
    let cluster: std::net::SocketAddr = server
        .cluster_addr()
        .expect("clustered")
        .as_str()
        .parse()
        .expect("advertise is a literal address in tests");
    let own: serde_json::Value = serde_json::from_slice(
        &RpcClient::new(
            Some(Signer::new(SECRET)),
            Arc::new(AlwaysHealthy),
            RpcClientConfig::default(),
        )
        .call(cluster, "GET", "/_cluster/route-hits", Vec::new())
        .await
        .expect("GET /_cluster/route-hits"),
    )
    .expect("json body");
    assert_eq!(
        own,
        json!({ "hits": {}, "front_door": false }),
        "a node that binds no front door must say so to its peers, not merely to its own admin \
         read: {own}"
    );

    assert_eq!(
        hits,
        json!({ "installed": true, "hits": { "svc": 0 }, "front_door": "none" }),
        "the table is installed fleet-wide even where no listener is bound, and the answer says so rather than leaving the zero to read as a dead route (#403): {hits}"
    );

    server.shutdown().await;
}
