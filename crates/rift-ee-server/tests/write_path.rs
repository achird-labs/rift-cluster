//! Issue #9 slice 2 gate: the clustered admin write path end to end.
//!
//! These drive `compose::start` and speak plain HTTP to the public admin
//! address, exactly as a client would: mutations must commit through Raft,
//! materialize in the local engine (a live listener, not just a table), carry
//! the cluster headers, forward from non-leaders, refuse cleanly without a
//! quorum, and survive a full restart.

use std::time::Duration;

use clap::Parser;
use rift_ee_server::cli::EeCli;
use rift_ee_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

const SECRET: &str = "write-path-test-secret";

fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
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
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

/// An address that was bound and released — free right now.
fn reserve_port() -> u16 {
    let held = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    held.local_addr().expect("addr").port()
}

/// Poll `/readyz` until the node reports Ready; the reconcile gate opens
/// asynchronously after start.
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

fn minimal_imposter(port: u16) -> serde_json::Value {
    json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "id": "a",
            "responses": [{ "is": { "statusCode": 201, "body": "from-a" } }],
        }],
    })
}

/// The stub-served body on the imposter's own data port — the proof that a
/// committed config became a live listener, not just a table row.
async fn served_body(port: u16) -> Option<String> {
    let response = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .ok()?;
    response.text().await.ok()
}

/// Poll the data port until it serves `want` (the engine drive runs after the
/// admin response on follower nodes without a barrier, so give it a moment).
async fn wait_served(port: u16, want: &str) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if served_body(port).await.as_deref() == Some(want) {
            return true;
        }
        if std::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn post_imposter_commits_binds_and_carries_cluster_headers() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();

    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 201);

    let revision = response
        .headers()
        .get("rift-cluster-revision")
        .and_then(|v| v.to_str().ok())
        .expect("revision header present")
        .to_owned();
    assert!(
        revision.starts_with(&format!("default:{port}@")),
        "revision names the tenant, port and log index: {revision}"
    );
    let op_id = response
        .headers()
        .get("rift-cluster-op-id")
        .and_then(|v| v.to_str().ok())
        .expect("op-id header present");
    uuid::Uuid::parse_str(op_id).expect("op id is a uuid");

    let body: serde_json::Value = response.json().await.expect("mutation body is json");
    assert_eq!(body["port"], port, "upstream's own response shape: {body}");

    // The proxied read path sees it, and the engine actually serves it.
    let read: serde_json::Value = reqwest::get(format!("http://{admin}/imposters/{port}"))
        .await
        .expect("proxied get")
        .json()
        .await
        .expect("json");
    assert_eq!(read["port"], port);
    let self_link = read["_links"]["self"]["href"].as_str().expect("self link");
    assert!(
        self_link.contains(&admin.to_string()),
        "HATEOAS links must carry the public authority, not the loopback one: {self_link}"
    );
    assert!(wait_served(port, "from-a").await, "imposter must be bound");

    // R4 hygiene: a write that answered success retired its intent — nothing
    // stays parked to replay later.
    assert!(
        server
            .node()
            .expect("clustered")
            .parked_intents()
            .expect("read intents")
            .is_empty(),
        "a successful write must leave nothing parked"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn stub_crud_terminates_and_replicates_order() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 201);

    // Add "b" at the end, replace "a" in place, delete "b" by id.
    let response = client
        .post(format!("http://{admin}/imposters/{port}/stubs"))
        .json(&json!({
            "stub": {
                "id": "b",
                "responses": [{ "is": { "statusCode": 200, "body": "from-b" } }],
            },
        }))
        .send()
        .await
        .expect("add stub");
    assert_eq!(response.status().as_u16(), 200, "add stub by POST");

    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs/by-id/a"))
        .json(&json!({
            "responses": [{ "is": { "statusCode": 200, "body": "a-replaced" } }],
        }))
        .send()
        .await
        .expect("replace by id");
    assert_eq!(response.status().as_u16(), 200, "replace stub by id");

    let response = client
        .delete(format!("http://{admin}/imposters/{port}/stubs/by-id/b"))
        .send()
        .await
        .expect("delete by id");
    assert_eq!(response.status().as_u16(), 200, "delete stub by id");

    let read: serde_json::Value = reqwest::get(format!("http://{admin}/imposters/{port}"))
        .await
        .expect("get imposter")
        .json()
        .await
        .expect("json");
    let stubs = read["stubs"].as_array().expect("stubs array");
    assert_eq!(stubs.len(), 1, "only the replaced 'a' remains: {read}");
    assert!(
        wait_served(port, "a-replaced").await,
        "the live engine serves the replaced stub"
    );

    // A by-id edit against a missing id is a clean 404, not a committed no-op.
    let response = client
        .delete(format!("http://{admin}/imposters/{port}/stubs/by-id/ghost"))
        .send()
        .await
        .expect("delete missing id");
    assert_eq!(response.status().as_u16(), 404);

    server.shutdown().await;
}

#[tokio::test]
async fn delete_imposter_returns_the_removed_config_and_unbinds() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let created = client
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(created.status().as_u16(), 201);
    assert!(wait_served(port, "from-a").await);

    let response = client
        .delete(format!("http://{admin}/imposters/{port}"))
        .send()
        .await
        .expect("delete imposter");
    assert_eq!(response.status().as_u16(), 200);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["port"], port, "the removed config comes back: {body}");

    let read = reqwest::get(format!("http://{admin}/imposters/{port}"))
        .await
        .expect("get after delete");
    assert_eq!(read.status().as_u16(), 404);

    // Deleting it again is a 404 straight from the front — nothing to commit.
    let again = client
        .delete(format!("http://{admin}/imposters/{port}"))
        .send()
        .await
        .expect("delete again");
    assert_eq!(again.status().as_u16(), 404);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if served_body(port).await.is_none() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the data port must unbind after the delete applies"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    server.shutdown().await;
}

#[tokio::test]
async fn a_config_without_a_port_is_refused_with_the_typed_envelope() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();

    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(&json!({ "protocol": "http" }))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert!(
        body["errors"][0]["type"].is_string(),
        "typed error envelope, never a hand-rolled shape: {body}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn the_api_key_gates_terminated_routes_like_upstream() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(
        &state,
        &["--cluster-allow-solo", "--api-key", "sesame"],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let refused = client
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post without auth");
    assert_eq!(refused.status().as_u16(), 401);

    let accepted = client
        .post(format!("http://{admin}/imposters"))
        .header("authorization", "sesame")
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post with auth");
    assert_eq!(accepted.status().as_u16(), 201);

    server.shutdown().await;
}

#[tokio::test]
async fn a_follower_write_forwards_to_the_leader_and_the_barrier_holds() {
    let leader_state = TempDir::new().expect("tempdir");
    let leader = compose::start(cluster_cli(&leader_state, &["--cluster-allow-solo"]))
        .await
        .expect("leader starts");
    wait_ready(&leader).await;
    let seed = leader.cluster_addr().expect("cluster addr").to_string();

    let follower_state = TempDir::new().expect("tempdir");
    let follower = compose::start(cluster_cli(&follower_state, &["--cluster-seeds", &seed]))
        .await
        .expect("follower joins");
    wait_ready(&follower).await;

    let port = reserve_port();
    let response = reqwest::Client::new()
        .post(format!("http://{}/imposters", follower.admin_addr()))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post on the follower");
    assert_eq!(
        response.status().as_u16(),
        201,
        "a follower must forward, not refuse"
    );
    assert!(
        response.headers().get("rift-cluster-warnings").is_none(),
        "with both nodes healthy the barrier leaves no warning"
    );

    // The default ready-nodes barrier means the 2xx already implies the
    // follower has applied it: its local read serves the config immediately.
    let read = reqwest::get(format!("http://{}/imposters/{port}", follower.admin_addr()))
        .await
        .expect("follower-local read");
    assert_eq!(
        read.status().as_u16(),
        200,
        "read-your-write on the follower"
    );

    follower.shutdown().await;
    leader.shutdown().await;
}

#[tokio::test]
async fn writes_without_a_quorum_answer_unavailable() {
    let leader_state = TempDir::new().expect("tempdir");
    let leader = compose::start(cluster_cli(&leader_state, &["--cluster-allow-solo"]))
        .await
        .expect("leader starts");
    wait_ready(&leader).await;
    let seed = leader.cluster_addr().expect("cluster addr").to_string();

    let follower_state = TempDir::new().expect("tempdir");
    let follower = compose::start(cluster_cli(&follower_state, &["--cluster-seeds", &seed]))
        .await
        .expect("follower joins");
    wait_ready(&follower).await;

    // Kill the leader: the survivor of a 2-node cluster has no quorum, so a
    // write must answer 503 with the typed `unavailable` envelope (R4's
    // durable parking is the intents slice; the retry hint stands in).
    leader.shutdown().await;

    let port = reserve_port();
    let client = reqwest::Client::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let response = client
            .post(format!("http://{}/imposters", follower.admin_addr()))
            .json(&minimal_imposter(port))
            .send()
            .await
            .expect("post without quorum");
        let status = response.status().as_u16();
        if status == 503 {
            let body: serde_json::Value = response.json().await.expect("json");
            assert_eq!(
                body["errors"][0]["type"], "unavailable",
                "the stable type slug, never a hand-rolled shape: {body}"
            );
            break;
        }
        // The survivor may still believe in the dead leader for an election
        // timeout or two; anything other than 503 must be a transient.
        assert!(
            std::time::Instant::now() < deadline,
            "no-quorum write never surfaced 503/unavailable (last status {status})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    follower.shutdown().await;
}

#[tokio::test]
async fn a_restarted_node_rebinds_its_committed_imposters() {
    let state = TempDir::new().expect("tempdir");
    let port = reserve_port();

    {
        let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
            .await
            .expect("solo cluster starts");
        wait_ready(&server).await;
        let response = reqwest::Client::new()
            .post(format!("http://{}/imposters", server.admin_addr()))
            .json(&minimal_imposter(port))
            .send()
            .await
            .expect("post imposter");
        assert_eq!(response.status().as_u16(), 201);
        assert!(wait_served(port, "from-a").await);
        server.shutdown().await;
    }

    // Same state dir: the node resumes from its log (no re-init), catches up,
    // reconciles the engine, and only then reports Ready — at which point the
    // imposter serves again. This is the cluster-reconciled gate end to end.
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("restart resumes from durable state");
    wait_ready(&server).await;
    assert!(
        wait_served(port, "from-a").await,
        "a restarted node must rebind its committed imposters"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn replace_all_upserts_then_prunes() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let (a, b) = (reserve_port(), reserve_port());
    let client = reqwest::Client::new();

    for port in [a, b] {
        let response = client
            .post(format!("http://{admin}/imposters"))
            .json(&minimal_imposter(port))
            .send()
            .await
            .expect("post imposter");
        assert_eq!(response.status().as_u16(), 201);
    }

    // Replace the whole set with {b (changed), c}: a must be pruned, b
    // updated in place, c created.
    let c = reserve_port();
    let mut replacement_b = minimal_imposter(b);
    replacement_b["stubs"][0]["responses"][0]["is"]["body"] = json!("b-replaced");
    let response = client
        .put(format!("http://{admin}/imposters"))
        .json(&json!({ "imposters": [replacement_b, minimal_imposter(c)] }))
        .send()
        .await
        .expect("put imposters");
    assert_eq!(response.status().as_u16(), 200);
    assert!(
        response.headers().get("rift-cluster-revision").is_some(),
        "collection ops carry the revision header too"
    );

    let list: serde_json::Value = reqwest::get(format!("http://{admin}/imposters"))
        .await
        .expect("list")
        .json()
        .await
        .expect("json");
    let ports: Vec<u64> = list["imposters"]
        .as_array()
        .expect("imposters array")
        .iter()
        .filter_map(|imposter| imposter["port"].as_u64())
        .collect();
    assert!(
        !ports.contains(&u64::from(a)),
        "a must be pruned: {ports:?}"
    );
    assert!(ports.contains(&u64::from(b)) && ports.contains(&u64::from(c)));
    assert!(wait_served(b, "b-replaced").await, "b serves its new stub");
    assert!(wait_served(c, "from-a").await, "c is live");

    server.shutdown().await;
}

#[tokio::test]
async fn index_addressed_stub_edits_terminate_via_the_stored_config() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let mut config = minimal_imposter(port);
    config["stubs"] = json!([
        { "id": "a", "responses": [{ "is": { "statusCode": 200, "body": "from-a" } }] },
        { "id": "b", "responses": [{ "is": { "statusCode": 200, "body": "from-b" } }] },
    ]);
    let response = client
        .post(format!("http://{admin}/imposters"))
        .json(&config)
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 201);

    // Positional replace of index 0, positional delete of index 1.
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs/0"))
        .json(&json!({
            "id": "a",
            "responses": [{ "is": { "statusCode": 200, "body": "a-v2" } }],
        }))
        .send()
        .await
        .expect("replace at index");
    assert_eq!(response.status().as_u16(), 200);

    let response = client
        .delete(format!("http://{admin}/imposters/{port}/stubs/1"))
        .send()
        .await
        .expect("delete at index");
    assert_eq!(response.status().as_u16(), 200);

    // Out-of-bounds is a clean 404 with nothing committed.
    let response = client
        .delete(format!("http://{admin}/imposters/{port}/stubs/7"))
        .send()
        .await
        .expect("delete out of bounds");
    assert_eq!(response.status().as_u16(), 404);

    // Whole-list replace through PUT /stubs.
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs"))
        .json(&json!({
            "stubs": [
                { "id": "z", "responses": [{ "is": { "statusCode": 200, "body": "from-z" } }] },
            ],
        }))
        .send()
        .await
        .expect("replace stub list");
    assert_eq!(response.status().as_u16(), 200);

    let read: serde_json::Value = reqwest::get(format!("http://{admin}/imposters/{port}"))
        .await
        .expect("get imposter")
        .json()
        .await
        .expect("json");
    let ids: Vec<&str> = read["stubs"]
        .as_array()
        .expect("stubs")
        .iter()
        .filter_map(|s| s["id"].as_str())
        .collect();
    assert_eq!(ids, ["z"], "the list replace wins: {read}");
    assert!(wait_served(port, "from-z").await);

    server.shutdown().await;
}

#[tokio::test]
async fn barrier_none_answers_without_waiting_for_the_fleet() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(
        &state,
        &["--cluster-allow-solo", "--cluster-write-barrier", "none"],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();

    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 201);
    assert!(response.headers().get("rift-cluster-revision").is_some());

    server.shutdown().await;
}

#[tokio::test]
async fn the_injection_gate_fails_closed_on_terminated_routes() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();

    // `inject` without --allowInjection must be the same 400 upstream gives.
    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(&json!({
            "port": port,
            "protocol": "http",
            "stubs": [{ "responses": [{ "inject": "(req) => ({ statusCode: 200 })" }] }],
        }))
        .send()
        .await
        .expect("post injected imposter");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["code"], "invalid injection", "{body}");

    server.shutdown().await;
}

#[tokio::test]
async fn the_api_key_still_guards_proxied_routes() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(
        &state,
        &["--cluster-allow-solo", "--api-key", "sesame"],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let client = reqwest::Client::new();

    // A proxied read without the key is refused by the loopback admin the
    // front forwards to — the proxy must not strip or bypass its auth.
    let refused = client
        .get(format!("http://{admin}/imposters"))
        .send()
        .await
        .expect("get without auth");
    assert_eq!(refused.status().as_u16(), 401);

    let accepted = client
        .get(format!("http://{admin}/imposters"))
        .header("authorization", "sesame")
        .send()
        .await
        .expect("get with auth");
    assert_eq!(accepted.status().as_u16(), 200);

    server.shutdown().await;
}

#[tokio::test]
async fn a_dead_follower_is_named_in_the_warnings_header() {
    let leader_state = TempDir::new().expect("tempdir");
    let leader = compose::start(cluster_cli(
        &leader_state,
        &[
            "--cluster-allow-solo",
            "--cluster-write-barrier-timeout",
            "1",
        ],
    ))
    .await
    .expect("leader starts");
    wait_ready(&leader).await;
    let seed = leader.cluster_addr().expect("cluster addr").to_string();

    let f1_state = TempDir::new().expect("tempdir");
    let f1 = compose::start(cluster_cli(&f1_state, &["--cluster-seeds", &seed]))
        .await
        .expect("follower 1 joins");
    wait_ready(&f1).await;
    let f2_state = TempDir::new().expect("tempdir");
    let f2 = compose::start(cluster_cli(&f2_state, &["--cluster-seeds", &seed]))
        .await
        .expect("follower 2 joins");
    wait_ready(&f2).await;

    // Kill one follower: 2 of 3 keep quorum, so the write commits — and the
    // barrier names exactly the dead node in the warnings header.
    f2.shutdown().await;

    let port = reserve_port();
    let response = reqwest::Client::new()
        .post(format!("http://{}/imposters", leader.admin_addr()))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post with a dead follower");
    assert_eq!(
        response.status().as_u16(),
        201,
        "a majority still commits; the barrier only warns"
    );
    let warnings = response
        .headers()
        .get("rift-cluster-warnings")
        .and_then(|v| v.to_str().ok())
        .expect("warnings header names the unapplied node");
    assert!(
        warnings.starts_with("unapplied="),
        "documented warning shape: {warnings}"
    );

    f1.shutdown().await;
    leader.shutdown().await;
}

/// A fixed-bind variant so a node can restart on the same cluster address —
/// membership entries carry the address, so a restarted peer must reclaim it.
fn cluster_cli_at(state: &TempDir, bind: &str, extra: &[&str]) -> EeCli {
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
        "127.0.0.1:0".to_owned(),
        "--cluster-secret".to_owned(),
        SECRET.to_owned(),
        "--cluster-state-dir".to_owned(),
        state.path().to_string_lossy().into_owned(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

/// Issue #9 slice 3: retrying with the same Idempotency-Key is exactly once —
/// same revision back, single application.
#[tokio::test]
async fn an_idempotency_key_makes_retries_exactly_once() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let created = client
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(created.status().as_u16(), 201);

    let add = || {
        client
            .post(format!("http://{admin}/imposters/{port}/stubs"))
            .header("idempotency-key", "retry-me-please")
            .json(&json!({
                "stub": {
                    "id": "b",
                    "responses": [{ "is": { "statusCode": 200, "body": "from-b" } }],
                },
            }))
            .send()
    };
    let first = add().await.expect("first add");
    assert_eq!(first.status().as_u16(), 200);
    let first_revision = first
        .headers()
        .get("rift-cluster-revision")
        .and_then(|v| v.to_str().ok())
        .expect("revision header")
        .to_owned();

    let retry = add().await.expect("retried add");
    assert_eq!(retry.status().as_u16(), 200);
    let retry_revision = retry
        .headers()
        .get("rift-cluster-revision")
        .and_then(|v| v.to_str().ok())
        .expect("revision header");
    assert_eq!(
        retry_revision, first_revision,
        "the retry must collapse to the original application"
    );

    let read: serde_json::Value = reqwest::get(format!("http://{admin}/imposters/{port}"))
        .await
        .expect("get imposter")
        .json()
        .await
        .expect("json");
    assert_eq!(
        read["stubs"].as_array().expect("stubs").len(),
        2,
        "stub b applied exactly once: {read}"
    );

    server.shutdown().await;
}

/// Issue #9 slice 3, R4 end to end: a write refused for lack of quorum is
/// parked — with its op id on the 503 — and applies BY ITSELF once quorum
/// returns, with no client retry.
#[tokio::test]
async fn a_parked_write_replays_when_quorum_returns() {
    let leader_bind = {
        let held = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve");
        held.local_addr().expect("addr").to_string()
    };
    let leader_state = TempDir::new().expect("tempdir");
    let leader = compose::start(cluster_cli_at(
        &leader_state,
        &leader_bind,
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("leader starts");
    wait_ready(&leader).await;
    let seed = leader.cluster_addr().expect("cluster addr").to_string();

    let follower_state = TempDir::new().expect("tempdir");
    let follower = compose::start(cluster_cli(&follower_state, &["--cluster-seeds", &seed]))
        .await
        .expect("follower joins");
    wait_ready(&follower).await;

    leader.shutdown().await;

    // Park a write on the quorum-less survivor: 503, but with the op id.
    let port = reserve_port();
    let client = reqwest::Client::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let response = client
            .post(format!("http://{}/imposters", follower.admin_addr()))
            .json(&minimal_imposter(port))
            .send()
            .await
            .expect("post without quorum");
        if response.status().as_u16() == 503 {
            assert!(
                response.headers().get("rift-cluster-op-id").is_some(),
                "the 503 must carry the parked op id"
            );
            assert!(response.headers().get("retry-after").is_some());
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no-quorum write never surfaced 503"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Quorum returns: the old leader restarts on its fixed address and resumes
    // from its log. The survivor's replay loop must apply the parked intent
    // with NO further client action.
    let leader = compose::start(cluster_cli_at(
        &leader_state,
        &leader_bind,
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("leader restarts");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) =
            reqwest::get(format!("http://{}/imposters/{port}", follower.admin_addr())).await
            && response.status().as_u16() == 200
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the parked intent never replayed after quorum returned"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    follower.shutdown().await;
    leader.shutdown().await;
}

/// Issue #9 slice 3: `--cluster-admin-async` answers 202 + op id after parking
/// and the write applies in the background.
#[tokio::test]
async fn async_mode_answers_202_and_applies_in_the_background() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(
        &state,
        &["--cluster-allow-solo", "--cluster-admin-async"],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();

    let post = || {
        reqwest::Client::new()
            .post(format!("http://{admin}/imposters"))
            .header("idempotency-key", "async-once")
            .json(&minimal_imposter(port))
            .send()
    };
    let response = post().await.expect("post imposter");
    assert_eq!(response.status().as_u16(), 202);
    assert!(response.headers().get("rift-cluster-op-id").is_some());
    let body: serde_json::Value = response.json().await.expect("json");
    let op_id = body["opId"].as_str().expect("opId").to_owned();
    uuid::Uuid::parse_str(&op_id).expect("op id is a uuid");
    assert_eq!(
        body["opIds"],
        json!([op_id]),
        "single-op mutations poll the base id itself"
    );

    // The same Idempotency-Key answers the same op id — a client retrying the
    // 202 keeps polling one op, and dedup keeps the apply single.
    let retry: serde_json::Value = post()
        .await
        .expect("retried post")
        .json()
        .await
        .expect("json");
    assert_eq!(retry["opId"].as_str().expect("opId"), op_id);

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(read) = reqwest::get(format!("http://{admin}/imposters/{port}")).await
            && read.status().as_u16() == 200
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the async write never applied"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        wait_served(port, "from-a").await,
        "and the engine serves it"
    );

    server.shutdown().await;
}

/// Issue #9 slice 3: a keyed multi-op mutation (`PUT /imposters`) retried with
/// the same Idempotency-Key stays exactly-once for the whole sequence.
#[tokio::test]
async fn a_keyed_replace_all_retries_exactly_once() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let (a, b) = (reserve_port(), reserve_port());
    let client = reqwest::Client::new();

    let put = || {
        client
            .put(format!("http://{admin}/imposters"))
            .header("idempotency-key", "replace-once")
            .json(&json!({ "imposters": [minimal_imposter(a), minimal_imposter(b)] }))
            .send()
    };
    let first = put().await.expect("first put");
    assert_eq!(first.status().as_u16(), 200);
    let first_revision = first
        .headers()
        .get("rift-cluster-revision")
        .and_then(|v| v.to_str().ok())
        .expect("revision header")
        .to_owned();

    let retry = put().await.expect("retried put");
    assert_eq!(retry.status().as_u16(), 200);
    assert_eq!(
        retry
            .headers()
            .get("rift-cluster-revision")
            .and_then(|v| v.to_str().ok())
            .expect("revision header"),
        first_revision,
        "every op in the sequence must dedup, so the last revision is unchanged"
    );

    let list: serde_json::Value = reqwest::get(format!("http://{admin}/imposters"))
        .await
        .expect("list")
        .json()
        .await
        .expect("json");
    assert_eq!(
        list["imposters"].as_array().expect("imposters").len(),
        2,
        "{list}"
    );

    server.shutdown().await;
}

/// Issue #9 slice 3, the crash half of R4: the node that PARKED the intent
/// restarts, and its own on-disk ledger — not the client — is what completes
/// the write once quorum returns.
#[tokio::test]
async fn a_restarted_node_replays_its_own_parked_intents() {
    let reserve_addr = || {
        let held = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve");
        held.local_addr().expect("addr").to_string()
    };
    let (leader_bind, follower_bind) = (reserve_addr(), reserve_addr());
    let leader_state = TempDir::new().expect("tempdir");
    let follower_state = TempDir::new().expect("tempdir");

    let leader = compose::start(cluster_cli_at(
        &leader_state,
        &leader_bind,
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("leader starts");
    wait_ready(&leader).await;
    let seed = leader.cluster_addr().expect("cluster addr").to_string();

    let follower = compose::start(cluster_cli_at(
        &follower_state,
        &follower_bind,
        &["--cluster-seeds", &seed],
    ))
    .await
    .expect("follower joins");
    wait_ready(&follower).await;

    // Lose quorum, park a write on the follower.
    leader.shutdown().await;
    let port = reserve_port();
    let client = reqwest::Client::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let response = client
            .post(format!("http://{}/imposters", follower.admin_addr()))
            .json(&minimal_imposter(port))
            .send()
            .await
            .expect("post without quorum");
        if response.status().as_u16() == 503 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "never saw the 503");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !follower
            .node()
            .expect("clustered")
            .parked_intents()
            .expect("intents")
            .is_empty(),
        "the refused write must be parked"
    );

    // Kill the parking node itself, then bring the whole cluster back.
    follower.shutdown().await;
    let leader = compose::start(cluster_cli_at(
        &leader_state,
        &leader_bind,
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("leader restarts");
    let follower = compose::start(cluster_cli_at(
        &follower_state,
        &follower_bind,
        &["--cluster-seeds", &seed],
    ))
    .await
    .expect("follower restarts from its durable state");

    // No client in sight: the follower's replay loop must finish the write
    // from its on-disk ledger.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) =
            reqwest::get(format!("http://{}/imposters/{port}", follower.admin_addr())).await
            && response.status().as_u16() == 200
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the restarted node never replayed its own parked intent"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    follower.shutdown().await;
    leader.shutdown().await;
}

/// Issue #15 / #9 slice 4: a pause is replicated config — it answers through
/// the control plane, survives a restart, and resuming finds the imposter's
/// stubs intact (the toggle applies in place, never a replace).
#[tokio::test]
async fn disable_replicates_survives_restart_and_preserves_state() {
    let bind = {
        let held = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve");
        held.local_addr().expect("addr").to_string()
    };
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli_at(&state, &bind, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let created = client
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(created.status().as_u16(), 201);
    assert!(wait_served(port, "from-a").await);

    let response = client
        .post(format!("http://{admin}/imposters/{port}/disable"))
        .send()
        .await
        .expect("disable");
    assert_eq!(response.status().as_u16(), 200);
    assert!(
        response.headers().get("rift-cluster-revision").is_some(),
        "the toggle is a replicated write and says so"
    );
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["message"], "Imposter disabled", "upstream's own shape");

    // The data plane answers 503 while paused (still bound, not gone).
    let paused = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .expect("paused imposter still answers TCP");
    assert_eq!(paused.status().as_u16(), 503);

    let read: serde_json::Value = reqwest::get(format!("http://{admin}/imposters/{port}"))
        .await
        .expect("get imposter")
        .json()
        .await
        .expect("json");
    assert_eq!(read["enabled"], false, "{read}");

    // Restart on the same state: the pause must survive (the whole point).
    server.shutdown().await;
    let server = compose::start(cluster_cli_at(&state, &bind, &["--cluster-allow-solo"]))
        .await
        .expect("restart");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let paused = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .expect("still bound after restart");
    assert_eq!(
        paused.status().as_u16(),
        503,
        "a restart must not silently re-enable a paused imposter"
    );

    // Resume: the stub set survived the pause/restart cycle intact.
    let response = client
        .post(format!("http://{admin}/imposters/{port}/enable"))
        .send()
        .await
        .expect("enable");
    assert_eq!(response.status().as_u16(), 200);
    assert!(
        wait_served(port, "from-a").await,
        "resume serves the original stub — nothing was reset"
    );

    server.shutdown().await;
}

/// Issue #15: the pause converges — disabling through one node's admin stops
/// the OTHER node's engine serving within the write barrier.
#[tokio::test]
async fn disable_on_one_node_pauses_the_fleet() {
    let leader_state = TempDir::new().expect("tempdir");
    let leader = compose::start(cluster_cli(&leader_state, &["--cluster-allow-solo"]))
        .await
        .expect("leader starts");
    wait_ready(&leader).await;
    let seed = leader.cluster_addr().expect("cluster addr").to_string();
    let follower_state = TempDir::new().expect("tempdir");
    let follower = compose::start(cluster_cli(&follower_state, &["--cluster-seeds", &seed]))
        .await
        .expect("follower joins");
    wait_ready(&follower).await;

    let port = reserve_port();
    let client = reqwest::Client::new();
    let created = client
        .post(format!("http://{}/imposters", leader.admin_addr()))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(created.status().as_u16(), 201);
    assert!(wait_served(port, "from-a").await);

    // Disable through the FOLLOWER's admin (forward path), barrier default on:
    // the 2xx implies every node has applied the pause.
    let response = client
        .post(format!(
            "http://{}/imposters/{port}/disable",
            follower.admin_addr()
        ))
        .send()
        .await
        .expect("disable via follower");
    assert_eq!(response.status().as_u16(), 200);

    let paused = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .expect("paused imposter answers");
    assert_eq!(
        paused.status().as_u16(),
        503,
        "the fleet's engine is paused"
    );

    // Both nodes' config planes agree — the barrier's 2xx promised exactly this.
    for admin in [leader.admin_addr(), follower.admin_addr()] {
        let view: serde_json::Value = reqwest::get(format!("http://{admin}/imposters/{port}"))
            .await
            .expect("get imposter")
            .json()
            .await
            .expect("json");
        assert_eq!(view["enabled"], false, "node {admin} disagrees: {view}");
    }

    follower.shutdown().await;
    leader.shutdown().await;
}

/// Issue #15: toggling a port that never existed answers 404 through the
/// front — the typed shape, not a committed no-op dressed as success.
#[tokio::test]
async fn disabling_a_ghost_port_is_a_404() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;

    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/imposters/59999/disable",
            server.admin_addr()
        ))
        .send()
        .await
        .expect("disable ghost");
    assert_eq!(response.status().as_u16(), 404);
    let body: serde_json::Value = response.json().await.expect("json");
    assert!(
        body["errors"][0]["type"].is_string(),
        "typed envelope: {body}"
    );

    server.shutdown().await;
}

/// Issue #15: a replayed toggle (same Idempotency-Key) collapses to the
/// original application — same revision both times.
#[tokio::test]
async fn a_keyed_toggle_retries_exactly_once() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let created = client
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(created.status().as_u16(), 201);

    let disable = || {
        client
            .post(format!("http://{admin}/imposters/{port}/disable"))
            .header("idempotency-key", "pause-once")
            .send()
    };
    let first = disable().await.expect("first disable");
    assert_eq!(first.status().as_u16(), 200);
    let first_revision = first
        .headers()
        .get("rift-cluster-revision")
        .and_then(|v| v.to_str().ok())
        .expect("revision header")
        .to_owned();

    let retry = disable().await.expect("retried disable");
    assert_eq!(retry.status().as_u16(), 200);
    assert_eq!(
        retry
            .headers()
            .get("rift-cluster-revision")
            .and_then(|v| v.to_str().ok())
            .expect("revision header"),
        first_revision,
        "the retry must collapse to the original toggle"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Issue #47: `_rift.script` `file:`/`ref:` sources resolve on the accepting
// node before replication (upstream #356: nothing unresolved is ever stored).
// ---------------------------------------------------------------------------

fn file_script_imposter(port: u16, file: &str) -> serde_json::Value {
    json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "responses": [{ "_rift": { "script": { "file": file } } }],
        }],
    })
}

async fn rendered_imposter(admin: std::net::SocketAddr, port: u16) -> (u16, serde_json::Value) {
    let response = reqwest::get(format!("http://{admin}/imposters/{port}"))
        .await
        .expect("get imposter");
    let status = response.status().as_u16();
    let body = response.json().await.unwrap_or(serde_json::Value::Null);
    (status, body)
}

const GREET_RHAI: &str = r#"fn respond(ctx) { pass() }"#;

#[tokio::test]
async fn file_scripts_resolve_before_replication() {
    let state = TempDir::new().expect("tempdir");
    let scripts = TempDir::new().expect("scripts dir");
    std::fs::write(scripts.path().join("greet.rhai"), GREET_RHAI).expect("write script");
    let scripts_dir = scripts.path().to_string_lossy().into_owned();
    let server = compose::start(cluster_cli(
        &state,
        &[
            "--cluster-allow-solo",
            "--allowInjection",
            "--scripts-dir",
            &scripts_dir,
        ],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();

    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(&file_script_imposter(port, "greet.rhai"))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 201);

    // The render is the applied replicated state: inline code, no `file` left.
    let (status, body) = rendered_imposter(admin, port).await;
    assert_eq!(status, 200, "{body}");
    let script = &body["stubs"][0]["responses"][0]["_rift"]["script"];
    assert_eq!(script["code"], GREET_RHAI, "{body}");
    assert_eq!(script["engine"], "rhai", "{body}");
    assert!(
        script.get("file").is_none(),
        "unresolved file ref replicated: {body}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn ref_scripts_resolve_before_replication() {
    let state = TempDir::new().expect("tempdir");
    let scripts = TempDir::new().expect("scripts dir");
    std::fs::write(scripts.path().join("greet.rhai"), GREET_RHAI).expect("write script");
    let scripts_dir = scripts.path().to_string_lossy().into_owned();
    let server = compose::start(cluster_cli(
        &state,
        &[
            "--cluster-allow-solo",
            "--allowInjection",
            "--scripts-dir",
            &scripts_dir,
        ],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();

    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(&json!({
            "port": port,
            "protocol": "http",
            "_rift": { "scripts": { "greet": { "file": "greet.rhai" } } },
            "stubs": [{
                "responses": [{ "_rift": { "script": { "ref": "greet" } } }],
            }],
        }))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 201);

    let (status, body) = rendered_imposter(admin, port).await;
    assert_eq!(status, 200, "{body}");
    let script = &body["stubs"][0]["responses"][0]["_rift"]["script"];
    assert_eq!(script["code"], GREET_RHAI, "{body}");
    assert!(
        script.get("ref").is_none(),
        "unresolved ref replicated: {body}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn add_stub_resolves_ref_against_the_stored_registry() {
    let state = TempDir::new().expect("tempdir");
    let scripts = TempDir::new().expect("scripts dir");
    std::fs::write(scripts.path().join("greet.rhai"), GREET_RHAI).expect("write script");
    let scripts_dir = scripts.path().to_string_lossy().into_owned();
    let server = compose::start(cluster_cli(
        &state,
        &[
            "--cluster-allow-solo",
            "--allowInjection",
            "--scripts-dir",
            &scripts_dir,
        ],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{admin}/imposters"))
        .json(&json!({
            "port": port,
            "protocol": "http",
            "_rift": { "scripts": { "greet": { "file": "greet.rhai" } } },
            "stubs": [{
                "id": "a",
                "responses": [{ "is": { "statusCode": 200, "body": "plain" } }],
            }],
        }))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 201);

    // A ref: stub-add resolves against the stored, already-resolved registry.
    let response = client
        .post(format!("http://{admin}/imposters/{port}/stubs"))
        .json(&json!({
            "stub": { "responses": [{ "_rift": { "script": { "ref": "greet" } } }] },
        }))
        .send()
        .await
        .expect("add stub");
    assert_eq!(response.status().as_u16(), 200);
    let (_, body) = rendered_imposter(admin, port).await;
    let script = &body["stubs"][1]["responses"][0]["_rift"]["script"];
    assert_eq!(script["code"], GREET_RHAI, "{body}");
    assert!(
        script.get("ref").is_none(),
        "unresolved ref replicated: {body}"
    );

    // An unknown ref is refused with upstream's message; nothing is stored.
    let response = client
        .post(format!("http://{admin}/imposters/{port}/stubs"))
        .json(&json!({
            "stub": { "responses": [{ "_rift": { "script": { "ref": "nope" } } }] },
        }))
        .send()
        .await
        .expect("add bad stub");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["code"], "400", "{body}");
    assert_eq!(body["errors"][0]["type"], "bad data", "{body}");
    let message = body["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(
        message.starts_with("Script resolution failed:")
            && message.contains("unknown script ref 'nope'"),
        "{body}"
    );
    let (_, body) = rendered_imposter(admin, port).await;
    assert_eq!(body["stubs"].as_array().map(Vec::len), Some(2), "{body}");

    server.shutdown().await;
}

#[tokio::test]
async fn script_resolution_failure_is_upstream_400_and_nothing_is_stored() {
    let state = TempDir::new().expect("tempdir");
    let scripts = TempDir::new().expect("scripts dir");
    let scripts_dir = scripts.path().to_string_lossy().into_owned();
    let server = compose::start(cluster_cli(
        &state,
        &[
            "--cluster-allow-solo",
            "--allowInjection",
            "--scripts-dir",
            &scripts_dir,
        ],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let client = reqwest::Client::new();

    // (a) missing file → upstream 400 shape, and nothing was committed.
    let port = reserve_port();
    let response = client
        .post(format!("http://{admin}/imposters"))
        .json(&file_script_imposter(port, "missing.rhai"))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["code"], "400", "{body}");
    assert_eq!(body["errors"][0]["type"], "bad data", "{body}");
    let message = body["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(message.starts_with("Script resolution failed:"), "{body}");
    let (status, _) = rendered_imposter(admin, port).await;
    assert_eq!(status, 404, "a refused imposter must not be committed");

    // (b) a path escaping --scripts-dir is rejected without reading.
    let response = client
        .post(format!("http://{admin}/imposters"))
        .json(&file_script_imposter(port, "../escape.rhai"))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["code"], "400", "{body}");
    assert_eq!(body["errors"][0]["type"], "bad data", "{body}");
    let message = body["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(message.contains("escapes the scripts root"), "{body}");
    let (status, _) = rendered_imposter(admin, port).await;
    assert_eq!(status, 404, "a refused imposter must not be committed");
    server.shutdown().await;

    // (c) file: with no --scripts-dir configured is refused outright.
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(
        &state,
        &["--cluster-allow-solo", "--allowInjection"],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(&file_script_imposter(port, "greet.rhai"))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["code"], "400", "{body}");
    assert_eq!(body["errors"][0]["type"], "bad data", "{body}");
    let message = body["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("cannot be resolved: no --scripts-dir"),
        "{body}"
    );
    let (status, _) = rendered_imposter(admin, port).await;
    assert_eq!(status, 404, "a refused imposter must not be committed");

    server.shutdown().await;
}

#[tokio::test]
async fn batch_resolution_failure_refuses_the_whole_put() {
    let state = TempDir::new().expect("tempdir");
    let scripts = TempDir::new().expect("scripts dir");
    let scripts_dir = scripts.path().to_string_lossy().into_owned();
    let server = compose::start(cluster_cli(
        &state,
        &[
            "--cluster-allow-solo",
            "--allowInjection",
            "--scripts-dir",
            &scripts_dir,
        ],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let good_port = reserve_port();
    let bad_port = reserve_port();

    let response = reqwest::Client::new()
        .put(format!("http://{admin}/imposters"))
        .json(&json!({
            "imposters": [
                minimal_imposter(good_port),
                file_script_imposter(bad_port, "missing.rhai"),
            ],
        }))
        .send()
        .await
        .expect("put imposters");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["code"], "400", "{body}");
    assert_eq!(body["errors"][0]["type"], "bad data", "{body}");
    let message = body["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(
        message.starts_with(&format!(
            "Script resolution failed in imposter[1] (port Some({bad_port}))"
        )),
        "{body}"
    );
    // The whole batch is refused pre-park: imposter[0] must not exist either.
    let (status, _) = rendered_imposter(admin, good_port).await;
    assert_eq!(status, 404, "a refused batch must commit nothing");

    server.shutdown().await;
}

#[tokio::test]
async fn the_injection_gate_still_wins_over_resolution() {
    let state = TempDir::new().expect("tempdir");
    // Injection OFF and no scripts dir: a file: script surface must be refused
    // by the gate (invalid injection), never reach resolution.
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();

    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(&file_script_imposter(reserve_port(), "greet.rhai"))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["code"], "invalid injection", "{body}");

    server.shutdown().await;
}

#[tokio::test]
async fn replace_stub_routes_resolve_scripts() {
    let state = TempDir::new().expect("tempdir");
    let scripts = TempDir::new().expect("scripts dir");
    std::fs::write(scripts.path().join("greet.rhai"), GREET_RHAI).expect("write script");
    let scripts_dir = scripts.path().to_string_lossy().into_owned();
    let server = compose::start(cluster_cli(
        &state,
        &[
            "--cluster-allow-solo",
            "--allowInjection",
            "--scripts-dir",
            &scripts_dir,
        ],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{admin}/imposters"))
        .json(&json!({
            "port": port,
            "protocol": "http",
            "_rift": { "scripts": { "greet": { "file": "greet.rhai" } } },
            "stubs": [
                { "id": "a", "responses": [{ "is": { "statusCode": 200, "body": "a" } }] },
                { "id": "b", "responses": [{ "is": { "statusCode": 200, "body": "b" } }] },
            ],
        }))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 201);

    // ReplaceStubAt (index-addressed): a file: script resolves through the
    // full-config PutImposter path.
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs/0"))
        .json(&json!({ "responses": [{ "_rift": { "script": { "file": "greet.rhai" } } }] }))
        .send()
        .await
        .expect("replace stub at 0");
    assert_eq!(response.status().as_u16(), 200);
    let (_, body) = rendered_imposter(admin, port).await;
    let script = &body["stubs"][0]["responses"][0]["_rift"]["script"];
    assert_eq!(script["code"], GREET_RHAI, "{body}");
    assert!(
        script.get("file").is_none(),
        "unresolved file ref replicated: {body}"
    );

    // ReplaceStubById: a ref: script resolves against the stored registry
    // through the PatchStubs path.
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs/by-id/b"))
        .json(&json!({ "responses": [{ "_rift": { "script": { "ref": "greet" } } }] }))
        .send()
        .await
        .expect("replace stub by id");
    assert_eq!(response.status().as_u16(), 200);
    let (_, body) = rendered_imposter(admin, port).await;
    let script = &body["stubs"][1]["responses"][0]["_rift"]["script"];
    assert_eq!(script["code"], GREET_RHAI, "{body}");
    assert!(
        script.get("ref").is_none(),
        "unresolved ref replicated: {body}"
    );

    // ReplaceStubs (list replace): resolves via the stored config's registry.
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs"))
        .json(&json!({
            "stubs": [{ "responses": [{ "_rift": { "script": { "ref": "greet" } } }] }],
        }))
        .send()
        .await
        .expect("replace stubs");
    assert_eq!(response.status().as_u16(), 200);
    let (_, body) = rendered_imposter(admin, port).await;
    assert_eq!(body["stubs"].as_array().map(Vec::len), Some(1), "{body}");
    let script = &body["stubs"][0]["responses"][0]["_rift"]["script"];
    assert_eq!(script["code"], GREET_RHAI, "{body}");
    assert!(
        script.get("ref").is_none(),
        "unresolved ref replicated: {body}"
    );

    // A failing replace leaves the stored stub untouched.
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs/0"))
        .json(&json!({ "responses": [{ "_rift": { "script": { "file": "missing.rhai" } } }] }))
        .send()
        .await
        .expect("replace stub with missing file");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["code"], "400", "{body}");
    assert_eq!(body["errors"][0]["type"], "bad data", "{body}");
    let message = body["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(message.starts_with("Script resolution failed:"), "{body}");
    let (_, body) = rendered_imposter(admin, port).await;
    let script = &body["stubs"][0]["responses"][0]["_rift"]["script"];
    assert_eq!(
        script["code"], GREET_RHAI,
        "a refused replace must change nothing: {body}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn ref_against_absent_imposter_is_unknown_ref_400() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(
        &state,
        &["--cluster-allow-solo", "--allowInjection"],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();

    // Resolution runs before any imposter-exists check (upstream's observable
    // order): an unknown ref against an absent imposter is 400, not 404.
    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters/{port}/stubs"))
        .json(&json!({
            "stub": { "responses": [{ "_rift": { "script": { "ref": "nope" } } }] },
        }))
        .send()
        .await
        .expect("add stub to absent imposter");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["code"], "400", "{body}");
    assert_eq!(body["errors"][0]["type"], "bad data", "{body}");
    let message = body["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(
        message.starts_with("Script resolution failed:")
            && message.contains("unknown script ref 'nope'"),
        "{body}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn batch_put_resolves_file_scripts() {
    let state = TempDir::new().expect("tempdir");
    let scripts = TempDir::new().expect("scripts dir");
    std::fs::write(scripts.path().join("greet.rhai"), GREET_RHAI).expect("write script");
    let scripts_dir = scripts.path().to_string_lossy().into_owned();
    let server = compose::start(cluster_cli(
        &state,
        &[
            "--cluster-allow-solo",
            "--allowInjection",
            "--scripts-dir",
            &scripts_dir,
        ],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let first = reserve_port();
    let second = reserve_port();

    let response = reqwest::Client::new()
        .put(format!("http://{admin}/imposters"))
        .json(&json!({
            "imposters": [
                file_script_imposter(first, "greet.rhai"),
                file_script_imposter(second, "greet.rhai"),
            ],
        }))
        .send()
        .await
        .expect("put imposters");
    assert_eq!(response.status().as_u16(), 200);
    for port in [first, second] {
        let (status, body) = rendered_imposter(admin, port).await;
        assert_eq!(status, 200, "{body}");
        let script = &body["stubs"][0]["responses"][0]["_rift"]["script"];
        assert_eq!(script["code"], GREET_RHAI, "{body}");
        assert!(
            script.get("file").is_none(),
            "unresolved file ref replicated: {body}"
        );
    }

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Issue #46: expected-revision preconditions (If-Match) on admin writes.
// ---------------------------------------------------------------------------

fn revision_of(response: &reqwest::Response) -> String {
    response
        .headers()
        .get("rift-cluster-revision")
        .expect("mutation responses carry the revision header")
        .to_str()
        .expect("ascii")
        .to_owned()
}

#[tokio::test]
async fn if_match_preconditions_guard_single_imposter_writes() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--cluster-allow-solo"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 201);
    let rev1 = revision_of(&response);

    // A matching If-Match applies and returns the new revision.
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs/0"))
        .header("if-match", &rev1)
        .json(&json!({ "responses": [{ "is": { "statusCode": 200, "body": "first-writer" } }] }))
        .send()
        .await
        .expect("conditioned edit");
    assert_eq!(response.status().as_u16(), 200);
    let rev2 = revision_of(&response);
    assert_ne!(rev1, rev2, "a successful mutation advances the revision");

    // Replaying the now-stale token is a 409 resource conflict, upstream shape.
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs/0"))
        .header("if-match", &rev1)
        .json(&json!({ "responses": [{ "is": { "statusCode": 200, "body": "late-writer" } }] }))
        .send()
        .await
        .expect("stale conditioned edit");
    assert_eq!(response.status().as_u16(), 409);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["code"], "409", "{body}");
    assert_eq!(body["errors"][0]["type"], "resource conflict", "{body}");
    let message = body["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(message.starts_with("revision conflict"), "{body}");

    // The first edit won and stayed won.
    let read: serde_json::Value = reqwest::get(format!("http://{admin}/imposters/{port}"))
        .await
        .expect("get")
        .json()
        .await
        .expect("json");
    assert_eq!(
        read["stubs"][0]["responses"][0]["is"]["body"], "first-writer",
        "{read}"
    );

    // Without If-Match the write is unconditional: last-writer-wins unchanged.
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs/0"))
        .json(&json!({ "responses": [{ "is": { "statusCode": 200, "body": "unconditional" } }] }))
        .send()
        .await
        .expect("unconditional edit");
    assert_eq!(response.status().as_u16(), 200);

    // A collection-wide mutation cannot be conditioned on a per-record token.
    let response = client
        .put(format!("http://{admin}/imposters"))
        .header("if-match", &rev2)
        .json(&json!({ "imposters": [minimal_imposter(port)] }))
        .send()
        .await
        .expect("conditioned collection write");
    assert_eq!(response.status().as_u16(), 400);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["errors"][0]["type"], "bad data", "{body}");

    // Expecting a revision of an absent record cannot hold: committed 409.
    let absent = reserve_port();
    let response = client
        .post(format!("http://{admin}/imposters"))
        .header("if-match", format!("default:{absent}@5"))
        .json(&minimal_imposter(absent))
        .send()
        .await
        .expect("conditioned create of an absent record");
    assert_eq!(response.status().as_u16(), 409);

    // Malformed preconditions refuse over HTTP too — never parse-and-ignore.
    for bad in ["*", "not-a-revision"] {
        let response = client
            .put(format!("http://{admin}/imposters/{port}/stubs/0"))
            .header("if-match", bad)
            .json(&json!({ "responses": [{ "is": { "statusCode": 200, "body": "x" } }] }))
            .send()
            .await
            .expect("malformed if-match");
        assert_eq!(response.status().as_u16(), 400, "{bad:?}");
        let body: serde_json::Value = response.json().await.expect("json");
        assert_eq!(body["errors"][0]["type"], "bad data", "{bad:?}: {body}");
    }
    // A present-but-unreadable header must refuse, not degrade to
    // unconditional (the silent-CAS-bypass failure mode).
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs/0"))
        .header(
            "if-match",
            reqwest::header::HeaderValue::from_bytes(b"caf\xc3\xa9").expect("opaque bytes"),
        )
        .json(&json!({ "responses": [{ "is": { "statusCode": 200, "body": "x" } }] }))
        .send()
        .await
        .expect("non-ascii if-match");
    assert_eq!(response.status().as_u16(), 400);

    // DELETE honors the precondition like every other single-imposter route.
    let response = client
        .delete(format!("http://{admin}/imposters/{port}"))
        .header("if-match", &rev1)
        .send()
        .await
        .expect("stale conditioned delete");
    assert_eq!(
        response.status().as_u16(),
        409,
        "a stale delete must refuse"
    );
    let current = revision_of(
        &client
            .put(format!("http://{admin}/imposters/{port}/stubs/0"))
            .json(&json!({ "responses": [{ "is": { "statusCode": 200, "body": "final" } }] }))
            .send()
            .await
            .expect("refresh revision"),
    );
    let response = client
        .delete(format!("http://{admin}/imposters/{port}"))
        .header("if-match", &current)
        .send()
        .await
        .expect("current conditioned delete");
    assert_eq!(
        response.status().as_u16(),
        200,
        "a current-token delete applies"
    );

    server.shutdown().await;
}

/// #46: a conditioned write in async-admin mode is parked, replayed by the
/// background submit, and the state machine's refusal still holds — the
/// conflicting edit never lands, and the ops surface reports the committed
/// refusal.
#[tokio::test]
async fn an_async_conditioned_conflict_is_parked_then_refused() {
    use rift_cluster::rpc::{AlwaysHealthy, RpcClient, RpcClientConfig, Signer};
    use std::sync::Arc;

    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(
        &state,
        &["--cluster-allow-solo", "--cluster-admin-async"],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = reserve_port();
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 202);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(read) = reqwest::get(format!("http://{admin}/imposters/{port}")).await
            && read.status().as_u16() == 200
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the async create never applied"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A hopeless precondition still answers 202 (accepted-and-parked); the
    // refusal is the SM's committed outcome, surfaced via the ops endpoint.
    let response = client
        .put(format!("http://{admin}/imposters/{port}/stubs/0"))
        .header("if-match", "999999")
        .json(&json!({ "responses": [{ "is": { "statusCode": 200, "body": "clobber" } }] }))
        .send()
        .await
        .expect("conditioned async edit");
    assert_eq!(response.status().as_u16(), 202);
    let body: serde_json::Value = response.json().await.expect("json");
    let op_id = body["opId"].as_str().expect("opId").to_owned();

    let rpc = RpcClient::new(
        Some(Signer::new(SECRET)),
        Arc::new(AlwaysHealthy),
        RpcClientConfig::default(),
    );
    let ops_addr = server.cluster_addr().expect("cluster addr");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let reported = loop {
        let raw = rpc
            .call(
                ops_addr,
                "GET",
                &format!("/_cluster/ops/{op_id}"),
                Vec::new(),
            )
            .await
            .expect("ops endpoint answers");
        let reported: serde_json::Value = serde_json::from_slice(&raw).expect("json");
        if reported["state"] != "pending" {
            break reported;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the parked op never resolved"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(reported["state"], "failed", "{reported}");
    let detail = reported["detail"].as_str().unwrap_or_default();
    assert!(detail.starts_with("revision conflict"), "{reported}");

    // And the conflicting edit never landed.
    let read: serde_json::Value = reqwest::get(format!("http://{admin}/imposters/{port}"))
        .await
        .expect("get")
        .json()
        .await
        .expect("json");
    assert_eq!(
        read["stubs"][0]["responses"][0]["is"]["body"], "from-a",
        "{read}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn a_stale_if_match_cannot_clobber_through_a_follower() {
    let leader_state = TempDir::new().expect("tempdir");
    let leader = compose::start(cluster_cli(&leader_state, &["--cluster-allow-solo"]))
        .await
        .expect("leader starts");
    wait_ready(&leader).await;
    let seed = leader.cluster_addr().expect("cluster addr").to_string();

    let follower_state = TempDir::new().expect("tempdir");
    let follower = compose::start(cluster_cli(&follower_state, &["--cluster-seeds", &seed]))
        .await
        .expect("follower joins");
    wait_ready(&follower).await;

    let port = reserve_port();
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/imposters", leader.admin_addr()))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    assert_eq!(response.status().as_u16(), 201);
    let rev1 = revision_of(&response);

    // Writer A mutates through the leader unconditionally.
    let response = client
        .put(format!(
            "http://{}/imposters/{port}/stubs/0",
            leader.admin_addr()
        ))
        .json(&json!({ "responses": [{ "is": { "statusCode": 200, "body": "writer-a" } }] }))
        .send()
        .await
        .expect("writer A");
    assert_eq!(response.status().as_u16(), 200);

    // Writer B, holding the pre-A token, writes through the FOLLOWER: the
    // precondition forwards intact and the state machine refuses it.
    let response = client
        .put(format!(
            "http://{}/imposters/{port}/stubs/0",
            follower.admin_addr()
        ))
        .header("if-match", &rev1)
        .json(&json!({ "responses": [{ "is": { "statusCode": 200, "body": "writer-b" } }] }))
        .send()
        .await
        .expect("writer B via follower");
    assert_eq!(
        response.status().as_u16(),
        409,
        "the exact clobber the admin_front module doc documents as broken pre-#46"
    );

    let read: serde_json::Value =
        reqwest::get(format!("http://{}/imposters/{port}", leader.admin_addr()))
            .await
            .expect("get")
            .json()
            .await
            .expect("json");
    assert_eq!(
        read["stubs"][0]["responses"][0]["is"]["body"], "writer-a",
        "{read}"
    );

    // The same write without If-Match remains last-writer-wins.
    let response = client
        .put(format!(
            "http://{}/imposters/{port}/stubs/0",
            follower.admin_addr()
        ))
        .json(&json!({ "responses": [{ "is": { "statusCode": 200, "body": "writer-b" } }] }))
        .send()
        .await
        .expect("writer B unconditional");
    assert_eq!(response.status().as_u16(), 200);

    follower.shutdown().await;
    leader.shutdown().await;
}
