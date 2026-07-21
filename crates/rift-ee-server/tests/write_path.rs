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
