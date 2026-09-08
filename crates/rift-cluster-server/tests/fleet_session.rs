//! C2 (issue #185, RFC-006 §5.2–§5.3): the `/_fleet/*` projection and the session-cookie exchange.
//!
//! One test per acceptance criterion in the issue, driven over real HTTP — the whole point of §5.3
//! is that the flow is API-visible and needs no browser, so the tests are the proof of that claim
//! rather than an approximation of it.

use std::time::Duration;

use clap::Parser;
use rift_cluster::rpc::{AlwaysHealthy, RpcClient, RpcClientConfig, Signer};
use rift_cluster::{ControlOp, ControlRequest, RaftNode};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use tempfile::TempDir;

mod common;

use common::ports::{reserve_addr, reserve_port};
use common::seen::Seen;

const SECRET: &str = "fleet-session-secret";

/// The fleet's one admin credential (D-73). Every fixture here runs with the plane closed,
/// because an open plane authenticates everything and would make each of these tests pass
/// whether or not the gate works.
const API_KEY: &str = "fleet-session-api-key";

/// A single-node fleet: the shape most of these tests need.
fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    let mut args = vec!["--cluster-allow-solo"];
    args.extend_from_slice(extra);
    cluster_on(state, "127.0.0.1:0", &args)
}

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
        "--api-key".to_owned(),
        API_KEY.to_owned(),
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
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn seed(node: &RaftNode, op_id: u128, op: ControlOp) {
    let response = node
        .write(ControlRequest {
            op_id: uuid::Uuid::from_u128(op_id),
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op,
        })
        .await
        .expect("seed op commits");
    assert_eq!(response.outcome, rift_cluster::ControlOutcome::Applied);
}

/// Pull the `rift_session` cookie value out of a `Set-Cookie` header.
fn session_cookie(seen: &Seen) -> String {
    let raw = seen
        .header("set-cookie")
        .expect("a successful login sets a cookie");
    raw.split(';')
        .next()
        .and_then(|kv| kv.strip_prefix("rift_session="))
        .expect("the cookie is named rift_session")
        .to_owned()
}

/// AC1 + AC5: curl can log in, hold the cookie, and read fleet health — and the cookie carries every
/// attribute §5.3 specifies.
///
/// The attributes are not cosmetic. `HttpOnly` is what stops any script on the page — injected or
/// not — from reading the session, which is the entire reason the key is exchanged for a cookie
/// rather than kept in the page.
#[tokio::test]
async fn curl_can_log_in_hold_a_cookie_and_read_fleet_health() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    let key = API_KEY;

    let response = client
        .post(format!("http://{admin}/session"))
        .json(&serde_json::json!({ "apiKey": key }))
        .send()
        .await
        .expect("login");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "login must succeed: {seen}");

    let raw_cookie = seen.header("set-cookie").expect("Set-Cookie").to_owned();
    for attribute in [
        "HttpOnly",
        "Secure",
        "SameSite=Strict",
        "Max-Age=28800",
        "Path=/",
    ] {
        assert!(
            raw_cookie.contains(attribute),
            "cookie is missing {attribute}: {raw_cookie}"
        );
    }

    // The cookie alone reads fleet health — no bearer anywhere in this request.
    let token = session_cookie(&seen);
    let response = client
        .get(format!("http://{admin}/_fleet/health"))
        .header("cookie", format!("rift_session={token}"))
        .send()
        .await
        .expect("fleet health");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "cookie must read fleet health: {seen}");
    let body = seen.json();
    assert!(body.get("ready").is_some(), "{seen}");
    assert!(
        body.get("ring").and_then(|r| r.get("members")).is_some(),
        "{seen}"
    );

    server.shutdown().await;
}

/// AC3: the projection reports everything the cluster port does, and adds only what it documents.
///
/// Asserted against the cluster port's live body rather than a hand-written fixture, exactly as the
/// issue asks. The bodies are built by one shared function, so this is a regression guard on the
/// wiring rather than the primary guarantee — but if the projection ever *dropped* a field,
/// operators reading `/_cluster/*` and a console reading `/_fleet/*` would disagree about the same
/// fleet, which is worse than either being absent.
///
/// Since #361 the two are no longer key-for-key identical: `/_fleet/members` carries a `members`
/// fan-out the cluster port deliberately does not. See the assertion for why that direction is
/// allowed and the other is not.
#[tokio::test]
async fn fleet_projection_matches_the_cluster_port_shapes() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let cluster: std::net::SocketAddr = server
        .cluster_addr()
        .expect("cluster port bound")
        .to_string()
        .parse()
        .expect("cluster addr");
    let rpc = RpcClient::new(
        Some(Signer::new(SECRET)),
        std::sync::Arc::new(AlwaysHealthy),
        RpcClientConfig::default(),
    );
    let client = reqwest::Client::new();

    let key = API_KEY;

    for (fleet_path, cluster_path) in [
        ("/_fleet/members", "/_cluster/members"),
        ("/_fleet/health", "/_cluster/health"),
    ] {
        let via_admin = Seen::of(
            client
                .get(format!("http://{admin}{fleet_path}"))
                .header("authorization", key)
                .send()
                .await
                .expect("fleet read"),
        )
        .await;
        assert_eq!(via_admin.status, 200, "{fleet_path}: {via_admin}");

        // The cluster port speaks the cluster RPC protocol — it version-negotiates and answers 426
        // to a plain HTTP GET — so it is read with the same client every other cluster-port test
        // uses rather than with reqwest.
        let raw = rpc
            .call(cluster, "GET", cluster_path, Vec::new())
            .await
            .unwrap_or_else(|e| panic!("GET {cluster_path}: {e}"));
        let via_cluster: serde_json::Value =
            serde_json::from_slice(&raw).expect("cluster port answers json");

        let mut a: Vec<String> = via_admin
            .json()
            .as_object()
            .expect("object")
            .keys()
            .cloned()
            .collect();
        let mut b: Vec<String> = via_cluster
            .as_object()
            .expect("object")
            .keys()
            .cloned()
            .collect();
        a.sort();
        b.sort();

        // The fleet projection may **add**, never **drop** (issue #361).
        //
        // This was an equality assertion until `/_fleet/members` gained `members`, the per-voter
        // fan-out the console needs and the cluster port deliberately does not serve — it is the
        // target of that fan-out, so making it fleet-wide too would have every peer fan out to
        // every other peer.
        //
        // Split rather than relaxed, because the two directions mean opposite things. A field the
        // projection *dropped* is the drift this test was written to catch: an operator reading the
        // cluster port and a console reading the fleet port would disagree about the same fleet. A
        // field it *added* is only ever the documented extension below — anything else is caught
        // just as loudly as before.
        let permitted_additions: &[&str] = match fleet_path {
            // #361: each voter's own applied index, folded here.
            "/_fleet/members" => &["members"],
            // #360: the parked-write depth summed across voters. `parked_intents` itself is NOT
            // listed — it is in the shared `health_body`, so both ports carry it and it is not an
            // addition at all. Only the fleet-wide sum is. Alphabetical, because the keys are
            // compared sorted; `blob_fetch_stalls_fleet` was the other entry until D-72 (#549)
            // removed the blob store it folded.
            "/_fleet/health" => &["parked_intents_fleet"],
            _ => &[],
        };
        let dropped: Vec<&String> = b.iter().filter(|key| !a.contains(key)).collect();
        let added: Vec<&str> = a
            .iter()
            .filter(|key| !b.contains(key))
            .map(String::as_str)
            .collect();

        assert!(
            dropped.is_empty(),
            "{fleet_path} dropped {dropped:?} that {cluster_path} reports — the projection has \
             drifted from the surface it projects"
        );
        assert_eq!(
            added, permitted_additions,
            "{fleet_path} adds fields {cluster_path} does not, beyond the documented extension"
        );
    }

    server.shutdown().await;
}

/// AC2: the bearer path is byte-identical to before sessions existed.
///
/// A regression here breaks every existing client, and it is the kind that passes a happy-path test
/// while being badly wrong — so it is asserted, not eyeballed. In particular a bearer request must
/// still succeed with **no** CSRF header: bearer callers are exempt by design, because a bearer
/// cannot be attached by a victim's browser, which is the whole attack.
#[tokio::test]
async fn the_bearer_path_is_unchanged_and_exempt_from_csrf() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    let key = API_KEY;

    // A read.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/openapi.json"))
            .header("authorization", key)
            .send()
            .await
            .expect("contract read"),
    )
    .await;
    assert_eq!(seen.status, 200, "bearer read must be unaffected: {seen}");

    // A state-changing request with a bearer and NO CSRF header must still succeed.
    let seen = Seen::of(
        client
            .post(format!("http://{admin}/imposters"))
            .header("authorization", key)
            .json(&serde_json::json!({"port": 5599, "protocol": "http"}))
            .send()
            .await
            .expect("create imposter"),
    )
    .await;
    assert!(
        seen.status == 200 || seen.status == 201 || seen.status == 202,
        "a bearer-authenticated mutation must not require a CSRF header: {seen}"
    );

    // No credential at all is still refused the way it was.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/openapi.json"))
            .send()
            .await
            .expect("anon contract read"),
    )
    .await;
    assert_eq!(seen.status, 401, "an anonymous read must still 401: {seen}");

    server.shutdown().await;
}

/// AC4: a cookie-authenticated mutation without `X-Rift-CSRF` is refused; with it, it succeeds.
#[tokio::test]
async fn cookie_mutations_require_the_csrf_header() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    let key = API_KEY;

    let login = Seen::of(
        client
            .post(format!("http://{admin}/session"))
            .json(&serde_json::json!({ "apiKey": key }))
            .send()
            .await
            .expect("login"),
    )
    .await;
    let token = session_cookie(&login);
    let cookie = format!("rift_session={token}");

    // A cookie-authenticated read needs no CSRF header — only state-changing requests do.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/openapi.json"))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("cookie contract read"),
    )
    .await;
    assert_eq!(seen.status, 200, "a cookie read must not need CSRF: {seen}");

    // The same mutation, cookie-authenticated, without the header.
    let seen = Seen::of(
        client
            .post(format!("http://{admin}/imposters"))
            .header("cookie", &cookie)
            .json(&serde_json::json!({"port": 5601, "protocol": "http"}))
            .send()
            .await
            .expect("cookie mutation"),
    )
    .await;
    assert_eq!(
        seen.status, 403,
        "a cookie-authenticated mutation without X-Rift-CSRF must be refused: {seen}"
    );

    // And with it.
    let seen = Seen::of(
        client
            .post(format!("http://{admin}/imposters"))
            .header("cookie", &cookie)
            .header("x-rift-csrf", "1")
            .json(&serde_json::json!({"port": 5602, "protocol": "http"}))
            .send()
            .await
            .expect("cookie mutation with csrf"),
    )
    .await;
    assert!(
        seen.status == 200 || seen.status == 201 || seen.status == 202,
        "the same mutation with X-Rift-CSRF must succeed: {seen}"
    );

    server.shutdown().await;
}

/// AC7: rotating the session-signing key invalidates every outstanding session at once.
///
/// Structural rather than swept: every token carries the key record's revision, and verification
/// refuses a token whose revision is not the current one. No session table is consulted, because
/// none exists.
#[tokio::test]
async fn rotating_the_signing_key_invalidates_every_session() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let op_id = 1u128;

    let key = API_KEY;

    let login = Seen::of(
        client
            .post(format!("http://{admin}/session"))
            .json(&serde_json::json!({ "apiKey": key }))
            .send()
            .await
            .expect("login"),
    )
    .await;
    let cookie = format!("rift_session={}", session_cookie(&login));

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/_fleet/health"))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("health before rotation"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");

    // Rotate: a fresh key under a new revision. There is no rotation endpoint by design (a sixth
    // route would break the contract's parity gate), so this goes through the control plane the
    // way an operator tool would.
    seed(
        node,
        op_id,
        ControlOp::SessionKeyPut {
            key: "f".repeat(64),
        },
    )
    .await;

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/_fleet/health"))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("health after rotation"),
    )
    .await;
    assert_ne!(
        seen.status, 200,
        "a cookie minted under the previous signing key still authenticates after rotation: {seen}"
    );

    server.shutdown().await;
}

/// `GET /_fleet/ops/{opId}` really routes and really reports a committed op.
///
/// The third projected route, and the one `every_direct_route_is_actually_served` cannot speak to
/// (an unknown op id legitimately 404s, so a 404 there proves nothing). Without this, the route's
/// only coverage would be `fleet::classify`'s string-parsing unit tests, which say nothing about
/// whether it is wired or authorized.
#[tokio::test]
async fn fleet_ops_reports_a_committed_op() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let op_id = 1u128;

    let key = API_KEY;

    // A committed op with an id we know, so the poll target actually exists.
    let known = uuid::Uuid::from_u128(op_id);
    seed(
        node,
        op_id,
        ControlOp::PutRoutes {
            table: Default::default(),
        },
    )
    .await;

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/_fleet/ops/{known}"))
            .header("authorization", key)
            .send()
            .await
            .expect("ops poll"),
    )
    .await;
    assert_eq!(seen.status, 200, "a committed op must be pollable: {seen}");
    assert_eq!(
        seen.json().get("state").and_then(|v| v.as_str()),
        Some("applied"),
        "{seen}"
    );

    // An unknown id is a 404, indistinguishable from a malformed one.
    let seen = Seen::of(
        client
            .get(format!(
                "http://{admin}/_fleet/ops/00000000-0000-0000-0000-000000000000"
            ))
            .header("authorization", key)
            .send()
            .await
            .expect("unknown op poll"),
    )
    .await;
    assert_eq!(seen.status, 404, "{seen}");

    server.shutdown().await;
}

/// `GET /_fleet/members` really carries the fleet's name (issue #373).
///
/// The unit tests either side of this one prove the pieces — that a `FleetNamePut` applies, that
/// `fleet_name()` reads it back, that the route classifies and authorizes. None of them proves the
/// value reaches the wire, which is the entire point of the feature: the console reads this body
/// and nothing else. Without this test the field could be dropped from `members_body` and every
/// other test would stay green.
///
/// Both states are asserted, because the unnamed one is not an edge case — it is what every
/// existing deployment looks like the moment it upgrades, and `null` there has to be a fact
/// ("nobody has named this fleet") rather than a gap.
#[tokio::test]
async fn fleet_members_carries_the_fleet_name() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let key = API_KEY;

    let members = |client: reqwest::Client, admin: String| async move {
        Seen::of(
            client
                .get(format!("http://{admin}/_fleet/members"))
                .header("authorization", key)
                .send()
                .await
                .expect("fleet members read"),
        )
        .await
    };

    // Before anyone names it: an absence, reported as one.
    let seen = members(client.clone(), admin.clone()).await;
    assert_eq!(seen.status, 200, "{seen}");
    assert_eq!(
        seen.json().get("fleet_name"),
        Some(&serde_json::Value::Null),
        "an unnamed fleet must report `null`, not omit the field and not invent a name: {seen}"
    );
    assert_eq!(
        seen.json().get("fleet_name_unavailable"),
        Some(&serde_json::Value::Bool(false)),
        "nothing failed to read, so the unavailable flag must say so: {seen}"
    );

    op_id += 1;
    seed(
        node,
        op_id,
        ControlOp::FleetNamePut {
            name: "rift-prod-eu".to_owned(),
        },
    )
    .await;

    let seen = members(client, admin).await;
    assert_eq!(seen.status, 200, "{seen}");
    assert_eq!(
        seen.json().get("fleet_name").and_then(|v| v.as_str()),
        Some("rift-prod-eu"),
        "the committed fleet name must reach the body the console actually reads: {seen}"
    );
    assert_eq!(
        seen.json().get("fleet_name_unavailable"),
        Some(&serde_json::Value::Bool(false)),
        "{seen}"
    );

    server.shutdown().await;
}

/// `DELETE /session` clears the cookie.
#[tokio::test]
async fn deleting_a_session_clears_the_cookie() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    let seen = Seen::of(
        client
            .delete(format!("http://{admin}/session"))
            .send()
            .await
            .expect("logout"),
    )
    .await;
    assert_eq!(seen.status, 204, "{seen}");
    let raw = seen.header("set-cookie").expect("logout clears the cookie");
    assert!(raw.contains("Max-Age=0"), "cookie is not cleared: {raw}");

    server.shutdown().await;
}

/// **The one credential mints a session, and the cookie alone reads every node** (#550, D-73).
///
/// This is the test the proxy leg needs. `admin_front` authenticates a cookie itself, then hands
/// the request to the loopback listener — which since #550 runs open-source Rift's own
/// `--api-key` gate, a raw constant-time compare against `Authorization`. A cookie-authenticated
/// request carries no `Authorization` at all, so unless the front *injects* the configured key on
/// that leg, `GET /imposters` answers `401` on a fleet where the login just succeeded. Before
/// #550 the front forwarded the session token as the credential, which upstream's compare will
/// never accept.
///
/// Both proxied shapes are driven — the collection listing and the single-imposter read — because
/// they take different decoration paths through `handle` and could plausibly be wired
/// differently. Two nodes, and the login happens on exactly one of them: the signing key is
/// replicated state, so a cookie minted on the founder must verify on the joiner with no second
/// login and no shared process state beyond the Raft log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_api_key_mints_a_session_and_the_cookie_is_accepted_on_every_node() {
    let founder_state = TempDir::new().expect("tempdir");
    let joiner_state = TempDir::new().expect("tempdir");
    let founder_bind = reserve_addr();

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
    let client = reqwest::Client::new();
    let port = reserve_port();

    // Something to read back. Written with the key as a bearer, which is the curl path.
    let seen = Seen::of(
        client
            .post(format!("http://{founder_admin}/imposters"))
            .header("authorization", API_KEY)
            .json(&serde_json::json!({ "port": port, "protocol": "http" }))
            .send()
            .await
            .expect("create imposter"),
    )
    .await;
    assert_eq!(seen.status, 201, "the key creates an imposter: {seen}");

    // Log in once, on the founder.
    let login = Seen::of(
        client
            .post(format!("http://{founder_admin}/session"))
            .json(&serde_json::json!({ "apiKey": API_KEY }))
            .send()
            .await
            .expect("login"),
    )
    .await;
    assert_eq!(login.status, 200, "the API key mints a session: {login}");
    let cookie = format!("rift_session={}", session_cookie(&login));

    // Every node, both proxied read shapes, cookie only — no `Authorization` anywhere.
    for admin in [&founder_admin, &joiner_admin] {
        // The joiner applies the imposter through the log, which is not instantaneous.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let seen = Seen::of(
                client
                    .get(format!("http://{admin}/imposters/{port}"))
                    .header("cookie", &cookie)
                    .send()
                    .await
                    .expect("single imposter read"),
            )
            .await;
            assert_ne!(
                seen.status, 401,
                "a cookie-authenticated proxied read was refused on {admin} — the front is not \
                 injecting the configured --api-key on the loopback leg: {seen}"
            );
            if seen.status == 200 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{admin} never applied the imposter: {seen}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let seen = Seen::of(
            client
                .get(format!("http://{admin}/imposters"))
                .header("cookie", &cookie)
                .send()
                .await
                .expect("imposter listing"),
        )
        .await;
        assert_eq!(
            seen.status, 200,
            "the cookie must read the listing on {admin}: {seen}"
        );
    }

    joiner.shutdown().await;
    founder.shutdown().await;
}

/// The gate is real: with `--api-key` set, an unauthenticated admin request is refused on every
/// surface this front serves — terminated, proxied and directly-served alike.
///
/// Without this the whole suite would pass against a front that authenticated nobody, because
/// every other test presents a credential.
#[tokio::test]
async fn a_keyed_fleet_refuses_an_unauthenticated_request_on_every_surface() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    for path in [
        // Directly served.
        "/openapi.json",
        "/front-door/routes",
        "/_fleet/health",
        // Terminated.
        "/admin/requests",
        // Proxied to the loopback.
        "/imposters",
        // Not classified by anything — the route-existence oracle.
        "/no/such/route",
    ] {
        let seen = Seen::of(
            client
                .get(format!("http://{admin}{path}"))
                .send()
                .await
                .expect("anonymous read"),
        )
        .await;
        assert_eq!(
            seen.status, 401,
            "{path} answered an anonymous caller on a keyed fleet: {seen}"
        );
    }

    // A *wrong* key is refused too, and is never an invitation to look for a cookie.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/openapi.json"))
            .header("authorization", "not-the-key")
            .send()
            .await
            .expect("wrong-key read"),
    )
    .await;
    assert_eq!(seen.status, 401, "{seen}");

    server.shutdown().await;
}

/// `/__rift/{port}/*` is data-plane traffic and stays open on a keyed fleet — and, critically,
/// the admin credential is never injected onto it: the gateway leg reaches the imposter, where an
/// `Authorization` header would land in its predicates and its recorded request log.
#[tokio::test]
async fn the_gateway_stays_open_and_never_carries_the_admin_key() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let port = reserve_port();

    let seen = Seen::of(
        client
            .post(format!("http://{admin}/imposters"))
            .header("authorization", API_KEY)
            .json(&serde_json::json!({
                "port": port,
                "protocol": "http",
                "stubs": [{ "responses": [{ "is": { "statusCode": 204 } }] }],
            }))
            .send()
            .await
            .expect("create imposter"),
    )
    .await;
    assert_eq!(seen.status, 201, "{seen}");

    // No credential: the gateway must answer anyway.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/__rift/{port}/anything"))
            .send()
            .await
            .expect("gateway request"),
    )
    .await;
    assert_eq!(
        seen.status, 204,
        "gateway traffic must not be gated by the admin key: {seen}"
    );

    // And the imposter never saw an `Authorization` header.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/imposters/{port}/savedRequests"))
            .header("authorization", API_KEY)
            .send()
            .await
            .expect("saved requests"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    assert!(
        !seen.body.to_lowercase().contains("authorization"),
        "the admin key leaked into the imposter's recorded request: {seen}"
    );

    server.shutdown().await;
}
