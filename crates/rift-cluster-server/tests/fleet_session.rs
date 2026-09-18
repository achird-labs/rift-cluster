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

/// A credential that is *not* this fleet's — an app under test authenticating to its own mock.
/// Deliberately shares no prefix or suffix with [`API_KEY`], so an assertion that it survived
/// cannot be satisfied by a partial match on the key.
const APP_BEARER: &str = "Bearer someone-elses-token";

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

    // Rotate: a fresh key under a new revision, written straight to the control plane. This test
    // pins the *mechanism* — a token dies the instant its revision is superseded — independently
    // of what triggers it, which is why it stays an in-process write now that
    // `POST /session/rotate` exists (D-85) and is pinned by the two-node test.
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

/// `DELETE /session` clears the cookie — and, like every other state-changing request the browser
/// attaches the cookie to, only when `X-Rift-CSRF` is present. A cross-site page must not be able
/// to force an operator's logout; the route is unauthenticated, so the gate is the only thing
/// standing between it and one.
#[tokio::test]
async fn deleting_a_session_clears_the_cookie() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    // Without the header: refused, and no clearing `Set-Cookie` either.
    let seen = Seen::of(
        client
            .delete(format!("http://{admin}/session"))
            .send()
            .await
            .expect("logout without csrf"),
    )
    .await;
    assert_eq!(
        seen.status, 403,
        "a logout without X-Rift-CSRF is a forgeable request and must be refused: {seen}"
    );
    assert!(
        seen.header("set-cookie").is_none(),
        "a refused logout must not clear the cookie: {seen}"
    );

    let seen = Seen::of(
        client
            .delete(format!("http://{admin}/session"))
            .header("x-rift-csrf", "1")
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

/// `PUT /admin/fleet/name` over the wire (issue #373): the terminated handler parses the body it
/// documents, commits `FleetNamePut`, answers `200`, and the name reads back from the node's
/// applied state. The unit tests either side prove the pieces; nothing else drives the route.
#[tokio::test]
async fn put_fleet_name_round_trips_through_the_route() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    for name in ["rift-prod-eu", "rift-prod-eu (blue)"] {
        let seen = Seen::of(
            client
                .put(format!("http://{admin}/admin/fleet/name"))
                .header("authorization", API_KEY)
                .json(&serde_json::json!({ "name": name }))
                .send()
                .await
                .expect("put fleet name"),
        )
        .await;
        assert_eq!(seen.status, 200, "{seen}");
        assert_eq!(
            node.fleet_name().expect("fleet name reads").as_deref(),
            Some(name),
            "the committed name must be readable from applied state"
        );
    }

    server.shutdown().await;
}

/// A malformed `PUT /admin/fleet/name` body is a `4xx` and commits nothing — not JSON, the
/// wrong type, a missing field, and the two values `ControlOp::validate` refuses (blank, and over
/// `MAX_FLEET_NAME_CHARS`).
#[tokio::test]
async fn put_fleet_name_refuses_a_malformed_body_without_committing() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    let too_long = "n".repeat(rift_cluster::control::MAX_FLEET_NAME_CHARS + 1);
    let bodies: [(&str, String); 5] = [
        ("not JSON", "this is not json".to_owned()),
        ("wrong type", serde_json::json!({ "name": 5 }).to_string()),
        ("missing field", serde_json::json!({}).to_string()),
        ("blank", serde_json::json!({ "name": "   " }).to_string()),
        (
            "too long",
            serde_json::json!({ "name": too_long }).to_string(),
        ),
    ];
    for (label, body) in bodies {
        let seen = Seen::of(
            client
                .put(format!("http://{admin}/admin/fleet/name"))
                .header("authorization", API_KEY)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .expect("put fleet name"),
        )
        .await;
        assert!(
            (400..500).contains(&seen.status),
            "a {label} body must be a 4xx: {seen}"
        );
        assert_eq!(
            node.fleet_name().expect("fleet name reads"),
            None,
            "a refused {label} body must not have committed a name"
        );
    }

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
///
/// The same two nodes then pin rotation end to end — **Pins D-85**: `POST /session/rotate`,
/// called on the joiner so the write is forwarded to the leader, kills the founder-minted cookie
/// on *both* nodes, and a fresh login still works afterwards.
/// `rotating_the_signing_key_invalidates_every_session` proves the mechanism solo; this is the
/// only test in which "every session" spans a second node, and the only one that proves anything
/// outside the process can trigger the revocation at all.
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

    // Rotation is the only revocation D-73 leaves, and it has to reach every node: a cookie that
    // dies on the founder but keeps reading the joiner is a session that was never revoked. It is
    // triggered here the way an operator does it (D-85) — `POST /session/rotate` — and on the
    // **joiner**, so the write is one a follower had to forward to the leader before any of it
    // could be true.
    //
    // The two refusals come first, on the same route, because a rotation anyone can call is a
    // denial of service on every operator at once.
    let seen = Seen::of(
        client
            .post(format!("http://{joiner_admin}/session/rotate"))
            .send()
            .await
            .expect("rotate without a credential"),
    )
    .await;
    assert_eq!(
        seen.status, 401,
        "an unauthenticated rotation must be refused: {seen}"
    );
    let seen = Seen::of(
        client
            .post(format!("http://{joiner_admin}/session/rotate"))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("rotate by cookie without the CSRF header"),
    )
    .await;
    assert_eq!(
        seen.status, 403,
        "a cookie-authenticated rotation without X-Rift-CSRF is forgeable and must be refused: \
         {seen}"
    );

    let seen = Seen::of(
        client
            .post(format!("http://{joiner_admin}/session/rotate"))
            .header("authorization", API_KEY)
            .send()
            .await
            .expect("rotate"),
    )
    .await;
    assert_eq!(seen.status, 204, "the key rotates the signing key: {seen}");
    for admin in [&founder_admin, &joiner_admin] {
        // The joiner applies the rotation through the log, which is not instantaneous — so the
        // answer waited for is the refusal, and a `200` is what must stop appearing.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let seen = Seen::of(
                client
                    .get(format!("http://{admin}/imposters/{port}"))
                    .header("cookie", &cookie)
                    .send()
                    .await
                    .expect("read after rotation"),
            )
            .await;
            if seen.status == 401 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{admin} still accepts a cookie minted under the rotated-out signing key: {seen}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // A rotation ends every session; it does not end *sessions*. Logging in again — on the node
    // that served the rotation — must mint a cookie the other node accepts, or the lever is a
    // lockout rather than a revocation.
    let login = Seen::of(
        client
            .post(format!("http://{joiner_admin}/session"))
            .json(&serde_json::json!({ "apiKey": API_KEY }))
            .send()
            .await
            .expect("login after rotation"),
    )
    .await;
    assert_eq!(
        login.status, 200,
        "a rotation must not stop the key minting new sessions: {login}"
    );
    let fresh = format!("rift_session={}", session_cookie(&login));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = Seen::of(
            client
                .get(format!("http://{founder_admin}/imposters/{port}"))
                .header("cookie", &fresh)
                .send()
                .await
                .expect("read with the post-rotation cookie"),
        )
        .await;
        if seen.status == 200 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a cookie minted after the rotation is not accepted on the other node: {seen}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The mode D-85 advertises most loudly, and the one the bearer rotation above cannot show: an
    // operator already *in* the console evicts a thief by rotating with the session cookie plus the
    // CSRF header. It follows structurally from the shared `authenticate`, but nothing pinned it —
    // a change that made rotation bearer-only would have left the whole suite green. The caller's
    // own cookie is the one that dies, which is the point rather than a side effect.
    let seen = Seen::of(
        client
            .post(format!("http://{founder_admin}/session/rotate"))
            .header("cookie", &fresh)
            .header("x-rift-csrf", "1")
            .send()
            .await
            .expect("rotate by cookie"),
    )
    .await;
    assert_eq!(
        seen.status, 204,
        "a session cookie plus the CSRF header must be able to rotate: {seen}"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = Seen::of(
            client
                .get(format!("http://{joiner_admin}/imposters/{port}"))
                .header("cookie", &fresh)
                .send()
                .await
                .expect("read after the cookie-authenticated rotation"),
        )
        .await;
        if seen.status == 401 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the rotating caller's own cookie outlived the rotation it asked for: {seen}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    joiner.shutdown().await;
    founder.shutdown().await;
}

/// Pins D-85: a node restarted with a different `--api-key` refuses the cookies minted under the
/// old one — no rotation written, no coordination, nothing replicated.
///
/// The control at the end is the half that makes this a test rather than an assertion that
/// restarts break sessions: restarted with the *same* key over the same state directory, the very
/// same cookie still authenticates, which is what chapter 9 promises a session survives.
#[tokio::test]
async fn a_changed_api_key_ends_the_sessions_it_minted() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let client = reqwest::Client::new();

    let login = Seen::of(
        client
            .post(format!("http://{}/session", server.admin_addr()))
            .json(&serde_json::json!({ "apiKey": API_KEY }))
            .send()
            .await
            .expect("login"),
    )
    .await;
    assert_eq!(login.status, 200, "{login}");
    let cookie = format!("rift_session={}", session_cookie(&login));
    server.shutdown().await;

    let mut restarted = cluster_cli(&state, &[]);
    restarted.oss.api_key = Some("a-completely-different-fleet-key".to_owned());
    let server = compose::start(restarted)
        .await
        .expect("restarts under a new key");
    wait_ready(&server).await;
    let seen = Seen::of(
        client
            .get(format!("http://{}/_fleet/health", server.admin_addr()))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("health under the new key"),
    )
    .await;
    assert_eq!(
        seen.status, 401,
        "a cookie minted under the old --api-key still authenticates after the key changed: \
         {seen}"
    );
    server.shutdown().await;

    // Control: the same state, the same cookie, the original key — still a live session.
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("restarts under the original key");
    wait_ready(&server).await;
    let seen = Seen::of(
        client
            .get(format!("http://{}/_fleet/health", server.admin_addr()))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("health under the original key"),
    )
    .await;
    assert_eq!(
        seen.status, 200,
        "restarting under the unchanged key must not end a session: {seen}"
    );
    server.shutdown().await;
}

/// Pins D-85's fail-closed half: a rotation that cannot commit must not answer as though it had.
///
/// `session_rotate` builds its `204` *after* `commit_new_session_key` returns, so dropping that one
/// guard would report a fleet-wide revocation that never happened — a success status for work that
/// did not occur, which is the shape this codebase treats as a defect even at a last resort. Every
/// other rotation test either refuses before the write or lets the write succeed; this is the only
/// one in which the write itself fails, so without it that guard can be deleted with the suite
/// still green.
///
/// Quorum is broken by killing the founder of a two-node fleet. The joiner is left unable to commit
/// either way — as a voter it cannot reach a quorum of two alone, and as a learner it has no leader
/// to forward to — which is the precondition this test needs and the reason it asserts the *class*
/// of refusal rather than one exact status. What it does assert exactly is that the answer is not
/// `204` and that no clearing `Set-Cookie` rides along: telling a browser its session is over is
/// the same lie as the `204`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rotation_that_cannot_commit_does_not_answer_as_though_it_had() {
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
    let joiner_admin = joiner.admin_addr().to_string();
    let client = reqwest::Client::new();

    // Log in through the joiner while the fleet still has a quorum, so the signing key exists and
    // the cookie below is a real one — the rotation that fails later is failing to *replace* a key,
    // not to mint the first.
    let login = Seen::of(
        client
            .post(format!("http://{joiner_admin}/session"))
            .json(&serde_json::json!({ "apiKey": API_KEY }))
            .send()
            .await
            .expect("login"),
    )
    .await;
    assert_eq!(login.status, 200, "{login}");
    let cookie = format!("rift_session={}", session_cookie(&login));

    founder.shutdown().await;

    let seen = Seen::of(
        client
            .post(format!("http://{joiner_admin}/session/rotate"))
            .header("authorization", API_KEY)
            .send()
            .await
            .expect("rotate without a quorum"),
    )
    .await;
    assert_ne!(
        seen.status, 204,
        "a rotation that never committed answered as though every session had ended: {seen}"
    );
    assert!(
        seen.status == 503 || seen.status == 504,
        "a rotation that cannot commit must be refused as unavailable or timed out, not {}: {seen}",
        seen.status
    );
    assert!(
        seen.header("set-cookie").is_none(),
        "a refused rotation must not tell the browser its session is over: {seen}"
    );
    // The cookie is untouched by a rotation that did not happen: same key, same revision.
    let seen = Seen::of(
        client
            .get(format!("http://{joiner_admin}/_fleet/health"))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("health after the refused rotation"),
    )
    .await;
    assert_ne!(
        seen.status, 401,
        "a rotation that was refused must not have ended the caller's session: {seen}"
    );

    joiner.shutdown().await;
}

/// The two refusals `POST /session/rotate` owes that a keyed two-node fleet cannot show: a fleet
/// with no `--api-key` has no sessions to end and must say so rather than commit a signing key,
/// and every other method on the path is a `405` rather than a silent fall-through to the
/// classifier.
#[tokio::test]
async fn rotate_refuses_an_open_plane_and_every_method_but_post() {
    let state = TempDir::new().expect("tempdir");
    let mut open = cluster_cli(&state, &[]);
    open.oss.api_key = None;
    let server = compose::start(open).await.expect("open-plane fleet starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let client = reqwest::Client::new();

    let seen = Seen::of(
        client
            .post(format!("http://{}/session/rotate", server.admin_addr()))
            .send()
            .await
            .expect("rotate on an open plane"),
    )
    .await;
    assert_eq!(
        seen.status, 400,
        "an open plane has no sessions to end and must refuse rather than commit a key: {seen}"
    );
    assert!(
        node.session_key().expect("read session key").is_none(),
        "a refused rotation must not have committed a signing key"
    );
    server.shutdown().await;

    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("keyed fleet starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let seen = Seen::of(
        client
            .delete(format!("http://{}/session/rotate", server.admin_addr()))
            .header("authorization", API_KEY)
            .send()
            .await
            .expect("DELETE on the rotate route"),
    )
    .await;
    assert_eq!(
        seen.status, 405,
        "only POST rotates; anything else must be a 405, not a route miss: {seen}"
    );

    // A *wrong* bearer is a refusal, never a fall-through to the cookie branch — the same rule
    // `authenticate` applies everywhere, asserted on this route because it is the one that writes.
    let seen = Seen::of(
        client
            .post(format!("http://{}/session/rotate", server.admin_addr()))
            .header("authorization", "not-this-fleets-key")
            .send()
            .await
            .expect("rotate with the wrong key"),
    )
    .await;
    assert_eq!(
        seen.status, 401,
        "a rotation presenting the wrong key must be refused: {seen}"
    );
    assert!(
        node.session_key().expect("read session key").is_none(),
        "no refused request on this route may have committed a signing key"
    );
    server.shutdown().await;
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
        // Terminated (a GET the front answers itself, rather than proxying).
        "/imposters/4545/spaces",
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

/// The recorded requests in a `savedRequests` body — a bare array or an object carrying one.
fn recorded_requests(seen: &Seen) -> Vec<serde_json::Value> {
    match seen.json() {
        serde_json::Value::Array(requests) => requests,
        serde_json::Value::Object(doc) => doc
            .get("requests")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_else(|| {
                panic!("an object savedRequests body has a `requests` array: {seen}")
            }),
        _ => panic!("savedRequests body is neither an array nor an object: {seen}"),
    }
}

/// One header of a recorded request, by name, case-insensitively — the journal preserves the
/// casing the client sent, so a `get("authorization")` would be a test that passes on the wrong
/// spelling. `None` means the header is absent, which is what every credential assertion below
/// is really asking.
fn recorded_header(request: &serde_json::Value, name: &str) -> Option<String> {
    request
        .get("headers")?
        .as_object()?
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.to_string())
}

/// `/__rift/{port}/*` is data-plane traffic and stays open on a keyed fleet — and, critically,
/// **no** admin credential reaches the imposter on it (D-73), by any of the three routes one
/// could take. The gateway leg reaches the imposter, where each would land in its predicates,
/// its recorded request log, and any proxying stub's outbound request, so this drives all three
/// through one recording imposter:
///
/// 1. the configured key is never *injected* on this leg (the client sends nothing);
/// 2. the `rift_session` cookie the browser attaches on its own is stripped from `Cookie` —
///    console and gateway share an origin, so this is not hypothetical;
/// 3. an `Authorization` the *client* sent carrying the fleet's own key is dropped. Upstream
///    exempts `/__rift/*` from its key gate, so nothing else would stop a CI script that stamps
///    the key onto every rift call from leaking it into an app-under-test's journal.
///
/// And the boundary that keeps (3) from being "drop every `Authorization`": someone else's
/// bearer **must** survive, because an app under test authenticates to its own mock and an
/// imposter legitimately predicates on that. A strip that took the header unconditionally would
/// pass every assertion above and break real scenarios; this fails it.
///
/// `recordRequests` is on, and the recorded requests are asserted to *exist* before their
/// headers are inspected. Upstream defaults recording off and `record_request` returns before it
/// looks at a header, so without both of those this test passed against an empty journal
/// whatever the front forwarded — deleting the injection guard left it green.
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
                "recordRequests": true,
                "stubs": [{ "responses": [{ "is": { "statusCode": 204 } }] }],
            }))
            .send()
            .await
            .expect("create imposter"),
    )
    .await;
    assert_eq!(seen.status, 201, "{seen}");

    // A real, verifying session token — the thing that must not leak.
    let login = Seen::of(
        client
            .post(format!("http://{admin}/session"))
            .json(&serde_json::json!({ "apiKey": API_KEY }))
            .send()
            .await
            .expect("login"),
    )
    .await;
    assert_eq!(login.status, 200, "{login}");
    let token = session_cookie(&login);

    // Request 1 — the browser shape. No admin credential presented as one; the session cookie
    // rides in `Cookie` next to an app cookie the imposter is entitled to see. The gateway must
    // answer regardless.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/__rift/{port}/anything"))
            .header("cookie", format!("rift_session={token}; other=keep"))
            .send()
            .await
            .expect("gateway request"),
    )
    .await;
    assert_eq!(
        seen.status, 204,
        "gateway traffic must not be gated by the admin key: {seen}"
    );

    // Request 2 — the CI-script shape: the caller stamps the fleet's *own* key onto every rift
    // call, including this one. Still open (upstream exempts the prefix), and the key must not
    // ride through.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/__rift/{port}/anything"))
            .header("authorization", API_KEY)
            .send()
            .await
            .expect("gateway request carrying the fleet key"),
    )
    .await;
    assert_eq!(
        seen.status, 204,
        "presenting the admin key must not change how the open gateway answers: {seen}"
    );

    // Request 3 — an app under test authenticating to its own mock. Not this fleet's credential,
    // so it is none of the front's business and must arrive intact.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/__rift/{port}/anything"))
            .header("authorization", APP_BEARER)
            .send()
            .await
            .expect("gateway request carrying an app's own bearer"),
    )
    .await;
    assert_eq!(seen.status, 204, "{seen}");

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

    // Vacuity guard: the journal holds all three requests, so every header assertion below is
    // about a request that was actually recorded — and each carries a `headers` object, so an
    // absent header is absence and not a missing map.
    let recorded = recorded_requests(&seen);
    assert_eq!(
        recorded.len(),
        3,
        "exactly the three gateway requests must have been recorded: {seen}"
    );
    for (i, request) in recorded.iter().enumerate() {
        assert!(
            request.get("headers").and_then(|h| h.as_object()).is_some(),
            "recorded request {i} carries its headers: {seen}"
        );
    }

    // Neither admin credential's *value* anywhere in the journal. The strongest form of the
    // claim and the one that does not depend on a header name: it covers request 2's key even if
    // some future rewrite moved it.
    assert!(
        !seen.body.contains(API_KEY),
        "the admin key's value reached the imposter's recorded requests: {seen}"
    );
    assert!(
        !seen.body.contains(&token),
        "the session token reached the imposter's recorded requests: {seen}"
    );

    // Request 1: the client sent no `Authorization`, so one appearing here could only have been
    // injected by the front.
    assert_eq!(
        recorded_header(&recorded[0], "authorization"),
        None,
        "an Authorization header the client never sent reached the imposter: {seen}"
    );

    // Request 2: the client *did* send one and it was the fleet's key, so the header must be
    // gone entirely — not blanked, not rewritten.
    assert_eq!(
        recorded_header(&recorded[1], "authorization"),
        None,
        "the caller's own Authorization carried the fleet key through to the imposter: {seen}"
    );

    // Request 3: someone else's bearer is untouched. Dropping every `Authorization` would satisfy
    // both assertions above and fail this one.
    let app_bearer = recorded_header(&recorded[2], "authorization")
        .unwrap_or_else(|| panic!("an app's own bearer must reach its mock: {seen}"));
    assert!(
        app_bearer.contains("someone-elses-token"),
        "an app's own bearer must reach its mock intact: {app_bearer}"
    );

    // The app cookie survives on request 1 — an imposter legitimately predicates on cookies, so
    // the strip must take the one pair and not the header.
    let cookie = recorded_header(&recorded[0], "cookie")
        .unwrap_or_else(|| panic!("the app cookie must reach the imposter: {seen}"));
    assert!(cookie.contains("other=keep"), "{cookie}");
    assert!(
        !cookie.contains("rift_session"),
        "the session cookie pair must be stripped from the gateway leg: {cookie}"
    );

    server.shutdown().await;
}

/// D-73's authentication rule at the wire: a *present* `Authorization` is judged on its own, and
/// a value that is not even readable as a string is a refusal — never an absence that lets a
/// cookie on the same request authenticate it instead. The cookie is shown to be valid on its own
/// first, so the `401` is provably the header's doing.
#[tokio::test]
async fn an_unreadable_authorization_header_is_refused_even_beside_a_valid_cookie() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    let login = Seen::of(
        client
            .post(format!("http://{admin}/session"))
            .json(&serde_json::json!({ "apiKey": API_KEY }))
            .send()
            .await
            .expect("login"),
    )
    .await;
    assert_eq!(login.status, 200, "{login}");
    let cookie = format!("rift_session={}", session_cookie(&login));

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/_fleet/health"))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("cookie-only read"),
    )
    .await;
    assert_eq!(
        seen.status, 200,
        "the cookie alone must authenticate: {seen}"
    );

    // RFC 9110 obs-text: legal on the wire, never a `&str`.
    let opaque = reqwest::header::HeaderValue::from_bytes(&[0xff, 0xfe])
        .expect("obs-text is a legal header value");
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/_fleet/health"))
            .header("cookie", &cookie)
            .header("authorization", opaque)
            .send()
            .await
            .expect("unreadable-bearer read"),
    )
    .await;
    assert_eq!(
        seen.status, 401,
        "an unreadable Authorization must be refused, not treated as absent so the cookie \
         authenticates: {seen}"
    );

    server.shutdown().await;
}
