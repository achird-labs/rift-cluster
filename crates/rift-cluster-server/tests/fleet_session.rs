//! C2 (issue #185, RFC-006 §5.2–§5.3): the `/_fleet/*` projection and the session-cookie exchange.
//!
//! One test per acceptance criterion in the issue, driven over real HTTP — the whole point of §5.3
//! is that the flow is API-visible and needs no browser, so the tests are the proof of that claim
//! rather than an approximation of it.

use std::time::Duration;

use clap::Parser;
use rift_cluster::control::{FLEET_SCOPE, PrincipalId, Role};
use rift_cluster::rpc::{AlwaysHealthy, RpcClient, RpcClientConfig, Signer};
use rift_cluster::{ControlOp, ControlRequest, RaftNode, TenantId};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use tempfile::TempDir;

mod common;

use common::seen::Seen;

const SECRET: &str = "fleet-session-secret";

fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    let mut args = vec![
        "rift-cluster-server".to_owned(),
        "--port".to_owned(),
        "0".to_owned(),
        "--metrics-port".to_owned(),
        "0".to_owned(),
        "--cluster".to_owned(),
        "--cluster-allow-solo".to_owned(),
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

/// A fleet admin, which is the only role `/_fleet/*` admits (RFC-006 §12 Q3, settled in
/// `docs/architecture/08-tenancy-security.md`).
async fn seed_fleet_admin(node: &RaftNode, op_id: &mut u128, raw_key: &str) -> PrincipalId {
    let principal = rift_cluster::control::Principal {
        id: rift_cluster::control::api_key_principal_id(raw_key),
        display_name: "fleet".to_owned(),
        auth: rift_cluster::control::AuthSource::ApiKey {
            hash: rift_cluster::control::hash_api_key(raw_key),
        },
        disabled: false,
    };
    let id = principal.id.clone();
    seed(
        node,
        *op_id,
        ControlOp::PrincipalPut {
            tenant: TenantId::default(),
            principal,
        },
    )
    .await;
    *op_id += 1;
    seed(
        node,
        *op_id,
        ControlOp::BindingPut {
            tenant: TenantId::new(FLEET_SCOPE),
            principal_id: id.clone(),
            role: Role::FleetAdmin,
        },
    )
    .await;
    *op_id += 1;
    id
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
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let key = "fleet-session-login";
    seed_fleet_admin(node, &mut op_id, key).await;

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
    let node = server.node().expect("clustered");
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
    let mut op_id = 1u128;

    let key = "fleet-shape-parity";
    seed_fleet_admin(node, &mut op_id, key).await;

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
            // addition at all. Only the fleet-wide sum is.
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
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let key = "fleet-bearer-unchanged";
    seed_fleet_admin(node, &mut op_id, key).await;

    // A read.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/whoami"))
            .header("authorization", key)
            .send()
            .await
            .expect("whoami"),
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
            .get(format!("http://{admin}/admin/whoami"))
            .send()
            .await
            .expect("anon whoami"),
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
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let key = "fleet-csrf";
    seed_fleet_admin(node, &mut op_id, key).await;

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
            .get(format!("http://{admin}/admin/whoami"))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("cookie whoami"),
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

/// AC6: revoking a binding cuts a live **session**, not just a live bearer.
///
/// This is the criterion a TTL cache over session→bindings would silently break — the named mutant
/// `c25_key_revocation_survives_a_partition` exists to catch. The cookie proves authentication only;
/// bindings are re-resolved from applied state on every request, so the revocation lands
/// immediately rather than at the end of the session's 8-hour TTL.
#[tokio::test]
async fn revoking_a_binding_cuts_a_live_session() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let key = "fleet-revoke";
    let principal_id = seed_fleet_admin(node, &mut op_id, key).await;

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

    // The session works.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/_fleet/health"))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("health before revocation"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");

    // Revoke the fleet binding out from under the live cookie.
    seed(
        node,
        op_id,
        ControlOp::BindingDelete {
            tenant: TenantId::new(FLEET_SCOPE),
            principal_id: principal_id.clone(),
        },
    )
    .await;

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/_fleet/health"))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("health after revocation"),
    )
    .await;
    assert_ne!(
        seen.status, 200,
        "the cookie still reads fleet health after its binding was revoked — a cache over \
         session -> bindings has been introduced, which is exactly the mutant C25 exists to \
         catch: {seen}"
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
    let mut op_id = 1u128;

    let key = "fleet-rotate";
    seed_fleet_admin(node, &mut op_id, key).await;

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
            tenant: TenantId::new(FLEET_SCOPE),
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

/// `/_fleet/*` is `ClusterAdmin` — FleetAdmin exclusively (RFC-006 §12 Q3, settled in Chapter 8).
///
/// The decision is only real if a non-fleet-admin is actually refused, so that is what is asserted
/// rather than the presence of the route.
#[tokio::test]
async fn fleet_routes_refuse_a_non_fleet_admin() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // A principal bound only inside a tenant, never on the fleet scope.
    let tenant_key = "fleet-tenant-only";
    let principal = rift_cluster::control::Principal {
        id: rift_cluster::control::api_key_principal_id(tenant_key),
        display_name: "tenant admin".to_owned(),
        auth: rift_cluster::control::AuthSource::ApiKey {
            hash: rift_cluster::control::hash_api_key(tenant_key),
        },
        disabled: false,
    };
    let id = principal.id.clone();
    seed(
        node,
        op_id,
        ControlOp::PrincipalPut {
            tenant: TenantId::default(),
            principal,
        },
    )
    .await;
    op_id += 1;
    seed(
        node,
        op_id,
        ControlOp::BindingPut {
            tenant: TenantId::default(),
            principal_id: id,
            role: Role::TenantAdmin,
        },
    )
    .await;

    // The ops route is included deliberately. It is the only projected route with a path parameter
    // and the only one whose legitimate answer for a missing resource is a 404, which makes it the
    // one most likely to be special-cased later — "handle the 404 first" would ship an
    // unauthenticated read of fleet op status with every other test still green.
    for path in [
        "/_fleet/members",
        "/_fleet/health",
        "/_fleet/ops/0189dcf0-0454-4e0b-a10c-8a8f8dccce1f",
    ] {
        let seen = Seen::of(
            client
                .get(format!("http://{admin}{path}"))
                .header("authorization", tenant_key)
                .send()
                .await
                .expect("fleet read"),
        )
        .await;
        assert_ne!(
            seen.status, 200,
            "{path} served fleet topology to a principal with no fleet-scoped binding: {seen}"
        );
    }

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
    let mut op_id = 1u128;

    let key = "fleet-ops-poll";
    seed_fleet_admin(node, &mut op_id, key).await;

    // A committed op with an id we know, so the poll target actually exists.
    let known = uuid::Uuid::from_u128(op_id);
    seed(
        node,
        op_id,
        ControlOp::PutRoutes {
            tenant: TenantId::default(),
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

/// The legacy `--api-key` cannot hold a console session.
///
/// It resolves to a synthetic identity with no principal row, and a session token names a principal
/// that every later request re-reads. Minting one would answer `200` with a cookie that
/// authenticates never — so the exchange is refused outright instead. Regression test for a defect
/// review caught: without this, a fleet mid-migration off `--api-key` would see "login worked, then
/// everything is 401" with nothing in the logs to explain it.
#[tokio::test]
async fn the_legacy_api_key_cannot_mint_a_session() {
    let state = TempDir::new().expect("tempdir");
    let legacy = "legacy-console-key";
    let server = compose::start(cluster_cli(&state, &["--api-key", legacy]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    // It still authenticates as a bearer — this slice must not break the migration path.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/whoami"))
            .header("authorization", legacy)
            .send()
            .await
            .expect("legacy whoami"),
    )
    .await;
    assert_eq!(
        seen.status, 200,
        "the legacy key must still work as a bearer: {seen}"
    );

    // But it cannot be exchanged for a cookie.
    let seen = Seen::of(
        client
            .post(format!("http://{admin}/session"))
            .json(&serde_json::json!({ "apiKey": legacy }))
            .send()
            .await
            .expect("legacy login"),
    )
    .await;
    assert_eq!(
        seen.status, 400,
        "the legacy key must be refused a session rather than handed an unusable one: {seen}"
    );
    assert!(
        seen.header("set-cookie").is_none(),
        "a refused login must not set a cookie: {seen}"
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
