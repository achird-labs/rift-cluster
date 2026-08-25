//! `GET /openapi.json` (RFC-006 §5.1, issue #184): the published contract is reachable over HTTP,
//! is the real document rather than an empty placeholder, and is gated the same way `/admin/whoami`
//! is.
//!
//! The document's *shape* is asserted by the unit tests in `src/openapi.rs` (route parity,
//! `x-rift-origin`, the header components). What can only be checked here is that the binary
//! actually serves it, with the right content type and the right authentication posture.

use std::time::Duration;

use clap::Parser;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use tempfile::TempDir;

mod common;

use common::seen::Seen;

const SECRET: &str = "openapi-contract-secret";

/// A solo clustered invocation on an ephemeral port. Each test binary carries its own copy of small
/// fixtures like this, per `tests/common/mod.rs`.
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

/// AC3: the endpoint serves the contract, and it is the real one.
///
/// The `paths` count and the spot-checked entries are what stop this from passing against an empty
/// or truncated document — a `200` carrying `{}` would otherwise look identical to success here and
/// only fail later, inside a generated client's codegen.
#[tokio::test]
async fn openapi_json_endpoint_serves_the_contract() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();

    let response = reqwest::get(format!("http://{admin}/openapi.json"))
        .await
        .expect("openapi.json");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "{seen}");
    assert_eq!(
        seen.header("content-type"),
        Some("application/json"),
        "the contract must be served as JSON: {seen}"
    );

    let doc = seen.json();
    let version = doc
        .get("openapi")
        .and_then(|v| v.as_str())
        .expect("openapi version");
    assert!(
        version.starts_with("3.1."),
        "must be OpenAPI 3.1, got {version}"
    );

    let paths = doc
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .expect("paths object");
    assert!(
        paths.len() > 20,
        "the served document has only {} paths — this looks like a placeholder, not the contract",
        paths.len()
    );
    for path in [
        "/imposters",
        "/admin/tenants",
        "/admin/audit",
        "/admin/whoami",
        "/openapi.json",
    ] {
        assert!(
            paths.contains_key(path),
            "{path} missing from the served contract"
        );
    }

    // The endpoint publishes itself, and publishes it as EE-terminated.
    let self_entry = paths
        .get("/openapi.json")
        .and_then(|p| p.get("get"))
        .expect("GET /openapi.json is published");
    assert_eq!(
        self_entry.get("x-rift-origin").and_then(|v| v.as_str()),
        Some("ee")
    );

    server.shutdown().await;
}

/// The successor to #184's C2 tripwire, and a stronger check than the one it replaces.
///
/// #184 carried a `C2_PENDING_ROUTES` guard asserting the `/_fleet/*` and `/session` routes were not
/// yet served. This PR serves them, so that guard has done its job and is gone. What replaces it is
/// the general form: **every route declared in `HANDLE_DIRECT_ROUTES` must actually be answered by a
/// running node.**
///
/// This closes a tautology a reviewer found in #184. `every_published_ee_path_is_routable` cannot
/// pin these routes, because `is_terminated_here` short-circuits on the same constant before it
/// would consult the real router — so the constant checks itself, and deleting the `/admin/whoami`
/// arm from `handle` would leave the contract publishing a route nothing serves, with every test
/// green. Only an HTTP probe against a live node catches that.
///
/// A 404 is the failure being hunted. Any other status — including `401`/`403`, which mean the route
/// exists and the gate ran — proves the arm is wired.
#[tokio::test]
async fn every_direct_route_is_actually_served() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    for (method, template) in rift_cluster_server::openapi::HANDLE_DIRECT_ROUTES {
        // `/_fleet/ops/{opId}` is the one entry whose *correct* answer for a resource that does not
        // exist is itself a 404 — an unknown op id and an unrouted path are deliberately
        // indistinguishable there, the same posture the cluster port takes. A 404 therefore proves
        // nothing either way and this check cannot speak to it. Its routing **and its
        // authorization** are pinned instead by `tests/fleet_session.rs`:
        // `fleet_routes_refuse_a_non_fleet_admin` includes it, and
        // `fleet_ops_reports_a_committed_op` polls a real committed op through it.
        if template.contains("{opId}") {
            continue;
        }
        // A real UUID: `/_fleet/ops/{opId}` parses the segment, and a malformed id names no op at
        // all — so a placeholder would probe a different arm than the one under test.
        let path = template.replace("{opId}", "0189dcf0-0454-4e0b-a10c-8a8f8dccce1f");
        let verb = reqwest::Method::from_bytes(method.as_bytes()).expect("known method");
        let response = client
            .request(verb, format!("http://{admin}{path}"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{method} {path} request failed: {e}"));
        let seen = Seen::of(response).await;
        assert_ne!(
            seen.status, 404,
            "{method} {path} is declared in HANDLE_DIRECT_ROUTES and published in the contract, \
             but no node serves it — either the arm was never added to `handle`, or it was \
             removed while the declaration stayed behind: {seen}"
        );
    }

    server.shutdown().await;
}

/// Pins D-21: the contract publishes no operation that adds or removes a cluster member. The only
/// path naming membership is the read-only `GET /_fleet/members` projection, and nothing names a
/// learner or a voter — membership changes only by a node joining or leaving.
///
/// Pins D-24: no published path triggers a snapshot, a log compaction or a purge. The cluster
/// maintains itself; the admin API offers no lever for it.
///
/// Asserted against the *served* document rather than the YAML on disk: `src/openapi.rs`'s parity
/// tests force every route in `HANDLE_DIRECT_ROUTES` into the contract, so a route added there
/// surfaces here even if the YAML was edited to match.
#[tokio::test]
async fn the_contract_offers_no_membership_or_snapshot_operation() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();

    let response = reqwest::get(format!("http://{admin}/openapi.json"))
        .await
        .expect("openapi.json");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "{seen}");
    let doc = seen.json();
    let paths = doc
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .expect("paths object");

    const HTTP_METHODS: [&str; 7] = ["get", "put", "post", "delete", "options", "head", "patch"];
    for (path, item) in paths {
        let lower = path.to_ascii_lowercase();
        for forbidden in ["snapshot", "compact", "purge", "learner", "voter"] {
            assert!(
                !lower.contains(forbidden),
                "{path} is published: the admin API must not offer a {forbidden} operation"
            );
        }
        if lower.contains("member") {
            let methods: Vec<&str> = item
                .as_object()
                .map(|ops| {
                    ops.keys()
                        .map(String::as_str)
                        .filter(|k| HTTP_METHODS.contains(k))
                        .collect()
                })
                .unwrap_or_default();
            assert_eq!(
                methods,
                ["get"],
                "{path} must stay a read-only projection of membership, got {methods:?}"
            );
        }
    }

    server.shutdown().await;
}

/// The contract maps every tenancy and audit route, so it is authenticated — the same posture as
/// `/admin/whoami`. Without a configured key the node is in bootstrap bypass and answers anyone,
/// which is why this test configures one: otherwise it would assert nothing.
///
/// Pins D-44 (the key half): a configured `--api-key` alone closes the open admin plane.
#[tokio::test]
async fn openapi_json_requires_authentication_once_a_key_is_configured() {
    let state = TempDir::new().expect("tempdir");
    let key = "openapi-admin-key";
    let server = compose::start(cluster_cli(&state, &["--api-key", key]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();

    let response = client
        .get(format!("http://{admin}/openapi.json"))
        .send()
        .await
        .expect("unauthenticated request");
    let seen = Seen::of(response).await;
    assert_eq!(
        seen.status, 401,
        "the contract must not be readable by an unauthenticated scanner: {seen}"
    );

    let response = client
        .get(format!("http://{admin}/openapi.json"))
        .header("authorization", key)
        .send()
        .await
        .expect("authenticated request");
    let seen = Seen::of(response).await;
    assert_eq!(
        seen.status, 200,
        "a valid key must read the contract: {seen}"
    );
    assert!(seen.body.contains("\"openapi\""), "{seen}");

    server.shutdown().await;
}
