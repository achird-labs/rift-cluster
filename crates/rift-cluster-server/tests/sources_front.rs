//! Issue #239: the imposter-source inspection surface on the **public admin
//! front** — `GET /admin/sources` and `GET /admin/sources/{id}`.
//!
//! The cluster-port surface (`tests/sources.rs`, issue #134) already proves
//! the source lifecycle end to end; what is proven here is the read surface an
//! operator's console actually reaches: RBAC-gated at `source.read`, scoped by
//! `X-Rift-Tenant`, answering the §8.4 cross-tenant `404`, and — the reason
//! this API needed a decision rather than plumbing — keeping fleet-replicated
//! facts and this-node-only facts structurally apart in the response.
//!
//! These drive `compose::start` and speak plain HTTP to the public admin
//! address, exactly as `tenancy_api.rs` does, seeding fixture state by
//! submitting `ControlOp`s directly through the node.

use std::time::Duration;

use clap::Parser;
use rift_cluster::control::{OnDrift, Quotas, Role, SourceMode};
use rift_cluster::{ControlOp, ControlRequest, RaftNode, TenantId};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::Value;
use tempfile::TempDir;

mod common;

use common::seen::Seen;

const SECRET: &str = "sources-front-test-secret";

/// A clustered invocation on an explicit cluster `bind`. Mirrors
/// `tenancy_api.rs`'s `cluster_on` — each test binary carries its own copy of
/// small fixtures like this, per `tests/common/mod.rs`.
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

fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    let mut args = vec!["--cluster-allow-solo"];
    args.extend_from_slice(extra);
    cluster_on(state, "127.0.0.1:0", &args)
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

/// Submit `op` directly through the node — fixture setup only. Everything
/// actually *under test* here goes through the HTTP surface.
async fn seed(node: &RaftNode, op_id: &mut u128, op: ControlOp) {
    let response = node
        .write(ControlRequest {
            op_id: uuid::Uuid::from_u128(*op_id),
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op,
        })
        .await
        .expect("seed op commits");
    assert_eq!(
        response.outcome,
        rift_cluster::ControlOutcome::Applied,
        "seed op {op_id} must apply cleanly"
    );
    *op_id += 1;
}

fn tenant_put(tenant: &str) -> ControlOp {
    ControlOp::TenantPut {
        tenant: TenantId::new(tenant),
        display_name: tenant.to_owned(),
        quotas: Quotas::default(),
        journal_retention_secs: 0,
    }
}

/// A pinned source declaration; the `scripted` scheme is never pulled here, so
/// no provider for it needs to exist.
fn source_put(tenant: &str, id: &str) -> ControlOp {
    ControlOp::SourcePut {
        tenant: TenantId::new(tenant),
        id: id.to_owned(),
        uri: format!("scripted://cfg/{id}.json"),
        mode: SourceMode::Pinned,
        auth_ref: None,
        on_drift: OnDrift::Overwrite,
        poll_secs: None,
    }
}

/// A principal reachable from a real `Authorization: <raw_key>` header, bound
/// `role` in `tenant`. Same derivation rule as `rbac.rs`: the id must be
/// `api_key_principal_id(raw_key)` or `principal::resolve_bindings` can never
/// find it.
async fn seed_key(node: &RaftNode, op_id: &mut u128, tenant: &str, raw_key: &str, role: Role) {
    let principal = rift_cluster::control::Principal {
        id: rift_cluster::control::api_key_principal_id(raw_key),
        display_name: format!("{role:?}-{tenant}"),
        auth: rift_cluster::control::AuthSource::ApiKey {
            hash: rift_cluster::control::hash_api_key(raw_key),
        },
        disabled: false,
    };
    let id = principal.id.clone();
    seed(
        node,
        op_id,
        ControlOp::PrincipalPut {
            tenant: TenantId::new(tenant),
            principal,
        },
    )
    .await;
    seed(
        node,
        op_id,
        ControlOp::BindingPut {
            tenant: TenantId::new(tenant),
            principal_id: id,
            role,
        },
    )
    .await;
}

async fn get(client: &reqwest::Client, admin: &str, path: &str, key: &str, tenant: &str) -> Seen {
    let response = client
        .get(format!("http://{admin}{path}"))
        .header("authorization", key)
        .header("x-rift-tenant", tenant)
        .send()
        .await
        .expect("request sends");
    Seen::of(response).await
}

struct Fixture {
    _state: TempDir,
    server: ComposedServer,
    admin: String,
}

/// One solo cluster with two tenants, a Viewer key in `acme`, and one source
/// declared in each tenant.
async fn start() -> Fixture {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let mut op_id = 1u128;

    seed(node, &mut op_id, tenant_put("acme")).await;
    seed(node, &mut op_id, tenant_put("other")).await;
    seed_key(node, &mut op_id, "acme", VIEWER_KEY, Role::Viewer).await;
    seed(node, &mut op_id, source_put("acme", "payments")).await;
    seed(node, &mut op_id, source_put("acme", "billing")).await;
    seed(node, &mut op_id, source_put("other", "foreign")).await;

    let admin = server.admin_addr().to_string();
    Fixture {
        _state: state,
        server,
        admin,
    }
}

const VIEWER_KEY: &str = "sources-front-viewer-key";

#[tokio::test]
async fn a_viewer_reads_sources_with_fleet_and_node_facts_apart() {
    let fixture = start().await;
    let client = reqwest::Client::new();

    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources",
        VIEWER_KEY,
        "acme",
    )
    .await;
    assert_eq!(seen.status, 200, "viewer list: {seen}");
    let body: Value = serde_json::from_str(&seen.body).expect("list is JSON");

    // The fleet-replicated half: exactly the store projection, id-ascending,
    // and only this tenant's rows.
    let sources = body["sources"].as_array().expect("sources array");
    let ids: Vec<&str> = sources
        .iter()
        .map(|s| s["id"].as_str().expect("id"))
        .collect();
    assert_eq!(ids, ["billing", "payments"], "id-ascending, acme only");
    for record in sources {
        assert_eq!(record["mode"], "pinned");
        assert_eq!(record["onDrift"], "overwrite");
        assert!(record["revision"].is_u64(), "revision travels in the body");
        // The whole point of #239's design decision: a node-local observation
        // must never sit on the fleet-replicated record, where two converged
        // nodes' answers would stop being byte-comparable.
        assert!(
            record.get("lastPollError").is_none(),
            "poll status must not be flattened into the replicated record: {record}"
        );
    }

    // The node-local half: names its scope (which node answered), and carries
    // poll errors — none here, which an empty map states explicitly rather
    // than by omission.
    let node_local = body["nodeLocal"].as_object().expect("nodeLocal object");
    assert!(node_local["nodeId"].is_u64(), "the observing node is named");
    assert_eq!(
        node_local["pollErrors"],
        serde_json::json!({}),
        "no poll has failed on this node"
    );

    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources/payments",
        VIEWER_KEY,
        "acme",
    )
    .await;
    assert_eq!(seen.status, 200, "viewer get: {seen}");
    let body: Value = serde_json::from_str(&seen.body).expect("get is JSON");
    assert_eq!(body["source"]["id"], "payments");
    assert_eq!(body["source"]["uri"], "scripted://cfg/payments.json");
    assert!(body["source"].get("lastPollError").is_none());
    assert!(body["nodeLocal"]["nodeId"].is_u64());

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn a_source_in_another_tenant_answers_404_not_403() {
    let fixture = start().await;
    let client = reqwest::Client::new();

    // `foreign` exists — in `other`. To an `acme`-scoped read it must be
    // indistinguishable from a source that never existed (RFC-002 §8.4): a 403
    // would confirm the id is real in someone else's tenant.
    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources/foreign",
        VIEWER_KEY,
        "acme",
    )
    .await;
    assert_eq!(seen.status, 404, "cross-tenant read: {seen}");

    // And the tenant the caller holds no binding in answers 404 for the whole
    // surface, list included — same §8.4 rule, one level up.
    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources",
        VIEWER_KEY,
        "other",
    )
    .await;
    assert_eq!(seen.status, 404, "unbound tenant list: {seen}");

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn an_unauthenticated_read_answers_401() {
    let fixture = start().await;
    let client = reqwest::Client::new();

    // Same precedent as `tenancy_api.rs`'s dedicated 401 test: the shared gate
    // is proven generically elsewhere, but this is what pins these two routes
    // to it if the dispatch ever changes.
    for path in ["/admin/sources", "/admin/sources/payments"] {
        let response = client
            .get(format!("http://{}{path}", fixture.admin))
            .header("x-rift-tenant", "acme")
            .send()
            .await
            .expect("request sends");
        let seen = Seen::of(response).await;
        assert_eq!(seen.status, 401, "unauthenticated {path}: {seen}");
    }

    fixture.server.shutdown().await;
}
