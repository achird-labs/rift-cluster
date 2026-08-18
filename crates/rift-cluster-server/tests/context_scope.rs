//! Issue #288 (RFC-005 S1): the two `contextScope` deviations closed on the public admin front.
//!
//! - `fleet` is `FleetAdmin`-gated at admission: a config setting `contextScope: "fleet"` is
//!   refused with a `400` naming the requirement — nothing committed — unless the writing
//!   principal holds `FleetAdmin`; then it is admitted and behaves as before.
//! - `tenant` is a real scope: admitted for any Editor, and its spaces listing (issue #374) is
//!   served under the tenant's own prefix, while a fleet-scoped listing is served only to a
//!   `FleetAdmin` and stays `unavailable: "fleet-scope"` for everyone else.
//!
//! Drives `compose::start` and speaks plain HTTP, seeding tenants and keys by submitting
//! `ControlOp`s directly through the node, exactly as `specs_front.rs` does.

use std::time::Duration;

use clap::Parser;
use rift_cluster::control::{FLEET_SCOPE, Quotas, Role};
use rift_cluster::stores::ClusteredFlowStoreProvider;
use rift_cluster::{ControlOp, ControlRequest, RaftNode, TenantId};
use rift_cluster_base::seams::{FlowStoreProvider as _, ImposterConfig};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;

use common::ports::reserve_port;
use common::seen::Seen;

const SECRET: &str = "context-scope-test-secret";

fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    let mut args = vec![
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

/// A principal reachable from `Authorization: <raw_key>`, bound `role` in `tenant`. A
/// `FleetAdmin` binds on [`FLEET_SCOPE`]; its principal record still lives under `default`, the
/// way `tenancy_api.rs`'s `seed_fleet_admin` mints one.
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
    let record_tenant = if tenant == FLEET_SCOPE {
        TenantId::default()
    } else {
        TenantId::new(tenant)
    };
    seed(
        node,
        op_id,
        ControlOp::PrincipalPut {
            tenant: record_tenant,
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

const EDITOR_KEY: &str = "context-scope-editor-key";
const OTHER_EDITOR_KEY: &str = "context-scope-other-editor-key";
const FLEET_KEY: &str = "context-scope-fleet-admin-key";

struct Fixture {
    _state: TempDir,
    server: ComposedServer,
    admin: String,
}

/// One solo cluster: tenants `acme` and `beta`, an Editor key in each, and a `FleetAdmin` key
/// (bound on the fleet scope, `X-Rift-Tenant: acme` when it acts).
async fn start() -> Fixture {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let mut op_id = 1u128;
    for tenant in ["acme", "beta"] {
        seed(
            node,
            &mut op_id,
            ControlOp::TenantPut {
                tenant: TenantId::new(tenant),
                display_name: tenant.to_owned(),
                quotas: Quotas::default(),
                journal_retention_secs: 0,
            },
        )
        .await;
    }
    seed_key(node, &mut op_id, "acme", EDITOR_KEY, Role::Editor).await;
    seed_key(node, &mut op_id, "beta", OTHER_EDITOR_KEY, Role::Editor).await;
    seed_key(node, &mut op_id, FLEET_SCOPE, FLEET_KEY, Role::FleetAdmin).await;
    let admin = server.admin_addr().to_string();
    Fixture {
        _state: state,
        server,
        admin,
    }
}

fn imposter(port: u16, scope: &str) -> Value {
    json!({
        "port": port,
        "protocol": "http",
        "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "ok" } }] }],
        "_rift": { "flowState": { "contextScope": scope } },
    })
}

async fn post_imposter(admin: &str, key: &str, tenant: &str, body: &Value) -> Seen {
    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .header("authorization", key)
        .header("x-rift-tenant", tenant)
        .json(body)
        .send()
        .await
        .expect("post imposter");
    Seen::of(response).await
}

/// Write one flow-state entry through the composed server's own `FlowNet`, via the same
/// `ClusteredFlowStoreProvider::provide` the engine drives — so the store resolves its scope
/// (and, for `tenant`, its owning tenant) exactly as a served imposter's would.
async fn write_flow_entry(fixture: &Fixture, config: &Value, flow_id: &str) {
    let net = fixture.server.flow_net().expect("clustered").clone();
    let config: ImposterConfig = serde_json::from_value(config.clone()).expect("config parses");
    let store = ClusteredFlowStoreProvider::new(net)
        .provide(&config)
        .expect("the clustered provider always provides");
    let flow_id = flow_id.to_owned();
    tokio::task::spawn_blocking(move || store.set(&flow_id, "step", json!("paid")))
        .await
        .expect("blocking op")
        .expect("flow write");
}

async fn get(admin: &str, key: &str, tenant: &str, path: &str) -> Seen {
    let response = reqwest::Client::new()
        .get(format!("http://{admin}{path}"))
        .header("authorization", key)
        .header("x-rift-tenant", tenant)
        .send()
        .await
        .expect("get");
    Seen::of(response).await
}

#[tokio::test]
async fn fleet_scope_needs_fleet_admin_at_admission_and_nothing_lands_otherwise() {
    let fixture = start().await;
    let port = reserve_port();
    let before = fixture
        .server
        .node()
        .expect("clustered")
        .status()
        .last_applied
        .expect("applied");

    // An Editor of `acme` — a real, authorized principal — is refused: the scope crosses every
    // tenant's boundary, and only `FleetAdmin` may cross it.
    let seen = post_imposter(&fixture.admin, EDITOR_KEY, "acme", &imposter(port, "fleet")).await;
    assert_eq!(seen.status, 400, "{seen}");
    assert!(seen.body.contains("contextScope"), "names the knob: {seen}");
    assert!(
        seen.body.contains("FleetAdmin"),
        "names the requirement: {seen}"
    );
    assert_eq!(
        fixture
            .server
            .node()
            .expect("clustered")
            .status()
            .last_applied
            .expect("applied"),
        before,
        "a refused fleet config must not write a single log entry"
    );
    let seen = get(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &format!("/imposters/{port}"),
    )
    .await;
    assert_eq!(seen.status, 404, "nothing committed: {seen}");

    // A batch carrying one fleet-scoped config is refused as a whole, before any op parks.
    let seen = Seen::of(
        reqwest::Client::new()
            .put(format!("http://{}/imposters", fixture.admin))
            .header("authorization", EDITOR_KEY)
            .header("x-rift-tenant", "acme")
            .json(&json!({ "imposters": [imposter(port, "imposter"), imposter(reserve_port(), "fleet")] }))
            .send()
            .await
            .expect("put imposters"),
    )
    .await;
    assert_eq!(seen.status, 400, "{seen}");
    assert!(seen.body.contains("FleetAdmin"), "{seen}");
    let seen = get(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &format!("/imposters/{port}"),
    )
    .await;
    assert_eq!(
        seen.status, 404,
        "the imposter-scoped half of a refused batch did not land either: {seen}"
    );

    // The FleetAdmin is admitted. `GET /imposters/{port}` cannot show the scope — upstream's
    // `_rift.flowState` echo is a fail-closed allowlist that never carries `contextScope` — so
    // the applied scope is observed through the one route that resolves it: the spaces listing,
    // which an Editor is refused for a fleet-scoped imposter and served for any other.
    let seen = post_imposter(&fixture.admin, FLEET_KEY, "acme", &imposter(port, "fleet")).await;
    assert_eq!(seen.status, 201, "{seen}");
    let seen = get(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &format!("/imposters/{port}"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    let seen = get(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &format!("/imposters/{port}/spaces"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    assert_eq!(
        seen.json()["unavailable"],
        "fleet-scope",
        "the applied config is fleet-scoped: {seen}"
    );

    // Migration honesty: the fleet imposter keeps serving, but an Editor re-admitting it now
    // needs FleetAdmin — the same 400 — while re-admitting it back at `imposter` scope goes
    // through, so nothing is stuck.
    let seen = post_imposter(&fixture.admin, EDITOR_KEY, "acme", &imposter(port, "fleet")).await;
    assert!(
        seen.status == 400 && seen.body.contains("FleetAdmin"),
        "an Editor cannot re-admit a fleet-scoped config: {seen}"
    );
    let seen = get(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &format!("/imposters/{port}/spaces"),
    )
    .await;
    assert_eq!(
        seen.json()["unavailable"],
        "fleet-scope",
        "still serving as admitted, fleet-scoped: {seen}"
    );
    let seen = post_imposter(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &imposter(port, "imposter"),
    )
    .await;
    assert_eq!(
        seen.status, 201,
        "an Editor may take it back to imposter scope: {seen}"
    );
    let seen = get(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &format!("/imposters/{port}/spaces"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    assert!(
        seen.json().get("unavailable").is_none(),
        "back at imposter scope the Editor's listing is served: {seen}"
    );

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn tenant_scope_is_admitted_for_an_editor_and_its_spaces_list_is_served() {
    let fixture = start().await;
    let port = reserve_port();
    let seen = post_imposter(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &imposter(port, "tenant"),
    )
    .await;
    assert_eq!(
        seen.status, 201,
        "an Editor may opt into tenant scope: {seen}"
    );

    // A write through the served imposter's own store lands under `tacme:` — the tenant is not
    // in the config, so this is the provider resolving it from the control-plane owner of the
    // port, on the real bound `FlowNet`.
    write_flow_entry(&fixture, &imposter(port, "tenant"), "checkout").await;

    // Issue #374's listing: a tenant-scoped imposter's spaces are bounded by the caller's own
    // tenant prefix, so the listing is served — no `unavailable`, not partial — and it finds
    // the row, which it can only do if the listing's `t<tenant>:` and the store's agree.
    let seen = get(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &format!("/imposters/{port}/spaces"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    let body = seen.json();
    assert!(
        body.get("unavailable").is_none(),
        "a tenant-scoped listing is servable: {body}"
    );
    assert_eq!(body["partial"], false, "{body}");
    let rows = body["spaces"].as_array().expect("spaces array");
    assert_eq!(
        rows.len(),
        1,
        "the one space written under `tacme:`: {body}"
    );
    assert_eq!(rows[0]["space"], "checkout", "{body}");
    assert_eq!(rows[0]["entryCount"], 1, "{body}");

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn a_fleet_scoped_spaces_list_is_served_to_fleet_admin_only() {
    let fixture = start().await;
    let port = reserve_port();
    let seen = post_imposter(&fixture.admin, FLEET_KEY, "acme", &imposter(port, "fleet")).await;
    assert_eq!(seen.status, 201, "{seen}");
    write_flow_entry(&fixture, &imposter(port, "fleet"), "checkout").await;

    // An Editor of the owning tenant still gets #374's refusal: the `f:` namespace carries no
    // tenant component, so a listing would enumerate every tenant's fleet-scoped flows.
    let seen = get(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &format!("/imposters/{port}/spaces"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    let body = seen.json();
    assert_eq!(body["unavailable"], "fleet-scope", "{body}");
    assert_eq!(body["partial"], true, "{body}");
    assert_eq!(body["spaces"], json!([]), "{body}");

    // The FleetAdmin — whose whole role is to cross that boundary — is served the real list.
    let seen = get(
        &fixture.admin,
        FLEET_KEY,
        "acme",
        &format!("/imposters/{port}/spaces"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    let body = seen.json();
    assert!(
        body.get("unavailable").is_none(),
        "the gate that #374 was waiting for exists now: {body}"
    );
    assert_eq!(body["partial"], false, "{body}");
    let rows = body["spaces"].as_array().expect("spaces array");
    assert_eq!(
        rows.len(),
        1,
        "the real `f:` row, not an empty pass: {body}"
    );
    assert_eq!(rows[0]["space"], "checkout", "{body}");
    assert_eq!(rows[0]["entryCount"], 1, "{body}");

    // And a beta Editor cannot even see the imposter — the ownership gate is upstream of all of
    // this, unchanged.
    let seen = get(
        &fixture.admin,
        OTHER_EDITOR_KEY,
        "beta",
        &format!("/imposters/{port}/spaces"),
    )
    .await;
    assert_eq!(
        seen.status, 404,
        "another tenant's port is not there: {seen}"
    );

    fixture.server.shutdown().await;
}

/// The gate is about who *sets* the scope, not who touches the imposter afterwards. An Editor
/// replacing a stub by index (`PUT /imposters/{port}/stubs`, which the front commits as a
/// whole-config `PutImposter` rebuilt from the stored config) sends no `flowState` at all, so on
/// a fleet-scoped imposter a `FleetAdmin` admitted it must go through — refusing it would blame
/// the Editor for a knob they never wrote, while the by-id edit next to it went through.
#[tokio::test]
async fn an_editor_edits_stubs_by_index_on_a_fleet_scoped_imposter_without_the_role() {
    let fixture = start().await;
    let port = reserve_port();
    let seen = post_imposter(&fixture.admin, FLEET_KEY, "acme", &imposter(port, "fleet")).await;
    assert_eq!(seen.status, 201, "{seen}");

    let response = reqwest::Client::new()
        .put(format!("http://{}/imposters/{port}/stubs", fixture.admin))
        .header("authorization", EDITOR_KEY)
        .header("x-rift-tenant", "acme")
        .json(&json!({
            "stubs": [{ "responses": [{ "is": { "statusCode": 204 } }] }]
        }))
        .send()
        .await
        .expect("replace stubs");
    let seen = Seen::of(response).await;
    assert_eq!(
        seen.status, 200,
        "an Editor's stub edit on a fleet-scoped imposter is not an admission of the scope: {seen}"
    );

    // And the imposter is still fleet-scoped afterwards — the rebuild kept the stored knob.
    let seen = get(
        &fixture.admin,
        EDITOR_KEY,
        "acme",
        &format!("/imposters/{port}/spaces"),
    )
    .await;
    assert_eq!(seen.json()["unavailable"], "fleet-scope", "{seen}");

    fixture.server.shutdown().await;
}
