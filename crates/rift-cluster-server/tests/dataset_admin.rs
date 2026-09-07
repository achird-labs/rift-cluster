//! Issue #287 (RFC-005 D3) gate: the dataset admin surface, over real HTTP.
//!
//! The unit tests prove the router and the RBAC table in isolation. These prove the things only a
//! live front can show: which role gets which status, that a cross-tenant probe is
//! indistinguishable from a missing one, and that a content read serves the uploaded bytes back
//! as CSV.

use std::time::Duration;

use clap::Parser;
use rift_cluster::control::{AuthSource, FLEET_SCOPE, Principal, PrincipalId, Role, hash_api_key};
use rift_cluster::{ControlOp, ControlRequest, RaftNode, TenantId};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

mod common;

const SECRET: &str = "dataset-admin-test-secret";
const CUSTOMERS: &str = "id,name,tier\n1,Ada,gold\n2,Grace,silver\n";

fn cluster_cli(state: &TempDir) -> EeCli {
    EeCli::try_parse_from([
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
    ])
    .expect("parses")
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
    node.submit(ControlRequest {
        op_id: uuid::Uuid::from_u128(op_id),
        principal: None,
        issued_at_secs: 0,
        expected_revision: None,
        op,
    })
    .await
    .expect("seed commits");
}

/// A principal bound to exactly one `(tenant, role)`; returns its raw API key.
async fn seed_principal(
    node: &RaftNode,
    op_id: &mut u128,
    label: &str,
    tenant: &str,
    role: Role,
) -> String {
    let raw_key = format!("dataset-key-{label}");
    let principal = Principal {
        id: rift_cluster::control::api_key_principal_id(&raw_key),
        display_name: label.to_owned(),
        auth: AuthSource::ApiKey {
            hash: hash_api_key(&raw_key),
        },
        disabled: false,
    };
    let principal_id: PrincipalId = principal.id.clone();
    seed(
        node,
        *op_id,
        ControlOp::PrincipalPut {
            tenant: TenantId::new(tenant),
            principal,
        },
    )
    .await;
    *op_id += 1;
    let binding_tenant = if role == Role::FleetAdmin {
        FLEET_SCOPE
    } else {
        tenant
    };
    seed(
        node,
        *op_id,
        ControlOp::BindingPut {
            tenant: TenantId::new(binding_tenant),
            principal_id,
            role,
        },
    )
    .await;
    *op_id += 1;
    raw_key
}

struct Fixture {
    _state: TempDir,
    server: ComposedServer,
    admin: std::net::SocketAddr,
    viewer: String,
    editor: String,
    client: reqwest::Client,
}

/// A solo node with `acme` and `other` tenants, and a viewer and an editor principal in `acme`.
async fn fixture() -> Fixture {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state)).await.expect("starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered").clone();
    let mut op_id = 1u128;
    for tenant in ["acme", "other"] {
        seed(
            &node,
            op_id,
            ControlOp::TenantPut {
                tenant: TenantId::new(tenant),
                display_name: tenant.to_owned(),
                quotas: rift_cluster::control::Quotas::default(),
                journal_retention_secs: 0,
            },
        )
        .await;
        op_id += 1;
    }
    let viewer = seed_principal(&node, &mut op_id, "viewer", "acme", Role::Viewer).await;
    let editor = seed_principal(&node, &mut op_id, "editor", "acme", Role::Editor).await;
    let admin = server.admin_addr();
    Fixture {
        _state: state,
        server,
        admin,
        viewer,
        editor,
        client: reqwest::Client::new(),
    }
}

impl Fixture {
    async fn upload(&self, key: &str, tenant: &str, name: &str, csv: &str) -> (u16, String) {
        let response = self
            .client
            .post(format!(
                "http://{}/admin/tenants/{tenant}/datasets",
                self.admin
            ))
            .header("authorization", key)
            .header("x-rift-dataset-name", name)
            .header("x-rift-dataset-key-columns", "id")
            .header("content-type", "text/csv")
            .body(csv.to_owned())
            .send()
            .await
            .expect("upload");
        let status = response.status().as_u16();
        (status, response.text().await.unwrap_or_default())
    }

    async fn get(&self, key: &str, path: &str) -> (u16, String, Option<String>) {
        let response = self
            .client
            .get(format!("http://{}{path}", self.admin))
            .header("authorization", key)
            .send()
            .await
            .expect("get");
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        (
            status,
            response.text().await.unwrap_or_default(),
            content_type,
        )
    }

    async fn delete(&self, key: &str, path: &str) -> (u16, String) {
        let response = self
            .client
            .delete(format!("http://{}{path}", self.admin))
            .header("authorization", key)
            .send()
            .await
            .expect("delete");
        let status = response.status().as_u16();
        (status, response.text().await.unwrap_or_default())
    }
}

/// E1/E2 — the read/redefine ladder, end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_viewer_reads_datasets_but_only_an_editor_writes_them() {
    let f = fixture().await;
    let (status, body) = f.upload(&f.editor, "acme", "customers", CUSTOMERS).await;
    assert_eq!(status, 201, "an Editor uploads: {body}");

    let (status, _, _) = f.get(&f.viewer, "/admin/tenants/acme/datasets").await;
    assert_eq!(status, 200, "a Viewer lists");
    let (status, _, _) = f
        .get(&f.viewer, "/admin/tenants/acme/datasets/customers")
        .await;
    assert_eq!(status, 200, "a Viewer reads history");

    let (status, _) = f.upload(&f.viewer, "acme", "other", CUSTOMERS).await;
    assert_eq!(status, 403, "a Viewer does not upload");
    let (status, _) = f
        .delete(&f.viewer, "/admin/tenants/acme/datasets/customers")
        .await;
    assert_eq!(status, 403, "a Viewer does not delete");
}

/// E3 — a cross-tenant probe is a 404, and byte-identical to a missing dataset.
///
/// 403 would confirm the dataset exists, which is the leak RFC-002 §8.4 closes. Asserting the two
/// bodies are *equal* is the part that matters: two different 404s would still be an oracle.
///
/// Pins D-45: another tenant's dataset and a missing dataset are one byte-identical 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_tenant_probe_is_indistinguishable_from_a_missing_dataset() {
    let f = fixture().await;
    let (status, body) = f.upload(&f.editor, "acme", "customers", CUSTOMERS).await;
    assert_eq!(status, 201, "{body}");

    // `acme`'s Editor reaching into `other`, where the dataset does not exist either way.
    let (cross_status, cross_body, _) = f
        .get(&f.editor, "/admin/tenants/other/datasets/customers")
        .await;
    let (absent_status, absent_body, _) = f
        .get(&f.editor, "/admin/tenants/acme/datasets/nonexistent")
        .await;

    assert_eq!(cross_status, 404, "a cross-tenant probe is 404, not 403");
    assert_eq!(absent_status, 404);
    assert_eq!(
        cross_body, absent_body,
        "the two 404s must be byte-identical, or the difference is the oracle"
    );
}

/// E4 — a content read serves the uploaded bytes back, byte for byte, as CSV.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_content_read_serves_the_uploaded_bytes_as_csv() {
    let f = fixture().await;
    let (status, body) = f.upload(&f.editor, "acme", "customers", CUSTOMERS).await;
    assert_eq!(status, 201, "{body}");

    // A listing and a history read both answer, as JSON.
    let (status, _, content_type) = f.get(&f.editor, "/admin/tenants/acme/datasets").await;
    assert_eq!(status, 200);
    assert_eq!(content_type.as_deref(), Some("application/json"));
    let (status, _, _) = f
        .get(&f.editor, "/admin/tenants/acme/datasets/customers")
        .await;
    assert_eq!(status, 200);

    // The export.
    let (status, csv, content_type) = f
        .get(
            &f.editor,
            "/admin/tenants/acme/datasets/customers/1/content",
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(csv, CUSTOMERS, "the bytes come back exactly as uploaded");
    assert_eq!(
        content_type.as_deref(),
        Some("text/csv; charset=utf-8"),
        "a CSV is served as one, not as JSON"
    );
}

/// E10 — an upload commits through the #285 pipeline and is immediately readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upload_commits_and_is_readable_straight_back() {
    let f = fixture().await;
    let (status, body) = f.upload(&f.editor, "acme", "customers", CUSTOMERS).await;
    assert_eq!(status, 201, "{body}");
    assert!(
        body.contains("\"rows\":2"),
        "the response reports what was stored: {body}"
    );

    let (status, listing, _) = f.get(&f.editor, "/admin/tenants/acme/datasets").await;
    assert_eq!(status, 200);
    assert!(listing.contains("customers"), "{listing}");
    assert!(
        listing.contains("\"bindings\":0"),
        "nothing binds it yet: {listing}"
    );

    // A second upload of the same name is a new version, never a mutation of the first.
    let (status, _) = f
        .upload(
            &f.editor,
            "acme",
            "customers",
            "id,name,tier\n1,Ada-v2,gold\n",
        )
        .await;
    assert_eq!(status, 201);
    let (status, history, _) = f
        .get(&f.editor, "/admin/tenants/acme/datasets/customers")
        .await;
    assert_eq!(status, 200);
    assert!(
        history.contains("\"version\":1") && history.contains("\"version\":2"),
        "both versions survive: {history}"
    );

    // v1's bytes are still v1's — the point of versioning.
    let (status, csv, _) = f
        .get(
            &f.editor,
            "/admin/tenants/acme/datasets/customers/1/content",
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        csv, CUSTOMERS,
        "an upload never rewrites an existing version"
    );
}

/// E11 — #285's validation refuses through the route, naming what is wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_key_is_refused_naming_the_column() {
    let f = fixture().await;
    let (status, body) = f
        .upload(&f.editor, "acme", "dupes", "id,name\n1,Ada\n1,Grace\n")
        .await;
    assert_eq!(status, 400, "a duplicate key cannot commit: {body}");
    assert!(
        body.contains("id"),
        "the refusal names the offending column: {body}"
    );
}

/// E7 — an absent version is a 404, not a 500 and not a coerced 'latest'.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_content_read_of_an_absent_version_is_a_404() {
    let f = fixture().await;
    let (status, body) = f.upload(&f.editor, "acme", "customers", CUSTOMERS).await;
    assert_eq!(status, 201, "{body}");

    let (status, _, _) = f
        .get(
            &f.editor,
            "/admin/tenants/acme/datasets/customers/99/content",
        )
        .await;
    assert_eq!(status, 404, "there is no version 99");
}

/// E13 — a non-numeric version is a 404, not a 500 and not a coerced 'latest'.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_numeric_version_is_a_404() {
    let f = fixture().await;
    let (status, body) = f.upload(&f.editor, "acme", "customers", CUSTOMERS).await;
    assert_eq!(status, 201, "{body}");

    let (status, _, _) = f
        .get(
            &f.editor,
            "/admin/tenants/acme/datasets/customers/latest/content",
        )
        .await;
    assert_eq!(status, 404, "'latest' is not a version");
}

/// E8/E9 — delete is refused while bound, and succeeds once nothing binds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bound_dataset_cannot_be_deleted_until_its_binding_goes() {
    let f = fixture().await;
    let (status, body) = f.upload(&f.editor, "acme", "customers", CUSTOMERS).await;
    assert_eq!(status, 201, "{body}");

    let port = common::ports::reserve_port();
    let bound = json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "id": "bound",
            "responses": [{
                "is": { "statusCode": 200, "body": "${row}[name]" },
                "_rift": { "dataset": {
                    "name": "customers",
                    "key": { "from": { "query": "id" }, "using": { "method": "regex", "selector": ".*" } },
                    "keyColumn": "id",
                    "into": "${row}"
                } }
            }]
        }]
    });
    let response = f
        .client
        .post(format!("http://{}/imposters", f.admin))
        .header("authorization", f.editor.clone())
        .header("x-rift-tenant", "acme")
        .json(&bound)
        .send()
        .await
        .expect("bind");
    assert_eq!(response.status().as_u16(), 201, "the binding commits");

    let (status, listing, _) = f.get(&f.editor, "/admin/tenants/acme/datasets").await;
    assert_eq!(status, 200);
    assert!(
        listing.contains("\"bindings\":1"),
        "the listing warns that a delete will be refused: {listing}"
    );

    let (status, body) = f
        .delete(&f.editor, "/admin/tenants/acme/datasets/customers")
        .await;
    assert_eq!(status, 409, "a bound dataset is not deletable: {body}");

    // Remove the binding, and the same delete succeeds.
    let response = f
        .client
        .delete(format!("http://{}/imposters/{port}", f.admin))
        .header("authorization", f.editor.clone())
        .header("x-rift-tenant", "acme")
        .send()
        .await
        .expect("unbind");
    assert!(response.status().is_success(), "the imposter goes");

    let (status, body) = f
        .delete(&f.editor, "/admin/tenants/acme/datasets/customers")
        .await;
    assert_eq!(status, 204, "unbound, it deletes: {body}");

    let (status, listing, _) = f.get(&f.editor, "/admin/tenants/acme/datasets").await;
    assert_eq!(status, 200);
    assert!(
        !listing.contains("customers"),
        "and it is gone from the listing: {listing}"
    );
    let _ = &f.server;
}
