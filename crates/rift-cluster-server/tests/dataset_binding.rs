//! Issue #286 (RFC-005 D2) gate: a `_rift.dataset` block actually serves rows.
//!
//! The unit tests in `rift_cluster::datasets` prove the two transforms in isolation. These prove
//! the thing that matters to a user and that no pure test can: that a bound stub, committed
//! through Raft and applied into a live engine, answers with the row — and that the pin holds when
//! the dataset moves underneath it.
//!
//! Datasets are seeded through `node().submit(DatasetPut)` rather than over HTTP because D2 ships
//! no dataset routes — those are D3 (#287). That is a property of the slice, not a shortcut: the
//! op is the same one the D3 route will commit, so what is exercised here is the whole path from a
//! committed dataset to a served byte.

use std::time::Duration;

use clap::Parser;
use rift_cluster::control::{DatasetRecord, Digest};
use rift_cluster::{ControlOp, ControlOutcome, ControlRequest, TenantId};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

mod common;

use common::ports::reserve_port;

const SECRET: &str = "dataset-binding-test-secret";

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

fn digest_hex(csv: &str) -> String {
    use sha2::{Digest as _, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(csv.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Commit a dataset, keyed on its first column. Every call is a new version of `name`.
async fn put_dataset(server: &ComposedServer, name: &str, csv: &str) {
    let mut lines = csv.lines();
    let columns: Vec<String> = lines
        .next()
        .unwrap_or_default()
        .split(',')
        .map(|c| c.trim().to_owned())
        .collect();
    let rows = lines.count() as u64;
    let response = server
        .node()
        .expect("clustered")
        .submit(ControlRequest {
            op_id: uuid::Uuid::new_v4(),
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op: ControlOp::DatasetPut {
                tenant: TenantId::default(),
                record: DatasetRecord {
                    name: name.to_owned(),
                    digest: Digest::new(digest_hex(csv)),
                    key_columns: vec![columns[0].clone()],
                    delimiter: ',',
                    columns,
                    rows,
                    bytes: csv.len() as u64,
                },
                csv: Some(csv.to_owned()),
                origin: 0,
            },
        })
        .await
        .expect("dataset commits");
    assert_eq!(response.outcome, ControlOutcome::Applied, "dataset applied");
}

/// An imposter whose single stub binds `dataset` and echoes one column.
fn bound_imposter(port: u16, dataset: &str, version: Option<u64>, body: &str) -> serde_json::Value {
    let mut binding = json!({
        "name": dataset,
        "key": { "from": { "query": "id" }, "using": { "method": "regex", "selector": ".*" } },
        "keyColumn": "id",
        "into": "${row}"
    });
    if let Some(version) = version {
        binding["version"] = json!(version);
    }
    json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "id": "bound",
            "responses": [{
                "is": { "statusCode": 200, "body": body },
                "_rift": { "dataset": binding }
            }]
        }]
    })
}

async fn post_imposter(admin: std::net::SocketAddr, body: &serde_json::Value) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(body)
        .send()
        .await
        .expect("post imposter");
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

/// Fetch `?id=<key>` from the imposter's own data port, retrying until it is listening.
async fn served(port: u16, key: &str) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = reqwest::get(format!("http://127.0.0.1:{port}/?id={key}")).await
            && response.status().as_u16() == 200
            && let Ok(body) = response.text().await
        {
            return body;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "imposter on {port} never served"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

const CUSTOMERS: &str = "id,name,tier\n1,Ada,gold\n2,Grace,silver\n";

/// The whole point of the slice: a bound stub answers with the row the key selects.
///
/// This is what no unit test can reach — it proves the compiled `lookup` was not merely written
/// into the config but was executed by the engine on a live listener, and that `${row}[column]`
/// substitution works through the cluster path rather than only upstream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bound_stub_serves_the_row_its_key_selects() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state)).await.expect("starts");
    wait_ready(&server).await;
    put_dataset(&server, "customers", CUSTOMERS).await;

    let port = reserve_port();
    let (status, body) = post_imposter(
        server.admin_addr(),
        &bound_imposter(port, "customers", None, "${row}[name]/${row}[tier]"),
    )
    .await;
    assert_eq!(status, 201, "binding a live dataset commits: {body}");

    assert_eq!(served(port, "1").await, "Ada/gold");
    assert_eq!(
        served(port, "2").await,
        "Grace/silver",
        "a different key selects a different row"
    );
}

/// RFC-005 §11, deliberately unverified until now: does `{{ }}` templating evaluate lookup output?
///
/// The issue asks for this to be **traced and pinned either way**. It is pinned here as
/// *inert*: a CSV cell containing a literal `{{ uuid }}` is served verbatim, because templating is
/// opt-in per response (`_rift.templated`, upstream #359) and a bound response does not set it.
/// If this ever starts rendering, a dataset becomes a template-injection surface — someone's CSV
/// export would silently execute — so the assertion is on the literal text, not on "not empty".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_template_token_inside_a_dataset_value_is_served_literally() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state)).await.expect("starts");
    wait_ready(&server).await;
    put_dataset(&server, "templated", "id,note\n1,{{ uuid }}\n2,plain\n").await;

    let port = reserve_port();
    let (status, body) = post_imposter(
        server.admin_addr(),
        &bound_imposter(port, "templated", None, "${row}[note]"),
    )
    .await;
    assert_eq!(status, 201, "{body}");

    assert_eq!(
        served(port, "1").await,
        "{{ uuid }}",
        "a template token in dataset data must be inert — rendering it would make any uploaded \
         CSV an injection surface"
    );
}

/// The pin holds: a later upload does not move a serving stub.
///
/// This is the property that makes a binding safe to leave running. Without the pin, uploading a
/// corrected dataset would silently change what every bound stub answers — with no config write
/// and nothing in the audit trail to explain it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_upload_does_not_move_a_bound_stub() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state)).await.expect("starts");
    wait_ready(&server).await;
    put_dataset(&server, "customers", CUSTOMERS).await;

    let port = reserve_port();
    let (status, body) = post_imposter(
        server.admin_addr(),
        &bound_imposter(port, "customers", None, "${row}[name]"),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(served(port, "1").await, "Ada");

    // v2 renames the same key. The bound stub is pinned to v1 and must not notice.
    put_dataset(&server, "customers", "id,name,tier\n1,Ada-v2,gold\n").await;
    assert_eq!(
        served(port, "1").await,
        "Ada",
        "the stub is pinned to the version it bound; a new upload is not a config change"
    );
}

/// Admission refuses a binding to a dataset that is not there, and says which.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binding_an_absent_dataset_is_refused_with_its_name() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state)).await.expect("starts");
    wait_ready(&server).await;

    let port = reserve_port();
    let (status, body) = post_imposter(
        server.admin_addr(),
        &bound_imposter(port, "nope", None, "${row}[name]"),
    )
    .await;
    assert_eq!(status, 400, "an absent dataset is refused: {body}");
    assert!(
        body.contains("nope"),
        "the refusal names the dataset the operator asked for: {body}"
    );
}

/// Admission refuses a `keyColumn` the dataset does not declare, and says which.
///
/// Refused at admission rather than left to fail at lookup time: an undeclared column carries no
/// uniqueness proof, so the engine would pick among duplicate matches in hash order — a stub that
/// answers differently on different nodes, which is precisely what a replicated mock must not do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binding_an_undeclared_key_column_is_refused_with_its_name() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state)).await.expect("starts");
    wait_ready(&server).await;
    put_dataset(&server, "customers", CUSTOMERS).await;

    let port = reserve_port();
    let mut imposter = bound_imposter(port, "customers", None, "${row}[name]");
    imposter["stubs"][0]["responses"][0]["_rift"]["dataset"]["keyColumn"] = json!("tier");

    let (status, body) = post_imposter(server.admin_addr(), &imposter).await;
    assert_eq!(status, 400, "an undeclared key column is refused: {body}");
    assert!(
        body.contains("tier"),
        "the refusal names the column: {body}"
    );
}

/// The stored record keeps the operator's own binding, not the compiled result.
///
/// `GET` must answer with what was written — the declarative block, now carrying the resolved
/// pin. Rendering the compiled `lookup` instead would show an operator a node-local filesystem
/// path they never wrote and cannot act on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stored_record_keeps_the_declarative_binding() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state)).await.expect("starts");
    wait_ready(&server).await;
    put_dataset(&server, "customers", CUSTOMERS).await;

    let admin = server.admin_addr();
    let port = reserve_port();
    let (status, body) = post_imposter(
        admin,
        &bound_imposter(port, "customers", None, "${row}[name]"),
    )
    .await;
    assert_eq!(status, 201, "{body}");

    let rendered = reqwest::get(format!("http://{admin}/imposters/{port}"))
        .await
        .expect("get imposter")
        .text()
        .await
        .expect("body");

    assert!(
        rendered.contains("\"dataset\""),
        "the declarative block survives the round trip: {rendered}"
    );
    assert!(
        rendered.contains(&digest_hex(CUSTOMERS)),
        "and carries the resolved pin, so an operator can see what it is bound to: {rendered}"
    );
    assert!(
        !rendered.contains("fromDataSource"),
        "the compiled lookup is engine-facing only and must not leak into the stored record: \
         {rendered}"
    );
}
