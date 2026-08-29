//! Issue #278 (RFC-004 S2): the `/specs` surface on the public admin front.
//!
//! A spec is a tenant-owned, content-addressed control-plane object: `PUT
//! /specs/{id}` compiles it on the accepting node and commits the bytes,
//! `deploy` compiles again and commits the imposter plus its provenance in one
//! barrier, and every read answers from local applied state. What is proven
//! here is the contract a client sees — RBAC at `spec.*`, the §8.4 cross-tenant
//! `404`, zero log growth on an unchanged re-import, deploy visible on every
//! node after the 2xx, drift + edit-time warnings, and delete/force semantics.
//!
//! Drives `compose::start` and speaks plain HTTP, seeding tenants and keys by
//! submitting `ControlOp`s directly through the node, exactly as
//! `sources_front.rs` does.

use std::time::Duration;

use clap::Parser;
use rift_cluster::control::{Quotas, Role};
use rift_cluster::{ControlOp, ControlRequest, RaftNode, TenantId};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;

use common::ports::reserve_port;
use common::seen::Seen;

const SECRET: &str = "specs-front-test-secret";

/// The compiler crate's own fixture, so what this test deploys is exactly what
/// `rift-cluster-spec`'s golden file pins.
const PETSTORE: &str = include_str!("../../rift-cluster-spec/tests/fixtures/petstore.yaml");

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

/// Submit `op` directly through the node — fixture setup only.
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

const VIEWER_KEY: &str = "specs-front-viewer-key";
const EDITOR_KEY: &str = "specs-front-editor-key";
const OTHER_EDITOR_KEY: &str = "specs-front-other-editor-key";

struct Fixture {
    _state: TempDir,
    server: ComposedServer,
    admin: String,
}

/// One solo cluster with tenants `acme` and `other`, a Viewer and an Editor
/// key in `acme`, and an Editor key in `other`.
async fn start() -> Fixture {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    seed_all(server.node().expect("clustered")).await;
    let admin = server.admin_addr().to_string();
    Fixture {
        _state: state,
        server,
        admin,
    }
}

async fn seed_all(node: &RaftNode) {
    let mut op_id = 1u128;
    seed(node, &mut op_id, tenant_put("acme")).await;
    seed(node, &mut op_id, tenant_put("other")).await;
    seed_key(node, &mut op_id, "acme", VIEWER_KEY, Role::Viewer).await;
    seed_key(node, &mut op_id, "acme", EDITOR_KEY, Role::Editor).await;
    seed_key(node, &mut op_id, "other", OTHER_EDITOR_KEY, Role::Editor).await;
}

struct Api {
    client: reqwest::Client,
    admin: String,
}

impl Api {
    fn new(admin: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            admin: admin.to_owned(),
        }
    }

    fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        key: &str,
        tenant: &str,
    ) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("http://{}{path}", self.admin))
            .header("authorization", key)
            .header("x-rift-tenant", tenant)
    }

    async fn get(&self, path: &str, key: &str, tenant: &str) -> Seen {
        let response = self
            .request(reqwest::Method::GET, path, key, tenant)
            .send()
            .await
            .expect("request sends");
        Seen::of(response).await
    }

    async fn put_spec(&self, id: &str, body: &[u8], key: &str, tenant: &str) -> Seen {
        let response = self
            .request(reqwest::Method::PUT, &format!("/specs/{id}"), key, tenant)
            .header("content-type", "application/yaml")
            .body(body.to_vec())
            .send()
            .await
            .expect("request sends");
        Seen::of(response).await
    }

    async fn post_json(&self, path: &str, body: &Value, key: &str, tenant: &str) -> Seen {
        let response = self
            .request(reqwest::Method::POST, path, key, tenant)
            .json(body)
            .send()
            .await
            .expect("request sends");
        Seen::of(response).await
    }

    async fn delete(&self, path: &str, key: &str, tenant: &str) -> Seen {
        let response = self
            .request(reqwest::Method::DELETE, path, key, tenant)
            .send()
            .await
            .expect("request sends");
        Seen::of(response).await
    }
}

fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn last_applied(server: &ComposedServer) -> u64 {
    server
        .node()
        .expect("clustered")
        .status()
        .last_applied
        .expect("an applied index")
}

/// The stub-served body on the imposter's own data port.
async fn served(port: u16, path: &str) -> Option<(u16, String)> {
    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .header("X-Request-Id", "t")
        .send()
        .await
        .ok()?;
    let status = response.status().as_u16();
    Some((status, response.text().await.ok()?))
}

async fn wait_served(port: u16, path: &str) -> Option<(u16, String)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(seen) = served(port, path).await {
            return Some(seen);
        }
        if std::time::Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Re-issue a write while the fleet answers that it cannot take one yet.
///
/// A node that has just been joined by a follower is briefly unable to accept a
/// write: the membership change is in flight, so the node this test holds is
/// momentarily not the leader, or is parked replaying. The admin API says so
/// honestly — `503 unavailable`, body `no quorum / leader unreachable (parked
/// for replay): local write refused: not the leader` — and a real client
/// retries, which is what `--cluster` clients are documented to do. Asserting
/// on the first answer instead made
/// `deploy_serves_the_compiled_imposter_on_every_node_after_the_2xx` fail three
/// times in one day of CI, once on a plain `master` push with no PR involved.
///
/// **Only 503 is retried, and only until the deadline.** A 503 that persists is
/// returned like any other answer, so the assertion that follows fails with the
/// real status and body rather than with a timeout message that says nothing —
/// and no other status is waited out, so a wrong answer stays a wrong answer.
async fn when_writable<F, Fut>(mut send: F) -> Seen
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Seen>,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = send().await;
        if seen.status != 503 || std::time::Instant::now() > deadline {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn an_editor_imports_a_spec_and_an_unchanged_re_put_writes_nothing() {
    let fixture = start().await;
    let api = Api::new(&fixture.admin);

    let seen = api
        .put_spec("petstore", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 201, "first import creates: {seen}");
    let body = seen.json();
    assert_eq!(body["id"], "petstore");
    assert_eq!(body["format"], "yaml");
    assert_eq!(body["unchanged"], false);
    let digest = body["digest"].as_str().expect("digest is a string");
    assert!(is_sha256_hex(digest), "digest is sha256 hex: {digest}");
    assert!(
        seen.header("rift-cluster-revision").is_some(),
        "an import is an ordinary terminated write and carries the revision: {seen}"
    );
    assert!(seen.header("rift-cluster-op-id").is_some(), "{seen}");
    let after_import = last_applied(&fixture.server);

    // Read back: the stored document is byte-identical to what was sent.
    let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
    assert_eq!(seen.status, 200, "{seen}");
    let record = seen.json();
    assert_eq!(record["id"], "petstore");
    assert_eq!(record["digest"], digest);
    assert_eq!(record["format"], "yaml");
    assert_eq!(record["source"], "inline");
    assert_eq!(record["ports"], json!([]));
    assert_eq!(record["drifted"], false);
    assert_eq!(record["document"], PETSTORE);
    assert!(record["revision"].is_u64());

    // The list: id, digest, bound ports, drifted — and no document.
    let seen = api.get("/specs", EDITOR_KEY, "acme").await;
    assert_eq!(seen.status, 200, "{seen}");
    let list = seen.json();
    let specs = list["specs"].as_array().expect("specs array");
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0]["id"], "petstore");
    assert_eq!(specs[0]["digest"], digest);
    assert_eq!(specs[0]["ports"], json!([]));
    assert_eq!(specs[0]["drifted"], false);
    assert!(
        specs[0].get("document").is_none(),
        "the list never carries documents"
    );

    // Unchanged re-PUT: 200, `unchanged: true`, and not one log entry.
    let seen = api
        .put_spec("petstore", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 200, "{seen}");
    let body = seen.json();
    assert_eq!(body["unchanged"], true);
    assert_eq!(body["digest"], digest);
    assert_eq!(
        last_applied(&fixture.server),
        after_import,
        "an unchanged re-import must not write a single log entry"
    );

    // A changed document is a real re-import: new digest, one more entry.
    let changed = PETSTORE.replace("Swagger Petstore", "Swagger Petstore v2");
    let seen = api
        .put_spec("petstore", changed.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 200, "re-import of an existing id: {seen}");
    let body = seen.json();
    assert_eq!(body["unchanged"], false);
    assert_ne!(body["digest"], digest);
    assert!(last_applied(&fixture.server) > after_import);
    let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
    assert_eq!(seen.json()["document"], changed);

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn a_viewer_lists_and_reads_but_cannot_import_or_delete() {
    let fixture = start().await;
    let api = Api::new(&fixture.admin);
    let seen = api
        .put_spec("petstore", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 201, "{seen}");

    let seen = api.get("/specs", VIEWER_KEY, "acme").await;
    assert_eq!(seen.status, 200, "viewer lists: {seen}");
    assert_eq!(seen.json()["specs"][0]["id"], "petstore");
    let seen = api.get("/specs/petstore", VIEWER_KEY, "acme").await;
    assert_eq!(seen.status, 200, "viewer reads: {seen}");
    let seen = api
        .post_json("/specs/petstore/compile", &json!({}), VIEWER_KEY, "acme")
        .await;
    assert_eq!(seen.status, 200, "compile is a read: {seen}");

    let seen = api
        .put_spec("petstore", PETSTORE.as_bytes(), VIEWER_KEY, "acme")
        .await;
    assert_eq!(seen.status, 403, "viewer cannot import: {seen}");
    assert!(seen.body.contains("spec.write"), "names the action: {seen}");
    let seen = api.delete("/specs/petstore", VIEWER_KEY, "acme").await;
    assert_eq!(seen.status, 403, "viewer cannot delete: {seen}");
    assert!(
        seen.body.contains("spec.delete"),
        "names the action: {seen}"
    );
    let seen = api
        .post_json(
            "/specs/petstore/deploy",
            &json!({ "port": reserve_port() }),
            VIEWER_KEY,
            "acme",
        )
        .await;
    assert_eq!(seen.status, 403, "viewer cannot deploy: {seen}");
    assert!(seen.body.contains("spec.write"), "names the action: {seen}");

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn cross_tenant_probes_answer_exactly_like_absence() {
    let fixture = start().await;
    let api = Api::new(&fixture.admin);
    let seen = api
        .put_spec("foreign", PETSTORE.as_bytes(), OTHER_EDITOR_KEY, "other")
        .await;
    assert_eq!(seen.status, 201, "{seen}");

    let probe = api.get("/specs/foreign", EDITOR_KEY, "acme").await;
    let absent = api.get("/specs/ghost", EDITOR_KEY, "acme").await;
    assert_eq!(probe.status, 404, "another tenant's spec: {probe}");
    assert_eq!(absent.status, 404, "a spec nobody has: {absent}");
    // Same envelope, same kind, same message shape — a probe cannot tell
    // "not yours" from "not there".
    let probe_body = probe.json();
    let absent_body = absent.json();
    assert_eq!(
        probe_body["errors"][0]["type"],
        absent_body["errors"][0]["type"]
    );
    assert_eq!(
        probe_body["errors"][0]["message"]
            .as_str()
            .map(|m| m.replace("foreign", "ghost")),
        absent_body["errors"][0]["message"]
            .as_str()
            .map(str::to_owned),
        "the two 404 bodies differ only by the id the caller typed"
    );

    // Listing under `acme` never shows `other`'s spec.
    let seen = api.get("/specs", EDITOR_KEY, "acme").await;
    assert_eq!(seen.json()["specs"], json!([]));

    // Deploying, compiling and deleting another tenant's spec are the same 404.
    for seen in [
        api.post_json(
            "/specs/foreign/deploy",
            &json!({ "port": reserve_port() }),
            EDITOR_KEY,
            "acme",
        )
        .await,
        api.post_json("/specs/foreign/compile", &json!({}), EDITOR_KEY, "acme")
            .await,
        api.delete("/specs/foreign", EDITOR_KEY, "acme").await,
    ] {
        assert_eq!(seen.status, 404, "{seen}");
    }

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn an_import_that_cannot_compile_or_exceeds_the_cap_is_refused_before_commit() {
    let fixture = start().await;
    let api = Api::new(&fixture.admin);
    let before = last_applied(&fixture.server);

    let seen = api
        .put_spec("bad", b"this: is: not: openapi", EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 400, "a document that does not compile: {seen}");
    assert_eq!(seen.json()["errors"][0]["type"], "bad data");

    let swagger2 = br#"{"swagger":"2.0","info":{"title":"x","version":"1"},"paths":{}}"#;
    let seen = api.put_spec("bad", swagger2, EDITOR_KEY, "acme").await;
    assert_eq!(
        seen.status, 400,
        "the compiler's own refusal is surfaced: {seen}"
    );
    assert!(
        seen.body.contains("3.0"),
        "names the supported version: {seen}"
    );

    let seen = api
        .put_spec("bad", &[0xff, 0xfe, b'{', b'}'], EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 400, "a spec is text: {seen}");

    let seen = api
        .put_spec("bad id", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert!(
        seen.status == 400 || seen.status == 404,
        "an unusable id never reaches the log: {seen}"
    );

    let mut huge = PETSTORE.as_bytes().to_vec();
    huge.extend(std::iter::repeat_n(b'#', 4 * 1024 * 1024));
    let seen = api.put_spec("huge", &huge, EDITOR_KEY, "acme").await;
    assert_eq!(seen.status, 413, "over the 4 MiB cap: {seen}");
    assert!(seen.body.contains("4194304"), "names the cap: {seen}");

    assert_eq!(
        last_applied(&fixture.server),
        before,
        "nothing refused here may have written a log entry"
    );
    let seen = api.get("/specs", EDITOR_KEY, "acme").await;
    assert_eq!(seen.json()["specs"], json!([]));

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn deploy_serves_the_compiled_imposter_on_every_node_after_the_2xx() {
    let leader_state = TempDir::new().expect("tempdir");
    let leader = compose::start(cluster_cli(&leader_state, &[]))
        .await
        .expect("leader starts");
    wait_ready(&leader).await;
    seed_all(leader.node().expect("clustered")).await;
    let seed_addr = leader.cluster_addr().expect("cluster addr").to_string();

    let follower_state = TempDir::new().expect("tempdir");
    let follower = compose::start(cluster_on(
        &follower_state,
        "127.0.0.1:0",
        &["--cluster-seeds", &seed_addr],
    ))
    .await
    .expect("follower joins");
    wait_ready(&follower).await;

    let on_leader = Api::new(&leader.admin_addr().to_string());
    let on_follower = Api::new(&follower.admin_addr().to_string());

    let seen = on_leader
        .put_spec("petstore", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 201, "{seen}");
    let digest = seen.json()["digest"].as_str().expect("digest").to_owned();

    // Import on the leader, deploy on the follower: the spec bytes are fleet
    // state, so the follower compiles the same document.
    let port = reserve_port();
    let seen = on_follower
        .post_json(
            "/specs/petstore/deploy",
            &json!({ "port": port }),
            EDITOR_KEY,
            "acme",
        )
        .await;
    assert_eq!(
        seen.status, 201,
        "first deploy creates the imposter: {seen}"
    );
    let revision = seen
        .header("rift-cluster-revision")
        .expect("deploy is an ordinary terminated write")
        .to_owned();
    assert!(
        revision.starts_with(&format!("default:{port}@")),
        "the revision token names the imposter port: {revision}"
    );
    assert!(seen.header("rift-cluster-op-id").is_some());
    assert!(
        seen.header("rift-spec-warnings").is_none(),
        "the compiler's own output never warns against its own spec: {seen}"
    );
    let deployed = seen.json();
    assert_eq!(deployed["port"], port);
    let ids: Vec<&str> = deployed["stubs"]
        .as_array()
        .expect("stubs")
        .iter()
        .filter_map(|s| s["id"].as_str())
        .collect();
    assert!(
        ids.contains(&"spec:listPets:200"),
        "compiled stubs: {ids:?}"
    );

    // After the 2xx, EVERY node serves it — the write barrier is the promise.
    for (label, api) in [("leader", &on_leader), ("follower", &on_follower)] {
        let seen = api
            .get(&format!("/imposters/{port}"), EDITOR_KEY, "acme")
            .await;
        assert_eq!(seen.status, 200, "{label} imposter read: {seen}");
        assert!(
            seen.body.contains("spec:showPetById:200"),
            "{label}: {seen}"
        );

        let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
        assert_eq!(seen.status, 200, "{label} spec read: {seen}");
        let record = seen.json();
        assert_eq!(record["ports"], json!([port]), "{label}: bound");
        assert_eq!(record["drifted"], false, "{label}");
        assert_eq!(record["digest"], digest, "{label}");

        let seen = api.get("/specs", EDITOR_KEY, "acme").await;
        assert_eq!(
            seen.json()["specs"][0]["ports"],
            json!([port]),
            "{label} list"
        );
    }
    let (status, body) = wait_served(port, "/pets/1")
        .await
        .expect("the compiled mock answers on its data port");
    assert_eq!(status, 200, "{body}");
    let pet: Value = serde_json::from_str(&body).expect("a JSON pet");
    assert!(pet["id"].is_u64(), "schema-shaped body: {pet}");
    assert!(pet["name"].is_string(), "schema-shaped body: {pet}");

    // A second deploy replaces (200, not 201) and stays bound.
    //
    // Through `when_writable`, like every write below it: the follower joined a
    // few lines up, so the membership change may still be in flight and the
    // leader may refuse a write for a moment. What is being asserted is that a
    // redeploy replaces rather than creates — not how quickly a two-node fleet
    // settles after a join, which is D-25's business and is covered elsewhere.
    // Bound outside the closure: `post_json` borrows the body, and a `json!`
    // temporary built inside would not outlive the future the closure returns.
    let redeploy = json!({ "port": port });
    let seen = when_writable(|| {
        on_leader.post_json("/specs/petstore/deploy", &redeploy, EDITOR_KEY, "acme")
    })
    .await;
    assert_eq!(seen.status, 200, "redeploy replaces: {seen}");

    // Delete refuses while bound; `?force` unbinds first and the imposter stays.
    let seen = when_writable(|| on_leader.delete("/specs/petstore", EDITOR_KEY, "acme")).await;
    assert_eq!(
        seen.status, 409,
        "bound specs are not deleted by accident: {seen}"
    );
    assert_eq!(seen.json()["errors"][0]["type"], "resource conflict");
    assert!(
        seen.body.contains(&port.to_string()),
        "names the port: {seen}"
    );
    assert!(seen.body.contains("force"), "says how: {seen}");
    let seen =
        when_writable(|| on_leader.delete("/specs/petstore?force", EDITOR_KEY, "acme")).await;
    assert_eq!(seen.status, 200, "{seen}");
    assert_eq!(seen.json()["unboundPorts"], json!([port]));
    for (label, api) in [("leader", &on_leader), ("follower", &on_follower)] {
        let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
        assert_eq!(seen.status, 404, "{label}: gone after force delete: {seen}");
        let seen = api
            .get(&format!("/imposters/{port}"), EDITOR_KEY, "acme")
            .await;
        assert_eq!(
            seen.status, 200,
            "{label}: the imposter is never torn down: {seen}"
        );
    }

    follower.shutdown().await;
    leader.shutdown().await;
}

#[tokio::test]
async fn a_hand_edit_of_a_bound_imposter_marks_drift_and_warns_on_schema_violations() {
    let fixture = start().await;
    let api = Api::new(&fixture.admin);
    let seen = api
        .put_spec("petstore", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 201, "{seen}");
    let port = reserve_port();
    let seen = api
        .post_json(
            "/specs/petstore/deploy",
            &json!({ "port": port }),
            EDITOR_KEY,
            "acme",
        )
        .await;
    assert_eq!(seen.status, 201, "{seen}");

    // A stub replacement whose static body contradicts the Pet schema
    // (`id` must be an integer): admitted — a divergent fixture is legitimate
    // — but named in `Rift-Spec-Warnings`, and the port is now drifted.
    let violating = json!({
        "predicates": [{ "equals": { "method": "GET", "path": "/pets/1" } }],
        "responses": [{ "is": {
            "statusCode": 200,
            "headers": { "Content-Type": "application/json" },
            "body": { "id": "not-a-number", "name": "rex" }
        }}]
    });
    let seen = Seen::of(
        api.request(
            reqwest::Method::PUT,
            &format!("/imposters/{port}/stubs/by-id/spec:showPetById:200"),
            EDITOR_KEY,
            "acme",
        )
        .json(&violating)
        .send()
        .await
        .expect("sends"),
    )
    .await;
    assert!(
        (200..300).contains(&seen.status),
        "warn, never refuse: {seen}"
    );
    let warnings = seen
        .header("rift-spec-warnings")
        .expect("a violating static body is reported in the header");
    assert!(
        warnings.contains("spec:showPetById:200"),
        "names the stub: {warnings}"
    );
    assert!(warnings.contains("/id"), "points at the field: {warnings}");

    let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
    assert_eq!(seen.json()["drifted"], true, "a hand edit is drift: {seen}");
    assert_eq!(
        seen.json()["ports"],
        json!([port]),
        "and the port stays bound"
    );

    // A schema-conforming hand edit is still drift, but nothing to warn about.
    let conforming = json!({
        "predicates": [{ "equals": { "method": "GET", "path": "/pets/1" } }],
        "responses": [{ "is": {
            "statusCode": 200,
            "headers": { "Content-Type": "application/json" },
            "body": { "id": 7, "name": "rex" }
        }}]
    });
    let seen = Seen::of(
        api.request(
            reqwest::Method::PUT,
            &format!("/imposters/{port}/stubs/by-id/spec:showPetById:200"),
            EDITOR_KEY,
            "acme",
        )
        .json(&conforming)
        .send()
        .await
        .expect("sends"),
    )
    .await;
    assert!((200..300).contains(&seen.status), "{seen}");
    assert!(
        seen.header("rift-spec-warnings").is_none(),
        "a conforming body has nothing to warn about: {seen}"
    );

    // Redeploying resets the drift baseline.
    let seen = api
        .post_json(
            "/specs/petstore/deploy",
            &json!({ "port": port }),
            EDITOR_KEY,
            "acme",
        )
        .await;
    assert_eq!(seen.status, 200, "{seen}");
    let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
    assert_eq!(
        seen.json()["drifted"],
        false,
        "a deploy is the baseline: {seen}"
    );

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn compile_is_a_dry_run_that_commits_nothing() {
    let fixture = start().await;
    let api = Api::new(&fixture.admin);
    let seen = api
        .put_spec("petstore", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 201, "{seen}");
    let before = last_applied(&fixture.server);
    let port = reserve_port();

    let seen = api
        .post_json(
            "/specs/petstore/compile",
            &json!({ "port": port }),
            EDITOR_KEY,
            "acme",
        )
        .await;
    assert_eq!(seen.status, 200, "{seen}");
    let body = seen.json();
    assert_eq!(
        body["imposter"]["port"], port,
        "compiled for the requested port"
    );
    let operations = body["operations"].as_array().expect("operations");
    let list_pets = operations
        .iter()
        .find(|op| op["id"] == "listPets")
        .expect("listPets is an operation");
    assert_eq!(list_pets["method"], "GET");
    assert_eq!(list_pets["pathTemplate"], "/pets");
    assert!(
        list_pets["stubIds"]
            .as_array()
            .expect("stubIds")
            .iter()
            .any(|id| id == "spec:listPets:200"),
        "{list_pets}"
    );
    assert_eq!(
        body["diff"]["deployed"], false,
        "nothing on that port yet: {body}"
    );
    assert_eq!(
        last_applied(&fixture.server),
        before,
        "a dry run commits nothing"
    );
    let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
    assert_eq!(seen.json()["ports"], json!([]), "and binds nothing");

    // With no port in the body and nothing bound, there is nothing to diff against.
    let seen = api
        .post_json("/specs/petstore/compile", &json!({}), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 200, "{seen}");
    assert!(seen.json()["diff"].is_null(), "{seen}");

    // Once deployed, the diff is against the deployed config: identical right
    // after a deploy, and a hand edit shows up under `changed`.
    let seen = api
        .post_json(
            "/specs/petstore/deploy",
            &json!({ "port": port }),
            EDITOR_KEY,
            "acme",
        )
        .await;
    assert_eq!(seen.status, 201, "{seen}");
    let seen = api
        .post_json("/specs/petstore/compile", &json!({}), EDITOR_KEY, "acme")
        .await;
    let diff = &seen.json()["diff"];
    assert_eq!(
        diff["port"], port,
        "the single bound port is the implied target: {diff}"
    );
    assert_eq!(diff["deployed"], true);
    assert_eq!(diff["added"], json!([]));
    assert_eq!(diff["removed"], json!([]));
    assert_eq!(diff["changed"], json!([]));

    let seen = Seen::of(
        api.request(
            reqwest::Method::PUT,
            &format!("/imposters/{port}/stubs/by-id/spec:showPetById:200"),
            EDITOR_KEY,
            "acme",
        )
        .json(&json!({
            "predicates": [{ "equals": { "method": "GET", "path": "/pets/1" } }],
            "responses": [{ "is": { "statusCode": 200, "body": { "id": 7, "name": "rex" } } }]
        }))
        .send()
        .await
        .expect("sends"),
    )
    .await;
    assert!((200..300).contains(&seen.status), "{seen}");
    let seen = api
        .post_json("/specs/petstore/compile", &json!({}), EDITOR_KEY, "acme")
        .await;
    let diff = &seen.json()["diff"];
    assert_eq!(diff["changed"], json!(["spec:showPetById:200"]), "{diff}");
    assert_eq!(diff["added"], json!([]));
    assert_eq!(diff["removed"], json!([]));

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn deploy_refuses_a_bad_body_and_honours_if_match_on_the_imposter() {
    let fixture = start().await;
    let api = Api::new(&fixture.admin);
    let seen = api
        .put_spec("petstore", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 201, "{seen}");
    let port = reserve_port();

    let seen = api
        .post_json("/specs/petstore/deploy", &json!({}), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 400, "a deploy needs a port: {seen}");
    let seen = api
        .post_json(
            "/specs/petstore/deploy",
            &json!({ "port": port, "policy": "skip" }),
            EDITOR_KEY,
            "acme",
        )
        .await;
    assert_eq!(
        seen.status, 400,
        "drift policy is S3, not silently ignored: {seen}"
    );
    assert!(
        seen.body.contains("279"),
        "points at the slice that ships it: {seen}"
    );
    let seen = api
        .post_json(
            "/specs/ghost/deploy",
            &json!({ "port": port }),
            EDITOR_KEY,
            "acme",
        )
        .await;
    assert_eq!(seen.status, 404, "{seen}");

    let seen = api
        .post_json(
            "/specs/petstore/deploy",
            &json!({ "port": port }),
            EDITOR_KEY,
            "acme",
        )
        .await;
    assert_eq!(seen.status, 201, "{seen}");
    let current = seen
        .header("rift-cluster-revision")
        .expect("revision")
        .to_owned();

    // A stale precondition refuses; the current one lets the redeploy through.
    let stale = format!("default:{port}@1");
    let seen = Seen::of(
        api.request(
            reqwest::Method::POST,
            "/specs/petstore/deploy",
            EDITOR_KEY,
            "acme",
        )
        .header("if-match", &stale)
        .json(&json!({ "port": port }))
        .send()
        .await
        .expect("sends"),
    )
    .await;
    assert_eq!(seen.status, 409, "stale If-Match: {seen}");
    let seen = Seen::of(
        api.request(
            reqwest::Method::POST,
            "/specs/petstore/deploy",
            EDITOR_KEY,
            "acme",
        )
        .header("if-match", &current)
        .json(&json!({ "port": port }))
        .send()
        .await
        .expect("sends"),
    )
    .await;
    assert_eq!(seen.status, 200, "current If-Match: {seen}");
    assert_ne!(
        seen.header("rift-cluster-revision"),
        Some(current.as_str()),
        "the redeploy moved the revision"
    );

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn deploy_replaces_a_hand_written_imposter_and_stamps_its_provenance() {
    let fixture = start().await;
    let api = Api::new(&fixture.admin);
    let seen = api
        .put_spec("petstore", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 201, "{seen}");

    // A plain, hand-written imposter on the port first — nothing to do with any spec.
    let port = reserve_port();
    let seen = api
        .post_json(
            "/imposters",
            &json!({
                "port": port,
                "protocol": "http",
                "stubs": [{ "id": "hand", "responses": [{ "is": { "statusCode": 418, "body": "teapot" } }] }],
            }),
            EDITOR_KEY,
            "acme",
        )
        .await;
    assert_eq!(seen.status, 201, "{seen}");
    let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
    assert_eq!(
        seen.json()["ports"],
        json!([]),
        "a hand-written imposter binds no spec"
    );

    // Deploying over it replaces it (200, not 201) and the port is now spec-bound and clean.
    let seen = api
        .post_json(
            "/specs/petstore/deploy",
            &json!({ "port": port }),
            EDITOR_KEY,
            "acme",
        )
        .await;
    assert_eq!(
        seen.status, 200,
        "replacing an existing imposter is a 200: {seen}"
    );
    let deployed = seen.json();
    let ids: Vec<&str> = deployed["stubs"]
        .as_array()
        .expect("stubs")
        .iter()
        .filter_map(|s| s["id"].as_str())
        .collect();
    assert!(ids.contains(&"spec:showPetById:200"), "{ids:?}");
    assert!(
        !ids.contains(&"hand"),
        "the hand-written stub was replaced, not merged: {ids:?}"
    );
    let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
    assert_eq!(seen.json()["ports"], json!([port]));
    assert_eq!(seen.json()["drifted"], false);

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn force_delete_unbinds_every_port_a_spec_is_deployed_to() {
    let fixture = start().await;
    let api = Api::new(&fixture.admin);
    let seen = api
        .put_spec("petstore", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 201, "{seen}");
    let [port_a, port_b] = {
        let mut ports = [reserve_port(), reserve_port()];
        ports.sort_unstable();
        ports
    };
    for port in [port_a, port_b] {
        let seen = api
            .post_json(
                "/specs/petstore/deploy",
                &json!({ "port": port }),
                EDITOR_KEY,
                "acme",
            )
            .await;
        assert_eq!(seen.status, 201, "{seen}");
    }
    let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
    assert_eq!(
        seen.json()["ports"],
        json!([port_a, port_b]),
        "both bound, ascending"
    );

    // `?force=false` is a declined force, not a spelled-out one.
    let seen = api
        .delete("/specs/petstore?force=false", EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 409, "force=false must not force: {seen}");
    assert!(
        seen.body.contains(&port_a.to_string()) && seen.body.contains(&port_b.to_string()),
        "{seen}"
    );

    let seen = api
        .delete("/specs/petstore?force=true", EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 200, "{seen}");
    assert_eq!(seen.json()["unboundPorts"], json!([port_a, port_b]));
    for port in [port_a, port_b] {
        let seen = api
            .get(&format!("/imposters/{port}"), EDITOR_KEY, "acme")
            .await;
        assert_eq!(
            seen.status, 200,
            "unbind never tears the imposter down: {seen}"
        );
    }
    let seen = api.get("/specs/petstore", EDITOR_KEY, "acme").await;
    assert_eq!(seen.status, 404, "{seen}");
    // Re-importing after the delete starts clean: nothing is bound.
    let seen = api
        .put_spec("petstore", PETSTORE.as_bytes(), EDITOR_KEY, "acme")
        .await;
    assert_eq!(seen.status, 201, "{seen}");
    assert_eq!(
        api.get("/specs/petstore", EDITOR_KEY, "acme").await.json()["ports"],
        json!([]),
        "the force-delete cleared the ports' provenance"
    );

    fixture.server.shutdown().await;
}
