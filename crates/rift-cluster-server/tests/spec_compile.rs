//! `POST /specs/compile` — the one-shot OpenAPI import (D-72, #549).
//!
//! The endpoint replaced a whole stored-spec subsystem, so what these pin is not "it compiles"
//! but the three properties the replacement rests on: the compiled JSON is *deployable through
//! the ordinary write path*, the request is bounded, and **nothing is retained**.
//!
//! Driven through `compose::start` rather than the classifier, because the claim that matters is
//! end-to-end: a caller compiles, `PUT /imposters` the result, and the stub answers.

use std::time::Duration;

use clap::Parser;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose;
use tempfile::TempDir;

mod common;

const SECRET: &str = "spec-compile-test-secret";

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
        "--cluster-allow-solo".to_owned(),
        "--cluster-state-dir".to_owned(),
        state.path().to_string_lossy().into_owned(),
    ])
    .expect("parses")
}

async fn wait_ready(server: &compose::ComposedServer) {
    let probes = server.probe_addr().expect("probes bound under --cluster");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = reqwest::get(format!("http://{probes}/readyz")).await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the node never became ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A minimal OpenAPI 3.0 document with one operation that returns a JSON object.
const PETSTORE_YAML: &str = r#"openapi: 3.0.0
info:
  title: pets
  version: 1.0.0
paths:
  /pets:
    get:
      operationId: listPets
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                type: object
                properties:
                  name:
                    type: string
"#;

/// The whole point of the endpoint, end to end: compile, `PUT /imposters` what came back, and
/// the stub answers on the port the caller asked for.
///
/// Pins D-72's central claim — that a compiled spec reaches the fleet through the **one**
/// `PutImposter` path every other config takes — which is only meaningful if the compiler's
/// output is accepted verbatim by that path. A response that needed massaging before it could be
/// `PUT` would mean the two halves had drifted, and this is the only test that would notice.
#[tokio::test]
async fn a_compiled_spec_deploys_through_put_imposters_and_serves() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = common::ports::reserve_port();
    let client = reqwest::Client::new();

    let compiled: serde_json::Value = client
        .post(format!(
            "http://{admin}/specs/compile?port={port}&name=pets"
        ))
        .header("content-type", "application/yaml")
        .body(PETSTORE_YAML)
        .send()
        .await
        .expect("compile")
        .json()
        .await
        .expect("the compile answers JSON");

    let imposter = compiled
        .get("imposter")
        .expect("the response carries the compiled imposter");
    assert_eq!(
        imposter["port"], port,
        "the compiled imposter must carry the port the caller asked for: {imposter}"
    );
    assert_eq!(imposter["name"], "pets");
    let operations = compiled["operations"]
        .as_array()
        .expect("the response carries the operation index");
    assert_eq!(operations.len(), 1, "{operations:?}");
    assert_eq!(operations[0]["id"], "listPets");
    assert_eq!(operations[0]["method"], "GET");
    assert_eq!(operations[0]["pathTemplate"], "/pets");
    assert!(
        !operations[0]["stubIds"]
            .as_array()
            .expect("stubIds")
            .is_empty(),
        "an operation names the stubs that serve it: {operations:?}"
    );

    // Verbatim: the body is exactly what the compile answered, with nothing added.
    let deployed = client
        .put(format!("http://{admin}/imposters"))
        .json(&serde_json::json!({ "imposters": [imposter] }))
        .send()
        .await
        .expect("deploy the compiled imposter");
    assert!(
        deployed.status().is_success(),
        "the compiled imposter must be accepted by the ordinary write path: {}",
        deployed.status()
    );

    let answered = client
        .get(format!("http://127.0.0.1:{port}/pets"))
        .send()
        .await
        .expect("the deployed imposter answers");
    assert_eq!(answered.status().as_u16(), 200);

    server.shutdown().await;
}

/// Pins D-72's "stores nothing": after a successful compile there is no spec to read back, and
/// the retired store's routes are gone rather than empty.
///
/// `404` on `GET /specs`, not `200 {"specs":[]}` — an empty list would mean the store is still
/// there and merely unpopulated, which is precisely the state this change removed. And the
/// compile must not have written config either: a compile is not a deploy.
#[tokio::test]
async fn a_compile_retains_nothing_and_the_spec_store_is_gone() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = common::ports::reserve_port();
    let client = reqwest::Client::new();

    let before = server
        .node()
        .expect("clustered")
        .status()
        .last_applied
        .expect("something applied at bootstrap");

    let response = client
        .post(format!("http://{admin}/specs/compile?port={port}"))
        .header("content-type", "application/yaml")
        .body(PETSTORE_YAML)
        .send()
        .await
        .expect("compile");
    assert_eq!(response.status().as_u16(), 200);

    assert_eq!(
        server
            .node()
            .expect("clustered")
            .configured_ports()
            .expect("read configs"),
        Vec::<u16>::new(),
        "a compile must not write config — only the PUT that follows it does"
    );
    assert_eq!(
        server
            .node()
            .expect("clustered")
            .status()
            .last_applied
            .expect("applied"),
        before,
        "a compile must mint no control op at all"
    );

    for path in ["/specs", "/specs/pets"] {
        let gone = client
            .get(format!("http://{admin}{path}"))
            .send()
            .await
            .expect("read the retired spec store");
        assert_eq!(
            gone.status().as_u16(),
            404,
            "{path} must be gone, not an empty collection"
        );
    }

    server.shutdown().await;
}

/// The body is bounded before it is parsed: one byte over `MAX_SPEC_BYTES` is `413`, not a
/// four-megabyte parse the node pays for and then refuses.
#[tokio::test]
async fn a_body_over_the_cap_is_refused_with_413() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = common::ports::reserve_port();

    let oversize = "x".repeat(rift_cluster_spec::MAX_SPEC_BYTES + 1);
    let response = reqwest::Client::new()
        .post(format!("http://{admin}/specs/compile?port={port}"))
        .header("content-type", "application/yaml")
        .body(oversize)
        .send()
        .await
        .expect("post an oversize body");
    assert_eq!(response.status().as_u16(), 413);

    server.shutdown().await;
}

/// The cap is inclusive: a body of exactly `MAX_SPEC_BYTES` compiles. Beside the `+1 → 413` case
/// so the boundary is pinned from both sides — an off-by-one in the limit reads as either test
/// alone passing.
///
/// Padded with a YAML comment rather than a bigger document, so the thing being measured is the
/// byte bound and not the compiler's appetite for operations.
#[tokio::test]
async fn a_body_of_exactly_the_cap_compiles() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = common::ports::reserve_port();

    let padding = rift_cluster_spec::MAX_SPEC_BYTES - PETSTORE_YAML.len() - "# \n".len();
    let at_cap = format!("{PETSTORE_YAML}# {}\n", "x".repeat(padding));
    assert_eq!(at_cap.len(), rift_cluster_spec::MAX_SPEC_BYTES);

    let response = reqwest::Client::new()
        .post(format!("http://{admin}/specs/compile?port={port}"))
        .header("content-type", "application/yaml")
        .body(at_cap)
        .send()
        .await
        .expect("post a body of exactly the cap");
    assert_eq!(
        response.status().as_u16(),
        200,
        "exactly MAX_SPEC_BYTES is within the cap, not over it"
    );
    let compiled: serde_json::Value = response.json().await.expect("the compile answers JSON");
    assert_eq!(
        compiled["imposter"]["port"], port,
        "the padded document must compile to the same imposter: {compiled}"
    );

    server.shutdown().await;
}

/// JSON and YAML are both accepted — the compiler parses one superset — and both spellings of
/// one document produce the same *contract*: the same stub ids, predicates, statuses and headers,
/// and the same operation index.
///
/// **What they do not produce is the same bytes, and that is deliberate.** The compiler seeds its
/// synthesized example values from `SpecDigest::of(spec_bytes)` — the digest of the document as
/// written — so re-spelling a document in the other format changes the placeholder strings in the
/// stub bodies while changing nothing about what the mock matches or answers with. Asserting
/// whole-response equality here would fail for that reason alone and read as a bug in the
/// endpoint, so this pins the boundary explicitly: everything except the synthesized body agrees.
#[tokio::test]
async fn json_and_yaml_spellings_of_one_document_compile_to_the_same_contract() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = common::ports::reserve_port();
    let client = reqwest::Client::new();

    let as_json = serde_yaml::from_str::<serde_json::Value>(PETSTORE_YAML)
        .expect("the fixture parses")
        .to_string();

    let compile = |body: String, content_type: &'static str| {
        client
            .post(format!("http://{admin}/specs/compile?port={port}"))
            .header("content-type", content_type)
            .body(body)
            .send()
    };

    let mut from_yaml: serde_json::Value = compile(PETSTORE_YAML.to_owned(), "application/yaml")
        .await
        .expect("compile yaml")
        .json()
        .await
        .expect("json body");
    let mut from_json: serde_json::Value = compile(as_json, "application/json")
        .await
        .expect("compile json")
        .json()
        .await
        .expect("json body");

    // The operation index is a pure function of the document's meaning, so it must match whole.
    assert_eq!(from_yaml["operations"], from_json["operations"]);

    // Both must actually have produced a synthesized body, or blanking it below would make the
    // comparison vacuous — which is the way this test could rot into passing on nothing.
    let body_of = |v: &mut serde_json::Value| -> serde_json::Value {
        let slot = &mut v["imposter"]["stubs"][0]["responses"][0]["is"]["body"];
        assert!(!slot.is_null(), "the compiled stub must carry a body: {v}");
        std::mem::replace(slot, serde_json::Value::Null)
    };
    let yaml_body = body_of(&mut from_yaml);
    let json_body = body_of(&mut from_json);
    assert_ne!(
        yaml_body, json_body,
        "the synthesized example is seeded from the document's bytes, so the two spellings must \
         differ here — if they stopped differing, the seed is no longer the digest and this \
         test's whole premise needs re-reading"
    );
    assert_eq!(
        yaml_body["name"].as_str().map(|s| s.starts_with("string-")),
        Some(true),
        "both bodies are the same shape, only differently seeded: {yaml_body}"
    );

    assert_eq!(
        from_yaml, from_json,
        "with the seeded example set aside, the two spellings must compile identically"
    );

    server.shutdown().await;
}

/// `port` is required, and its absence is a `400` — the compile has no stored record to infer a
/// port from, and a portless imposter cannot be `PUT` under `--cluster` at all.
///
/// A `404` here would be the wrong answer and the tempting one: the route exists, so the caller
/// must be told what is missing rather than sent to check the path they typed correctly.
#[tokio::test]
async fn a_compile_without_a_port_is_a_400_naming_the_missing_parameter() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let client = reqwest::Client::new();

    for query in ["", "?name=pets", "?port=0", "?port=not-a-number"] {
        let response = client
            .post(format!("http://{admin}/specs/compile{query}"))
            .header("content-type", "application/yaml")
            .body(PETSTORE_YAML)
            .send()
            .await
            .expect("compile without a usable port");
        assert_eq!(
            response.status().as_u16(),
            400,
            "query {query:?} must be a 400"
        );
        let body = response.text().await.expect("body");
        assert!(
            body.contains("port"),
            "the refusal must name the parameter: {body}"
        );
    }

    server.shutdown().await;
}

/// A document the compiler refuses is a `400` on this route, not a `500`: an unsupported OpenAPI
/// version is the caller's input being wrong, not the server's.
#[tokio::test]
async fn a_document_that_does_not_compile_is_a_400() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let port = common::ports::reserve_port();

    let response = reqwest::Client::new()
        .post(format!("http://{admin}/specs/compile?port={port}"))
        .header("content-type", "application/yaml")
        .body("openapi: 2.0\ninfo:\n  title: old\n  version: 1.0.0\npaths: {}\n")
        .send()
        .await
        .expect("compile an unsupported version");
    assert_eq!(response.status().as_u16(), 400);

    server.shutdown().await;
}
