//! `/admin/sources` (issue #134): the source control-plane surface on the
//! authenticated cluster port, end to end over real HTTP.
//!
//! The state machine's semantics are covered in `rift-cluster`; what is proven
//! here is the surface an operator actually touches — that a source round-trips
//! through create/list/get/pull/delete, that the pull's effects are visible in
//! `/_cluster/config` provenance, and that the refusals an operator can trigger
//! come back as refusals rather than as 500s.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rift_cluster::rpc::{AlwaysHealthy, RpcClient, RpcClientConfig, RpcError, Signer};
use rift_cluster::{NodeConfig, RaftNode, SourcePuller, sources};
use rift_ee::seams::{
    FetchedImposters, ImposterConfig, ImposterSource, SourceMeta, SourceRef, SourceRegistry,
};
use rift_ee_server::cluster_api;
use rift_ee_server::readiness::{GATE_JOINED, Readiness};
use tempfile::TempDir;

const SECRET: &str = "sources-test-secret";

/// A source whose content the test controls, counting fetches so "fetch once"
/// is an assertion rather than a claim.
struct ScriptedSource {
    fetches: Arc<AtomicUsize>,
    body: std::sync::Mutex<Vec<ImposterConfig>>,
    version: std::sync::Mutex<String>,
    routes_block: std::sync::Mutex<bool>,
    intercept_block: std::sync::Mutex<bool>,
}

impl ImposterSource for ScriptedSource {
    fn schemes(&self) -> &'static [&'static str] {
        &["scripted"]
    }

    fn fetch<'a>(
        &'a self,
        _r: &'a SourceRef,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<FetchedImposters>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            let routes = if *self.routes_block.lock().expect("routes lock") {
                Some(rift_ee::seams::RouteTable::default())
            } else {
                None
            };
            let intercept = if *self.intercept_block.lock().expect("intercept lock") {
                Some(rift_ee::rift_http_proxy::intercept_control::InterceptStartOptions::default())
            } else {
                None
            };
            Ok(FetchedImposters {
                configs: self.body.lock().expect("body lock").clone(),
                intercept,
                routes,
                meta: SourceMeta {
                    version: Some(self.version.lock().expect("version lock").clone()),
                    fetched_at: std::time::SystemTime::now(),
                },
                unchanged: false,
            })
        })
    }
}

struct Fixture {
    node: Arc<RaftNode>,
    addr: SocketAddr,
    source: Arc<ScriptedSource>,
    fetches: Arc<AtomicUsize>,
    _dir: TempDir,
}

fn imposter(port: u16, name: &str) -> ImposterConfig {
    serde_json::from_value(serde_json::json!({
        "port": port,
        "protocol": "http",
        "name": name,
    }))
    .expect("test config parses")
}

async fn start() -> Fixture {
    let dir = TempDir::new().expect("tempdir");
    let readiness = Arc::new(Readiness::awaiting([GATE_JOINED]));
    let slot = cluster_api::NodeSlot::default();

    let fetches = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(ScriptedSource {
        fetches: Arc::clone(&fetches),
        body: std::sync::Mutex::new(vec![imposter(9301, "v1")]),
        version: std::sync::Mutex::new("v1".to_owned()),
        routes_block: std::sync::Mutex::new(false),
        intercept_block: std::sync::Mutex::new(false),
    });
    let mut registry = SourceRegistry::new();
    registry
        .register(Arc::clone(&source) as Arc<dyn ImposterSource>)
        .expect("register the scripted source");
    let puller = Arc::new(SourcePuller::new(registry));

    let config = NodeConfig {
        node_id: 1,
        bind: "127.0.0.1:0".parse().expect("bind addr"),
        advertise: None,
        data_dir: dir.path().to_path_buf(),
        secret: Some(SECRET.to_owned()),
        routes: sources::routes(
            cluster_api::routes(rift_cluster::Router::new(), slot.clone(), readiness.clone()),
            Arc::clone(&puller),
        ),
        // Tables-only: this suite is about the control surface, not about
        // whether a port binds locally.
        engine: None,
    };
    let node = Arc::new(RaftNode::start(config).await.expect("node starts"));
    slot.set(&node).expect("the slot is bound exactly once");
    puller.bind(&node).expect("the puller binds exactly once");
    node.cluster_init().await.expect("bootstrap");
    readiness.satisfy(GATE_JOINED);
    let addr: SocketAddr = node
        .advertise()
        .as_str()
        .parse()
        .expect("advertise is a literal address in tests");
    Fixture {
        node,
        addr,
        source,
        fetches,
        _dir: dir,
    }
}

fn client() -> RpcClient {
    RpcClient::new(
        Some(Signer::new(SECRET)),
        Arc::new(AlwaysHealthy),
        RpcClientConfig::default(),
    )
}

async fn call(
    client: &RpcClient,
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let payload = if body.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(&body).expect("encode")
    };
    let raw = client
        .call(addr, method, path, payload)
        .await
        .unwrap_or_else(|e| panic!("{method} {path}: {e}"));
    serde_json::from_slice(&raw).expect("json body")
}

async fn call_err(
    client: &RpcClient,
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: serde_json::Value,
) -> RpcError {
    let payload = if body.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(&body).expect("encode")
    };
    client
        .call(addr, method, path, payload)
        .await
        .expect_err("expected a refusal")
}

#[tokio::test]
async fn a_source_round_trips_through_create_list_get_pull_and_delete() {
    let fixture = start().await;
    let client = client();

    let listed = call(
        &client,
        fixture.addr,
        "GET",
        "/admin/sources",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(listed["sources"], serde_json::json!([]));

    let created = call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "mocks", "uri": "scripted://cfg/i.json", "onDrift": "overwrite" }),
    )
    .await;
    assert_eq!(created["id"], "mocks");
    assert_eq!(created["uri"], "scripted://cfg/i.json");
    assert_eq!(created["mode"], "pinned", "pinned is the default mode");
    assert_eq!(created["onDrift"], "overwrite");
    assert_eq!(created["drifted"], false);
    assert_eq!(created["ports"], serde_json::json!([]));
    assert_eq!(
        fixture.fetches.load(Ordering::SeqCst),
        0,
        "declaring a source must not fetch it: a pull is an explicit act in pinned mode"
    );

    let listed = call(
        &client,
        fixture.addr,
        "GET",
        "/admin/sources",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(listed["sources"].as_array().expect("array").len(), 1);

    let fetched = call(
        &client,
        fixture.addr,
        "GET",
        "/admin/sources/mocks",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(fetched["id"], "mocks");

    let pulled = call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(pulled["unchanged"], false);
    assert_eq!(pulled["version"], "v1");
    assert_eq!(pulled["changed"], serde_json::json!([9301]));
    assert_eq!(fixture.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.node.configured_ports().expect("ports"),
        vec![9301],
        "the pull's configs are committed, not just reported"
    );

    // Provenance is visible where an operator compares nodes for convergence.
    let config = call(
        &client,
        fixture.addr,
        "GET",
        "/_cluster/config",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        config["provenance"],
        serde_json::json!([{ "port": 9301, "sourceId": "mocks", "version": "v1" }])
    );

    let after = call(
        &client,
        fixture.addr,
        "GET",
        "/admin/sources/mocks",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(after["lastVersion"], "v1");
    assert_eq!(after["lastOutcome"], "applied");
    assert_eq!(after["ports"], serde_json::json!([9301]));

    call(
        &client,
        fixture.addr,
        "DELETE",
        "/admin/sources/mocks",
        serde_json::Value::Null,
    )
    .await;
    let listed = call(
        &client,
        fixture.addr,
        "GET",
        "/admin/sources",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(listed["sources"], serde_json::json!([]));
    assert_eq!(
        fixture.node.configured_ports().expect("ports"),
        vec![9301],
        "deleting a source stops tracking the uri; it does not tear down live imposters"
    );
}

/// The no-change fast path, over the wire: a second pull of identical content
/// reports `unchanged` and moves nothing.
#[tokio::test]
async fn an_unchanged_pull_reports_unchanged_and_writes_no_log_entry() {
    let fixture = start().await;
    let client = client();
    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "mocks", "uri": "scripted://cfg/i.json" }),
    )
    .await;
    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;
    let before = fixture.node.status().last_applied.expect("applied index");

    let again = call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(again["unchanged"], true);
    assert_eq!(again["changed"], serde_json::json!([]));
    assert_eq!(
        fixture.node.status().last_applied.expect("applied index"),
        before,
        "identical content must produce no log entry"
    );

    // A real change moves it again.
    *fixture.source.body.lock().expect("body lock") = vec![imposter(9301, "v2")];
    *fixture.source.version.lock().expect("version lock") = "v2".to_owned();
    let changed = call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(changed["unchanged"], false);
    assert!(
        fixture.node.status().last_applied.expect("applied index") > before,
        "changed content must commit"
    );
}

/// Drift, end to end: a hand edit of a source-owned port shows up as `drifted`
/// on the source, and the next pull resolves it.
#[tokio::test]
async fn a_hand_edit_shows_as_drift_and_the_next_pull_resolves_it() {
    let fixture = start().await;
    let client = client();
    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "mocks", "uri": "scripted://cfg/i.json" }),
    )
    .await;
    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;

    fixture
        .node
        .put_imposter(imposter(9301, "hand-edited"))
        .await
        .expect("a hand edit commits");

    let record = call(
        &client,
        fixture.addr,
        "GET",
        "/admin/sources/mocks",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        record["drifted"], true,
        "an operator must be able to see that their fleet no longer matches the source"
    );

    *fixture.source.version.lock().expect("version lock") = "v2".to_owned();
    *fixture.source.body.lock().expect("body lock") = vec![imposter(9301, "v2")];
    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;
    let record = call(
        &client,
        fixture.addr,
        "GET",
        "/admin/sources/mocks",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(record["drifted"], false, "overwrite resolves the drift");
}

/// A skipped pull records the digest it *saw*, not one the fleet holds. So
/// after switching a drifted source to `overwrite` — which an operator does
/// precisely to make the source win — the next pull of that same content must
/// actually apply it, not short-circuit as "unchanged" and strand the fleet on
/// the hand-edited state.
#[tokio::test]
async fn a_skipped_pull_does_not_short_circuit_the_pull_that_resolves_it() {
    let fixture = start().await;
    let client = client();
    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "mocks", "uri": "scripted://cfg/i.json", "onDrift": "skip" }),
    )
    .await;
    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;

    // Drift it, then move the source on: the pull is skipped, but records v2.
    fixture
        .node
        .put_imposter(imposter(9301, "hand-edited"))
        .await
        .expect("a hand edit commits");
    *fixture.source.version.lock().expect("version lock") = "v2".to_owned();
    *fixture.source.body.lock().expect("body lock") = vec![imposter(9301, "v2")];
    let skipped = call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        skipped["unchanged"], false,
        "a skip is not a no-change pull"
    );
    assert_eq!(
        skipped["skipped"], true,
        "a skip must not be reported as an apply: it names no ports and the fleet does not hold it"
    );
    assert_eq!(skipped["changed"], serde_json::json!([]));
    let record = call(
        &client,
        fixture.addr,
        "GET",
        "/admin/sources/mocks",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(record["lastOutcome"], "skipped");
    assert_eq!(record["lastVersion"], "v2");

    // The operator now asks the source to win. Same content as the skipped
    // pull — this must apply, not report "unchanged".
    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "mocks", "uri": "scripted://cfg/i.json", "onDrift": "overwrite" }),
    )
    .await;
    let resolved = call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        resolved["unchanged"], false,
        "the digest a skipped pull recorded was never applied, so it must not short-circuit"
    );
    let record = call(
        &client,
        fixture.addr,
        "GET",
        "/admin/sources/mocks",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(record["lastOutcome"], "applied");
    assert_eq!(record["drifted"], false);
    let body = fixture
        .node
        .get_imposter(9301)
        .expect("read")
        .expect("present");
    assert!(
        body.contains("\"v2\""),
        "the fleet must actually hold what the source declares: {body}"
    );
}

/// Every refusal an operator can provoke must come back as a *refusal*, with a
/// status that says whose problem it is — never as an opaque 500.
#[tokio::test]
async fn operator_errors_are_refusals_not_internal_failures() {
    let fixture = start().await;
    let client = client();

    // A credential-bearing uri: refused before anything is written.
    let err = call_err(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "leaky", "uri": "scripted://user:pw@cfg/i.json" }),
    )
    .await;
    assert_eq!(err.status(), 400, "{err}");
    assert!(err.to_string().contains("auth_ref"), "{err}");

    // A scheme this build has no provider for: told up front, with what it does
    // serve, rather than stored and failing at every pull forever.
    let err = call_err(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "gitty", "uri": "git+https://h/r#main:p" }),
    )
    .await;
    assert_eq!(err.status(), 400, "{err}");
    assert!(err.to_string().contains("scripted"), "{err}");

    // `tracking` mode until #135.
    let err = call_err(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "poller", "uri": "scripted://cfg/i.json", "mode": "tracking" }),
    )
    .await;
    assert_eq!(err.status(), 400, "{err}");
    assert!(err.to_string().contains("#135"), "{err}");

    // An unknown source, read and pulled.
    for (method, path) in [
        ("GET", "/admin/sources/ghost"),
        ("POST", "/admin/sources/ghost/pull"),
    ] {
        let err = call_err(&client, fixture.addr, method, path, serde_json::Value::Null).await;
        assert_eq!(err.status(), 404, "{method} {path}: {err}");
    }

    assert!(
        fixture.node.sources().expect("sources").is_empty(),
        "no refused source may have been stored"
    );
}

/// A source document may declare things a clustered pull does not apply. The
/// operator is told, in the response — silently ignoring a block they wrote is
/// how a config ends up "applied" and not doing what it says.
#[tokio::test]
async fn a_routes_block_in_a_source_document_is_reported_not_silently_dropped() {
    let fixture = start().await;
    let client = client();
    *fixture.source.routes_block.lock().expect("routes lock") = true;

    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "mocks", "uri": "scripted://cfg/i.json" }),
    )
    .await;
    let pulled = call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;
    let warnings = pulled["warnings"].as_array().expect("warnings array");
    assert_eq!(warnings.len(), 1);
    let warning = warnings[0].as_str().expect("string");
    assert!(warning.contains("routes"), "{warning}");
    assert!(
        warning.contains("/front-door/routes"),
        "the warning must say where routes DO come from: {warning}"
    );
    // The imposters still applied — the block is ignored, not fatal.
    assert_eq!(fixture.node.configured_ports().expect("ports"), vec![9301]);
}

/// A document declaring an `intercept` block refuses the pull outright. The
/// cluster refuses the TLS-MITM listener fleet-wide because its state is
/// per-node and is not replicated, so applying the imposters and dropping the
/// block would leave the operator with a listener they configured and never
/// got.
#[tokio::test]
async fn an_intercept_block_in_a_source_document_refuses_the_pull() {
    let fixture = start().await;
    let client = client();
    *fixture.source.intercept_block.lock().expect("lock") = true;

    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "mocks", "uri": "scripted://cfg/i.json" }),
    )
    .await;
    let err = call_err(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(err.status(), 400, "{err}");
    assert!(err.to_string().contains("intercept"), "{err}");
    assert!(
        fixture.node.configured_ports().expect("ports").is_empty(),
        "a refused pull must apply nothing, not the imposters-minus-the-block"
    );
}

/// A hand edit is repaired by pulling, even when the upstream document has not
/// changed — which is the ordinary case, since the operator edited the *fleet*,
/// not the source. Answering "unchanged" here would make drift unrepairable
/// except by editing the document upstream.
#[tokio::test]
async fn a_pull_repairs_drift_even_when_the_document_is_unchanged() {
    let fixture = start().await;
    let client = client();
    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources",
        serde_json::json!({ "id": "mocks", "uri": "scripted://cfg/i.json" }),
    )
    .await;
    call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;

    fixture
        .node
        .put_imposter(imposter(9301, "hand-edited"))
        .await
        .expect("a hand edit commits");

    // Same document, byte for byte — only the fleet moved.
    let repaired = call(
        &client,
        fixture.addr,
        "POST",
        "/admin/sources/mocks/pull",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        repaired["unchanged"], false,
        "the fleet no longer matches the source, so there IS something to do"
    );
    assert_eq!(repaired["skipped"], false);
    let body = fixture
        .node
        .get_imposter(9301)
        .expect("read")
        .expect("present");
    assert!(
        body.contains("\"v1\"") && !body.contains("hand-edited"),
        "the source's own content must be restored: {body}"
    );
    let record = call(
        &client,
        fixture.addr,
        "GET",
        "/admin/sources/mocks",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(record["drifted"], false);
}

/// The source surface rides the authenticated cluster port, so it is subject to
/// the same credential as everything else there.
#[tokio::test]
async fn the_source_surface_requires_the_cluster_credential() {
    let fixture = start().await;
    let anonymous = RpcClient::new(None, Arc::new(AlwaysHealthy), RpcClientConfig::default());
    let err = anonymous
        .call(fixture.addr, "GET", "/admin/sources", Vec::new())
        .await
        .expect_err("an unauthenticated read must be refused");
    assert_eq!(err.status(), 401, "{err}");
}
