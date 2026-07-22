//! `/_cluster/*` (issue #10 AC4): the operator surface on the authenticated
//! cluster port, answering from this node's committed state.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rift_cluster::rpc::{AlwaysHealthy, RpcClient, RpcClientConfig, Signer};
use rift_cluster::{NodeConfig, RaftNode};
use rift_ee_server::cluster_api;
use rift_ee_server::readiness::{GATE_JOINED, Readiness};
use tempfile::TempDir;

const SECRET: &str = "cluster-api-test-secret";

struct Fixture {
    node: Arc<RaftNode>,
    addr: SocketAddr,
    _dir: TempDir,
}

async fn start() -> Fixture {
    let dir = TempDir::new().expect("tempdir");
    let readiness = Arc::new(Readiness::awaiting([GATE_JOINED]));
    let slot = cluster_api::NodeSlot::default();
    let config = NodeConfig {
        node_id: 1,
        bind: "127.0.0.1:0".parse().expect("bind addr"),
        advertise: None,
        data_dir: dir.path().to_path_buf(),
        secret: Some(SECRET.to_owned()),
        routes: cluster_api::routes(rift_cluster::Router::new(), slot.clone(), readiness.clone()),
        engine: None,
    };
    let node = Arc::new(RaftNode::start(config).await.expect("node starts"));
    slot.set(&node).expect("the slot is bound exactly once");
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
        _dir: dir,
    }
}

fn client(secret: Option<&str>) -> RpcClient {
    RpcClient::new(
        secret.map(Signer::new),
        Arc::new(AlwaysHealthy),
        RpcClientConfig::default(),
    )
}

async fn get(client: &RpcClient, addr: SocketAddr, path: &str) -> serde_json::Value {
    let body = client
        .call(addr, "GET", path, Vec::new())
        .await
        .unwrap_or_else(|e| panic!("GET {path}: {e}"));
    serde_json::from_slice(&body).expect("json body")
}

#[tokio::test]
async fn members_reports_this_nodes_view_of_the_cluster() {
    let fixture = start().await;
    let client = client(Some(SECRET));

    let members = get(&client, fixture.addr, "/_cluster/members").await;
    assert_eq!(members["node_id"], 1);
    assert_eq!(members["voters"], serde_json::json!([1]));
    assert_eq!(members["current_leader"], 1);
    assert_eq!(members["is_leader"], true);
}

#[tokio::test]
async fn config_and_imposters_report_committed_state() {
    let fixture = start().await;
    let client = client(Some(SECRET));

    let config = get(&client, fixture.addr, "/_cluster/config").await;
    assert_eq!(config["ports"], serde_json::json!([]));

    fixture
        .node
        .put_imposter(
            serde_json::from_value(serde_json::json!({ "port": 4545, "protocol": "http" }))
                .expect("test config parses"),
        )
        .await
        .expect("committed write");

    let config = get(&client, fixture.addr, "/_cluster/config").await;
    assert_eq!(config["ports"], serde_json::json!([4545]));

    let imposters = get(&client, fixture.addr, "/_cluster/imposters").await;
    assert_eq!(imposters["imposters"][0]["port"], 4545);
    assert_eq!(
        imposters["imposters"][0]["config"]["protocol"], "http",
        "the committed body is reported as JSON, not an escaped string"
    );
}

#[tokio::test]
async fn health_reports_readiness_and_the_ring() {
    let fixture = start().await;
    let client = client(Some(SECRET));

    let health = get(&client, fixture.addr, "/_cluster/health").await;
    assert_eq!(health["ready"], true);
    assert_eq!(health["pending_gates"], serde_json::json!([]));
    assert_eq!(health["isolated"], false);
    // A single-node cluster's ring is itself, at the membership log index.
    assert_eq!(health["ring"]["members"], serde_json::json!([1]));
    assert!(
        health["ring"]["m_idx"].as_u64().is_some(),
        "ring epoch must be reported so nodes can be compared: {health}"
    );
}

#[tokio::test]
async fn the_operator_surface_requires_the_cluster_credential() {
    let fixture = start().await;
    let anonymous = client(None);

    let err = anonymous
        .call(fixture.addr, "GET", "/_cluster/members", Vec::new())
        .await
        .expect_err("unauthenticated call must be refused");
    let rendered = err.to_string();
    assert!(
        rendered.contains("401") || rendered.to_lowercase().contains("auth"),
        "expected an auth failure, got: {rendered}"
    );

    let wrong = client(Some("not-the-cluster-secret"));
    assert!(
        wrong
            .call(fixture.addr, "GET", "/_cluster/members", Vec::new())
            .await
            .is_err(),
        "a wrong secret must not authenticate"
    );
}

#[tokio::test]
async fn control_plane_routes_are_not_shadowed_by_the_operator_surface() {
    // The node must still replicate: registering extra routes cannot displace
    // the Raft endpoints the cluster depends on.
    let fixture = start().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture.node.status().is_leader {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("node keeps leadership with the operator routes registered");
}

/// Issue #9 slice 3: `GET /_cluster/ops/:id` reports the three states, and an
/// unknown (or lapsed) id is a 404, not an empty success.
#[tokio::test]
async fn ops_endpoint_reports_applied_pending_and_unknown() {
    use rift_cluster::{ControlOp, ControlOutcome, ControlRequest, TenantId};

    let fixture = start().await;
    let client = client(Some(SECRET));

    // Applied: submit with a known op id, then read it back.
    let applied_id = uuid::Uuid::from_u128(0xA11D);
    let response = fixture
        .node
        .write(ControlRequest {
            op_id: applied_id,
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op: ControlOp::PutImposter {
                tenant: TenantId::default(),
                config: serde_json::from_value(
                    serde_json::json!({ "port": 4546, "protocol": "http" }),
                )
                .expect("config parses"),
            },
        })
        .await
        .expect("write commits");
    assert_eq!(response.outcome, ControlOutcome::Applied);

    let reported = get(
        &client,
        fixture.addr,
        &format!("/_cluster/ops/{applied_id}"),
    )
    .await;
    assert_eq!(reported["state"], "applied");
    assert_eq!(reported["revision"], response.revision);

    // Failed ops are terminal and queryable too.
    let failed_id = uuid::Uuid::from_u128(0xFA11);
    fixture
        .node
        .write(ControlRequest {
            op_id: failed_id,
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op: ControlOp::DeleteAll {
                tenant: TenantId::new("acme"),
            },
        })
        .await
        .expect("write commits (with a failed outcome)");
    let reported = get(&client, fixture.addr, &format!("/_cluster/ops/{failed_id}")).await;
    assert_eq!(reported["state"], "failed");
    assert!(
        reported["detail"]
            .as_str()
            .expect("detail")
            .contains("tenant"),
        "{reported}"
    );

    // Pending: parked but never submitted.
    let pending_id = uuid::Uuid::from_u128(0x9E4D);
    fixture
        .node
        .park_intent(&ControlRequest {
            op_id: pending_id,
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op: ControlOp::DeleteImposter {
                tenant: TenantId::default(),
                port: 4547,
            },
        })
        .expect("park");
    let reported = get(
        &client,
        fixture.addr,
        &format!("/_cluster/ops/{pending_id}"),
    )
    .await;
    assert_eq!(reported["state"], "pending");

    // Unknown id → the request errors rather than fabricating a state.
    let unknown = uuid::Uuid::from_u128(0xDEAD);
    let err = client
        .call(
            fixture.addr,
            "GET",
            &format!("/_cluster/ops/{unknown}"),
            Vec::new(),
        )
        .await
        .expect_err("unknown op must not answer 200");
    assert!(format!("{err}").contains("route"), "{err}");
}
