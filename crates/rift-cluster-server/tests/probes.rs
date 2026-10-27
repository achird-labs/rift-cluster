//! `/readyz` and `/healthz` (issue #10 AC3): the load-balancer gate is closed
//! until every registered startup gate has reported in, and closes again the
//! moment a graceful leave begins.

use std::sync::Arc;

use rift_cluster_base::rift_http_proxy::server::ListenAddr;
use rift_cluster_server::probes;
use rift_cluster_server::readiness::{GATE_JOINED, Readiness};

/// Stands in for a gate a later phase registers (config-sync's initial
/// reconcile), so the multi-gate semantics are covered before there are two.
const GATE_RECONCILED: &str = "config-reconciled";

async fn probe(base: &str, path: &str) -> (u16, serde_json::Value) {
    let response = reqwest::get(format!("http://{base}{path}"))
        .await
        .expect("probe request");
    let status = response.status().as_u16();
    let body = response.json().await.expect("probe body is json");
    (status, body)
}

#[tokio::test]
async fn readyz_is_503_until_every_gate_reports_and_names_what_is_pending() {
    let readiness = Arc::new(Readiness::awaiting([GATE_JOINED, GATE_RECONCILED]));
    let listener = probes::bind(
        ListenAddr::Exact("127.0.0.1:0".parse().expect("addr")),
        readiness.clone(),
    )
    .await
    .expect("probe listener binds");
    let base = listener.local_addr().to_string();

    let (status, body) = probe(&base, "/readyz").await;
    assert_eq!(status, 503);
    assert_eq!(body["status"], "not-ready");
    let pending: Vec<&str> = body["pending"]
        .as_array()
        .expect("pending is an array")
        .iter()
        .map(|v| v.as_str().expect("gate name"))
        .collect();
    assert_eq!(pending, [GATE_JOINED, GATE_RECONCILED]);

    readiness.satisfy(GATE_JOINED);
    let (status, body) = probe(&base, "/readyz").await;
    assert_eq!(status, 503, "one gate left: {body}");
    assert_eq!(body["pending"], serde_json::json!([GATE_RECONCILED]));

    readiness.satisfy(GATE_RECONCILED);
    let (status, body) = probe(&base, "/readyz").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ready");
    assert_eq!(body["pending"], serde_json::json!([]));

    listener.shutdown().await;
}

#[tokio::test]
async fn healthz_answers_200_while_the_process_serves_regardless_of_readiness() {
    let readiness = Arc::new(Readiness::awaiting([GATE_JOINED]));
    let listener = probes::bind(
        ListenAddr::Exact("127.0.0.1:0".parse().expect("addr")),
        readiness.clone(),
    )
    .await
    .expect("probe listener binds");
    let base = listener.local_addr().to_string();

    // Liveness and readiness are different questions: a node still converging is
    // alive, and restarting it would only slow the convergence down.
    let (status, body) = probe(&base, "/healthz").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ok");
    assert_eq!(probe(&base, "/readyz").await.0, 503);

    listener.shutdown().await;
}

#[tokio::test]
async fn draining_closes_the_gate_again_so_the_balancer_sheds_first() {
    let readiness = Arc::new(Readiness::awaiting([GATE_JOINED]));
    readiness.satisfy(GATE_JOINED);
    let listener = probes::bind(
        ListenAddr::Exact("127.0.0.1:0".parse().expect("addr")),
        readiness.clone(),
    )
    .await
    .expect("probe listener binds");
    let base = listener.local_addr().to_string();
    assert_eq!(probe(&base, "/readyz").await.0, 200);

    // The first step of a SIGTERM graceful leave, before any socket closes.
    readiness.start_draining();

    let (status, body) = probe(&base, "/readyz").await;
    assert_eq!(status, 503);
    assert_eq!(body["status"], "draining");
    // Liveness must stay up while draining, or the orchestrator kills the node
    // mid-drain and turns a graceful leave into a hard one.
    assert_eq!(probe(&base, "/healthz").await.0, 200);

    // Draining is terminal: a late gate report cannot re-open the balancer.
    readiness.satisfy(GATE_JOINED);
    assert_eq!(probe(&base, "/readyz").await.0, 503);

    listener.shutdown().await;
}

#[tokio::test]
async fn an_unknown_probe_path_is_404_not_a_misleading_ok() {
    let readiness = Arc::new(Readiness::awaiting([]));
    let listener = probes::bind(
        ListenAddr::Exact("127.0.0.1:0".parse().expect("addr")),
        readiness,
    )
    .await
    .expect("probe listener binds");
    let base = listener.local_addr().to_string();

    assert_eq!(probe(&base, "/readyz").await.0, 200, "no gates => ready");
    assert_eq!(probe(&base, "/metrics").await.0, 404);

    listener.shutdown().await;
}

/// Pins D-113: the default probe address answers on IPv4 and IPv6 alike — `[::]` dual-stack — so the
/// kubelet's probe of an IPv6 pod IP reaches it. Skipped where the host has no IPv6 loopback.
#[tokio::test]
async fn an_unset_probe_address_answers_on_ipv4_and_ipv6() {
    if std::net::TcpListener::bind("[::1]:0").is_err() {
        return;
    }
    let readiness = Arc::new(Readiness::awaiting([]));
    let listener = probes::bind(ListenAddr::Any(0), readiness)
        .await
        .expect("bind every interface");
    let port = listener.local_addr().port();
    for host in ["127.0.0.1", "[::1]"] {
        let answered = reqwest::get(format!("http://{host}:{port}/healthz"))
            .await
            .unwrap_or_else(|e| panic!("/healthz on {host}: {e}"));
        assert_eq!(answered.status().as_u16(), 200, "{host}");
    }
    listener.shutdown().await;
}
