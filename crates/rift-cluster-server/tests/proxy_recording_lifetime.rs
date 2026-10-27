//! D-108 (#689) end to end: a stub-less proxyOnce recording survives every whole-config write
//! the admin front commits as `PutImposter` — an index-addressed stub delete and a `GET` →
//! `PUT …/stubs` round trip — so the origin is not called again. The unit tests in
//! `raft/store.rs` pin the apply arm; this pins that those routes reach it and that nothing on
//! the request path (the owner's completion cache included) masks a purge.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod common;

use common::ports::{reserve_addr, reserve_port};

const SECRET: &str = "proxy-recording-lifetime-secret";
const BOUND: Duration = Duration::from_secs(20);

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(BOUND)
        .build()
        .expect("client builds")
}

fn solo(state: &TempDir) -> EeCli {
    EeCli::try_parse_from([
        "rift-cluster-server",
        "--port",
        "0",
        "--metrics-port",
        "0",
        "--cluster",
        "--cluster-bind",
        &reserve_addr(),
        "--cluster-probe-bind",
        "127.0.0.1:0",
        "--cluster-secret",
        SECRET,
        "--cluster-state-dir",
        &state.path().to_string_lossy(),
        "--cluster-allow-solo",
    ])
    .expect("parses")
}

async fn wait_ready(server: &ComposedServer) {
    let probes = server.probe_addr().expect("probes bound under --cluster");
    let deadline = Instant::now() + BOUND;
    loop {
        if let Ok(response) = client().get(format!("http://{probes}/readyz")).send().await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(Instant::now() < deadline, "node never became ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// An origin that answers every request `200 origin` and counts the requests it served.
async fn counting_origin() -> (u16, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("origin binds");
    let port = listener.local_addr().expect("addr").port();
    let served = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&served);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let counter = Arc::clone(&counter);
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    match socket.read(&mut byte).await {
                        Ok(1) => head.push(byte[0]),
                        _ => return,
                    }
                }
                counter.fetch_add(1, Ordering::SeqCst);
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\norigin",
                    )
                    .await;
            });
        }
    });
    (port, served)
}

/// `GET /a` on the imposter, polled until it answers `origin` — the recorded or forwarded body.
async fn get_a(port: u16) {
    let deadline = Instant::now() + BOUND;
    loop {
        if let Ok(response) = client()
            .get(format!("http://127.0.0.1:{port}/a"))
            .send()
            .await
            && response.text().await.is_ok_and(|body| body == "origin")
        {
            return;
        }
        assert!(Instant::now() < deadline, "the imposter never answered /a");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stub_less_recording_survives_whole_config_edits_without_reforwarding() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(solo(&state))
        .await
        .expect("solo node starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();
    let (origin, served) = counting_origin().await;
    let port = reserve_port();

    let created = client()
        .post(format!("http://{admin}/imposters"))
        .json(&json!({
            "port": port,
            "protocol": "http",
            "stubs": [
                { "predicates": [{ "equals": { "path": "/static" } }],
                  "responses": [{ "is": { "body": "static" } }] },
                // No predicateGenerators: the recording is stub-less, its row the only replay source.
                { "responses": [{ "proxy": {
                    "to": format!("http://127.0.0.1:{origin}"), "mode": "proxyOnce" } }] }
            ]
        }))
        .send()
        .await
        .expect("create");
    assert_eq!(created.status().as_u16(), 201);

    get_a(port).await;
    get_a(port).await;
    assert_eq!(
        served.load(Ordering::SeqCst),
        1,
        "recorded once, then replayed"
    );

    let deleted = client()
        .delete(format!("http://{admin}/imposters/{port}/stubs/0"))
        .send()
        .await
        .expect("index delete");
    assert_eq!(deleted.status().as_u16(), 200);
    get_a(port).await;
    assert_eq!(
        served.load(Ordering::SeqCst),
        1,
        "an index delete commits a PutImposter, which must keep the stub-less recording"
    );

    let imposter: serde_json::Value = client()
        .get(format!("http://{admin}/imposters/{port}"))
        .send()
        .await
        .expect("get")
        .json()
        .await
        .expect("json");
    let replaced = client()
        .put(format!("http://{admin}/imposters/{port}/stubs"))
        .json(&json!({ "stubs": imposter["stubs"] }))
        .send()
        .await
        .expect("replace stubs");
    assert_eq!(replaced.status().as_u16(), 200);
    get_a(port).await;
    assert_eq!(
        served.load(Ordering::SeqCst),
        1,
        "a GET → PUT …/stubs round trip must keep the stub-less recording"
    );

    server.shutdown().await;
}
