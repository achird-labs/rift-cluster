//! End-to-end tests over a real loopback cluster port: a signed client talking
//! to a bound server, plus the refusals a hostile or mismatched peer would hit.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rift_cluster::RpcError;
use rift_cluster::bridge::{Bridge, BridgeConfig, CallerClass};
use rift_cluster::rpc::client::AlwaysHealthy;
use rift_cluster::rpc::routes::HandlerFuture;
use rift_cluster::rpc::{
    Router, RpcClient, RpcClientConfig, RpcServer, RpcServerConfig, Signer, Verifier,
};

const SECRET: &str = "cluster-secret-under-test";

/// Bind a server on an ephemeral port with an `/echo` route and serve it on the
/// bridge's cluster-io runtime.
fn start_server(bridge: &Bridge, verifier: Option<Arc<Verifier>>) -> SocketAddr {
    start_server_capped(bridge, verifier, rift_cluster::rpc::DEFAULT_MAX_BODY_BYTES)
}

fn start_server_capped(
    bridge: &Bridge,
    verifier: Option<Arc<Verifier>>,
    max_body_bytes: u64,
) -> SocketAddr {
    let router = Router::new().route(
        "POST",
        "/internal/v1/echo",
        Arc::new(|body: Vec<u8>| -> HandlerFuture { Box::pin(async move { Ok(body) }) }),
    );
    let handle = bridge.handle();
    let (addr_tx, addr_rx) = std::sync::mpsc::sync_channel(1);
    handle.spawn(async move {
        let server = RpcServer::bind(
            "127.0.0.1:0".parse().expect("valid loopback address"),
            RpcServerConfig {
                verifier,
                router,
                max_body_bytes,
            },
        )
        .await
        .expect("cluster port binds");
        addr_tx
            .send(server.local_addr().expect("bound address"))
            .expect("test receiver alive");
        server.serve().await;
    });
    addr_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("server reports its address")
}

fn client(signer: Option<Signer>) -> RpcClient {
    RpcClient::new(signer, Arc::new(AlwaysHealthy), RpcClientConfig::default())
}

fn bridge() -> Bridge {
    Bridge::start(BridgeConfig {
        data_plane_permits: 4,
        script_pool_permits: 16,
        io_threads: 2,
    })
    .expect("cluster-io runtime starts")
}

#[test]
fn rpc_loopback_round_trip() {
    let bridge = bridge();
    let addr = start_server(&bridge, Some(Arc::new(Verifier::new(SECRET))));
    let client = client(Some(Signer::new(SECRET)));

    // Drive it exactly as production does: a synchronous caller parking on the
    // bridge while cluster-io performs the request.
    let response = bridge
        .call(CallerClass::DataPlane, Duration::from_secs(5), async move {
            client
                .call(
                    addr,
                    "POST",
                    "/internal/v1/echo",
                    b"{\"hello\":\"peer\"}".to_vec(),
                )
                .await
        })
        .expect("round trip succeeds");

    assert_eq!(response, b"{\"hello\":\"peer\"}");
}

#[test]
fn rpc_rejects_a_peer_with_the_wrong_secret() {
    let bridge = bridge();
    let addr = start_server(&bridge, Some(Arc::new(Verifier::new(SECRET))));
    let client = client(Some(Signer::new("a different secret")));

    let err = bridge
        .call(CallerClass::DataPlane, Duration::from_secs(5), async move {
            client
                .call(addr, "POST", "/internal/v1/echo", b"{}".to_vec())
                .await
        })
        .expect_err("a forged MAC must be refused");

    assert!(matches!(err, RpcError::Unauthorized(_)), "{err:?}");
}

#[test]
fn rpc_rejects_an_unsigned_peer() {
    let bridge = bridge();
    let addr = start_server(&bridge, Some(Arc::new(Verifier::new(SECRET))));
    let client = client(None);

    let err = bridge
        .call(CallerClass::DataPlane, Duration::from_secs(5), async move {
            client
                .call(addr, "POST", "/internal/v1/echo", b"{}".to_vec())
                .await
        })
        .expect_err("an unsigned request must be refused");

    assert!(matches!(err, RpcError::Unauthorized(_)), "{err:?}");
}

#[test]
fn rpc_unknown_route_is_typed_error() {
    let bridge = bridge();
    let addr = start_server(&bridge, Some(Arc::new(Verifier::new(SECRET))));
    let client = client(Some(Signer::new(SECRET)));

    let err = bridge
        .call(CallerClass::DataPlane, Duration::from_secs(5), async move {
            client
                .call(addr, "POST", "/internal/v1/nope", b"{}".to_vec())
                .await
        })
        .expect_err("an unregistered route must not be served");

    assert!(matches!(err, RpcError::UnknownRoute { .. }), "{err:?}");
}

#[test]
fn rpc_replayed_request_is_refused_by_the_server() {
    let bridge = bridge();
    let addr = start_server(&bridge, Some(Arc::new(Verifier::new(SECRET))));
    let signer = Signer::new(SECRET);

    // Replay the *same* credential twice — what a captured request would do.
    let body = b"{}".to_vec();
    let header = signer.header_at(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after the unix epoch")
            .as_secs(),
        "fixed-nonce",
        rift_cluster::rpc::SignedRequest {
            method: "POST",
            path: "/internal/v1/echo",
            body: &body,
        },
    );

    let send = |header: String, body: Vec<u8>| {
        bridge
            .handle()
            .block_on(async move { raw_post(addr, "/internal/v1/echo", &header, body).await })
    };

    assert_eq!(
        send(header.clone(), body.clone()),
        200,
        "first use is accepted"
    );
    assert_eq!(send(header, body), 401, "the replay is refused");
}

/// Minimal signed-request sender that bypasses `RpcClient` so a test can reuse
/// one credential across two requests.
async fn raw_post(addr: SocketAddr, path: &str, auth: &str, body: Vec<u8>) -> u16 {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let request = hyper::Request::builder()
        .method("POST")
        .uri(format!("http://{addr}{path}"))
        .header(
            "x-rift-cluster-proto",
            rift_cluster::rpc::PROTO_VERSION.to_string(),
        )
        .header("x-rift-cluster-auth", auth)
        .body(Full::new(Bytes::from(body)))
        .expect("request builds");
    client
        .request(request)
        .await
        .expect("peer responds")
        .status()
        .as_u16()
}

#[test]
fn rpc_rejects_an_incompatible_protocol_major() {
    let bridge = bridge();
    let addr = start_server(&bridge, Some(Arc::new(Verifier::new(SECRET))));

    let status = bridge.handle().block_on(async move {
        use http_body_util::Full;
        use hyper::body::Bytes;
        use hyper_util::client::legacy::Client;
        use hyper_util::rt::TokioExecutor;

        let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
        let request = hyper::Request::builder()
            .method("POST")
            .uri(format!("http://{addr}/internal/v1/echo"))
            .header("x-rift-cluster-proto", "99.0")
            .body(Full::new(Bytes::new()))
            .expect("request builds");
        client
            .request(request)
            .await
            .expect("peer responds")
            .status()
            .as_u16()
    });

    assert_eq!(status, 426, "an incompatible major must be told to upgrade");
}

#[test]
fn rpc_caps_an_oversized_body_without_a_declared_length() {
    // A chunked request declares no length, so a cap that trusts the sender's
    // Content-Length enforces nothing. This runs unauthenticated on purpose:
    // the body is buffered before the credential is checked, so anyone who can
    // reach the cluster port could otherwise exhaust the node's memory.
    //
    // The cap is set small so the refusal is deterministic. Streaming past a
    // 32 MiB cap races the server's early response — the server is entitled to
    // answer 413 and stop reading while the client is still sending, which
    // surfaces at the client as a connection error rather than a status. That
    // race is inherent to HTTP, not a defect, so the test does not depend on it.
    let bridge = bridge();
    let addr = start_server_capped(&bridge, Some(Arc::new(Verifier::new(SECRET))), 1024);

    let outcome = bridge.handle().block_on(async move {
        use futures_lite::stream;
        use http_body_util::{BodyExt, StreamBody};
        use hyper::body::{Bytes, Frame};
        use hyper_util::client::legacy::Client;
        use hyper_util::rt::TokioExecutor;

        // 8 KiB in 512-byte frames: over the 1 KiB cap, no Content-Length.
        let chunks = (0..16)
            .map(|_| Ok::<_, std::convert::Infallible>(Frame::data(Bytes::from(vec![b'x'; 512]))));
        let body = StreamBody::new(stream::iter(chunks)).boxed();

        let client: Client<_, _> = Client::builder(TokioExecutor::new()).build_http();
        let request = hyper::Request::builder()
            .method("POST")
            .uri(format!("http://{addr}/internal/v1/echo"))
            .header(
                "x-rift-cluster-proto",
                rift_cluster::rpc::PROTO_VERSION.to_string(),
            )
            .body(body)
            .expect("request builds");
        client.request(request).await.map(|r| r.status().as_u16())
    });

    // An `Err` here means the server closed the connection mid-body, which is
    // also a refusal; what must never happen is a success.
    if let Ok(status) = outcome {
        assert_eq!(status, 413, "an oversized chunked body must be refused");
    }

    // The point of the cap: the node is still standing and still serving.
    let client = client(Some(Signer::new(SECRET)));
    let response = bridge
        .call(CallerClass::DataPlane, Duration::from_secs(5), async move {
            client
                .call(addr, "POST", "/internal/v1/echo", b"ok".to_vec())
                .await
        })
        .expect("server survives an oversized request");
    assert_eq!(response, b"ok");
}

#[test]
fn rpc_serves_a_body_at_the_cap() {
    // The boundary the other test's refusal is measured against: exactly at the
    // cap must still succeed, or the limit would be off by one frame.
    let bridge = bridge();
    let addr = start_server_capped(&bridge, Some(Arc::new(Verifier::new(SECRET))), 1024);
    let client = client(Some(Signer::new(SECRET)));

    let body = vec![b'y'; 1024];
    let response = bridge
        .call(CallerClass::DataPlane, Duration::from_secs(5), {
            let body = body.clone();
            async move { client.call(addr, "POST", "/internal/v1/echo", body).await }
        })
        .expect("a body exactly at the cap is served");
    assert_eq!(response, body);
}

#[test]
fn rpc_insecure_server_serves_unsigned_peers() {
    // The explicitly-acknowledged insecure mode: no verifier, no credential.
    let bridge = bridge();
    let addr = start_server(&bridge, None);
    let client = client(None);

    let response = bridge
        .call(CallerClass::DataPlane, Duration::from_secs(5), async move {
            client
                .call(addr, "POST", "/internal/v1/echo", b"plain".to_vec())
                .await
        })
        .expect("insecure mode accepts unsigned peers");

    assert_eq!(response, b"plain");
}
