//! The cluster port's HTTP server: authenticate, negotiate, dispatch.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, Limited, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use super::RpcError;
use super::auth::{AUTH_HEADER, SignedRequest, Verifier};
use super::routes::{PROTO_HEADER, Router, StreamReply, negotiate, recipient};

/// Default cap on an accepted request body. Cluster payloads are small control
/// messages; an unbounded reader is a memory bomb, and the *reader* is capped
/// rather than a declared length checked — a chunked request declares no length
/// at all, and the cap has to hold before the credential is verified, so it must
/// not depend on anything the sender chooses to tell us.
pub const DEFAULT_MAX_BODY_BYTES: u64 = 32 * 1024 * 1024;

/// How the server was configured to authenticate peers.
pub struct RpcServerConfig {
    /// `None` runs the port unauthenticated — only reachable via an explicit
    /// insecure acknowledgment at startup (see [`crate::config`]).
    pub verifier: Option<Arc<Verifier>>,
    pub router: Router,
    /// Cap on a single request body. Defaults to [`DEFAULT_MAX_BODY_BYTES`].
    pub max_body_bytes: u64,
    /// The node this server answers as. When set, a request addressed to another node is refused
    /// before any handler runs (D-96). `None` for a server with no member identity.
    pub node_id: Option<u64>,
}

impl RpcServerConfig {
    /// Config for a router with the default body cap.
    #[must_use]
    pub fn new(verifier: Option<Arc<Verifier>>, router: Router) -> Self {
        Self {
            verifier,
            router,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            node_id: None,
        }
    }

    /// Answer as member `node_id`: refuse requests addressed to any other node (D-96).
    #[must_use]
    pub fn with_node_id(mut self, node_id: u64) -> Self {
        self.node_id = Some(node_id);
        self
    }
}

/// A bound cluster RPC endpoint.
pub struct RpcServer {
    listener: TcpListener,
    config: Arc<RpcServerConfig>,
}

impl RpcServer {
    /// Bind the cluster port with `SO_REUSEADDR`.
    ///
    /// `SO_REUSEADDR` lets a restarting node rebind its address immediately even
    /// while connections accepted by the previous instance (which share the
    /// listener's local port) are still draining — without it, a fast restart
    /// races those sockets and fails with `EADDRINUSE`. It never permits a second
    /// *listener* on a live port, so it does not weaken binding.
    pub async fn bind(addr: SocketAddr, config: RpcServerConfig) -> std::io::Result<Self> {
        let socket = match addr {
            SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        socket.set_reuseaddr(true)?;
        socket.bind(addr)?;
        let listener = socket.listen(1024)?;
        Ok(Self {
            listener,
            config: Arc::new(config),
        })
    }

    /// The address actually bound (resolves an ephemeral `:0` request).
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serve until the task is dropped or cancelled.
    ///
    /// Connection tasks are owned by a [`JoinSet`], not detached: when this future
    /// is dropped (the accept task is aborted at shutdown), the set is dropped and
    /// every in-flight connection is aborted with it — so stopping a node actually
    /// releases its sockets, rather than leaving peers talking to a zombie whose
    /// Raft has already stopped.
    pub async fn serve(self) {
        // A systemic accept failure (fd exhaustion) returns instantly and
        // forever, so a bare `continue` would spin a core and flood the log
        // exactly when the node is already in trouble. Back off instead, and
        // reset as soon as an accept succeeds.
        let mut backoff = Duration::from_millis(1);
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let accepted = tokio::select! {
                // Reap finished connections so the set stays bounded to the live
                // ones; disabled while empty so the arm never resolves spuriously.
                // A cancelled task is an expected shutdown; a panicked one is a
                // real bug and must not vanish silently.
                Some(joined) = connections.join_next(), if !connections.is_empty() => {
                    if let Err(e) = joined
                        && e.is_panic()
                    {
                        tracing::error!(error = %e, "cluster rpc connection task panicked");
                    }
                    continue;
                }
                accepted = self.listener.accept() => accepted,
            };
            let (stream, peer) = match accepted {
                Ok(accepted) => {
                    backoff = Duration::from_millis(1);
                    accepted
                }
                Err(e) => {
                    tracing::debug!(error = %e, ?backoff, "cluster rpc accept failed");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                    continue;
                }
            };
            let config = Arc::clone(&self.config);
            connections.spawn(async move {
                let service = service_fn(move |req| {
                    let config = Arc::clone(&config);
                    async move { Ok::<_, std::convert::Infallible>(handle(config, req).await) }
                });
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    tracing::debug!(error = %e, %peer, "cluster rpc connection ended");
                }
            });
        }
    }
}

/// Every response body the port sends: one buffered document, or a stream (D-101).
type ResponseBody = UnsyncBoxBody<Bytes, std::io::Error>;

/// What a dispatched request answers with.
enum Reply {
    Json(Vec<u8>),
    Stream(StreamReply),
}

fn buffered(bytes: Bytes) -> ResponseBody {
    Full::new(bytes)
        .map_err(|never| match never {})
        .boxed_unsync()
}

async fn handle(config: Arc<RpcServerConfig>, req: Request<Incoming>) -> Response<ResponseBody> {
    match dispatch(&config, req).await {
        Ok(Reply::Json(body)) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(buffered(Bytes::from(body)))
            .unwrap_or_else(|_| error_response(&RpcError::Handler("malformed response".into()))),
        Ok(Reply::Stream(StreamReply { content_type, body })) => {
            let frames = futures_util::StreamExt::map(body, |chunk| chunk.map(Frame::data));
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", content_type)
                .body(StreamBody::new(frames).boxed_unsync())
                .unwrap_or_else(|_| error_response(&RpcError::Handler("malformed response".into())))
        }
        Err(e) => error_response(&e),
    }
}

fn error_response(err: &RpcError) -> Response<ResponseBody> {
    let mut body = serde_json::json!({
        "error": err.reason(),
        "message": err.to_string(),
    });
    // A write that was durably accepted before the cluster failed to commit it
    // is not lost — the replay loop owns it. Naming the op is what lets the
    // client poll `GET /_cluster/ops/:id` instead of blind-retrying a write
    // that may already be on its way (Ch. 4 write path).
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(err.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "application/json");
    // The leader hint travels as a field, not just inside the rendered message:
    // a caller that has to parse prose to find the leader cannot act on it, which
    // is exactly how a join at a follower burned its whole deadline (#391).
    if let RpcError::NotLeader {
        leader: Some(leader),
    } = err
    {
        body["leader"] = serde_json::Value::String(leader.clone());
    }
    // Ids travel as strings: they exceed 2^53, and an envelope a JSON reader rounds would name the
    // wrong node.
    if let RpcError::WrongNode { expected, actual } = err {
        body["expected"] = serde_json::Value::String(expected.to_string());
        body["actual"] = serde_json::Value::String(actual.to_string());
    }
    if let RpcError::Unavailable {
        op_id: Some(op_id), ..
    } = err
    {
        body["opId"] = serde_json::Value::String(op_id.clone());
        builder = builder
            .header(crate::decorate::HEADER_OP_ID, op_id.as_str())
            .header("retry-after", "1");
    }
    builder
        .body(buffered(Bytes::from(body.to_string())))
        // Infallible in practice: the status is validated above and the body is
        // owned. Falling back to a bare 500 keeps the signature total.
        .unwrap_or_else(|_| {
            let mut resp = Response::new(buffered(Bytes::from_static(b"{}")));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        })
}

async fn dispatch(config: &RpcServerConfig, req: Request<Incoming>) -> Result<Reply, RpcError> {
    let method = req.method().as_str().to_owned();
    // Sign the query too: a handler that later reads one would otherwise have
    // an unauthenticated input, and the client signs whatever it puts on the
    // wire, so the two must cover the same string.
    let path = req
        .uri()
        .path_and_query()
        .map_or_else(|| req.uri().path().to_owned(), ToString::to_string);

    let header = |name: &str| -> Option<String> {
        req.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let proto = header(PROTO_HEADER);
    let credential = header(AUTH_HEADER);

    // Version first: a peer on an incompatible major may not even encode the
    // credential the way this build reads it, so "you are too old/new" is the
    // honest answer rather than an authentication failure.
    negotiate(proto.as_deref())?;

    // `Limited` stops reading at the cap, so an oversized body is refused
    // having buffered at most `MAX_BODY_BYTES` — whether or not the sender
    // declared a length, and before any credential is checked.
    let limit = config.max_body_bytes;
    let body = Limited::new(req.into_body(), limit as usize)
        .collect()
        .await
        .map_err(|_| RpcError::BodyTooLarge { limit })?
        .to_bytes();

    if let Some(verifier) = &config.verifier {
        verifier.verify(
            credential.as_deref(),
            SignedRequest {
                method: &method,
                path: &path,
                body: &body,
            },
        )?;
    }

    // After the credential, so a caller without the cluster secret learns nothing about which node
    // answers at this address; before any handler, so a request meant for another member never
    // touches this node's state (D-96).
    if let (Some(to), Some(me)) = (recipient(&path)?, config.node_id)
        && to != me
    {
        return Err(RpcError::WrongNode {
            expected: to,
            actual: me,
        });
    }

    if let Some(handler) = config.router.lookup(&method, &path) {
        return handler.call(body.to_vec()).await.map(Reply::Json);
    }
    if let Some((handler, suffix)) = config.router.lookup_prefix(&method, &path) {
        return handler.call(suffix, body.to_vec()).await.map(Reply::Json);
    }
    if let Some((handler, suffix)) = config.router.lookup_stream_prefix(&method, &path) {
        return handler.call(suffix).await.map(Reply::Stream);
    }
    Err(RpcError::UnknownRoute { method, path })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::rpc::{AlwaysHealthy, HandlerFuture, RpcClient, RpcClientConfig, Signer};

    const SECRET: &str = "addressed-recipient-secret";

    /// A server that believes it is node 77, with one route whose calls are counted.
    async fn server_as_77() -> (SocketAddr, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let router = Router::new().route(
            "POST",
            "/count",
            Arc::new(move |_body: Vec<u8>| -> HandlerFuture {
                let seen = Arc::clone(&seen);
                Box::pin(async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    Ok(b"{}".to_vec())
                })
            }),
        );
        let config =
            RpcServerConfig::new(Some(Arc::new(Verifier::new(SECRET))), router).with_node_id(77);
        let server = RpcServer::bind("127.0.0.1:0".parse().expect("addr"), config)
            .await
            .expect("bind");
        let addr = server.local_addr().expect("addr");
        (addr, calls, tokio::spawn(server.serve()))
    }

    fn client(secret: &str) -> RpcClient {
        RpcClient::new(
            Some(Signer::new(secret)),
            Arc::new(AlwaysHealthy),
            RpcClientConfig {
                connect_timeout: Duration::from_millis(500),
                request_timeout: Duration::from_secs(2),
                max_retries: 0,
            },
        )
    }

    /// Pins D-96: a request named for another node is refused before any handler runs, naming both
    /// ids; one named for this node, or not named at all (an older sender), reaches the handler.
    #[tokio::test]
    async fn a_request_named_for_another_node_never_reaches_a_handler() {
        let (addr, calls, _server) = server_as_77().await;
        let client = client(SECRET);

        let refused = client.call(addr, "POST", "/count?to=3", Vec::new()).await;
        assert_eq!(
            refused,
            Err(RpcError::WrongNode {
                expected: 3,
                actual: 77
            })
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "the handler must not run for another node's request"
        );

        client
            .call(addr, "POST", "/count?to=77", Vec::new())
            .await
            .expect("named for this node");
        client
            .call(addr, "POST", "/count", Vec::new())
            .await
            .expect("unnamed, from an older sender");
        client
            .call(addr, "POST", "/count?x=1&to=77", Vec::new())
            .await
            .expect("named after another parameter");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    /// Pins D-96: a signed request with a malformed recipient is a sender bug, and the gate fails
    /// closed rather than treating it as unnamed.
    #[tokio::test]
    async fn a_malformed_recipient_is_refused_as_a_bad_request() {
        let (addr, calls, _server) = server_as_77().await;
        let refused = client(SECRET)
            .call(addr, "POST", "/count?to=abc", Vec::new())
            .await;
        assert!(
            matches!(refused, Err(RpcError::BadRequest(_))),
            "got {refused:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    /// Pins D-96: the recipient is checked only after the credential, so a caller without the
    /// cluster secret learns nothing about which node answers at an address.
    #[tokio::test]
    async fn an_unauthenticated_caller_is_refused_before_the_recipient_is_compared() {
        let (addr, _calls, _server) = server_as_77().await;
        let refused = client("not-the-secret")
            .call(addr, "POST", "/count?to=3", Vec::new())
            .await;
        assert!(
            matches!(refused, Err(RpcError::Unauthorized(_))),
            "got {refused:?}"
        );
    }

    /// Pins D-96: the recipient is inside the signed string. A request signed for node 3 and
    /// re-addressed in flight to node 77 fails the MAC instead of reaching node 77's handler.
    #[test]
    fn re_addressing_a_signed_request_breaks_its_signature() {
        let signer = Signer::new(SECRET);
        let verifier = Verifier::new(SECRET);
        let header = signer.header(SignedRequest {
            method: "POST",
            path: "/internal/v1/raft/vote?to=3",
            body: b"{}",
        });
        let replayed = verifier.verify(
            Some(&header),
            SignedRequest {
                method: "POST",
                path: "/internal/v1/raft/vote?to=77",
                body: b"{}",
            },
        );
        assert_eq!(replayed, Err(crate::rpc::AuthError::BadMac));
    }

    /// Pins D-101 (#652): a streamed route answers a signed GET with its body as it is produced —
    /// the first response on the cluster port that is not one buffered JSON document. The route is
    /// behind the same credential, recipient and route checks as every other.
    #[tokio::test]
    async fn a_stream_route_streams_its_body_to_a_signed_get() {
        use crate::rpc::{StreamFuture, StreamReply};
        let router = Router::new().route_stream_prefix(
            "GET",
            "/stream/",
            Arc::new(|suffix: String| -> StreamFuture {
                Box::pin(async move {
                    let chunks: Vec<std::io::Result<Bytes>> = vec![
                        Ok(Bytes::from(format!("{suffix}:"))),
                        Ok(Bytes::from_static(b"abc")),
                        Ok(Bytes::from_static(b"def")),
                    ];
                    Ok(StreamReply {
                        content_type: "application/zstd",
                        body: Box::pin(futures_util::stream::iter(chunks)),
                    })
                })
            }),
        );
        let server = RpcServer::bind(
            "127.0.0.1:0".parse().expect("addr"),
            RpcServerConfig::new(Some(Arc::new(Verifier::new(SECRET))), router).with_node_id(77),
        )
        .await
        .expect("bind");
        let addr = server.local_addr().expect("addr");
        let _server = tokio::spawn(server.serve());

        let body = client(SECRET)
            .fetch_stream(addr, "/stream/x/3?to=77", Duration::from_secs(2))
            .await
            .expect("a signed GET streams");
        let bytes = body.collect().await.expect("read the stream").to_bytes();
        assert_eq!(&bytes[..], b"x/3?to=77:abcdef");

        let refused = client("not-the-secret")
            .fetch_stream(addr, "/stream/x/3", Duration::from_secs(2))
            .await;
        assert!(
            matches!(refused, Err(RpcError::Unauthorized(_))),
            "got {refused:?}"
        );
        let elsewhere = client(SECRET)
            .fetch_stream(addr, "/stream/x/3?to=3", Duration::from_secs(2))
            .await;
        assert!(
            matches!(elsewhere, Err(RpcError::WrongNode { .. })),
            "got {elsewhere:?}"
        );
        let unknown = client(SECRET)
            .fetch_stream(addr, "/nope", Duration::from_secs(2))
            .await;
        assert!(
            matches!(unknown, Err(RpcError::UnknownRoute { .. })),
            "got {unknown:?}"
        );
    }

    /// Pins D-96: a server that has not been told its id cannot compare, and accepts the request.
    #[tokio::test]
    async fn a_server_without_an_identity_ignores_the_recipient() {
        let router = Router::new().route(
            "POST",
            "/count",
            Arc::new(|_body: Vec<u8>| -> HandlerFuture { Box::pin(async { Ok(b"{}".to_vec()) }) }),
        );
        let server = RpcServer::bind(
            "127.0.0.1:0".parse().expect("addr"),
            RpcServerConfig::new(Some(Arc::new(Verifier::new(SECRET))), router),
        )
        .await
        .expect("bind");
        let addr = server.local_addr().expect("addr");
        let _server = tokio::spawn(server.serve());
        client(SECRET)
            .call(addr, "POST", "/count?to=3", Vec::new())
            .await
            .expect("no identity, no comparison");
    }
}
