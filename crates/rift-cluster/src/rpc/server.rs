//! The cluster port's HTTP server: authenticate, negotiate, dispatch.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use super::RpcError;
use super::auth::{AUTH_HEADER, SignedRequest, Verifier};
use super::routes::{PROTO_HEADER, Router, negotiate};
use crate::metrics;

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
}

impl RpcServerConfig {
    /// Config for a router with the default body cap.
    #[must_use]
    pub fn new(verifier: Option<Arc<Verifier>>, router: Router) -> Self {
        Self {
            verifier,
            router,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
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

async fn handle(config: Arc<RpcServerConfig>, req: Request<Incoming>) -> Response<Full<Bytes>> {
    match dispatch(&config, req).await {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap_or_else(|_| error_response(&RpcError::Handler("malformed response".into()))),
        Err(e) => {
            metrics::rpc_failure(e.reason());
            error_response(&e)
        }
    }
}

fn error_response(err: &RpcError) -> Response<Full<Bytes>> {
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
        .body(Full::new(Bytes::from(body.to_string())))
        // Infallible in practice: the status is validated above and the body is
        // owned. Falling back to a bare 500 keeps the signature total.
        .unwrap_or_else(|_| {
            let mut resp = Response::new(Full::new(Bytes::from_static(b"{}")));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        })
}

async fn dispatch(config: &RpcServerConfig, req: Request<Incoming>) -> Result<Vec<u8>, RpcError> {
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

    if let Some(handler) = config.router.lookup(&method, &path) {
        return handler.call(body.to_vec()).await;
    }
    if let Some((handler, suffix)) = config.router.lookup_prefix(&method, &path) {
        return handler.call(suffix, body.to_vec()).await;
    }
    Err(RpcError::UnknownRoute { method, path })
}
