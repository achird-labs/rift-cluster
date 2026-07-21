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
    /// Bind the cluster port.
    pub async fn bind(addr: SocketAddr, config: RpcServerConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
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
    pub async fn serve(self) {
        // A systemic accept failure (fd exhaustion) returns instantly and
        // forever, so a bare `continue` would spin a core and flood the log
        // exactly when the node is already in trouble. Back off instead, and
        // reset as soon as an accept succeeds.
        let mut backoff = Duration::from_millis(1);
        loop {
            let (stream, peer) = match self.listener.accept().await {
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
            tokio::spawn(async move {
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
    let body = serde_json::json!({
        "error": err.reason(),
        "message": err.to_string(),
    })
    .to_string();
    Response::builder()
        .status(StatusCode::from_u16(err.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
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

    let handler = config
        .router
        .lookup(&method, &path)
        .ok_or_else(|| RpcError::UnknownRoute {
            method: method.clone(),
            path: path.clone(),
        })?;

    handler.call(body.to_vec()).await
}
