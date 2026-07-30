//! The unauthenticated probe listener: `GET /readyz` and `GET /healthz`.
//!
//! These are the orchestrator's two questions and they are deliberately
//! separate. `/healthz` asks "is this process alive?" — answered while the node
//! is still converging and while it is draining, because restarting it in either
//! state makes things worse. `/readyz` asks "should traffic go here?" and is
//! gated by [`Readiness`].
//!
//! The listener is its own port rather than a route on the admin API: the admin
//! server is composed inside the open-source `ServerBuilder` and is not
//! extensible, and probes must stay unauthenticated even when the admin API
//! requires an API key.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::readiness::Readiness;

/// How many probe connections may be in flight at once. A load balancer and a
/// kubelet need a handful; anything approaching this is a flood, not a fleet.
const MAX_CONCURRENT_PROBES: usize = 64;

/// A bound, serving probe listener.
pub struct ProbeListener {
    local_addr: SocketAddr,
    task: JoinHandle<()>,
}

impl ProbeListener {
    /// The address actually bound (resolves an ephemeral `:0` request).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stop serving probes and release the port.
    pub async fn shutdown(self) {
        self.task.abort();
        // The accept task owns every connection task, so awaiting it here is what
        // makes the port actually free by the time this returns. A cancellation
        // is the expected outcome; anything else is a bug that would otherwise
        // vanish at shutdown, which is exactly when nobody is looking.
        if let Err(e) = self.task.await
            && !e.is_cancelled()
        {
            tracing::error!(error = %e, "probe accept task ended abnormally");
        }
    }
}

/// Bind the probe port and start serving. A bind failure is fatal to startup:
/// a node whose readiness gate never answers is one an orchestrator will either
/// never route to or never stop routing to, depending on its defaults.
pub async fn bind(addr: SocketAddr, readiness: Arc<Readiness>) -> std::io::Result<ProbeListener> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(%local_addr, "cluster probes listening on /readyz and /healthz");

    let task = tokio::spawn(async move {
        // A systemic accept failure (fd exhaustion) returns instantly and
        // forever, so a bare `continue` would spin a core and flood the log
        // exactly when the node is already in trouble. Back off instead, and
        // reset as soon as an accept succeeds — the same shape the cluster RPC
        // listener uses.
        let mut backoff = Duration::from_millis(1);
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let accepted = tokio::select! {
                // Reap finished connections so the set stays bounded to the live
                // ones; disabled while empty so the arm never resolves
                // spuriously. A cancelled task is an expected shutdown; a
                // panicked one is a real bug and must not vanish silently.
                Some(joined) = connections.join_next(), if !connections.is_empty() => {
                    if let Err(e) = joined
                        && e.is_panic()
                    {
                        tracing::error!(error = %e, "probe connection task panicked");
                    }
                    continue;
                }
                accepted = listener.accept() => accepted,
            };
            let (stream, _peer) = match accepted {
                Ok(accepted) => {
                    backoff = Duration::from_millis(1);
                    accepted
                }
                Err(e) => {
                    // The listener stays bound when a single accept fails, so
                    // this does not surface to the orchestrator as an
                    // unreachable probe — without a log line the operator would
                    // see probes flap with nothing to correlate it against.
                    tracing::warn!(error = %e, ?backoff, "probe listener accept failed");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                    continue;
                }
            };
            // This port is unauthenticated by design, so an unbounded connection
            // flood must degrade into refusing probes rather than into
            // exhausting the process's file descriptors.
            if connections.len() >= MAX_CONCURRENT_PROBES {
                tracing::warn!(
                    in_flight = connections.len(),
                    "probe listener at its connection cap; refusing a connection"
                );
                drop(stream);
                continue;
            }
            let readiness = Arc::clone(&readiness);
            connections.spawn(async move {
                let service = service_fn(move |req| {
                    let readiness = Arc::clone(&readiness);
                    async move { Ok::<_, std::convert::Infallible>(handle(&readiness, &req)) }
                });
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    tracing::debug!(error = %e, "probe connection ended");
                }
            });
        }
    });

    Ok(ProbeListener { local_addr, task })
}

fn handle(readiness: &Readiness, req: &Request<Incoming>) -> Response<Full<Bytes>> {
    match (req.method(), req.uri().path()) {
        (&hyper::Method::GET, "/readyz") => {
            let state = readiness.state();
            let status = if state.is_ready() {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            json(
                status,
                &serde_json::json!({
                    "status": state.as_str(),
                    "pending": readiness.pending(),
                }),
            )
        }
        (&hyper::Method::GET, "/healthz") => json(
            StatusCode::OK,
            &serde_json::json!({ "status": "ok", "readiness": readiness.state().as_str() }),
        ),
        _ => json(
            StatusCode::NOT_FOUND,
            &serde_json::json!({ "error": "notFound", "probes": ["/readyz", "/healthz"] }),
        ),
    }
}

fn json(status: StatusCode, body: &serde_json::Value) -> Response<Full<Bytes>> {
    // `body` is built from owned JSON values here and at every call site, so
    // serialization cannot fail; the fallback keeps the signature total without
    // ever answering a probe with a wrong status.
    let encoded = serde_json::to_vec(body).unwrap_or_else(|e| {
        tracing::error!(error = %e, "probe body failed to serialize");
        Vec::from(br#"{"error":"internalError"}"#.as_slice())
    });
    let mut response = Response::new(Full::new(Bytes::from(encoded)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response
}
