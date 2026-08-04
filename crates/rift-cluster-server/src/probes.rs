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

/// How long the mode detection below waits for a loopback TCP connect. A
/// refused connect on loopback is immediate; this budget only bounds a wedged
/// stack, and it must fit well inside the image's `HEALTHCHECK --timeout=3s`.
const PROBE_DETECT_TIMEOUT: Duration = Duration::from_millis(250);

// The same guard upstream keeps on its half of the Docker budget
// (`healthcheck_default_timeout_is_under_the_dockerfile_budget`): detection
// plus the probe's default timeout must stay under HEALTHCHECK --timeout=3s,
// and raising either past it should be a compile error, not a container that
// flaps unhealthy under load.
const _: () = assert!(
    PROBE_DETECT_TIMEOUT.as_millis() as u64
        + rift_cluster_base::rift_http_proxy::server::DEFAULT_HEALTHCHECK_TIMEOUT_SECS * 1000
        < 3000
);

/// The URL the `healthcheck` subcommand probes when `--url` is not given,
/// derived from the same flags (and `RIFT_*` environment) the server itself
/// parses — which is what lets one exec-form container `HEALTHCHECK` line be
/// correct in both modes (#297).
///
/// Clustered, the target is the probe listener's `/healthz`. Unclustered there
/// is no probe listener at all — an un-clustered node is indistinguishable from
/// the open-source binary — so the probe falls back to upstream's own default,
/// the admin API's `/health`. A wildcard bind is probed on loopback for the
/// same reason upstream's `default_url` maps one there: "every interface" is
/// not an address to connect *to*.
///
/// When the parsed flags say "unclustered", that answer is only as good as the
/// environment this exec saw: a container clustered purely by command-line
/// arguments shows the healthcheck exec no `RIFT_CLUSTER`. So the negative is
/// verified against the node itself — within the container's network namespace
/// (the shape this subcommand exists for), a listener on the probe port exists
/// if and only if this node is clustered, and probing the admin `/health` on a
/// clustered node is a wrong verdict, not just a wrong URL (the clustered
/// front answers an anonymous `/health` with 401 once principals or an API key
/// exist). The connect failure is the domain answer "genuinely unclustered",
/// not a swallowed error.
#[must_use]
pub fn healthcheck_url(cluster: bool, probe_bind: SocketAddr, host: &str, port: u16) -> String {
    let connect_ip = match probe_bind.ip() {
        ip if ip.is_unspecified() => std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        ip => ip,
    };
    let connect_addr = SocketAddr::new(connect_ip, probe_bind.port());
    let probe_url = match connect_ip {
        std::net::IpAddr::V6(ip) => format!("http://[{ip}]:{}/healthz", probe_bind.port()),
        std::net::IpAddr::V4(ip) => format!("http://{ip}:{}/healthz", probe_bind.port()),
    };
    if cluster || std::net::TcpStream::connect_timeout(&connect_addr, PROBE_DETECT_TIMEOUT).is_ok()
    {
        probe_url
    } else {
        rift_cluster_base::rift_http_proxy::healthcheck::default_url(host, port)
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

#[cfg(test)]
mod tests {
    use super::healthcheck_url;

    /// One test for both sides of the detection, in sequence on one port, so
    /// no sibling test can be handed the port in between and flip an
    /// assertion.
    #[test]
    fn healthcheck_url_detects_a_bound_probe_listener_and_falls_back_without_one() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        assert_eq!(
            healthcheck_url(false, addr, "0.0.0.0", 2525),
            format!("http://127.0.0.1:{}/healthz", addr.port())
        );
        drop(listener);
        assert_eq!(
            healthcheck_url(false, addr, "0.0.0.0", 2525),
            "http://127.0.0.1:2525/health"
        );
    }

    #[test]
    fn healthcheck_url_clustered_probes_the_probe_listener() {
        assert_eq!(
            healthcheck_url(true, "0.0.0.0:2526".parse().expect("addr"), "0.0.0.0", 2525),
            "http://127.0.0.1:2526/healthz"
        );
    }

    #[test]
    fn healthcheck_url_honors_an_overridden_probe_port() {
        assert_eq!(
            healthcheck_url(true, "0.0.0.0:9999".parse().expect("addr"), "0.0.0.0", 2525),
            "http://127.0.0.1:9999/healthz"
        );
    }

    #[test]
    fn healthcheck_url_keeps_an_explicit_probe_host() {
        assert_eq!(
            healthcheck_url(
                true,
                "10.0.0.7:2526".parse().expect("addr"),
                "0.0.0.0",
                2525
            ),
            "http://10.0.0.7:2526/healthz"
        );
    }

    #[test]
    fn healthcheck_url_brackets_an_ipv6_probe_host() {
        assert_eq!(
            healthcheck_url(true, "[::1]:2526".parse().expect("addr"), "0.0.0.0", 2525),
            "http://[::1]:2526/healthz"
        );
        assert_eq!(
            healthcheck_url(true, "[::]:2526".parse().expect("addr"), "0.0.0.0", 2525),
            "http://127.0.0.1:2526/healthz"
        );
    }
}
