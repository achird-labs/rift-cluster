//! The `/_cluster/*` operator surface (RFC-001 §11.1).
//!
//! These endpoints ride the cluster port rather than the admin API: they answer
//! questions about the fleet, they must be authenticated with the cluster
//! credential rather than the admin API key, and the cluster port already has
//! exactly that transport. Every answer comes from *this node's* applied state,
//! so comparing two nodes' answers is what tells an operator whether the fleet
//! has converged.

use std::sync::{Arc, OnceLock, Weak};

use rift_cluster::rpc::{HandlerFuture, RpcError};
use rift_cluster::{RaftNode, Router};

use crate::readiness::Readiness;

/// Binding an already-bound [`NodeSlot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the cluster operator surface was bound to a node twice")]
pub struct NodeSlotAlreadyBound;

/// A late-filled handle to the node the endpoints report on.
///
/// The node cannot exist when its routes are built — the routes are needed to
/// bind the cluster port, whose address the node then advertises — so the
/// handlers read it through this slot, which the composition fills in
/// immediately after the node is constructed. The cluster port is already
/// accepting by then, so a request landing in that window is answered with a
/// clean "not available yet" rather than being served stale.
///
/// The handle is [`Weak`] on purpose. The node owns the task that serves these
/// very handlers, so a strong reference here would be a cycle: the node would
/// keep itself alive, its `Drop` (which aborts the accept loop and releases the
/// cluster port and the redb lock) would never run, and every startup path that
/// bails out after the node exists would leak a live node for the life of the
/// process.
#[derive(Clone, Default)]
pub struct NodeSlot(Arc<OnceLock<Weak<RaftNode>>>);

impl NodeSlot {
    /// Publish the node to the handlers.
    ///
    /// Binding twice is a composition bug — the second node would be invisible
    /// to the whole operator surface — so it is returned rather than logged,
    /// leaving the caller no way to continue as if it had worked.
    pub fn set(&self, node: &Arc<RaftNode>) -> Result<(), NodeSlotAlreadyBound> {
        self.0
            .set(Arc::downgrade(node))
            .map_err(|_| NodeSlotAlreadyBound)
    }

    fn node(&self) -> Result<Arc<RaftNode>, RpcError> {
        self.0
            .get()
            .ok_or_else(|| RpcError::Handler("cluster node is not available yet".to_owned()))?
            .upgrade()
            .ok_or_else(|| RpcError::Handler("cluster node is shutting down".to_owned()))
    }
}

/// Register the operator endpoints onto `base`.
#[must_use]
pub fn routes(base: Router, slot: NodeSlot, readiness: Arc<Readiness>) -> Router {
    let members = slot.clone();
    let config = slot.clone();
    let imposters = slot.clone();
    let ops = slot.clone();
    let health = slot;

    base.route_prefix(
        "GET",
        "/_cluster/ops/",
        Arc::new(move |suffix: String, _body: Vec<u8>| -> HandlerFuture {
            let slot = ops.clone();
            Box::pin(async move {
                let node = slot.node()?;
                // The suffix may carry a query string; the id is the path part.
                let id = suffix.split('?').next().unwrap_or_default();
                // A malformed id names no op the same way an unknown one
                // does: 404, never the internal-failure bucket.
                let Ok(op_id) = id.parse::<uuid::Uuid>() else {
                    return Err(RpcError::UnknownRoute {
                        method: "GET".to_owned(),
                        path: format!("/_cluster/ops/{id}"),
                    });
                };
                let value = match node.read_op(&op_id).map_err(handler_error)? {
                    Some(response) => match response.outcome {
                        rift_cluster::ControlOutcome::Applied => serde_json::json!({
                            "state": "applied",
                            "revision": response.revision,
                        }),
                        rift_cluster::ControlOutcome::Failed { reason } => serde_json::json!({
                            "state": "failed",
                            "revision": response.revision,
                            "detail": reason,
                        }),
                    },
                    None if node.intent_parked(&op_id).map_err(handler_error)? => {
                        serde_json::json!({ "state": "pending" })
                    }
                    // Unknown ids and ops whose dedup window has lapsed are
                    // indistinguishable; both answer 404.
                    None => {
                        return Err(RpcError::UnknownRoute {
                            method: "GET".to_owned(),
                            path: format!("/_cluster/ops/{id}"),
                        });
                    }
                };
                serde_json::to_vec(&value).map_err(handler_error)
            })
        }),
    )
    .route(
        "GET",
        "/_cluster/members",
        json_handler(move || {
            let status = members.node()?.status();
            Ok(serde_json::json!({
                "node_id": status.node_id,
                "is_leader": status.is_leader,
                "current_leader": status.current_leader,
                "last_applied": status.last_applied,
                "voters": status.voters,
            }))
        }),
    )
    .route(
        "GET",
        "/_cluster/config",
        json_handler(move || {
            let node = config.node()?;
            let ports = node.configured_ports().map_err(handler_error)?;
            Ok(serde_json::json!({
                "ports": ports,
                "last_applied": node.status().last_applied,
            }))
        }),
    )
    .route(
        "GET",
        "/_cluster/imposters",
        json_handler(move || {
            let node = imposters.node()?;
            let ports = node.configured_ports().map_err(handler_error)?;
            let mut reported = Vec::with_capacity(ports.len());
            for port in ports {
                let body = node.get_imposter(port).map_err(handler_error)?;
                // A committed body that will not parse is corruption, not an
                // absent imposter: report it as such rather than hiding the port.
                let config = match body {
                    Some(body) => {
                        serde_json::from_str::<serde_json::Value>(&body).unwrap_or_else(|e| {
                            // Surfacing it in the response only helps whoever
                            // happens to poll; corruption of committed state has
                            // to reach the logs too.
                            tracing::error!(
                                port,
                                error = %e,
                                "committed imposter config will not parse"
                            );
                            serde_json::json!({ "error": e.to_string() })
                        })
                    }
                    None => serde_json::Value::Null,
                };
                reported.push(serde_json::json!({ "port": port, "config": config }));
            }
            Ok(serde_json::json!({ "imposters": reported }))
        }),
    )
    .route(
        "GET",
        "/_cluster/health",
        json_handler(move || {
            let node = health.node()?;
            let ring = node.ring();
            Ok(serde_json::json!({
                "ready": readiness.state().is_ready(),
                "state": readiness.state().as_str(),
                "pending_gates": readiness.pending(),
                "isolated": node.is_isolated(),
                "ring": {
                    "m_idx": ring.m_idx(),
                    "members": ring.members(),
                },
            }))
        }),
    )
}

/// Report a node-side failure as a handler error. The node's own error types
/// are already operator-readable, so the transport carries the message rather
/// than re-classifying it.
fn handler_error(e: impl std::fmt::Display) -> RpcError {
    RpcError::Handler(e.to_string())
}

/// Wrap a synchronous, body-less reporter into a cluster-port handler.
fn json_handler<F>(report: F) -> Arc<dyn rift_cluster::rpc::Handler>
where
    F: Fn() -> Result<serde_json::Value, RpcError> + Send + Sync + 'static,
{
    Arc::new(move |_body: Vec<u8>| -> HandlerFuture {
        let reported = report().and_then(|value| serde_json::to_vec(&value).map_err(handler_error));
        Box::pin(async move { reported })
    })
}
