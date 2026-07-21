//! The `/_cluster/*` operator surface (RFC-001 §11.1).
//!
//! These endpoints ride the cluster port rather than the admin API: they answer
//! questions about the fleet, they must be authenticated with the cluster
//! credential rather than the admin API key, and the cluster port already has
//! exactly that transport. Every answer comes from *this node's* applied state,
//! so comparing two nodes' answers is what tells an operator whether the fleet
//! has converged.

use std::sync::{Arc, OnceLock};

use rift_cluster::rpc::{HandlerFuture, RpcError};
use rift_cluster::{RaftNode, Router};

use crate::readiness::Readiness;

/// A late-filled handle to the node the endpoints report on.
///
/// The node cannot exist when its routes are built — the routes are needed to
/// bind the cluster port, whose address the node then advertises — so the
/// handlers read it through this slot, which the composition fills in before the
/// node accepts any traffic.
#[derive(Clone, Default)]
pub struct NodeSlot(Arc<OnceLock<Arc<RaftNode>>>);

impl NodeSlot {
    /// Publish the node to the handlers. Filling an already-filled slot is a
    /// composition bug — the second node would be invisible — so it is reported.
    pub fn set(&self, node: Arc<RaftNode>) {
        if self.0.set(node).is_err() {
            tracing::error!("cluster operator surface bound to a node twice");
        }
    }

    fn get(&self) -> Result<&Arc<RaftNode>, RpcError> {
        self.0
            .get()
            .ok_or_else(|| RpcError::Handler("cluster node is not available yet".to_owned()))
    }
}

/// Register the operator endpoints onto `base`.
#[must_use]
pub fn routes(base: Router, slot: NodeSlot, readiness: Arc<Readiness>) -> Router {
    let members = slot.clone();
    let config = slot.clone();
    let imposters = slot.clone();
    let health = slot;

    base.route(
        "GET",
        "/_cluster/members",
        json_handler(move || {
            let status = members.get()?.status();
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
            let node = config.get()?;
            let ports = node
                .configured_ports()
                .map_err(|e| RpcError::Handler(e.to_string()))?;
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
            let node = imposters.get()?;
            let ports = node
                .configured_ports()
                .map_err(|e| RpcError::Handler(e.to_string()))?;
            let mut reported = Vec::with_capacity(ports.len());
            for port in ports {
                let body = node
                    .get_imposter(port)
                    .map_err(|e| RpcError::Handler(e.to_string()))?;
                // A committed body that will not parse is corruption, not an
                // absent imposter: report it as such rather than hiding the port.
                let config = match body {
                    Some(body) => serde_json::from_str::<serde_json::Value>(&body)
                        .unwrap_or_else(|e| serde_json::json!({ "error": e.to_string() })),
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
            let node = health.get()?;
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

/// Wrap a synchronous, body-less reporter into a cluster-port handler.
fn json_handler<F>(report: F) -> Arc<dyn rift_cluster::rpc::Handler>
where
    F: Fn() -> Result<serde_json::Value, RpcError> + Send + Sync + 'static,
{
    Arc::new(move |_body: Vec<u8>| -> HandlerFuture {
        let reported = report().and_then(|value| {
            serde_json::to_vec(&value).map_err(|e| RpcError::Handler(e.to_string()))
        });
        Box::pin(async move { reported })
    })
}
