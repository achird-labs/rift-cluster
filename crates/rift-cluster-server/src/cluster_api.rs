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
                match op_body(&node, &op_id)? {
                    Some(value) => serde_json::to_vec(&value).map_err(handler_error),
                    // Unknown ids and ops whose dedup window has lapsed are
                    // indistinguishable; both answer 404.
                    None => Err(RpcError::UnknownRoute {
                        method: "GET".to_owned(),
                        path: format!("/_cluster/ops/{id}"),
                    }),
                }
            })
        }),
    )
    .route(
        "GET",
        "/_cluster/members",
        json_handler(move || Ok(members_body(members.node()?.as_ref()))),
    )
    .route(
        "GET",
        "/_cluster/config",
        json_handler(move || {
            let node = config.node()?;
            // issue #182: resource reads went tenant-aware, and `configured_ports`
            // now answers fleet-wide — `(tenant, port)` per row instead of a bare
            // port list. This endpoint is deliberately fleet-wide (design section
            // E): it is what an operator diffs across nodes to see whether the
            // fleet has converged, and "converged" now has to mean "on the same
            // tenant's config at that port", not just "the same port". Emitting
            // `{tenant, port}` rows keeps that honest instead of flattening the
            // tenant back out.
            let ports: Vec<serde_json::Value> = node
                .configured_ports()
                .map_err(handler_error)?
                .into_iter()
                .map(|(tenant, port)| {
                    serde_json::json!({
                        "tenant": tenant,
                        "port": port,
                    })
                })
                .collect();
            // Provenance (issue #134): which imposters a source owns, and at
            // which version. Reported here rather than only under
            // `/admin/sources` because this is the endpoint an operator
            // compares across nodes to see whether the fleet has converged —
            // and "converged on the same configs from the same source version"
            // is the question a source-driven fleet actually asks. Tenant-qualified
            // for the same reason `ports` above is (issue #182).
            let provenance: Vec<serde_json::Value> = node
                .config_provenance()
                .map_err(handler_error)?
                .into_iter()
                .map(|(tenant, port, source)| {
                    serde_json::json!({
                        "tenant": tenant,
                        "port": port,
                        "sourceId": source.id,
                        "version": source.version,
                    })
                })
                .collect();
            Ok(serde_json::json!({
                "ports": ports,
                "provenance": provenance,
                "last_applied": node.status().last_applied,
            }))
        }),
    )
    .route(
        "GET",
        "/_cluster/imposters",
        json_handler(move || {
            let node = imposters.node()?;
            // issue #182: `configured_ports` now hands back `(tenant, port)` pairs
            // fleet-wide, so the owning tenant for each port is already in hand —
            // no need for a second `owning_tenant` lookup (and its documented
            // non-O(1) cost) per port.
            let ports = node.configured_ports().map_err(handler_error)?;
            let mut reported = Vec::with_capacity(ports.len());
            for (tenant, port) in ports {
                let body = node
                    .get_imposter(tenant.as_str(), port)
                    .map_err(handler_error)?;
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
                // `bind_failure` is what makes this endpoint the per-`(port, node)` view RFC-001
                // §7.4.6 promises. Bind status is a node-local observation — it cannot ride the
                // deterministic raft apply — so it is reported by each node about itself, here and
                // in `rift_cluster_bind_failures`, rather than replicated. Absent (`null`) is the
                // healthy answer; present means the node holds the config and serves the imposter
                // in-process but never bound its port.
                //
                // Sourced from `bind_failure`, not the general `apply_failures` map, so a parse or
                // stub-patch failure is never mislabelled as bind divergence — see
                // `RedbStateMachine::bind_failure`.
                reported.push(serde_json::json!({
                    "tenant": tenant,
                    "port": port,
                    "config": config,
                    "bind_failure": node.bind_failure(port),
                }));
            }
            Ok(serde_json::json!({ "imposters": reported }))
        }),
    )
    .route(
        "GET",
        "/_cluster/health",
        json_handler(move || Ok(health_body(health.node()?.as_ref(), &readiness))),
    )
}

/// Build the `/_cluster/members` body: this node's view of raft membership.
///
/// Shared with the `/_fleet/members` admin-port projection ([`crate::fleet`],
/// RFC-006 §5.2) so the two ports answer from one code path rather than two
/// implementations that a test has to keep honest — one implementation
/// called twice cannot diverge.
pub(crate) fn members_body(node: &RaftNode) -> serde_json::Value {
    let status = node.status();
    serde_json::json!({
        "node_id": status.node_id,
        "is_leader": status.is_leader,
        "current_leader": status.current_leader,
        "last_applied": status.last_applied,
        "voters": status.voters,
    })
}

/// Build the `/_cluster/health` body: readiness plus this node's ring view.
///
/// Shared with `/_fleet/health` for the same reason as [`members_body`].
pub(crate) fn health_body(node: &RaftNode, readiness: &Readiness) -> serde_json::Value {
    let ring = node.ring();
    serde_json::json!({
        "ready": readiness.state().is_ready(),
        "state": readiness.state().as_str(),
        "pending_gates": readiness.pending(),
        "isolated": node.is_isolated(),
        "ring": {
            "m_idx": ring.m_idx(),
            "members": ring.members(),
        },
    })
}

/// Build the `/_cluster/ops/:id` body for a well-formed op id.
///
/// `Ok(None)` means the id names no op — unknown ids and ops whose dedup
/// window has lapsed are indistinguishable, and both are the caller's 404.
/// Shared with `/_fleet/ops/:id` for the same reason as [`members_body`].
pub(crate) fn op_body(
    node: &RaftNode,
    op_id: &uuid::Uuid,
) -> Result<Option<serde_json::Value>, RpcError> {
    let value = match node.read_op(op_id).map_err(handler_error)? {
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
        None if node.intent_parked(op_id).map_err(handler_error)? => {
            serde_json::json!({ "state": "pending" })
        }
        None => return Ok(None),
    };
    Ok(Some(value))
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
