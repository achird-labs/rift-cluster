//! The read-only `/_fleet/*` admin-port projection (RFC-006 §5.2, issue #185).
//!
//! `/_cluster/*` ([`crate::cluster_api`]) answers the same three questions,
//! but rides the cluster port and is authenticated with the cluster
//! credential. An operator working the admin API has no way to reach it
//! without also holding a credential this surface was never meant to hand
//! out. `/_fleet/*` re-exposes the same bodies on the admin port instead —
//! by calling the cluster port's own builders rather than reimplementing
//! them, so the two ports cannot drift. Two independent implementations
//! checked by a test can still diverge between test runs; one implementation
//! called twice cannot.
//!
//! This module is deliberately pure: it classifies a request into a
//! [`FleetRoute`] and renders a body. It does not authenticate, authorize, or
//! build an HTTP response — that stays the admin front's job, kept in one
//! place rather than spread across every route that needs it.

use hyper::Method;
use rift_cluster::RaftNode;

use crate::cluster_api::{health_body, members_body, op_body};
use crate::readiness::Readiness;

/// A recognized `/_fleet/*` route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FleetRoute {
    Members,
    Health,
    Op(uuid::Uuid),
}

/// Classify `(method, path)` into a `/_fleet/*` route, or `None` if it names
/// none of the three this projection answers.
///
/// A malformed op id names no op the same way an unknown one does (the same
/// reasoning `/_cluster/ops/:id` already documents), so it classifies to
/// `None` here too — the caller's generic 404, not a route match that then
/// fails to render.
pub(crate) fn classify(method: &Method, path: &str) -> Option<FleetRoute> {
    if *method != Method::GET {
        return None;
    }
    match path.strip_prefix("/_fleet/")? {
        "members" => Some(FleetRoute::Members),
        "health" => Some(FleetRoute::Health),
        rest => {
            let id = rest.strip_prefix("ops/")?;
            // The id may carry a query string; only the path part names the op.
            let id = id.split('?').next().unwrap_or_default();
            id.parse::<uuid::Uuid>().ok().map(FleetRoute::Op)
        }
    }
}

/// Render the body for `route`. `Ok(None)` is the caller's 404 — it only
/// happens for [`FleetRoute::Op`], since `classify` never produces `Members`
/// or `Health` for a request that could fail to render one.
pub(crate) fn body(
    route: &FleetRoute,
    node: &RaftNode,
    readiness: &Readiness,
) -> Result<Option<serde_json::Value>, String> {
    match route {
        FleetRoute::Members => Ok(Some(members_body(node))),
        FleetRoute::Health => Ok(Some(health_body(node, readiness))),
        FleetRoute::Op(op_id) => op_body(node, op_id).map_err(|e| e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_members() {
        assert_eq!(
            classify(&Method::GET, "/_fleet/members"),
            Some(FleetRoute::Members)
        );
    }

    #[test]
    fn classifies_health() {
        assert_eq!(
            classify(&Method::GET, "/_fleet/health"),
            Some(FleetRoute::Health)
        );
    }

    #[test]
    fn classifies_a_well_formed_op_id() {
        let id = uuid::Uuid::new_v4();
        assert_eq!(
            classify(&Method::GET, &format!("/_fleet/ops/{id}")),
            Some(FleetRoute::Op(id))
        );
    }

    #[test]
    fn a_malformed_op_id_names_no_route() {
        assert_eq!(classify(&Method::GET, "/_fleet/ops/not-a-uuid"), None);
    }

    #[test]
    fn an_unrecognized_path_names_no_route() {
        assert_eq!(classify(&Method::GET, "/_fleet/config"), None);
    }

    #[test]
    fn a_non_get_method_names_no_route() {
        assert_eq!(classify(&Method::POST, "/_fleet/members"), None);
    }
}
