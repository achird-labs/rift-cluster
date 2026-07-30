//! The U-9 loopback authorizer (issue #161): defence in depth for the OSS
//! admin's own port.
//!
//! `admin_front` decides first for every public request — it is the only
//! surface that can render RFC-002 §8.4's cross-tenant `404` (upstream's
//! `AdminAuthorizer` hook can only answer `403`, unconditionally, see
//! `crate::admin_front`'s module doc) — so this authorizer's job is narrower:
//! make sure the loopback core admin, which `admin_front` proxies to, is never
//! a second, unguarded door. Anything that reaches it — through `admin_front`
//! or, on a host that can reach loopback, directly — is still gated here, as
//! a `403`.
//!
//! Installed via `ServerBuilder::admin_authorizer` in `crate::compose`.

use std::sync::Weak;

use rift_cluster::{RaftNode, TenantId};
use rift_cluster_base::seams::{AdminAuthorizer, AuthzDecision, AuthzRequest};

use crate::authz::{self, Decision};
use crate::principal;

/// Backed by the same [`authz::decide`] evaluator and the same
/// [`principal::resolve_bindings`] credential resolution `admin_front` uses —
/// installing a second, independently-written check here is exactly how the
/// two enforcement points would drift apart.
pub struct EeAuthorizer {
    /// `Weak` so this authorizer can never be what keeps the node alive —
    /// the same rule every other long-lived handle onto the node follows
    /// (see `crate::cluster_api::NodeSlot`, `crate::admin_front::FrontState`).
    pub node: Weak<RaftNode>,
    pub api_key: Option<String>,
    pub legacy_key_is_fleet_admin: bool,
}

impl AdminAuthorizer for EeAuthorizer {
    fn authorize(&self, req: AuthzRequest<'_>) -> AuthzDecision {
        let Some(node) = self.node.upgrade() else {
            // The node is tearing down: refuse rather than guess at a
            // decision with nothing to read it against.
            return AuthzDecision::Deny {
                reason: "cluster node is shutting down",
            };
        };

        let action = principal::map_action(
            req.action,
            false,
            req.space.is_some(),
            req.params.iter().any(|(name, _)| *name == "scenario"),
        );

        // Upstream's own scope header (`x-rift-scope`, not the cluster
        // `X-Rift-Tenant` `admin_front` reads): this hook is upstream's
        // generic seam, and `scope` is the generic equivalent — see
        // `AuthzRequest::scope`'s doc. A request that reached this authorizer
        // through `admin_front`'s proxy carries whatever headers the client
        // sent verbatim, so a client that set `X-Rift-Tenant` but not
        // `x-rift-scope` is read here as the default tenant; that is fine,
        // because `admin_front` has already made the real decision for that
        // request. This authorizer exists for the path that skips it.
        let requested = req.scope.map(TenantId::new).unwrap_or_default();

        let resolved = match principal::resolve_bindings(
            &node,
            self.api_key.as_deref(),
            self.legacy_key_is_fleet_admin,
            req.credential,
        ) {
            Ok(Some(resolved)) => resolved,
            Ok(None) => {
                return match principal::should_bypass(&node, self.api_key.as_deref()) {
                    Ok(true) => AuthzDecision::allow(),
                    Ok(false) => AuthzDecision::Deny {
                        reason: "unauthenticated",
                    },
                    // Fail closed: a bypass check that cannot read the fleet's
                    // principals must never default to "open".
                    Err(_) => AuthzDecision::Deny {
                        reason: "cannot read the fleet's principals",
                    },
                };
            }
            // Fail closed: a bindings read that errors must never fall
            // through to an allow.
            Err(_) => {
                return AuthzDecision::Deny {
                    reason: "cannot read principal bindings",
                };
            }
        };

        match authz::decide(&resolved.bindings, action, &requested) {
            // U-10 attribution (issue #855): naming the principal here is
            // what lets a change event say who caused it.
            Decision::Allow { .. } => AuthzDecision::Allow {
                principal: Some(resolved.principal_id),
            },
            Decision::Deny(_) => AuthzDecision::Deny {
                reason: "not authorized",
            },
        }
    }
}
