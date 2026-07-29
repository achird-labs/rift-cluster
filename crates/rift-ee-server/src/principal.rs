//! Resolving an admin request's credential to a principal's tenant bindings,
//! and mapping a classified route to the [`authz::Action`](crate::authz::Action)
//! it is checked against (RFC-002 §3.4, §4.1, §8.5, issue #161).
//!
//! Shared by both enforcement points ([`crate::admin_front`]'s own gate on the
//! public listener, and [`crate::authorizer::EeAuthorizer`] on the loopback
//! OSS admin) so the legacy-key migration and the argon2id key lookup are
//! defined exactly once. Enforcing the same rule two different ways in two
//! places is how the two quietly drift apart.
//!
//! This module does I/O (state-machine reads through [`RaftNode`]), unlike
//! [`crate::authz`], which is deliberately pure — see that module's doc for
//! why the split matters.

use rift_cluster::control::{AuthSource, FLEET_SCOPE, Role, api_key_principal_id, verify_api_key};
use rift_cluster::{NodeError, RaftNode, TenantId};
use rift_ee::seams::actions;
use subtle::ConstantTimeEq;

use crate::authz::Action;

/// A resolved principal: the identity attributed to the request (for U-10 —
/// see `EventContext`/`ControlRequest.principal`) and the bindings
/// [`crate::authz::decide`] evaluates against.
pub(crate) struct Resolved {
    pub principal_id: String,
    pub bindings: Vec<(TenantId, Role)>,
}

/// The synthetic identity attributed to a request authenticated with the
/// legacy `--api-key` (RFC-002 §3.4). Not a real [`rift_cluster::control::PrincipalId`]
/// — no such row exists — but stable, so audit output can tell every legacy-key
/// request apart from a real principal's.
const LEGACY_PRINCIPAL_ID: &str = "legacy:api-key";

/// Resolve `credential` (the raw `Authorization` header value, verbatim) to a
/// principal's bindings.
///
/// `Ok(None)` means no principal resolved — the caller decides whether that
/// is an outright `401`/`403` or, when the fleet defines no principal and no
/// `--api-key` is configured, the pre-#161 open-admin-plane bypass (see
/// [`should_bypass`]).
///
/// Order: the legacy `--api-key` first — one constant-time compare, no
/// state-machine read — then a stored principal (RFC-002 §3.2's
/// argon2id-verified [`AuthSource::ApiKey`] only; OIDC/mTLS are v2 and never
/// resolve from a bearer credential here). Any storage error propagates
/// rather than being read as "no principal": a failure to read bindings must
/// never fall through to allow.
pub(crate) fn resolve_bindings(
    node: &RaftNode,
    api_key: Option<&str>,
    legacy_key_is_fleet_admin: bool,
    credential: Option<&str>,
) -> Result<Option<Resolved>, NodeError> {
    let Some(presented) = credential else {
        return Ok(None);
    };

    if let Some(expected) = api_key
        && bool::from(presented.as_bytes().ct_eq(expected.as_bytes()))
    {
        let mut bindings = vec![(TenantId::default(), Role::TenantAdmin)];
        if legacy_key_is_fleet_admin {
            bindings.push((TenantId::new(FLEET_SCOPE), Role::FleetAdmin));
        }
        return Ok(Some(Resolved {
            principal_id: LEGACY_PRINCIPAL_ID.to_owned(),
            bindings,
        }));
    }

    let principal_id = api_key_principal_id(presented);
    let Some(stored) = node.principal(principal_id.as_str())? else {
        return Ok(None);
    };
    // A disabled principal must resolve to nothing, not to its (still-valid)
    // bindings — disabling is how a fleet revokes a key without waiting on
    // key rotation, and RFC-002 §3.1/§8.5 makes that a Raft-committed fact
    // with no per-node cache to lag behind it.
    if stored.disabled {
        return Ok(None);
    }
    let AuthSource::ApiKey { hash } = &stored.auth else {
        // v1 ships API keys only (RFC-002 §7); a principal minted for a
        // future auth source cannot be resolved from a bearer credential.
        return Ok(None);
    };
    if !verify_api_key(presented, hash) {
        return Ok(None);
    }
    let bindings = node.principal_bindings(principal_id.as_str())?;
    Ok(Some(Resolved {
        principal_id: principal_id.as_str().to_owned(),
        bindings,
    }))
}

/// Whether the admin plane should stay fully open — the pre-#161 default,
/// preserved so an upgrade does not start denying a fleet that never defined
/// any authorization data (RFC-002 §3.4). `Ok(true)` only when neither an
/// `--api-key` nor any principal exists at all; this is also what
/// `rift_cluster_no_principals` reports.
pub(crate) fn should_bypass(node: &RaftNode, api_key: Option<&str>) -> Result<bool, NodeError> {
    Ok(api_key.is_none() && !node.has_any_principals()?)
}

/// Map upstream's action-string classification (`rift_ee::seams::classify`'s
/// `AuthzTarget`, or the U-9 `AuthzRequest` the loopback hook receives) onto
/// our closed [`Action`] set (RFC-002 §4.1).
///
/// Upstream's action strings are coarser than our enum in a few places — every
/// read under `/imposters/:port` classifies as `imposter.read` regardless of
/// whether it is stubs, saved requests or scenarios — but that coarseness
/// never changes an authorization *outcome*: every `*Read` action in RFC-002
/// §4.2's role table is granted together, starting at `Viewer`, so folding
/// them onto [`Action::ImposterRead`] decides identically to whichever exact
/// read it actually was.
///
/// Writes and deletes DO need disambiguation — Operator-tier "disturb" actions
/// (`ScenarioReset`, `SpaceTeardown`/`FlowStateClear`, `SavedRequestsClear`)
/// vs Editor-tier "redefine" actions (`ScenarioWrite`, `SpaceStubWrite`) grant
/// to different roles — so this reads the `space`/`scenario` signal upstream's
/// classifier already extracted rather than re-parsing the path.
///
/// `is_flow_state` distinguishes `SpaceTeardown` from `FlowStateClear` for a
/// `DELETE` with a space, when the caller knows the path prefix
/// (`admin_front`'s own proxied mapping does; the loopback [`AuthzRequest`]
/// carries no path, so [`crate::authorizer::EeAuthorizer`] always passes
/// `false`). That is harmless either way: both are Operator-tier, so which of
/// the two is reported never changes the decision, only an audit label
/// (#163).
#[must_use]
pub(crate) fn map_action(
    action: &str,
    is_flow_state: bool,
    has_space: bool,
    has_scenario_param: bool,
) -> Action {
    if action == actions::IMPOSTER_VERIFY {
        return Action::VerifyRun;
    }
    if action == actions::EVENTS_READ {
        // `/events` is **fleet-wide data until #163 filters it**, so it is
        // gated at fleet-wide authority — deliberately stricter than RFC-002
        // §4.2, which puts `StreamSubscribe` at Viewer.
        //
        // The reason is that §4.3 point 2 has two halves and only the first is
        // built: subscribe is authorized here, but the stream is not yet
        // tenant-filtered server-side, because the SSE payload is upstream's
        // own and carries no tenant to filter on. Returning `StreamSubscribe`
        // would therefore let a *Viewer of one tenant* receive every other
        // tenant's recorded requests — bodies, not just existence. That is a
        // worse leak than the 403-vs-404 oracle this slice exists to close, and
        // it is precisely what §4.3 warns about: "filtering in the client would
        // mean the server had already sent another tenant's events."
        //
        // So the gate fails closed: only a `FleetAdmin` — who is entitled to
        // every tenant's data anyway, and therefore leaks nothing by receiving
        // an unfiltered stream — may subscribe. `Action::StreamSubscribe`
        // remains the correct action and stays in the table; #163 restores this
        // arm to it in the same change that adds server-side filtering, at
        // which point Viewers regain the route.
        return Action::ClusterAdmin;
    }
    if action == actions::SYSTEM_READ
        || action == actions::SYSTEM_WRITE
        || action == actions::INTERCEPT_READ
        || action == actions::INTERCEPT_WRITE
    {
        // Fleet-level surfaces with no per-tenant meaning (config/metrics/
        // logs/reload, TLS-intercept control) and no action of their own in
        // RFC-002 §4.1's closed list, which predates them: gated at
        // `ClusterAdmin`, the one action reserved to `FleetAdmin`, rather than
        // left unauthorized just because nobody has named a finer action yet.
        return Action::ClusterAdmin;
    }
    if action == actions::IMPOSTER_READ {
        return Action::ImposterRead;
    }
    if action == actions::IMPOSTER_WRITE {
        if has_scenario_param {
            return Action::ScenarioWrite;
        }
        if has_space {
            // Covers both `POST .../spaces/:id/stubs` (redefining a space's
            // stubs) and `PUT .../flow-state/:id/:key` (setting a flow-state
            // value): both redefine behaviour rather than merely disturbing
            // it, so both sit at the Editor tier.
            return Action::SpaceStubWrite;
        }
        // The only `imposter.write` route reaching this mapping with neither
        // param is `POST .../scenarios/reset` — every other write (imposter
        // and stub CRUD, enable/disable) terminates in `admin_front` before
        // ever reaching upstream's classifier.
        return Action::ScenarioReset;
    }
    if action == actions::IMPOSTER_DELETE {
        if is_flow_state {
            return Action::FlowStateClear;
        }
        if has_space {
            return Action::SpaceTeardown;
        }
        // `DELETE .../savedRequests`, `.../requests` and
        // `.../savedProxyResponses` all land here with neither a space nor a
        // flow-state prefix; RFC-002 §4.1 has one action for all three.
        return Action::SavedRequestsClear;
    }

    // An upstream action string this mapping does not recognize: fail closed
    // onto the most restrictive action (FleetAdmin-only) rather than guess.
    Action::ClusterAdmin
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_fold_onto_imposter_read_regardless_of_route() {
        for (space, scenario) in [(false, false), (true, false), (false, true)] {
            assert_eq!(
                map_action(actions::IMPOSTER_READ, false, space, scenario),
                Action::ImposterRead
            );
        }
    }

    #[test]
    fn scenario_state_write_is_distinguished_by_the_scenario_param() {
        assert_eq!(
            map_action(actions::IMPOSTER_WRITE, false, false, true),
            Action::ScenarioWrite
        );
    }

    #[test]
    fn space_stub_write_is_distinguished_by_the_space_field() {
        assert_eq!(
            map_action(actions::IMPOSTER_WRITE, false, true, false),
            Action::SpaceStubWrite
        );
    }

    #[test]
    fn scenarios_reset_is_the_write_with_neither_signal() {
        assert_eq!(
            map_action(actions::IMPOSTER_WRITE, false, false, false),
            Action::ScenarioReset
        );
    }

    #[test]
    fn deletes_split_by_flow_state_and_space() {
        assert_eq!(
            map_action(actions::IMPOSTER_DELETE, true, true, false),
            Action::FlowStateClear,
            "flow-state wins over space when both are set"
        );
        assert_eq!(
            map_action(actions::IMPOSTER_DELETE, false, true, false),
            Action::SpaceTeardown
        );
        assert_eq!(
            map_action(actions::IMPOSTER_DELETE, false, false, false),
            Action::SavedRequestsClear
        );
    }

    #[test]
    fn verify_events_and_system_actions_map_as_documented() {
        assert_eq!(
            map_action(actions::IMPOSTER_VERIFY, false, false, false),
            Action::VerifyRun
        );
        assert_eq!(
            map_action(actions::EVENTS_READ, false, false, false),
            Action::ClusterAdmin,
            "the /events stream is not yet tenant-filtered, so it carries every \
             tenant's recorded requests and must require fleet-wide authority; \
             #163 restores this to StreamSubscribe when it adds filtering"
        );
        for action in [
            actions::SYSTEM_READ,
            actions::SYSTEM_WRITE,
            actions::INTERCEPT_READ,
            actions::INTERCEPT_WRITE,
        ] {
            assert_eq!(
                map_action(action, false, false, false),
                Action::ClusterAdmin
            );
        }
    }

    /// The `/events` gate is stricter than the role table on purpose, and the
    /// gap between them is the thing to protect.
    ///
    /// `Role::Viewer` grants `StreamSubscribe` (RFC-002 §4.2) and that is
    /// correct — but the *route* cannot be served at that level until the
    /// stream is tenant-filtered, because it carries every tenant's recorded
    /// requests. This asserts the two facts together so nobody "fixes" the
    /// mapping back to `StreamSubscribe` by reading the role table alone and
    /// concluding the gate is wrong.
    #[test]
    fn the_events_route_outranks_the_stream_subscribe_grant_until_filtering_lands() {
        use crate::authz::role_allows;

        assert!(
            role_allows(Role::Viewer, Action::StreamSubscribe),
            "the table is right: a viewer may subscribe to its own tenant's stream"
        );
        assert!(
            !role_allows(
                Role::Viewer,
                map_action(actions::EVENTS_READ, false, false, false)
            ),
            "but the unfiltered /events route must be out of a viewer's reach"
        );
        assert!(
            !role_allows(
                Role::TenantAdmin,
                map_action(actions::EVENTS_READ, false, false, false)
            ),
            "and out of a tenant admin's — the leak is cross-tenant, so tenant-scoped \
             authority is exactly what must not open it"
        );
        assert!(
            role_allows(
                Role::FleetAdmin,
                map_action(actions::EVENTS_READ, false, false, false)
            ),
            "a fleet admin is entitled to every tenant's data, so an unfiltered stream \
             leaks nothing to it"
        );
    }

    #[test]
    fn an_unrecognized_action_string_fails_closed() {
        assert_eq!(
            map_action("something.new", false, false, false),
            Action::ClusterAdmin,
            "a classifier that cannot classify must treat the request as the dangerous class"
        );
    }
}
