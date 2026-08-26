//! RBAC evaluation for the clustered admin plane (RFC-002 §4, issue #161).
//!
//! This module is **pure**: it holds the closed [`Action`] set, the
//! `Role → Action` grant table, and an evaluator that turns
//! `(bindings, action, tenant)` into a [`Decision`]. It reads no state, does no
//! I/O and knows nothing about HTTP, which is what makes the whole authorization
//! matrix unit-testable without standing up a cluster.
//!
//! # Why one evaluator and two call sites
//!
//! The front door splits admin traffic in two, and each half misses the other's
//! gate:
//!
//! - **Terminated** requests (writes that become `ControlOp`s) never reach
//!   upstream's admin gate at all — they are answered by `admin_front`.
//! - **Proxied** requests (everything else) never reach `admin_front`'s own
//!   check — they are forwarded to the loopback core admin.
//!
//! So enforcement is installed twice: on the loopback admin via
//! `ServerBuilder::admin_authorizer`, and in `admin_front` for the terminated
//! routes. Both consult *this* evaluator. Enforcing in one place only leaves the
//! other wide open, which RFC-002 §4.3 means by "all of them, or the boundary is
//! decorative" — and is the single most likely way this slice ships broken.
//!
//! # Deny by default
//!
//! [`decide`] returns [`Decision::Deny`] unless a binding positively grants the
//! action. Roles are purely additive (RFC-002 §4), so there is no deny-override
//! rule and none is provided — adding one later would change the evaluation
//! semantics for every existing binding, so it is a deliberate closed door.

use rift_cluster::TenantId;
use rift_cluster::control::{FLEET_SCOPE, Role};

/// Every authorizable operation on the admin plane (RFC-002 §4.1).
///
/// **Closed on purpose.** An open action set means a new route can be added
/// without anyone deciding who may call it, which is how authorization gaps
/// appear one convenience at a time. Adding a route means adding a variant
/// here — and because [`crate::admin_front`] maps its `Terminated` variants to
/// these with a wildcard-free `match`, forgetting to is a compile error rather
/// than a silently unauthorized route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Action {
    ImposterRead,
    ImposterWrite,
    ImposterDelete,
    StubWrite,
    LifecycleToggle,
    SavedRequestsRead,
    SavedRequestsClear,
    ScenarioRead,
    ScenarioWrite,
    ScenarioReset,
    SpaceStubWrite,
    SpaceTeardown,
    FlowStateRead,
    FlowStateClear,
    /// Ask the server to send a sample request to one of this tenant's
    /// imposters (issue #335).
    ///
    /// Its own variant rather than a ride on `ImposterRead`, for two reasons.
    /// It is not a read in *effect* — a try advances scenario state, appends to
    /// the request log and can trigger proxyOnce recording, which is the
    /// "disturb" shape that puts it in the Operator arm beside
    /// [`Action::ScenarioReset`]. And it is the first action under which the
    /// server originates outbound HTTP on a caller's behalf, so an audit record
    /// (`as_str`) has to be able to say that is what happened rather than
    /// naming it as an ordinary read.
    ImposterTry,
    SourceRead,
    VerifyRun,
    StreamSubscribe,
    TenantManage,
    AuditRead,
    ClusterAdmin,
    /// List, read, or dry-run compile a spec (RFC-004 §4.3, issue #278) — landed with S2 because a
    /// terminated route cannot ship without its action.
    SpecRead,
    /// Import (`PUT /specs/{id}`) or deploy a spec.
    ///
    /// Deploy additionally requires [`Action::ImposterWrite`] on the target port — checked in
    /// `admin_front::terminate_spec_deploy`, not here, because the port lives in the request body
    /// rather than the route. `SpecWrite` alone must not become a back door into imposter
    /// mutation just because every role that holds it today also holds `ImposterWrite`.
    SpecWrite,
    /// `DELETE /specs/{id}`.
    SpecDelete,
    /// Read a dataset's listing, its version history, or its bytes (RFC-005 §5, #287).
    DatasetRead,
    /// Upload a new version of a dataset. Editor-tier: a new table of rows redefines what every
    /// stub bound to that dataset answers.
    DatasetWrite,
    /// Delete a dataset. Editor-tier for the same reason, and refused outright while bound.
    DatasetDelete,
}

impl Action {
    /// Every action, for exhaustive matrix tests and for the `--help`-style
    /// listings #162 will want.
    ///
    /// Kept beside the enum so the two cannot drift: `every_action_is_listed`
    /// fails if a variant is added without extending this.
    pub const ALL: [Action; 27] = [
        Action::ImposterRead,
        Action::ImposterWrite,
        Action::ImposterDelete,
        Action::StubWrite,
        Action::LifecycleToggle,
        Action::SavedRequestsRead,
        Action::SavedRequestsClear,
        Action::ScenarioRead,
        Action::ScenarioWrite,
        Action::ScenarioReset,
        Action::SpaceStubWrite,
        Action::SpaceTeardown,
        Action::FlowStateRead,
        Action::FlowStateClear,
        Action::ImposterTry,
        Action::SourceRead,
        Action::VerifyRun,
        Action::StreamSubscribe,
        Action::TenantManage,
        Action::AuditRead,
        Action::ClusterAdmin,
        Action::SpecRead,
        Action::SpecWrite,
        Action::SpecDelete,
        Action::DatasetRead,
        Action::DatasetWrite,
        Action::DatasetDelete,
    ];

    /// A stable string for audit records and logs (#163 consumes these).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Action::ImposterRead => "imposter.read",
            Action::ImposterWrite => "imposter.write",
            Action::ImposterDelete => "imposter.delete",
            Action::StubWrite => "stub.write",
            Action::LifecycleToggle => "lifecycle.toggle",
            Action::SavedRequestsRead => "savedRequests.read",
            Action::SavedRequestsClear => "savedRequests.clear",
            Action::ScenarioRead => "scenario.read",
            Action::ScenarioWrite => "scenario.write",
            Action::ScenarioReset => "scenario.reset",
            Action::SpaceStubWrite => "space.stubWrite",
            Action::SpaceTeardown => "space.teardown",
            Action::FlowStateRead => "flowState.read",
            Action::FlowStateClear => "flowState.clear",
            Action::ImposterTry => "imposter.try",
            Action::SourceRead => "source.read",
            Action::VerifyRun => "verify.run",
            Action::StreamSubscribe => "stream.subscribe",
            Action::TenantManage => "tenant.manage",
            Action::AuditRead => "audit.read",
            Action::ClusterAdmin => "cluster.admin",
            Action::SpecRead => "spec.read",
            Action::SpecWrite => "spec.write",
            Action::SpecDelete => "spec.delete",
            // `write`, not `put`, and not by accident: an action's RBAC string and its audit
            // string are one namespace, and #285 already ships
            // `ControlOp::DatasetPut.audit_action() == "dataset.write"`. Issue #287's prose asks
            // for `"dataset.put"`, which would make this the only action in the system that
            // disagrees with its own audit string — on the resource whose whole point here is
            // that access to it be answerable as a log query.
            Action::DatasetRead => "dataset.read",
            Action::DatasetWrite => "dataset.write",
            Action::DatasetDelete => "dataset.delete",
        }
    }
}

/// Does `role` grant `action`? The RFC-002 §4.2 table, and the only place it
/// exists.
///
/// The ladder is a strict superset chain — Viewer ⊂ Operator ⊂ Editor ⊂
/// TenantAdmin ⊂ FleetAdmin — because roles are purely additive. Written as
/// explicit per-role arms rather than as a numeric rank comparison: a rank
/// makes "is X at least Y" cheap but makes *which* actions a role grants
/// invisible, and this table is the thing a security reviewer reads.
///
/// Two deliberate placements worth stating, both from §4.1:
///
/// - **`AuditRead` is not a Viewer grant**, despite being a read. Reading who
///   did what and changing who may do what are different powers; it starts at
///   `TenantAdmin`. Bundling it with the other reads would make every viewer an
///   auditor by accident.
/// - **`AuditRead` is not part of `TenantManage`** either, for the mirror
///   reason: collapsing them would make every principal-manager an auditor.
#[must_use]
pub fn role_allows(role: Role, action: Action) -> bool {
    // The `matches!` arms are exhaustive over `Action` by construction: each
    // lists the actions that role adds, and the fallthrough chains to the
    // weaker role. A new `Action` variant is denied by every role until it is
    // placed here, which is the correct default for an action nobody has
    // decided about yet.
    match role {
        Role::Viewer => matches!(
            action,
            Action::ImposterRead
                | Action::SavedRequestsRead
                | Action::ScenarioRead
                | Action::FlowStateRead
                // A source declaration is config an imposter was built from,
                // not a credential: `auth_ref` is a name (control.rs refuses
                // URIs carrying secrets), so reading it sits with the other
                // Viewer reads. *Writing* one is a different power and gets
                // its own action when the write surface ships (#239 scoped
                // reads only).
                | Action::SourceRead
                | Action::StreamSubscribe
                // Reading a spec (listing, fetching, or dry-run compiling it) is the same power as
                // reading the imposter it describes — a Viewer may see what a mock was built from.
                | Action::SpecRead
                // RFC-005 §5 (issue #287): a dataset is the table of rows a bound stub serves, so
                // reading one is the same power as reading the stub it feeds. Note this covers the
                // *content* read too — which is a bulk export, and is why that one read is the
                // single named exception to reads-are-not-audited (RFC-002 §9).
                | Action::DatasetRead
        ),
        Role::Operator => {
            role_allows(Role::Viewer, action)
                || matches!(
                    action,
                    Action::LifecycleToggle
                        | Action::SavedRequestsClear
                        | Action::ScenarioReset
                        | Action::SpaceTeardown
                        | Action::FlowStateClear
                        | Action::ImposterTry
                )
        }
        Role::Editor => {
            role_allows(Role::Operator, action)
                || matches!(
                    action,
                    Action::ImposterWrite
                        | Action::ImposterDelete
                        | Action::StubWrite
                        | Action::SpaceStubWrite
                        | Action::ScenarioWrite
                        | Action::VerifyRun
                        // RFC-004 §4.3 (issue #278): importing or deploying a spec redefines what
                        // an imposter answers, which is the Operator/Editor line — and `SpecWrite`
                        // alone must not be a back door into imposter mutation, so `deploy` also
                        // requires `ImposterWrite` on the target port (checked in the front, not
                        // here).
                        | Action::SpecWrite
                        | Action::SpecDelete
                        // RFC-005 §5 (issue #287): a dataset is the table of rows a bound stub
                        // serves, so replacing or removing one redefines what that stub answers —
                        // the same Operator/Editor line `SpecWrite` sits on. Reading one is a
                        // Viewer's business.
                        | Action::DatasetWrite
                        | Action::DatasetDelete
                )
        }
        Role::TenantAdmin => {
            role_allows(Role::Editor, action)
                || matches!(action, Action::TenantManage | Action::AuditRead)
        }
        // Everything, in every tenant, plus the cluster surface. The tenant
        // half is `decide`'s job, not this table's: this answers only "may this
        // role do this thing".
        Role::FleetAdmin => true,
    }
}

/// One principal's binding set: which role it holds in which tenant.
///
/// A `FleetAdmin` binding always names [`FLEET_SCOPE`] (`"*"`) — `BindingPut`
/// refuses it anywhere else, and refuses every other role there (#159).
pub type Bindings = [(TenantId, Role)];

/// Why a request was refused, and — the part that matters — **which status it
/// renders as** (RFC-002 §8.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Denial {
    /// No credential, or one that authenticates nobody. `401`.
    Unauthenticated,
    /// The principal holds no binding in the requested tenant. **`404`**, not
    /// `403`, and the body must be byte-identical to a genuine not-found:
    /// telling an outsider "that exists but is not yours" makes the API an
    /// oracle for other tenants' port numbers.
    NotBoundToTenant,
    /// Bound to the tenant, but the role does not grant the action. `403` —
    /// safe, because the caller already knows the tenant exists; they are in it.
    InsufficientRole { role: Role, action: Action },
}

/// The outcome of an authorization decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Allowed, and the tenant the request is scoped to. Callers **must** use
    /// this tenant for the operation rather than re-reading the request header:
    /// see [`decide`]'s note on the create path.
    Allow {
        tenant: TenantId,
    },
    Deny(Denial),
}

impl Decision {
    #[must_use]
    pub fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allow { .. })
    }
}

/// The evaluator. Pure over its inputs.
///
/// `requested` is the tenant the caller asked for — from `X-Rift-Tenant`, or the
/// tenant that already owns the addressed resource. The header **selects among
/// existing bindings; it never grants one** (RFC-002 §8.1), which is why this
/// takes an already-resolved binding set: the caller must authenticate first,
/// load *that principal's* bindings, and only then intersect with `requested`.
/// Reading the header before authentication is the confused-deputy bug in full.
///
/// # The create path
///
/// A new imposter has no prior owner, so for a create the header decides which
/// tenant *acquires* the resource. The caller must use the [`Decision::Allow`]
/// tenant returned here for **both** the authority check and the ownership
/// record — one binding, used twice. Checking against the principal's default
/// binding while recording ownership from the header is what would let an
/// Editor of A create resources owned by B.
///
/// # FleetAdmin
///
/// A `FleetAdmin` binding grants every action in *every* tenant, so it matches
/// regardless of `requested` — that is what fleet-wide means. It is the only
/// role whose binding tenant (`"*"`) does not have to equal the requested one.
#[must_use]
pub fn decide(bindings: &Bindings, action: Action, requested: &TenantId) -> Decision {
    // Fleet privilege first: it is not scoped to `requested`, so a fleet admin
    // must not fall through to the not-bound branch for a tenant it holds no
    // per-tenant binding in.
    if bindings
        .iter()
        .any(|(tenant, role)| *role == Role::FleetAdmin && tenant.as_str() == FLEET_SCOPE)
    {
        return Decision::Allow {
            tenant: requested.clone(),
        };
    }

    // `ClusterAdmin` is FleetAdmin-only by construction: no other role grants
    // it, so an ordinary binding can never reach an allow for it below.
    let Some((_, role)) = bindings.iter().find(|(tenant, _)| tenant == requested) else {
        return Decision::Deny(Denial::NotBoundToTenant);
    };

    if role_allows(*role, action) {
        Decision::Allow {
            tenant: requested.clone(),
        }
    } else {
        Decision::Deny(Denial::InsufficientRole {
            role: *role,
            action,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(id: &str) -> TenantId {
        TenantId::new(id)
    }

    fn bound(id: &str, role: Role) -> Vec<(TenantId, Role)> {
        vec![(tenant(id), role)]
    }

    /// `Action::ALL` must actually be all of them.
    ///
    /// The matrix test below iterates `ALL`, so a variant missing from it would
    /// silently drop a whole column — the authorization equivalent of a test
    /// that passes because it never ran.
    #[test]
    fn every_action_is_listed_in_all() {
        assert_eq!(
            Action::ALL.len(),
            27,
            "RFC-002 §4.1 defines 21 actions, RFC-004 §4.3 adds three (issue #278) and RFC-005 §5 \
             adds three more (issue #287); ALL must carry every one"
        );
        let unique: std::collections::BTreeSet<_> = Action::ALL.iter().collect();
        assert_eq!(unique.len(), Action::ALL.len(), "ALL contains a duplicate");
        let slugs: std::collections::BTreeSet<_> = Action::ALL.iter().map(|a| a.as_str()).collect();
        assert_eq!(
            slugs.len(),
            Action::ALL.len(),
            "two actions share an audit slug — #163 could not tell them apart"
        );
    }

    /// The spec actions' audit slugs are part of the contract (#278): the audit stream and
    /// `role {r:?} does not grant {slug}` refusals both name them.
    #[test]
    fn spec_action_slugs_are_stable() {
        assert_eq!(Action::SpecRead.as_str(), "spec.read");
        assert_eq!(Action::SpecWrite.as_str(), "spec.write");
        assert_eq!(Action::SpecDelete.as_str(), "spec.delete");
    }

    /// The whole of RFC-002 §4.2, asserted cell by cell.
    ///
    /// Written as an explicit expected-grant list per role rather than derived
    /// from `role_allows`, because a table generated from the implementation
    /// tests only that the implementation equals itself.
    #[test]
    fn the_role_action_matrix_matches_rfc_002_section_4_2() {
        let viewer = [
            Action::ImposterRead,
            Action::SavedRequestsRead,
            Action::ScenarioRead,
            Action::FlowStateRead,
            Action::SourceRead,
            Action::SpecRead,
            Action::DatasetRead,
            Action::StreamSubscribe,
        ];
        let operator_adds = [
            Action::LifecycleToggle,
            Action::SavedRequestsClear,
            Action::ScenarioReset,
            Action::SpaceTeardown,
            Action::FlowStateClear,
            Action::ImposterTry,
        ];
        let editor_adds = [
            Action::ImposterWrite,
            Action::ImposterDelete,
            Action::StubWrite,
            Action::SpaceStubWrite,
            Action::ScenarioWrite,
            Action::VerifyRun,
            // RFC-004 §4.3 (issue #278): importing or deploying a spec redefines what an
            // imposter answers, which is the Operator/Editor line — and `SpecWrite` alone
            // must not be a back door into imposter mutation, so `deploy` also requires
            // `ImposterWrite` on the target port (checked in the front, not here).
            Action::SpecWrite,
            Action::SpecDelete,
            Action::DatasetWrite,
            Action::DatasetDelete,
        ];
        let tenant_admin_adds = [Action::TenantManage, Action::AuditRead];

        let expected = |role: Role| -> Vec<Action> {
            let mut granted: Vec<Action> = match role {
                Role::Viewer => viewer.to_vec(),
                Role::Operator => viewer.iter().chain(&operator_adds).copied().collect(),
                Role::Editor => viewer
                    .iter()
                    .chain(&operator_adds)
                    .chain(&editor_adds)
                    .copied()
                    .collect(),
                Role::TenantAdmin => viewer
                    .iter()
                    .chain(&operator_adds)
                    .chain(&editor_adds)
                    .chain(&tenant_admin_adds)
                    .copied()
                    .collect(),
                Role::FleetAdmin => Action::ALL.to_vec(),
            };
            granted.sort_unstable();
            granted
        };

        for role in [
            Role::Viewer,
            Role::Operator,
            Role::Editor,
            Role::TenantAdmin,
            Role::FleetAdmin,
        ] {
            let want = expected(role);
            for action in Action::ALL {
                let got = role_allows(role, action);
                assert_eq!(
                    got,
                    want.contains(&action),
                    "role {role:?} × action {action:?}: expected {}, got {got}",
                    want.contains(&action)
                );
            }
        }
    }

    /// The ladder is a strict superset chain, and each step actually adds
    /// something. A role that granted nothing new would be a modelling error
    /// the per-cell test above cannot see.
    #[test]
    fn roles_are_purely_additive_and_each_step_widens() {
        let granted = |role: Role| -> std::collections::BTreeSet<Action> {
            Action::ALL
                .into_iter()
                .filter(|a| role_allows(role, *a))
                .collect()
        };
        let ladder = [
            Role::Viewer,
            Role::Operator,
            Role::Editor,
            Role::TenantAdmin,
            Role::FleetAdmin,
        ];
        for pair in ladder.windows(2) {
            let (weaker, stronger) = (granted(pair[0]), granted(pair[1]));
            assert!(
                weaker.is_subset(&stronger),
                "{:?} must grant everything {:?} does — roles are additive",
                pair[1],
                pair[0]
            );
            assert!(
                stronger.len() > weaker.len(),
                "{:?} adds nothing over {:?}",
                pair[1],
                pair[0]
            );
        }
        assert_eq!(
            granted(Role::FleetAdmin).len(),
            Action::ALL.len(),
            "FleetAdmin must grant every action"
        );
    }

    /// `ClusterAdmin` is reachable only through a fleet binding. If any
    /// tenant-scoped role granted it, `/_cluster/*` would be reachable by a
    /// tenant's own admin.
    #[test]
    fn cluster_admin_is_fleet_only() {
        for role in [
            Role::Viewer,
            Role::Operator,
            Role::Editor,
            Role::TenantAdmin,
        ] {
            assert!(
                !role_allows(role, Action::ClusterAdmin),
                "{role:?} must not grant ClusterAdmin"
            );
        }
        assert!(role_allows(Role::FleetAdmin, Action::ClusterAdmin));
    }

    /// Audit is not a Viewer read and not part of TenantManage — the two
    /// placements §4.1 calls out explicitly.
    #[test]
    fn audit_read_is_neither_an_ordinary_read_nor_bundled_with_tenant_manage() {
        assert!(!role_allows(Role::Viewer, Action::AuditRead));
        assert!(!role_allows(Role::Operator, Action::AuditRead));
        assert!(!role_allows(Role::Editor, Action::AuditRead));
        assert!(role_allows(Role::TenantAdmin, Action::AuditRead));
        assert!(role_allows(Role::FleetAdmin, Action::AuditRead));
    }

    /// Issue #335: a try is an Operator power, not a Viewer one.
    ///
    /// Pinned on its own rather than left to the matrix above, because the
    /// pressure to move it is real and specific: the affordance is most useful
    /// to a Viewer diagnosing why a stub will not match, and the sibling
    /// `Copy curl` button (#334) *is* offered to every role. The line being
    /// held is that a Viewer may compose a request, but may not have the
    /// **server originate one on their behalf** — a try mutates scenario state,
    /// the request log, and proxy recordings. Widening this to Viewer is a
    /// deliberate decision to be argued, not a refactor to be made quietly.
    #[test]
    fn a_try_is_an_operator_power_not_a_viewer_read() {
        assert!(
            !role_allows(Role::Viewer, Action::ImposterTry),
            "a Viewer gets the curl button (#334), not a server-originated send"
        );
        for role in [
            Role::Operator,
            Role::Editor,
            Role::TenantAdmin,
            Role::FleetAdmin,
        ] {
            assert!(
                role_allows(role, Action::ImposterTry),
                "{role:?} must be able to try a stub"
            );
        }
        assert_eq!(
            Action::ImposterTry.as_str(),
            "imposter.try",
            "the audit slug is a published contract (#163 consumes it)"
        );
    }

    /// Deny by default: an unbound principal is refused everything, and the
    /// refusal is `NotBoundToTenant` (→ 404), never `InsufficientRole` (→ 403).
    /// A 403 here would confirm the tenant exists to someone outside it.
    #[test]
    fn an_unbound_principal_is_denied_every_action_as_not_found() {
        for action in Action::ALL {
            assert_eq!(
                decide(&[], action, &tenant("acme")),
                Decision::Deny(Denial::NotBoundToTenant),
                "{action:?} must be denied to a principal with no bindings"
            );
        }
    }

    /// A binding in another tenant does not carry over. This is the cross-tenant
    /// case, and it must read as not-found rather than forbidden.
    #[test]
    fn a_binding_in_one_tenant_grants_nothing_in_another() {
        let bindings = bound("acme", Role::TenantAdmin);
        assert_eq!(
            decide(&bindings, Action::ImposterRead, &tenant("globex")),
            Decision::Deny(Denial::NotBoundToTenant),
            "even a tenant admin of acme is a stranger to globex"
        );
        assert!(decide(&bindings, Action::ImposterRead, &tenant("acme")).is_allowed());
    }

    /// In-tenant but under-privileged is `403`, and it names the role and
    /// action so the refusal is actionable to someone who legitimately is in
    /// the tenant.
    #[test]
    fn insufficient_role_inside_the_tenant_is_distinguishable_from_not_being_in_it() {
        let bindings = bound("acme", Role::Viewer);
        assert_eq!(
            decide(&bindings, Action::ImposterWrite, &tenant("acme")),
            Decision::Deny(Denial::InsufficientRole {
                role: Role::Viewer,
                action: Action::ImposterWrite,
            })
        );
    }

    /// A fleet admin is allowed in every tenant, including ones it holds no
    /// per-tenant binding in — and must not fall through to the not-bound
    /// branch on the way.
    #[test]
    fn fleet_admin_is_allowed_in_every_tenant_including_unknown_ones() {
        let bindings = vec![(TenantId::new(FLEET_SCOPE), Role::FleetAdmin)];
        for action in Action::ALL {
            for t in ["acme", "globex", "never-created"] {
                assert_eq!(
                    decide(&bindings, action, &tenant(t)),
                    Decision::Allow { tenant: tenant(t) },
                    "fleet admin must be allowed {action:?} in {t}"
                );
            }
        }
    }

    /// The allowed tenant echoes what was requested, which is what the create
    /// path records as the owner. If this returned the *binding's* tenant
    /// instead, an Editor of A creating with `X-Rift-Tenant: A` would be fine,
    /// but a fleet admin creating into B would silently record `"*"` as owner.
    #[test]
    fn the_decision_carries_the_requested_tenant_for_the_create_path() {
        let fleet = vec![(TenantId::new(FLEET_SCOPE), Role::FleetAdmin)];
        assert_eq!(
            decide(&fleet, Action::ImposterWrite, &tenant("globex")),
            Decision::Allow {
                tenant: tenant("globex")
            },
            "the owner recorded must be the tenant asked for, never the binding's scope"
        );

        let editor = bound("acme", Role::Editor);
        assert_eq!(
            decide(&editor, Action::ImposterWrite, &tenant("acme")),
            Decision::Allow {
                tenant: tenant("acme")
            }
        );
        // And an Editor of A cannot acquire into B — the §8.1 create test, at
        // the evaluator level.
        assert_eq!(
            decide(&editor, Action::ImposterWrite, &tenant("globex")),
            Decision::Deny(Denial::NotBoundToTenant)
        );
    }

    /// A `FleetAdmin` role recorded against an ordinary tenant must not confer
    /// fleet privilege. `BindingPut` refuses to write such a row (#159), so this
    /// asserts the evaluator does not *also* have to be trusted to reject it —
    /// defence in depth for a row that could only arrive from a hand-edited
    /// state dir or a future bug.
    #[test]
    fn a_fleet_admin_role_bound_to_an_ordinary_tenant_is_not_fleet_wide() {
        let bogus = bound("acme", Role::FleetAdmin);
        assert_eq!(
            decide(&bogus, Action::ClusterAdmin, &tenant("globex")),
            Decision::Deny(Denial::NotBoundToTenant),
            "fleet privilege must come from a binding on the reserved scope, not from the role alone"
        );
        // In its own tenant it still behaves as the role says, which is the
        // conservative reading: the row is malformed, not a reason to panic.
        assert!(decide(&bogus, Action::ImposterWrite, &tenant("acme")).is_allowed());
    }

    /// The three dataset actions sit on the read/disturb/redefine ladder where #287 puts them.
    ///
    /// Asserted per-role rather than through the big matrix so the *reason* survives: a dataset is
    /// a table of rows that stubs serve, so reading one is a Viewer's business, and replacing or
    /// removing one redefines what every bound stub answers — an Editor's.
    #[test]
    fn a_viewer_reads_datasets_but_only_an_editor_redefines_them() {
        assert!(role_allows(Role::Viewer, Action::DatasetRead));
        assert!(!role_allows(Role::Viewer, Action::DatasetWrite));
        assert!(!role_allows(Role::Viewer, Action::DatasetDelete));

        for action in [
            Action::DatasetRead,
            Action::DatasetWrite,
            Action::DatasetDelete,
        ] {
            assert!(role_allows(Role::Editor, action), "{action:?}");
        }
    }

    /// An action's RBAC string and its audit string are **one namespace**, and the verb is `write`.
    ///
    /// Every other resource holds this: `Action::SpecWrite` is `"spec.write"` and
    /// `ControlOp::SpecPut`/`SpecBind`/`SpecUnbind` all audit as `"spec.write"`. #285 already ships
    /// `ControlOp::DatasetPut.audit_action() == "dataset.write"`, so an `Action` spelled
    /// `"dataset.put"` — as issue #287's prose asks for — would be the only action in the system
    /// that disagrees with its own audit string, for the very resource whose point is that access
    /// to it be answerable as a log query.
    #[test]
    fn dataset_action_strings_match_the_audit_namespace() {
        assert_eq!(Action::DatasetRead.as_str(), "dataset.read");
        assert_eq!(Action::DatasetWrite.as_str(), "dataset.write");
        assert_eq!(Action::DatasetDelete.as_str(), "dataset.delete");

        assert_eq!(
            Action::DatasetWrite.as_str(),
            rift_cluster::ControlOp::DatasetPut {
                tenant: rift_cluster::TenantId::default(),
                record: dataset_record(),
                csv: Some("id\n1\n".to_owned()),
                origin: 0,
            }
            .audit_action()
            .expect("a dataset write is audited"),
            "the RBAC action and the audit action for the same operation must be the same string"
        );
    }

    fn dataset_record() -> rift_cluster::control::DatasetRecord {
        rift_cluster::control::DatasetRecord {
            name: "customers".to_owned(),
            digest: rift_cluster::control::Digest::new("0".repeat(64)),
            key_columns: vec!["id".to_owned()],
            delimiter: ',',
            columns: vec!["id".to_owned()],
            rows: 1,
            bytes: 5,
        }
    }

    /// All three join `Action::ALL`, which is what makes the role matrix exhaustive.
    #[test]
    fn every_dataset_action_is_in_action_all() {
        for action in [
            Action::DatasetRead,
            Action::DatasetWrite,
            Action::DatasetDelete,
        ] {
            assert!(Action::ALL.contains(&action), "{action:?} missing from ALL");
        }
    }
}
