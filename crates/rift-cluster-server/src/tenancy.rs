//! RFC-002 §5's tenancy admin surface (issue #162): tenants, principals,
//! bindings, and `whoami`.
//!
//! Every route here **terminates at the front door**, reads included. There is
//! no upstream `/admin/tenants` to proxy to — these records live only in the
//! clustered control plane — which makes this the same shape as
//! `GET /front-door/routes` (issue #131), the existing precedent for a read
//! that terminates in a `classify` otherwise made of writes.
//!
//! # Where the tenant comes from
//!
//! From the **path**, not `X-Rift-Tenant`. On a resource route the header
//! selects which of the caller's bindings they are acting under; here the
//! tenant is the record being administered. `admin_front::scope_for` is what
//! carries that distinction to `authorize_action`, and its doc says what goes
//! wrong without it.
//!
//! # Keys are shown once
//!
//! [`dispatch`]'s [`Route::PrincipalCreate`] arm mints the key, hashes it, and
//! returns the raw value in that one response. The control plane stores only
//! the argon2id hash and the
//! SHA-256 fingerprint the id is built from, neither of which can reproduce
//! it — so there is nothing for a later `GET` to leak, and the acceptance test
//! scans the on-disk redb bytes to prove it.

use hyper::{Method, StatusCode};
use rift_cluster::audit_export::{ExportStatus, ExportStatusSnapshot};
use rift_cluster::control::{
    AuthSource, FLEET_SCOPE, Principal, PrincipalId, Quotas, Role, Tenant, api_key_principal_id,
    generate_api_key, hash_api_key,
};
use rift_cluster::{ControlOp, ControlRequest, DEFAULT_AUDIT_BATCH_MAX_ROWS, RaftNode, TenantId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::authz::Action;
use crate::principal::Resolved;

/// The one route with no action to authorize (RFC-002 §4.1).
pub(crate) const WHOAMI_PATH: &str = "/admin/whoami";

const AUDIT_PATH: &str = "/admin/audit";

/// Where the fleet's audit export sink (issue #164) is declared and read.
///
/// A prefix of neither `AUDIT_PATH` nor `TENANTS_PATH`, but checked ahead of
/// both in [`classify`] anyway, defensively: `AUDIT_PATH` is matched by exact
/// equality today, so `"/admin/audit/sink"` cannot already fall into
/// [`Route::AuditRead`] by accident — but a later change to that comparison
/// (a prefix match, say) must not be able to silently swallow this path and
/// hand a `TenantAdmin` the fleet's sink under an `AuditRead` authorization.
const AUDIT_SINK_PATH: &str = "/admin/audit/sink";

/// Where the fleet's operator-set name (issue #373) is written.
///
/// Checked with the other exact-path routes, ahead of the `/admin/tenants/`
/// prefix strip below, for the same defensive reason [`AUDIT_SINK_PATH`] is:
/// this path must never be reachable through a broader matcher's
/// authorization tier by accident.
const FLEET_NAME_PATH: &str = "/admin/fleet/name";

/// Rows returned when the caller names no `limit`, and the ceiling on what they
/// may ask for. A bound rather than an option: the audit table is a journal, so
/// an unbounded read is an unbounded response, and the endpoint that answers it
/// is one any tenant admin can reach.
const AUDIT_DEFAULT_LIMIT: usize = 500;
const AUDIT_MAX_LIMIT: usize = 5_000;

const TENANTS_PATH: &str = "/admin/tenants";
const TENANTS_PREFIX: &str = "/admin/tenants/";

/// One classified tenancy route. Carries the tenant it addresses so that
/// [`Route::scope`] and [`Route::action`] can both be answered without
/// re-parsing the path — the second parse is where the two would drift.
/// The test-only `EnumDiscriminants` derive backs the route-parity gate's variant-coverage check
/// (issue #184) — see the note on [`crate::admin_front::Terminated`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(test, derive(strum::EnumDiscriminants))]
#[cfg_attr(test, strum_discriminants(derive(strum::EnumIter, Ord, PartialOrd)))]
pub(crate) enum Route {
    /// `POST /admin/tenants`
    TenantCreate,
    /// `GET /admin/tenants`
    TenantList,
    /// `GET /admin/tenants/:id`
    TenantRead(TenantId),
    /// `PUT /admin/tenants/:id`
    TenantPut(TenantId),
    /// `DELETE /admin/tenants/:id`
    TenantDelete(TenantId),
    /// `POST /admin/tenants/:id/principals` — mints a principal *and* its
    /// binding to `:id` in one committed op.
    PrincipalCreate(TenantId),
    /// `GET /admin/tenants/:id/principals`
    PrincipalList(TenantId),
    /// `PUT /admin/tenants/:id/principals/:pid`
    PrincipalPut(TenantId, PrincipalId),
    /// `DELETE /admin/tenants/:id/principals/:pid`
    PrincipalDelete(TenantId, PrincipalId),
    /// `PUT /admin/tenants/:id/bindings/:pid`
    BindingPut(TenantId, PrincipalId),
    /// `DELETE /admin/tenants/:id/bindings/:pid`
    BindingDelete(TenantId, PrincipalId),
    /// `GET /admin/audit?since=&limit=` (RFC-002 §9, issue #163).
    ///
    /// Carries the parsed query rather than the raw string so `dispatch` never
    /// re-parses what `classify` already read.
    ///
    /// Carries no tenant of its own: the row filter is derived in `dispatch`
    /// from the tenant the authorization decision was made against, never from
    /// the route and never from anything the request body said.
    AuditRead { since: u64, limit: usize },
    /// `GET /admin/audit/sink` (issue #164): the fleet's declared export sink,
    /// plus this node's own export status when it is reachable.
    AuditSinkRead,
    /// `PUT /admin/audit/sink`: declare or replace the sink.
    AuditSinkPut,
    /// `DELETE /admin/audit/sink`: stop exporting without losing the
    /// checkpoint (`AuditSink::revision` is what a re-declared sink resumes
    /// from, not this delete).
    AuditSinkDelete,
    /// `PUT /admin/fleet/name` (issue #373): set or rename the fleet's
    /// operator-facing name. No `GET` here — the name is read on
    /// `/_cluster/members` and `/_fleet/members`, which every node already
    /// answers unauthenticated; a second, authenticated read surface for the
    /// same fact would be redundant. No `DELETE` — out of scope for #373.
    FleetNamePut,
}

impl Route {
    /// The tenant this route is authorized against, or `None` when the route
    /// names none of its own and the caller's `X-Rift-Tenant` decides — the
    /// same rule every imposter route already follows.
    ///
    /// The fleet-level routes ([`Route::TenantCreate`], [`Route::TenantList`])
    /// scope to [`FLEET_SCOPE`] rather than to any tenant: creating and
    /// enumerating tenants is not an operation *within* one. `authz::decide`
    /// allows a `FleetAdmin` there (its binding names `"*"`) and refuses
    /// everyone else with `NotBoundToTenant` → the §8.4 `404`, which is the
    /// right answer for a surface a tenant admin should not learn the shape of.
    pub(crate) fn scope(&self) -> Option<TenantId> {
        match self {
            Route::TenantCreate
            | Route::TenantList
            // The sink is fleet state (one export, fleet-wide — see
            // `ControlOp::AuditSinkPut`'s doc), never a tenant's own record, so
            // it scopes the same way the tenant-record routes do: to the fleet
            // scope, not to whatever tenant the caller's header happens to
            // name. A `TenantAdmin` of `acme` sending `X-Rift-Tenant: acme`
            // must not become eligible for a route this decision pins to `*`.
            | Route::AuditSinkRead
            | Route::AuditSinkPut
            | Route::AuditSinkDelete
            // The fleet name is fleet state for the identical reason: one name,
            // fleet-wide, never a tenant's own record. A `TenantAdmin` of `acme`
            // sending `X-Rift-Tenant: acme` must not become eligible to rename
            // the fleet every other tenant is also looking at.
            | Route::FleetNamePut => Some(TenantId::new(FLEET_SCOPE)),
            Route::TenantRead(tenant)
            | Route::TenantPut(tenant)
            | Route::TenantDelete(tenant)
            | Route::PrincipalCreate(tenant)
            | Route::PrincipalList(tenant)
            | Route::PrincipalPut(tenant, _)
            | Route::PrincipalDelete(tenant, _)
            | Route::BindingPut(tenant, _)
            | Route::BindingDelete(tenant, _) => Some(tenant.clone()),
            // `None`, and the distinction matters: the path carries no tenant,
            // so the caller's header names the tenant they are acting as, and
            // the authorization decision is made against *that*.
            //
            // Returning a constant here instead — `default`, or `FLEET_SCOPE` —
            // is the shape that looks harmless and is not. A route scope
            // *replaces* the header (see `admin_front::authorize_action`), so a
            // constant `default` authorizes every audit read against the default
            // tenant: a `TenantAdmin` of any other tenant holds no binding there
            // and is refused `NotBoundToTenant` → `404` on their own audit
            // stream, which RFC-002 §9 explicitly grants them. `FLEET_SCOPE`
            // fails the same way for the same reason. Only the header can name
            // a tenant this route was not told about.
            Route::AuditRead { .. } => None,
        }
    }

    /// The RFC-002 §4.1 action that authorizes this route.
    ///
    /// Three tiers, and the middle one is the subtle one:
    ///
    /// - **`TenantManage`** (TenantAdmin+, within the addressed tenant):
    ///   listing and minting principals in a tenant, and binding a principal to
    ///   a role *inside* that tenant. This is a tenant admin's own job.
    /// - **`ClusterAdmin`** (FleetAdmin only): the tenant records themselves,
    ///   and `PrincipalPut`/`PrincipalDelete`. Principals are a fleet-global
    ///   namespace (`sm_principals` is keyed by id alone), so a `TenantAdmin`
    ///   of A deleting a principal would destroy a credential B also relies
    ///   on — RFC-002 §3's reason for making both fleet-only.
    /// - **`ClusterAdmin` for a binding on [`FLEET_SCOPE`]**, even though the
    ///   same route inside a real tenant is `TenantManage`. `validate` refuses
    ///   every role but `FleetAdmin` on `"*"`, so a binding there *is* a grant
    ///   of fleet privilege, and granting fleet privilege must require fleet
    ///   privilege (RFC-002 §4.2). This is what reconciles the issue's prose
    ///   ("bindings are FleetAdmin-only") with its acceptance criteria ("a
    ///   TenantAdmin of A can BindingDelete within A; it cannot PUT a binding
    ///   on `\"*\"`") — the privilege being granted, not the route shape, is
    ///   what decides.
    pub(crate) fn action(&self) -> Action {
        match self {
            Route::TenantCreate
            | Route::TenantList
            | Route::TenantRead(_)
            | Route::TenantPut(_)
            | Route::TenantDelete(_)
            | Route::PrincipalPut(_, _)
            | Route::PrincipalDelete(_, _)
            // Where the fleet's audit stream ships to is a fleet-scoped
            // decision, not a tenant-scoped read — RFC-002 §4.1's ceiling for
            // this surface. Deliberately NOT `AuditRead`: a `TenantAdmin`
            // trusted to read their own tenant's audit rows is not thereby
            // trusted to redirect where every tenant's rows are shipped.
            | Route::AuditSinkRead
            | Route::AuditSinkPut
            | Route::AuditSinkDelete
            // Same tier as the audit sink and for the same reason: a fleet-wide
            // rename is a fleet-scoped decision, not a tenant-scoped one.
            | Route::FleetNamePut => Action::ClusterAdmin,
            Route::PrincipalCreate(_) | Route::PrincipalList(_) => Action::TenantManage,
            // Deliberately NOT `TenantManage` (RFC-002 §4.1): reading who did
            // what and changing who may do what are different powers, so a
            // principal-manager is not automatically an auditor. `AuditRead`
            // starts at `TenantAdmin`, which is why an `Editor` gets 403 here.
            Route::AuditRead { .. } => Action::AuditRead,
            Route::BindingPut(tenant, _) | Route::BindingDelete(tenant, _) => {
                if tenant.as_str() == FLEET_SCOPE {
                    Action::ClusterAdmin
                } else {
                    Action::TenantManage
                }
            }
        }
    }
}

/// Classify a tenancy route, or `None` when `path` is not one of ours.
///
/// A recognized path with an unsupported method returns `None` too, which lets
/// it fall through to the proxy and answer upstream's own 404/405 — the same
/// thing `admin_front::classify` does for every other route it half-matches.
pub(crate) fn classify(method: &Method, path: &str, query: Option<&str>) -> Option<Route> {
    // Checked ahead of `AUDIT_PATH` (see `AUDIT_SINK_PATH`'s doc): this route
    // must never be reachable through `Route::AuditRead`'s `TenantManage`-
    // adjacent authorization tier.
    if path == AUDIT_SINK_PATH {
        return match *method {
            Method::GET => Some(Route::AuditSinkRead),
            Method::PUT => Some(Route::AuditSinkPut),
            Method::DELETE => Some(Route::AuditSinkDelete),
            _ => None,
        };
    }
    if path == AUDIT_PATH {
        return match *method {
            Method::GET => Some(audit_route(query)),
            _ => None,
        };
    }
    // Checked ahead of the `/admin/tenants/` prefix strip below, for the same defensive reason
    // `AUDIT_SINK_PATH` is: this route must never be reachable through a broader matcher by
    // accident, and `None` here (rather than falling through) is what makes an unserved method
    // a 404/405 instead of an accidental match further down.
    if path == FLEET_NAME_PATH {
        return match *method {
            Method::PUT => Some(Route::FleetNamePut),
            _ => None,
        };
    }
    if path == TENANTS_PATH {
        return match *method {
            Method::POST => Some(Route::TenantCreate),
            Method::GET => Some(Route::TenantList),
            _ => None,
        };
    }
    let rest = path.strip_prefix(TENANTS_PREFIX)?;
    let segments: Vec<&str> = rest.split('/').collect();
    // Percent-decoding is deliberately not done: a tenant id is
    // `[a-z0-9][a-z0-9-]{0,63}` and a principal id is control-character-free
    // ASCII, so nothing legal needs escaping. Decoding here would let
    // `%2f` smuggle an extra path segment past this matcher.
    match segments.as_slice() {
        [tenant] if !tenant.is_empty() => {
            let tenant = TenantId::new(*tenant);
            match *method {
                Method::GET => Some(Route::TenantRead(tenant)),
                Method::PUT => Some(Route::TenantPut(tenant)),
                Method::DELETE => Some(Route::TenantDelete(tenant)),
                _ => None,
            }
        }
        [tenant, "principals"] if !tenant.is_empty() => {
            let tenant = TenantId::new(*tenant);
            match *method {
                Method::POST => Some(Route::PrincipalCreate(tenant)),
                Method::GET => Some(Route::PrincipalList(tenant)),
                _ => None,
            }
        }
        [tenant, "principals", pid] if !tenant.is_empty() && !pid.is_empty() => {
            let tenant = TenantId::new(*tenant);
            let pid = PrincipalId::new(*pid);
            match *method {
                Method::PUT => Some(Route::PrincipalPut(tenant, pid)),
                Method::DELETE => Some(Route::PrincipalDelete(tenant, pid)),
                _ => None,
            }
        }
        [tenant, "bindings", pid] if !tenant.is_empty() && !pid.is_empty() => {
            let tenant = TenantId::new(*tenant);
            let pid = PrincipalId::new(*pid);
            match *method {
                Method::PUT => Some(Route::BindingPut(tenant, pid)),
                Method::DELETE => Some(Route::BindingDelete(tenant, pid)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Parse `?since=&limit=` into a [`Route::AuditRead`].
///
/// An unparseable or absent value takes the default rather than refusing. This
/// is a **domain-optional parse**, not a swallow: both parameters are pure
/// pagination with a safe default (`since=0` is the start of the journal, and
/// the default limit is already the answer for a caller who named none), so
/// there is no failure to hide — and a 400 on a malformed `since` would make
/// the audit endpoint harder to reach in exactly the incident where someone is
/// hand-typing the URL.
///
/// `limit` is clamped to [`AUDIT_MAX_LIMIT`]. A caller asking for more is given
/// the ceiling rather than an error, for the same reason.
fn audit_route(query: Option<&str>) -> Route {
    let mut since = 0u64;
    let mut limit = AUDIT_DEFAULT_LIMIT;
    for pair in query.unwrap_or_default().split('&') {
        match pair.split_once('=') {
            Some(("since", value)) => since = value.parse().unwrap_or(0),
            Some(("limit", value)) => {
                limit = value
                    .parse()
                    .unwrap_or(AUDIT_DEFAULT_LIMIT)
                    .min(AUDIT_MAX_LIMIT);
            }
            _ => {}
        }
    }
    Route::AuditRead { since, limit }
}

// ---------------------------------------------------------------------------
// Wire shapes
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TenantBody {
    /// Required on `POST /admin/tenants` (the id has nowhere else to come
    /// from); ignored on `PUT /admin/tenants/:id`, where the path is
    /// authoritative — see [`tenant_upsert`].
    #[serde(default)]
    pub id: Option<String>,
    pub display_name: String,
    #[serde(default)]
    pub quotas: Quotas,
    /// Seconds the M3 request shards keep this tenant's journal; `0` =
    /// unlimited. A per-tenant *policy*, not a quota — see `Quotas`' doc for
    /// why it sits beside them rather than among them (RFC-002 §11 Q2).
    #[serde(default)]
    pub journal_retention_secs: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PrincipalCreateBody {
    pub display_name: String,
    pub role: Role,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PrincipalPutBody {
    pub display_name: String,
    /// **Required**, deliberately not `#[serde(default)]`.
    ///
    /// This is a whole-record replace, so an omitted field would take the
    /// default — and the default for `disabled` is `false`. Disabling is how a
    /// fleet revokes a credential immediately, so an operator renaming a
    /// principal and forgetting this field would silently *un-revoke* it. That
    /// is the one omission on this surface that undoes a security action, so it
    /// must be stated rather than defaulted.
    pub disabled: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BindingBody {
    pub role: Role,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditSinkBody {
    pub uri: String,
    #[serde(default)]
    pub auth_ref: Option<String>,
    /// Optional: an omitted value takes [`DEFAULT_AUDIT_BATCH_MAX_ROWS`], not
    /// `0` — `#[serde(default)]` on a bare `u32` would silently ship nothing
    /// forever, which `control::validate` already refuses, but refusing a
    /// caller who simply left the field out (the documented, supported shape)
    /// would be the wrong way to enforce that.
    #[serde(default)]
    pub batch_max_rows: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FleetNameBody {
    /// Required, deliberately not `#[serde(default)]`: an operator who omitted the field asked
    /// for nothing, and a blank fleet name is the confusing state this whole feature exists to
    /// remove, not a neutral default to fall back to. A missing field is a `BadRequest`, same as
    /// any other malformed body.
    pub name: String,
}

/// The sink as the admin surface reports it: what `AuditSink` itself carries
/// (a URI and a credential *name*, never a credential — see that type's doc),
/// plus this node's own view of whether it is currently exporting.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuditSinkView {
    uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_ref: Option<String>,
    batch_max_rows: u32,
    /// The revision of the `AuditSinkPut` that produced this record.
    revision: u64,
    /// `None` when this node cannot report export status — `audit_sink()`
    /// answers from every replica's own applied state, but only the leader
    /// runs the exporter, so a follower's `GET` still names the fleet's sink
    /// with no status attached rather than fabricating one.
    #[serde(skip_serializing_if = "Option::is_none")]
    export_status: Option<ExportStatusView>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportStatusView {
    running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    shipped_rows: u64,
    consecutive_failures: u32,
}

impl From<ExportStatusSnapshot> for ExportStatusView {
    fn from(snapshot: ExportStatusSnapshot) -> Self {
        Self {
            running: snapshot.running,
            last_error: snapshot.last_error,
            shipped_rows: snapshot.shipped_rows,
            consecutive_failures: snapshot.consecutive_failures,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TenantView {
    id: String,
    display_name: String,
    quotas: Quotas,
    created_at_secs: u64,
    deleted: bool,
    journal_retention_secs: u64,
}

impl From<Tenant> for TenantView {
    fn from(t: Tenant) -> Self {
        Self {
            id: t.id.to_string(),
            display_name: t.display_name,
            quotas: t.quotas,
            created_at_secs: t.created_at_secs,
            deleted: t.deleted,
            journal_retention_secs: t.journal_retention_secs,
        }
    }
}

/// A principal as the admin surface reports it.
///
/// There is no field here for the credential, and that is the point: the type
/// makes "a `GET` leaked the key" unrepresentable rather than merely untested.
/// `auth` reports the *kind* of credential, never any part of its value.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PrincipalView {
    id: String,
    display_name: String,
    auth: &'static str,
    disabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<Role>,
}

impl PrincipalView {
    fn new(principal: Principal, role: Option<Role>) -> Self {
        Self {
            id: principal.id.to_string(),
            display_name: principal.display_name,
            auth: match principal.auth {
                AuthSource::ApiKey { .. } => "apiKey",
                AuthSource::Oidc { .. } => "oidc",
                AuthSource::MtlsSan { .. } => "mtlsSan",
            },
            disabled: principal.disabled,
            role,
        }
    }
}

/// The `POST .../principals` response — the **only** place a raw key ever
/// appears.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IssuedPrincipal {
    id: String,
    display_name: String,
    role: Role,
    tenant: String,
    /// Shown once. Not stored anywhere, not recoverable, not in any later read.
    api_key: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Whoami {
    /// `null` under the open-admin-plane bypass (no principals defined and no
    /// `--api-key`), where there genuinely is no identity to report. Reported
    /// as an explicit null with `authorizationDisabled: true` rather than
    /// omitted, so an operator can tell "nobody is authenticated" from "the
    /// fleet is not enforcing anything at all".
    principal_id: Option<String>,
    /// The principal's own `displayName`, when it has a stored record to carry one.
    ///
    /// `null` for the two identities that have no row to read it from: the open-admin-plane
    /// bypass (no principal at all) and the legacy `--api-key`'s synthetic principal, which is
    /// minted in code rather than committed. A client renders [`Self::principal_id`] in that
    /// case — which for the legacy identity is already the readable `legacy:api-key`.
    ///
    /// This exists so a console can put a *name* in front of an operator. The alternative it
    /// replaces was rendering `principal_id`, which for a minted key is `key:<sha256-hex>` —
    /// not a credential (the raw key is unrecoverable from it, and argon2id is the actual
    /// boundary), but indistinguishable from one on screen, which teaches the wrong instinct
    /// about what is safe to share.
    display_name: Option<String>,
    bindings: Vec<WhoamiBinding>,
    authorization_disabled: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WhoamiBinding {
    tenant: String,
    role: Role,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// What a tenancy handler produces: either a body to render at `status`, or a
/// [`ControlOp`] the caller commits and then renders `then` for.
///
/// Split this way so the whole surface's *submission* path is written once, in
/// [`commit`], instead of once per route — six hand-rolled park/submit/unpark
/// sequences is six chances to forget the parking that makes the write durable.
pub(crate) enum Outcome {
    Body {
        status: StatusCode,
        body: Vec<u8>,
    },
    Commit {
        op: ControlOp,
        status: StatusCode,
        /// Rendered *after* the op commits and this node has applied it.
        then: Option<Vec<u8>>,
    },
}

/// Serialize `value`, or fail loudly.
///
/// Not `.unwrap_or_default()`: a serialization failure that becomes an empty
/// `200 OK` body is the exact shape this repo has shipped as a defect before
/// (#606/#608). These types are all plain owned data with no non-string map
/// keys, so an error here is a defect in this file — reported as a `500`, never
/// swallowed into a success.
fn json(value: &impl Serialize) -> Result<Vec<u8>, String> {
    serde_json::to_vec(value).map_err(|e| e.to_string())
}

/// Run one tenancy route. `body` is the (already length-limited) request body.
///
/// `Err(reason)` is a client-shaped refusal message; the caller renders it as a
/// `400`. Storage failures are `Err` too and render as `500` — never as an
/// empty success.
pub(crate) fn dispatch(
    node: &Arc<RaftNode>,
    route: Route,
    body: &[u8],
    // The tenant `authz::decide` allowed this call against; see
    // `admin_front::terminate_tenancy`.
    authorized_tenant: &TenantId,
    bindings: &[(TenantId, Role)],
    // This node's own exporter status (issue #164), when one is wired up.
    // `None` on a build that never spawned an `AuditExporter` (a test harness,
    // say) — [`Route::AuditSinkRead`] is the only arm that reads it, and it
    // already treats a follower's own absent status as unremarkable, so a
    // wholly absent exporter is unremarkable too.
    export_status: Option<&ExportStatus>,
) -> Result<Outcome, TenancyError> {
    match route {
        Route::AuditRead { since, limit } => {
            // Who sees what (RFC-002 §9). A `FleetAdmin` sees the fleet; anyone
            // else sees exactly the tenant they were authorized as, and nothing
            // else.
            //
            // Server-side, deliberately. Handing a tenant admin the fleet's
            // rows and trusting a client to narrow them would mean the server
            // had already sent another tenant's audit history — the same
            // mistake §4.3 warns about for the event stream, and the reason
            // `/events` is still fleet-admin-only.
            //
            // The narrowing key is `authorized_tenant` — the very tenant
            // `authz::decide` allowed this call against — so the rows returned
            // and the decision that permitted them cannot disagree. Deriving it
            // instead by scanning `bindings` for the first tenant-admin binding
            // would quietly serve the wrong tenant to a principal bound to more
            // than one, and would do it while looking authorized.
            //
            // Empty bindings mean the RBAC **bypass** — no principals are
            // configured on this fleet, so `authorize_action` never resolved a
            // credential and there is no principal to scope to. That is
            // fleet-wide, not `default`-wide: narrowing to `authorized_tenant`
            // there would silently hide every non-default tenant's rows during
            // exactly the window an operator uses to provision the first tenants
            // and principals, and it would do it behind a normal `200`. On an
            // unenforced fleet there is no authorization to respect, so hiding
            // rows buys no isolation — only a wrong answer.
            let fleet_wide = bindings.is_empty()
                || bindings.iter().any(|(tenant, role)| {
                    *role == Role::FleetAdmin && tenant.as_str() == FLEET_SCOPE
                });
            let filter = if fleet_wide {
                None
            } else {
                Some(authorized_tenant.clone())
            };
            let rows = node
                .audit_since(since, filter.as_ref().map(TenantId::as_str), limit)
                .map_err(|e| TenancyError::Storage(e.to_string()))?;
            Ok(Outcome::Body {
                status: StatusCode::OK,
                body: json(&rows).map_err(TenancyError::Storage)?,
            })
        }
        Route::AuditSinkRead => {
            let Some(sink) = node
                .audit_sink()
                .map_err(|e| TenancyError::Storage(e.to_string()))?
            else {
                return Err(TenancyError::NotFound);
            };
            let view = AuditSinkView {
                uri: sink.uri,
                auth_ref: sink.auth_ref,
                batch_max_rows: sink.batch_max_rows,
                revision: sink.revision,
                // Only the leader exports, so only the leader has a status
                // worth reporting. A follower's exporter sits parked with
                // `running: false, shippedRows: 0, consecutiveFailures: 0` —
                // which is byte-identical to a *leader* whose exporter is
                // wedged and has shipped nothing. Omitting the field on a
                // follower keeps "no status here" distinguishable from "status,
                // and it is all zeroes", which is the whole question an
                // operator is asking when they read this endpoint.
                export_status: export_status
                    .filter(|_| node.is_leader())
                    .map(ExportStatus::snapshot)
                    .map(Into::into),
            };
            Ok(Outcome::Body {
                status: StatusCode::OK,
                body: json(&view).map_err(TenancyError::Storage)?,
            })
        }
        Route::AuditSinkPut => {
            let parsed: AuditSinkBody = parse(body)?;
            Ok(Outcome::Commit {
                op: ControlOp::AuditSinkPut {
                    // The fleet scope, not `TenantId::default()`. This op *is*
                    // audited, and `AuditRow::tenant` means "the tenant the op
                    // acted on" — a fleet-wide sink acts on the fleet. Writing
                    // `default` here would file a fleet-scoped configuration
                    // change under one ordinary tenant's name, in the very
                    // stream this feature exists to produce.
                    tenant: TenantId::new(FLEET_SCOPE),
                    uri: parsed.uri,
                    auth_ref: parsed.auth_ref,
                    batch_max_rows: parsed
                        .batch_max_rows
                        .unwrap_or(DEFAULT_AUDIT_BATCH_MAX_ROWS),
                },
                status: StatusCode::OK,
                then: None,
            })
        }
        Route::AuditSinkDelete => Ok(Outcome::Commit {
            op: ControlOp::AuditSinkDelete {
                tenant: TenantId::new(FLEET_SCOPE),
            },
            status: StatusCode::NO_CONTENT,
            then: None,
        }),
        Route::FleetNamePut => {
            let parsed: FleetNameBody = parse(body)?;
            Ok(Outcome::Commit {
                op: ControlOp::FleetNamePut {
                    // The fleet scope, not `TenantId::default()` — same reasoning as
                    // `AuditSinkPut` just above: this op is audited, and a fleet-wide rename
                    // filed under one ordinary tenant's name would be a wrong audit row, not
                    // just a wrong route.
                    tenant: TenantId::new(FLEET_SCOPE),
                    name: parsed.name,
                },
                status: StatusCode::OK,
                then: None,
            })
        }
        Route::TenantCreate => {
            let parsed: TenantBody = parse(body)?;
            let Some(id) = parsed.id.as_deref() else {
                return Err(TenancyError::BadRequest(
                    "POST /admin/tenants requires an \"id\"".to_owned(),
                ));
            };
            tenant_upsert(TenantId::new(id), parsed, StatusCode::CREATED)
        }
        Route::TenantPut(tenant) => {
            let parsed: TenantBody = parse(body)?;
            // The path wins over the body. Accepting a body `id` that disagrees
            // would make the addressed record and the written record two
            // different things — and the *authorization* was decided against
            // the path one, so honouring the body would authorize A and write B.
            if let Some(id) = parsed.id.as_deref()
                && id != tenant.as_str()
            {
                return Err(TenancyError::BadRequest(format!(
                    "body id {id:?} does not match the path tenant {:?}",
                    tenant.as_str()
                )));
            }
            tenant_upsert(tenant, parsed, StatusCode::OK)
        }
        Route::TenantDelete(tenant) => Ok(Outcome::Commit {
            op: ControlOp::TenantDelete { tenant },
            status: StatusCode::NO_CONTENT,
            then: None,
        }),
        Route::TenantList => {
            let tenants: Vec<TenantView> = node
                .tenants()
                .map_err(|e| TenancyError::Storage(e.to_string()))?
                .into_iter()
                .map(TenantView::from)
                .collect();
            Ok(Outcome::Body {
                status: StatusCode::OK,
                body: json(&tenants).map_err(TenancyError::Storage)?,
            })
        }
        Route::TenantRead(tenant) => {
            let found = node
                .tenant(tenant.as_str())
                .map_err(|e| TenancyError::Storage(e.to_string()))?;
            match found {
                Some(record) => Ok(Outcome::Body {
                    status: StatusCode::OK,
                    body: json(&TenantView::from(record)).map_err(TenancyError::Storage)?,
                }),
                None => Err(TenancyError::NotFound),
            }
        }
        Route::PrincipalCreate(tenant) => {
            let parsed: PrincipalCreateBody = parse(body)?;
            // Minted here, in the handler, and never again: the key exists in
            // this response and in the operator's hands. What goes into the log
            // is the argon2id hash and an id derived from the key's SHA-256,
            // and neither can reproduce it.
            let raw_key = generate_api_key();
            let principal = Principal {
                id: api_key_principal_id(&raw_key),
                // Cloned because the stored record and the one-time response
                // each need their own copy of the same name; only one of the
                // two can take the parsed `String` by move.
                display_name: parsed.display_name.clone(),
                auth: AuthSource::ApiKey {
                    hash: hash_api_key(&raw_key),
                },
                disabled: false,
            };
            let issued = IssuedPrincipal {
                id: principal.id.to_string(),
                display_name: parsed.display_name,
                role: parsed.role,
                tenant: tenant.to_string(),
                api_key: raw_key,
            };
            let rendered = json(&issued).map_err(TenancyError::Storage)?;
            Ok(Outcome::Commit {
                op: ControlOp::PrincipalCreate {
                    tenant,
                    principal,
                    role: parsed.role,
                },
                status: StatusCode::CREATED,
                then: Some(rendered),
            })
        }
        Route::PrincipalList(tenant) => {
            let principals: Vec<PrincipalView> = node
                .tenant_principals(tenant.as_str())
                .map_err(|e| TenancyError::Storage(e.to_string()))?
                .into_iter()
                .map(|(principal, role)| PrincipalView::new(principal, Some(role)))
                .collect();
            Ok(Outcome::Body {
                status: StatusCode::OK,
                body: json(&principals).map_err(TenancyError::Storage)?,
            })
        }
        Route::PrincipalPut(tenant, principal_id) => {
            let parsed: PrincipalPutBody = parse(body)?;
            // The credential is **not** replaceable through this route. A
            // principal's id is derived from its key (`key:<sha256>`), so a new
            // key is a new id and therefore a new principal — an "update the
            // key" operation would have to write a different row, which is a
            // create, not an update. Rotation is `POST .../principals` followed
            // by `DELETE` of the old one, and that is the honest shape.
            let existing = node
                .principal(principal_id.as_str())
                .map_err(|e| TenancyError::Storage(e.to_string()))?
                .ok_or(TenancyError::NotFound)?;
            Ok(Outcome::Commit {
                op: ControlOp::PrincipalPut {
                    tenant,
                    principal: Principal {
                        id: principal_id,
                        display_name: parsed.display_name,
                        auth: existing.auth,
                        disabled: parsed.disabled,
                    },
                },
                status: StatusCode::OK,
                then: None,
            })
        }
        Route::PrincipalDelete(tenant, principal_id) => Ok(Outcome::Commit {
            op: ControlOp::PrincipalDelete {
                tenant,
                principal_id,
            },
            status: StatusCode::NO_CONTENT,
            then: None,
        }),
        Route::BindingPut(tenant, principal_id) => {
            let parsed: BindingBody = parse(body)?;
            Ok(Outcome::Commit {
                op: ControlOp::BindingPut {
                    tenant,
                    principal_id,
                    role: parsed.role,
                },
                status: StatusCode::OK,
                then: None,
            })
        }
        Route::BindingDelete(tenant, principal_id) => Ok(Outcome::Commit {
            op: ControlOp::BindingDelete {
                tenant,
                principal_id,
            },
            status: StatusCode::NO_CONTENT,
            then: None,
        }),
    }
}

fn tenant_upsert(
    tenant: TenantId,
    parsed: TenantBody,
    status: StatusCode,
) -> Result<Outcome, TenancyError> {
    Ok(Outcome::Commit {
        op: ControlOp::TenantPut {
            tenant,
            display_name: parsed.display_name,
            quotas: parsed.quotas,
            journal_retention_secs: parsed.journal_retention_secs,
        },
        status,
        then: None,
    })
}

fn parse<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, TenancyError> {
    serde_json::from_slice(body).map_err(|e| TenancyError::BadRequest(format!("invalid body: {e}")))
}

/// How a tenancy route can fail before anything is committed.
#[derive(Debug)]
pub(crate) enum TenancyError {
    BadRequest(String),
    /// Rendered as RFC-002 §8.4's fixed 404 body, byte-identical to a
    /// cross-tenant refusal — a caller must not be able to tell "no such
    /// tenant" from "not yours".
    NotFound,
    Storage(String),
}

/// `GET /admin/whoami`: the caller's own identity and bindings.
///
/// `resolved` is `None` under the open-admin-plane bypass. Reporting that
/// honestly is the point — this route is also the cheapest possible check that
/// authorization is wired at all, and a fleet enforcing nothing must not look
/// the same as one enforcing something.
pub(crate) fn whoami_body(
    resolved: Option<&Resolved>,
    display_name: Option<String>,
) -> Result<Vec<u8>, String> {
    let view = match resolved {
        Some(resolved) => Whoami {
            principal_id: Some(resolved.principal_id.clone()),
            display_name,
            bindings: resolved
                .bindings
                .iter()
                .map(|(tenant, role)| WhoamiBinding {
                    tenant: tenant.to_string(),
                    role: *role,
                })
                .collect(),
            authorization_disabled: false,
        },
        None => Whoami {
            principal_id: None,
            display_name: None,
            bindings: Vec::new(),
            authorization_disabled: true,
        },
    };
    json(&view)
}

/// Mint a [`ControlRequest`] for a tenancy op.
///
/// `expected_revision` is always `None`: preconditions are defined against
/// `sm_configs` rows and route-table revisions (see
/// `control::precondition_target`), so there is nothing for a tenancy op to
/// condition on.
pub(crate) fn mint_request(
    op: ControlOp,
    principal: Option<String>,
    op_id: Uuid,
) -> ControlRequest {
    ControlRequest {
        op_id,
        principal,
        issued_at_secs: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
        expected_revision: None,
        op,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(id: &str) -> TenantId {
        TenantId::new(id)
    }

    #[test]
    fn every_documented_route_classifies() {
        let cases: [(Method, &str, Route); 11] = [
            (Method::POST, "/admin/tenants", Route::TenantCreate),
            (Method::GET, "/admin/tenants", Route::TenantList),
            (
                Method::GET,
                "/admin/tenants/acme",
                Route::TenantRead(tenant("acme")),
            ),
            (
                Method::PUT,
                "/admin/tenants/acme",
                Route::TenantPut(tenant("acme")),
            ),
            (
                Method::DELETE,
                "/admin/tenants/acme",
                Route::TenantDelete(tenant("acme")),
            ),
            (
                Method::POST,
                "/admin/tenants/acme/principals",
                Route::PrincipalCreate(tenant("acme")),
            ),
            (
                Method::GET,
                "/admin/tenants/acme/principals",
                Route::PrincipalList(tenant("acme")),
            ),
            (
                Method::PUT,
                "/admin/tenants/acme/principals/key:abc",
                Route::PrincipalPut(tenant("acme"), PrincipalId::new("key:abc")),
            ),
            (
                Method::DELETE,
                "/admin/tenants/acme/principals/key:abc",
                Route::PrincipalDelete(tenant("acme"), PrincipalId::new("key:abc")),
            ),
            (
                Method::PUT,
                "/admin/tenants/acme/bindings/key:abc",
                Route::BindingPut(tenant("acme"), PrincipalId::new("key:abc")),
            ),
            (
                Method::DELETE,
                "/admin/tenants/acme/bindings/key:abc",
                Route::BindingDelete(tenant("acme"), PrincipalId::new("key:abc")),
            ),
        ];
        for (method, path, expected) in cases {
            assert_eq!(
                classify(&method, path, None).as_ref(),
                Some(&expected),
                "{method} {path}"
            );
        }
    }

    #[test]
    fn a_path_we_do_not_own_is_not_classified() {
        for (method, path) in [
            (Method::GET, "/imposters"),
            (Method::GET, "/admin/whoami"),
            (Method::GET, "/admin/tenants/"),
            (Method::GET, "/admin/tenantsomething"),
            (Method::GET, "/admin/tenants/acme/unknown"),
            (Method::GET, "/admin/tenants/acme/principals/"),
            // A recognized path with an unsupported method falls through to
            // the proxy rather than being claimed and 405'd here.
            (Method::PATCH, "/admin/tenants"),
        ] {
            assert_eq!(classify(&method, path, None), None, "{method} {path}");
        }
    }

    /// The reconciliation stated in [`Route::action`]: the same route is
    /// tenant-tier inside a tenant and fleet-tier on the fleet scope, because
    /// what differs is the privilege being granted.
    #[test]
    fn a_binding_on_the_fleet_scope_needs_fleet_privilege() {
        let pid = PrincipalId::new("key:abc");
        assert_eq!(
            Route::BindingPut(tenant("acme"), pid.clone()).action(),
            Action::TenantManage,
            "binding inside a tenant is a tenant admin's own job"
        );
        assert_eq!(
            Route::BindingPut(tenant(FLEET_SCOPE), pid.clone()).action(),
            Action::ClusterAdmin,
            "a binding on \"*\" can only be fleet-admin, so it needs fleet privilege"
        );
        assert_eq!(
            Route::BindingDelete(tenant(FLEET_SCOPE), pid).action(),
            Action::ClusterAdmin,
            "revoking fleet privilege is fleet business too"
        );
    }

    /// Principals are fleet-global (`sm_principals` is keyed by id alone), so
    /// deleting one is not a tenant-scoped act even on a tenant-shaped path.
    #[test]
    fn principal_lifecycle_outside_the_create_path_is_fleet_only() {
        let pid = PrincipalId::new("key:abc");
        assert_eq!(
            Route::PrincipalDelete(tenant("acme"), pid.clone()).action(),
            Action::ClusterAdmin
        );
        assert_eq!(
            Route::PrincipalPut(tenant("acme"), pid).action(),
            Action::ClusterAdmin
        );
        assert_eq!(
            Route::PrincipalCreate(tenant("acme")).action(),
            Action::TenantManage,
            "minting an identity grants nothing outside the tenant, so it is tenant-tier"
        );
    }

    #[test]
    fn the_fleet_level_routes_scope_to_the_fleet_not_to_a_tenant() {
        assert_eq!(
            Route::TenantCreate.scope().as_ref().map(TenantId::as_str),
            Some(FLEET_SCOPE)
        );
        assert_eq!(
            Route::TenantList.scope().as_ref().map(TenantId::as_str),
            Some(FLEET_SCOPE)
        );
        assert_eq!(
            Route::TenantRead(tenant("acme"))
                .scope()
                .as_ref()
                .map(TenantId::as_str),
            Some("acme")
        );
    }

    /// `GET /admin/audit` names no tenant in its path, so it must defer to the
    /// caller's `X-Rift-Tenant` rather than pin a constant. A route scope
    /// *replaces* that header, so any `Some(..)` here would authorize every
    /// audit read against one fixed tenant — and a `TenantAdmin` of any other
    /// would get `404` on the stream RFC-002 §9 grants them.
    #[test]
    fn the_audit_route_defers_to_the_callers_tenant_rather_than_pinning_one() {
        assert_eq!(
            Route::AuditRead {
                since: 0,
                limit: 10,
            }
            .scope(),
            None
        );
    }

    /// `?since=&limit=` is parsed once, in `classify`, so `dispatch` never
    /// re-parses it. The fallbacks are a domain-optional parse — both parameters
    /// are pure pagination with a safe default — but "safe default" is a claim
    /// worth checking, particularly the `limit` clamp: without it any caller
    /// with `audit.read` could ask for the whole journal in one response.
    #[test]
    fn the_audit_query_is_parsed_clamped_and_defaulted() {
        let route = |q: Option<&str>| match audit_route(q) {
            Route::AuditRead { since, limit } => (since, limit),
            other => panic!("audit_route must always classify as AuditRead, got {other:?}"),
        };

        assert_eq!(
            route(None),
            (0, AUDIT_DEFAULT_LIMIT),
            "no query = the start of the journal, one default page"
        );
        assert_eq!(route(Some("since=42&limit=10")), (42, 10));
        assert_eq!(
            route(Some("limit=10&since=42")),
            (42, 10),
            "order is not significant"
        );
        assert_eq!(
            route(Some("limit=999999")),
            (0, AUDIT_MAX_LIMIT),
            "an unbounded read is an unbounded response: limit must be clamped"
        );
        assert_eq!(
            route(Some("since=notanumber")),
            (0, AUDIT_DEFAULT_LIMIT),
            "an unparseable page cursor falls back to the start rather than 400ing"
        );
        assert_eq!(
            route(Some("unrelated=1")),
            (0, AUDIT_DEFAULT_LIMIT),
            "unknown parameters are ignored, not fatal"
        );
    }

    /// The type that renders a principal has no field a key could occupy.
    #[test]
    fn a_rendered_principal_carries_no_credential() {
        let raw = "rift_super-secret-value";
        let view = PrincipalView::new(
            Principal {
                id: api_key_principal_id(raw),
                display_name: "svc".to_owned(),
                auth: AuthSource::ApiKey {
                    hash: hash_api_key(raw),
                },
                disabled: false,
            },
            Some(Role::Editor),
        );
        let rendered = String::from_utf8(json(&view).expect("serializes")).expect("utf8");
        assert!(
            !rendered.contains(raw),
            "the raw key must not survive into a read: {rendered}"
        );
        assert!(
            !rendered.contains("$argon2id$"),
            "nor the stored hash, which is still credential material: {rendered}"
        );
        assert!(rendered.contains("\"auth\":\"apiKey\""), "{rendered}");
    }

    #[test]
    fn whoami_distinguishes_an_unenforced_fleet_from_an_unbound_principal() {
        let bypass = String::from_utf8(whoami_body(None, None).expect("serializes")).expect("utf8");
        assert!(
            bypass.contains("\"authorizationDisabled\":true"),
            "{bypass}"
        );
        assert!(bypass.contains("\"principalId\":null"), "{bypass}");

        let resolved = Resolved {
            principal_id: "key:abc".to_owned(),
            bindings: vec![(tenant("acme"), Role::Editor)],
        };
        let bound = String::from_utf8(
            whoami_body(Some(&resolved), Some("Demo Editor".to_owned())).expect("serializes"),
        )
        .expect("utf8");
        assert!(bound.contains("\"authorizationDisabled\":false"), "{bound}");
        assert!(bound.contains("\"role\":\"editor\""), "{bound}");
        assert!(bound.contains("\"displayName\":\"Demo Editor\""), "{bound}");
    }

    /// The two identities with no stored row report `displayName: null` rather than inventing
    /// one — and a client tells them apart by `authorizationDisabled`, not by the missing name.
    #[test]
    fn an_identity_with_no_stored_row_reports_a_null_display_name() {
        let legacy = Resolved {
            principal_id: "legacy:api-key".to_owned(),
            bindings: vec![(tenant("default"), Role::TenantAdmin)],
        };
        let rendered =
            String::from_utf8(whoami_body(Some(&legacy), None).expect("serializes")).expect("utf8");
        assert!(rendered.contains("\"displayName\":null"), "{rendered}");
        assert!(
            rendered.contains("\"principalId\":\"legacy:api-key\""),
            "the id stays readable, so a client has something to fall back to: {rendered}"
        );
    }

    #[test]
    fn an_issued_key_carries_the_recognizable_prefix_and_is_unique() {
        let a = generate_api_key();
        let b = generate_api_key();
        assert!(a.starts_with("rift_"), "{a}");
        assert_ne!(a, b, "two mints must not collide");
        assert!(a.len() > 40, "256 bits of entropy, base64: {a}");
    }

    // -- audit export sink admin surface (issue #164) -----------------------

    #[test]
    fn the_audit_sink_route_classifies_by_method() {
        assert_eq!(
            classify(&Method::GET, AUDIT_SINK_PATH, None),
            Some(Route::AuditSinkRead)
        );
        assert_eq!(
            classify(&Method::PUT, AUDIT_SINK_PATH, None),
            Some(Route::AuditSinkPut)
        );
        assert_eq!(
            classify(&Method::DELETE, AUDIT_SINK_PATH, None),
            Some(Route::AuditSinkDelete)
        );
        // A recognized path with an unsupported method falls through to the
        // proxy rather than being claimed and 405'd here — the same rule
        // every other route on this surface follows.
        assert_eq!(
            classify(&Method::POST, AUDIT_SINK_PATH, None),
            None,
            "POST is not one of this route's supported methods"
        );
    }

    /// The exact swallow `AUDIT_SINK_PATH`'s doc warns about: the sink path
    /// must classify as its own route — with its own `ClusterAdmin` action —
    /// never fall through to `Route::AuditRead`'s `AuditRead` (tenant-reader)
    /// tier. A `TenantAdmin` must not gain fleet-sink visibility by that route
    /// mixup.
    #[test]
    fn the_audit_sink_route_is_never_classified_as_audit_read() {
        let classified = classify(&Method::GET, AUDIT_SINK_PATH, None);
        assert_eq!(classified, Some(Route::AuditSinkRead));
        assert_ne!(
            classified,
            Some(Route::AuditRead {
                since: 0,
                limit: AUDIT_DEFAULT_LIMIT,
            }),
            "the sink route must never be indistinguishable from a plain audit read"
        );
        assert_eq!(
            classified.as_ref().map(Route::action),
            Some(Action::ClusterAdmin),
            "and it must carry the sink's own (fleet-tier) action, not AuditRead's"
        );
    }

    /// RFC-002 §4.1: where the fleet's audit ships to is fleet business, so
    /// every method on this route is `ClusterAdmin`, scoped to the fleet —
    /// never `TenantManage` and never a tenant named by the caller's header.
    #[test]
    fn every_audit_sink_route_is_cluster_admin_scoped_to_the_fleet() {
        for route in [
            Route::AuditSinkRead,
            Route::AuditSinkPut,
            Route::AuditSinkDelete,
        ] {
            assert_eq!(route.action(), Action::ClusterAdmin, "{route:?}");
            assert_eq!(
                route.scope().as_ref().map(TenantId::as_str),
                Some(FLEET_SCOPE),
                "{route:?}"
            );
        }
    }

    /// A malformed `PUT` body is a client-shaped `400`, never a silent
    /// default: the rule this repo learned the hard way (a serde failure that
    /// becomes an empty `200 OK` is a shipped-bug shape here).
    #[test]
    fn a_malformed_audit_sink_put_body_is_refused_not_defaulted() {
        let result: Result<AuditSinkBody, TenancyError> = parse(b"not json at all");
        assert!(
            matches!(result, Err(TenancyError::BadRequest(_))),
            "a malformed body must produce a real refusal, not a defaulted success"
        );
    }

    /// An absent `batchMaxRows` takes the documented default rather than the
    /// bare-`u32` zero `#[serde(default)]` would otherwise produce — a `0`
    /// would ship nothing forever, and `control::validate` already refuses it,
    /// so a caller who simply omitted the field must not be refused for it.
    #[test]
    fn an_omitted_batch_max_rows_parses_as_none_not_zero() {
        let parsed: AuditSinkBody =
            parse(br#"{"uri":"https://collector.example/audit"}"#).expect("parses");
        assert_eq!(parsed.batch_max_rows, None);
    }

    // -- issue #373: the fleet's operator-set name ----------------------------

    #[test]
    fn classify_routes_put_admin_fleet_name() {
        assert_eq!(
            classify(&Method::PUT, "/admin/fleet/name", None),
            Some(Route::FleetNamePut)
        );
    }

    #[test]
    fn the_fleet_name_path_rejects_methods_it_does_not_serve() {
        // `None` here is what makes the surface a 404/405 rather than an accidental fall-through
        // into the `/admin/tenants/` matcher below it.
        for method in [Method::GET, Method::POST, Method::DELETE, Method::PATCH] {
            assert_eq!(
                classify(&method, "/admin/fleet/name", None),
                None,
                "{method} /admin/fleet/name is not a route this surface serves"
            );
        }
    }

    #[test]
    fn the_fleet_name_route_is_fleet_scoped_and_cluster_admin() {
        // Both halves matter and they fail differently: the wrong scope lets a tenant admin
        // sending `X-Rift-Tenant: acme` become eligible at all; the wrong action lets a
        // tenant-tier principal through once eligible.
        assert_eq!(
            Route::FleetNamePut.scope(),
            Some(TenantId::new(FLEET_SCOPE))
        );
        assert_eq!(Route::FleetNamePut.action(), Action::ClusterAdmin);
    }

    #[test]
    fn a_fleet_name_body_parses_its_name() {
        let parsed: FleetNameBody = parse(br#"{"name":"rift-prod-eu"}"#).expect("parses");
        assert_eq!(parsed.name, "rift-prod-eu");
    }

    #[test]
    fn a_fleet_name_body_without_a_name_is_refused() {
        // Not defaulted to "": an operator who omitted the field asked for nothing, and a blank
        // fleet name is the confusing state, not a neutral one.
        let err = parse::<FleetNameBody>(br#"{}"#)
            .expect_err("a body with no name must be a real refusal");
        assert!(matches!(err, TenancyError::BadRequest(_)), "{err:?}");
    }
}
