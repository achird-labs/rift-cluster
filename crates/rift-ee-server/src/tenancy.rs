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
use rift_cluster::control::{
    AuthSource, FLEET_SCOPE, Principal, PrincipalId, Quotas, Role, Tenant, api_key_principal_id,
    generate_api_key, hash_api_key,
};
use rift_cluster::{ControlOp, ControlRequest, RaftNode, TenantId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::authz::Action;
use crate::principal::Resolved;

/// The one route with no action to authorize (RFC-002 §4.1).
pub(crate) const WHOAMI_PATH: &str = "/admin/whoami";

const AUDIT_PATH: &str = "/admin/audit";

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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    AuditRead {
        since: u64,
        limit: usize,
        /// `None` = the fleet. Filled in by `dispatch` from the caller's
        /// bindings, **never** from the request — see its arm.
        tenant: Option<TenantId>,
    },
}

impl Route {
    /// The tenant this route is authorized against.
    ///
    /// The fleet-level routes ([`Route::TenantCreate`], [`Route::TenantList`])
    /// scope to [`FLEET_SCOPE`] rather than to any tenant: creating and
    /// enumerating tenants is not an operation *within* one. `authz::decide`
    /// allows a `FleetAdmin` there (its binding names `"*"`) and refuses
    /// everyone else with `NotBoundToTenant` → the §8.4 `404`, which is the
    /// right answer for a surface a tenant admin should not learn the shape of.
    pub(crate) fn scope(&self) -> TenantId {
        match self {
            Route::TenantCreate | Route::TenantList => TenantId::new(FLEET_SCOPE),
            Route::TenantRead(tenant)
            | Route::TenantPut(tenant)
            | Route::TenantDelete(tenant)
            | Route::PrincipalCreate(tenant)
            | Route::PrincipalList(tenant)
            | Route::PrincipalPut(tenant, _)
            | Route::PrincipalDelete(tenant, _)
            | Route::BindingPut(tenant, _)
            | Route::BindingDelete(tenant, _) => tenant.clone(),
            // Scoped to the fleet, and the tenant narrowing happens *after*
            // the decision rather than in it. A `TenantAdmin` holds no binding
            // on `"*"`, so scoping this to the fleet would 404 them — but
            // RFC-002 §9 says they may read their own tenant's rows. So the
            // authorization scope is the caller's own requested tenant (the
            // header, or `default`), and `dispatch` derives the row filter from
            // the bindings that decision was made against.
            Route::AuditRead { .. } => TenantId::default(),
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
            | Route::PrincipalDelete(_, _) => Action::ClusterAdmin,
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
    if path == AUDIT_PATH {
        return match *method {
            Method::GET => Some(audit_route(query)),
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
    Route::AuditRead {
        since,
        limit,
        tenant: None,
    }
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
    bindings: &[(TenantId, Role)],
) -> Result<Outcome, TenancyError> {
    match route {
        Route::AuditRead { since, limit, .. } => {
            // Who sees what (RFC-002 §9). The filter is derived **here**, from
            // the bindings the authorization decision was made against — never
            // from anything the request said. A `FleetAdmin` sees the fleet; a
            // `TenantAdmin` sees its own tenant and nothing else.
            //
            // Server-side, deliberately. Handing a tenant admin the fleet's
            // rows and trusting a client to narrow them would mean the server
            // had already sent another tenant's audit history — the same
            // mistake §4.3 warns about for the event stream, and the reason
            // `/events` is still fleet-admin-only.
            let fleet_wide = bindings
                .iter()
                .any(|(tenant, role)| *role == Role::FleetAdmin && tenant.as_str() == FLEET_SCOPE);
            let filter = if fleet_wide {
                None
            } else {
                // The tenant this principal holds `AuditRead` in. `decide`
                // already established they hold it somewhere; with no fleet
                // binding that somewhere is a single tenant, and a principal
                // bound to several sees the one it is acting as.
                Some(
                    bindings
                        .iter()
                        .find(|(_, role)| matches!(role, Role::TenantAdmin | Role::FleetAdmin))
                        .map_or_else(TenantId::default, |(tenant, _)| tenant.clone()),
                )
            };
            let rows = node
                .audit_since(since, filter.as_ref().map(TenantId::as_str), limit)
                .map_err(|e| TenancyError::Storage(e.to_string()))?;
            Ok(Outcome::Body {
                status: StatusCode::OK,
                body: json(&rows).map_err(TenancyError::Storage)?,
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
pub(crate) fn whoami_body(resolved: Option<&Resolved>) -> Result<Vec<u8>, String> {
    let view = match resolved {
        Some(resolved) => Whoami {
            principal_id: Some(resolved.principal_id.clone()),
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
            bindings: Vec::new(),
            authorization_disabled: true,
        },
    };
    json(&view)
}

/// Mint a [`ControlRequest`] for a tenancy op.
///
/// `expected_revision` is always `None`: preconditions are defined against
/// `sm_configs` rows (see `control::precondition_target`), so there is nothing
/// for a tenancy op to condition on.
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
        assert_eq!(Route::TenantCreate.scope().as_str(), FLEET_SCOPE);
        assert_eq!(Route::TenantList.scope().as_str(), FLEET_SCOPE);
        assert_eq!(Route::TenantRead(tenant("acme")).scope().as_str(), "acme");
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
        let bypass = String::from_utf8(whoami_body(None).expect("serializes")).expect("utf8");
        assert!(
            bypass.contains("\"authorizationDisabled\":true"),
            "{bypass}"
        );
        assert!(bypass.contains("\"principalId\":null"), "{bypass}");

        let resolved = Resolved {
            principal_id: "key:abc".to_owned(),
            bindings: vec![(tenant("acme"), Role::Editor)],
        };
        let bound =
            String::from_utf8(whoami_body(Some(&resolved)).expect("serializes")).expect("utf8");
        assert!(bound.contains("\"authorizationDisabled\":false"), "{bound}");
        assert!(bound.contains("\"role\":\"editor\""), "{bound}");
    }

    #[test]
    fn an_issued_key_carries_the_recognizable_prefix_and_is_unique() {
        let a = generate_api_key();
        let b = generate_api_key();
        assert!(a.starts_with("rift_"), "{a}");
        assert_ne!(a, b, "two mints must not collide");
        assert!(a.len() > 40, "256 bits of entropy, base64: {a}");
    }
}
