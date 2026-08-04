//! The published OpenAPI 3.1 contract (RFC-006 §5.1, issue #184) and the machinery that keeps it
//! honest.
//!
//! One contract, three consumers: the docs, the console's generated TypeScript client, and the MCP
//! server's tool input schemas (RFC-006 §8.2). The schema is **hand-authored** because the front's
//! router is hand-rolled hyper — [`crate::admin_front`]'s `classify` is a wildcard-free `match`, not
//! a framework with derive-based schema extraction, so annotation tooling (utoipa et al.) has no
//! shape to attach to.
//!
//! Hand-authored means drift is possible, so drift is made **mechanical rather than a matter of
//! discipline**. [`parity_report`] compares the routes the front actually serves against the paths
//! the contract publishes, in both directions, and the tests in this module fail CI on any
//! difference. The guard is layered, strongest first:
//!
//! 1. **Compile time, for the write surface.** `contract_route` and `tenancy_contract_route` are
//!    exhaustive matches with no wildcard arm, so a new `Terminated` / `tenancy::Route` variant
//!    fails to *compile* until it is given a published path — the same tripwire `action_for`,
//!    `addressed_port` and `scope_for` already use. On its own that only forces the variant to be
//!    *named*, not to enter the set being compared, so `every_terminated_variant_has_a_representative`
//!    closes the rest with `EnumDiscriminants`: the representative list cannot fall behind the enum.
//! 2. **Both directions at runtime.** Every EE path in the contract is fed through the real
//!    `classify`, and every route the exhaustive matches produce must appear in the contract.
//! 3. **Upstream by declaration.** `ImposterRoute` is `pub(crate)` inside the vendored submodule and
//!    unreachable from here, so the proxied surface is a declared table pinned by asserting each
//!    entry does *not* terminate locally — i.e. it really does proxy.
//!
//! **What is still not enforced, stated plainly.** Two route shapes have no compile-time tripwire,
//! because neither acquires a `Terminated` variant to hang one on:
//!
//! - a **new upstream route** arriving in a submodule bump — layer 3 is a declared table, so this
//!   needs a human;
//! - a **new read served directly from `handle`** — it never reaches `classify`, so it would be
//!   absent from [`HANDLE_DIRECT_ROUTES`] *and* from the contract, and the parity comparison would
//!   stay green comparing two sets that are each missing it. The *declared* half of that list is
//!   pinned by probing the live surface over HTTP
//!   (`tests/openapi.rs::every_direct_route_is_actually_served`), which is the only mechanism that
//!   catches a route being declared-but-unserved; catching the reverse — served-but-undeclared —
//!   remains a human step.
//!
//! **`GET /console` (C3, issue #186) is that shape, and is deliberately in neither list.** The
//! prediction above was right about the mechanism and wrong about the conclusion for this route:
//! the console serves the SPA's static assets, not an API operation. There is nothing for a
//! generated client to call, no request or response schema to publish, and no `x-rift-origin` that
//! would be true of it. Adding it would also break the probe: `every_direct_route_is_actually_served`
//! runs against a binary built **without** `--features console`, where `/console` correctly 404s.
//!
//! What stands in for the contract here is a pair of mutually exclusive integration tests, which
//! together pin the route in both directions more tightly than a contract entry would:
//! `tests/console_off.rs` asserts it is *not* served with the feature off (the parity invariant),
//! and `tests/console.rs` asserts it *is*, with the right headers, when the feature is on.

use std::sync::OnceLock;

/// The hand-authored contract, embedded at compile time so the binary serves the exact bytes that
/// were reviewed.
const CONTRACT_YAML: &str = include_str!("../../../docs/api/openapi-ee.yaml");

/// The parsed contract, built once. Parsing is deferred rather than done at startup so a node that
/// never serves `/openapi.json` never pays for it.
static CONTRACT: OnceLock<Result<serde_json::Value, String>> = OnceLock::new();

/// The contract as JSON, or the parse error.
///
/// The error is carried as a `String` rather than swallowed: `GET /openapi.json` answers `500` with
/// it, because a contract that will not parse is a broken build, and answering `200` with an empty
/// document would publish a lie to a generated client.
pub(crate) fn contract() -> Result<&'static serde_json::Value, &'static str> {
    CONTRACT
        .get_or_init(|| {
            serde_yaml::from_str::<serde_json::Value>(CONTRACT_YAML)
                .map_err(|err| format!("embedded openapi-ee.yaml is not valid YAML: {err}"))
        })
        .as_ref()
        .map_err(String::as_str)
}

/// The contract rendered as the JSON bytes `GET /openapi.json` serves.
pub(crate) fn contract_json() -> Result<Vec<u8>, &'static str> {
    let doc = contract()?;
    // Near-unreachable — `doc` came from `serde_yaml` as a `serde_json::Value`, so it is already
    // known-representable. Still propagated rather than defaulted, and the message says which of
    // the two failure modes it was, because a 500 whose body cannot distinguish "bad YAML" from
    // "bad JSON render" costs an hour of the wrong investigation.
    serde_json::to_vec(doc)
        .map_err(|_| "embedded openapi contract parsed but could not be rendered as JSON")
}

/// Routes the front terminates in `handle` **before** `classify` ever sees them, because `classify`
/// is the *write* classifier: these are reads, plus the session exchange, which authenticates rather
/// than mutating replicated config. None of them acquires a `Terminated` variant.
///
/// **This is a weak layer, and it is the weakest one in the gate.** Being hand-maintained, the
/// parity comparison only ever checks it in the documented→routable direction — and even that is
/// tautological, because `is_terminated_here` short-circuits on this very constant before it would
/// call `classify`, so for these entries the assertion is the constant checking itself. A read arm
/// added to `handle` and forgotten here is absent from both this list and the contract, so parity
/// stays green while the route runs undocumented. There is no compile-time tripwire for this shape:
/// a directly-served read has no variant to hang one on.
///
/// What closes it is probing the **live surface** over HTTP:
/// `tests/openapi.rs::every_direct_route_is_actually_served` asserts every entry here is really
/// answered by a running node, which is what catches an arm deleted from `handle` while the contract
/// still publishes it. Adding a route to `handle` still means adding it here *and* to the contract
/// by hand — that half remains a human step, stated rather than pretended away.
///
/// Public so that integration test can reach it; a duplicated list is exactly how the two halves
/// would drift.
pub const HANDLE_DIRECT_ROUTES: [(&str, &str); 8] = [
    ("GET", "/front-door/routes"),
    ("GET", "/admin/whoami"),
    ("GET", "/openapi.json"),
    // C2 (#185): the read-only fleet projection and the session exchange.
    ("GET", "/_fleet/members"),
    ("GET", "/_fleet/health"),
    ("GET", "/_fleet/ops/{opId}"),
    ("POST", "/session"),
    ("DELETE", "/session"),
];

/// The route-parity gate (issue #184's acceptance criteria).
///
/// Test-only by construction: none of this ships in the binary, which serves the contract and
/// nothing more. Keeping it under `cfg(test)` also means the exhaustive matches below are compiled
/// by `cargo test` and `cargo clippy --all-targets` — both of which CI runs — so the compile-time
/// tripwire still fires on a new route without putting dead weight in the release build.
#[cfg(test)]
mod parity {
    use std::collections::BTreeSet;

    use hyper::Method;
    use rift_cluster::control::{PrincipalId, TenantId};

    use crate::admin_front::{Terminated, classify};
    use crate::tenancy;

    /// One published operation: an OpenAPI path template plus its method.
    ///
    /// Ordered so `parity_report`'s difference sets come out stable and a failure message reads the
    /// same on every run.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub(crate) struct RouteKey {
        pub(crate) path: String,
        pub(crate) method: String,
    }

    impl RouteKey {
        fn new(method: &Method, path: &str) -> Self {
            Self {
                path: path.to_owned(),
                method: method.as_str().to_owned(),
            }
        }
    }

    impl std::fmt::Display for RouteKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{} {}", self.method, self.path)
        }
    }

    /// Every operation the contract publishes, with the origin it declares.
    ///
    /// An operation missing `x-rift-origin` is reported through `missing_origin` rather than silently
    /// defaulted — defaulting is how the `x-rift-origin` acceptance criterion would become decorative.
    pub(crate) struct ContractRoutes {
        pub(crate) ee: BTreeSet<RouteKey>,
        pub(crate) upstream: BTreeSet<RouteKey>,
        pub(crate) missing_origin: Vec<RouteKey>,
        pub(crate) unknown_origin: Vec<(RouteKey, String)>,
    }

    impl ContractRoutes {
        pub(crate) fn all(&self) -> BTreeSet<RouteKey> {
            self.ee.union(&self.upstream).cloned().collect()
        }
    }

    /// The HTTP methods OpenAPI allows as keys inside a path item. Anything else there (`parameters`,
    /// `summary`, a `$ref`) is path-item metadata, not an operation.
    pub(crate) const OPERATION_KEYS: [&str; 8] = [
        "get", "put", "post", "delete", "options", "head", "patch", "trace",
    ];

    /// Read the published operations out of a parsed contract.
    pub(crate) fn contract_routes(doc: &serde_json::Value) -> ContractRoutes {
        let mut ee = BTreeSet::new();
        let mut upstream = BTreeSet::new();
        let mut missing_origin = Vec::new();
        let mut unknown_origin = Vec::new();

        let paths = doc.get("paths").and_then(serde_json::Value::as_object);
        let Some(paths) = paths else {
            return ContractRoutes {
                ee,
                upstream,
                missing_origin,
                unknown_origin,
            };
        };

        for (path, item) in paths {
            let Some(item) = item.as_object() else {
                continue;
            };
            for verb in OPERATION_KEYS {
                let Some(operation) = item.get(verb) else {
                    continue;
                };
                let key = RouteKey {
                    path: path.clone(),
                    method: verb.to_ascii_uppercase(),
                };
                match operation.get("x-rift-origin").and_then(|v| v.as_str()) {
                    Some("ee") => {
                        ee.insert(key);
                    }
                    Some("upstream") => {
                        upstream.insert(key);
                    }
                    Some(other) => unknown_origin.push((key, other.to_owned())),
                    None => missing_origin.push(key),
                }
            }
        }

        ContractRoutes {
            ee,
            upstream,
            missing_origin,
            unknown_origin,
        }
    }

    /// The symmetric difference between what is served and what is published.
    ///
    /// Deliberately a plain data structure over two sets rather than a bundle of assertions: the
    /// negative-control test drives this same function with a synthetic route set to prove it actually
    /// reports a gap, which is what stops the parity test from passing vacuously.
    #[derive(Debug, Default, PartialEq, Eq)]
    pub(crate) struct ParityReport {
        /// Served by the front, absent from the contract.
        pub(crate) undocumented: Vec<RouteKey>,
        /// Published by the contract, not served by the front.
        pub(crate) unserved: Vec<RouteKey>,
    }

    impl ParityReport {
        pub(crate) fn is_clean(&self) -> bool {
            self.undocumented.is_empty() && self.unserved.is_empty()
        }
    }

    pub(crate) fn parity_report(
        served: &BTreeSet<RouteKey>,
        documented: &BTreeSet<RouteKey>,
    ) -> ParityReport {
        ParityReport {
            undocumented: served.difference(documented).cloned().collect(),
            unserved: documented.difference(served).cloned().collect(),
        }
    }

    /// The published operation for a terminated write route.
    ///
    /// **Exhaustive with no wildcard arm, on purpose** — the same tripwire `action_for` uses. A
    /// `Terminated` variant added without a line here fails to compile, so a new route cannot reach
    /// production while being invisible to the contract.
    pub(crate) fn contract_route(kind: &Terminated) -> RouteKey {
        match kind {
            Terminated::Create => RouteKey::new(&Method::POST, "/imposters"),
            Terminated::ReplaceAllImposters => RouteKey::new(&Method::PUT, "/imposters"),
            Terminated::DeleteAllImposters => RouteKey::new(&Method::DELETE, "/imposters"),
            Terminated::DeleteImposter(_) => RouteKey::new(&Method::DELETE, "/imposters/{port}"),
            Terminated::AddStub(_) => RouteKey::new(&Method::POST, "/imposters/{port}/stubs"),
            Terminated::ReplaceStubs(_) => RouteKey::new(&Method::PUT, "/imposters/{port}/stubs"),
            Terminated::ReplaceStubAt(_, _) => {
                RouteKey::new(&Method::PUT, "/imposters/{port}/stubs/{stubIndex}")
            }
            Terminated::DeleteStubAt(_, _) => {
                RouteKey::new(&Method::DELETE, "/imposters/{port}/stubs/{stubIndex}")
            }
            Terminated::ReplaceStubById(_, _) => {
                RouteKey::new(&Method::PUT, "/imposters/{port}/stubs/by-id/{stubId}")
            }
            Terminated::DeleteStubById(_, _) => {
                RouteKey::new(&Method::DELETE, "/imposters/{port}/stubs/by-id/{stubId}")
            }
            // The two `SetEnabled` arms are distinct *paths*, not a parameterized one, so the boolean
            // selects which operation is published rather than collapsing into a single entry.
            Terminated::SetEnabled(_, true) => {
                RouteKey::new(&Method::POST, "/imposters/{port}/enable")
            }
            Terminated::SetEnabled(_, false) => {
                RouteKey::new(&Method::POST, "/imposters/{port}/disable")
            }
            // One spelling per representative, same as the `SetEnabled` pair above — except here
            // the two spellings are true aliases of *one* variant (`classify` cannot tell which
            // path a `ReadSavedRequests`/`ClearSavedRequests` came from, by design: it is one
            // handler either way), so `contract_route` can only publish one of the two real paths.
            // `SAVED_REQUESTS_ALIAS_ROUTES` below covers the other.
            Terminated::ReadSavedRequests(_) => {
                RouteKey::new(&Method::GET, "/imposters/{port}/savedRequests")
            }
            Terminated::ClearSavedRequests(_) => {
                RouteKey::new(&Method::DELETE, "/imposters/{port}/savedRequests")
            }
            Terminated::PutRoutes => RouteKey::new(&Method::PUT, "/front-door/routes"),
            Terminated::DeleteRoute(_) => {
                RouteKey::new(&Method::DELETE, "/front-door/routes/{routeId}")
            }
            Terminated::Tenancy(route) => tenancy_contract_route(route),
            Terminated::SourceList => RouteKey::new(&Method::GET, "/admin/sources"),
            Terminated::SourceRead(_) => RouteKey::new(&Method::GET, "/admin/sources/{sourceId}"),
            Terminated::SourcePut => RouteKey::new(&Method::POST, "/admin/sources"),
            Terminated::SourceDelete(_) => {
                RouteKey::new(&Method::DELETE, "/admin/sources/{sourceId}")
            }
            Terminated::SourcePull(_) => {
                RouteKey::new(&Method::POST, "/admin/sources/{sourceId}/pull")
            }
        }
    }

    /// The published operation for a tenancy route. Exhaustive for the same reason as
    /// [`contract_route`].
    pub(crate) fn tenancy_contract_route(route: &tenancy::Route) -> RouteKey {
        use tenancy::Route;
        match route {
            Route::TenantCreate => RouteKey::new(&Method::POST, "/admin/tenants"),
            Route::TenantList => RouteKey::new(&Method::GET, "/admin/tenants"),
            Route::TenantRead(_) => RouteKey::new(&Method::GET, "/admin/tenants/{tenantId}"),
            Route::TenantPut(_) => RouteKey::new(&Method::PUT, "/admin/tenants/{tenantId}"),
            Route::TenantDelete(_) => RouteKey::new(&Method::DELETE, "/admin/tenants/{tenantId}"),
            Route::PrincipalCreate(_) => {
                RouteKey::new(&Method::POST, "/admin/tenants/{tenantId}/principals")
            }
            Route::PrincipalList(_) => {
                RouteKey::new(&Method::GET, "/admin/tenants/{tenantId}/principals")
            }
            Route::PrincipalPut(_, _) => RouteKey::new(
                &Method::PUT,
                "/admin/tenants/{tenantId}/principals/{principalId}",
            ),
            Route::PrincipalDelete(_, _) => RouteKey::new(
                &Method::DELETE,
                "/admin/tenants/{tenantId}/principals/{principalId}",
            ),
            Route::BindingPut(_, _) => RouteKey::new(
                &Method::PUT,
                "/admin/tenants/{tenantId}/bindings/{principalId}",
            ),
            Route::BindingDelete(_, _) => RouteKey::new(
                &Method::DELETE,
                "/admin/tenants/{tenantId}/bindings/{principalId}",
            ),
            Route::AuditRead { .. } => RouteKey::new(&Method::GET, "/admin/audit"),
            Route::AuditSinkRead => RouteKey::new(&Method::GET, "/admin/audit/sink"),
            Route::AuditSinkPut => RouteKey::new(&Method::PUT, "/admin/audit/sink"),
            Route::AuditSinkDelete => RouteKey::new(&Method::DELETE, "/admin/audit/sink"),
        }
    }

    pub(crate) use super::HANDLE_DIRECT_ROUTES;

    /// One representative of every [`Terminated`] variant.
    ///
    /// Paired with [`contract_route`]'s exhaustive match: the match makes a new variant a compile
    /// error, and this list is what turns the variants into a set the parity test can compare. Add a
    /// variant, and the compiler stops you here first.
    pub(crate) fn terminated_representatives() -> Vec<Terminated> {
        use tenancy::Route;
        let tenant = TenantId::new("acme");
        let principal = PrincipalId::new("p-1");
        vec![
            Terminated::Create,
            Terminated::ReplaceAllImposters,
            Terminated::DeleteAllImposters,
            Terminated::DeleteImposter(4545),
            Terminated::AddStub(4545),
            Terminated::ReplaceStubs(4545),
            Terminated::ReplaceStubAt(4545, 0),
            Terminated::DeleteStubAt(4545, 0),
            Terminated::ReplaceStubById(4545, "s-1".to_owned()),
            Terminated::DeleteStubById(4545, "s-1".to_owned()),
            Terminated::SetEnabled(4545, true),
            Terminated::SetEnabled(4545, false),
            Terminated::ReadSavedRequests(4545),
            Terminated::ClearSavedRequests(4545),
            Terminated::PutRoutes,
            Terminated::DeleteRoute("svc".to_owned()),
            Terminated::Tenancy(Route::TenantCreate),
            Terminated::Tenancy(Route::TenantList),
            Terminated::Tenancy(Route::TenantRead(tenant.clone())),
            Terminated::Tenancy(Route::TenantPut(tenant.clone())),
            Terminated::Tenancy(Route::TenantDelete(tenant.clone())),
            Terminated::Tenancy(Route::PrincipalCreate(tenant.clone())),
            Terminated::Tenancy(Route::PrincipalList(tenant.clone())),
            Terminated::Tenancy(Route::PrincipalPut(tenant.clone(), principal.clone())),
            Terminated::Tenancy(Route::PrincipalDelete(tenant.clone(), principal.clone())),
            Terminated::Tenancy(Route::BindingPut(tenant.clone(), principal.clone())),
            Terminated::Tenancy(Route::BindingDelete(tenant, principal)),
            Terminated::Tenancy(Route::AuditRead {
                since: 0,
                limit: 500,
            }),
            Terminated::Tenancy(Route::AuditSinkRead),
            Terminated::Tenancy(Route::AuditSinkPut),
            Terminated::Tenancy(Route::AuditSinkDelete),
            Terminated::SourceList,
            Terminated::SourceRead("payments".to_owned()),
            Terminated::SourcePut,
            Terminated::SourceDelete("payments".to_owned()),
            Terminated::SourcePull("payments".to_owned()),
        ]
    }

    /// The `.../requests` alias half of `ReadSavedRequests`/`ClearSavedRequests` (issue #223).
    ///
    /// `classify` collapses both spellings onto the same `Terminated` variant — one handler, two
    /// paths, exactly as upstream's own `router.rs` already treats them — so `contract_route` can
    /// publish only the `savedRequests` spelling per representative. This is the other, kept as a
    /// declared table for the same reason [`HANDLE_DIRECT_ROUTES`] is: there is no second variant
    /// to hang a compile-time tripwire on, because there is no second variant at all.
    const SAVED_REQUESTS_ALIAS_ROUTES: [(&str, &str); 2] = [
        ("GET", "/imposters/{port}/requests"),
        ("DELETE", "/imposters/{port}/requests"),
    ];

    /// Every operation this crate terminates: the write surface plus the reads `handle` answers itself.
    pub(crate) fn ee_served_routes() -> BTreeSet<RouteKey> {
        terminated_representatives()
            .iter()
            .map(contract_route)
            .chain(HANDLE_DIRECT_ROUTES.iter().map(|(method, path)| RouteKey {
                path: (*path).to_owned(),
                method: (*method).to_owned(),
            }))
            .chain(
                SAVED_REQUESTS_ALIAS_ROUTES
                    .iter()
                    .map(|(method, path)| RouteKey {
                        path: (*path).to_owned(),
                        method: (*method).to_owned(),
                    }),
            )
            .collect()
    }

    /// The upstream surface a client reaches *through* the front.
    ///
    /// Declared rather than derived: `ImposterRoute` and `route_by_path` are `pub(crate)` inside the
    /// vendored submodule and unreachable from this crate. `declared_upstream_routes_are_not_terminated_locally`
    /// pins every entry by asserting the front really does proxy it, but a genuinely new upstream route
    /// appearing in a submodule bump still needs a human to notice — this is the weakest layer of the
    /// guard and is documented as such rather than dressed up.
    pub(crate) fn upstream_proxied_routes() -> BTreeSet<RouteKey> {
        [
            ("GET", "/"),
            ("GET", "/health"),
            ("GET", "/config"),
            ("GET", "/logs"),
            ("GET", "/metrics"),
            ("POST", "/admin/reload"),
            ("GET", "/imposters"),
            ("GET", "/imposters/{port}"),
            ("GET", "/imposters/{port}/stubs"),
            ("GET", "/imposters/{port}/stubs/{stubIndex}"),
            ("GET", "/imposters/{port}/stubs/by-id/{stubId}"),
            // NOT `savedRequests`/`requests` (GET+DELETE): issue #223 terminates the no-`since`
            // GET as a fleet merge-on-read and the DELETE as a transitional peer fan-out — see
            // `ee_served_routes`'s `SAVED_REQUESTS_ALIAS_ROUTES`. `?since=` still proxies, but a
            // `RouteKey` carries no query, so that exception lives in the contract's prose, not
            // in a second entry here.
            ("POST", "/imposters/{port}/verify"),
            ("DELETE", "/imposters/{port}/savedProxyResponses"),
            ("GET", "/imposters/{port}/scenarios"),
            ("PUT", "/imposters/{port}/scenarios/{scenarioName}/state"),
            ("POST", "/imposters/{port}/scenarios/reset"),
            ("GET", "/imposters/{port}/spaces/{flowId}"),
            ("DELETE", "/imposters/{port}/spaces/{flowId}"),
            ("GET", "/imposters/{port}/spaces/{flowId}/stubs"),
            ("POST", "/imposters/{port}/spaces/{flowId}/stubs"),
            ("DELETE", "/admin/imposters/{port}/flow-state/{flowId}"),
            ("GET", "/admin/imposters/{port}/flow-state/{flowId}/{key}"),
            ("PUT", "/admin/imposters/{port}/flow-state/{flowId}/{key}"),
            (
                "DELETE",
                "/admin/imposters/{port}/flow-state/{flowId}/{key}",
            ),
        ]
        .into_iter()
        .map(|(method, path)| RouteKey {
            path: path.to_owned(),
            method: method.to_owned(),
        })
        .collect()
    }

    /// Substitute a concrete value for every `{placeholder}` so a published template can be fed to the
    /// real router.
    pub(crate) fn sample_path(template: &str) -> String {
        template
            .replace("{port}", "4545")
            .replace("{stubIndex}", "0")
            .replace("{stubId}", "s-1")
            .replace("{routeId}", "svc")
            .replace("{tenantId}", "acme")
            .replace("{principalId}", "p-1")
            .replace("{scenarioName}", "checkout")
            .replace("{flowId}", "flow-1")
            // A real UUID: `/_fleet/ops/{opId}` parses the segment, and a malformed id names no op at
            // all, so a placeholder like "op-1" would probe a different code path than the live route.
            .replace("{opId}", "0189dcf0-0454-4e0b-a10c-8a8f8dccce1f")
            .replace("{key}", "k")
    }

    /// Does the front terminate this request itself, rather than proxying it?
    ///
    /// Both halves matter: `classify` is the write classifier, and `handle` answers a few reads before
    /// it. Checking only the former would report `GET /front-door/routes` as proxied, which it is not.
    pub(crate) fn is_terminated_here(method: &str, path: &str) -> bool {
        // Compared against each entry's *sampled* form, not the raw template. Callers pass a
        // concrete path, and `/_fleet/ops/{opId}` is the first direct route carrying a placeholder —
        // matching the template verbatim silently missed it and reported a served route as proxied.
        if HANDLE_DIRECT_ROUTES
            .iter()
            .any(|(m, p)| *m == method && sample_path(p) == path)
        {
            return true;
        }
        let Ok(method) = Method::from_bytes(method.as_bytes()) else {
            return false;
        };
        let (path, query) = match path.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (path, None),
        };
        classify(&method, path, query).is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::parity::*;
    use super::*;
    use crate::admin_front::{Terminated, TerminatedDiscriminants};
    use crate::tenancy::RouteDiscriminants;

    fn parsed() -> &'static serde_json::Value {
        contract().expect("embedded openapi-ee.yaml must parse")
    }

    /// AC3 (document half): the embedded contract is a structurally valid OpenAPI 3.1 document.
    ///
    /// Structural rather than full meta-schema validation: vendoring the OpenAPI 3.1 meta-schema to
    /// validate against would add a dependency for the parts of it this repo does not use. What is
    /// asserted here is what a generated client actually needs to exist.
    #[test]
    fn contract_is_structurally_openapi_3_1() {
        let doc = parsed();

        let version = doc
            .get("openapi")
            .and_then(|v| v.as_str())
            .expect("`openapi` version field");
        assert!(
            version.starts_with("3.1."),
            "contract must declare OpenAPI 3.1, got {version}"
        );

        let info = doc.get("info").expect("`info` object");
        assert!(info.get("title").and_then(|v| v.as_str()).is_some());
        assert!(info.get("version").and_then(|v| v.as_str()).is_some());

        let paths = doc
            .get("paths")
            .and_then(serde_json::Value::as_object)
            .expect("`paths` object");
        assert!(!paths.is_empty(), "contract publishes no paths");

        // Every operation needs an operationId (the generated client names methods from it) and a
        // responses object (a client with no response type is not a client).
        for (path, item) in paths {
            let item = item.as_object().expect("path item is an object");
            for verb in OPERATION_KEYS {
                let Some(operation) = item.get(verb) else {
                    continue;
                };
                assert!(
                    operation
                        .get("operationId")
                        .and_then(|v| v.as_str())
                        .is_some(),
                    "{} {path} has no operationId",
                    verb.to_ascii_uppercase()
                );
                assert!(
                    operation
                        .get("responses")
                        .and_then(serde_json::Value::as_object)
                        .is_some_and(|r| !r.is_empty()),
                    "{} {path} declares no responses",
                    verb.to_ascii_uppercase()
                );
            }
        }
    }

    /// AC4: every operation records which side owns its contract.
    #[test]
    fn every_operation_declares_its_origin() {
        let routes = contract_routes(parsed());
        assert!(
            routes.missing_origin.is_empty(),
            "operations without x-rift-origin: {:?}",
            routes.missing_origin
        );
        assert!(
            routes.unknown_origin.is_empty(),
            "operations with an x-rift-origin outside {{ee, upstream}}: {:?}",
            routes.unknown_origin
        );
        assert!(!routes.ee.is_empty(), "contract publishes no EE operations");
        assert!(
            !routes.upstream.is_empty(),
            "contract publishes no upstream operations — the whole surface is documented, \
             not just the EE half"
        );
    }

    /// AC1, the headline criterion: the contract's path set equals the served route set.
    #[test]
    fn openapi_paths_match_the_served_route_set() {
        let routes = contract_routes(parsed());
        let served: BTreeSet<RouteKey> = ee_served_routes()
            .into_iter()
            .chain(upstream_proxied_routes())
            .collect();

        let report = parity_report(&served, &routes.all());
        assert!(
            report.is_clean(),
            "contract has drifted from the served surface.\n  served but undocumented: {:?}\n  \
             documented but not served: {:?}",
            report.undocumented,
            report.unserved
        );
    }

    /// AC1 (origin half): a route the front terminates must not be published as `upstream`, and
    /// vice versa. Without this the path *set* could match while every operation lied about who
    /// owns it.
    #[test]
    fn each_operation_is_published_under_the_side_that_serves_it() {
        let routes = contract_routes(parsed());
        let served_ee: BTreeSet<RouteKey> = ee_served_routes().into_iter().collect();
        let served_upstream: BTreeSet<RouteKey> = upstream_proxied_routes().into_iter().collect();

        let ee_mislabelled: Vec<_> = routes.ee.intersection(&served_upstream).collect();
        assert!(
            ee_mislabelled.is_empty(),
            "published as x-rift-origin: ee but actually proxied: {ee_mislabelled:?}"
        );
        let upstream_mislabelled: Vec<_> = routes.upstream.intersection(&served_ee).collect();
        assert!(
            upstream_mislabelled.is_empty(),
            "published as x-rift-origin: upstream but actually terminated here: \
             {upstream_mislabelled:?}"
        );
    }

    /// AC2, the negative control — the test that stops AC1 from being decorative.
    ///
    /// The issue asks to "add a route in the test's own fixture and show it red". A route cannot be
    /// added to the real `classify` from a test, so what is proven instead is that the *checker*
    /// reports a route that has no schema entry. If this test ever passes with an empty report,
    /// `openapi_paths_match_the_served_route_set` is passing vacuously.
    #[test]
    fn route_parity_checker_catches_an_undocumented_route() {
        let documented = contract_routes(parsed()).all();

        let mut served: BTreeSet<RouteKey> = ee_served_routes()
            .into_iter()
            .chain(upstream_proxied_routes())
            .collect();
        let smuggled = RouteKey {
            path: "/imposters/{port}/undocumented-by-construction".to_owned(),
            method: "POST".to_owned(),
        };
        served.insert(smuggled.clone());

        let report = parity_report(&served, &documented);
        assert!(
            !report.is_clean(),
            "the parity checker did not notice an undocumented route"
        );
        assert_eq!(
            report.undocumented,
            vec![smuggled],
            "the checker must name exactly the route that has no schema entry"
        );
        assert!(report.unserved.is_empty());

        // And the converse: a contract entry nothing serves is reported too.
        let orphan = RouteKey {
            path: "/never-served".to_owned(),
            method: "GET".to_owned(),
        };
        let mut documented_with_orphan = documented.clone();
        documented_with_orphan.insert(orphan.clone());
        let report = parity_report(&contract_routes(parsed()).all(), &documented_with_orphan);
        assert_eq!(report.unserved, vec![orphan]);
    }

    /// Layer 2 of the drift guard: every path the contract publishes as EE is actually classified
    /// by the real router, not merely spelled the same way. Catches a typo'd template that would
    /// otherwise satisfy set equality on both sides.
    #[test]
    fn every_published_ee_path_is_routable() {
        let ee = contract_routes(parsed()).ee;
        assert!(
            !ee.is_empty(),
            "no EE paths to probe — this test would pass vacuously"
        );
        for key in ee {
            let concrete = sample_path(&key.path);
            assert!(
                is_terminated_here(&key.method, &concrete),
                "{key} is published as x-rift-origin: ee but the front does not terminate \
                 {concrete}"
            );
        }
    }

    /// Layer 3: the declared upstream table really does proxy — no entry secretly terminates here.
    #[test]
    fn declared_upstream_routes_are_not_terminated_locally() {
        for key in upstream_proxied_routes() {
            let concrete = sample_path(&key.path);
            assert!(
                !is_terminated_here(&key.method, &concrete),
                "{key} is declared upstream-proxied but the front terminates {concrete}"
            );
        }
    }

    /// AC5: the cluster protocol headers are published with their real semantics, not as
    /// placeholders. These live only in code comments today, so this slice is what turns them into
    /// a contract.
    #[test]
    fn cluster_protocol_headers_are_documented() {
        let doc = parsed();
        let rendered = serde_json::to_string(doc).expect("contract renders");

        for header in [
            "If-Match",
            "Idempotency-Key",
            "Rift-Cluster-Revision",
            "Rift-Cluster-Op-Id",
            "X-Rift-Tenant",
            "Rift-Cluster-Partial",
        ] {
            assert!(
                rendered.contains(header),
                "{header} is not documented anywhere in the contract"
            );
        }

        // A named component with a description each, rather than a bare mention in prose: a
        // generated client reads components, and "documented with their real semantics" is not
        // satisfied by the header name appearing in a summary string.
        let components = doc
            .get("components")
            .and_then(|c| c.get("parameters"))
            .and_then(serde_json::Value::as_object)
            .expect("components.parameters");
        for component in ["IfMatch", "IdempotencyKey", "TenantHeader"] {
            let param = components
                .get(component)
                .unwrap_or_else(|| panic!("components.parameters.{component} is missing"));
            let description = param
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            assert!(
                description.len() > 40,
                "components.parameters.{component} needs a real description of the semantics, \
                 got {description:?}"
            );
        }

        let response_headers = doc
            .get("components")
            .and_then(|c| c.get("headers"))
            .and_then(serde_json::Value::as_object)
            .expect("components.headers");
        for header in [
            "RiftClusterRevision",
            "RiftClusterOpId",
            "RiftClusterPartial",
        ] {
            assert!(
                response_headers.contains_key(header),
                "components.headers.{header} is missing"
            );
        }
    }

    /// Resolve a parameter that may be a `$ref` into `#/components/parameters/*`.
    ///
    /// The contract is asserted as raw parsed YAML — nothing resolves `$ref` for us. A test that
    /// reads `name`/`in` straight off the entry sees neither on a `$ref`d parameter and concludes
    /// the operation declares nothing, which is a false failure. Resolving here also means a
    /// dangling `$ref` fails loudly instead of silently reading as "no such parameter".
    fn resolve_parameter<'a>(
        doc: &'a serde_json::Value,
        param: &'a serde_json::Value,
    ) -> &'a serde_json::Value {
        let Some(reference) = param.get("$ref").and_then(serde_json::Value::as_str) else {
            return param;
        };
        let name = reference
            .strip_prefix("#/components/parameters/")
            .unwrap_or_else(|| panic!("unsupported parameter $ref {reference}"));
        doc.get("components")
            .and_then(|c| c.get("parameters"))
            .and_then(|p| p.get(name))
            .unwrap_or_else(|| panic!("dangling parameter $ref {reference}"))
    }

    /// C5 (#188): the single-imposter read is where an editor gets its `If-Match` token, so the
    /// contract must say the header is there — a client generated without it has no typed way to
    /// reach the one value the whole conditional-write flow starts from.
    #[test]
    fn the_single_imposter_read_declares_the_revision_header() {
        let doc = parsed();
        let headers = doc
            .get("paths")
            .and_then(|p| p.get("/imposters/{port}"))
            .and_then(|p| p.get("get"))
            .and_then(|g| g.get("responses"))
            .and_then(|r| r.get("200"))
            .and_then(|r| r.get("headers"))
            .and_then(serde_json::Value::as_object)
            .expect("getImposter 200 declares response headers");
        let reference = headers
            .get("Rift-Cluster-Revision")
            .and_then(|h| h.get("$ref"))
            .and_then(serde_json::Value::as_str);
        assert_eq!(
            reference,
            Some("#/components/headers/RiftClusterRevision"),
            "the read's token must be the same component the write responses declare"
        );
    }

    /// The gate #216 closes. A schema that says `type: object` and nothing else tells a generated
    /// client nothing — `openapi-typescript` renders it `Record<string, never>` — and 19 of them
    /// rode through the route-parity era precisely because nothing looked at schema *shape*. A
    /// shapeless schema here is one whose map carries no shape signal at all: no `properties`, no
    /// `items`, no `$ref`, and no `additionalProperties` — the last because
    /// `{type: object, additionalProperties: true}` is a *declared decision* that a body is
    /// free-form, which is a different thing from never having decided.
    #[test]
    fn no_operation_ships_a_shapeless_object_schema() {
        let doc = parsed();
        let mut offenders = Vec::new();

        let shapeless = |schema: &serde_json::Value| -> bool {
            let Some(map) = schema.as_object() else {
                return false;
            };
            map.get("type").and_then(serde_json::Value::as_str) == Some("object")
                && !map.contains_key("properties")
                && !map.contains_key("additionalProperties")
                && !map.contains_key("$ref")
                && !map.contains_key("items")
                && !map.contains_key("oneOf")
                && !map.contains_key("anyOf")
                && !map.contains_key("allOf")
        };

        let mut check = |path: &str, op: &str, where_: &str, media: &serde_json::Value| {
            if let Some(schema) = media.get("schema")
                && shapeless(schema)
            {
                offenders.push(format!("{op} {path} ({where_})"));
            }
        };

        let paths = doc
            .get("paths")
            .and_then(serde_json::Value::as_object)
            .expect("paths");
        for (path, item) in paths {
            let Some(item) = item.as_object() else {
                continue;
            };
            for (method, op) in item {
                let Some(op) = op.as_object() else { continue };
                if let Some(body) = op
                    .get("requestBody")
                    .and_then(|b| b.get("content"))
                    .and_then(|c| c.get("application/json"))
                {
                    check(path, method, "requestBody", body);
                }
                if let Some(responses) = op.get("responses").and_then(serde_json::Value::as_object)
                {
                    for (status, response) in responses {
                        if let Some(media) = response
                            .get("content")
                            .and_then(|c| c.get("application/json"))
                        {
                            check(path, method, &format!("response {status}"), media);
                        }
                    }
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "shapeless `type: object` schemas — declare the real shape, or make free-form explicit              with `additionalProperties: true` and a description saying why:\n  {}",
            offenders.join("\n  ")
        );
    }

    fn journal_read_200<'a>(doc: &'a serde_json::Value, path: &str) -> &'a serde_json::Value {
        doc.get("paths")
            .and_then(|p| p.get(path))
            .and_then(|p| p.get("get"))
            .and_then(|g| g.get("responses"))
            .and_then(|r| r.get("200"))
            .unwrap_or_else(|| panic!("{path} GET has no 200 response"))
    }

    /// The two journal reads answer a **bare array**, and the contract has to say so.
    ///
    /// Declaring the body as a bare `type: object` is not merely imprecise: `openapi-typescript`
    /// renders it as `Record<string, never>`, so the console gets no usable type and hand-writes
    /// the shape instead — which is the traceability gap RFC-006 §11 exists to prevent. A route
    /// whose declared body shape disagrees with the server is worse than an undocumented one,
    /// because a generated client compiles against it and fails at runtime.
    ///
    /// Both paths are asserted because `router.rs` maps `["savedRequests"] | ["requests"]` to the
    /// same `ImposterRoute::SavedRequests` — they are one handler behind two spellings, so a
    /// contract that describes them differently is describing the same bytes two ways.
    #[test]
    fn recorded_request_journal_reads_declare_the_array_they_return() {
        let doc = parsed();

        for path in [
            "/imposters/{port}/requests",
            "/imposters/{port}/savedRequests",
        ] {
            let schema = journal_read_200(doc, path)
                .get("content")
                .and_then(|c| c.get("application/json"))
                .and_then(|c| c.get("schema"))
                .unwrap_or_else(|| panic!("{path} 200 declares no JSON schema"));

            assert_eq!(
                schema.get("type").and_then(serde_json::Value::as_str),
                Some("array"),
                "{path} answers a bare JSON array of RecordedRequest; the contract declares \
                 {schema:?}"
            );
            assert_eq!(
                schema
                    .get("items")
                    .and_then(|i| i.get("$ref"))
                    .and_then(serde_json::Value::as_str),
                Some("#/components/schemas/RecordedRequest"),
                "{path} items must reference the shared RecordedRequest schema"
            );
        }
    }

    /// The schema has to match what `serde` actually emits, not what the Rust struct looks like.
    ///
    /// Three of these fields are traps. `_mode` keeps its underscore (an explicit `rename` that
    /// overrides `rename_all = "camelCase"`) and is skipped entirely for a text body, so `"binary"`
    /// is the only value that ever reaches the wire — a schema declaring `text` would describe a
    /// response the engine cannot produce. A single-valued header serializes as a **bare string**,
    /// not a one-element array. And `body`/`_mode` are `skip_serializing_if`, so they are the only
    /// two optional fields.
    #[test]
    fn recorded_request_schema_matches_the_engine_serialization() {
        let doc = parsed();
        let schema = doc
            .get("components")
            .and_then(|c| c.get("schemas"))
            .and_then(|s| s.get("RecordedRequest"))
            .expect("components.schemas.RecordedRequest is missing");

        let required: BTreeSet<&str> = schema
            .get("required")
            .and_then(serde_json::Value::as_array)
            .expect("RecordedRequest declares no required fields")
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect();
        assert_eq!(
            required,
            BTreeSet::from([
                "requestFrom",
                "method",
                "path",
                "query",
                "headers",
                "timestamp"
            ]),
            "required must be exactly the fields serde always emits — `body` and `_mode` carry \
             skip_serializing_if and must stay optional"
        );

        let properties = schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .expect("RecordedRequest declares no properties");

        let mode_values: Vec<&str> = properties
            .get("_mode")
            .and_then(|m| m.get("enum"))
            .and_then(serde_json::Value::as_array)
            .expect("_mode must be declared with the enum of values that reach the wire")
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect();
        assert_eq!(
            mode_values,
            vec!["binary"],
            "a text body omits `_mode` entirely (skip_serializing_if = is_text_mode), so `binary` \
             is the only value the engine can emit"
        );

        // The diagnostics the request log answers "why did this 404" from (#208). Optional — an
        // entry without an outcome predates the field or never had one recorded — but the chain
        // must stay declared: matchOutcome -> MatchOutcome -> TriedStub -> TriedWhy, or the
        // generated client loses the panel's whole type.
        assert_eq!(
            properties
                .get("matchOutcome")
                .and_then(|m| m.get("$ref"))
                .and_then(serde_json::Value::as_str),
            Some("#/components/schemas/MatchOutcome"),
            "matchOutcome must reference the shared component"
        );
        // Optionality needs no separate assertion: the exact-set equality on `required` above
        // already refuses a future edit that promotes it.
        for component in ["MatchOutcome", "TriedStub", "TriedWhy"] {
            assert!(
                doc.get("components")
                    .and_then(|c| c.get("schemas"))
                    .and_then(|s| s.get(component))
                    .is_some(),
                "components.schemas.{component} is missing"
            );
        }

        // The bare-string case is the one a naive `array of string` schema gets wrong, and it is
        // also the common case — every single-valued header takes it.
        let header_values = properties
            .get("headers")
            .and_then(|h| h.get("additionalProperties"))
            .expect("headers must declare its value shape");
        let alternatives = header_values
            .get("oneOf")
            .or_else(|| header_values.get("anyOf"))
            .and_then(serde_json::Value::as_array)
            .expect("a header value is either a bare string or an array of strings");
        let shapes: BTreeSet<&str> = alternatives
            .iter()
            .filter_map(|a| a.get("type"))
            .filter_map(serde_json::Value::as_str)
            .collect();
        assert_eq!(
            shapes,
            BTreeSet::from(["array", "string"]),
            "multi_value_headers::serialize emits a scalar for one value and an array for many"
        );
    }

    /// The cursor headers are the endpoint's pagination contract, and their **presence** is the
    /// protocol: `x-rift-next-index` is absent on a degraded read, and `x-rift-truncated` appears
    /// only when it is true, so an SDK probes for the header rather than parsing a value. That is
    /// exactly the kind of semantics a bare mention in prose loses, so this asserts a real
    /// component description and a reference from the response that emits it.
    #[test]
    fn the_journal_cursor_headers_are_declared_where_they_are_emitted() {
        let doc = parsed();
        let components = doc
            .get("components")
            .and_then(|c| c.get("headers"))
            .and_then(serde_json::Value::as_object)
            .expect("components.headers");

        for component in ["XRiftNextIndex", "XRiftTruncated"] {
            let description = components
                .get(component)
                .unwrap_or_else(|| panic!("components.headers.{component} is missing"))
                .get("description")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            assert!(
                description.len() > 40,
                "components.headers.{component} needs a real description of when it is emitted, \
                 got {description:?}"
            );
        }

        for path in [
            "/imposters/{port}/requests",
            "/imposters/{port}/savedRequests",
        ] {
            let declared = journal_read_200(doc, path)
                .get("headers")
                .and_then(serde_json::Value::as_object)
                .unwrap_or_else(|| panic!("{path} 200 declares no response headers"));
            // Asserting the `$ref`, not merely the key: an inline header schema at the response
            // site would satisfy a presence check while quietly diverging from the component whose
            // description carries the presence semantics — which is the only place they are stated.
            for (header, component) in [
                ("x-rift-next-index", "XRiftNextIndex"),
                ("x-rift-truncated", "XRiftTruncated"),
            ] {
                let reference = declared
                    .get(header)
                    .unwrap_or_else(|| {
                        panic!("{path} emits {header} but its 200 does not declare it")
                    })
                    .get("$ref")
                    .and_then(serde_json::Value::as_str);
                assert_eq!(
                    reference,
                    Some(format!("#/components/headers/{component}").as_str()),
                    "{path}'s {header} must reference the shared component, not restate it inline"
                );
            }
        }
    }

    /// The clear side of the same two paths, which is the same defect one method over.
    ///
    /// `handle_clear_requests` parses `match` — and **only** `match`, never `since`, so a clear
    /// cannot be given a cursor — then delegates to `handle_get`, which is why a successful clear
    /// answers the imposter rather than an empty object. A bad clause is a 400 and a missing
    /// imposter a 404, both of which the alias spelling used to leave undeclared while its twin
    /// declared them.
    #[test]
    fn the_journal_clears_declare_what_they_accept_and_answer() {
        let doc = parsed();

        for path in [
            "/imposters/{port}/requests",
            "/imposters/{port}/savedRequests",
        ] {
            let delete = doc
                .get("paths")
                .and_then(|p| p.get(path))
                .and_then(|p| p.get("delete"))
                .unwrap_or_else(|| panic!("{path} DELETE"));

            let query: BTreeSet<&str> = delete
                .get("parameters")
                .and_then(serde_json::Value::as_array)
                .unwrap_or_else(|| panic!("{path} DELETE declares no parameters"))
                .iter()
                .map(|p| resolve_parameter(doc, p))
                .filter(|p| p.get("in").and_then(serde_json::Value::as_str) == Some("query"))
                .filter_map(|p| p.get("name"))
                .filter_map(serde_json::Value::as_str)
                .collect();
            assert_eq!(
                query,
                BTreeSet::from(["match"]),
                "{path} DELETE narrows by `match` and takes no cursor"
            );

            let responses = delete
                .get("responses")
                .and_then(serde_json::Value::as_object)
                .unwrap_or_else(|| panic!("{path} DELETE responses"));
            for status in ["400", "401", "403", "404"] {
                assert!(
                    responses.contains_key(status),
                    "{path} DELETE can answer {status} but does not declare it"
                );
            }
        }
    }

    /// `since` and `match` are the only two query parameters `request_filter.rs` parses, and both
    /// change the response. An undeclared parameter is invisible to a generated client, so the
    /// cursor work has nothing to call.
    ///
    /// Asserted on both spellings for the same reason as the body shape: one handler, two paths.
    /// Declaring the parameters on only one of them would document the same route two ways.
    #[test]
    fn the_journal_reads_declare_the_query_parameters_the_engine_parses() {
        let doc = parsed();

        for path in [
            "/imposters/{port}/requests",
            "/imposters/{port}/savedRequests",
        ] {
            let get = doc
                .get("paths")
                .and_then(|p| p.get(path))
                .and_then(|p| p.get("get"))
                .unwrap_or_else(|| panic!("{path} GET"));

            let query: BTreeSet<&str> = get
                .get("parameters")
                .and_then(serde_json::Value::as_array)
                .unwrap_or_else(|| panic!("{path} declares no parameters"))
                .iter()
                .map(|p| resolve_parameter(doc, p))
                .filter(|p| p.get("in").and_then(serde_json::Value::as_str) == Some("query"))
                .filter_map(|p| p.get("name"))
                .filter_map(serde_json::Value::as_str)
                .collect();
            assert_eq!(
                query,
                BTreeSet::from(["since", "match"]),
                "{path}: request_filter.rs parses exactly `since` and `match`"
            );

            // An unparseable `since` or an unsupported `match` clause answers 400, and both reads
            // sit behind the same authorization gate that answers 401/403/404.
            let responses = get
                .get("responses")
                .and_then(serde_json::Value::as_object)
                .unwrap_or_else(|| panic!("{path} responses"));
            for status in ["400", "401", "403", "404"] {
                let response = responses.get(status).unwrap_or_else(|| {
                    panic!("{path} can answer {status} but does not declare it")
                });
                // Presence alone would be satisfied by an empty `{}`, which tells a generated client
                // nothing about the body it will actually receive. Every one of these carries an
                // `Error` payload, whether by component reference or inline schema.
                let describes_a_body = response.get("$ref").is_some()
                    || response
                        .get("content")
                        .and_then(|c| c.get("application/json"))
                        .and_then(|c| c.get("schema"))
                        .is_some();
                assert!(
                    describes_a_body,
                    "{path}'s {status} declares no body — an empty response object documents nothing"
                );
            }
        }
    }

    /// The gate's strongest layer, closed.
    ///
    /// `contract_route`'s exhaustive match forces a new `Terminated` variant to be *given* a path,
    /// but nothing forced that path into the set the parity test compares — the representative list
    /// is hand-written, so a variant added with a `contract_route` arm and no representative would
    /// leave the served set unchanged, the documented set unchanged, and every parity test green
    /// while the route shipped undocumented. That is the one hole a reviewer found in this design.
    ///
    /// `EnumDiscriminants` closes it: the discriminant enum gains a variant automatically, so this
    /// assertion fails until a representative exists. Same for `tenancy::Route`.
    #[test]
    fn every_terminated_variant_has_a_representative() {
        use strum::IntoEnumIterator;

        let covered: BTreeSet<TerminatedDiscriminants> = terminated_representatives()
            .iter()
            .map(TerminatedDiscriminants::from)
            .collect();
        let all: BTreeSet<TerminatedDiscriminants> = TerminatedDiscriminants::iter().collect();
        let missing: Vec<_> = all.difference(&covered).collect();
        assert!(
            missing.is_empty(),
            "these Terminated variants have no representative, so their routes are invisible to \
             the parity comparison and could ship undocumented: {missing:?}"
        );

        let covered: BTreeSet<RouteDiscriminants> = terminated_representatives()
            .iter()
            .filter_map(|kind| match kind {
                Terminated::Tenancy(route) => Some(RouteDiscriminants::from(route)),
                _ => None,
            })
            .collect();
        let all: BTreeSet<RouteDiscriminants> = RouteDiscriminants::iter().collect();
        let missing: Vec<_> = all.difference(&covered).collect();
        assert!(
            missing.is_empty(),
            "these tenancy::Route variants have no representative: {missing:?}"
        );
    }

    /// The representatives that drive the exhaustive-match enumeration must not collapse: two
    /// variants mapping to one `RouteKey` would silently shrink the served set.
    #[test]
    fn terminated_representatives_are_distinct() {
        let keys: Vec<RouteKey> = terminated_representatives()
            .iter()
            .map(contract_route)
            .collect();
        let distinct: BTreeSet<&RouteKey> = keys.iter().collect();
        assert_eq!(
            distinct.len(),
            keys.len(),
            "two Terminated variants publish the same operation"
        );
    }
}
