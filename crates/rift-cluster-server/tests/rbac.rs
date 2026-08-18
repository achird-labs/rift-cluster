//! Issue #161: RBAC enforcement end to end — the admin front's own gate for
//! terminated *and* proxied routes, the §8.1 create-path tenant fix, the
//! §8.4 cross-tenant `404`, and the `rift_cluster_no_principals` gauge.
//!
//! These drive `compose::start` and speak plain HTTP to the public admin
//! address, exactly as `write_path.rs` does — the point is that enforcement
//! is real at the wire, not merely in the evaluator unit tests `authz.rs`
//! already carries. Principals, tenants and bindings are seeded by
//! submitting `ControlOp`s directly through the node (the admin HTTP surface
//! for them is #162 and is not needed here).

use std::time::Duration;

use clap::Parser;
use rift_cluster::control::{
    AuthSource, FLEET_SCOPE, Principal, PrincipalId, Quotas, Role, hash_api_key,
};
use rift_cluster::{ControlOp, ControlRequest, RaftNode, TenantId};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

mod common;

use common::ports::reserve_port;
use common::seen::Seen;

const SECRET: &str = "rbac-test-secret";

/// `spawn_metrics_sampler` (wired into every `compose::start` under
/// `--cluster`) resamples `rift_cluster_no_principals` into the *global*
/// `prometheus` registry — shared by every test in this binary, since
/// `cargo test` runs them concurrently on threads within one process. Only
/// one test here asserts on that gauge's value, but every other test in this
/// file also starts a cluster whose sampler writes to the same static, so an
/// assertion racing a sibling's sampler is a real flake, not a theoretical
/// one. Every test takes this lock for its whole body — the metrics test
/// needs exclusivity to assert; every other test needs to not undermine that
/// exclusivity by mutating the gauge out from under it.
static METRICS_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    let mut args = vec![
        "rift-cluster-server".to_owned(),
        "--port".to_owned(),
        "0".to_owned(),
        "--metrics-port".to_owned(),
        "0".to_owned(),
        "--cluster".to_owned(),
        "--cluster-bind".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--cluster-probe-bind".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--cluster-secret".to_owned(),
        SECRET.to_owned(),
        "--cluster-allow-solo".to_owned(),
        "--cluster-state-dir".to_owned(),
        state.path().to_string_lossy().into_owned(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

/// Poll `/readyz` until the node reports Ready; the reconcile gate opens
/// asynchronously after start. Copied from `write_path.rs` rather than
/// shared — see `tests/common/mod.rs`'s doc on why each test binary carries
/// its own copy of small fixtures like this.
async fn wait_ready(server: &ComposedServer) {
    let probes = server.probe_addr().expect("probes bound under --cluster");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = reqwest::get(format!("http://{probes}/readyz")).await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node never became ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn minimal_imposter(port: u16) -> serde_json::Value {
    json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "id": "a",
            "responses": [{ "is": { "statusCode": 200, "body": "hi" } }],
        }],
    })
}

/// Submit `op` directly through the node — the ControlOps from #159, standing
/// in for the admin CRUD surface #162 has not shipped yet.
async fn seed(node: &RaftNode, op_id: u128, op: ControlOp) {
    let response = node
        .write(ControlRequest {
            op_id: uuid::Uuid::from_u128(op_id),
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op,
        })
        .await
        .expect("seed op commits");
    assert_eq!(
        response.outcome,
        rift_cluster::ControlOutcome::Applied,
        "seed op {op_id} must apply cleanly"
    );
}

fn tenant_put(tenant: &str, display_name: &str) -> ControlOp {
    ControlOp::TenantPut {
        tenant: TenantId::new(tenant),
        display_name: display_name.to_owned(),
        quotas: Quotas::default(),
        journal_retention_secs: 0,
    }
}

/// A principal bound to `raw_key` (argon2id-hashed, never stored raw — same
/// rule RFC-002 §8.2 states for a real key issuance).
///
/// The principal's id is **derived from the raw key**
/// (`rift_cluster::control::api_key_principal_id`), not chosen freely: that
/// is exactly how `principal::resolve_bindings` looks a presented credential
/// up in production (issue #161) — a fast, non-secret fingerprint of the
/// key, verified against the stored argon2id hash. Seeding a principal under
/// any other id would make it unreachable from a real `Authorization`
/// header, and the request would 401 with nothing wrong in the evaluator —
/// exactly the failure mode this comment exists to head off.
fn principal_with_key(display_name: &str, raw_key: &str) -> Principal {
    Principal {
        id: rift_cluster::control::api_key_principal_id(raw_key),
        display_name: display_name.to_owned(),
        auth: AuthSource::ApiKey {
            hash: hash_api_key(raw_key),
        },
        disabled: false,
    }
}

fn principal_put(tenant: &str, principal: Principal) -> ControlOp {
    ControlOp::PrincipalPut {
        tenant: TenantId::new(tenant),
        principal,
    }
}

fn binding_put(tenant: &str, principal_id: &PrincipalId, role: Role) -> ControlOp {
    ControlOp::BindingPut {
        tenant: TenantId::new(tenant),
        principal_id: principal_id.clone(),
        role,
    }
}

/// Seed a principal bound to exactly one `(tenant, role)`, keyed by a unique
/// raw API key derived from `label`. Returns the raw key to authenticate
/// with.
///
/// `FleetAdmin` is special-cased to bind against [`FLEET_SCOPE`] rather than
/// `tenant`: `BindingPut` refuses `FleetAdmin` anywhere else (#159), so a
/// caller asking for `FleetAdmin` still gets a binding that actually commits
/// — the principal record itself still lives under `tenant` (its liveness
/// check only, not an isolation boundary; see `ControlOp::PrincipalPut`'s
/// doc).
async fn seed_bound_principal(
    node: &RaftNode,
    op_id: &mut u128,
    label: &str,
    tenant: &str,
    role: Role,
) -> String {
    let raw_key = format!("rbac-key-{label}");
    let principal = principal_with_key(label, &raw_key);
    let principal_id = principal.id.clone();
    seed(node, *op_id, principal_put(tenant, principal)).await;
    *op_id += 1;
    let binding_tenant = if role == Role::FleetAdmin {
        FLEET_SCOPE
    } else {
        tenant
    };
    seed(
        node,
        *op_id,
        binding_put(binding_tenant, &principal_id, role),
    )
    .await;
    *op_id += 1;
    raw_key
}

/// Issue #161's central acceptance criterion: enforcing on one surface only
/// (terminated *or* proxied) is the single most likely way this ships
/// broken, so every role is driven through both — a terminated write
/// (`POST /imposters`, Editor+) and a proxied write
/// (`DELETE /imposters/:port/savedRequests`, Operator+).
///
/// Tenant `default` throughout: T1 (#159) still only serves the default
/// tenant's configs onto the local engine, so a non-default-tenant imposter
/// would never actually bind — irrelevant to the authorization decision this
/// test is about, but it would make the *allowed* cases 404 at the engine
/// for the wrong reason.
#[tokio::test]
async fn the_role_action_matrix_holds_through_terminated_and_proxied_routes() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // A real, engine-bound imposter for the proxied probe (`savedRequests`)
    // to act against, created directly through the node — fixture setup, not
    // itself under test.
    let probe_port = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(minimal_imposter(probe_port)).expect("config parses"),
        },
    )
    .await;
    op_id += 1;

    // The second flag covers both `SavedRequestsClear` and `LifecycleToggle`: `role_allows` grants
    // them at the same rung (Operator adds both), so one column stating "Operator and above" is
    // the table's shape rather than a coincidence two columns would obscure.
    let cases = [
        (Role::Viewer, false, false),
        (Role::Operator, false, true),
        (Role::Editor, true, true),
        (Role::TenantAdmin, true, true),
        (Role::FleetAdmin, true, true),
    ];

    for (role, create_allowed, operator_allowed) in cases {
        let label = format!("matrix-{role:?}");
        let raw_key = seed_bound_principal(node, &mut op_id, &label, "default", role).await;

        // Terminated: POST /imposters.
        let create_port = reserve_port();
        let response = client
            .post(format!("http://{admin}/imposters"))
            .header("authorization", &raw_key)
            .json(&minimal_imposter(create_port))
            .send()
            .await
            .expect("post imposter");
        let seen = Seen::of(response).await;
        assert_eq!(
            seen.status,
            if create_allowed { 201 } else { 403 },
            "{role:?} terminated create (ImposterWrite): {seen}"
        );

        // Proxied: DELETE /imposters/:port/savedRequests.
        let response = client
            .delete(format!(
                "http://{admin}/imposters/{probe_port}/savedRequests"
            ))
            .header("authorization", &raw_key)
            .send()
            .await
            .expect("clear saved requests");
        let seen = Seen::of(response).await;
        assert_eq!(
            seen.status,
            if operator_allowed { 200 } else { 403 },
            "{role:?} proxied clear (SavedRequestsClear): {seen}"
        );

        // Terminated: POST /imposters/:port/{disable,enable} — `Action::LifecycleToggle`.
        //
        // The web console's only mutation (RFC-006 §4, #187), and the reason this arm exists
        // rather than being assumed from the two above: the console hides the enable/disable
        // control from a Viewer, and RFC-006 §3 rule 3 says that hiding is UX while the API is
        // the boundary. Nothing asserted the boundary half for *this* action until now — the
        // matrix covered `ImposterWrite` and `SavedRequestsClear`, and the tests that do call
        // these routes run open-admin with no principal at all.
        //
        // Toggled off and back on within one iteration so the imposter is left enabled for the
        // next role's probes; for a refused role both calls are 403 and nothing changes.
        for (verb, allowed) in [("disable", operator_allowed), ("enable", operator_allowed)] {
            let response = client
                .post(format!("http://{admin}/imposters/{probe_port}/{verb}"))
                .header("authorization", &raw_key)
                .send()
                .await
                .expect("toggle imposter lifecycle");
            let seen = Seen::of(response).await;
            assert_eq!(
                seen.status,
                if allowed { 200 } else { 403 },
                "{role:?} terminated {verb} (LifecycleToggle): {seen}"
            );
        }
    }

    server.shutdown().await;
}

/// RFC-002 §8.4: a cross-tenant probe for a resource that exists must be
/// byte-identical to a probe for one that does not — status, body, AND
/// headers.
///
/// The original version of this test compared two probes that both fell
/// through the *same* `Denial::NotBoundToTenant` arm (an outsider probing an
/// existing port vs a nonexistent one) — tautological, since one code branch
/// producing the same fixed string twice proves nothing about
/// indistinguishability. The comparison §8.4 actually demands is between
/// *different* refusal reasons that must still be unrecognizable from one
/// another: "you hold no binding here" (`Denial::NotBoundToTenant`) vs "you
/// are genuinely bound and authorized here, but this build cannot serve
/// anything outside `default`" (the B2/B3 tenant-serving guard in
/// `admin_front::authorize_action`). Both are answered by the same
/// `tenant_boundary_not_found` helper, so this test is also what would fail
/// if that sharing were ever broken — e.g. by a hand-rolled duplicate 404 at
/// one of the two call sites drifting from the other.
#[tokio::test]
async fn cross_tenant_probes_are_indistinguishable_from_probes_of_nothing() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // An imposter that genuinely exists, in the tenant the requester is NOT
    // bound to.
    let existing_port = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(minimal_imposter(existing_port)).expect("config parses"),
        },
    )
    .await;
    op_id += 1;

    seed(node, op_id, tenant_put("acme", "Acme Corp")).await;
    op_id += 1;
    // Bound to "acme" as a genuine TenantAdmin — enough authority to read an
    // imposter, in its own tenant, were this build able to serve one there.
    let raw_key =
        seed_bound_principal(node, &mut op_id, "outsider", "acme", Role::TenantAdmin).await;

    // Probe 1: not bound to `default` at all (no `X-Rift-Tenant` header, so
    // the requested tenant defaults to `default`) — `Denial::NotBoundToTenant`.
    let not_bound = Seen::of(
        client
            .get(format!("http://{admin}/imposters/{existing_port}"))
            .header("authorization", &raw_key)
            .send()
            .await
            .expect("probe existing port in a tenant this principal is not bound to"),
    )
    .await;

    // Probe 2: genuinely bound to `acme` with `TenantAdmin` — `decide` grants `ImposterRead`
    // there — reaching for a port that belongs to **`default`**. This is the probe that matters
    // now, and it did not exist before issue #182: until then the blanket serving guard refused
    // every non-default tenant on the tenant alone, so no request ever reached a resource lookup
    // and there was no cross-tenant *resource* boundary to test. Now that `acme` is served, the
    // ownership gate is the only thing standing between an authorized `acme` admin and
    // `default`'s imposter — and it must refuse indistinguishably from "you are not bound here".
    let cross_tenant = Seen::of(
        client
            .get(format!("http://{admin}/imposters/{existing_port}"))
            .header("authorization", &raw_key)
            .header("x-rift-tenant", "acme")
            .send()
            .await
            .expect("probe another tenant's port from a tenant this principal IS bound to"),
    )
    .await;

    assert_eq!(not_bound.status, 404, "{not_bound}");
    assert_eq!(
        cross_tenant.status, 404,
        "another tenant's imposter must read as not-found: {cross_tenant}"
    );
    assert_eq!(
        not_bound.body, cross_tenant.body,
        "a caller must not be able to tell 'not bound here' from 'bound here, but that port is \
         another tenant's' by body"
    );
    assert_eq!(
        headers_excluding_date(&not_bound),
        headers_excluding_date(&cross_tenant),
        "nor by headers (aside from `date`, which every response carries and which hyper's \
         server stamps with the real wall-clock time of each request — comparing it would fail \
         two genuinely identical responses sent a second apart, not detect a leak) — \
         {not_bound} vs {cross_tenant}"
    );

    // And the other half, which is what makes the two assertions above meaningful rather than
    // vacuous (issue #182's acceptance criterion): `acme` reading **its own** imposter succeeds.
    // Before this change this was a 404 too — every tenant but `default` was refused wholesale —
    // so a test that only asserted the refusals would have passed just as well against a build
    // that serves nobody.
    let acme_port = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::new("acme"),
            config: serde_json::from_value(minimal_imposter(acme_port)).expect("config parses"),
        },
    )
    .await;

    let own = Seen::of(
        client
            .get(format!("http://{admin}/imposters/{acme_port}"))
            .header("authorization", &raw_key)
            .header("x-rift-tenant", "acme")
            .send()
            .await
            .expect("read acme's own imposter"),
    )
    .await;
    assert_eq!(
        own.status, 200,
        "an Editor of acme must be served acme's own imposter: {own}"
    );

    server.shutdown().await;
}

/// A response's headers, minus `date` — see the call site above for why that
/// one header is excluded from an indistinguishability comparison: it is real
/// wall-clock time, present on every response by construction, and never a
/// function of which branch produced the response.
fn headers_excluding_date(seen: &Seen) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = seen
        .headers
        .iter()
        .filter(|(name, _)| *name != "date")
        .map(|(name, value)| {
            (
                name.to_string(),
                value.to_str().unwrap_or("<non-utf8>").to_owned(),
            )
        })
        .collect();
    pairs.sort();
    pairs
}

/// Issue #161's B1/B2/B3 in combination: a principal genuinely bound (and
/// authorized) to a non-default tenant must be refused — by the B2/B3
/// tenant-serving guard, since this build cannot serve anything but
/// `default` — and, crucially, that refusal must leave `default`'s own data
/// untouched.
///
/// This is the regression test for the exploit B1 describes: before it was
/// fixed, `DELETE /imposters` authorized against `acme` still emitted
/// `ControlOp::DeleteAll { tenant: TenantId::default() }` — a hardcoded
/// tenant that ignored what was actually authorized — destroying `default`'s
/// imposters instead of (harmlessly, since nothing is bound there) acme's.
/// The B2/B3 guard added in the same change closes the hole earlier still,
/// by refusing the request before any op is even built — so today this can
/// only demonstrate the *combination* holding, not B1's line in isolation:
/// with the guard in place, a reverted B1 has no HTTP-observable effect,
/// because the request never reaches `build_mutation`. That is stated
/// plainly rather than left implicit — B1 has no test that fails on its own
/// while B2/B3 stands.
#[tokio::test]
async fn a_refused_cross_tenant_delete_leaves_the_default_tenant_untouched() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // `default`'s own imposter — the thing that must survive.
    let victim_port = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(minimal_imposter(victim_port)).expect("config parses"),
        },
    )
    .await;
    op_id += 1;

    seed(node, op_id, tenant_put("acme", "Acme Corp")).await;
    op_id += 1;
    // TenantAdmin of acme — Editor-tier and above, so `ImposterDelete` really
    // is granted; the refusal below must come from the serving guard, not
    // from an insufficient role.
    let acme_key =
        seed_bound_principal(node, &mut op_id, "acme-admin", "acme", Role::TenantAdmin).await;
    // A second principal, bound to `default`, used only to verify the victim
    // survived — reading it through the same authorized admin surface rather
    // than reaching around it into the node.
    let default_key =
        seed_bound_principal(node, &mut op_id, "default-reader", "default", Role::Viewer).await;

    let response = client
        .delete(format!("http://{admin}/imposters"))
        .header("authorization", &acme_key)
        .header("x-rift-tenant", "acme")
        .send()
        .await
        .expect("delete-all authorized against acme");
    let seen = Seen::of(response).await;
    // Issue #182 changed *why* `default`'s imposter survives, and the new reason is the stronger
    // one. It used to survive because the serving guard refused the request outright — acme was
    // unservable, so nothing ran. Now acme is served: the delete-all succeeds, over **acme's own
    // set**, which is empty. The invariant this test is named for is unchanged and is now being
    // tested against a request that actually executes rather than one that was turned away at the
    // door.
    assert_eq!(
        seen.status, 200,
        "acme's delete-all is authorized and now genuinely runs, over acme's own set: {seen}"
    );

    let response = client
        .get(format!("http://{admin}/imposters/{victim_port}"))
        .header("authorization", &default_key)
        .send()
        .await
        .expect("re-read default's imposter");
    assert_eq!(
        response.status().as_u16(),
        200,
        "a refused cross-tenant delete-all must not have touched default's imposters"
    );

    server.shutdown().await;
}

/// In-tenant but under-privileged is `403`, and must read as distinct from
/// the cross-tenant `404` above — otherwise the two are the same signal and
/// #161 has not actually separated "not yours" from "yours, but you can't".
#[tokio::test]
async fn in_tenant_insufficient_role_is_403_not_404() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let raw_key =
        seed_bound_principal(node, &mut op_id, "viewer-only", "default", Role::Viewer).await;

    let port = reserve_port();
    let response = client
        .post(format!("http://{admin}/imposters"))
        .header("authorization", &raw_key)
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter");
    let seen = Seen::of(response).await;
    assert_eq!(
        seen.status, 403,
        "a Viewer in its own tenant must be refused as forbidden, not not-found: {seen}"
    );

    server.shutdown().await;
}

/// Issue #335, at the wire: the try endpoint reaches a real imposter, and only for someone who
/// may and only for a port they own.
///
/// Everything about the *exchange* is unit-tested against a canned server in `admin_front`. What
/// can only be proved here is the part that involves a real cluster: that the route classifies,
/// authorizes, resolves the imposter's own port, and comes back with what that imposter actually
/// served — and that a Viewer and a foreign port do not get there at all.
#[tokio::test]
async fn a_try_reaches_the_imposter_only_for_a_role_and_a_port_that_may() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // A real imposter in `default` with two stubs, both seeded at config time. The `/boom` stub
    // must come **first**: the catch-all below it has no predicates, so it matches everything and
    // would shadow anything placed after it.
    let port = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(json!({
                "port": port,
                "protocol": "http",
                "stubs": [
                    {
                        "id": "boom",
                        "predicates": [{ "equals": { "path": "/boom" } }],
                        "responses": [{ "is": { "statusCode": 503, "body": "down" } }],
                    },
                    { "id": "a", "responses": [{ "is": { "statusCode": 200, "body": "hi" } }] },
                ],
            }))
            .expect("config parses"),
        },
    )
    .await;
    op_id += 1;

    let operator =
        seed_bound_principal(node, &mut op_id, "try-operator", "default", Role::Operator).await;
    let viewer =
        seed_bound_principal(node, &mut op_id, "try-viewer", "default", Role::Viewer).await;

    let envelope = json!({ "method": "GET", "path": "/anything" });

    // The happy path: the imposter's own answer comes back, inside a 200.
    let allowed = Seen::of(
        client
            .post(format!("http://{admin}/admin/imposters/{port}/try"))
            .header("authorization", &operator)
            .json(&envelope)
            .send()
            .await
            .expect("try as operator"),
    )
    .await;
    assert_eq!(
        allowed.status, 200,
        "an Operator may try an imposter in its own tenant: {allowed}"
    );
    let body = allowed.json();
    assert_eq!(
        body["status"], 200,
        "the imposter's own status rides in the payload: {allowed}"
    );
    assert_eq!(
        body["body"], "hi",
        "and its body is what `minimal_imposter` serves — proving the exchange really happened \
         against the imposter on its own port, not against some default: {allowed}"
    );
    assert!(
        body["elapsedMs"].is_number(),
        "the timing an operator judges a stub by must be present: {allowed}"
    );

    // The design's central split, asserted at the wire rather than only at `perform_try`: the
    // imposter answering `503` is a *successful* try. The endpoint still says `200` and the `503`
    // rides in the payload, so a client can tell "the mock said 503" from "the mock could not be
    // reached" (which is the endpoint's own `502`).
    let mock_failed = Seen::of(
        client
            .post(format!("http://{admin}/admin/imposters/{port}/try"))
            .header("authorization", &operator)
            .json(&json!({ "method": "GET", "path": "/boom" }))
            .send()
            .await
            .expect("try the 503 stub"),
    )
    .await;
    assert_eq!(
        mock_failed.status, 200,
        "the endpoint succeeded — only the mock failed: {mock_failed}"
    );
    assert_eq!(
        mock_failed.json()["status"],
        503,
        "the imposter's own status must ride in the payload: {mock_failed}"
    );

    // A Viewer gets the curl button (#334), never a server-originated send.
    let refused = Seen::of(
        client
            .post(format!("http://{admin}/admin/imposters/{port}/try"))
            .header("authorization", &viewer)
            .json(&envelope)
            .send()
            .await
            .expect("try as viewer"),
    )
    .await;
    assert_eq!(
        refused.status, 403,
        "a Viewer is refused as forbidden — they are in the tenant, so 404 would be a lie: \
         {refused}"
    );
    assert!(
        refused.body.contains("imposter.try"),
        "the refusal names the action it lacked: {refused}"
    );

    server.shutdown().await;
}

/// Issue #335, the escalation this endpoint most needs to not have: **owning the record is not
/// owning the socket.**
///
/// A `PutImposter` whose bind fails still commits and still reads back — deliberately, so a bind
/// failure cannot wedge the replicated log (`bind_failure_does_not_fail_apply`). So a tenant can
/// hold a committed config for a port that some *other* process on this box is listening on.
///
/// Without the `is_locally_bound` gate that gap is a privilege escalation, not a curiosity: an
/// Editor holds both `ImposterWrite` and `ImposterTry`, so they could claim a port already held by
/// an internal listener — the metrics endpoint, the probe listener, the cluster RPC port — and
/// then use the try to send a request of their choosing to it and read the reply back through the
/// admin API. Every other containment property here is downstream of this one.
///
/// The test stands a plain listener on a port first, commits an imposter for it (which succeeds,
/// and must), then tries it. Nothing may be dialled.
#[tokio::test]
async fn a_try_will_not_dial_a_port_this_node_never_bound() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // Something else is already on this port — standing in for the metrics/probe/RPC listeners an
    // attacker would actually be reaching for. Held for the whole test.
    //
    // Bound to `0.0.0.0`, deliberately, because that is what the imposter manager itself binds
    // (`config.host.unwrap_or("0.0.0.0")`) and it is the only spelling whose collision is refused
    // on every platform. A `127.0.0.1` squatter is *not* equivalent here: BSD accepts a `0.0.0.0`
    // bind alongside it, so the imposter would bind successfully and `is_bound()` would be true —
    // which is exactly the shape `a_try_answers_from_this_nodes_engine_not_from_whoever_holds_loopback`
    // exists to cover, now that issue #344 closed it (the exchange is dispatched in-process to the
    // imposter this gate resolved, never dialled, so there is nothing left for a more-specific
    // socket to win). This test's `0.0.0.0` squatter still stands for the other case, which #344
    // does not touch: a bind that fails outright, so the port really is unbound.
    let squatter = std::net::TcpListener::bind("0.0.0.0:0").expect("bind the squatter");
    let port = squatter.local_addr().expect("addr").port();

    // The commit succeeds even though the bind cannot. That is the documented behaviour, and it is
    // exactly what makes the gate necessary — so assert it rather than assuming it.
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(minimal_imposter(port)).expect("config parses"),
        },
    )
    .await;
    op_id += 1;
    assert_eq!(
        node.owning_tenant(port).expect("read"),
        Some(TenantId::default()),
        "the record committed despite the bind failing — if this ever stops being true, the gate \
         under test is still correct but this test no longer proves anything"
    );

    let operator =
        seed_bound_principal(node, &mut op_id, "squat-op", "default", Role::Operator).await;

    let seen = Seen::of(
        client
            .post(format!("http://{admin}/admin/imposters/{port}/try"))
            .header("authorization", &operator)
            .json(&json!({ "method": "GET", "path": "/metrics" }))
            .send()
            .await
            .expect("try the squatted port"),
    )
    .await;

    assert_eq!(
        seen.status, 502,
        "a port this node never bound must be refused, not dialled: {seen}"
    );
    assert!(
        seen.body.contains("not bound"),
        "and the refusal must say why: {seen}"
    );
    // The squatter never accepted a connection, which is the real assertion — the status above
    // could in principle be reached by dialling and failing.
    squatter
        .set_nonblocking(true)
        .expect("non-blocking for the accept probe");
    assert!(
        squatter.accept().is_err(),
        "the try connected to a socket this tenant does not actually own"
    );

    server.shutdown().await;
}

/// Issue #344, the hole issue #335 left open on BSD/macOS: **the try answers from this node's
/// engine, never from whoever happens to hold loopback.**
///
/// The imposter manager binds `0.0.0.0:{port}`; BSD accepts that alongside a foreign socket on
/// `127.0.0.1:{port}`, and the kernel routes a loopback dial to the more specific one. So with the
/// old loopback dial, `is_bound()` was true, the gate passed, and the try reached the squatter.
/// Since #344 the try is dispatched in-process to the imposter `is_locally_bound` consulted, over
/// an in-memory connection — there is no dial to misroute.
///
/// Platform-honest: on BSD/macOS the imposter binds and the try must answer with **its** stub
/// while the squatter accepts nothing; on Linux the colliding bind is refused, so the imposter is
/// not bound and the try is refused with the same `502` `a_try_will_not_dial_...` pins — and the
/// squatter still accepts nothing. Both branches prove the one thing this test exists for: no
/// connection ever reaches the socket the tenant does not own. The branch is chosen by asking the
/// node, not by `cfg!`, so a platform that starts behaving like the other is still tested truly.
#[tokio::test]
async fn a_try_answers_from_this_nodes_engine_not_from_whoever_holds_loopback() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // The more-specific socket: exactly the shape `a_try_will_not_dial_...` records as the
    // residual it could not close. Held for the whole test; the accept probe at the end is the
    // real assertion.
    let squatter = std::net::TcpListener::bind("127.0.0.1:0").expect("bind the squatter");
    let port = squatter.local_addr().expect("addr").port();
    squatter
        .set_nonblocking(true)
        .expect("non-blocking for the accept probe");

    let mut config = minimal_imposter(port);
    config["stubs"] = json!([{
        "responses": [{ "is": { "statusCode": 200, "body": "{\"from\":\"imposter\"}" } }]
    }]);
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(config).expect("config parses"),
        },
    )
    .await;
    op_id += 1;
    let operator =
        seed_bound_principal(node, &mut op_id, "loopback-op", "default", Role::Operator).await;

    let seen = Seen::of(
        client
            .post(format!("http://{admin}/admin/imposters/{port}/try"))
            .header("authorization", &operator)
            .json(&json!({ "method": "GET", "path": "/anything" }))
            .send()
            .await
            .expect("try the port"),
    )
    .await;

    // The real assertion, before anything else touches the port: the try connected to nothing.
    // Under the old loopback dial this is where BSD/macOS failed — the kernel handed the dial to
    // the more specific socket, i.e. the squatter.
    assert!(
        squatter.accept().is_err(),
        "the try must never connect to a socket the tenant does not own"
    );

    if node.is_locally_bound(port) {
        // BSD/macOS: the wildcard bind coexisted with the squatter, and the try still answered
        // from the imposter — the engine's stub, not whoever holds 127.0.0.1.
        assert_eq!(
            seen.status, 200,
            "the try answers from the imposter this node serves: {seen}"
        );
        assert_eq!(seen.json()["status"], 200, "{seen}");
        assert_eq!(
            seen.json()["body"],
            "{\"from\":\"imposter\"}",
            "the imposter's own stub answered, not whoever holds 127.0.0.1: {seen}"
        );
        // Informational, not asserted: show whether a plain loopback dial reaches the squatter
        // on this box (the loophole this test closes). BSD routes it to the more specific
        // socket; a `SO_REUSEPORT` group or a busy box can occasionally hand it elsewhere, which
        // is exactly why the property under test is "the try dials nothing", not "the dial goes
        // to the squatter".
        let probe = std::net::TcpStream::connect(("127.0.0.1", port));
        let reached_squatter = squatter.accept().is_ok();
        eprintln!(
            "loopback probe: connect={} reached_squatter={reached_squatter}",
            probe.is_ok()
        );
    } else {
        // Linux: the colliding bind was refused, so the record exists and the socket does not —
        // the `is_locally_bound` gate refuses, exactly as `a_try_will_not_dial_...` pins.
        assert_eq!(seen.status, 502, "{seen}");
        assert!(seen.body.contains("not bound"), "{seen}");
    }

    server.shutdown().await;
}

/// Issue #344: a stub that injects a connection-level fault has no response on the wire — the
/// serve loop resets or closes the socket. In-process there is no socket to reset, so the try
/// says what the wire would have done rather than presenting the engine's carrier `502` as the
/// imposter's answer.
#[tokio::test]
async fn a_connection_fault_stub_is_an_explicit_transport_outcome() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let port = reserve_port();
    let mut config = minimal_imposter(port);
    config["stubs"] = json!([{ "responses": [{ "fault": "CONNECTION_RESET_BY_PEER" }] }]);
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(config).expect("config parses"),
        },
    )
    .await;
    op_id += 1;
    let operator =
        seed_bound_principal(node, &mut op_id, "fault-op", "default", Role::Operator).await;

    let seen = Seen::of(
        client
            .post(format!("http://{admin}/admin/imposters/{port}/try"))
            .header("authorization", &operator)
            .json(&json!({ "method": "GET", "path": "/reset" }))
            .send()
            .await
            .expect("try the fault stub"),
    )
    .await;

    assert_eq!(
        seen.status, 502,
        "a connection fault is the endpoint reporting no response, not a 200: {seen}"
    );
    assert!(
        seen.body.contains("CONNECTION_RESET_BY_PEER"),
        "the outcome names the fault the stub injects: {seen}"
    );

    server.shutdown().await;
}

/// Issue #335: the endpoint must not become a port scanner.
///
/// The containment claim is that a try can only reach an imposter the caller can already see, and
/// that "not yours" is indistinguishable from "not there". If the two 404s differed by so much as
/// a header, sweeping the port range would map which ports other tenants hold — with the added
/// sting that this is the one endpoint that would then *dial* them.
#[tokio::test]
async fn an_unknown_and_a_foreign_port_are_the_same_404_to_a_try() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // A real imposter, owned by `default`.
    let owned_by_default = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(minimal_imposter(owned_by_default))
                .expect("config parses"),
        },
    )
    .await;
    op_id += 1;

    seed(node, op_id, tenant_put("acme", "Acme Corp")).await;
    op_id += 1;
    // Genuinely bound to `acme`, and genuinely holds `imposter.try` there.
    let acme = seed_bound_principal(node, &mut op_id, "acme-op", "acme", Role::Operator).await;

    let envelope = json!({ "method": "GET", "path": "/anything" });
    let try_as_acme = |port: u16| {
        client
            .post(format!("http://{admin}/admin/imposters/{port}/try"))
            .header("authorization", &acme)
            .header("x-rift-tenant", "acme")
            .json(&envelope)
            .send()
    };

    // A port that belongs to another tenant...
    let foreign = Seen::of(try_as_acme(owned_by_default).await.expect("foreign port")).await;
    // ...and one that belongs to nobody at all.
    let unknown = Seen::of(try_as_acme(reserve_port()).await.expect("unknown port")).await;

    assert_eq!(foreign.status, 404, "another tenant's port: {foreign}");
    assert_eq!(unknown.status, 404, "a port nobody holds: {unknown}");
    assert_eq!(
        foreign.body, unknown.body,
        "a caller must not be able to tell an existing port from an absent one by body: \
         {foreign} vs {unknown}"
    );
    assert_eq!(
        headers_excluding_date(&foreign),
        headers_excluding_date(&unknown),
        "nor by headers: {foreign} vs {unknown}"
    );

    server.shutdown().await;
}

/// An unauthenticated request to a path nothing classifies must still `401`,
/// never `404` — a `404` there would make the admin plane an unauthenticated
/// route-existence oracle (RFC-002 §4.3, mirroring upstream's own
/// authenticate-before-route-parse contract).
#[tokio::test]
async fn unauthenticated_unknown_path_is_401_not_404() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // A principal must exist somewhere in the fleet, or the admin plane's
    // pre-#161 open bypass applies and there is nothing to be unauthenticated
    // against — see `no_fleet_wide_bypass_...` below for that state.
    let _raw_key = seed_bound_principal(node, &mut op_id, "someone", "default", Role::Viewer).await;

    for auth in [None, Some("not-a-real-credential")] {
        let mut request = client.get(format!("http://{admin}/definitely-not-a-route"));
        if let Some(auth) = auth {
            request = request.header("authorization", auth);
        }
        let seen = Seen::of(request.send().await.expect("request")).await;
        assert_eq!(seen.status, 401, "credential {auth:?}: {seen}");
    }

    server.shutdown().await;
}

/// RFC-002 §7: the gateway is a stated non-goal for authentication. Nothing
/// about restructuring `admin_front::handle` to authorize every other route
/// may start gating this one.
#[tokio::test]
async fn gateway_traffic_reaches_the_engine_with_no_credential() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let mut op_id = 1u128;

    // A principal exists, so an admin route would 401 without a credential —
    // proving the gateway's exemption is real and not just "nothing is
    // configured yet".
    let _raw_key = seed_bound_principal(node, &mut op_id, "someone", "default", Role::Viewer).await;

    let port = reserve_port();
    let response = reqwest::get(format!("http://{admin}/__rift/{port}/anything"))
        .await
        .expect("gateway request");
    assert_ne!(
        response.status().as_u16(),
        401,
        "the gateway must never demand an admin credential"
    );

    server.shutdown().await;
}

/// RFC-002 §8.1: the sharpest edge. An Editor of tenant A naming tenant B in
/// `X-Rift-Tenant` on a create must not acquire a resource in B — the header
/// selects among existing bindings, it never grants one.
///
/// The Editor's own tenant is `default`, not a second `TenantPut`-created
/// one: T1 (#159) still only syncs `default`-tenant configs onto the local
/// engine ("storing is not serving in this slice" — `control::validate`'s
/// doc), so a create committed under any other tenant would durably commit
/// but never bind, and the render step's post-commit re-read would 404 for a
/// reason unrelated to authorization. Using `default` for the *successful*
/// half keeps this test about the §8.1 boundary, not about T1's serving
/// scope.
#[tokio::test]
async fn an_editor_cannot_use_x_rift_tenant_to_create_into_a_tenant_it_is_not_bound_to() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    seed(node, op_id, tenant_put("acme", "Acme Corp")).await;
    op_id += 1;
    // Editor of default only — never bound to acme.
    let raw_key =
        seed_bound_principal(node, &mut op_id, "default-editor", "default", Role::Editor).await;

    let stolen_port = reserve_port();
    let response = client
        .post(format!("http://{admin}/imposters"))
        .header("authorization", &raw_key)
        .header("x-rift-tenant", "acme")
        .json(&minimal_imposter(stolen_port))
        .send()
        .await
        .expect("post imposter into an unbound tenant");
    let seen = Seen::of(response).await;
    assert_eq!(
        seen.status, 404,
        "an Editor of default naming acme must be refused as not-bound, and nothing created: {seen}"
    );

    // The same Editor, naming its own tenant, succeeds — proving the refusal
    // above is about *which* tenant, not a blanket denial.
    let own_port = reserve_port();
    let response = client
        .post(format!("http://{admin}/imposters"))
        .header("authorization", &raw_key)
        .header("x-rift-tenant", "default")
        .json(&minimal_imposter(own_port))
        .send()
        .await
        .expect("post imposter into its own tenant");
    assert_eq!(
        response.status().as_u16(),
        201,
        "an Editor may still create in its own tenant"
    );

    server.shutdown().await;
}

/// Issue #253: the three source-write routes require `imposter.write` (`putSource`,
/// `pullSource`) or `imposter.delete` (`deleteSource`) — Editor and up; Operator and Viewer are
/// refused with the same `403` this file's other write probes use, never the `404` reserved for
/// a tenant boundary the caller genuinely isn't bound to (this test stays inside `default`
/// throughout, so every refusal here is role-insufficiency, not tenancy).
///
/// Each sub-probe uses its own id, declared by nobody, so the three are independent of each
/// other's outcome and of ordering:
///
/// - `putSource` uses a `file:` URI (the composed server always registers `FileSource`) that this
///   build never has to dereference — declaring a source only checks the scheme is registered, so
///   the assertion is purely about RBAC, not about a real file existing.
/// - `pullSource` targets an id nobody declared, so an Editor who clears the gate gets the honest
///   `404` for "no such source" (`PullError::UnknownSource`) rather than actually fetching
///   anything — deterministic and network-free.
/// - `deleteSource` targets an id nobody declared too: `SourceDelete` is idempotent on an absent
///   id (`SourcePuller::delete`'s own doc, mirroring `DeleteImposter`), so an Editor sees `200`
///   for a delete that removed nothing.
#[tokio::test]
async fn source_write_routes_require_editor_or_above() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let cases = [
        (Role::Viewer, false),
        (Role::Operator, false),
        (Role::Editor, true),
        (Role::TenantAdmin, true),
        (Role::FleetAdmin, true),
    ];

    for (role, allowed) in cases {
        let label = format!("source-rbac-{role:?}");
        let raw_key = seed_bound_principal(node, &mut op_id, &label, "default", role).await;

        // POST /admin/sources — Action::ImposterWrite.
        let response = client
            .post(format!("http://{admin}/admin/sources"))
            .header("authorization", &raw_key)
            .json(&serde_json::json!({
                "id": format!("put-{role:?}"),
                "uri": "file:///rbac-test-never-dereferenced.json",
            }))
            .send()
            .await
            .expect("post source");
        let seen = Seen::of(response).await;
        /*
         * An RBAC test asserts the GATE, not the body validator. A refused role gets exactly 403;
         * an allowed one gets whatever the handler makes of the request — here a 400, because the
         * fixture URI names no host and the source validator says so. That 400 is itself the proof
         * the gate let it through, and pinning 200 would couple this file to what makes a valid
         * source URI, which is `sources_front.rs`'s business and would break here every time that
         * changed.
         */
        if allowed {
            assert_ne!(
                seen.status, 403,
                "{role:?} POST /admin/sources (ImposterWrite) must not be refused: {seen}"
            );
        } else {
            assert_eq!(
                seen.status, 403,
                "{role:?} POST /admin/sources (ImposterWrite): {seen}"
            );
        }

        // POST /admin/sources/:id/pull — Action::ImposterWrite, unknown id.
        let response = client
            .post(format!("http://{admin}/admin/sources/pull-{role:?}/pull"))
            .header("authorization", &raw_key)
            .send()
            .await
            .expect("pull source");
        let seen = Seen::of(response).await;
        // Same reasoning: an allowed role reaches the handler and gets a 404 for the unknown id —
        // what matters here is only that it is not a 403.
        if allowed {
            assert_ne!(
                seen.status, 403,
                "{role:?} POST /admin/sources/:id/pull (ImposterWrite) must not be refused: {seen}"
            );
        } else {
            assert_eq!(
                seen.status, 403,
                "{role:?} POST /admin/sources/:id/pull (ImposterWrite): {seen}"
            );
        }

        // DELETE /admin/sources/:id — Action::ImposterDelete, unknown id.
        let response = client
            .delete(format!("http://{admin}/admin/sources/delete-{role:?}"))
            .header("authorization", &raw_key)
            .send()
            .await
            .expect("delete source");
        let seen = Seen::of(response).await;
        // `ImposterDelete`, not `ImposterWrite` — the one place these three routes' actions differ,
        // and the reason an Editor-but-not-deleter would show up here rather than above.
        if allowed {
            assert_ne!(
                seen.status, 403,
                "{role:?} DELETE /admin/sources/:id (ImposterDelete) must not be refused: {seen}"
            );
        } else {
            assert_eq!(
                seen.status, 403,
                "{role:?} DELETE /admin/sources/:id (ImposterDelete): {seen}"
            );
        }
    }

    server.shutdown().await;
}

/// `rift_cluster_no_principals` (issue #161): observable and correct before
/// and after a `PrincipalPut` — the gauge that makes the open-admin-plane
/// bypass an audited state rather than a silent one.
///
/// Reads straight off the global `prometheus` registry `rift_cluster::metrics`
/// registers into, in-process — the same technique that crate's own tests
/// use, and the reason the workspace pins one `prometheus` version.
#[tokio::test]
async fn no_principals_gauge_flips_when_a_principal_is_seeded() {
    fn gauge() -> Option<f64> {
        prometheus::gather()
            .into_iter()
            .find(|family| family.get_name() == "rift_cluster_no_principals")?
            .get_metric()
            .first()
            .map(|m| m.get_gauge().get_value())
    }

    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");

    assert_eq!(
        gauge(),
        Some(1.0),
        "a fresh fleet with no principals must report the gauge, not just internally know it"
    );

    seed(node, 1, tenant_put("acme", "Acme Corp")).await;
    seed(
        node,
        2,
        principal_put("acme", principal_with_key("someone", "irrelevant-key")),
    )
    .await;

    // The periodic sampler (5s in production) drives this from here; poll
    // rather than sleep a fixed window, so the test is fast when the sampler
    // is prompt and only as slow as it has to be otherwise.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if gauge() == Some(0.0) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rift_cluster_no_principals never flipped to 0 after a PrincipalPut"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    server.shutdown().await;
}

/// A fleet with no `--api-key` and no principal keeps today's behavior — the
/// pre-#161 open admin plane — so an upgrade never starts denying a fleet
/// that never set up authorization.
#[tokio::test]
async fn an_unconfigured_fleet_stays_open_admin_by_default() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();

    let port = reserve_port();
    let response = reqwest::Client::new()
        .post(format!("http://{admin}/imposters"))
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter with no credential at all");
    let seen = Seen::of(response).await;
    assert_eq!(
        seen.status, 201,
        "no principals and no --api-key must mean open, not 401: {seen}"
    );

    server.shutdown().await;
}

/// RFC-002 §3.4's staged migration: the legacy `--api-key` maps to a
/// synthetic principal bound `TenantAdmin` on `default`, and
/// `--cluster-legacy-key-is-fleet-admin` (default true) additionally grants
/// `FleetAdmin` — which is what `ClusterAdmin`-tier actions need.
#[tokio::test]
async fn the_legacy_api_key_defaults_to_fleet_admin_and_can_be_downgraded() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &["--api-key", "legacy-secret"]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let client = reqwest::Client::new();

    // TenantAdmin-tier: an ordinary create, on `default`, with no header.
    let port = reserve_port();
    let response = client
        .post(format!("http://{admin}/imposters"))
        .header("authorization", "legacy-secret")
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter with the legacy key");
    assert_eq!(response.status().as_u16(), 201);

    // ClusterAdmin-tier: `/config` classifies as `system.read`, mapped to
    // `Action::ClusterAdmin` — FleetAdmin only. The default-true flag must
    // grant it.
    let response = client
        .get(format!("http://{admin}/config"))
        .header("authorization", "legacy-secret")
        .send()
        .await
        .expect("get config with the legacy key");
    assert_eq!(
        response.status().as_u16(),
        200,
        "the legacy key defaults to fleet-admin, so a cluster-tier read must succeed \
         (its sibling below correctly pins the off-state to 403; this one must pin \
         the on-state to 200, not merely to 'not 403')"
    );

    server.shutdown().await;
}

/// The staged-off end of the same migration: with the flag explicitly
/// disabled, the legacy key keeps `default`-tenant access but loses the
/// fleet-wide grant.
#[tokio::test]
async fn the_legacy_api_key_loses_fleet_admin_when_the_flag_is_off() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(
        &state,
        &[
            "--api-key",
            "legacy-secret",
            "--cluster-legacy-key-is-fleet-admin",
            "false",
        ],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr();
    let client = reqwest::Client::new();

    // Still TenantAdmin on default: an ordinary create still works.
    let port = reserve_port();
    let response = client
        .post(format!("http://{admin}/imposters"))
        .header("authorization", "legacy-secret")
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter with the legacy key");
    assert_eq!(response.status().as_u16(), 201);

    // No longer fleet-admin: a cluster-tier read is now forbidden.
    let response = client
        .get(format!("http://{admin}/config"))
        .header("authorization", "legacy-secret")
        .send()
        .await
        .expect("get config with the legacy key");
    assert_eq!(
        response.status().as_u16(),
        403,
        "with the flag off, the legacy key must lose the fleet-wide grant"
    );

    server.shutdown().await;
}

/// Issue #161, blocker B4: a fleet mid-migration off `--api-key`, with real
/// principals also defined, must not 401 every request from those principals.
///
/// Before the fix, `cli.oss.api_key` (upstream's own raw compare, gated on
/// the loopback admin *before* `EeAuthorizer` ever runs) still carried
/// `--api-key`'s value even though `admin_front` had already authenticated
/// and authorized the request under its own credential. A real principal's
/// key is never equal to the legacy `--api-key` string, so upstream's compare
/// 401'd every proxied request and every terminated write's post-commit
/// render — the entire admin surface, for every principal but the one
/// holding the legacy key. This drives both a terminated write (`POST
/// /imposters`) and a proxied read (`GET /config`, which maps to
/// `Action::ClusterAdmin` — hence a `FleetAdmin` principal here) through a
/// fleet that has both `--api-key` and a real principal configured, using
/// only the principal's own key.
#[tokio::test]
async fn a_real_principals_own_key_is_not_401d_by_the_legacy_api_key_gate() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(
        &state,
        &["--api-key", "legacy-secret-for-other-clients"],
    ))
    .await
    .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // A real principal, bound FleetAdmin, whose key is deliberately NOT the
    // configured `--api-key` — the exact fleet-mid-migration shape.
    let raw_key = seed_bound_principal(
        node,
        &mut op_id,
        "real-principal",
        "default",
        Role::FleetAdmin,
    )
    .await;

    // Terminated write.
    let port = reserve_port();
    let response = client
        .post(format!("http://{admin}/imposters"))
        .header("authorization", &raw_key)
        .json(&minimal_imposter(port))
        .send()
        .await
        .expect("post imposter with the principal's own key");
    let seen = Seen::of(response).await;
    assert_eq!(
        seen.status, 201,
        "a real principal's own key must not be 401'd by the unrelated --api-key: {seen}"
    );

    // Proxied read, at the tier only the legacy api-key gate (not RBAC) used
    // to be able to refuse: if the raw compare still ran, this would 401
    // even though `admin_front` and `EeAuthorizer` both already allow it.
    let response = client
        .get(format!("http://{admin}/config"))
        .header("authorization", &raw_key)
        .send()
        .await
        .expect("get config with the principal's own key");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "{seen}");

    // The loopback must still be gated, not thrown open: a credential that
    // resolves to nobody — neither the legacy key nor a stored principal —
    // must still be refused.
    let response = client
        .get(format!("http://{admin}/config"))
        .header("authorization", "not-anybodys-key")
        .send()
        .await
        .expect("get config with a bogus credential");
    assert_eq!(
        response.status().as_u16(),
        401,
        "clearing --api-key from the loopback's own gate must not leave it open; \
         EeAuthorizer is the gate now"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Issue #163 — who may read `GET /admin/audit`, and what they see.
//
// RFC-002 §9 gives `AuditRead` its own action, deliberately *not* folded into
// `TenantManage` (§4.1): reading who did what and changing who may do what are
// different powers, so a principal-manager is not automatically an auditor. The
// tenant narrowing is server-side — the alternative is a server that has already
// sent another tenant's audit history and is trusting the client to hide it.
// ---------------------------------------------------------------------------

/// AC5. One server, three callers, three different answers — driven over real
/// HTTP because the claim is about what the wire returns, not what the evaluator
/// concludes.
#[tokio::test]
async fn the_audit_stream_is_visible_by_role_and_scoped_by_tenant() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 9_000u128;

    seed(node, op_id, tenant_put("acme", "Acme")).await;
    op_id += 1;
    seed(node, op_id, tenant_put("globex", "Globex")).await;
    op_id += 1;

    let editor = seed_bound_principal(node, &mut op_id, "audit-editor", "acme", Role::Editor).await;
    let acme_admin =
        seed_bound_principal(node, &mut op_id, "audit-acme", "acme", Role::TenantAdmin).await;
    let fleet_admin =
        seed_bound_principal(node, &mut op_id, "audit-fleet", "acme", Role::FleetAdmin).await;

    // Two writes, one per tenant, so a leak is visible rather than hypothetical.
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::new("acme"),
            config: Box::new(
                serde_json::from_value(minimal_imposter(19081)).expect("config parses"),
            ),
        },
    )
    .await;
    op_id += 1;
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::new("globex"),
            config: Box::new(
                serde_json::from_value(minimal_imposter(19082)).expect("config parses"),
            ),
        },
    )
    .await;

    // `X-Rift-Tenant` names the tenant the caller is acting as, exactly as it
    // does on every other tenant-scoped route: `GET /admin/audit` carries no
    // tenant in its path, so it has nothing else to go on.
    let read_audit = |key: String, tenant: &'static str| {
        let client = client.clone();
        async move {
            client
                .get(format!("http://{admin}/admin/audit"))
                .header("authorization", key)
                .header("x-rift-tenant", tenant)
                .send()
                .await
                .expect("audit read")
        }
    };

    // An Editor may write imposters all day and still not read who did.
    let response = read_audit(editor, "acme").await;
    assert_eq!(
        response.status().as_u16(),
        403,
        "AuditRead is not part of the Editor tier: reading who did what and \
         doing it are different powers"
    );

    // A TenantAdmin sees its own tenant and nothing else.
    let response = read_audit(acme_admin, "acme").await;
    assert_eq!(response.status().as_u16(), 200);
    let rows: Vec<serde_json::Value> = response.json().await.expect("audit body is JSON");
    assert!(
        !rows.is_empty(),
        "a tenant admin must see its own tenant's rows"
    );
    assert!(
        rows.iter().all(|r| r["tenant"] == "acme"),
        "a tenant admin must never receive another tenant's audit history: {rows:?}"
    );
    assert!(
        rows.iter().any(|r| r["resource"] == "19081"),
        "and must see its own write: {rows:?}"
    );

    // A FleetAdmin sees the fleet.
    let response = read_audit(fleet_admin, "acme").await;
    assert_eq!(response.status().as_u16(), 200);
    let fleet_rows: Vec<serde_json::Value> = response.json().await.expect("audit body is JSON");
    assert!(
        fleet_rows.iter().any(|r| r["tenant"] == "globex"),
        "a fleet admin sees every tenant's rows: {fleet_rows:?}"
    );
    assert!(
        fleet_rows.len() > rows.len(),
        "the fleet view is strictly wider than one tenant's"
    );

    server.shutdown().await;
}

/// The isolation claim, tested against the implementation that would break it.
///
/// A principal bound `TenantAdmin` in **two** tenants is the case where deriving
/// the row filter by scanning bindings for the first tenant-admin binding — the
/// obvious implementation, and the one this code deliberately does not use —
/// silently serves the wrong tenant while looking perfectly authorized. With one
/// binding per principal (every other test here) that bug is invisible, because
/// the first match is always the right one.
#[tokio::test]
async fn a_principal_bound_to_two_tenants_sees_only_the_one_it_is_acting_as() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 9_800u128;

    seed(node, op_id, tenant_put("acme", "Acme")).await;
    op_id += 1;
    seed(node, op_id, tenant_put("globex", "Globex")).await;
    op_id += 1;

    // One principal, TenantAdmin in both tenants.
    let raw_key = "rbac-key-audit-dual".to_owned();
    let principal = principal_with_key("audit-dual", &raw_key);
    let principal_id = principal.id.clone();
    seed(node, op_id, principal_put("acme", principal)).await;
    op_id += 1;
    for tenant in ["acme", "globex"] {
        seed(
            node,
            op_id,
            binding_put(tenant, &principal_id, Role::TenantAdmin),
        )
        .await;
        op_id += 1;
    }

    for (tenant, port) in [("acme", 19181u16), ("globex", 19182)] {
        seed(
            node,
            op_id,
            ControlOp::PutImposter {
                tenant: TenantId::new(tenant),
                config: Box::new(
                    serde_json::from_value(minimal_imposter(port)).expect("config parses"),
                ),
            },
        )
        .await;
        op_id += 1;
    }

    // Acting as each tenant in turn must yield that tenant's rows and no others.
    for (acting_as, own_port, other_port) in
        [("acme", "19181", "19182"), ("globex", "19182", "19181")]
    {
        let response = client
            .get(format!("http://{admin}/admin/audit"))
            .header("authorization", &raw_key)
            .header("x-rift-tenant", acting_as)
            .send()
            .await
            .expect("audit read");
        assert_eq!(response.status().as_u16(), 200, "acting as {acting_as}");
        let rows: Vec<serde_json::Value> = response.json().await.expect("audit body is JSON");

        assert!(
            rows.iter().all(|r| r["tenant"] == acting_as),
            "acting as {acting_as}, every row must be {acting_as}'s: {rows:?}"
        );
        assert!(
            rows.iter().any(|r| r["resource"] == own_port),
            "acting as {acting_as}, its own write must be present: {rows:?}"
        );
        assert!(
            !rows.iter().any(|r| r["resource"] == other_port),
            "acting as {acting_as}, the other tenant's write must NOT be: {rows:?}"
        );
    }

    server.shutdown().await;
}

/// AC7, as far as the §9 design permits it. The criteria ask for a
/// `ScenarioReset` row; that mutation is **proxied** to the loopback core admin
/// and never becomes a `ControlOp`, so a log-derived projection cannot see it.
/// Producing one would mean recording at the front door — a second, per-node
/// audit path that can disagree with the log, which is the one thing §9 refuses.
/// What is real and asserted here is the distinction the criterion is actually
/// about: a replicated **write** is audited, and a plain **read** is not.
#[tokio::test]
async fn a_replicated_write_is_audited_and_a_plain_read_is_not() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 9_500u128;

    let auditor =
        seed_bound_principal(node, &mut op_id, "audit-reads", "default", Role::FleetAdmin).await;

    let before = node.audit_since(0, None, 10_000).expect("read audit").len();

    // A read over the admin surface.
    let response = client
        .get(format!("http://{admin}/imposters"))
        .header("authorization", &auditor)
        .send()
        .await
        .expect("list imposters");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        node.audit_since(0, None, 10_000).expect("read audit").len(),
        before,
        "reads are not audited in v1 (§9, on log volume)"
    );

    // A write over the same surface.
    let response = client
        .post(format!("http://{admin}/imposters"))
        .header("authorization", &auditor)
        .json(&minimal_imposter(19091))
        .send()
        .await
        .expect("create imposter");
    assert!(
        response.status().is_success(),
        "the write must land: {}",
        response.status()
    );

    let rows = node.audit_since(0, None, 10_000).expect("read audit");
    assert_eq!(
        rows.len(),
        before + 1,
        "a replicated write is audited exactly once: {rows:?}"
    );
    let row = rows.last().expect("the write's row");
    assert_eq!(row.action, "imposter.write");
    assert_eq!(row.resource, "19091");
    assert!(
        row.principal.is_some(),
        "U-10: the row names the principal the admin front authenticated, not \
         an anonymous write: {row:?}"
    );

    server.shutdown().await;
}

/// Issue #182 AC5: the imposter listing is narrowed to the caller's own tenant — **including
/// `default`**.
///
/// The listing is the one proxied read the ownership gate cannot cover: it names no port, so there
/// is nothing to check ownership of, and it goes verbatim to a local engine that now binds every
/// tenant's imposters. Without the response filter an authorized caller would be handed the whole
/// fleet's imposters, which is the same leak the per-port gate closes, reached by a route that has
/// no port.
///
/// The `default` half is the deliberate behaviour change: before this, `default` saw everything
/// because everything *was* default's.
#[tokio::test]
async fn an_imposter_listing_shows_only_the_callers_own_tenant() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let default_port = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(minimal_imposter(default_port)).expect("config parses"),
        },
    )
    .await;
    op_id += 1;

    seed(node, op_id, tenant_put("acme", "Acme Corp")).await;
    op_id += 1;
    let acme_port = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::new("acme"),
            config: serde_json::from_value(minimal_imposter(acme_port)).expect("config parses"),
        },
    )
    .await;
    op_id += 1;

    let acme_key = seed_bound_principal(node, &mut op_id, "acme-admin", "acme", Role::Viewer).await;
    let default_key =
        seed_bound_principal(node, &mut op_id, "default-reader", "default", Role::Viewer).await;

    let ports_seen_by = |key: String, tenant: Option<&'static str>| {
        let client = client.clone();
        async move {
            let mut request = client
                .get(format!("http://{admin}/imposters"))
                .header("authorization", key);
            if let Some(tenant) = tenant {
                request = request.header("x-rift-tenant", tenant);
            }
            let seen = Seen::of(request.send().await.expect("list imposters")).await;
            assert_eq!(seen.status, 200, "{seen}");
            seen.json()["imposters"]
                .as_array()
                .expect("an imposters array")
                .iter()
                .filter_map(|e| e["port"].as_u64())
                .collect::<Vec<_>>()
        }
    };

    let acme_sees = ports_seen_by(acme_key, Some("acme")).await;
    assert_eq!(
        acme_sees,
        vec![u64::from(acme_port)],
        "acme's listing must contain acme's imposter and nothing else"
    );

    let default_sees = ports_seen_by(default_key, None).await;
    assert_eq!(
        default_sees,
        vec![u64::from(default_port)],
        "default no longer sees other tenants' imposters — the deliberate behaviour change"
    );

    server.shutdown().await;
}

/// Issue #182 AC3: tenancy isolates *administration*, not traffic.
///
/// RFC-002 §7 keeps the data plane open, and this change must not narrow it: an imposter owned by
/// a non-default tenant answers on its own port, with **no credential at all**. That is the half
/// of the tenancy boundary it would be easy to over-enforce while closing the admin one — and the
/// union engine-sync is what makes it true, since the local engine now binds every tenant's ports
/// rather than only `default`'s.
#[tokio::test]
async fn a_tenanted_imposter_answers_the_data_plane_with_no_credential() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let mut op_id = 1u128;

    seed(node, op_id, tenant_put("acme", "Acme Corp")).await;
    op_id += 1;
    let acme_port = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::new("acme"),
            config: serde_json::from_value(minimal_imposter(acme_port)).expect("config parses"),
        },
    )
    .await;

    // The bind happens on the engine-sync that follows the apply, so poll rather than assert once.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = reqwest::get(format!("http://127.0.0.1:{acme_port}/")).await {
            assert_eq!(
                response.status().as_u16(),
                200,
                "acme's imposter must answer the data plane unauthenticated"
            );
            assert_eq!(response.text().await.expect("body"), "hi");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the local engine never bound acme's imposter — union sync did not reach it"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    server.shutdown().await;
}

/// Issue #182, found in review: the **set-level** imposter ops answer with a re-read of the
/// collection, and that body is just as much a listing as `GET /imposters` is.
///
/// `PUT /imposters` renders the set afterwards; `DELETE /imposters` captures it beforehand as
/// "what was removed". Both go through the loopback to an engine that now binds every tenant, so
/// filtering only the proxied `GET` left two routes handing an Editor of one tenant the whole
/// fleet's imposters — with `?replayable=true`, their stubs too. The narrowing lives in `fetch`
/// precisely so these two cannot diverge from the `GET` path again.
#[tokio::test]
async fn set_level_imposter_ops_do_not_answer_with_another_tenants_imposters() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let default_port = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(minimal_imposter(default_port)).expect("config parses"),
        },
    )
    .await;
    op_id += 1;

    seed(node, op_id, tenant_put("acme", "Acme Corp")).await;
    op_id += 1;
    let acme_key =
        seed_bound_principal(node, &mut op_id, "acme-editor", "acme", Role::Editor).await;

    let acme_port = reserve_port();
    let replaced = Seen::of(
        client
            .put(format!("http://{admin}/imposters"))
            .header("authorization", &acme_key)
            .header("x-rift-tenant", "acme")
            .json(&json!({ "imposters": [minimal_imposter(acme_port)] }))
            .send()
            .await
            .expect("wholesale replace as acme"),
    )
    .await;
    assert_eq!(replaced.status, 200, "{replaced}");
    let ports: Vec<u64> = replaced.json()["imposters"]
        .as_array()
        .expect("an imposters array")
        .iter()
        .filter_map(|e| e["port"].as_u64())
        .collect();
    assert!(
        !ports.contains(&u64::from(default_port)),
        "the replace re-read leaked another tenant's imposter: {ports:?}"
    );
    assert_eq!(ports, vec![u64::from(acme_port)]);

    let deleted = Seen::of(
        client
            .delete(format!("http://{admin}/imposters"))
            .header("authorization", &acme_key)
            .header("x-rift-tenant", "acme")
            .send()
            .await
            .expect("delete-all as acme"),
    )
    .await;
    assert_eq!(deleted.status, 200, "{deleted}");
    let deleted_ports: Vec<u64> = deleted.json()["imposters"]
        .as_array()
        .expect("an imposters array")
        .iter()
        .filter_map(|e| e["port"].as_u64())
        .collect();
    assert!(
        !deleted_ports.contains(&u64::from(default_port)),
        "the delete-all capture leaked another tenant's imposter: {deleted_ports:?}"
    );

    // And the delete really was scoped: default's imposter is still there.
    let default_key =
        seed_bound_principal(node, &mut op_id, "default-reader", "default", Role::Viewer).await;
    let survivor = Seen::of(
        client
            .get(format!("http://{admin}/imposters/{default_port}"))
            .header("authorization", &default_key)
            .send()
            .await
            .expect("re-read default's imposter"),
    )
    .await;
    assert_eq!(
        survivor.status, 200,
        "acme's delete-all must not have touched default's imposter: {survivor}"
    );

    server.shutdown().await;
}

/// Issue #182, found in review: the ownership gate must not become a port-existence oracle.
///
/// A port owned by another tenant and a port owned by nobody must answer identically. They did not:
/// the gate renders §8.4's terse "Not Found" while an unowned port fell through to upstream, whose
/// 404 names the port. Sweeping the range would then have mapped exactly which ports other tenants
/// hold — reconnaissance that §8.4 exists to deny.
#[tokio::test]
async fn an_unowned_port_is_indistinguishable_from_another_tenants_port() {
    let _guard = METRICS_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let default_port = reserve_port();
    seed(
        node,
        op_id,
        ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(minimal_imposter(default_port)).expect("config parses"),
        },
    )
    .await;
    op_id += 1;

    seed(node, op_id, tenant_put("acme", "Acme Corp")).await;
    op_id += 1;
    let acme_key = seed_bound_principal(node, &mut op_id, "acme-admin", "acme", Role::Viewer).await;

    let probe = |port: u16| {
        let client = client.clone();
        let key = acme_key.clone();
        async move {
            Seen::of(
                client
                    .get(format!("http://{admin}/imposters/{port}"))
                    .header("authorization", key)
                    .header("x-rift-tenant", "acme")
                    .send()
                    .await
                    .expect("probe"),
            )
            .await
        }
    };

    let owned_by_other = probe(default_port).await;
    let owned_by_nobody = probe(reserve_port()).await;

    assert_eq!(owned_by_other.status, 404, "{owned_by_other}");
    assert_eq!(owned_by_nobody.status, 404, "{owned_by_nobody}");
    assert_eq!(
        owned_by_other.body, owned_by_nobody.body,
        "a port held by another tenant must not be distinguishable from a free one — otherwise \
         sweeping the range maps every other tenant's ports"
    );

    server.shutdown().await;
}
