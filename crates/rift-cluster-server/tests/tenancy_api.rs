//! Issue #162: RFC-002 §5's tenancy admin surface, end to end — key issuance
//! that shows a key once and stores it nowhere, an unknown key that costs zero
//! argon2 work, the §8.4 cross-tenant `404`, the fleet-vs-tenant privilege
//! split, `PrincipalCreate`'s single revision, and `whoami`.
//!
//! These drive `compose::start` and speak plain HTTP to the public admin
//! address, exactly as `rbac.rs` does. Where `rbac.rs` had to seed principals
//! by submitting `ControlOp`s directly through the node — because no admin
//! surface for them existed — these tests use the surface itself, which is
//! what this slice adds.

use std::time::Duration;

use clap::Parser;
use rift_cluster::control::{FLEET_SCOPE, PrincipalId, Quotas, Role, argon2_verifications};
use rift_cluster::{ControlOp, ControlRequest, RaftNode, TenantId};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;

use common::seen::Seen;

const SECRET: &str = "tenancy-test-secret";

/// `control::argon2_verifications` is a **process-global** counter, and
/// `cargo test` runs this file's tests concurrently on threads within one
/// process — so every other test here authenticates into the same number that
/// `an_unknown_key_costs_zero_argon2_verifications` is trying to observe a
/// delta on.
///
/// Without exclusivity that test reads a moving baseline and fails (or, worse,
/// passes) for reasons that have nothing to do with the property. Every test in
/// this file takes the lock for its whole body: the counting test needs
/// exclusivity to assert, and every other test needs to not undermine it. Same
/// shape, and the same reason, as `rbac.rs`'s `METRICS_LOCK`.
static ARGON2_COUNTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A clustered invocation on an explicit cluster `bind`. Mirrors
/// `clustered.rs`'s `cluster_on` — each test binary carries its own copy of
/// small fixtures like this, per `tests/common/mod.rs`.
fn cluster_on(state: &TempDir, bind: &str, extra: &[&str]) -> EeCli {
    let mut args = vec![
        "rift-cluster-server".to_owned(),
        "--port".to_owned(),
        "0".to_owned(),
        "--metrics-port".to_owned(),
        "0".to_owned(),
        "--cluster".to_owned(),
        "--cluster-bind".to_owned(),
        bind.to_owned(),
        "--cluster-probe-bind".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--cluster-secret".to_owned(),
        SECRET.to_owned(),
        "--cluster-state-dir".to_owned(),
        state.path().to_string_lossy().into_owned(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

/// The common case: a solo cluster on an ephemeral port.
fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    let mut args = vec!["--cluster-allow-solo"];
    args.extend_from_slice(extra);
    cluster_on(state, "127.0.0.1:0", &args)
}

/// Poll `node`'s membership until it holds exactly `want` voters, bounded.
async fn wait_voter_count(node: &RaftNode, want: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let voters = node.status().voters;
        if voters.len() == want {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expected {want} voters, saw {voters:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

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

/// Submit `op` directly through the node — fixture setup only. Everything
/// actually *under test* here goes through the HTTP surface.
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

/// The fleet admin every test needs before it can call the surface at all.
/// Seeded through the node, since minting the *first* fleet admin is a
/// bootstrap problem this slice does not solve (RFC-002 §3.4's bypass is what
/// covers it in practice).
async fn seed_fleet_admin(node: &RaftNode, op_id: &mut u128, raw_key: &str) -> PrincipalId {
    let principal = rift_cluster::control::Principal {
        id: rift_cluster::control::api_key_principal_id(raw_key),
        display_name: "fleet".to_owned(),
        auth: rift_cluster::control::AuthSource::ApiKey {
            hash: rift_cluster::control::hash_api_key(raw_key),
        },
        disabled: false,
    };
    let id = principal.id.clone();
    seed(
        node,
        *op_id,
        ControlOp::PrincipalPut {
            tenant: TenantId::default(),
            principal,
        },
    )
    .await;
    *op_id += 1;
    seed(
        node,
        *op_id,
        ControlOp::BindingPut {
            tenant: TenantId::new(FLEET_SCOPE),
            principal_id: id.clone(),
            role: Role::FleetAdmin,
        },
    )
    .await;
    *op_id += 1;
    id
}

/// The `rift-cluster-revision` header's revision component, which every
/// committed write on this surface carries. Format: `<tenant>@<revision>`.
fn revision_of(response: &reqwest::Response) -> u64 {
    let raw = response
        .headers()
        .get("rift-cluster-revision")
        .and_then(|v| v.to_str().ok())
        .expect("a committed write reports its revision");
    raw.rsplit_once('@')
        .map(|(_, rev)| rev)
        .expect("revision header is <tenant>@<revision>")
        .parse()
        .expect("revision is a number")
}

/// Returns the revision the tenant was created at.
async fn create_tenant(client: &reqwest::Client, admin: &str, key: &str, id: &str) -> u64 {
    let response = client
        .post(format!("http://{admin}/admin/tenants"))
        .header("authorization", key)
        .json(&json!({ "id": id, "displayName": id }))
        .send()
        .await
        .expect("create tenant");
    let revision = revision_of(&response);
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 201, "create tenant {id}: {seen}");
    revision
}

/// `POST /admin/tenants/:id/principals`, returning `(principal id, raw key)`.
async fn mint_principal(
    client: &reqwest::Client,
    admin: &str,
    key: &str,
    tenant: &str,
    role: Role,
) -> (String, String) {
    let response = client
        .post(format!("http://{admin}/admin/tenants/{tenant}/principals"))
        .header("authorization", key)
        .json(&json!({ "displayName": "svc", "role": role }))
        .send()
        .await
        .expect("mint principal");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 201, "mint in {tenant}: {seen}");
    let body: Value = serde_json::from_str(&seen.body).expect("json body");
    let id = body["id"].as_str().expect("id").to_owned();
    let raw = body["apiKey"].as_str().expect("apiKey").to_owned();
    (id, raw)
}

/// **The regression that matters** (RFC-002 §8.2): the raw key is in the
/// creation response and nowhere else — not in a later read, and not in the
/// bytes on disk.
///
/// The on-disk scan is the half that would otherwise rot silently. A `GET` that
/// omits the key proves only that this renderer omits it; the log and the
/// snapshot are where a credential would become *permanently* unredactable,
/// because there is no way to rewrite a committed Raft entry.
#[tokio::test]
async fn an_issued_key_is_shown_once_and_never_lands_on_disk() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-once";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;
    create_tenant(&client, &admin, fleet_key, "acme").await;
    let (principal_id, raw_key) =
        mint_principal(&client, &admin, fleet_key, "acme", Role::Editor).await;

    // A later read of the same principal never carries it.
    let response = client
        .get(format!("http://{admin}/admin/tenants/acme/principals"))
        .header("authorization", fleet_key)
        .send()
        .await
        .expect("list principals");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "{seen}");
    assert!(
        seen.body.contains(&principal_id),
        "the principal must be listed: {seen}"
    );
    assert!(
        !seen.body.contains(&raw_key),
        "a read leaked the issued key: {seen}"
    );

    // The key still authenticates, so it really was issued — this is what
    // rules out the test passing because nothing was minted at all.
    let response = client
        .get(format!("http://{admin}/admin/whoami"))
        .header("authorization", &raw_key)
        .send()
        .await
        .expect("whoami");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "the issued key must authenticate: {seen}");
    assert!(seen.body.contains(&principal_id), "{seen}");

    server.shutdown().await;

    // Now the bytes. Every file under the state dir, scanned for the raw key.
    let mut scanned = 0usize;
    let needle = raw_key.as_bytes();
    for entry in walk(state.path()) {
        let bytes = std::fs::read(&entry).expect("read state file");
        scanned += 1;
        assert!(
            !bytes.windows(needle.len()).any(|w| w == needle),
            "the raw key is on disk in {}",
            entry.display()
        );
    }
    assert!(scanned > 0, "the scan found no state files to check");
}

/// Every regular file under `root`, recursively.
fn walk(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                out.push(path);
            }
        }
    }
    out
}

/// A credential whose fingerprint indexes no principal must answer `401`
/// having performed **zero** argon2id verifications.
///
/// argon2id is deliberately expensive in *memory* (19 MiB per attempt at the
/// pinned cost), so an endpoint anyone can reach that hashes on every presented
/// credential is a memory-amplification lever, not merely slow. Asserted with a
/// counter rather than a timer: a timing assertion for this property is flaky by
/// construction and would be the first test anyone disabled.
#[tokio::test]
async fn an_unknown_key_costs_zero_argon2_verifications() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    // A principal must exist, or the request takes the open-admin-plane bypass
    // and never reaches the lookup this test is about.
    seed_fleet_admin(node, &mut op_id, "tenancy-fleet-zero").await;

    // The counter is process-global and other tests in this binary hash
    // concurrently, so the assertion is on the *delta this request causes*
    // being zero — which cannot be measured against a moving baseline. Two
    // reads bracketing one request would race. Instead: assert the request is
    // refused, and that a key whose fingerprint matches nothing cannot reach a
    // verification at all, by construction of the id-keyed lookup — then prove
    // the counter does move for a key that DOES resolve, so a counter stuck at
    // zero cannot make this pass.
    let before = argon2_verifications();
    let response = client
        .get(format!("http://{admin}/admin/whoami"))
        .header("authorization", "rift_this-key-was-never-issued")
        .send()
        .await
        .expect("whoami with an unknown key");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 401, "an unknown key must be refused: {seen}");

    let after_unknown = argon2_verifications();
    assert_eq!(
        after_unknown,
        before,
        "an unknown key performed {} argon2 verification(s); the id-keyed lookup must miss \
         before any hashing happens",
        after_unknown - before
    );

    // The counter is not simply broken: a key that resolves does hash.
    let response = client
        .get(format!("http://{admin}/admin/whoami"))
        .header("authorization", "tenancy-fleet-zero")
        .send()
        .await
        .expect("whoami with a known key");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "{seen}");
    assert!(
        argon2_verifications() > after_unknown,
        "a resolving key must reach the argon2 compare, or the counter proves nothing"
    );

    server.shutdown().await;
}

/// RFC-002 §8.4: probing another tenant's principals must be indistinguishable
/// from probing a tenant that does not exist.
///
/// Status and body are compared. Headers are not enumerated, because both
/// refusals are the *same* `tenant_boundary_not_found()` call — one function,
/// so there are no two header sets to drift apart. What would actually be worth
/// asserting is that the two refusals come from different *reasons*, and that
/// is what the two probes below arrange.
#[tokio::test]
async fn a_cross_tenant_probe_is_indistinguishable_from_a_missing_tenant() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-probe";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;
    create_tenant(&client, &admin, fleet_key, "alpha").await;
    create_tenant(&client, &admin, fleet_key, "beta").await;
    let (_, alpha_key) =
        mint_principal(&client, &admin, fleet_key, "alpha", Role::TenantAdmin).await;

    // Alpha's admin probing beta (which exists, and holds principals).
    let existing = Seen::of(
        client
            .get(format!("http://{admin}/admin/tenants/beta/principals"))
            .header("authorization", &alpha_key)
            .send()
            .await
            .expect("probe beta"),
    )
    .await;

    // ...and probing a tenant that was never created.
    let absent = Seen::of(
        client
            .get(format!("http://{admin}/admin/tenants/ghost/principals"))
            .header("authorization", &alpha_key)
            .send()
            .await
            .expect("probe ghost"),
    )
    .await;

    assert_eq!(existing.status, 404, "cross-tenant probe: {existing}");
    assert_eq!(absent.status, 404, "absent-tenant probe: {absent}");
    assert_eq!(
        existing.body, absent.body,
        "the two refusals differ in body, making the API an oracle for which tenants exist"
    );

    // And the surface still works for the tenant it IS bound to — otherwise
    // this test would pass on a build that 404s everything.
    let own = Seen::of(
        client
            .get(format!("http://{admin}/admin/tenants/alpha/principals"))
            .header("authorization", &alpha_key)
            .send()
            .await
            .expect("probe own tenant"),
    )
    .await;
    assert_eq!(own.status, 200, "alpha's own principals: {own}");

    server.shutdown().await;
}

/// The privilege split: a `TenantAdmin` administers roles **inside** its
/// tenant, and nothing beyond it. Deleting a principal is fleet business
/// (principals are fleet-global, so the delete would take a credential another
/// tenant relies on), and binding on the fleet scope is a grant of fleet
/// privilege.
#[tokio::test]
async fn tenant_admin_may_bind_within_its_tenant_but_not_mint_fleet_privilege() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-split";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;
    create_tenant(&client, &admin, fleet_key, "alpha").await;
    let (admin_id, alpha_admin_key) =
        mint_principal(&client, &admin, fleet_key, "alpha", Role::TenantAdmin).await;
    let (victim_id, _) = mint_principal(&client, &admin, fleet_key, "alpha", Role::Viewer).await;

    // CAN: re-bind a principal within its own tenant.
    let seen = Seen::of(
        client
            .put(format!(
                "http://{admin}/admin/tenants/alpha/bindings/{victim_id}"
            ))
            .header("authorization", &alpha_admin_key)
            .json(&json!({ "role": "editor" }))
            .send()
            .await
            .expect("rebind within alpha"),
    )
    .await;
    assert_eq!(seen.status, 200, "tenant admin may rebind in alpha: {seen}");

    // CAN: revoke a binding within its own tenant.
    let seen = Seen::of(
        client
            .delete(format!(
                "http://{admin}/admin/tenants/alpha/bindings/{victim_id}"
            ))
            .header("authorization", &alpha_admin_key)
            .send()
            .await
            .expect("unbind within alpha"),
    )
    .await;
    assert_eq!(seen.status, 204, "tenant admin may unbind in alpha: {seen}");

    // CANNOT: delete the principal itself — fleet-global namespace.
    let seen = Seen::of(
        client
            .delete(format!(
                "http://{admin}/admin/tenants/alpha/principals/{victim_id}"
            ))
            .header("authorization", &alpha_admin_key)
            .send()
            .await
            .expect("delete principal"),
    )
    .await;
    assert_eq!(
        seen.status, 403,
        "PrincipalDelete is fleet-only (RFC-002 §3): {seen}"
    );

    // CANNOT: grant itself fleet privilege by binding on the fleet scope. A
    // 404 rather than a 403 is correct here and is not an accident: the
    // principal holds no binding on `"*"`, so §8.4's indistinguishable
    // not-found is exactly what a caller with no standing there must see.
    let seen = Seen::of(
        client
            .put(format!(
                "http://{admin}/admin/tenants/{FLEET_SCOPE}/bindings/{admin_id}"
            ))
            .header("authorization", &alpha_admin_key)
            .json(&json!({ "role": "fleet-admin" }))
            .send()
            .await
            .expect("self-promote"),
    )
    .await;
    assert_eq!(
        seen.status, 404,
        "a tenant admin must not reach the fleet scope: {seen}"
    );

    // And the promotion genuinely did not happen.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/whoami"))
            .header("authorization", &alpha_admin_key)
            .send()
            .await
            .expect("whoami"),
    )
    .await;
    assert!(
        !seen.body.contains("fleet-admin"),
        "the refused promotion still landed: {seen}"
    );

    server.shutdown().await;
}

/// `whoami` reflects a binding change on the next call — the cheapest possible
/// check that revocation is immediate rather than cached behind a TTL
/// (RFC-002 §8.5).
#[tokio::test]
async fn whoami_reflects_a_binding_change_on_the_next_call() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-whoami";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;
    create_tenant(&client, &admin, fleet_key, "alpha").await;
    let (subject_id, subject_key) =
        mint_principal(&client, &admin, fleet_key, "alpha", Role::Viewer).await;

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/whoami"))
            .header("authorization", &subject_key)
            .send()
            .await
            .expect("whoami before"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    assert!(seen.body.contains("\"role\":\"viewer\""), "{seen}");

    let seen = Seen::of(
        client
            .put(format!(
                "http://{admin}/admin/tenants/alpha/bindings/{subject_id}"
            ))
            .header("authorization", fleet_key)
            .json(&json!({ "role": "editor" }))
            .send()
            .await
            .expect("promote"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/whoami"))
            .header("authorization", &subject_key)
            .send()
            .await
            .expect("whoami after"),
    )
    .await;
    assert!(
        seen.body.contains("\"role\":\"editor\""),
        "the binding change must be visible on the very next call: {seen}"
    );

    // Revocation too — the direction that actually matters for security.
    let seen = Seen::of(
        client
            .delete(format!(
                "http://{admin}/admin/tenants/alpha/bindings/{subject_id}"
            ))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("revoke"),
    )
    .await;
    assert_eq!(seen.status, 204, "{seen}");

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/whoami"))
            .header("authorization", &subject_key)
            .send()
            .await
            .expect("whoami after revoke"),
    )
    .await;
    assert_eq!(
        seen.status, 200,
        "the credential still authenticates; it simply holds nothing: {seen}"
    );
    assert!(
        seen.body.contains("\"bindings\":[]"),
        "the revoked binding is still reported: {seen}"
    );

    server.shutdown().await;
}

/// A tenant a fleet admin created is readable, listable, and its tombstone is
/// visible after a delete — the surface's own round trip.
#[tokio::test]
async fn the_tenant_surface_round_trips_through_create_read_list_and_delete() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-crud";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;

    let seen = Seen::of(
        client
            .post(format!("http://{admin}/admin/tenants"))
            .header("authorization", fleet_key)
            .json(&json!({
                "id": "acme",
                "displayName": "Acme Corp",
                "quotas": { "maxImposters": 7, "maxStubsPerImposter": 11,
                            "maxFlowEntries": 13 },
            }))
            .send()
            .await
            .expect("create"),
    )
    .await;
    assert_eq!(seen.status, 201, "{seen}");

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/tenants/acme"))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("read"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    let body: Value = serde_json::from_str(&seen.body).expect("json");
    assert_eq!(body["displayName"], "Acme Corp", "{seen}");
    assert_eq!(body["quotas"]["maxImposters"], 7, "{seen}");
    assert_eq!(body["deleted"], false, "{seen}");

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/tenants"))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("list"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    assert!(seen.body.contains("Acme Corp"), "{seen}");

    let seen = Seen::of(
        client
            .delete(format!("http://{admin}/admin/tenants/acme"))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("delete"),
    )
    .await;
    assert_eq!(seen.status, 204, "{seen}");

    // The tombstone stays readable: an id that was used is not the same thing
    // as an id that is free, and this surface is where an operator learns the
    // difference.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/tenants/acme"))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("read after delete"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    let body: Value = serde_json::from_str(&seen.body).expect("json");
    assert_eq!(
        body["deleted"], true,
        "the tombstone must be visible: {seen}"
    );

    server.shutdown().await;
}

/// `POST .../principals` writes the principal and its binding at the **same
/// revision**: a state where one exists without the other is unreachable.
///
/// The assertion is the **revision delta**, not merely that both rows ended up
/// present. "Both rows exist by the time we look" is true of a two-op
/// implementation as well, so checking only that would pass on precisely the
/// design this op exists to rule out. A revision is a log index: the tenant
/// create commits at `R`, and if the mint were a `PrincipalPut` followed by a
/// `BindingPut` it would report `R + 2`. Asserting `R + 1` is what makes the
/// test fail on a regression to two ops.
///
/// Nothing else writes in between — the test holds `ARGON2_COUNTER_LOCK`, the
/// cluster is solo, and both writes are this test's own.
#[tokio::test]
async fn principal_create_writes_both_rows_in_one_revision() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-atomic";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;
    let tenant_revision = create_tenant(&client, &admin, fleet_key, "acme").await;

    let response = client
        .post(format!("http://{admin}/admin/tenants/acme/principals"))
        .header("authorization", fleet_key)
        .json(&json!({ "displayName": "svc", "role": "editor" }))
        .send()
        .await
        .expect("mint");
    let mint_revision = revision_of(&response);
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 201, "{seen}");
    let body: Value = serde_json::from_str(&seen.body).expect("json");
    let id = body["id"].as_str().expect("id");

    assert_eq!(
        mint_revision,
        tenant_revision + 1,
        "the mint advanced the log by {} entries; principal + binding must be ONE op, or a \
         replica can observe a principal with no binding",
        mint_revision - tenant_revision
    );

    // Both rows are present, and the binding is the one the request asked for.
    let principal = node.principal(id).expect("read principal");
    assert!(principal.is_some(), "the principal row must exist");
    let bindings = node.principal_bindings(id).expect("read bindings");
    assert_eq!(
        bindings,
        vec![(TenantId::new("acme"), Role::Editor)],
        "the binding row must exist, at the requested role"
    );

    server.shutdown().await;
}

/// A tenant-scoped mint may not grant fleet privilege: `PrincipalCreate` binds
/// against the path tenant, and `fleet-admin` binds only on `"*"`. Refused
/// rather than silently downgraded — an operator who asked for fleet privilege
/// must learn they did not get it.
#[tokio::test]
async fn a_tenant_scoped_mint_cannot_request_fleet_admin() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-nofleet";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;
    create_tenant(&client, &admin, fleet_key, "acme").await;

    let seen = Seen::of(
        client
            .post(format!("http://{admin}/admin/tenants/acme/principals"))
            .header("authorization", fleet_key)
            .json(&json!({ "displayName": "svc", "role": "fleet-admin" }))
            .send()
            .await
            .expect("mint fleet admin"),
    )
    .await;
    assert_eq!(
        seen.status, 400,
        "even a fleet admin may not mint fleet privilege inside a tenant: {seen}"
    );

    server.shutdown().await;
}

/// Quotas set through the surface land on the record verbatim — the field
/// #163 will enforce against has to arrive intact first.
#[tokio::test]
async fn quotas_written_through_the_surface_reach_the_record() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-quotas";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;
    create_tenant(&client, &admin, fleet_key, "acme").await;

    let seen = Seen::of(
        client
            .put(format!("http://{admin}/admin/tenants/acme"))
            .header("authorization", fleet_key)
            .json(&json!({
                "displayName": "Acme",
                "quotas": { "maxImposters": 3, "maxStubsPerImposter": 5,
                            "maxFlowEntries": 9 },
                "journalRetentionSecs": 60,
            }))
            .send()
            .await
            .expect("update quotas"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");

    let record = node
        .tenant("acme")
        .expect("read tenant")
        .expect("tenant exists");
    assert_eq!(
        record.quotas,
        Quotas {
            max_imposters: 3,
            max_stubs_per_imposter: 5,
            max_flow_entries: 9,
        }
    );

    server.shutdown().await;
}
/// The acceptance criterion says a binding change is visible through **any**
/// node, and a solo cluster cannot demonstrate that: with no peers there is
/// nothing for the write barrier to wait for, so a same-node read-your-writes
/// test would pass on a build whose replication was entirely broken.
///
/// So: two nodes. Mint and re-bind through the **founder**, then read `whoami`
/// through the **joiner** — a node that never saw the request, only the
/// committed log entry. That is the property RFC-002 §8.5 is really claiming,
/// and it is what makes "no authorization cache" meaningful: the joiner
/// resolves bindings from its own applied state on every request, so a stale
/// answer there would be a real revocation window.
#[tokio::test]
async fn a_binding_change_is_visible_through_a_node_that_never_saw_the_write() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let founder_state = TempDir::new().expect("tempdir");
    let joiner_state = TempDir::new().expect("tempdir");
    let founder_bind = common::ports::reserve_addr();

    let founder = compose::start(cluster_on(
        &founder_state,
        &founder_bind,
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("founder starts");
    let joiner = compose::start(cluster_on(
        &joiner_state,
        &common::ports::reserve_addr(),
        &["--cluster-seeds", &founder_bind],
    ))
    .await
    .expect("joiner starts");

    let founder_node = founder.node().expect("founder is clustered");
    wait_voter_count(founder_node, 2).await;
    wait_ready(&founder).await;
    wait_ready(&joiner).await;

    let founder_admin = founder.admin_addr().to_string();
    let joiner_admin = joiner.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-twonode";
    seed_fleet_admin(founder_node, &mut op_id, fleet_key).await;
    create_tenant(&client, &founder_admin, fleet_key, "alpha").await;
    let (subject_id, subject_key) =
        mint_principal(&client, &founder_admin, fleet_key, "alpha", Role::Viewer).await;

    // The principal was minted through the founder; read it through the joiner.
    let seen = Seen::of(
        client
            .get(format!("http://{joiner_admin}/admin/whoami"))
            .header("authorization", &subject_key)
            .send()
            .await
            .expect("whoami through the joiner"),
    )
    .await;
    assert_eq!(
        seen.status, 200,
        "a key minted on the founder must authenticate on the joiner: {seen}"
    );
    assert!(seen.body.contains("\"role\":\"viewer\""), "{seen}");

    // Promote through the founder...
    let seen = Seen::of(
        client
            .put(format!(
                "http://{founder_admin}/admin/tenants/alpha/bindings/{subject_id}"
            ))
            .header("authorization", fleet_key)
            .json(&json!({ "role": "editor" }))
            .send()
            .await
            .expect("promote on the founder"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");

    // ...and see it through the joiner on the very next call.
    let seen = Seen::of(
        client
            .get(format!("http://{joiner_admin}/admin/whoami"))
            .header("authorization", &subject_key)
            .send()
            .await
            .expect("whoami through the joiner after promotion"),
    )
    .await;
    assert!(
        seen.body.contains("\"role\":\"editor\""),
        "the joiner served a stale binding — this is the revocation window \
         RFC-002 §8.5's no-cache rule exists to close: {seen}"
    );

    // Revocation is the direction that actually matters, so assert it too.
    let seen = Seen::of(
        client
            .delete(format!(
                "http://{founder_admin}/admin/tenants/alpha/bindings/{subject_id}"
            ))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("revoke on the founder"),
    )
    .await;
    assert_eq!(seen.status, 204, "{seen}");

    let seen = Seen::of(
        client
            .get(format!("http://{joiner_admin}/admin/whoami"))
            .header("authorization", &subject_key)
            .send()
            .await
            .expect("whoami through the joiner after revocation"),
    )
    .await;
    assert!(
        seen.body.contains("\"bindings\":[]"),
        "the joiner still honours a revoked binding: {seen}"
    );

    joiner.shutdown().await;
    founder.shutdown().await;
}

/// A binding must name a principal that exists.
///
/// This closes a self-inflicted denial of service that only became reachable
/// when this slice exposed `BindingPut` over HTTP: `tenant_principals` resolves
/// every binding to its principal to answer
/// `GET /admin/tenants/:id/principals`, and reports an unresolvable row as
/// committed-state corruption. Without the guard, one mistyped principal id
/// from any tenant admin would durably and replicatedly break the very listing
/// an operator would use to find and remove it.
#[tokio::test]
async fn a_binding_naming_no_principal_is_refused_rather_than_breaking_the_listing() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-orphan";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;
    create_tenant(&client, &admin, fleet_key, "alpha").await;

    let seen = Seen::of(
        client
            .put(format!(
                "http://{admin}/admin/tenants/alpha/bindings/key:this-principal-does-not-exist"
            ))
            .header("authorization", fleet_key)
            .json(&json!({ "role": "viewer" }))
            .send()
            .await
            .expect("bind a nonexistent principal"),
    )
    .await;
    assert_ne!(
        seen.status / 100,
        2,
        "a binding naming no principal must be refused: {seen}"
    );

    // The listing still works — which is the property the guard exists for.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/tenants/alpha/principals"))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("list principals"),
    )
    .await;
    assert_eq!(
        seen.status, 200,
        "the refused binding must not have broken the listing: {seen}"
    );

    server.shutdown().await;
}

/// `Idempotency-Key` on a credential mint is refused, not silently ignored.
///
/// A replayed op id is collapsed by dedup to the original committed response
/// with nothing re-applied — but the key is minted per request, so honouring
/// the header would answer `201` with a freshly-generated key that was never
/// stored, against a principal id that does not exist. The client would be
/// holding a credential that cannot authenticate and no way to learn why.
#[tokio::test]
async fn an_idempotency_key_is_refused_when_minting_a_credential() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-idem";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;
    create_tenant(&client, &admin, fleet_key, "alpha").await;

    let seen = Seen::of(
        client
            .post(format!("http://{admin}/admin/tenants/alpha/principals"))
            .header("authorization", fleet_key)
            .header("idempotency-key", "retry-me")
            .json(&json!({ "displayName": "svc", "role": "editor" }))
            .send()
            .await
            .expect("mint with an idempotency key"),
    )
    .await;
    assert_eq!(
        seen.status, 400,
        "minting a credential cannot be idempotent and must say so: {seen}"
    );

    // The header is fine on every other route: this is a targeted refusal, not
    // a blanket one.
    let seen = Seen::of(
        client
            .put(format!("http://{admin}/admin/tenants/alpha"))
            .header("authorization", fleet_key)
            .header("idempotency-key", "retry-me-too")
            .json(&json!({ "displayName": "Alpha renamed" }))
            .send()
            .await
            .expect("idempotent tenant update"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");

    server.shutdown().await;
}

// -- audit export sink admin surface (issue #164 review: gap 1) -------------
//
// `tenancy.rs`'s unit tests cover `Route::scope()` and `Route::action()` for
// the three `/admin/audit/sink` routes, but nothing before this called
// `dispatch()` for them, so the lines that build the `ControlOp` were
// untested. One of those lines already shipped wrong once — `TenantId::
// default()` instead of `TenantId::new(FLEET_SCOPE)` — which would file a
// fleet-wide config change under one tenant's name in the audit stream. These
// tests drive the surface over real HTTP, exactly as the rest of this file
// does, so a regression there fails at the wire.

/// The round trip a fleet admin can do end to end: declare the sink, read it
/// back, see the change attributed to the fleet scope in the audit stream —
/// **the point of this test** — get refused a malformed body, then delete the
/// sink and confirm it is genuinely gone.
#[tokio::test]
async fn the_audit_sink_surface_round_trips_for_a_fleet_admin_and_is_audited_under_the_fleet_scope()
{
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-audit-sink";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;

    // PUT succeeds for a fleet admin.
    let response = client
        .put(format!("http://{admin}/admin/audit/sink"))
        .header("authorization", fleet_key)
        .json(&json!({
            "uri": "https://collector.example/audit",
            "batchMaxRows": 100,
        }))
        .send()
        .await
        .expect("declare sink");
    let put_revision = revision_of(&response);
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "declaring the sink: {seen}");

    // GET returns the full record: uri, batchMaxRows, revision — and no
    // authRef, since none was given (the field is `skip_serializing_if
    // Option::is_none`, not serialized as `null`).
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/audit/sink"))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("read sink"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    let body = seen.json();
    assert_eq!(body["uri"], "https://collector.example/audit", "{seen}");
    assert_eq!(body["batchMaxRows"], 100, "{seen}");
    assert_eq!(body["revision"], put_revision, "{seen}");
    assert!(
        body.get("authRef").is_none(),
        "no authRef was given, so the field must be omitted rather than null: {seen}"
    );

    // THE POINT OF THIS TEST: the resulting audit row is attributed to the
    // fleet scope, under the sink's own `cluster.admin` action — not to
    // whatever `TenantId::default()` would have named. This is exactly the
    // regression that already shipped once (see this file's doc above).
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/audit"))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("read audit"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    let rows: Vec<Value> = serde_json::from_str(&seen.body).expect("audit body is JSON");
    let sink_rows: Vec<&Value> = rows
        .iter()
        .filter(|r| r["action"] == "cluster.admin")
        .collect();
    assert!(
        !sink_rows.is_empty(),
        "no audit row named the sink change at all: {rows:?}"
    );
    assert!(
        sink_rows.iter().all(|r| r["tenant"] == FLEET_SCOPE),
        "the sink change must be attributed to the fleet scope (\"{FLEET_SCOPE}\"): a \
         regression to TenantId::default() would file it under an ordinary tenant's name \
         instead: {rows:?}"
    );

    // A malformed PUT body is a real refusal, never a silently-defaulted 200.
    let seen = Seen::of(
        client
            .put(format!("http://{admin}/admin/audit/sink"))
            .header("authorization", fleet_key)
            .json(&json!({ "uri": 12345 }))
            .send()
            .await
            .expect("malformed put"),
    )
    .await;
    assert_eq!(
        seen.status / 100,
        4,
        "a malformed body must be refused, not defaulted: {seen}"
    );

    // DELETE succeeds, and a subsequent GET is a genuine 404 — not the sink
    // still answering because the delete silently no-opped.
    let seen = Seen::of(
        client
            .delete(format!("http://{admin}/admin/audit/sink"))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("delete sink"),
    )
    .await;
    assert_eq!(seen.status, 204, "{seen}");

    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/audit/sink"))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("read sink after delete"),
    )
    .await;
    assert_eq!(
        seen.status, 404,
        "the sink must read as gone after delete: {seen}"
    );

    server.shutdown().await;
}

/// A tenant admin — genuinely privileged inside its own tenant — must not
/// reach the fleet's audit sink at all. `Route::scope()` pins every
/// `/admin/audit/sink` method to `FLEET_SCOPE`, so a principal bound only
/// inside `acme` holds no binding on `"*"` and is refused with the same `404`
/// (not `403`) that RFC-002 §8.4 gives any other caller with no standing on a
/// scope — the indistinguishable not-found, since a tenant admin must not be
/// able to tell "this route does not exist" from "you have no binding here".
#[tokio::test]
async fn a_tenant_admin_is_refused_on_every_audit_sink_method() {
    let _guard = ARGON2_COUNTER_LOCK.lock().await;
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let admin = server.admin_addr().to_string();
    let client = reqwest::Client::new();
    let mut op_id = 1u128;

    let fleet_key = "tenancy-fleet-audit-sink-tenant";
    seed_fleet_admin(node, &mut op_id, fleet_key).await;
    create_tenant(&client, &admin, fleet_key, "acme").await;
    let (_, tenant_admin_key) =
        mint_principal(&client, &admin, fleet_key, "acme", Role::TenantAdmin).await;

    // The fleet admin declares a sink first, so a wrongly-permissive GET or
    // DELETE below would have something real to (wrongly) show or remove.
    let seen = Seen::of(
        client
            .put(format!("http://{admin}/admin/audit/sink"))
            .header("authorization", fleet_key)
            .json(&json!({ "uri": "https://collector.example/audit" }))
            .send()
            .await
            .expect("fleet admin declares the sink"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");

    for (method, body) in [
        (reqwest::Method::GET, None),
        (
            reqwest::Method::PUT,
            Some(json!({ "uri": "https://evil.example/audit" })),
        ),
        (reqwest::Method::DELETE, None),
    ] {
        let mut request = client
            .request(method.clone(), format!("http://{admin}/admin/audit/sink"))
            .header("authorization", &tenant_admin_key);
        if let Some(body) = &body {
            request = request.json(body);
        }
        let seen = Seen::of(
            request
                .send()
                .await
                .expect("tenant admin on the sink route"),
        )
        .await;
        assert_eq!(
            seen.status, 404,
            "a tenant admin holds no binding on the fleet scope, so this route must read as \
             not-found, the same as any other unbound cross-scope probe: {method} {seen}"
        );
    }

    // And the sink the tenant admin could not touch is still there, untouched
    // by the refused PUT above.
    let seen = Seen::of(
        client
            .get(format!("http://{admin}/admin/audit/sink"))
            .header("authorization", fleet_key)
            .send()
            .await
            .expect("fleet admin re-reads the sink"),
    )
    .await;
    assert_eq!(seen.status, 200, "{seen}");
    assert_eq!(
        seen.json()["uri"],
        "https://collector.example/audit",
        "a refused tenant-admin PUT must not have overwritten the sink: {seen}"
    );

    server.shutdown().await;
}
