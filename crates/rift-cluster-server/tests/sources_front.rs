//! Issue #239: the imposter-source inspection surface on the **public admin
//! front** — `GET /admin/sources` and `GET /admin/sources/{id}`.
//!
//! The cluster-port surface (`tests/sources.rs`, issue #134) already proves
//! the source lifecycle end to end; what is proven here is the read surface an
//! operator's console actually reaches: RBAC-gated at `source.read`, scoped by
//! `X-Rift-Tenant`, answering the §8.4 cross-tenant `404`, and — the reason
//! this API needed a decision rather than plumbing — keeping fleet-replicated
//! facts and this-node-only facts structurally apart in the response.
//!
//! These drive `compose::start` and speak plain HTTP to the public admin
//! address, exactly as `tenancy_api.rs` does, seeding fixture state by
//! submitting `ControlOp`s directly through the node.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rift_cluster::control::{OnDrift, Quotas, Role, SourceMode};
use rift_cluster::rpc::{AlwaysHealthy, RpcClient, RpcClientConfig, Signer};
use rift_cluster::{ControlOp, ControlRequest, DEFAULT_TENANT, RaftNode, TenantId};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::Value;
use tempfile::TempDir;

mod common;

use common::seen::Seen;

const SECRET: &str = "sources-front-test-secret";

/// A clustered invocation on an explicit cluster `bind`. Mirrors
/// `tenancy_api.rs`'s `cluster_on` — each test binary carries its own copy of
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

fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    let mut args = vec!["--cluster-allow-solo"];
    args.extend_from_slice(extra);
    cluster_on(state, "127.0.0.1:0", &args)
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
async fn seed(node: &RaftNode, op_id: &mut u128, op: ControlOp) {
    let response = node
        .write(ControlRequest {
            op_id: uuid::Uuid::from_u128(*op_id),
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
    *op_id += 1;
}

fn tenant_put(tenant: &str) -> ControlOp {
    ControlOp::TenantPut {
        tenant: TenantId::new(tenant),
        display_name: tenant.to_owned(),
        quotas: Quotas::default(),
        journal_retention_secs: 0,
    }
}

/// A pinned source declaration; the `scripted` scheme is never pulled here, so
/// no provider for it needs to exist.
fn source_put(tenant: &str, id: &str) -> ControlOp {
    ControlOp::SourcePut {
        tenant: TenantId::new(tenant),
        id: id.to_owned(),
        uri: format!("scripted://cfg/{id}.json"),
        mode: SourceMode::Pinned,
        auth_ref: None,
        on_drift: OnDrift::Overwrite,
        poll_secs: None,
    }
}

/// A principal reachable from a real `Authorization: <raw_key>` header, bound
/// `role` in `tenant`. Same derivation rule as `rbac.rs`: the id must be
/// `api_key_principal_id(raw_key)` or `principal::resolve_bindings` can never
/// find it.
async fn seed_key(node: &RaftNode, op_id: &mut u128, tenant: &str, raw_key: &str, role: Role) {
    let principal = rift_cluster::control::Principal {
        id: rift_cluster::control::api_key_principal_id(raw_key),
        display_name: format!("{role:?}-{tenant}"),
        auth: rift_cluster::control::AuthSource::ApiKey {
            hash: rift_cluster::control::hash_api_key(raw_key),
        },
        disabled: false,
    };
    let id = principal.id.clone();
    seed(
        node,
        op_id,
        ControlOp::PrincipalPut {
            tenant: TenantId::new(tenant),
            principal,
        },
    )
    .await;
    seed(
        node,
        op_id,
        ControlOp::BindingPut {
            tenant: TenantId::new(tenant),
            principal_id: id,
            role,
        },
    )
    .await;
}

async fn get(client: &reqwest::Client, admin: &str, path: &str, key: &str, tenant: &str) -> Seen {
    let response = client
        .get(format!("http://{admin}{path}"))
        .header("authorization", key)
        .header("x-rift-tenant", tenant)
        .send()
        .await
        .expect("request sends");
    Seen::of(response).await
}

struct Fixture {
    _state: TempDir,
    server: ComposedServer,
    admin: String,
}

/// One solo cluster with two tenants, a Viewer key in `acme`, and one source
/// declared in each tenant.
async fn start() -> Fixture {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let mut op_id = 1u128;

    seed(node, &mut op_id, tenant_put("acme")).await;
    seed(node, &mut op_id, tenant_put("other")).await;
    seed_key(node, &mut op_id, "acme", VIEWER_KEY, Role::Viewer).await;
    seed(node, &mut op_id, source_put("acme", "payments")).await;
    seed(node, &mut op_id, source_put("acme", "billing")).await;
    seed(node, &mut op_id, source_put("other", "foreign")).await;

    let admin = server.admin_addr().to_string();
    Fixture {
        _state: state,
        server,
        admin,
    }
}

const VIEWER_KEY: &str = "sources-front-viewer-key";

/// Pins D-31: the replicated `SourceRecord` half of the response never carries
/// `lastPollError`; poll status is reported only under `nodeLocal`, stamped
/// with the node that observed it.
#[tokio::test]
async fn a_viewer_reads_sources_with_fleet_and_node_facts_apart() {
    let fixture = start().await;
    let client = reqwest::Client::new();

    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources",
        VIEWER_KEY,
        "acme",
    )
    .await;
    assert_eq!(seen.status, 200, "viewer list: {seen}");
    let body: Value = serde_json::from_str(&seen.body).expect("list is JSON");

    // The fleet-replicated half: exactly the store projection, id-ascending,
    // and only this tenant's rows.
    let sources = body["sources"].as_array().expect("sources array");
    let ids: Vec<&str> = sources
        .iter()
        .map(|s| s["id"].as_str().expect("id"))
        .collect();
    assert_eq!(ids, ["billing", "payments"], "id-ascending, acme only");
    for record in sources {
        assert_eq!(record["mode"], "pinned");
        assert_eq!(record["onDrift"], "overwrite");
        assert!(record["revision"].is_u64(), "revision travels in the body");
        // The whole point of #239's design decision: a node-local observation
        // must never sit on the fleet-replicated record, where two converged
        // nodes' answers would stop being byte-comparable.
        assert!(
            record.get("lastPollError").is_none(),
            "poll status must not be flattened into the replicated record: {record}"
        );
    }

    // The node-local half: names its scope (which node answered), and carries
    // poll errors — none here, which an empty map states explicitly rather
    // than by omission.
    let node_local = body["nodeLocal"].as_object().expect("nodeLocal object");
    // A STRING, not a `u64`. A raft id is a `u64` and JSON numbers are IEEE-754 doubles wherever
    // the reader is JavaScript, so an id above 2^53-1 arrives silently rounded — this endpoint was
    // naming a node that does not exist. Asserting it parses BACK to a `u64` is what makes this a
    // check on the encoding rather than merely on the type.
    let observing = node_local["nodeId"]
        .as_str()
        .expect("the observing node is named");
    assert!(
        observing.parse::<u64>().is_ok(),
        "the node id must still be a u64, just carried as text: {observing}"
    );
    assert_eq!(
        node_local["pollErrors"],
        serde_json::json!({}),
        "no poll has failed on this node"
    );

    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources/payments",
        VIEWER_KEY,
        "acme",
    )
    .await;
    assert_eq!(seen.status, 200, "viewer get: {seen}");
    let body: Value = serde_json::from_str(&seen.body).expect("get is JSON");
    assert_eq!(body["source"]["id"], "payments");
    assert_eq!(body["source"]["uri"], "scripted://cfg/payments.json");
    assert!(body["source"].get("lastPollError").is_none());
    // The single-source read carries the same node-local half as the list, so it gets the same
    // check: a string that still parses back to a `u64`.
    let observing = body["nodeLocal"]["nodeId"]
        .as_str()
        .expect("the observing node is named");
    assert!(
        observing.parse::<u64>().is_ok(),
        "still a u64, carried as text: {observing}"
    );

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn a_source_in_another_tenant_answers_404_not_403() {
    let fixture = start().await;
    let client = reqwest::Client::new();

    // `foreign` exists — in `other`. To an `acme`-scoped read it must be
    // indistinguishable from a source that never existed (RFC-002 §8.4): a 403
    // would confirm the id is real in someone else's tenant.
    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources/foreign",
        VIEWER_KEY,
        "acme",
    )
    .await;
    assert_eq!(seen.status, 404, "cross-tenant read: {seen}");

    // And the tenant the caller holds no binding in answers 404 for the whole
    // surface, list included — same §8.4 rule, one level up.
    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources",
        VIEWER_KEY,
        "other",
    )
    .await;
    assert_eq!(seen.status, 404, "unbound tenant list: {seen}");

    fixture.server.shutdown().await;
}

#[tokio::test]
async fn an_unauthenticated_read_answers_401() {
    let fixture = start().await;
    let client = reqwest::Client::new();

    // Same precedent as `tenancy_api.rs`'s dedicated 401 test: the shared gate
    // is proven generically elsewhere, but this is what pins these two routes
    // to it if the dispatch ever changes.
    for path in ["/admin/sources", "/admin/sources/payments"] {
        let response = client
            .get(format!("http://{}{path}", fixture.admin))
            .header("x-rift-tenant", "acme")
            .send()
            .await
            .expect("request sends");
        let seen = Seen::of(response).await;
        assert_eq!(seen.status, 401, "unauthenticated {path}: {seen}");
    }

    fixture.server.shutdown().await;
}

const EDITOR_ACME_KEY: &str = "sources-front-editor-acme-key";
const EDITOR_OTHER_KEY: &str = "sources-front-editor-other-key";

/// Issue #253: `POST /admin/sources` lands the declaration in the tenant `authorize_action`
/// resolved for the caller — `X-Rift-Tenant`, for an Editor bound there — never in whichever
/// tenant a bystander happens to read from. Proven from both directions: the declaring tenant
/// sees it, and a *different* tenant's own list (read by a principal genuinely bound there, not
/// merely refused at the boundary) does not.
#[tokio::test]
async fn an_editor_declares_a_source_into_its_own_tenant_and_nowhere_else() {
    let fixture = start().await;
    let client = reqwest::Client::new();
    let node = fixture.server.node().expect("clustered");
    let mut op_id = 1_000u128;
    seed_key(node, &mut op_id, "acme", EDITOR_ACME_KEY, Role::Editor).await;
    seed_key(node, &mut op_id, "other", EDITOR_OTHER_KEY, Role::Editor).await;

    let response = client
        .post(format!("http://{}/admin/sources", fixture.admin))
        .header("authorization", EDITOR_ACME_KEY)
        .header("x-rift-tenant", "acme")
        .json(&serde_json::json!({
            "id": "acme-only",
            // A scheme this build actually serves. `scripted:` is registered only by the
            // harness the read tests use; a declare is never dereferenced, so any served
            // scheme with a host does.
            "uri": "git+https://host/org/acme-only#main:mocks.json",
        }))
        .send()
        .await
        .expect("declare source");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "editor declare: {seen}");
    let created: Value = serde_json::from_str(&seen.body).expect("created is JSON");
    assert_eq!(created["id"], "acme-only");

    // Visible under the declaring tenant.
    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources/acme-only",
        EDITOR_ACME_KEY,
        "acme",
    )
    .await;
    assert_eq!(seen.status, 200, "acme reads its own declaration: {seen}");

    // Absent from `other`'s own list — read by a principal genuinely bound to `other` (so this is
    // a real "not there" within a tenant it CAN see, not the §8.4 boundary 404 a probe from a
    // principal that is NOT bound to `other` would get instead, which would prove nothing about
    // where the write landed).
    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources",
        EDITOR_OTHER_KEY,
        "other",
    )
    .await;
    assert_eq!(
        seen.status, 200,
        "other's editor lists its own tenant: {seen}"
    );
    let body: Value = serde_json::from_str(&seen.body).expect("list is JSON");
    let ids: Vec<&str> = body["sources"]
        .as_array()
        .expect("sources array")
        .iter()
        .map(|s| s["id"].as_str().expect("id"))
        .collect();
    assert_eq!(
        ids,
        ["foreign"],
        "acme's declaration must not appear in other's own list: {ids:?}"
    );

    /*
     * DELETE and PULL are tenant-scoped too — and neither was covered until a mutation run showed
     * both shipping green when pointed at `TenantId::default()`. The PUT above would still have
     * passed, because it is a different call site: each of the three carries its own tenant
     * argument, so each needs its own proof.
     *
     * `other`'s editor addressing `acme-only` must not reach acme's source. It is a real "not
     * there" within a tenant `other` genuinely holds, not a boundary refusal.
     */
    let response = client
        .delete(format!("http://{}/admin/sources/acme-only", fixture.admin))
        .header("authorization", EDITOR_OTHER_KEY)
        .header("x-rift-tenant", "other")
        .send()
        .await
        .expect("cross-tenant delete");
    /*
     * The status is deliberately NOT asserted: `SourceDelete` is an idempotent forget, so deleting
     * an id absent from the caller's own tenant is a legitimate 200 no-op. What must hold is that
     * acme's source is still there afterwards — asserted below. A status check here would pin the
     * wrong thing and would pass just as happily if the delete had reached across the boundary.
     */
    let _ = Seen::of(response).await;

    let response = client
        .post(format!(
            "http://{}/admin/sources/acme-only/pull",
            fixture.admin
        ))
        .header("authorization", EDITOR_OTHER_KEY)
        .header("x-rift-tenant", "other")
        .send()
        .await
        .expect("cross-tenant pull");
    // Same reasoning as the delete above — survival is the property, not the status.
    let _ = Seen::of(response).await;

    // And acme's source is still there — the point of the two calls above is that neither reached
    // it, which a status code alone does not prove.
    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources/acme-only",
        EDITOR_ACME_KEY,
        "acme",
    )
    .await;
    assert_eq!(
        seen.status, 200,
        "acme's source survives another tenant addressing it: {seen}"
    );

    /*
     * Pull's tenant argument, pinned the same positive way and for the same reason: acme pulling
     * its OWN source must find it. The fetch itself then fails — the URI is never dereferenceable
     * from a test — so the status is deliberately not pinned; what matters is that it is not the
     * 404 the mutant produces by looking in `default`, where this source does not exist.
     */
    let response = client
        .post(format!(
            "http://{}/admin/sources/acme-only/pull",
            fixture.admin
        ))
        .header("authorization", EDITOR_ACME_KEY)
        .header("x-rift-tenant", "acme")
        .send()
        .await
        .expect("own-tenant pull");
    let seen = Seen::of(response).await;
    assert_ne!(
        seen.status, 404,
        "acme pulling its own source must find it: {seen}"
    );

    /*
     * The assertion that actually pins delete's tenant argument — and the reason it is a POSITIVE
     * one rather than the cross-tenant probe above.
     *
     * A first attempt asserted only that `other` could not delete acme's source. That guards
     * nothing: point the front at `TenantId::default()` and `other`'s delete lands in `default`,
     * acme's source is in `acme`, and it survives either way — the test passes on the broken code.
     * What separates them is acme deleting its OWN source: with the tenant carried through it is
     * gone, and with `default` substituted it is untouched. Mutation-verified in both directions.
     */
    let response = client
        .delete(format!("http://{}/admin/sources/acme-only", fixture.admin))
        .header("authorization", EDITOR_ACME_KEY)
        .header("x-rift-tenant", "acme")
        .send()
        .await
        .expect("own-tenant delete");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "acme deletes its own source: {seen}");

    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources/acme-only",
        EDITOR_ACME_KEY,
        "acme",
    )
    .await;
    assert_eq!(
        seen.status, 404,
        "acme's own delete must actually remove it from acme: {seen}"
    );

    fixture.server.shutdown().await;
}

/// Issue #253's explicit constraint: promoting the write verbs to the admin front must leave the
/// cluster port's own default-tenant `POST /admin/sources` (issue #134) unchanged. Both now
/// delegate to the same `SourcePuller::put`, but the cluster port still calls it with
/// `TenantId::default()` hardcoded — never a caller-supplied tenant, because the cluster port has
/// no notion of one. Proven over the wire through the cluster port's own RPC signer, a listener
/// entirely separate from the admin front's RBAC the rest of this file drives.
#[tokio::test]
async fn the_cluster_port_still_declares_into_the_default_tenant() {
    let fixture = start().await;
    let node = fixture.server.node().expect("clustered");
    let cluster_addr: SocketAddr = node
        .advertise()
        .as_str()
        .parse()
        .expect("advertise is a literal address in tests");
    let rpc = RpcClient::new(
        Some(Signer::new(SECRET)),
        Arc::new(AlwaysHealthy),
        RpcClientConfig::default(),
    );

    let payload = serde_json::to_vec(&serde_json::json!({
        "id": "cluster-port-default",
        // See the note in the tenancy test above: a served scheme, never dereferenced.
        "uri": "git+https://host/org/cluster-port-default#main:mocks.json",
    }))
    .expect("encode");
    let raw = rpc
        .call(cluster_addr, "POST", "/admin/sources", payload)
        .await
        .expect("cluster port declare");
    let created: Value = serde_json::from_slice(&raw).expect("json body");
    assert_eq!(created["id"], "cluster-port-default");

    assert!(
        node.source(DEFAULT_TENANT, "cluster-port-default")
            .expect("read source")
            .is_some(),
        "the cluster port's own declare must land under DEFAULT_TENANT"
    );
    // And nowhere an admin-front tenant would see it as its own — `acme`'s own list (read by a
    // principal genuinely bound there) stays exactly the two the fixture seeded.
    let mut op_id = 1_000u128;
    seed_key(node, &mut op_id, "acme", EDITOR_ACME_KEY, Role::Editor).await;
    let client = reqwest::Client::new();
    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources",
        EDITOR_ACME_KEY,
        "acme",
    )
    .await;
    let body: Value = serde_json::from_str(&seen.body).expect("list is JSON");
    let ids: Vec<&str> = body["sources"]
        .as_array()
        .expect("sources array")
        .iter()
        .map(|s| s["id"].as_str().expect("id"))
        .collect();
    assert_eq!(
        ids,
        ["billing", "payments"],
        "the cluster port's default-tenant write must not appear under acme: {ids:?}"
    );

    fixture.server.shutdown().await;
}

/// Issue #253, secret hygiene: a URI carrying userinfo is refused before it ever reaches
/// `node.submit` — the same guarantee `control::validate` already gives the cluster port
/// (`tests/sources.rs::operator_errors_are_refusals_not_internal_failures`), now exercised
/// through the admin front's own `POST /admin/sources` (`SourcePuller::put`, shared by both).
/// "Refused before submit" is asserted here as "not even a committed `Failed` entry exists" —
/// reading the tenant's list back and finding nothing, not merely checking the response code. A
/// **separate** declaration naming a credential by reference (`authRef`), never embedding one,
/// must still succeed and round-trip that name byte-for-byte — proving the refusal above is about
/// the credential *embedded in the URI*, not a blanket rejection of `authRef`.
#[tokio::test]
async fn a_credential_bearing_uri_is_refused_before_it_ever_reaches_the_log() {
    let fixture = start().await;
    let client = reqwest::Client::new();
    let node = fixture.server.node().expect("clustered");
    let mut op_id = 1_000u128;
    seed_key(node, &mut op_id, "acme", EDITOR_ACME_KEY, Role::Editor).await;

    let response = client
        .post(format!("http://{}/admin/sources", fixture.admin))
        .header("authorization", EDITOR_ACME_KEY)
        .header("x-rift-tenant", "acme")
        .json(&serde_json::json!({
            "id": "leaky",
            "uri": "git+https://user:pw@host/repo",
        }))
        .send()
        .await
        .expect("declare a leaky source");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 400, "credential-bearing uri: {seen}");
    assert!(
        seen.body.contains("auth_ref"),
        "the refusal should point the operator at authRef instead of an embedded credential: \
         {seen}"
    );

    // Not committed at all — not even as a `Failed` entry, which would keep the refusal (and the
    // fact a credential was ever typed) on every replica's disk forever.
    let seen = get(
        &client,
        &fixture.admin,
        "/admin/sources",
        EDITOR_ACME_KEY,
        "acme",
    )
    .await;
    let body: Value = serde_json::from_str(&seen.body).expect("list is JSON");
    let ids: Vec<&str> = body["sources"]
        .as_array()
        .expect("sources array")
        .iter()
        .map(|s| s["id"].as_str().expect("id"))
        .collect();
    assert!(
        !ids.contains(&"leaky"),
        "a refused declare must leave no trace at all: {ids:?}"
    );

    // The named-credential twin: no userinfo anywhere in the uri, `authRef` names the credential
    // instead, and this build's `git+https:` provider is registered credentialed (`GitSource`) —
    // so this must be accepted, and `authRef` must come back as exactly the string it was sent as.
    let response = client
        .post(format!("http://{}/admin/sources", fixture.admin))
        .header("authorization", EDITOR_ACME_KEY)
        .header("x-rift-tenant", "acme")
        .json(&serde_json::json!({
            "id": "named-cred",
            "uri": "git+https://host/org/repo#main:mocks.json",
            "authRef": "acme-github-token",
        }))
        .send()
        .await
        .expect("declare with a named credential");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 200, "named credential: {seen}");
    let created: Value = serde_json::from_str(&seen.body).expect("created is JSON");
    assert_eq!(
        created["authRef"], "acme-github-token",
        "authRef must round-trip as exactly the reference string, never a credential: {created}"
    );

    fixture.server.shutdown().await;
}

// -- git as a detected capability, end to end (#270) -------------------------

/// AC2, on **both** declaration paths at once.
///
/// The unit tests prove `scheme_refusal` builds the right message; this proves
/// the message actually reaches a caller through the two entry points an
/// operator can reach — boot-time `--imposters`/`RIFT_IMPOSTERS`
/// (`declare_and_pull`) and the admin `PUT` (`put`) — rather than through one
/// of them while the other still answers the generic unknown-scheme refusal.
///
/// The puller is built over a provider set with `git+` marked unavailable and
/// bound to the fixture's real node, because the composed server under test is
/// running on a host that *does* have git: the `-static` condition cannot be
/// produced by composing normally, only by constructing the state composition
/// would have reached.
#[tokio::test]
async fn a_git_declaration_on_a_gitless_node_is_refused_with_the_cause_and_the_fix() {
    let fixture = start().await;
    let node = fixture.server.node().expect("clustered");

    let mut providers = rift_cluster::sources::SourceProviders::default();
    providers
        .register_unavailable(
            rift_cluster::sources::git::GIT_SCHEMES,
            "no `git` binary on PATH; install git, or use the default (non-static) image if this is `-static`",
        )
        .expect("git+ is unserved in this provider set");

    let puller = rift_cluster::sources::SourcePuller::new(providers);
    puller.bind(node).expect("bind the puller to the node");

    let uri = "git+https://example.com/repo#main:mocks.json";

    // Path 1 — boot-time declaration.
    let boot = puller
        .declare_and_pull("from-boot", uri, OnDrift::Fail)
        .await
        .expect_err("a gitless node must refuse a git+ source at declaration time");
    let boot = boot.to_string();

    // Path 2 — admin PUT.
    let body = serde_json::json!({ "id": "from-admin", "uri": uri }).to_string();
    let admin = puller
        .put(TenantId::default(), body.as_bytes(), None)
        .await
        .expect_err("the admin path must refuse it too");
    let admin = admin.to_string();

    for (path, refusal) in [("declare_and_pull", &boot), ("put", &admin)] {
        assert!(
            refusal.contains("`git+https:` sources are unavailable"),
            "{path} must name the scheme: {refusal}"
        );
        assert!(
            refusal.contains("no `git` binary on PATH"),
            "{path} must name the cause: {refusal}"
        );
        assert!(
            refusal.contains("use the default (non-static) image"),
            "{path} must name the fix: {refusal}"
        );
        assert!(
            !refusal.contains("no imposter source is registered"),
            "{path} must not report an unavailable scheme as an unknown one: {refusal}"
        );
    }

    // Nothing was stored: a refused declaration must not leave a source record
    // behind for the poll scheduler to retry forever.
    assert!(
        node.source(DEFAULT_TENANT, "from-boot")
            .expect("read local state")
            .is_none(),
        "a refused boot declaration must store nothing"
    );
    assert!(
        node.source(DEFAULT_TENANT, "from-admin")
            .expect("read local state")
            .is_none(),
        "a refused admin declaration must store nothing"
    );
}

/// The restart-onto-a-static-image case, which is the one that motivated
/// checking the scheme in `pull` at all.
///
/// A `git+https:` source was declared while the fleet ran the default flavor,
/// so the record is already in the replicated log — no declaration is involved
/// any more. The node then comes back on a `-static` image. Every later
/// `pull` of that record (a `refresh-now`, a tracking poll) must say the
/// scheme is unavailable *in this image*, not that the URI names an unknown
/// scheme: the URI was correct when it was accepted, and blaming it would send
/// the operator to fix the one thing that is not wrong.
#[tokio::test]
async fn pulling_an_already_declared_git_source_on_a_gitless_node_blames_the_image() {
    let fixture = start().await;
    let node = fixture.server.node().expect("clustered");

    // Seeded straight into the log, exactly as a default-flavor node would
    // have left it — deliberately NOT through the puller under test, which
    // would refuse it.
    let mut op_id = 9_000u128;
    seed(
        node,
        &mut op_id,
        ControlOp::SourcePut {
            tenant: TenantId::default(),
            id: "declared-before-the-restart".to_owned(),
            uri: "git+https://example.com/repo#main:mocks.json".to_owned(),
            mode: SourceMode::Pinned,
            auth_ref: None,
            on_drift: OnDrift::Overwrite,
            poll_secs: None,
        },
    )
    .await;

    let mut providers = rift_cluster::sources::SourceProviders::default();
    providers
        .register_unavailable(
            rift_cluster::sources::git::GIT_SCHEMES,
            "no `git` binary on PATH; install git, or use the default (non-static) image if this is `-static`",
        )
        .expect("git+ is unserved in this provider set");
    let puller = rift_cluster::sources::SourcePuller::new(providers);
    puller.bind(node).expect("bind the puller to the node");

    let err = puller
        .pull(DEFAULT_TENANT, "declared-before-the-restart", None)
        .await
        .expect_err("a stored git+ source cannot be pulled on a gitless node")
        .to_string();

    assert!(
        err.contains("`git+https:` sources are unavailable"),
        "pull must name the scheme: {err}"
    );
    assert!(
        err.contains("no `git` binary on PATH"),
        "pull must name the cause: {err}"
    );
    assert!(
        !err.contains("no imposter source is registered"),
        "the record's scheme was servable when it was written; it must not now read as unknown: {err}"
    );
    assert!(
        !err.contains("unknown source"),
        "the record exists — only its scheme is unavailable: {err}"
    );
}
