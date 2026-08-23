//! Issue #293's exit criteria against a **real cluster**: the three write semantics proven
//! by their effect on committed state, not by the shape of a stubbed response.
//!
//! The unit tests pin the decisions (which status is a park, how a key is derived) and
//! `mcp_session.rs` pins what goes on the wire. Neither can answer the question the issue
//! actually asks — *does a retried tool call commit once?* — because that is a property of
//! the state machine on the other side of the socket. So these drive the shipped tool
//! methods against a solo node and then read the committed state back.
//!
//! The tools are called **directly rather than over a protocol session**, which is
//! deliberate: the dedup criterion is "the same tool-call id replayed", and an rmcp client
//! mints a fresh request id per call, so a session could never express it. Everything below
//! `RiftMcp::stub_add` is the same production path a session takes.

use std::time::Duration;

use clap::Parser;
use rift_cluster::control::{FLEET_SCOPE, PrincipalId, Role};
use rift_cluster::{ControlOp, ControlRequest, RaftNode, TenantId};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use rift_cluster_server::mcp::{
    AdminClient, CreateParams, McpArgs, RiftMcp, StubAddParams, StubByIdParams, WriteOutcome,
};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::NumberOrString;
use tempfile::TempDir;

const SECRET: &str = "mcp-write-secret";
const KEY: &str = "mcp-write-key";

fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    let mut args = vec![
        "rift-cluster-server".to_owned(),
        "--port".to_owned(),
        "0".to_owned(),
        "--metrics-port".to_owned(),
        "0".to_owned(),
        "--cluster".to_owned(),
        "--cluster-allow-solo".to_owned(),
        "--cluster-bind".to_owned(),
        "127.0.0.1:0".to_owned(),
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
    assert_eq!(response.outcome, rift_cluster::ControlOutcome::Applied);
}

/// A principal that can both write imposters (Editor, per tenant) and read `/_fleet/ops`
/// (FleetAdmin) — `op_status` needs the second, every write tool needs the first.
async fn seed_principal(node: &RaftNode, op_id: &mut u128) -> PrincipalId {
    let principal = rift_cluster::control::Principal {
        id: rift_cluster::control::api_key_principal_id(KEY),
        display_name: "mcp".to_owned(),
        auth: rift_cluster::control::AuthSource::ApiKey {
            hash: rift_cluster::control::hash_api_key(KEY),
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
            tenant: TenantId::default(),
            principal_id: id.clone(),
            role: Role::Editor,
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

/// An `RiftMcp` pointed at a live admin front. One instance means one session nonce, which
/// is what makes a replayed request id derive the same idempotency key.
fn mcp_for(admin: &str) -> RiftMcp {
    let dir = TempDir::new().expect("tempdir");
    let key_file = dir.path().join("agent.key");
    std::fs::write(&key_file, KEY).expect("write key file");
    // The client reads the file inside `AdminClient::new`, so the directory only has to
    // outlive that call; `keep` says so rather than suppressing a destructor.
    let _ = dir.keep();

    let args = McpArgs {
        url: format!("http://{admin}").parse().expect("parse url"),
        api_key_file: key_file,
        timeout_secs: 10,
    };
    RiftMcp::new(AdminClient::new(&args).expect("build admin client"))
}

fn id(n: i64) -> rmcp::handler::server::common::RequestId {
    rmcp::handler::server::common::RequestId(NumberOrString::Number(n))
}

/// Boot a solo node with a seeded Editor/FleetAdmin principal and an MCP server on it.
async fn fleet(extra: &'static [&'static str]) -> (TempDir, ComposedServer, RiftMcp) {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state, extra))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let mut op_id = 1u128;
    seed_principal(node, &mut op_id).await;
    let mcp = mcp_for(&server.admin_addr().to_string());
    (state, server, mcp)
}

fn applied(outcome: &WriteOutcome) -> &serde_json::Value {
    match outcome {
        WriteOutcome::Applied { data, .. } => data,
        other => panic!("expected an applied write, got {other:?}"),
    }
}

fn revision(outcome: &WriteOutcome) -> u64 {
    match outcome {
        WriteOutcome::Applied {
            current_revision, ..
        } => current_revision.expect("an applied write must report its revision"),
        other => panic!("expected an applied write, got {other:?}"),
    }
}

async fn create_imposter(mcp: &RiftMcp, port: u16, call: i64) -> WriteOutcome {
    mcp.imposter_create(
        id(call),
        Parameters(CreateParams {
            port,
            imposter: serde_json::json!({ "protocol": "http", "stubs": [] }),
        }),
    )
    .await
    .expect("create must not fail")
    .0
}

fn stub(body: &str) -> serde_json::Value {
    serde_json::json!({ "responses": [ { "is": { "statusCode": 200, "body": body } } ] })
}

async fn stub_count(mcp: &RiftMcp, port: u16) -> usize {
    let answer = mcp
        .imposter_get(Parameters(
            serde_json::from_value(serde_json::json!({ "port": port })).expect("port params"),
        ))
        .await
        .expect("read back")
        .0;
    answer.data["stubs"]
        .as_array()
        .map_or(0, std::vec::Vec::len)
}

/// **AC2, the RFC's M2 exit.** The same tool-call id replayed commits exactly one op.
///
/// The control matters as much as the assertion: a *different* id must append a second
/// stub. Without it this test would also pass against an implementation that silently
/// dropped every repeated write, or one where `stub_add` never appended at all.
#[tokio::test]
async fn replaying_one_tool_call_id_commits_exactly_one_op() {
    let (_state, _server, mcp) = fleet(&[]).await;
    create_imposter(&mcp, 4601, 1).await;

    let params = || {
        Parameters(StubAddParams {
            port: 4601,
            stub: stub("first"),
            precondition: Default::default(),
        })
    };

    let first = mcp.stub_add(id(42), params()).await.expect("first add").0;
    let replay = mcp
        .stub_add(id(42), params())
        .await
        .expect("the replay is answered, not refused")
        .0;

    assert_eq!(
        stub_count(&mcp, 4601).await,
        1,
        "a replayed tool-call id must dedup to one committed stub, not append twice"
    );
    assert_eq!(
        revision(&first),
        revision(&replay),
        "the dedup must answer the original op's revision, so the agent cannot tell it retried"
    );

    // The control: a genuinely different call is a genuinely different write.
    mcp.stub_add(
        id(43),
        Parameters(StubAddParams {
            port: 4601,
            stub: stub("second"),
            precondition: Default::default(),
        }),
    )
    .await
    .expect("a new call id appends");
    assert_eq!(
        stub_count(&mcp, 4601).await,
        2,
        "a different tool-call id must append — otherwise the dedup above proves nothing"
    );
}

/// **AC3, the full conflict loop**: a stale `expected_revision` is refused with the
/// revision to rebase on, and retrying with that revision succeeds.
///
/// This is the loop the `If-Match` header was built for, and the whole point of returning
/// a structured conflict is that an agent can drive it without parsing prose.
#[tokio::test]
async fn a_stale_revision_conflicts_and_the_returned_revision_lets_the_retry_succeed() {
    let (_state, _server, mcp) = fleet(&[]).await;
    let created = create_imposter(&mcp, 4602, 1).await;
    let stale = revision(&created);

    // Someone else writes, moving the imposter past `stale`.
    mcp.stub_add(
        id(2),
        Parameters(StubAddParams {
            port: 4602,
            stub: stub("theirs"),
            precondition: Default::default(),
        }),
    )
    .await
    .expect("the other writer lands");

    let conflicted = mcp
        .stub_add(
            id(3),
            Parameters(StubAddParams {
                port: 4602,
                stub: stub("mine"),
                precondition: rift_cluster_server::mcp::Precondition {
                    expected_revision: Some(stale),
                },
            }),
        )
        .await
        .expect("a conflict is an answer, not an error")
        .0;

    let fresh = match &conflicted {
        WriteOutcome::Conflict {
            current_revision,
            message,
            ..
        } => {
            assert!(
                message.contains("revision conflict"),
                "the API's own refusal must reach the agent: {message}"
            );
            current_revision.expect("a conflict on an existing record must report its revision")
        }
        other => panic!("a stale expected_revision must conflict, got {other:?}"),
    };
    assert!(
        fresh > stale,
        "the reported revision ({fresh}) must be the one that refused the stale {stale}"
    );
    assert_eq!(
        stub_count(&mcp, 4602).await,
        1,
        "the refused write must not have been applied"
    );

    // Rebase and retry — a fresh call id, because a keyed retry of a 409 dedups to that
    // same 409 by design (admin_front.rs's own note).
    let retried = mcp
        .stub_add(
            id(4),
            Parameters(StubAddParams {
                port: 4602,
                stub: stub("mine"),
                precondition: rift_cluster_server::mcp::Precondition {
                    expected_revision: Some(fresh),
                },
            }),
        )
        .await
        .expect("the retry")
        .0;

    assert!(
        matches!(retried, WriteOutcome::Applied { .. }),
        "retrying on the returned revision must succeed, got {retried:?}"
    );
    assert_eq!(
        stub_count(&mcp, 4602).await,
        2,
        "the rebased write must land"
    );
}

/// **AC4**: a parked write hands back an op id, and `op_status` polls it to completion.
///
/// `--cluster-admin-async` is what makes this reachable without contriving a leaderless
/// window: it is the front's own "park durably, answer 202, apply behind you" mode, so the
/// answer the agent sees is the same shape a genuinely parked write produces.
#[tokio::test]
async fn a_parked_write_is_pollable_through_op_status() {
    let (_state, _server, mcp) = fleet(&["--cluster-admin-async"]).await;

    let outcome = create_imposter(&mcp, 4603, 1).await;
    let op_id = match &outcome {
        WriteOutcome::Parked { op_id, .. } => op_id.clone(),
        other => panic!("--cluster-admin-async must park the write, got {other:?}"),
    };

    // Poll to completion. The op is durable the moment it is parked, so this converges;
    // the deadline is here so a regression fails loudly instead of hanging the suite.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let state = loop {
        let answer = mcp
            .op_status(Parameters(
                serde_json::from_value(serde_json::json!({ "op_id": op_id }))
                    .expect("op status params"),
            ))
            .await
            .expect("op_status must answer for a parked op")
            .0;
        // `pending` is what a still-parked op reads as — `op_body` emits exactly
        // `pending` / `applied` / `failed`, and never `parked`. Breaking on anything but
        // `pending` would end the loop on the first response and assert against a state
        // the op only reaches by winning a race, which is a test that passes for the
        // wrong reason.
        if let Some(state) = answer.data["state"].as_str()
            && state != "pending"
        {
            break state.to_owned();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the parked op never reached a terminal state"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };

    assert_eq!(
        state, "applied",
        "the parked create must commit; op_status reported {state}"
    );
}

/// **AC5**: a read hands the agent the revision it needs to condition its next write on.
///
/// Asserted by *using* it rather than by its presence: the number is only meaningful if the
/// write path accepts it, and a test that merely checked the field was non-null would pass
/// against a revision from the wrong record.
#[tokio::test]
async fn imposter_get_reports_a_revision_the_write_path_accepts() {
    let (_state, _server, mcp) = fleet(&[]).await;
    create_imposter(&mcp, 4604, 1).await;

    let answer = mcp
        .imposter_get(Parameters(
            serde_json::from_value(serde_json::json!({ "port": 4604 })).expect("port params"),
        ))
        .await
        .expect("read back")
        .0;
    let revision = answer
        .current_revision
        .expect("imposter_get must report current_revision");

    let outcome = mcp
        .stub_add(
            id(9),
            Parameters(StubAddParams {
                port: 4604,
                stub: stub("conditioned"),
                precondition: rift_cluster_server::mcp::Precondition {
                    expected_revision: Some(revision),
                },
            }),
        )
        .await
        .expect("conditional write")
        .0;

    assert!(
        matches!(outcome, WriteOutcome::Applied { .. }),
        "the revision a read reports must be one the write path accepts, got {outcome:?}"
    );
}

/// A conditional write against a record that does not exist is a **404**, not a conflict.
///
/// The front resolves the target before it evaluates the precondition, so "there is no such
/// imposter" is answered as the missing resource it is. That is the honest answer and this
/// pins it, because the tempting alternative — reporting a conflict — would have to invent a
/// `current_revision` for a record that has none, and `0` is a real revision elsewhere (a
/// route table that was never written) so it is not available as a stand-in for "unknown".
#[tokio::test]
async fn a_conditional_write_against_an_absent_record_is_a_404_not_an_invented_conflict() {
    let (_state, _server, mcp) = fleet(&[]).await;

    let error = mcp
        .stub_delete(
            id(1),
            Parameters(StubByIdParams {
                port: 4699,
                stub_id: "nope".to_owned(),
                precondition: rift_cluster_server::mcp::Precondition {
                    expected_revision: Some(3),
                },
            }),
        )
        .await
        // `Json<T>` is not `Debug`; the outcome inside it is.
        .map(|answer| answer.0)
        .expect_err("an absent imposter is a missing resource, not a conflict");

    let message = error.message.to_string();
    assert!(
        message.contains("404"),
        "an absent record must answer 404: {message}"
    );
    assert!(
        !message.contains("current_revision"),
        "and must never carry an invented revision: {message}"
    );
}

/// The applied answer carries the front's own post-commit render, not a synthesized one —
/// so what the agent reads back is what the cluster stored.
#[tokio::test]
async fn an_applied_write_returns_the_front_s_own_render() {
    let (_state, _server, mcp) = fleet(&[]).await;
    let created = create_imposter(&mcp, 4605, 1).await;
    let data = applied(&created);
    assert_eq!(
        data["port"].as_u64(),
        Some(4605),
        "the create must answer the record the front committed: {data}"
    );
}
