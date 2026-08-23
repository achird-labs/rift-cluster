//! End-to-end gate for #292: a real MCP protocol session, driving the real tool
//! handlers, against a real HTTP admin API.
//!
//! This is the epic's **M1 exit criterion** — "an MCP session lists imposters against
//! a live cluster" — reduced to the smallest thing that still proves it: an rmcp
//! client and the shipped `RiftMcp` server on the two ends of an in-memory duplex,
//! with a stub admin API on a real TCP socket in between. Everything between the
//! client's `call_tool` and the socket is production code.
//!
//! What it deliberately does not exercise is `rmcp::transport::stdio()` itself —
//! two lines binding the same transport trait to the process's own stdin/stdout.
//! Swapping the duplex for a spawned child process would test tokio's stdio
//! plumbing, not this crate's.
//!
//! It also asserts on the **request line and headers the client actually sent**.
//! Every tool's endpoint literal and path format is otherwise unguarded: the pure
//! helpers (`scope_for`, `requests_query_params`) are unit-tested, but a typo in
//! `"_fleet/health"` or a dropped `/verify` suffix lives entirely in the tool
//! method and no unit test can see it.
//!
//! The stub speaks HTTP/1.1 by hand rather than pulling in a server framework: the
//! assertions are about the bytes on the wire (the `Authorization` header above
//! all), and a framework would only hide them.

use std::io::{Read as _, Write as _};
use std::sync::{Arc, Mutex};

use rift_cluster_server::mcp::{AdminClient, McpArgs, RiftMcp, ToolFailure};
use rmcp::ServiceExt as _;
use rmcp::model::CallToolRequestParams;
use rmcp::service::{RoleClient, RunningService};

/// A stub admin API that answers every request with the same body and records the
/// request head it saw, so a test can assert on what the client actually sent.
struct StubAdmin {
    url: String,
    seen: Arc<Mutex<Vec<String>>>,
}

impl StubAdmin {
    fn start(body: &'static str, extra_headers: &'static str) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stub admin API");
        let url = format!("http://{}", listener.local_addr().expect("stub addr"));
        let seen = Arc::new(Mutex::new(Vec::new()));

        let recorder = Arc::clone(&seen);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut socket) = stream else { break };
                let mut buf = [0u8; 8192];
                let Ok(read) = socket.read(&mut buf) else {
                    continue;
                };
                recorder
                    .lock()
                    .expect("stub lock")
                    .push(String::from_utf8_lossy(&buf[..read]).to_string());

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes());
                let _ = socket.flush();
            }
        });

        Self { url, seen }
    }

    /// The request heads received so far, oldest first.
    fn requests(&self) -> Vec<String> {
        self.seen.lock().expect("stub lock").clone()
    }
}

/// A stub admin API that answers a **scripted sequence** of responses (issue #293).
///
/// `StubAdmin` answers the same thing every time, which cannot express the two exchanges
/// the write semantics are made of: a 409 followed by the re-read that supplies
/// `current_revision`, and a park followed by an `op_status` poll. Each entry is
/// `(status line, extra headers, body)` and is used once, in order; the last entry repeats
/// so a test never has to count the requests it does not care about.
struct ScriptedAdmin {
    url: String,
    seen: Arc<Mutex<Vec<String>>>,
}

impl ScriptedAdmin {
    fn start(script: Vec<(&'static str, &'static str, &'static str)>) -> Self {
        assert!(!script.is_empty(), "a script needs at least one response");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind scripted admin API");
        let url = format!("http://{}", listener.local_addr().expect("stub addr"));
        let seen = Arc::new(Mutex::new(Vec::new()));

        let recorder = Arc::clone(&seen);
        std::thread::spawn(move || {
            let mut answered = 0usize;
            for stream in listener.incoming() {
                let Ok(mut socket) = stream else { break };
                let mut buf = [0u8; 16384];
                let Ok(read) = socket.read(&mut buf) else {
                    continue;
                };
                recorder
                    .lock()
                    .expect("stub lock")
                    .push(String::from_utf8_lossy(&buf[..read]).to_string());

                let (status, headers, body) = script[answered.min(script.len() - 1)];
                answered += 1;
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes());
                let _ = socket.flush();
            }
        });

        Self { url, seen }
    }

    fn requests(&self) -> Vec<String> {
        self.seen.lock().expect("stub lock").clone()
    }
}

/// Write a key file that outlives the call, and point args at `url`.
fn args_for(url: &str, key: &str) -> McpArgs {
    let dir = tempfile::tempdir().expect("tempdir");
    let key_file = dir.path().join("agent.key");
    std::fs::write(&key_file, key).expect("write key");
    // The client reads the file after this function returns, so the directory must
    // outlive the `TempDir` guard. `keep` says that; `mem::forget` would too, but
    // by suppressing a destructor rather than by expressing the intent.
    let _ = dir.keep();

    McpArgs {
        url: url.parse().expect("parse url"),
        api_key_file: key_file,
        timeout_secs: 10,
    }
}

/// An MCP session against a `RiftMcp` wired to `url`.
async fn session_against(url: &str, key: &str) -> RunningService<RoleClient, ()> {
    let client = AdminClient::new(&args_for(url, key)).expect("build client");
    let (server_io, client_io) = tokio::io::duplex(65536);

    tokio::spawn(async move {
        if let Ok(running) = RiftMcp::new(client).serve(server_io).await {
            let _ = running.waiting().await;
        }
    });

    ().serve(client_io).await.expect("client handshake")
}

fn call(name: &'static str, args: serde_json::Value) -> CallToolRequestParams {
    let params = CallToolRequestParams::new(name);
    match args {
        serde_json::Value::Null => params,
        serde_json::Value::Object(map) => params.with_arguments(map),
        other => panic!("tool arguments must be a JSON object, got {other}"),
    }
}

/// The request line (`GET /imposters?x=y HTTP/1.1`) of the nth recorded request.
fn request_line(head: &str) -> &str {
    head.lines().next().unwrap_or_default()
}

/// AC1 / the epic's M1 exit — an MCP session lists imposters over the protocol.
///
/// Drives `initialize`, `tools/list` and `tools/call` exactly as a coding agent
/// would, and asserts the imposter the admin API returned arrives at the client.
#[tokio::test]
async fn mcp_session_lists_imposters_over_the_protocol() {
    let stub = StubAdmin::start(r#"[{"port":4545,"protocol":"http"}]"#, "");
    let session = session_against(&stub.url, "s3cret-key").await;

    let tools = session.list_all_tools().await.expect("list tools");
    assert!(
        tools.iter().any(|t| t.name == "imposter_list"),
        "imposter_list must be advertised; got {:?}",
        tools.iter().map(|t| &t.name).collect::<Vec<_>>()
    );

    let result = session
        .call_tool(call("imposter_list", serde_json::Value::Null))
        .await
        .expect("call imposter_list");

    let rendered = serde_json::to_string(&result).expect("serialize result");
    assert!(
        rendered.contains("4545"),
        "the imposter from the admin API must reach the agent; got {rendered}"
    );

    // E4, proven on the wire rather than at the accessor: the unit test on
    // `header_value` cannot see a client that re-wraps the value on its way out.
    let requests = stub.requests();
    let head = requests.first().expect("the stub must have seen a request");
    assert!(
        head.to_lowercase().contains("authorization: s3cret-key"),
        "the raw key must be sent as the Authorization value; head was:\n{head}"
    );
    assert!(
        !head.to_lowercase().contains("bearer"),
        "no `Bearer ` prefix may be sent; head was:\n{head}"
    );

    session.cancel().await.ok();
}

/// Every tool's endpoint literal and path format, asserted against the request the
/// client actually sent.
///
/// This is the gap the pure-helper unit tests cannot cover: `"_fleet/health"`,
/// `"admin/whoami"`, `format!("imposters/{port}/verify")` and friends live only in
/// the tool methods, so a typo in any of them is invisible until an agent gets a
/// 404 from a real fleet.
#[tokio::test]
async fn every_tool_requests_the_endpoint_it_documents() {
    let stub = StubAdmin::start("{}", "");
    let session = session_against(&stub.url, "k").await;

    let cases: Vec<(&'static str, serde_json::Value, &str)> = vec![
        ("imposter_list", serde_json::Value::Null, "GET /imposters "),
        (
            "imposter_get",
            serde_json::json!({ "port": 4545 }),
            "GET /imposters/4545 ",
        ),
        (
            "requests_query",
            serde_json::json!({ "port": 4545 }),
            "GET /imposters/4545/requests ",
        ),
        (
            "routes_get",
            serde_json::Value::Null,
            "GET /front-door/routes ",
        ),
        (
            "fleet_health",
            serde_json::Value::Null,
            "GET /_fleet/health ",
        ),
        ("whoami", serde_json::Value::Null, "GET /admin/whoami "),
        (
            "verify",
            serde_json::json!({ "port": 4545, "options": {} }),
            "POST /imposters/4545/verify ",
        ),
    ];

    for (index, (tool, args, expected_line)) in cases.iter().enumerate() {
        session
            .call_tool(call(tool, args.clone()))
            .await
            .unwrap_or_else(|e| panic!("call {tool}: {e}"));

        let requests = stub.requests();
        let head = requests
            .get(index)
            .unwrap_or_else(|| panic!("{tool} must have produced request #{index}"));
        assert!(
            request_line(head).starts_with(expected_line.trim_end()),
            "{tool} must request `{expected_line}`; got `{}`",
            request_line(head)
        );
    }

    session.cancel().await.ok();
}

/// The write half of the endpoint gate (issue #293), plus the criterion that cannot be
/// checked anywhere but on the wire: **every** write tool sends an `Idempotency-Key`.
///
/// The derivation is unit-tested, but a tool that assembles its own request — or one added
/// later that forgets to go through `RiftMcp::write` — would still be keyless, and nothing
/// below the socket would notice. Asserting it per tool is what makes "every write is
/// idempotent by key" a property of the surface rather than of the eight call sites.
#[tokio::test]
async fn every_write_tool_sends_an_idempotency_key_to_the_endpoint_it_documents() {
    let stub = StubAdmin::start("{}", "");
    let session = session_against(&stub.url, "k").await;

    let cases: Vec<(&'static str, serde_json::Value, &str)> = vec![
        (
            "imposter_create",
            serde_json::json!({ "port": 4545, "imposter": { "protocol": "http" } }),
            "POST /imposters ",
        ),
        (
            "imposter_delete",
            serde_json::json!({ "port": 4545 }),
            "DELETE /imposters/4545 ",
        ),
        (
            "imposter_set_enabled",
            serde_json::json!({ "port": 4545, "enabled": false }),
            "POST /imposters/4545/disable ",
        ),
        (
            "stub_add",
            serde_json::json!({ "port": 4545, "stub": { "responses": [] } }),
            "POST /imposters/4545/stubs ",
        ),
        (
            "stub_replace",
            serde_json::json!({ "port": 4545, "stub_id": "abc", "stub": { "responses": [] } }),
            "PUT /imposters/4545/stubs/by-id/abc ",
        ),
        (
            "stub_delete",
            serde_json::json!({ "port": 4545, "stub_id": "abc" }),
            "DELETE /imposters/4545/stubs/by-id/abc ",
        ),
        (
            "routes_put",
            serde_json::json!({ "routes": [] }),
            "PUT /front-door/routes ",
        ),
        (
            "route_delete",
            serde_json::json!({ "route_id": "r1" }),
            "DELETE /front-door/routes/r1 ",
        ),
    ];

    for (index, (tool, args, expected_line)) in cases.iter().enumerate() {
        session
            .call_tool(call(tool, args.clone()))
            .await
            .unwrap_or_else(|e| panic!("call {tool}: {e}"));

        let requests = stub.requests();
        let head = requests
            .get(index)
            .unwrap_or_else(|| panic!("{tool} must have produced request #{index}"));
        assert!(
            request_line(head).starts_with(expected_line.trim_end()),
            "{tool} must request `{expected_line}`; got `{}`",
            request_line(head)
        );
        assert!(
            head.to_ascii_lowercase().contains("idempotency-key:"),
            "{tool} must send an Idempotency-Key; head was:\n{head}"
        );
    }

    session.cancel().await.ok();
}

/// E11 — `expected_revision` becomes `If-Match`, and its **absence** sends no `If-Match`
/// at all.
///
/// The absence half is the one worth pinning: synthesising a precondition when the agent
/// did not ask for one would silently turn every write conditional and start refusing
/// writes that were meant to land last-writer-wins, which is the front's own default.
#[tokio::test]
async fn expected_revision_becomes_if_match_and_absence_sends_none() {
    let stub = StubAdmin::start("{}", "");
    let session = session_against(&stub.url, "k").await;

    session
        .call_tool(call(
            "stub_delete",
            serde_json::json!({ "port": 4545, "stub_id": "abc", "expected_revision": 7 }),
        ))
        .await
        .expect("conditional delete");
    session
        .call_tool(call(
            "stub_delete",
            serde_json::json!({ "port": 4545, "stub_id": "abc" }),
        ))
        .await
        .expect("unconditional delete");

    let requests = stub.requests();
    let conditional = requests.first().expect("first request");
    let unconditional = requests.get(1).expect("second request");

    assert!(
        conditional.to_ascii_lowercase().contains("if-match: 7"),
        "expected_revision must be sent as If-Match; head was:\n{conditional}"
    );
    assert!(
        !unconditional.to_ascii_lowercase().contains("if-match"),
        "a write with no expected_revision must send no If-Match; head was:\n{unconditional}"
    );

    session.cancel().await.ok();
}

/// AC3 end-to-end through the protocol: a 409 becomes `{conflict: true, current_revision}`,
/// and the revision comes from the **re-read** the tool performs, not from the message.
#[tokio::test]
async fn a_conflicted_write_answers_with_the_revision_from_a_re_read() {
    let stub = ScriptedAdmin::start(vec![
        (
            "409 Conflict",
            "",
            r#"{"errors":[{"code":"resource_conflict","message":"revision conflict: expected revision 3, stored revision 5 on port 4545"}]}"#,
        ),
        // The re-read of `imposters/4545`, whose header is the authority for the number.
        (
            "200 OK",
            "Rift-Cluster-Revision: default:4545@5\r\n",
            r#"{"port":4545}"#,
        ),
    ]);
    let session = session_against(&stub.url, "k").await;

    let result = session
        .call_tool(call(
            "stub_delete",
            serde_json::json!({ "port": 4545, "stub_id": "abc", "expected_revision": 3 }),
        ))
        .await
        .expect("a conflict is an answer, not a protocol error");

    let rendered = serde_json::to_string(&result).expect("serialize");
    assert!(
        rendered.contains(r#"\"conflict\":true"#) || rendered.contains(r#""conflict":true"#),
        "a 409 must answer with the conflict flag; got {rendered}"
    );
    assert!(
        rendered.contains("\\\"current_revision\\\":5")
            || rendered.contains(r#""current_revision":5"#),
        "the re-read's revision must reach the agent; got {rendered}"
    );

    // The re-read is of the *record*, not of the write path: there is no GET on
    // `/imposters/4545/stubs/by-id/abc` at all.
    let requests = stub.requests();
    let reread = requests.get(1).expect("a re-read must have been made");
    assert!(
        request_line(reread).starts_with("GET /imposters/4545 "),
        "the re-read must target the record; got `{}`",
        request_line(reread)
    );

    session.cancel().await.ok();
}

/// E9 — when the re-read itself fails, the conflict is still reported. Dropping it because
/// the enrichment failed would turn a refusal into an error the agent cannot act on.
#[tokio::test]
async fn a_conflict_survives_a_failed_re_read() {
    let stub = ScriptedAdmin::start(vec![
        (
            "409 Conflict",
            "",
            r#"{"errors":[{"message":"revision conflict: expected revision 3, stored revision 5 on port 4545"}]}"#,
        ),
        // The record is gone by the time we look: an absent-record conflict has no
        // current revision to report, and inventing one would be worse than omitting it.
        (
            "404 Not Found",
            "",
            r#"{"errors":[{"message":"no imposter on port 4545"}]}"#,
        ),
    ]);
    let session = session_against(&stub.url, "k").await;

    let result = session
        .call_tool(call(
            "stub_delete",
            serde_json::json!({ "port": 4545, "stub_id": "abc", "expected_revision": 3 }),
        ))
        .await
        .expect("the conflict must still be answered");

    let rendered = serde_json::to_string(&result).expect("serialize");
    assert!(
        rendered.contains("conflict") && rendered.contains("true"),
        "the conflict must survive a failed re-read; got {rendered}"
    );
    assert!(
        !rendered.contains("current_revision"),
        "an unknown revision must be omitted, never fabricated; got {rendered}"
    );

    session.cancel().await.ok();
}

/// AC4 through the protocol: a parked write answers `{parked: true, op_id}` rather than
/// failing, so the agent knows to poll instead of retrying or giving up.
/// The status is load-bearing here, not scenery: a `200` carrying the very same op-id
/// header is an *applied* write, and reading it as parked would tell an agent to go poll a
/// write that already landed. Hence a scripted `202` rather than the fixed-200 stub.
#[tokio::test]
async fn a_parked_write_answers_with_its_op_id() {
    let stub = ScriptedAdmin::start(vec![(
        "202 Accepted",
        "Rift-Cluster-Op-Id: 0189dcf0-0454-4e0b-a10c-8a8f8dccce1f\r\n",
        r#"{"opId":"0189dcf0-0454-4e0b-a10c-8a8f8dccce1f","opIds":[]}"#,
    )]);
    let session = session_against(&stub.url, "k").await;

    let result = session
        .call_tool(call(
            "stub_add",
            serde_json::json!({ "port": 4545, "stub": { "responses": [] } }),
        ))
        .await
        .expect("a parked write is an answer, not a protocol error");

    let rendered = serde_json::to_string(&result).expect("serialize");
    assert!(
        rendered.contains("parked") && rendered.contains("0189dcf0-0454-4e0b-a10c-8a8f8dccce1f"),
        "a parked write must hand back its op id; got {rendered}"
    );

    session.cancel().await.ok();
}

/// The other side of the same coin: a **committed** write also carries `Rift-Cluster-Op-Id`,
/// and must not be reported as parked.
///
/// Without this the parked test above proves less than it looks: an implementation that
/// classified on the header alone would pass it, and would then tell an agent to poll every
/// write it ever made.
#[tokio::test]
async fn an_applied_write_carrying_an_op_id_is_not_reported_as_parked() {
    let stub = StubAdmin::start(
        r#"{"port":4545}"#,
        "Rift-Cluster-Op-Id: 0189dcf0-0454-4e0b-a10c-8a8f8dccce1f\r\nRift-Cluster-Revision: default:4545@2\r\n",
    );
    let session = session_against(&stub.url, "k").await;

    let result = session
        .call_tool(call(
            "stub_add",
            serde_json::json!({ "port": 4545, "stub": { "responses": [] } }),
        ))
        .await
        .expect("applied write");

    let rendered = serde_json::to_string(&result).expect("serialize");
    assert!(
        !rendered.contains("parked"),
        "a 200 is an applied write, not a parked one; got {rendered}"
    );
    assert!(
        rendered.contains("current_revision"),
        "an applied write must report the revision to condition the next one on; got {rendered}"
    );

    session.cancel().await.ok();
}

/// E16 — `op_status` on an op the fleet does not know relays the API's own 404.
///
/// The tool is a thin pass-through, so the risk is not that it invents a status today but
/// that a later "helpful" default does: answering `pending` for an unknown op would tell an
/// agent to keep polling forever for a write that was never queued. Pinning the relay makes
/// that a test failure rather than a silent behaviour change.
#[tokio::test]
async fn op_status_on_an_unknown_op_relays_the_apis_own_404() {
    let stub = ScriptedAdmin::start(vec![(
        "404 Not Found",
        "",
        r#"{"errors":[{"message":"no such op 0189dcf0-0454-4e0b-a10c-8a8f8dccce1f"}]}"#,
    )]);
    let session = session_against(&stub.url, "k").await;

    let err = session
        .call_tool(call(
            "op_status",
            serde_json::json!({ "op_id": "0189dcf0-0454-4e0b-a10c-8a8f8dccce1f" }),
        ))
        .await
        .expect_err("an unknown op id must be the API's 404, not an invented status");

    let rendered = format!("{err:?}");
    assert!(
        rendered.contains("404") || rendered.contains("no such op"),
        "the API's own refusal must reach the agent; got {rendered}"
    );
    assert!(
        !rendered.contains("pending") && !rendered.contains("parked"),
        "an unknown op must never be reported as a state it could be polled out of; got {rendered}"
    );

    session.cancel().await.ok();
}

/// A `202` that identifies no op is a failure, not a silent success.
///
/// The wire-level complement to the unit test of the same property: a 202 means the write
/// is parked, so reporting it as applied would stop the agent from ever polling for a
/// change still sitting in the replay queue. Scripted with neither the header nor an
/// `opId` in the body — the shape a proxy produces when it normalises away an unknown
/// header and rewrites the entity.
#[tokio::test]
async fn a_202_that_identifies_no_op_is_not_reported_as_applied() {
    let stub = ScriptedAdmin::start(vec![("202 Accepted", "", r#"{"status":"accepted"}"#)]);
    let session = session_against(&stub.url, "k").await;

    let err = session
        .call_tool(call(
            "stub_add",
            serde_json::json!({ "port": 4545, "stub": { "responses": [] } }),
        ))
        .await
        .expect_err("a 202 with no op id must not be reported as an applied write");

    let rendered = format!("{err:?}");
    assert!(
        !rendered.contains("\"data\""),
        "a parked-but-unidentified write must not be rendered as an applied one; got {rendered}"
    );

    session.cancel().await.ok();
}

/// AC "by-id enforcement": no tool exposes index-based stub addressing.
///
/// Asserted against the **advertised input schemas**, which is what an agent actually reads
/// — the front still serves the index routes, and the guarantee is that this surface does
/// not project them.
#[tokio::test]
async fn no_tool_exposes_index_based_stub_addressing() {
    let stub = StubAdmin::start("{}", "");
    let session = session_against(&stub.url, "k").await;

    for tool in session.list_all_tools().await.expect("list tools") {
        let schema = serde_json::to_string(&tool.input_schema).expect("serialize schema");
        for banned in ["\"index\"", "\"stub_index\"", "\"stubIndex\""] {
            assert!(
                !schema.contains(banned),
                "tool `{}` exposes index-based addressing via {banned}: {schema}",
                tool.name
            );
        }
    }

    session.cancel().await.ok();
}

/// E7 / E8 on the wire — the `match` argument is forwarded as a query parameter,
/// and the answer's `scope` reports which half of the read path served it.
#[tokio::test]
async fn requests_query_forwards_match_and_reports_its_scope() {
    let stub = StubAdmin::start("[]", "");
    let session = session_against(&stub.url, "k").await;

    let fleet = session
        .call_tool(call("requests_query", serde_json::json!({ "port": 4545 })))
        .await
        .expect("fleet-scoped read");
    let rendered = serde_json::to_string(&fleet).expect("serialize");
    assert!(
        rendered.contains(r#"\"scope\":\"fleet\""#) || rendered.contains(r#""scope":"fleet""#),
        "a read with no `match` must report fleet scope; got {rendered}"
    );

    let node = session
        .call_tool(call(
            "requests_query",
            serde_json::json!({ "port": 4545, "match": r#"{"equals":{"method":"GET"}}"# }),
        ))
        .await
        .expect("node-scoped read");
    let rendered = serde_json::to_string(&node).expect("serialize");
    assert!(
        rendered.contains(r#"\"scope\":\"node\""#) || rendered.contains(r#""scope":"node""#),
        "a read with `match` must report node scope; got {rendered}"
    );

    let requests = stub.requests();
    let second = requests.get(1).expect("two requests");
    assert!(
        request_line(second).contains("match="),
        "the match predicate must be forwarded as a query parameter; got `{}`",
        request_line(second)
    );

    session.cancel().await.ok();
}

/// `verify` must report `node` scope.
///
/// The front proxies `POST /imposters/{port}/verify` to the local engine
/// (`admin_front.rs`'s proxied set) and the fleet-count decoration only ever applies
/// to a list read or a single-imposter GET, so the counts are the answering node's
/// journal alone. Reporting no scope would claim the fleet/node distinction does not
/// arise here — and on a 3-node fleet that is how `times(3)` comes back as 1 with
/// nothing to say why.
#[tokio::test]
async fn verify_reports_node_scope() {
    let stub = StubAdmin::start(r#"{"count":1}"#, "");
    let session = session_against(&stub.url, "k").await;

    let result = session
        .call_tool(call(
            "verify",
            serde_json::json!({ "port": 4545, "options": {} }),
        ))
        .await
        .expect("call verify");

    let rendered = serde_json::to_string(&result).expect("serialize");
    assert!(
        rendered.contains(r#"\"scope\":\"node\""#) || rendered.contains(r#""scope":"node""#),
        "verify must report node scope; got {rendered}"
    );

    session.cancel().await.ok();
}

/// The reads with no fleet/node distinction must omit `scope` rather than guess.
#[tokio::test]
async fn undecidable_reads_omit_scope() {
    let stub = StubAdmin::start("{}", "");
    let session = session_against(&stub.url, "k").await;

    for tool in ["imposter_list", "routes_get", "whoami"] {
        let result = session
            .call_tool(call(tool, serde_json::Value::Null))
            .await
            .unwrap_or_else(|e| panic!("call {tool}: {e}"));
        let rendered = serde_json::to_string(&result).expect("serialize");
        assert!(
            !rendered.contains("scope"),
            "{tool} has no fleet/node distinction and must omit `scope`; got {rendered}"
        );
    }

    session.cancel().await.ok();
}

/// E9, end to end — a `Rift-Cluster-Partial` header stamped by the front reaches
/// the agent's answer rather than being dropped somewhere in the tool layer.
#[tokio::test]
async fn partial_marker_reaches_the_tool_answer() {
    let stub = StubAdmin::start("[]", "Rift-Cluster-Partial: node-3 unreachable\r\n");
    let session = session_against(&stub.url, "k").await;

    let result = session
        .call_tool(call("imposter_list", serde_json::Value::Null))
        .await
        .expect("call imposter_list");

    let rendered = serde_json::to_string(&result).expect("serialize");
    assert!(
        rendered.contains("node-3 unreachable"),
        "the partial marker must reach the agent; got {rendered}"
    );

    session.cancel().await.ok();
}

/// The `lint` tool must not touch the network — it is documented as a dry run.
#[tokio::test]
async fn lint_makes_no_network_call() {
    let stub = StubAdmin::start("{}", "");
    let session = session_against(&stub.url, "k").await;

    session
        .call_tool(call(
            "lint",
            serde_json::json!({ "json": "{ not json", "source_name": "bad.json" }),
        ))
        .await
        .expect("call lint");

    assert!(
        stub.requests().is_empty(),
        "lint must not call the admin API; it saw {:?}",
        stub.requests()
    );

    session.cancel().await.ok();
}

/// E16 / AC2 — an unreachable fleet is a clean typed transport failure, not a panic
/// and not a success with an empty answer.
#[tokio::test]
async fn unreachable_host_is_a_transport_error() {
    // Bind and immediately drop, so the port is one nothing is listening on.
    let addr = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.local_addr().expect("addr")
    };

    let client = AdminClient::new(&args_for(&format!("http://{addr}"), "k")).expect("build client");
    let failure = client
        .get_json("imposters", &[])
        .await
        .expect_err("an unreachable host must fail");

    assert!(
        matches!(failure, ToolFailure::Transport { .. }),
        "expected Transport, got {failure:?}"
    );
}

/// AC2 — a rejected key surfaces as `Unauthorized` carrying the API's own body, and
/// the rendered message never contains the key itself.
#[tokio::test]
async fn rejected_key_is_a_clean_unauthorized_error() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        let Ok((mut socket, _)) = listener.accept() else {
            return;
        };
        let mut buf = [0u8; 4096];
        let _ = socket.read(&mut buf);
        let body = r#"{"error":"unauthorized"}"#;
        let response = format!(
            "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = socket.write_all(response.as_bytes());
    });

    let client =
        AdminClient::new(&args_for(&format!("http://{addr}"), "wrong-key")).expect("build client");
    let failure = client
        .get_json("imposters", &[])
        .await
        .expect_err("a 401 must fail");

    assert!(
        matches!(failure, ToolFailure::Unauthorized { .. }),
        "expected Unauthorized, got {failure:?}"
    );
    let rendered = failure.to_string();
    assert!(
        rendered.contains(r#"{"error":"unauthorized"}"#),
        "the API's body must be relayed: {rendered}"
    );
    assert!(
        !rendered.contains("wrong-key"),
        "the key must never appear in an error message: {rendered}"
    );
}
