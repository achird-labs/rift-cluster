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
