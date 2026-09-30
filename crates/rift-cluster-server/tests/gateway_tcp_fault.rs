//! Issue #638 (upstream rift#1234): a TCP fault reached through a cluster node's shared listeners
//! aborts the client connection exactly as it does on the imposter's own port, instead of
//! answering with an HTTP status.
//!
//! Three doors reach an imposter without its own port: the **admin front's** `/__rift/` gateway
//! (the cluster's own listener, D-92), the **front door's** `/__rift/` fallback, and a **front
//! door route**. Before #638 the admin front proxied its gateway leg to the loopback admin
//! listener, which since rift 0.19.0 aborts on a fault — so the front answered its own
//! `503 local admin backend unreachable` instead, and a client could not tell a mock's fault from
//! a dead node.
//!
//! HTTP/1: per fault kind, byte-for-byte the imposter port's behaviour. HTTP/2 (the front door
//! only — the admin front is an HTTP/1 listener): every kind resets that one stream, and sibling
//! streams on the connection survive.
//!
//! Ported from upstream's `crates/rift-http-proxy/tests/issue_1234_gateway_tcp_fault.rs`.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use clap::Parser;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

mod common;

use common::TEST_LOCK;
use common::ports::reserve_port;

const SECRET: &str = "gateway-tcp-fault-test-secret";

/// Every carrier site: a top-level Mountebank `fault`, `_rift.fault.tcp`, and a script `reset()`.
const FAULT_PATHS: [&str; 5] = ["/reset", "/empty", "/garbage", "/malformed", "/script"];

/// A solo clustered node with a front door, and one fault imposter reachable through every door.
struct Fixture {
    server: ComposedServer,
    _state: TempDir,
    /// The imposter's own port — the behaviour every other door must reproduce.
    port: u16,
    /// The admin front's `/__rift/{port}` gateway prefix.
    admin_gateway: String,
    /// The front door's `/__rift/{port}` fallback prefix.
    front_gateway: String,
    /// The front door route that strips `/routed` and targets the imposter.
    front_route: String,
}

impl Fixture {
    async fn start() -> Self {
        let state = TempDir::new().expect("tempdir");
        let cli = EeCli::try_parse_from([
            "rift-cluster-server",
            "--port",
            "0",
            "--metrics-port",
            "0",
            "--cluster",
            "--cluster-bind",
            "127.0.0.1:0",
            "--cluster-probe-bind",
            "127.0.0.1:0",
            "--cluster-secret",
            SECRET,
            "--cluster-state-dir",
            &state.path().to_string_lossy(),
            "--cluster-allow-solo",
            "--front-door",
            "127.0.0.1:0",
            // The `/script` stub is an inline script.
            "--allowInjection",
        ])
        .expect("parses");
        let server = compose::start(cli).await.expect("solo cluster starts");
        wait_ready(&server).await;

        let admin = server.admin_addr();
        let front_door = server
            .front_door_addr()
            .expect("--front-door was given, must bind")
            .to_string();
        let port = reserve_port();
        let client = reqwest::Client::new();

        let created = client
            .post(format!("http://{admin}/imposters"))
            .json(&fault_imposter(port))
            .send()
            .await
            .expect("post imposter");
        assert_eq!(created.status().as_u16(), 201, "fault imposter admitted");

        let routed = client
            .put(format!("http://{admin}/front-door/routes"))
            .json(&json!({
                "routes": [{
                    "id": "routed",
                    "match": { "path_prefix": "/routed" },
                    "target": { "port": port, "strip_prefix": true },
                }],
            }))
            .send()
            .await
            .expect("put routes");
        assert_eq!(routed.status().as_u16(), 200, "route committed");

        let fixture = Self {
            port,
            admin_gateway: format!("http://{admin}/__rift/{port}"),
            front_gateway: format!("http://{front_door}/__rift/{port}"),
            front_route: format!("http://{front_door}/routed"),
            server,
            _state: state,
        };
        fixture.wait_bound().await;
        fixture
    }

    /// Every door, labelled, as a URL prefix a stub path is appended to.
    fn doors(&self) -> [(&'static str, &str); 3] {
        [
            ("admin front /__rift/", self.admin_gateway.as_str()),
            ("front door /__rift/", self.front_gateway.as_str()),
            ("front door route", self.front_route.as_str()),
        ]
    }

    /// The commit barrier waits for apply, not for the imposter's socket; poll the imposter's own
    /// port until the ordinary stub answers.
    async fn wait_bound(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let url = format!("http://127.0.0.1:{}/ok", self.port);
        loop {
            if let Ok(response) = reqwest::get(&url).await
                && response.status().as_u16() == 200
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "imposter {} never bound",
                self.port
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
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

fn fault_imposter(port: u16) -> serde_json::Value {
    json!({
        "port": port, "protocol": "http", "recordRequests": true,
        "stubs": [
            { "predicates": [{ "equals": { "path": "/reset" } }],
              "responses": [{ "fault": "CONNECTION_RESET_BY_PEER" }] },
            { "predicates": [{ "equals": { "path": "/empty" } }],
              "responses": [{ "is": { "statusCode": 200, "body": "never-seen" },
                              "_rift": { "fault": { "tcp": "empty" } } }] },
            { "predicates": [{ "equals": { "path": "/garbage" } }],
              "responses": [{ "is": { "statusCode": 200, "body": "never-seen" },
                              "_rift": { "fault": { "tcp": "garbage" } } }] },
            { "predicates": [{ "equals": { "path": "/malformed" } }],
              "responses": [{ "fault": "MALFORMED_RESPONSE_CHUNK" }] },
            { "predicates": [{ "equals": { "path": "/script" } }],
              "responses": [{ "_rift": { "script": {
                  "engine": "rhai", "code": "fn respond(ctx) { reset() }" } } }] },
            { "predicates": [{ "equals": { "path": "/ok" } }],
              "responses": [{ "is": { "statusCode": 200, "body": "ok" } }] }
        ]
    })
}

/// What an HTTP/1 client observes (upstream's probe, `admin_api_integration.rs`'s `tcp_faults`).
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Observed {
    /// The request failed before a valid HTTP response (reset / empty close / bad framing).
    SendFailed,
    /// Response headers parsed (real status line) but the body read failed.
    BodyFailed,
    /// A complete HTTP response with this status: the fault did NOT fire.
    FullResponse(u16),
}

/// A fresh HTTP/1 client per call, so one probe's aborted connection is never reused by the next.
async fn observe(url: &str) -> Observed {
    let client = reqwest::Client::builder()
        .http1_only()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("h1 client");
    match client.get(url).send().await {
        Err(_) => Observed::SendFailed,
        Ok(response) => {
            let status = response.status().as_u16();
            match response.bytes().await {
                Err(_) => Observed::BodyFailed,
                Ok(_) => Observed::FullResponse(status),
            }
        }
    }
}

fn expected(path: &str) -> Observed {
    match path {
        // The status line of the malformed-chunk fault parses; only the body fails.
        "/malformed" => Observed::BodyFailed,
        _ => Observed::SendFailed,
    }
}

/// Pins D-92: the admin front answers its gateway leg in-process, so a TCP fault through it — and
/// through both front-door paths — fails exactly as the imposter's own port does, per kind, and
/// never as an HTTP answer (the loopback proxy leg this replaced answered `503`).
#[tokio::test]
async fn h1_fault_through_every_door_matches_the_imposter_port() {
    let _guard = TEST_LOCK.lock().await;
    let fixture = Fixture::start().await;

    for path in FAULT_PATHS {
        let direct = observe(&format!("http://127.0.0.1:{}{path}", fixture.port)).await;
        assert_eq!(direct, expected(path), "imposter port, {path}");
        for (door, base) in fixture.doors() {
            assert_eq!(
                observe(&format!("{base}{path}")).await,
                direct,
                "{door} {path} must fail the way the imposter port does, not answer with a status"
            );
        }
    }

    // An ordinary stub on the same imposter still answers through every door.
    for (door, base) in fixture.doors() {
        let response = reqwest::get(format!("{base}/ok"))
            .await
            .unwrap_or_else(|e| panic!("{door}: ok request: {e}"));
        assert_eq!(response.status().as_u16(), 200, "{door}");
        assert_eq!(response.text().await.expect("body"), "ok", "{door}");
    }

    fixture.server.shutdown().await;
}

/// The front door negotiates HTTP/2 (the admin front does not): a fault resets only its own
/// stream with `INTERNAL_ERROR`, and a sibling request on the same connection still answers.
#[tokio::test]
async fn h2_fault_through_the_front_door_resets_only_its_stream() {
    let _guard = TEST_LOCK.lock().await;
    let fixture = Fixture::start().await;
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("h2 client");
    let ok = |url: String| {
        let client = client.clone();
        async move {
            let response = client.get(&url).send().await.expect("h2 ok request");
            assert_eq!(response.version(), reqwest::Version::HTTP_2, "{url}");
            assert_eq!(response.status().as_u16(), 200, "{url}");
            assert_eq!(response.text().await.expect("body"), "ok", "{url}");
        }
    };

    for base in [&fixture.front_gateway, &fixture.front_route] {
        ok(format!("{base}/ok")).await;
        for path in FAULT_PATHS {
            let err = client
                .get(format!("{base}{path}"))
                .send()
                .await
                .expect_err(&format!("{base}{path}: an h2 fault must reset the stream"));
            let chain = format!("{err:?}");
            assert!(
                chain.contains("INTERNAL_ERROR"),
                "{base}{path}: expected RST_STREAM(INTERNAL_ERROR), got {chain}"
            );
            ok(format!("{base}/ok")).await;
        }
    }

    fixture.server.shutdown().await;
}

/// h1 keep-alive: ok, fault, ok on one client. The fault kills that connection; the listener and
/// the client's next request (on a new connection) are unaffected — in particular the admin
/// front's `FaultIo` is per connection, so a fault never arms anyone else's.
#[tokio::test]
async fn h1_keep_alive_ok_fault_ok() {
    let _guard = TEST_LOCK.lock().await;
    let fixture = Fixture::start().await;

    for (door, base) in fixture.doors() {
        let client = reqwest::Client::builder()
            .http1_only()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("h1 client");
        let first = client.get(format!("{base}/ok")).send().await.expect("ok");
        assert_eq!(first.text().await.expect("body"), "ok", "{door}");
        assert!(
            client.get(format!("{base}/reset")).send().await.is_err(),
            "{door}: /reset must abort, not answer"
        );
        let last = client.get(format!("{base}/ok")).send().await.expect("ok");
        assert_eq!(last.text().await.expect("body"), "ok", "{door}");
    }

    // The admin plane on the same listener is untouched by the gateway's faults.
    let imposters = reqwest::get(format!("http://{}/imposters", fixture.server.admin_addr()))
        .await
        .expect("admin read after faults");
    assert_eq!(imposters.status().as_u16(), 200);

    fixture.server.shutdown().await;
}

// ---------------------------------------------------------------------------------------------
// Logging: an injected fault is the configured behaviour of a stub, not a server error.
// ---------------------------------------------------------------------------------------------

type Captured = Arc<Mutex<Vec<(tracing::Level, String)>>>;

struct Capture(Captured);

struct MessageVisitor<'a>(&'a mut String);

impl tracing::field::Visit for MessageVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0.push_str(&format!("{value:?}"));
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let meta = event.metadata();
        if meta.target().starts_with("rift") && *meta.level() <= tracing::Level::DEBUG {
            let mut message = String::new();
            event.record(&mut MessageVisitor(&mut message));
            self.0
                .lock()
                .expect("capture lock")
                .push((*meta.level(), message));
        }
    }
}

/// Process-wide capture: listener tasks run on runtime worker threads, which a thread-local
/// default would not see.
fn captured() -> Captured {
    static CAPTURED: OnceLock<Captured> = OnceLock::new();
    CAPTURED
        .get_or_init(|| {
            use tracing_subscriber::layer::SubscriberExt;
            let captured = Captured::default();
            let subscriber = tracing_subscriber::registry().with(Capture(captured.clone()));
            tracing::subscriber::set_global_default(subscriber).expect("install log capture");
            captured
        })
        .clone()
}

#[tokio::test]
async fn injected_faults_are_logged_at_debug_not_error() {
    let _guard = TEST_LOCK.lock().await;
    let captured = captured();
    let fixture = Fixture::start().await;

    for path in FAULT_PATHS {
        for (_, base) in fixture.doors() {
            let _ = observe(&format!("{base}{path}")).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let lines = captured.lock().expect("capture lock").clone();
    let errors: Vec<_> = lines
        .iter()
        .filter(|(level, msg)| *level == tracing::Level::ERROR && msg.contains("connection"))
        .collect();
    assert!(
        errors.is_empty(),
        "an injected fault must not log a connection ERROR: {errors:?}"
    );
    assert!(
        lines
            .iter()
            .any(|(level, msg)| *level == tracing::Level::DEBUG
                && msg.contains("admin front connection aborted by an injected TCP fault")),
        "the admin front records the abort, at debug"
    );

    fixture.server.shutdown().await;
}
