//! Issue #631: a `proxyOnce` request whose client disconnects mid-forward gives its claim back
//! immediately, end to end through a real clustered engine.
//!
//! #632 pinned the store half — `ClusterProxyStore::release_claim` is safe and effective when
//! called from a destructor (D-90) — but drove the seam directly. Nothing exercised the engine's
//! own guard (rift#1197's `HeldClaim`, vendored since `24bf587`), which is what turns a dropped
//! request future into that call. This is that proof: a real imposter, a real forward, a real
//! socket closed while the upstream is still thinking.
//!
//! **The disconnect travels two hops, not one.** The raw socket is closed on the EE front; hyper
//! drops that service future, which drops the pooled **loopback leg** to the engine's admin
//! listener, whose own service future then drops and runs `HeldClaim::drop`. Both legs are
//! HTTP/1 today (`admin_front.rs`'s `build_http()`), and the propagation depends on the h1 client
//! dispatcher *closing* that pooled connection rather than returning it to the pool. If that leg
//! ever negotiated h2 the cancellation shape changes and this test is what would go mysteriously
//! red — so the hop is named here rather than left to be rediscovered.
//!
//! **What the assertion rests on.** The claim's state is not observable from here:
//! `ComposedServer` exposes `node()` and `flow_net()` but no `proxy_net()`, and `pending_claims`
//! has no HTTP surface. It does not need one, because the *upstream connection count* separates
//! the two worlds by itself (D-66, D-90):
//!
//! | after the disconnect | claim released | claim still held |
//! |---|---|---|
//! | request 2 | claims, forwards, **records** | `InFlight` → forwards, records nothing |
//! | request 3 | `AlreadyRecorded` → **replays** | `InFlight` → forwards again |
//! | origin connections | **2** | **3** |
//!
//! Both worlds answer `200` with the same body, so **the connection count is the whole
//! discriminator** — do not "simplify" it away.
//!
//! **Two ceilings make this test meaningful, and both are load-bearing.** D-40's claim TTL is
//! 60 s, and the engine's own `PROXY_HTTP_CLIENT_TIMEOUT` is **30 s** — against a stalling origin
//! that timeout fires and releases the claim through the *failed-forward* path, **in the broken
//! world too**. So the whole claim→request-3 window must stay well under 30 s or this test can go
//! green against an engine with no guard at all. `WINDOW` asserts that directly; do not raise it
//! toward 30 s, and do not "fix a flake" here by raising `BOUND`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod common;

use common::ports::{reserve_addr, reserve_port};

const SECRET: &str = "proxy-disconnect-secret";
/// Hang-guard for every wait. The test is a few hundred ms of real work, so anything near this is
/// a hang, not a slow runner.
const BOUND: Duration = Duration::from_secs(20);
/// The claim→request-3 window must finish far inside the engine's 30 s proxy-client timeout,
/// which would otherwise release the claim on its own and make the discriminator meaningless.
const WINDOW: Duration = Duration::from_secs(10);
/// How long request 2 may take once the engine has been seen to abandon the forward. The release
/// is one bridge call (2 s deadline) plus at most one cross-node hop, so this separates "released
/// synchronously with the drop" from "released later by something else" — AC4's actual claim.
const PROMPT: Duration = Duration::from_secs(5);

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(BOUND)
        .build()
        .expect("client builds")
}

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

async fn wait_ready(server: &ComposedServer) {
    let probes = server.probe_addr().expect("probes bound under --cluster");
    let deadline = Instant::now() + BOUND;
    loop {
        if let Ok(response) = client().get(format!("http://{probes}/readyz")).send().await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(Instant::now() < deadline, "node never became ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Read an HTTP request head byte by byte, to the blank line. `None` if the peer went away first.
async fn read_head(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match socket.read(&mut byte).await {
            Ok(0) | Err(_) => return None,
            Ok(_) => head.push(byte[0]),
        }
    }
    Some(head)
}

/// The upstream the `proxyOnce` imposter forwards to, on an ephemeral port private to this test.
///
/// Hand-rolled rather than a second imposter with a `wait` behavior, because the point is to stall
/// **one** forward and then serve normally: the first connection is read and never answered,
/// holding request 1 inside its forward, and every later connection gets an ordinary `200`. A
/// `wait` behavior is per-stub, so it would stall request 2 as well — and requests 1 and 2 must
/// share a path, or they are different proxyOnce signatures and the test proves nothing.
///
/// Returns the port, the accepted-connection counter (the discriminator), and a signal that fires
/// when the **held** connection sees EOF — i.e. when the engine has actually abandoned request 1's
/// forward. That signal is what lets the test stop racing the cancellation.
fn stalling_origin() -> (u16, Arc<AtomicUsize>, tokio::sync::oneshot::Receiver<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("origin binds");
    let port = listener.local_addr().expect("origin addr").port();
    listener.set_nonblocking(true).expect("nonblocking");
    let connections = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&connections);
    let (abandoned_tx, abandoned_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener).expect("adopt listener");
        let mut abandoned_tx = Some(abandoned_tx);
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let seen = counter.fetch_add(1, Ordering::SeqCst) + 1;
            if seen == 1 {
                // Held, not dropped: dropping it would answer request 1 with a transport error,
                // which releases the claim through the failed-forward path instead of the
                // destructor this test is about.
                let notify = abandoned_tx.take();
                tokio::spawn(async move {
                    // Read the head first — `accept()` alone only proves *something* connected,
                    // while a parsed request head proves a real forward arrived.
                    if read_head(&mut socket).await.is_none() {
                        return;
                    }
                    // Never answer; just wait for the peer to go away. That is the engine
                    // dropping the forward future because its own caller vanished.
                    let mut sink = [0_u8; 256];
                    while let Ok(n) = socket.read(&mut sink).await {
                        if n == 0 {
                            break;
                        }
                    }
                    if let Some(notify) = notify {
                        let _ = notify.send(());
                    }
                });
                continue;
            }
            tokio::spawn(async move {
                if read_head(&mut socket).await.is_none() {
                    return;
                }
                // `connection: close` is load-bearing, not decoration: it stops the engine's
                // client pooling this upstream connection. Were it pooled, request 3's forward in
                // the *broken* world would reuse it, the origin would never `accept()` a third
                // time, the count would stay at 2 and this test would go green against a broken
                // engine. Do not remove it.
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-length: 11\r\nconnection: close\r\n\r\nfrom-origin",
                    )
                    .await;
                let _ = socket.flush().await;
                let _ = socket.shutdown().await;
            });
        }
    });

    (port, connections, abandoned_rx)
}

fn proxy_once_imposter(port: u16, origin_port: u16) -> serde_json::Value {
    json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "responses": [{
                "proxy": {
                    "to": format!("http://127.0.0.1:{origin_port}"),
                    "mode": "proxyOnce",
                    "predicateGenerators": [{ "matches": { "path": true } }],
                }
            }]
        }],
    })
}

async fn wait_connections(origin: &Arc<AtomicUsize>, want: usize, what: &str) {
    let deadline = Instant::now() + BOUND;
    loop {
        if origin.load(Ordering::SeqCst) >= want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: the origin never saw {want} connection(s) (saw {})",
            origin.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wait until `admin`'s applied state carries the imposter.
///
/// Deliberately a **config-plane** poll, not a warm-up through `/__rift/{port}/`: this imposter's
/// only stub is a proxy stub, so any data-plane probe is itself a forward — it would burn the
/// origin's stalled first connection and count toward the discriminator.
async fn wait_applied(admin: &str, port: u16) {
    let deadline = Instant::now() + BOUND;
    loop {
        if let Ok(response) = client()
            .get(format!("http://{admin}/imposters/{port}"))
            .send()
            .await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{admin} never applied imposter {port}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Pins D-90 end to end: the engine's `HeldClaim` guard turns a dropped request future into a
/// `release_claim`, and for this store that frees a **fleet-wide** claim at once rather than
/// wedging the signature for `claim_ttl`.
///
/// What falsifies it: an engine without rift#1197's guard. `git -C vendor/rift checkout a85f550`
/// and re-running is the mutation — it removes the guard itself rather than simulating its
/// absence, and the origin then sees three connections instead of two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_disconnect_mid_proxy_once_frees_the_claim() {
    let founder_state = TempDir::new().expect("tempdir");
    let joiner_state = TempDir::new().expect("tempdir");
    let founder_bind = reserve_addr();

    let founder = compose::start(cluster_on(
        &founder_state,
        &founder_bind,
        &["--cluster-allow-solo"],
    ))
    .await
    .expect("founder starts");
    wait_ready(&founder).await;
    // Two nodes, so the claim is arbitrated by the real clustered store rather than a solo
    // shortcut. Which node *owns* this signature is decided by the HRW ring and is not forced
    // here — the cross-node release hop is already pinned by
    // `crates/rift-cluster/tests/proxy_claims.rs`; what this test adds is the engine-side trigger.
    let joiner = compose::start(cluster_on(
        &joiner_state,
        &reserve_addr(),
        &["--cluster-seeds", &founder_bind],
    ))
    .await
    .expect("joiner starts");
    wait_ready(&joiner).await;

    let admin = format!("127.0.0.1:{}", founder.admin_addr().port());
    let joiner_admin = format!("127.0.0.1:{}", joiner.admin_addr().port());
    let (origin_port, origin, abandoned) = stalling_origin();
    let port = reserve_port();

    let created = tokio::time::timeout(
        BOUND,
        client()
            .post(format!("http://{admin}/imposters"))
            .json(&proxy_once_imposter(port, origin_port))
            .send(),
    )
    .await
    .expect("creating the imposter did not hang")
    .expect("post imposter");
    assert_eq!(
        created.status().as_u16(),
        201,
        "the proxyOnce imposter was admitted"
    );
    wait_applied(&admin, port).await;
    wait_applied(&joiner_admin, port).await;

    // -- request 1: claim it, stall in the forward, then vanish ------------
    let mut socket = tokio::time::timeout(BOUND, tokio::net::TcpStream::connect(admin.clone()))
        .await
        .expect("connect did not hang")
        .expect("raw socket to the gateway");
    socket
        .write_all(format!("GET /__rift/{port}/once HTTP/1.1\r\nHost: {admin}\r\n\r\n").as_bytes())
        .await
        .expect("send request 1");
    socket.flush().await.expect("flush request 1");

    // The origin reading a request head proves request 1 is past `try_claim` and inside the
    // forward, holding the claim — the precondition, observed from outside the claim table.
    wait_connections(&origin, 1, "request 1 must reach the origin").await;
    let claim_held_at = Instant::now();
    drop(socket);

    // Wait for the engine to actually abandon the forward rather than racing it. Without this,
    // request 2 can reach the claim owner *before* the release does, land `InFlight`, and fail at
    // the final assertion with the same `3 != 2` shape as a real regression — a flake
    // indistinguishable from the bug this test exists to catch.
    tokio::time::timeout(BOUND, abandoned)
        .await
        .expect("the engine must notice the disconnect")
        .expect("the origin's held connection reported EOF");
    let abandoned_at = Instant::now();

    // -- request 2: the signature must be claimable again ------------------
    let second = tokio::time::timeout(
        BOUND,
        client()
            .get(format!("http://{admin}/__rift/{port}/once"))
            .send(),
    )
    .await
    .expect("request 2 did not hang")
    .expect("request 2");
    assert_eq!(
        second.status().as_u16(),
        200,
        "request 2 is served (origin connections so far: {}). A non-2xx here means the forward \
         itself failed rather than the claim being stuck — read the count before concluding \
         anything about the release",
        origin.load(Ordering::SeqCst)
    );
    assert_eq!(
        second.text().await.expect("body 2"),
        "from-origin",
        "request 2 is answered by the origin"
    );
    assert!(
        abandoned_at.elapsed() < PROMPT,
        "the claim must be free as soon as the forward is abandoned: request 2 took {:?}, slow \
         enough that something other than the Drop guard may have released it",
        abandoned_at.elapsed()
    );
    wait_connections(&origin, 2, "request 2 must reach the origin").await;

    // -- request 3: recorded, so it replays without a third forward --------
    let third = tokio::time::timeout(
        BOUND,
        client()
            .get(format!("http://{admin}/__rift/{port}/once"))
            .send(),
    )
    .await
    .expect("request 3 did not hang")
    .expect("request 3");
    assert_eq!(third.status().as_u16(), 200, "request 3 is served");
    assert_eq!(
        third.text().await.expect("body 3"),
        "from-origin",
        "request 3 replays the recorded body"
    );

    // Before trusting the count, confirm the run was fast enough for it to mean anything: past
    // the engine's 30 s proxy-client timeout the stalled forward releases the claim by itself,
    // in the broken world too, and a `2` would prove nothing.
    assert!(
        claim_held_at.elapsed() < WINDOW,
        "the claim→request-3 window took {:?}, too close to the engine's 30 s proxy timeout for \
         the connection count to discriminate — this run is inconclusive, not a pass",
        claim_held_at.elapsed()
    );
    assert_eq!(
        origin.load(Ordering::SeqCst),
        2,
        "the origin must see exactly two connections: the abandoned request 1 and the recording \
         request 2. A third means request 2 was answered without recording — usually because the \
         disconnect did not give the claim back, but a momentary `Fenced`/`NotOwner`/isolated-owner \
         wobble forwards without recording too, so check the nodes for a \"proxy recording store \
         unavailable\" warn before blaming the guard"
    );

    founder.shutdown().await;
    joiner.shutdown().await;
}
