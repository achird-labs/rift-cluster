//! Issue #223's merge-on-read acceptance criteria, end to end against a real
//! three-node cluster.
//!
//! These live outside `clustered.rs` on purpose: that file's charter is the
//! *composition wiring* — bootstrap, seed-join, graceful leave, cleanup on the
//! error paths — and it drives `compose::start` to catch bugs that live in the
//! ordering of those steps. What is under test here is a **semantic** contract
//! layered on top of a cluster that already started correctly: that a
//! `savedRequests` read answers for the fleet rather than for whichever node
//! the load balancer happened to pick.
//!
//! Traffic is driven through each node's **own front door** rather than the
//! imposter's bound port. Three in-process nodes cannot each bind port `P` —
//! the first wins it and the rest serve the imposter unbound (issue #143's
//! `with_serve_unbound`) — so a request to `127.0.0.1:P` would always land on
//! the same node and every one of these tests would pass vacuously against a
//! single shard. Per-node front doors are what make the shards genuinely
//! distinct, and the `?since=` local reads below assert that they are.

use std::collections::BTreeSet;
use std::time::Duration;

use clap::Parser;
use rift_cluster::decorate::{HEADER_NEXT_INDEX, HEADER_PARTIAL, HEADER_TRUNCATED};
use rift_cluster::stores::JournalCursor;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use serde_json::json;
use tempfile::TempDir;

mod common;

use common::ports::{reserve_addr, reserve_port};
use common::seen::Seen;

const SECRET: &str = "fleet-journal-test-secret";

/// One clustered node with its own front door. Every port is ephemeral except
/// the cluster bind, which joiners need in order to seed off the founder.
fn node_cli(state: &TempDir, cluster_bind: &str, extra: &[&str]) -> EeCli {
    let mut args = vec![
        "rift-cluster-server".to_owned(),
        "--port".to_owned(),
        "0".to_owned(),
        "--metrics-port".to_owned(),
        "0".to_owned(),
        "--cluster".to_owned(),
        "--cluster-bind".to_owned(),
        cluster_bind.to_owned(),
        "--cluster-probe-bind".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--cluster-secret".to_owned(),
        SECRET.to_owned(),
        "--cluster-state-dir".to_owned(),
        state.path().to_string_lossy().into_owned(),
        "--front-door".to_owned(),
        "127.0.0.1:0".to_owned(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

async fn wait_ready(server: &ComposedServer, what: &str) {
    let probes = server.probe_addr().expect("probes bound under --cluster");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = reqwest::get(format!("http://{probes}/readyz")).await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: /readyz never reached 200"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `node`'s membership until it holds exactly `want` voters, bounded.
async fn wait_voter_count(node: &rift_cluster::RaftNode, want: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
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

/// `recordRequests` is not optional here: `Imposter::record_request` returns
/// early when it is off, so without it every journal in the fleet stays empty
/// and every assertion in this file would compare three empty sets and pass.
fn minimal_imposter(port: u16) -> serde_json::Value {
    json!({
        "port": port,
        "protocol": "http",
        "recordRequests": true,
        "stubs": [{
            "id": "a",
            "responses": [{ "is": { "statusCode": 201, "body": "recorded" } }],
        }],
    })
}

fn one_route(path_prefix: &str, target_port: u16) -> serde_json::Value {
    json!({
        "routes": [{
            "id": "svc",
            "match": { "path_prefix": path_prefix },
            "target": { "port": target_port },
        }],
    })
}

/// The whole front-door route table, one entry per `(path prefix, imposter)`.
///
/// `PUT /front-door/routes` is a whole-table replace, so reaching a *second* imposter means putting
/// both routes at once rather than adding one (issue #362's tests need two).
fn routes_for(routes: &[(&str, u16)]) -> serde_json::Value {
    json!({
        "routes": routes
            .iter()
            .enumerate()
            .map(|(index, (prefix, port))| json!({
                "id": format!("svc-{index}"),
                "match": { "path_prefix": prefix },
                "target": { "port": port },
            }))
            .collect::<Vec<_>>(),
    })
}

/// A three-node fleet, all ready, all voters, sharing one imposter and one
/// front-door route — the fixture every test in this file starts from.
struct Fleet {
    nodes: Vec<ComposedServer>,
    port: u16,
}

impl Fleet {
    async fn start(states: &[TempDir; 3]) -> Self {
        let founder_bind = reserve_addr();
        let founder = compose::start(node_cli(
            &states[0],
            &founder_bind,
            &["--cluster-allow-solo"],
        ))
        .await
        .expect("founder starts");
        wait_ready(&founder, "founder").await;

        let mut nodes = vec![founder];
        for (index, state) in states.iter().enumerate().skip(1) {
            let joiner = compose::start(node_cli(
                state,
                &reserve_addr(),
                &["--cluster-seeds", &founder_bind],
            ))
            .await
            .unwrap_or_else(|e| panic!("joiner {index} starts: {e}"));
            wait_ready(&joiner, &format!("joiner {index}")).await;
            nodes.push(joiner);
        }

        let founder_node = nodes[0].node().expect("clustered").clone();
        wait_voter_count(&founder_node, 3).await;

        // Both writes go to the founder and replicate: an imposter and a route
        // are control ops, so asserting per-node propagation is not this
        // fixture's job — `wait_dispatching` below just waits for it.
        let admin = nodes[0].admin_addr();
        let port = reserve_port();
        let client = reqwest::Client::new();
        let created = client
            .post(format!("http://{admin}/imposters"))
            .json(&minimal_imposter(port))
            .send()
            .await
            .expect("post imposter");
        assert_eq!(
            created.status().as_u16(),
            201,
            "imposter setup must succeed"
        );
        let routed = client
            .put(format!("http://{admin}/front-door/routes"))
            .json(&one_route("/svc", port))
            .send()
            .await
            .expect("put routes");
        assert_eq!(routed.status().as_u16(), 200, "route setup must succeed");

        let fleet = Self { nodes, port };
        for index in 0..fleet.nodes.len() {
            fleet.wait_dispatching(index).await;
        }
        fleet
    }

    fn admin(&self, index: usize) -> std::net::SocketAddr {
        self.nodes[index].admin_addr()
    }

    /// Install a second imposter behind `/svc2` and widen the route table to reach both (issue
    /// #362). Returns its port.
    ///
    /// The fixture deliberately ships one imposter, because every test before this one was about a
    /// single port's shards. A *fleet* journal cannot be tested that way: with one imposter, a walk
    /// that silently covered only the first port would pass everything.
    async fn add_imposter(&self) -> u16 {
        let admin = self.admin(0);
        let second = reserve_port();
        let client = reqwest::Client::new();
        let created = client
            .post(format!("http://{admin}/imposters"))
            .json(&minimal_imposter(second))
            .send()
            .await
            .expect("post second imposter");
        assert_eq!(
            created.status().as_u16(),
            201,
            "second imposter setup must succeed"
        );
        let routed = client
            .put(format!("http://{admin}/front-door/routes"))
            .json(&routes_for(&[("/svc", self.port), ("/svc2", second)]))
            .send()
            .await
            .expect("put routes");
        assert_eq!(routed.status().as_u16(), 200, "route setup must succeed");

        // Both imposters must actually dispatch before a test drives them, for `wait_dispatching`'s
        // reason: a 404 at the front door records nothing, so an unready route would make a merged
        // read look empty rather than wrong.
        for index in 0..self.nodes.len() {
            self.wait_dispatching(index).await;
            self.wait_dispatching_on("/svc2", index).await;
        }
        second
    }

    /// Poll the fleet read on node `index` until `want` holds of its recorded paths.
    ///
    /// Peer entries reach a node's replica cache on the anti-entropy cadence, so every assertion
    /// about *another* node's traffic has to be a poll rather than a single read. Waiting on the
    /// specific paths, not on a weaker proxy like "both ports appear", is what keeps it from
    /// passing before the traffic under test has arrived.
    async fn read_until(
        &self,
        index: usize,
        query: &str,
        want: impl Fn(&BTreeSet<String>) -> bool,
    ) -> serde_json::Value {
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        loop {
            let (status, json) = self.fleet_read(index, query).await;
            assert_eq!(status, 200, "the fleet read must answer: {json}");
            let paths = read_paths(&json);
            if want(&paths) {
                return json;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the fleet read never carried what this test waited for; saw {paths:?}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// `GET /admin/requests` against node `index` — the fleet journal read (issue #362).
    async fn fleet_read(&self, index: usize, query: &str) -> (u16, serde_json::Value) {
        let admin = self.admin(index);
        let response = reqwest::get(format!("http://{admin}/admin/requests{query}"))
            .await
            .expect("fleet journal read");
        let status = response.status().as_u16();
        let body = response.text().await.expect("a body");
        let json = serde_json::from_str(&body)
            .unwrap_or_else(|e| panic!("fleet read body is not JSON ({e}): {body}"));
        (status, json)
    }

    /// Take the last node out of the fleet so it can be shut down or gracefully
    /// left — both consume the `ComposedServer`. Only the last is removable, so
    /// the surviving nodes keep their indices.
    fn take_last(&mut self) -> ComposedServer {
        self.nodes.pop().expect("fleet has a node to take")
    }

    /// The founder's own view of the roster — what `merge_read` fans out over.
    fn founder_node(&self) -> std::sync::Arc<rift_cluster::RaftNode> {
        self.nodes[0].node().expect("clustered").clone()
    }

    fn front_door(&self, index: usize) -> std::net::SocketAddr {
        self.nodes[index]
            .front_door_addr()
            .expect("--front-door was given, must bind")
    }

    /// Poll node `index`'s front door until the replicated imposter and route
    /// have both landed there and it actually answers.
    ///
    /// The probe path is per-node because the successful poll is itself a
    /// recorded request: a shared `/svc/ready` would collapse all three into one
    /// entry in the merged *set* and hide which node contributed it. Only the
    /// 201 is recorded — earlier polls 404 at the front door, before any
    /// imposter sees them.
    async fn wait_dispatching(&self, index: usize) {
        self.wait_dispatching_on("/svc", index).await;
    }

    async fn wait_dispatching_on(&self, prefix: &str, index: usize) {
        let front_door = self.front_door(index);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(response) =
                reqwest::get(format!("http://{front_door}{prefix}/ready-{index}")).await
                && response.status().as_u16() == 201
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node {index}: the replicated imposter never began dispatching"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Send `count` requests through node `index`'s own front door, each with a
    /// distinguishable path so the merged read can be checked as a set.
    async fn drive(&self, index: usize, tag: &str, count: usize) {
        self.drive_on("/svc", index, tag, count).await;
    }

    async fn drive_on(&self, prefix: &str, index: usize, tag: &str, count: usize) {
        let front_door = self.front_door(index);
        for n in 0..count {
            let response = reqwest::get(format!("http://{front_door}{prefix}/{tag}-{n}"))
                .await
                .expect("request through the front door");
            assert_eq!(
                response.status().as_u16(),
                201,
                "node {index}: the imposter must answer, or nothing is recorded"
            );
        }
    }

    /// The fleet-wide merged read: no query string, so the front terminates it
    /// and merges every shard.
    async fn merged(&self, index: usize) -> Seen {
        let admin = self.admin(index);
        let port = self.port;
        let response = reqwest::get(format!("http://{admin}/imposters/{port}/savedRequests"))
            .await
            .expect("merged read");
        Seen::of(response).await
    }

    /// The local-only read: this node's **engine**, addressed directly, bypassing the front.
    ///
    /// It used to be `?since=0` through the front, which issue #223 left proxied. Issue #225
    /// terminates that route too — every requests-read through the front is now a fleet-wide
    /// merge — so the same URL would answer the merged set and this helper would silently stop
    /// meaning "local", taking the shard-partitioning assertions below with it.
    async fn local(&self, index: usize) -> Seen {
        let engine = self.nodes[index].engine_admin_addr();
        let port = self.port;
        let response = reqwest::get(format!(
            "http://{engine}/imposters/{port}/savedRequests?since=0"
        ))
        .await
        .expect("local read");
        Seen::of(response).await
    }
}

/// The recorded paths in a `savedRequests` body, as a set — the merge is
/// order-independent across shards, so order is not the contract.
/// Both envelopes are accepted because the two reads legitimately have
/// different owners: `?since=` is proxied to upstream's own handler, which
/// answers a bare array, while the merged read is the cluster front's and may
/// wrap it. What both must agree on is the *set of recorded paths*, which is
/// the only thing this file asserts about either.
fn paths(seen: &Seen) -> BTreeSet<String> {
    let body = seen.json();
    let entries = match &body {
        serde_json::Value::Array(entries) => entries.clone(),
        serde_json::Value::Object(_) => body
            .get("requests")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| {
                panic!("an object savedRequests body has a `requests` array: {seen}")
            })
            .clone(),
        _ => panic!("savedRequests body is neither an array nor an object: {seen}"),
    };
    entries
        .iter()
        .filter_map(|request| request.get("path").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect()
}

/// Acceptance criteria 1, 4 and 6 in one run, because they are one contract
/// seen from three sides: every node answers the *same* merged set (AC1), a
/// clear on one node is not resurrected by another's shard (AC4), and the
/// merged read is honest about being complete and — as of issue #225 — about
/// how to page it (AC6). That last clause used to read "about *not* being
/// paginable": #223 withheld the cursor because a scalar index names a
/// position in no shard in particular, and #225's vector token is the value
/// that replaced it.
///
/// The direct-to-engine local reads are load-bearing, not decoration: they are what stops the
/// whole test passing vacuously. If the front doors did not actually record on three different
/// nodes, the per-node shards would not partition the way this asserts, and a merge of three
/// copies of one shard would look identical to a merge of three distinct ones.
///
/// They no longer double as the positive control for `x-rift-next-index`. Issue #225 made the
/// merged read itself carry a cursor, so its presence is now asserted where it belongs — on the
/// merged read — rather than inferred from a proxied sibling.
///
/// Pins D-37 and D-38: three writer shards merge to one identical set on every node, and a
/// clear (a committed generation bump) removes them everywhere without resurrection.
#[tokio::test]
async fn every_node_answers_one_identical_merged_set_and_a_clear_does_not_resurrect() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let fleet = Fleet::start(&states).await;

    // Deliberately uneven, and every path distinct: an even split would not
    // catch a merge that returns one shard three times.
    fleet.drive(0, "a", 2).await;
    fleet.drive(1, "b", 3).await;
    fleet.drive(2, "c", 1).await;

    // Each front door recorded onto its own node and nowhere else. This is the
    // premise the rest of the test rests on, so it is asserted, not assumed —
    // `wait_dispatching` also drove one `/svc/ready` request per node, which is
    // why each local shard holds one more than `drive` sent.
    for (index, tag, sent) in [(0, "a", 2), (1, "b", 3), (2, "c", 1)] {
        let local = fleet.local(index).await;
        assert_eq!(local.status, 200, "node {index} local read: {local}");
        let local_paths = paths(&local);
        assert_eq!(
            local_paths.len(),
            sent + 1,
            "node {index} must hold only its own {sent} requests plus the readiness probe: {local_paths:?}"
        );
        assert!(
            local_paths
                .iter()
                .all(|path| path.contains(&format!("/{tag}-"))
                    || path == &format!("/svc/ready-{index}")),
            "node {index}'s shard must not contain another node's traffic: {local_paths:?}"
        );
        assert!(
            local.header(HEADER_NEXT_INDEX).is_some(),
            "the engine's own `?since=` read is paginable and must carry upstream's cursor: {local}"
        );
    }

    // AC1 + AC6: one identical set from every node, unstamped because every
    // peer answered, and no cursor because a merged read is not paginable.
    let expected: BTreeSet<String> = paths(&fleet.merged(0).await);
    assert_eq!(
        expected.len(),
        9,
        "the merged read must hold all six driven requests plus the three readiness probes: {expected:?}"
    );
    for index in 0..3 {
        let merged = fleet.merged(index).await;
        assert_eq!(merged.status, 200, "node {index} merged read: {merged}");
        assert_eq!(
            paths(&merged),
            expected,
            "node {index} answered a different merged set — a fleet read must not depend on which node served it"
        );
        assert!(
            merged.header(HEADER_PARTIAL).is_none(),
            "no peer was missing, so the merged read must not claim partial: {merged}"
        );
        // Issue #225 flips this. #223 withheld the cursor because a scalar index names a
        // position in no shard in particular; the vector token is the value that does mean the
        // same thing on every node, so a merged read now offers one — and it must, or a client
        // has no way to page a clustered journal at all.
        let token = merged
            .header(HEADER_NEXT_INDEX)
            .unwrap_or_else(|| panic!("a merged read must offer a vector cursor: {merged}"));
        assert!(
            JournalCursor::decode(token).is_ok(),
            "the issued cursor must be a token this fleet can read back, not an opaque-looking \
             string that only round-trips by accident: {token}"
        );
        assert!(
            merged.header(HEADER_TRUNCATED).is_none(),
            "nothing was evicted, so the merged read must not claim truncation: {merged}"
        );

        // AC1's other half: the listing's `numberOfRequests` is the fleet count,
        // and it must agree with the merged read rather than being a second,
        // independently-wrong number.
        let admin = fleet.admin(index);
        let listing = Seen::of(
            reqwest::get(format!("http://{admin}/imposters"))
                .await
                .expect("imposter listing"),
        )
        .await;
        let listing_body = listing.json();
        let count = listing_body
            .get("imposters")
            .and_then(serde_json::Value::as_array)
            .expect("listing has an `imposters` array")
            .iter()
            .find(|imposter| {
                imposter.get("port").and_then(serde_json::Value::as_u64)
                    == Some(u64::from(fleet.port))
            })
            .and_then(|imposter| imposter.get("numberOfRequests"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_else(|| panic!("node {index} listing names the imposter: {listing}"));
        assert_eq!(
            count,
            expected.len() as u64,
            "node {index}: fleet numberOfRequests must equal the merged read's size"
        );
    }

    // AC4: a clear issued on one node reaches the others' shards. Without the
    // fan-out this passes locally and then the next merged read pulls every
    // deleted entry straight back from a peer.
    let admin = fleet.admin(0);
    let port = fleet.port;
    let cleared = reqwest::Client::new()
        .delete(format!("http://{admin}/imposters/{port}/savedRequests"))
        .send()
        .await
        .expect("clear saved requests");
    assert_eq!(cleared.status().as_u16(), 200, "the clear must succeed");

    for index in 0..3 {
        let merged = fleet.merged(index).await;
        assert!(
            paths(&merged).is_empty(),
            "node {index} resurrected cleared requests from a peer's shard: {merged}"
        );

        // Blocker 1 (issue #224): a merged read reporting empty is not enough — the clear must
        // also zero `numberOfRequests`, upstream's own G-counter of every request seen
        // (recorded body or not), or the listing keeps reporting the pre-clear total forever
        // even though nothing is left to read back.
        let admin = fleet.admin(index);
        let listing = Seen::of(
            reqwest::get(format!("http://{admin}/imposters"))
                .await
                .expect("imposter listing"),
        )
        .await;
        let listing_body = listing.json();
        let count = listing_body
            .get("imposters")
            .and_then(serde_json::Value::as_array)
            .expect("listing has an `imposters` array")
            .iter()
            .find(|imposter| {
                imposter.get("port").and_then(serde_json::Value::as_u64)
                    == Some(u64::from(fleet.port))
            })
            .and_then(|imposter| imposter.get("numberOfRequests"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_else(|| panic!("node {index} listing names the imposter: {listing}"));
        assert_eq!(
            count, 0,
            "node {index}: a fleet-wide clear must zero numberOfRequests, not just empty the \
             merged read"
        );
    }
}

/// Drive the standard uneven traffic across a three-node fleet and return the
/// merged set every node agreed on, with the replica caches warmed as a side
/// effect of the read. Shared by both halves of AC2.
async fn driven_and_warmed(fleet: &Fleet) -> BTreeSet<String> {
    fleet.drive(0, "a", 2).await;
    fleet.drive(1, "b", 3).await;
    fleet.drive(2, "c", 1).await;

    // A merged read folds each peer's reply into this node's replica cache, so
    // this both establishes the pre-condition and warms node 0 deterministically
    // — waiting out the 5 s anti-entropy tick would be the same thing, slower
    // and racier.
    let merged = fleet.merged(0).await;
    assert!(
        merged.header(HEADER_PARTIAL).is_none(),
        "the fleet must be whole before a node is removed: {merged}"
    );
    let warmed = paths(&merged);
    assert_eq!(warmed.len(), 9, "every shard must be present: {warmed:?}");
    warmed
}

/// Acceptance criterion 2, first half: a node that dies **without leaving the
/// roster** must not erase its own traffic from the fleet's answer, and the
/// answer must say it is degraded.
///
/// This is also the positive control for `Rift-Cluster-Partial` that the
/// whole-fleet test above cannot provide: asserting a header is *absent* proves
/// nothing unless something else proves it can be present at all.
///
/// Pins D-37: an unreachable peer never stalls the merged read — its shard is served from
/// cache and the answer is stamped `Rift-Cluster-Partial`.
#[tokio::test]
async fn a_dead_but_still_rostered_peer_is_served_from_cache_and_stamped_partial() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let mut fleet = Fleet::start(&states).await;
    let whole = driven_and_warmed(&fleet).await;

    // Hard stop, not a graceful leave: the node stays a voter, so the merge
    // still expects it and cannot reach it.
    fleet.take_last().shutdown().await;

    let degraded = fleet.merged(0).await;
    assert_eq!(
        degraded.status, 200,
        "a degraded read still answers: {degraded}"
    );
    assert_eq!(
        degraded.header(HEADER_PARTIAL),
        Some("true"),
        "an unreachable voter must be declared, not silently dropped: {degraded}"
    );
    assert_eq!(
        paths(&degraded),
        whole,
        "`partial` must mean `possibly-stale`, never `omitted` — the dead node's \
         cached entries must survive its death"
    );
}

/// Acceptance criterion 2, second half: once the departed node is out of the
/// roster there is no missing peer, so the read must stop claiming to be
/// degraded.
///
/// Without this, a fleet that ever lost a node would stamp `partial` forever and
/// the header would decay into noise an operator learns to ignore.
#[tokio::test]
async fn a_peer_that_left_the_roster_no_longer_degrades_the_read() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let mut fleet = Fleet::start(&states).await;
    driven_and_warmed(&fleet).await;

    let founder_node = fleet.founder_node();
    fleet.take_last().graceful_leave().await;
    wait_voter_count(&founder_node, 2).await;

    let after = fleet.merged(0).await;
    assert_eq!(
        after.status, 200,
        "the surviving fleet still answers: {after}"
    );
    assert!(
        after.header(HEADER_PARTIAL).is_none(),
        "no voter is missing after a clean departure, so nothing is degraded: {after}"
    );
}

// ---------------------------------------------------------------------------
// The merged live tail (issue #348).
//
// Read over a raw socket rather than through an HTTP client: the stream's wire shape *is* part of
// its contract (`text/event-stream`, no buffering, chunked framing, `: ping`), and a client that
// normalises those away would let a regression in any of them pass. It also keeps the test tree
// free of a streaming-client dependency it does not otherwise have.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SseEvent {
    name: String,
    id: Option<String>,
    data: serde_json::Value,
}

/// An open SSE connection, de-chunked and parsed incrementally.
struct Tail {
    socket: tokio::net::TcpStream,
    /// De-chunked body bytes not yet consumed as complete frames.
    body: String,
    /// Raw chunked-transfer bytes not yet decoded.
    raw: Vec<u8>,
    headers: String,
    events: Vec<SseEvent>,
    pings: usize,
    /// The peer closed the connection; further reads would return 0 forever.
    closed: bool,
}

impl Tail {
    /// Open the tail on node `admin`, optionally resuming from `last_event_id`, and read as far as
    /// the response headers.
    async fn open(
        admin: std::net::SocketAddr,
        port: u16,
        query: &str,
        last_event_id: Option<&str>,
    ) -> Self {
        Self::open_at(
            admin,
            &format!("/imposters/{port}/savedRequests/stream{query}"),
            last_event_id,
        )
        .await
    }

    /// [`Self::open`] against an arbitrary path — the fleet tail (issue #362) is the same framing
    /// on a different route, so it gets the same harness rather than a second copy of it.
    async fn open_at(admin: std::net::SocketAddr, path: &str, last_event_id: Option<&str>) -> Self {
        use tokio::io::AsyncWriteExt as _;

        let mut socket = tokio::net::TcpStream::connect(admin)
            .await
            .expect("connect to the admin front");
        let resume = last_event_id
            .map(|id| format!("Last-Event-ID: {id}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: {admin}\r\n\
             Accept: text/event-stream\r\n\
             {resume}\r\n"
        );
        socket
            .write_all(request.as_bytes())
            .await
            .expect("write the stream request");

        let mut tail = Self {
            socket,
            body: String::new(),
            raw: Vec::new(),
            headers: String::new(),
            events: Vec::new(),
            pings: 0,
            closed: false,
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !tail.raw.windows(4).any(|w| w == b"\r\n\r\n") {
            assert!(
                std::time::Instant::now() < deadline,
                "the stream never sent response headers"
            );
            tail.read_more(Duration::from_secs(2)).await;
        }
        let split = tail
            .raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("checked above");
        tail.headers = String::from_utf8_lossy(&tail.raw[..split]).into_owned();
        tail.raw.drain(..split + 4);
        tail.decode();
        // Headers arriving does not mean the first frame has. Whether `hello` rides the same TCP
        // segment as the response head is a scheduling accident — it did under `--test-threads=1`
        // and did not under `3` — so wait for it here rather than letting every caller race it.
        // Bounded and non-fatal: an error response (a refused `Last-Event-ID`) legitimately
        // carries no frames at all, and those callers only read `headers`.
        tail.until(Duration::from_secs(10), |t| !t.events.is_empty())
            .await;
        tail
    }

    /// Read whatever is available within `budget`. A timeout is not a failure — an idle stream is
    /// the normal state — so this returns quietly either way.
    async fn read_more(&mut self, budget: Duration) {
        use tokio::io::AsyncReadExt as _;

        if self.closed {
            // Nothing more will arrive, and reading a closed socket returns 0 immediately — so
            // without this the deadline in `until` would be burned in a spin rather than a wait.
            tokio::time::sleep(budget).await;
            return;
        }
        let mut buf = [0_u8; 8192];
        // A timeout is the normal idle state of a live tail and means nothing. A read *error* is
        // the node having died or reset the connection, and must not be reported later as a bare
        // "nothing ever arrived" — that is the difference between a diagnosable failure and a
        // mystery timeout.
        match tokio::time::timeout(budget, self.socket.read(&mut buf)).await {
            Ok(Ok(0)) => self.closed = true,
            Ok(Ok(read)) => self.raw.extend_from_slice(&buf[..read]),
            Ok(Err(e)) => panic!("the stream connection failed mid-read: {e}"),
            Err(_) => {}
        }
    }

    /// Decode as many complete `Transfer-Encoding: chunked` chunks as `raw` holds, then parse as
    /// many complete SSE frames as the decoded body holds.
    fn decode(&mut self) {
        while let Some(eol) = self.raw.windows(2).position(|w| w == b"\r\n") {
            let Ok(header) = std::str::from_utf8(&self.raw[..eol]) else {
                break;
            };
            let Ok(size) = usize::from_str_radix(header.trim(), 16) else {
                break;
            };
            // header + CRLF + payload + CRLF
            if self.raw.len() < eol + 2 + size + 2 {
                break;
            }
            let payload = self.raw[eol + 2..eol + 2 + size].to_vec();
            self.raw.drain(..eol + 2 + size + 2);
            self.body.push_str(&String::from_utf8_lossy(&payload));
        }

        while let Some(end) = self.body.find("\n\n") {
            let frame: String = self.body.drain(..end + 2).collect();
            let frame = frame.trim_end();
            if frame.starts_with(':') {
                self.pings += 1;
                continue;
            }
            let mut name = String::new();
            let mut id = None;
            let mut data = String::new();
            for line in frame.lines() {
                if let Some(value) = line.strip_prefix("event: ") {
                    name = value.to_owned();
                } else if let Some(value) = line.strip_prefix("id: ") {
                    id = Some(value.to_owned());
                } else if let Some(value) = line.strip_prefix("data: ") {
                    data = value.to_owned();
                }
            }
            // Loud on purpose. Defaulting a malformed `data:` line to `Value::Null` would make
            // this harness the thing that hides the bug it exists to catch: every assertion below
            // reaches through `.get(...)`, which answers `None` on `Null`, so a corrupted frame
            // would quietly drop out of a *negative* assertion ("no peer entry carries `index`")
            // and turn an encoding or chunk-boundary defect into a pass.
            let parsed = serde_json::from_str(&data)
                .unwrap_or_else(|e| panic!("an SSE data line was not valid JSON ({e}): {data:?}"));
            self.events.push(SseEvent {
                name,
                id,
                data: parsed,
            });
        }
    }

    /// Pump the socket until `want` is satisfied or `budget` runs out. Returns whether it was.
    async fn until(&mut self, budget: Duration, want: impl Fn(&Self) -> bool) -> bool {
        let deadline = std::time::Instant::now() + budget;
        while std::time::Instant::now() < deadline {
            if want(self) {
                return true;
            }
            self.read_more(Duration::from_millis(200)).await;
            self.decode();
        }
        want(self)
    }

    fn hello(&self) -> &SseEvent {
        self.events
            .iter()
            .find(|e| e.name == "hello")
            .expect("every stream opens with hello")
    }

    /// The recorded paths delivered so far, in delivery order.
    fn delivered(&self) -> Vec<String> {
        self.events
            .iter()
            .filter(|e| e.name == "request")
            .filter_map(|e| {
                e.data
                    .get("request")?
                    .get("path")?
                    .as_str()
                    .map(str::to_owned)
            })
            .collect()
    }

    fn requests(&self) -> Vec<&SseEvent> {
        self.events.iter().filter(|e| e.name == "request").collect()
    }
}

/// AC1 + AC6: a tail on one node sees traffic recorded on the other two, within the latency the
/// stream itself declares — and the framing is upstream's.
#[tokio::test]
async fn the_merged_tail_delivers_what_every_node_records() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let fleet = Fleet::start(&states).await;

    let mut tail = Tail::open(fleet.admin(0), fleet.port, "", None).await;

    assert!(
        tail.headers.contains("200 OK"),
        "the tail must be terminated, not refused: {}",
        tail.headers
    );
    for expected in [
        "text/event-stream",
        "no-cache",
        // Upstream sets this so an intermediary cannot buffer the stream into uselessness; a
        // clustered tail behind a load balancer needs it at least as much.
        "no",
    ] {
        assert!(
            tail.headers
                .to_lowercase()
                .contains(&expected.to_lowercase()),
            "response headers must carry {expected:?}: {}",
            tail.headers
        );
    }

    let hello = tail.hello().clone();
    let declared = hello
        .data
        .get("clusterTailLatencyMs")
        .and_then(serde_json::Value::as_u64)
        .expect("hello declares the cluster tail latency");
    assert!(
        declared > 0,
        "a declared latency of zero would be a promise the anti-entropy cadence cannot keep"
    );
    assert_eq!(
        hello
            .data
            .get("types")
            .and_then(serde_json::Value::as_array),
        Some(&vec![serde_json::Value::from("requests")]),
        "the per-port alias is request-only, exactly as upstream's is"
    );
    assert!(
        hello
            .data
            .get("cursor")
            .and_then(serde_json::Value::as_str)
            .is_some(),
        "hello carries the start token, so a client can bootstrap a poll from it: {hello:?}"
    );

    // Traffic on the OTHER two nodes: these entries can only reach this tail through the merge.
    fleet.drive(1, "beta", 2).await;
    fleet.drive(2, "gamma", 2).await;

    // The declared latency is the contract, so it is also the budget — with a margin for the
    // fleet to actually route and record the requests, not for the tail to be late.
    let budget = Duration::from_millis(declared) * 3 + Duration::from_secs(5);
    let want: BTreeSet<String> = ["/svc/beta-0", "/svc/beta-1", "/svc/gamma-0", "/svc/gamma-1"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    let arrived = tail
        .until(budget, |t| {
            want.iter().all(|path| t.delivered().contains(path))
        })
        .await;
    assert!(
        arrived,
        "every peer-recorded request must arrive within the declared {declared} ms cadence; \
         delivered so far: {:?}",
        tail.delivered()
    );

    // Peer entries must not offer `index`: that seq is a position in another node's shard, and a
    // client handing it back as a legacy scalar `since` would have it read as ours.
    let peer_with_index = tail
        .requests()
        .into_iter()
        .filter(|e| {
            e.data
                .get("request")
                .and_then(|r| r.get("path"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|p| p.contains("beta") || p.contains("gamma"))
        })
        .filter(|e| e.data.get("index").is_some())
        .count();
    assert_eq!(
        peer_with_index,
        0,
        "no peer entry may carry `index`: {:?}",
        tail.requests()
    );

    for event in tail.requests() {
        assert!(
            event.id.is_some(),
            "every request event carries a resumption token: {event:?}"
        );
        assert!(
            event.data.get("flowId").is_some() && event.data.get("port").is_some(),
            "the data object keeps upstream's shape: {event:?}"
        );
    }
}

/// AC2 + AC3: reconnecting with the last `id:` loses nothing and repeats nothing.
#[tokio::test]
async fn a_reconnect_from_the_last_event_id_neither_loses_nor_repeats() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let fleet = Fleet::start(&states).await;

    let mut first = Tail::open(fleet.admin(0), fleet.port, "", None).await;
    fleet.drive(0, "first", 3).await;
    assert!(
        first
            .until(Duration::from_secs(20), |t| t.delivered().len() >= 3)
            .await,
        "the first connection must receive its batch: {:?}",
        first.delivered()
    );

    let seen_first = first.delivered();
    let resume_from = first
        .requests()
        .last()
        .and_then(|e| e.id.clone())
        .expect("the last delivered event carries the token to resume from");
    drop(first);

    // Recorded while nobody is listening: the gap a reconnect has to close.
    fleet.drive(1, "gap", 2).await;

    let mut second = Tail::open(fleet.admin(0), fleet.port, "", Some(&resume_from)).await;
    assert!(
        second
            .until(Duration::from_secs(30), |t| {
                let seen = t.delivered();
                ["/svc/gap-0", "/svc/gap-1"]
                    .iter()
                    .all(|p| seen.contains(&(*p).to_owned()))
            })
            .await,
        "a reconnect must deliver what was recorded while it was away: {:?}",
        second.delivered()
    );

    let seen_second = second.delivered();
    for already in &seen_first {
        assert!(
            !seen_second.contains(already),
            "{already} was delivered before the disconnect and must not repeat: {seen_second:?}"
        );
    }
    let unique: BTreeSet<&String> = seen_second.iter().collect();
    assert_eq!(
        unique.len(),
        seen_second.len(),
        "no entry may be delivered twice within one connection: {seen_second:?}"
    );
}

/// A `Last-Event-ID` the front cannot read is a typed 400 — never a defaulted position, which
/// would silently replay everything or silently skip it.
#[tokio::test]
async fn an_unusable_last_event_id_is_refused_rather_than_defaulted() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let fleet = Fleet::start(&states).await;

    let tail = Tail::open(
        fleet.admin(0),
        fleet.port,
        "",
        Some("not-a-cursor-token-at-all"),
    )
    .await;
    assert!(
        tail.headers.contains("400"),
        "an unreadable resumption token must be refused: {}",
        tail.headers
    );
}

/// A `?match=`-scoped tail keeps proxying (issue #223 review, B1), so it must NOT carry the
/// merged stream's `hello`. Proving it by the discriminator rather than by the absence of data:
/// upstream's own hello has no `clusterTailLatencyMs`, and only the merged tail adds one.
#[tokio::test]
async fn a_match_scoped_tail_still_proxies_to_the_local_engine() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let fleet = Fleet::start(&states).await;

    // Upstream's own clause syntax (`<field>=<value>`), deliberately: proving this path reaches
    // upstream's parser is half the point of the assertion below.
    let mut tail = Tail::open(fleet.admin(0), fleet.port, "?match=method=GET", None).await;
    assert!(
        tail.headers.contains("200 OK"),
        "a well-formed scoped tail must be accepted, not refused — a 400 here would make the \
         hello assertion below pass for the wrong reason: {}",
        tail.headers
    );
    assert!(
        tail.until(Duration::from_secs(15), |t| !t.events.is_empty())
            .await,
        "the proxied stream still opens and greets: {}",
        tail.headers
    );
    assert!(
        tail.hello().data.get("clusterTailLatencyMs").is_none(),
        "a predicate-scoped tail must reach the engine's own stream, not the merged one: {:?}",
        tail.hello()
    );
    assert!(
        tail.hello().data.get("seq").is_some(),
        "and the engine's hello is the one with a scalar bus seq: {:?}",
        tail.hello()
    );
}

/// AC4: a peer going unreachable mid-stream is *announced*, not silently absorbed.
///
/// This is the one acceptance criterion whose machinery is entirely new — the streamed `partial`
/// reads a verdict `anti_entropy_tick` now records, rather than the per-read fan-out the cursor
/// read uses — so an end-to-end assertion is what stops it from being plausible-but-untrue.
#[tokio::test]
async fn a_peer_dying_mid_stream_is_announced_not_silently_absorbed() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let mut fleet = Fleet::start(&states).await;

    let mut tail = Tail::open(fleet.admin(0), fleet.port, "", None).await;
    let declared = tail
        .hello()
        .data
        .get("clusterTailLatencyMs")
        .and_then(serde_json::Value::as_u64)
        .expect("hello declares the cadence");

    // The vacuity guard: a healthy fleet must NOT be announcing partial, or the assertion below
    // would pass on a stream that simply says `partial` all the time.
    fleet.drive(0, "healthy", 1).await;
    tail.until(Duration::from_secs(5), |t| !t.delivered().is_empty())
        .await;
    assert!(
        !tail.events.iter().any(|e| e.name == "partial"),
        "a healthy fleet must not be claiming a degraded merge: {:?}",
        tail.events
    );

    // Hard stop, not a graceful leave: the node stays a voter, so the tick still expects it and
    // cannot reach it — exactly the condition the stamp exists to report.
    fleet.take_last().shutdown().await;

    // The verdict is recorded by the anti-entropy tick, so it cannot appear faster than one
    // cadence; allow several, plus room for the transport to give up on the dead peer.
    let budget = Duration::from_millis(declared) * 4 + Duration::from_secs(20);
    let announced = tail
        .until(budget, |t| {
            t.events
                .iter()
                .any(|e| e.name == "partial" && e.data.get("partial") == Some(&true.into()))
        })
        .await;
    assert!(
        announced,
        "an unreachable voter must be declared on the stream, not silently dropped: {:?}",
        tail.events
    );
}

// ---- issue #362: the fleet journal, against a real three-node fleet ------------------------
//
// Everything below drives two imposters on three nodes. That combination is the point: the pure
// merge is tested exhaustively in `journal_net`'s own module, but `JournalNet::covered_slices` and
// `newest_by_port` fold the *replica cache* — keyed `(node, port)` — and no unit test can reach
// them, because a `JournalNet` built in isolation has no peers to cache. A bug in either (a
// transposed key, a stale rather than newest timestamp) would pass every unit test and lose a
// peer's entries in production.

/// The recorded paths in a fleet read body.
fn read_paths(body: &serde_json::Value) -> BTreeSet<String> {
    body["requests"]
        .as_array()
        .expect("requests is an array")
        .iter()
        .filter_map(|row| row.get("request")?.get("path")?.as_str().map(str::to_owned))
        .collect()
}

/// The fleet read answers for **every** imposter the tenant owns, not just the first, and states
/// its coverage.
///
/// Traffic is driven at both imposters through *different* nodes' front doors, so a correct answer
/// requires the walk to cross both the port dimension and the node dimension at once — which is
/// exactly the pair of loops the unit tests cannot exercise.
///
/// Pins D-32: a live fleet read carries the `coverage` block end to end — `covered` names both
/// imposters, `omitted` is empty and `capped` is false below the cap.
#[tokio::test]
async fn the_fleet_read_merges_every_imposter_the_tenant_owns() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let fleet = Fleet::start(&states).await;
    let second = fleet.add_imposter().await;

    fleet.drive_on("/svc", 0, "first", 2).await;
    fleet.drive_on("/svc2", 1, "second", 2).await;

    // Peer entries arrive on the anti-entropy cadence, so poll until the specific driven requests
    // are present. Waiting merely for "both ports appear" would fire immediately — `wait_dispatching`
    // already recorded a readiness probe on each — and then race the traffic this test is about.
    let body = fleet
        .read_until(0, "", |paths| {
            paths.contains("/svc/first-0") && paths.contains("/svc2/second-0")
        })
        .await;

    let paths = read_paths(&body);
    assert!(
        paths.contains("/svc/first-0") && paths.contains("/svc2/second-0"),
        "one read must carry both imposters' traffic: {paths:?}"
    );

    let covered: BTreeSet<u64> = body["coverage"]["covered"]
        .as_array()
        .expect("coverage.covered is an array")
        .iter()
        .filter_map(serde_json::Value::as_u64)
        .collect();
    assert!(
        covered.contains(&u64::from(fleet.port)) && covered.contains(&u64::from(second)),
        "coverage must name both imposters: {}",
        body["coverage"]
    );
    assert_eq!(
        body["coverage"]["capped"], false,
        "two imposters is far below the cap, so nothing may claim to be omitted"
    );
    assert!(
        body["coverage"]["omitted"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "nothing was left out: {}",
        body["coverage"]
    );
    assert!(
        body["cursor"]
            .as_str()
            .is_some_and(|token| !token.is_empty()),
        "the read hands back a resumable position"
    );
}

/// Resuming from the cursor a fleet read handed back delivers what arrived since, and nothing that
/// was already delivered — the cross-port half of issue #225's guarantee.
#[tokio::test]
async fn a_fleet_cursor_resumes_without_replaying_what_it_already_answered() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let fleet = Fleet::start(&states).await;
    fleet.add_imposter().await;

    fleet.drive_on("/svc", 0, "before", 2).await;
    fleet.drive_on("/svc2", 1, "before", 2).await;

    // Take the cursor only once both imposters' driven traffic is actually in the answer, so the
    // position genuinely spans both ports.
    let first = fleet
        .read_until(0, "", |paths| {
            paths.contains("/svc/before-0") && paths.contains("/svc2/before-0")
        })
        .await;
    let cursor = first["cursor"].as_str().expect("a cursor").to_owned();
    let delivered = read_paths(&first);

    // No fresh traffic yet, and no port joined: the cursor named them all, so a resumed read has
    // nothing to re-serve.
    let (status, json) = fleet.fleet_read(0, &format!("?since={cursor}")).await;
    assert_eq!(status, 200, "resuming must be accepted: {json}");
    assert!(
        json["joined"].as_array().is_some_and(Vec::is_empty),
        "no port joined: the cursor already named them all: {json}"
    );

    fleet.drive_on("/svc2", 2, "after", 1).await;

    let resumed = fleet
        .read_until(0, &format!("?since={cursor}"), |paths| {
            paths.contains("/svc2/after-0")
        })
        .await;

    // The property, stated without racing the cadence: a resumed read may legitimately carry an
    // entry that was still in flight when the cursor was taken, but it must never re-deliver one
    // the cursor already accounted for.
    let resumed_paths = read_paths(&resumed);
    let again: Vec<&String> = resumed_paths.intersection(&delivered).collect();
    assert!(
        again.is_empty(),
        "resuming must not replay what the first page already answered: {again:?}"
    );
}

/// The fleet tail delivers both imposters' traffic on one stream, and declares its coverage up
/// front — the streaming half of issue #362.
#[tokio::test]
async fn the_fleet_tail_delivers_every_imposter_on_one_stream() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let fleet = Fleet::start(&states).await;
    let second = fleet.add_imposter().await;

    let mut tail = Tail::open_at(fleet.admin(0), "/admin/requests/stream", None).await;
    assert!(
        tail.headers.contains("text/event-stream"),
        "the fleet tail must answer SSE: {}",
        tail.headers
    );

    let hello = tail.hello();
    assert_eq!(
        hello.data.get("scope").and_then(serde_json::Value::as_str),
        Some("fleet"),
        "hello names the scope, so a client cannot confuse this with a per-imposter tail: {:?}",
        hello.data
    );
    assert!(
        hello.data.get("clusterTailLatencyMs").is_some(),
        "the declared peer latency rides hello, as it does per imposter: {:?}",
        hello.data
    );
    let covered: BTreeSet<u64> = hello
        .data
        .get("coverage")
        .and_then(|coverage| coverage.get("covered"))
        .and_then(serde_json::Value::as_array)
        .expect("hello carries coverage")
        .iter()
        .filter_map(serde_json::Value::as_u64)
        .collect();
    assert!(
        covered.contains(&u64::from(fleet.port)) && covered.contains(&u64::from(second)),
        "the stream states which imposters it speaks for: {covered:?}"
    );

    // A connect never replays, so drive traffic only after the stream is open.
    fleet.drive_on("/svc", 1, "tailed", 1).await;
    fleet.drive_on("/svc2", 2, "tailed", 1).await;

    let arrived = tail
        .until(Duration::from_secs(45), |tail| {
            let delivered = tail.delivered();
            delivered.iter().any(|path| path == "/svc/tailed-0")
                && delivered.iter().any(|path| path == "/svc2/tailed-0")
        })
        .await;
    assert!(
        arrived,
        "both imposters' requests must arrive on one fleet stream: {:?}",
        tail.delivered()
    );

    // Every delivered event names its imposter — a merged row without one is unreadable.
    for event in tail.requests() {
        assert!(
            event
                .data
                .get("port")
                .and_then(serde_json::Value::as_u64)
                .is_some(),
            "a fleet row must name the imposter it came from: {:?}",
            event.data
        );
        assert!(
            event.id.is_some(),
            "every event carries the position to resume from: {:?}",
            event.data
        );
    }
}

/// A `Last-Event-ID` from the fleet tail resumes it: what was recorded while the connection was
/// down arrives, and what was already delivered does not arrive twice.
#[tokio::test]
async fn a_fleet_tail_reconnect_neither_loses_nor_repeats() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let fleet = Fleet::start(&states).await;
    fleet.add_imposter().await;

    let mut tail = Tail::open_at(fleet.admin(0), "/admin/requests/stream", None).await;
    fleet.drive_on("/svc", 0, "seen", 1).await;
    assert!(
        tail.until(Duration::from_secs(45), |tail| {
            tail.delivered().iter().any(|path| path == "/svc/seen-0")
        })
        .await,
        "the first request must arrive before the disconnect: {:?}",
        tail.delivered()
    );
    let resume = tail
        .requests()
        .last()
        .and_then(|event| event.id.clone())
        .expect("a delivered event carries an id");
    let already = tail.delivered();
    drop(tail);

    // Recorded while nothing is listening — the gap a reconnect has to close.
    fleet.drive_on("/svc2", 1, "missed", 1).await;

    let mut resumed = Tail::open_at(
        fleet.admin(0),
        "/admin/requests/stream",
        Some(resume.as_str()),
    )
    .await;
    assert!(
        resumed
            .until(Duration::from_secs(45), |tail| {
                tail.delivered().iter().any(|path| path == "/svc2/missed-0")
            })
            .await,
        "a reconnect must deliver what was recorded while it was away: {:?}",
        resumed.delivered()
    );
    assert!(
        !resumed
            .delivered()
            .iter()
            .any(|path| already.contains(path)),
        "and must not redeliver what the previous connection already had: {:?} after {:?}",
        resumed.delivered(),
        already
    );
}

/// An unusable `Last-Event-ID` is refused, never defaulted — including a *per-imposter* token,
/// which is a good cursor at the wrong endpoint and must say so rather than be read as a fleet
/// position.
#[tokio::test]
async fn an_unusable_fleet_last_event_id_is_refused_rather_than_defaulted() {
    let states = [
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
        TempDir::new().expect("tempdir"),
    ];
    let fleet = Fleet::start(&states).await;
    let admin = fleet.admin(0);

    // The per-imposter tail's own token: structurally valid, wrong scope.
    let per_imposter = Tail::open(admin, fleet.port, "", None).await;
    let borrowed = per_imposter
        .hello()
        .data
        .get("cursor")
        .and_then(serde_json::Value::as_str)
        .expect("the per-imposter hello carries a cursor")
        .to_owned();
    drop(per_imposter);

    for (label, token) in [
        ("garbage", "not-a-cursor!!"),
        ("legacy scalar", "42"),
        ("per-imposter token", borrowed.as_str()),
    ] {
        let refused = Tail::open_at(admin, "/admin/requests/stream", Some(token)).await;
        assert!(
            refused.headers.starts_with("HTTP/1.1 400"),
            "{label} must be refused, not defaulted to a position: {}",
            refused.headers
        );
    }
}
