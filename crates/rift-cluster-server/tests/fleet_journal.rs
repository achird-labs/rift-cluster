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
use rift_cluster::decorate::HEADER_PARTIAL;
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
        let front_door = self.front_door(index);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(response) =
                reqwest::get(format!("http://{front_door}/svc/ready-{index}")).await
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
        let front_door = self.front_door(index);
        for n in 0..count {
            let response = reqwest::get(format!("http://{front_door}/svc/{tag}-{n}"))
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

    /// The local-only read: `?since=` keeps upstream's own clause parser, so the
    /// front proxies it straight to this node's engine instead of merging.
    async fn local(&self, index: usize) -> Seen {
        let admin = self.admin(index);
        let port = self.port;
        let response = reqwest::get(format!(
            "http://{admin}/imposters/{port}/savedRequests?since=0"
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
/// merged read is honest about being complete and about not being paginable
/// (AC6).
///
/// The `?since=0` local reads are load-bearing, not decoration. They are the
/// positive control for `x-rift-next-index` — proving its absence on the merged
/// read is a deliberate difference rather than a header that never appears —
/// and they are what stops the whole test passing vacuously: if the front doors
/// did not actually record on three different nodes, the per-node shards would
/// not partition the way this asserts.
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
            local.header("x-rift-next-index").is_some(),
            "the local `?since=` read is paginable and must carry the cursor: {local}"
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
        assert!(
            merged.header("x-rift-next-index").is_none(),
            "a merged read spans shards and has no single cursor, so it must not offer one: {merged}"
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
