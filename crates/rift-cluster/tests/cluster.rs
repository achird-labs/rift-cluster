//! In-process 3-node integration + failover harness for the Raft control plane
//! (issue #11, Phase-1 subset).
//!
//! This drives real [`RaftNode`]s over real localhost TCP through the public
//! crate API — so it doubles as a check that the API is enough to stand up,
//! join, replicate, kill, and restart a cluster. It is deliberately *in-process*:
//! it covers the Phase-1 exit tests that need only nodes and a network, and NOT
//! the container-based chaos suite (Envoy + toxiproxy partitions, admin-API /
//! Prometheus assertions), which depends on the `rift-cluster-server` binary (#10) and
//! the HTTP config/metrics surface (#9) and lands when those exist. See
//! `tests/README.md` for the split and how to add a scenario.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rift_cluster::stores::ClusterJournal;
use rift_cluster::{
    ADMIT_CURRENCY_WAIT, Authority, ControlRequest, DEFAULT_TENANT, NodeConfig, NodeId, RaftNode,
    Router,
};
use tempfile::TempDir;

const SECRET: &str = "harness-cluster-secret";
const CONVERGE_DEADLINE: Duration = Duration::from_secs(10);
const LEADER_DEADLINE: Duration = Duration::from_secs(10);

/// Serializes the whole harness. Each scenario stands up its own cluster on
/// scarce localhost ports; running them concurrently makes independent tests
/// compete for ports and CPU, which surfaces as spurious bind failures rather
/// than real defects. One cluster at a time keeps the suite deterministic.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Reserve `n` *distinct* currently-free localhost ports. Every listener is held
/// open until all ports are chosen, so the OS cannot hand the same port to two of
/// them; they are then released together and the nodes rebind (with
/// `SO_REUSEADDR`) on their fixed port — which is what lets a node keep its
/// address across a restart so peers' committed membership stays valid.
fn reserve_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a free port"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().expect("read reserved port").port())
        .collect()
}

/// One node's stable identity across restarts: its id, its fixed address, and its
/// data directory (kept alive so redb survives a kill/restart).
struct Member {
    id: NodeId,
    addr: SocketAddr,
    dir: TempDir,
    /// `Arc` because the node-bound subsystems (the flow and pull-on-miss bridges in the
    /// composed server) take a `&Arc<RaftNode>` so they can hold a `Weak` back to it without
    /// keeping it alive.
    node: Option<Arc<RaftNode>>,
}

async fn spawn(id: NodeId, addr: SocketAddr, dir: &Path) -> Arc<RaftNode> {
    // Most of these tests drive `build_snapshot`/`install_snapshot` directly, so they need no help
    // provoking one. The exception is #428's catch-up test, which needs a real snapshot to cross
    // the wire and so passes the knob explicitly.
    spawn_with_snapshot_policy(id, addr, dir, None).await
}

async fn spawn_with_snapshot_policy(
    id: NodeId,
    addr: SocketAddr,
    dir: &Path,
    snapshot_log_entries: Option<u64>,
) -> Arc<RaftNode> {
    let config = NodeConfig {
        node_id: id,
        bind: addr,
        advertise: Some(Authority::from(addr)),
        data_dir: dir.to_path_buf(),
        secret: Some(SECRET.to_owned()),
        routes: Router::new(),
        engine: None,
        snapshot_log_entries,
    };
    // No retry-on-lock-contention: `RaftNode::shutdown` now waits for the Raft
    // core to release its storage handles before returning (#41), so a restart on
    // a directory whose previous node was shut down cannot race the redb lock.
    Arc::new(
        RaftNode::start(config)
            .await
            .unwrap_or_else(|e| panic!("start node {id}: {e}")),
    )
}

/// A running in-process cluster.
struct TestCluster {
    members: Vec<Member>,
    /// Retained so `restart` brings a node back with the same snapshot policy.
    snapshot_log_entries: Option<u64>,
}

impl TestCluster {
    /// Start `n` nodes, bootstrap node 1, and seed-join the rest through it, so
    /// the returned cluster is one converged group of `n` voters.
    async fn start(n: usize) -> Self {
        Self::start_full(n, None).await
    }

    /// [`Self::start`] with every node snapshotting every `entries` log entries and purging to
    /// the tip, so a member that falls behind must be caught up by `install_snapshot`.
    async fn start_with_snapshots(n: usize, entries: u64) -> Self {
        Self::start_full(n, Some(entries)).await
    }

    async fn start_full(n: usize, snapshot_log_entries: Option<u64>) -> Self {
        assert!(n >= 1, "a cluster needs at least one node");
        let mut members: Vec<Member> = reserve_ports(n)
            .into_iter()
            .enumerate()
            .map(|(i, port)| Member {
                id: (i + 1) as NodeId,
                addr: format!("127.0.0.1:{port}").parse().expect("addr"),
                dir: TempDir::new().expect("tempdir"),
                node: None,
            })
            .collect();

        let n1 = spawn_with_snapshot_policy(
            members[0].id,
            members[0].addr,
            members[0].dir.path(),
            snapshot_log_entries,
        )
        .await;
        n1.cluster_init().await.expect("bootstrap node 1");
        members[0].node = Some(n1);

        let seed = Authority::from(members[0].addr);
        for member in members.iter_mut().skip(1) {
            let node = spawn_with_snapshot_policy(
                member.id,
                member.addr,
                member.dir.path(),
                snapshot_log_entries,
            )
            .await;
            node.join_via(&seed)
                .await
                .unwrap_or_else(|e| panic!("node {} join: {e}", member.id));
            member.node = Some(node);
        }

        let cluster = Self {
            members,
            snapshot_log_entries,
        };
        let all: BTreeSet<NodeId> = cluster.members.iter().map(|m| m.id).collect();
        assert!(
            cluster.wait_voters(&all, CONVERGE_DEADLINE).await,
            "cluster did not converge on {} voters at startup",
            all.len()
        );
        cluster
    }

    fn member(&self, id: NodeId) -> &Member {
        self.members
            .iter()
            .find(|m| m.id == id)
            .unwrap_or_else(|| panic!("no member {id}"))
    }

    fn member_mut(&mut self, id: NodeId) -> &mut Member {
        self.members
            .iter_mut()
            .find(|m| m.id == id)
            .unwrap_or_else(|| panic!("no member {id}"))
    }

    fn live(&self) -> impl Iterator<Item = &RaftNode> {
        self.members.iter().filter_map(|m| m.node.as_deref())
    }

    /// The current leader as a shared handle, for the subsystems that bind to
    /// one.
    fn leader_handle(&self) -> Option<&Arc<RaftNode>> {
        self.members
            .iter()
            .filter_map(|m| m.node.as_ref())
            .find(|n| n.status().is_leader)
    }

    /// The node currently reporting itself leader, if any.
    fn leader(&self) -> Option<&RaftNode> {
        self.live().find(|n| n.status().is_leader)
    }

    /// Poll, bounded, for some live node to become leader; return its id.
    async fn wait_for_leader(&self, deadline: Duration) -> Option<NodeId> {
        let start = Instant::now();
        loop {
            if let Some(leader) = self.leader() {
                // current_leader agreeing rules out a just-stepped-down straggler.
                if leader.status().current_leader == Some(leader.id()) {
                    return Some(leader.id());
                }
            }
            if start.elapsed() > deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll, bounded, until every live node's applied config for `port` carries
    /// `want` as its name tag. Returns false on timeout (never a synthetic pass).
    async fn wait_converged(&self, port: u16, want: &str, deadline: Duration) -> bool {
        let name_of = |body: String| {
            serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v.get("name")?.as_str().map(str::to_owned))
        };
        let start = Instant::now();
        loop {
            let mut live = self.live().peekable();
            let converged = live.peek().is_some()
                && live.all(|n| {
                    n.get_imposter(DEFAULT_TENANT, port)
                        .expect("read config")
                        .and_then(name_of)
                        .as_deref()
                        == Some(want)
                });
            if converged {
                return true;
            }
            if start.elapsed() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll, bounded, until every live node's voter set equals `want`.
    async fn wait_voters(&self, want: &BTreeSet<NodeId>, deadline: Duration) -> bool {
        let start = Instant::now();
        loop {
            let mut live = self.live().peekable();
            let converged = live.peek().is_some()
                && live.all(|n| &n.status().voters.into_iter().collect::<BTreeSet<_>>() == want);
            if converged {
                return true;
            }
            if start.elapsed() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Write a minimal config for `port`, name-tagged `want`, on the current
    /// leader, returning its revision.
    async fn write_on_leader(&self, port: u16, want: &str) -> u64 {
        let leader = self.leader().expect("a leader to accept the write");
        let config = serde_json::from_value(serde_json::json!({
            "port": port,
            "protocol": "http",
            "host": "127.0.0.1",
            "name": want,
        }))
        .expect("test config parses");
        let response = leader
            .put_imposter(config)
            .await
            .expect("leader commits the write");
        assert_eq!(
            response.outcome,
            rift_cluster::ControlOutcome::Applied,
            "a valid write must apply"
        );
        response.revision
    }

    /// Stop node `id` and drop it, leaving its data directory intact for a later
    /// restart. Its fixed address is retained, so a restart reclaims it.
    async fn kill(&mut self, id: NodeId) {
        if let Some(node) = self.member_mut(id).node.take() {
            node.shutdown().await.ok();
        }
    }

    /// Restart node `id` on its original address and data directory. It rejoins
    /// automatically: its persisted log already carries the cluster membership,
    /// and its peers still hold its (unchanged) address.
    async fn restart(&mut self, id: NodeId) {
        let (mid, addr) = {
            let m = self.member(id);
            (m.id, m.addr)
        };
        // Ensure the previous instance is gone before rebinding the port.
        self.kill(id).await;
        let dir = self.member(id).dir.path().to_path_buf();
        let node = spawn_with_snapshot_policy(mid, addr, &dir, self.snapshot_log_entries).await;
        self.member_mut(id).node = Some(node);
    }

    async fn shutdown_all(&mut self) {
        for member in &mut self.members {
            if let Some(node) = member.node.take() {
                node.shutdown().await.ok();
            }
        }
    }

    /// Have member `id` gracefully leave the Raft membership, then shut down
    /// its own node process (a node that left has no further cluster role).
    /// Its data directory is left in place and can be reused: rejoining works
    /// on either a fresh directory (`test_rejoin_after_leave`, the redeployed-pod
    /// shape) or the retained one (`test_rejoin_after_leave_with_retained_state_dir`,
    /// the rolling-restart shape).
    async fn leave_gracefully(
        &mut self,
        id: NodeId,
        timeout: Duration,
    ) -> Result<rift_cluster::LeaveOutcome, rift_cluster::NodeError> {
        let result = {
            let node = self.member(id).node.as_ref().expect("member is running");
            node.leave(timeout).await
        };
        if let Some(node) = self.member_mut(id).node.take() {
            node.shutdown().await.ok();
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Phase-1 exit tests (in-process subset). Names match RFC §10 where applicable.
// ---------------------------------------------------------------------------

/// `test_config_sync_converges`: a write on the leader is served by every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_config_sync_converges() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster.write_on_leader(8080, "config-v1").await;
    assert!(
        cluster
            .wait_converged(8080, "config-v1", CONVERGE_DEADLINE)
            .await,
        "the write did not converge on all three nodes"
    );
    cluster.shutdown_all().await;
}

/// `test_node_rejoin`: killing a follower leaves the surviving quorum writable,
/// and the follower catches up on every missed write when it rejoins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_node_rejoin() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;

    // Pick a follower to kill (not the leader), so the survivors keep quorum.
    let leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let victim = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader)
        .expect("a follower to kill");

    cluster.kill(victim).await;

    // Two of three voters remain — writes must still commit and converge on them.
    cluster.write_on_leader(8080, "written-while-down").await;
    assert!(
        cluster
            .wait_converged(8080, "written-while-down", CONVERGE_DEADLINE)
            .await,
        "surviving quorum did not converge with one node down"
    );

    // The victim rejoins and catches up to the write it missed.
    cluster.restart(victim).await;
    assert!(
        cluster
            .wait_converged(8080, "written-while-down", CONVERGE_DEADLINE)
            .await,
        "rejoined node did not catch up to the missed write"
    );

    // And it is a full participant again, not merely caught up once: a *new*
    // write after the rejoin must also replicate to it.
    cluster.write_on_leader(8081, "written-after-rejoin").await;
    assert!(
        cluster
            .wait_converged(8081, "written-after-rejoin", CONVERGE_DEADLINE)
            .await,
        "rejoined node did not receive a write made after it came back"
    );
    cluster.shutdown_all().await;
}

/// `test_cold_start`: a full-cluster restart restores committed config, and an
/// all-empty fleet (never initialized) refuses to elect a leader / serve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cold_start() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster.write_on_leader(8080, "survives-cold-start").await;
    assert!(
        cluster
            .wait_converged(8080, "survives-cold-start", CONVERGE_DEADLINE)
            .await
    );

    // Full-cluster restart: every node comes back on its address and data dir.
    let ids: Vec<NodeId> = cluster.members.iter().map(|m| m.id).collect();
    cluster.shutdown_all().await;
    for id in &ids {
        cluster.restart(*id).await;
    }

    assert!(
        cluster.wait_for_leader(LEADER_DEADLINE).await.is_some(),
        "cluster did not re-elect a leader after a cold restart"
    );
    assert!(
        cluster
            .wait_converged(8080, "survives-cold-start", CONVERGE_DEADLINE)
            .await,
        "config was not restored after a full-cluster restart"
    );
    cluster.shutdown_all().await;
}

/// The all-empty half of cold-start: nodes that were never initialized must not
/// elect a leader — an empty fleet stays not-Ready rather than serving nothing
/// as if it were authoritative.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_uninitialized_fleet_never_ready() {
    let _serial = TEST_LOCK.lock().await;
    let ports = reserve_ports(2);
    let (da, db) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let a = spawn(
        1,
        format!("127.0.0.1:{}", ports[0]).parse().unwrap(),
        da.path(),
    )
    .await;
    let b = spawn(
        2,
        format!("127.0.0.1:{}", ports[1]).parse().unwrap(),
        db.path(),
    )
    .await;

    // Give elections every chance to (wrongly) happen, then assert none did.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !a.status().is_leader,
        "uninitialized node A must not be leader"
    );
    assert!(
        !b.status().is_leader,
        "uninitialized node B must not be leader"
    );
    assert_eq!(a.status().current_leader, None);
    assert_eq!(b.status().current_leader, None);

    a.shutdown().await.ok();
    b.shutdown().await.ok();
}

/// `test_leader_failover`: killing the leader elects a new one from the surviving
/// quorum, and the new leader can still commit writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_leader_failover() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let old_leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    cluster.kill(old_leader).await;

    let new_leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a new leader after the old one dies");
    assert_ne!(new_leader, old_leader, "a *new* leader must be elected");

    // The new leader still has a quorum and can commit.
    cluster.write_on_leader(9090, "after-failover").await;
    assert!(
        cluster
            .wait_converged(9090, "after-failover", CONVERGE_DEADLINE)
            .await,
        "the new leader could not replicate a post-failover write"
    );

    // No split brain: at this converged point at most one live node claims to be
    // leader (a regression electing two would show 2 here).
    let leaders = cluster.live().filter(|n| n.status().is_leader).count();
    assert!(
        leaders <= 1,
        "split brain: {leaders} live nodes claim leadership"
    );
    cluster.shutdown_all().await;
}

/// Issue #9: the write barrier degrades to a *warning* on an unreachable node —
/// the write itself stays committed. A healthy fleet reports nobody unapplied;
/// with a member killed, exactly that member is named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn barrier_names_exactly_the_unapplied_node() {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let leader_id = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    let revision = cluster.write_on_leader(8080, "barrier-healthy").await;
    let unapplied = cluster
        .leader()
        .expect("leader")
        .await_applied(revision, Duration::from_secs(5))
        .await;
    assert!(
        unapplied.is_empty(),
        "a healthy fleet leaves nobody unapplied: {unapplied:?}"
    );

    // Kill a follower (never the leader) and write again: the commit still
    // succeeds on the majority, and the barrier names the dead node — only it.
    let victim = [1, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id)
        .expect("a follower exists");
    cluster.kill(victim).await;

    let revision = cluster.write_on_leader(8081, "barrier-degraded").await;
    let unapplied = cluster
        .leader()
        .expect("leader")
        .await_applied(revision, Duration::from_millis(500))
        .await;
    assert_eq!(
        unapplied,
        vec![victim],
        "the barrier must name the dead node and nothing else"
    );
    cluster.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Issue #6: graceful membership departure.
// ---------------------------------------------------------------------------

/// `test_graceful_leave`: a follower leaves; membership shrinks to 2 voters,
/// the survivors still have a leader, and a write submitted after the leave
/// still commits and converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_graceful_leave() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let follower = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader)
        .expect("a follower to leave");

    cluster
        .leave_gracefully(follower, Duration::from_secs(5))
        .await
        .expect("a follower must be able to leave gracefully");

    let remaining: BTreeSet<NodeId> = cluster
        .members
        .iter()
        .map(|m| m.id)
        .filter(|&id| id != follower)
        .collect();
    assert!(
        cluster.wait_voters(&remaining, CONVERGE_DEADLINE).await,
        "membership did not shrink to the surviving two voters"
    );
    assert!(
        cluster.wait_for_leader(LEADER_DEADLINE).await.is_some(),
        "the survivors must still have a leader after the leave"
    );

    cluster.write_on_leader(8080, "after-leave").await;
    assert!(
        cluster
            .wait_converged(8080, "after-leave", CONVERGE_DEADLINE)
            .await,
        "a write submitted after the leave must still commit and converge"
    );
    cluster.shutdown_all().await;
}

/// Issue #69: the second departure from a three-node fleet is refused.
///
/// A whole-fleet teardown SIGTERMs every node, and without a floor each one
/// removes itself in turn: 3 → 2 → 1. The fleet ends with its entire control
/// plane on a single authoritative volume, and a cold start that has to wait
/// for *that* node before anything else can join. The floor stops the walk at
/// two; the refused node exits crash-equivalent and resumes on its next start.
///
/// Pins D-25: the leader refuses a graceful leave that would drop the voter set
/// below two — the first departure from three lands, the second is held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_leave_holds_the_voter_floor() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let followers: Vec<NodeId> = cluster
        .members
        .iter()
        .map(|m| m.id)
        .filter(|&id| id != leader)
        .collect();

    // The first departure is permitted: it lands at two voters, which is the
    // floor, not below it.
    assert_eq!(
        cluster
            .leave_gracefully(followers[0], Duration::from_secs(5))
            .await
            .expect("the first leave must be permitted"),
        rift_cluster::LeaveOutcome::Departed,
        "leaving a three-voter fleet is above the floor and must be allowed"
    );
    let remaining: BTreeSet<NodeId> = cluster
        .members
        .iter()
        .map(|m| m.id)
        .filter(|&id| id != followers[0])
        .collect();
    assert!(
        cluster.wait_voters(&remaining, CONVERGE_DEADLINE).await,
        "membership did not shrink to two after the first leave"
    );

    // The second is refused: it would leave a single voter. Asked directly
    // rather than through `leave_gracefully`, because that also shuts the node
    // down — and a refused node leaving the *process* while still holding a
    // vote is exactly what costs the survivors their quorum. Here the point is
    // the outcome, so the node stays up.
    //
    // It travels over the leave RPC (this node is not the leader), so it also
    // pins the reply's wire shape: a refusal is reported on the same
    // `LeaveAccepted` body an older client would simply ignore.
    let refused = {
        let node = cluster.member(followers[1]).node.as_ref().expect("running");
        node.leave(Duration::from_secs(5))
            .await
            .expect("a refused leave is an outcome, not an error")
    };
    assert_eq!(
        refused,
        rift_cluster::LeaveOutcome::Retained,
        "a leave that would drop the fleet to one voter must be refused"
    );
    assert!(
        cluster.wait_voters(&remaining, CONVERGE_DEADLINE).await,
        "the refused leave must leave the membership untouched"
    );

    // The two survivors are still a working cluster, not a wedged one.
    assert!(
        cluster.wait_for_leader(LEADER_DEADLINE).await.is_some(),
        "the survivors must still have a leader"
    );
    cluster.write_on_leader(8081, "after-floor").await;
    assert!(
        cluster
            .wait_converged(8081, "after-floor", CONVERGE_DEADLINE)
            .await,
        "a write must still commit after the floor refused a departure"
    );
    cluster.shutdown_all().await;
}

/// Issue #69: two nodes SIGTERMed at once must not both slip through.
///
/// The floor is read and acted on under the leader's membership gate — the
/// same serialization the auto-promote ceiling uses (#55). Without it both
/// departures read a pre-removal voter count, both pass the check, and the
/// fleet walks to one anyway.
///
/// Pins D-25: the floor holds under concurrent departures because it is
/// enforced by the leader under one gate — no orchestrator signal tells the
/// fleet that a teardown is under way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_leaves_never_walk_below_the_floor() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let followers: Vec<NodeId> = cluster
        .members
        .iter()
        .map(|m| m.id)
        .filter(|&id| id != leader)
        .collect();

    let (first, second) = {
        let a = cluster.member(followers[0]).node.as_ref().expect("running");
        let b = cluster.member(followers[1]).node.as_ref().expect("running");
        tokio::join!(
            a.leave(Duration::from_secs(5)),
            b.leave(Duration::from_secs(5))
        )
    };

    let outcomes = [
        first.expect("a concurrent leave must not error"),
        second.expect("a concurrent leave must not error"),
    ];
    let departed = outcomes
        .iter()
        .filter(|o| **o == rift_cluster::LeaveOutcome::Departed)
        .count();
    assert_eq!(
        departed, 1,
        "exactly one of two concurrent departures may land, got {outcomes:?}"
    );

    // Whichever one lost, the fleet is at two voters and still writable.
    let voters = cluster
        .leader()
        .expect("a surviving leader")
        .status()
        .voters
        .len();
    assert_eq!(voters, 2, "the floor must hold under concurrent departures");

    cluster.shutdown_all().await;
}

/// Issue #69: a two-node fleet cannot shed a voter at all.
///
/// Both of its nodes are load-bearing, so every graceful leave is refused and
/// every node resumes on restart. This is the behaviour change the floor
/// introduces for N=2, and it is the intended one: dropping to a single voter
/// is exactly the redundancy collapse the floor exists to prevent.
///
/// The **leader** is the one asked to leave, deliberately. A leader evicts
/// itself through the local path rather than the leave RPC, and that is the
/// mapping whose inversion would be worst: a leader reporting `Departed` after
/// being refused would write a departure marker for a node that is still a
/// member, and then refuse its own next start — the shape of the defect found
/// in #72. Every other floor test refuses a follower, so without this one the
/// local branch is never exercised.
///
/// Pins D-25 and D-26: a two-voter fleet sheds nobody, and the refused leader
/// reports `Retained` rather than a departure the marker would record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_node_leave_is_refused_by_the_floor() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(2).await;
    let leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let other = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader)
        .expect("the other node");

    assert_eq!(
        cluster
            .leave_gracefully(leader, Duration::from_secs(5))
            .await
            .expect("a refused leave is an outcome, not an error"),
        rift_cluster::LeaveOutcome::Retained,
        "a two-node fleet has no voter to spare, and a leader refusing itself must say so"
    );

    let both: BTreeSet<NodeId> = cluster.members.iter().map(|m| m.id).collect();
    let survivor = cluster.member(other).node.as_ref().expect("running");
    assert_eq!(
        survivor
            .status()
            .voters
            .iter()
            .copied()
            .collect::<BTreeSet<_>>(),
        both,
        "the survivor's membership must still name both nodes"
    );

    cluster.shutdown_all().await;
}

/// `test_graceful_leave_of_the_leader`: the leader leaves; a new leader
/// appears within 3s and membership shrinks to 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_graceful_leave_of_the_leader() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let old_leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    cluster
        .leave_gracefully(old_leader, Duration::from_secs(5))
        .await
        .expect("the leader must be able to leave gracefully");

    let remaining: BTreeSet<NodeId> = cluster
        .members
        .iter()
        .map(|m| m.id)
        .filter(|&id| id != old_leader)
        .collect();
    assert!(
        cluster.wait_voters(&remaining, CONVERGE_DEADLINE).await,
        "membership did not shrink to the surviving two voters"
    );

    let new_leader = cluster.wait_for_leader(Duration::from_secs(3)).await;
    assert!(
        matches!(new_leader, Some(id) if id != old_leader),
        "a new leader must appear within 3s of the old leader leaving, got {new_leader:?}"
    );

    cluster.shutdown_all().await;
}

/// `test_leave_is_bounded_without_a_leader`: a node with no reachable leader
/// must return from `leave` within a couple of seconds, not hang — a timeout
/// error is the expected (and only possible) outcome here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_leave_is_bounded_without_a_leader() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let followers: Vec<NodeId> = cluster
        .members
        .iter()
        .map(|m| m.id)
        .filter(|&id| id != leader)
        .collect();
    let (isolated, other_follower) = (followers[0], followers[1]);

    // Kill the leader and the other follower: `isolated` is a plain follower
    // that can never see a leader again — no quorum is possible with one of
    // three left standing.
    cluster.kill(leader).await;
    cluster.kill(other_follower).await;

    let started = Instant::now();
    let result = {
        let node = cluster.member(isolated).node.as_ref().expect("running");
        node.leave(Duration::from_millis(500)).await
    };
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "leave() must return promptly even with no reachable leader, took {elapsed:?}"
    );
    assert!(
        matches!(result, Err(rift_cluster::NodeError::Timeout { .. })),
        "leave without any reachable leader must time out, got {result:?}"
    );

    cluster.kill(isolated).await;
}

/// `test_rejoin_after_leave`: a node that left can `join_via` a seed again and
/// catch up on writes it missed while it was away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rejoin_after_leave() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let departed = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader)
        .expect("a follower to leave");

    cluster
        .leave_gracefully(departed, Duration::from_secs(5))
        .await
        .expect("graceful leave");

    let remaining: BTreeSet<NodeId> = cluster
        .members
        .iter()
        .map(|m| m.id)
        .filter(|&id| id != departed)
        .collect();
    assert!(
        cluster.wait_voters(&remaining, CONVERGE_DEADLINE).await,
        "membership did not shrink after the leave"
    );

    // Write while the departed node is away, so rejoining has to catch up.
    cluster
        .write_on_leader(9090, "written-while-departed")
        .await;
    assert!(
        cluster
            .wait_converged(9090, "written-while-departed", CONVERGE_DEADLINE)
            .await,
        "the surviving quorum did not converge while the departed node was away"
    );

    // Rejoin fresh: a genuinely new data directory reusing the vacated id,
    // exactly like a redeployed pod would.
    let new_dir = TempDir::new().expect("tempdir");
    let addr = cluster.member(departed).addr;
    let seed = cluster.leader().expect("a leader to seed off").advertise();
    let rejoined = spawn(departed, addr, new_dir.path()).await;
    rejoined.join_via(seed).await.expect("rejoin via seed");
    cluster.member_mut(departed).node = Some(rejoined);
    // Keep the fresh directory alive for the rest of the test (and
    // shutdown_all afterwards); the old one is no longer used.
    cluster.member_mut(departed).dir = new_dir;

    let full: BTreeSet<NodeId> = cluster.members.iter().map(|m| m.id).collect();
    assert!(
        cluster.wait_voters(&full, CONVERGE_DEADLINE).await,
        "rejoined node did not converge back to full voter membership"
    );
    assert!(
        cluster
            .wait_converged(9090, "written-while-departed", CONVERGE_DEADLINE)
            .await,
        "rejoined node did not catch up on the write made while it was away"
    );

    cluster.shutdown_all().await;
}

/// Issue #72: the same rejoin, but on the node's **retained** directory.
///
/// This is the rolling-restart shape — a Docker volume or k8s PVC outlives the
/// container, so the returning node has its whole Raft log, including a
/// membership it is no longer part of. `test_rejoin_after_leave` deliberately
/// uses a fresh directory and so never exercises this path; nothing did, in
/// process, until this test.
///
/// The retained log is a safe *prefix* of the cluster's, so re-admission as a
/// learner reconciles it by ordinary append/conflict handling rather than
/// needing the directory wiped. The final write is the half that matters most:
/// a returning node carrying a stale term must not disturb the quorum it
/// rejoins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rejoin_after_leave_with_retained_state_dir() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let departed = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader)
        .expect("a follower to leave");

    cluster
        .leave_gracefully(departed, Duration::from_secs(5))
        .await
        .expect("graceful leave");

    let remaining: BTreeSet<NodeId> = cluster
        .members
        .iter()
        .map(|m| m.id)
        .filter(|&id| id != departed)
        .collect();
    assert!(
        cluster.wait_voters(&remaining, CONVERGE_DEADLINE).await,
        "membership did not shrink after the leave"
    );

    // Written while it is away, so rejoining has to catch up on it.
    cluster
        .write_on_leader(9091, "written-while-departed")
        .await;
    assert!(
        cluster
            .wait_converged(9091, "written-while-departed", CONVERGE_DEADLINE)
            .await,
        "the surviving quorum did not converge while the departed node was away"
    );

    // The retained directory, not a fresh one: this is the whole point.
    let dir = cluster.member(departed).dir.path().to_path_buf();
    let addr = cluster.member(departed).addr;
    let seed = cluster.leader().expect("a leader to seed off").advertise();
    let rejoined = spawn(departed, addr, &dir).await;
    rejoined
        .join_via(seed)
        .await
        .expect("a node that left must rejoin on its retained directory");
    cluster.member_mut(departed).node = Some(rejoined);

    let full: BTreeSet<NodeId> = cluster.members.iter().map(|m| m.id).collect();
    assert!(
        cluster.wait_voters(&full, CONVERGE_DEADLINE).await,
        "rejoined node did not converge back to full voter membership"
    );
    assert!(
        cluster
            .wait_converged(9091, "written-while-departed", CONVERGE_DEADLINE)
            .await,
        "rejoined node did not catch up on the write made while it was away"
    );

    // The survivors were not destabilized by the returning node's stale state:
    // the cluster still commits after the rejoin.
    cluster.write_on_leader(9092, "after-rejoin").await;
    assert!(
        cluster
            .wait_converged(9092, "after-rejoin", CONVERGE_DEADLINE)
            .await,
        "the cluster stopped committing after the retained-state rejoin"
    );

    cluster.shutdown_all().await;
}

/// A `PutImposter` whose serialized entry is at least `target_bytes` — the bulk payload the
/// large-entry tests (#411, #430, #431, #428) need in order to exercise openraft and redb at
/// size.
///
/// Bulk used to come from a dataset CSV or a spec document; #549 removed both ops, and the
/// ceiling those tests pin is a property of **a log entry**, not of what happened to be in it.
/// An imposter config is the largest thing that still rides the log, so it is the honest carrier
/// now — and it goes through the same `PutImposter` admission every other config does, which the
/// dataset path did not.
///
/// `tag` makes two calls at the same size produce *different* bytes, so a test that writes N of
/// these writes N distinct entries rather than N copies of one.
fn bulky_request(port: u16, tag: &str, target_bytes: usize) -> ControlRequest {
    let mut body = String::with_capacity(target_bytes);
    body.push_str(tag);
    while body.len() < target_bytes {
        body.push('x');
    }
    body.truncate(target_bytes);
    let config: rift_cluster_base::seams::ImposterConfig =
        serde_json::from_value(serde_json::json!({
            "port": port,
            "protocol": "http",
            "host": "127.0.0.1",
            "stubs": [{
                "id": format!("bulk-{tag}"),
                "responses": [{ "is": { "statusCode": 200, "body": body } }],
            }],
        }))
        .expect("the bulky config parses");
    ControlRequest {
        op_id: uuid::Uuid::new_v4(),
        principal: None,
        issued_at_secs: 0,
        expected_revision: None,
        op: rift_cluster::ControlOp::PutImposter {
            tenant: rift_cluster::TenantId::default(),
            config: Box::new(config),
        },
    }
}

fn submit_request(op_id: u128, issued_at_secs: u64, op: rift_cluster::ControlOp) -> ControlRequest {
    ControlRequest {
        op_id: uuid::Uuid::from_u128(op_id),
        principal: Some("default/alice".to_owned()),
        issued_at_secs,
        expected_revision: None,
        op,
    }
}

/// AC2: a quota refusal is a *committed* decision — the same `Failed` outcome at
/// the same revision on all three nodes. That is what §11 open question 1 turns
/// on: the refusal is discoverable through `op_status` precisely because it is
/// in the log, not because the submitter saw an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quota_refusal_is_the_same_committed_decision_on_every_node() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    assert!(cluster.wait_for_leader(LEADER_DEADLINE).await.is_some());

    let leader = cluster.leader().expect("a leader");
    leader
        .write(submit_request(
            1,
            1_700_000_000,
            rift_cluster::ControlOp::TenantPut {
                tenant: rift_cluster::TenantId::new("acme"),
                display_name: "Acme".to_owned(),
                quotas: rift_cluster::control::Quotas {
                    max_imposters: 1,
                    ..rift_cluster::control::Quotas::default()
                },
                journal_retention_secs: 0,
            },
        ))
        .await
        .expect("tenant put commits");

    let imposter = |port: u16| {
        serde_json::from_value(serde_json::json!({
            "port": port,
            "protocol": "http",
            "host": "127.0.0.1",
        }))
        .expect("test config parses")
    };

    let first = leader
        .write(submit_request(
            2,
            1_700_000_001,
            rift_cluster::ControlOp::PutImposter {
                tenant: rift_cluster::TenantId::new("acme"),
                config: Box::new(imposter(18091)),
            },
        ))
        .await
        .expect("first imposter commits");
    assert_eq!(first.outcome, rift_cluster::ControlOutcome::Applied);

    let refused = leader
        .write(submit_request(
            3,
            1_700_000_002,
            rift_cluster::ControlOp::PutImposter {
                tenant: rift_cluster::TenantId::new("acme"),
                config: Box::new(imposter(18092)),
            },
        ))
        .await
        .expect("the refusal is a committed write, not a transport error");
    let rift_cluster::ControlOutcome::Failed { .. } = &refused.outcome else {
        panic!("the second imposter is over the ceiling: {refused:?}");
    };

    assert!(
        refused.revision > first.revision,
        "a refusal is a committed entry with a revision of its own: {refused:?}"
    );

    // The refusal really is the same decision everywhere: every node applies the
    // revision that refused it, and on every node the first imposter landed while
    // the second did not. A node that applied the entry differently — or skipped
    // it — would hold a different table from its peers.
    for node in cluster.live() {
        assert!(
            node.await_local_applied(refused.revision, CONVERGE_DEADLINE)
                .await,
            "node {} never applied the refusing revision {}",
            node.id(),
            refused.revision
        );
        assert!(
            node.imposter_config("acme", 18091).expect("read").is_some(),
            "node {} lost the imposter that was within quota",
            node.id()
        );
        assert!(
            node.imposter_config("acme", 18092).expect("read").is_none(),
            "node {} landed the imposter the quota refused",
            node.id()
        );
    }

    cluster.shutdown_all().await;
}

/// The fleet's name survives a process death (issue #373).
///
/// **Restart, not snapshot install** — the same correction the chaos README records for C18 and
/// `snapshot_round_trips_the_fleet_name`
/// in `raft/store.rs` drives `build_snapshot`/`install_snapshot` directly against a *fresh* state
/// machine; it never closes and reopens the same redb file. A restart is the far more common
/// event of the two — the default `LogEntries(5000)` policy means most nodes come back by
/// replaying their own persisted state, not by installing a snapshot — so a name that lived only
/// in a snapshot payload would be lost in exactly the ordinary case.
///
/// The failure it would produce is quiet, which is why it is worth a scenario: the node comes
/// back and every console reading the fleet through it says `Unnamed`, with nothing logged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fleet_name_survives_a_node_restart() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    let leader = cluster.leader().expect("a leader to accept the name");
    leader
        .submit(ControlRequest {
            op_id: uuid::Uuid::new_v4(),
            principal: None,
            issued_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
            expected_revision: None,
            op: rift_cluster::ControlOp::FleetNamePut {
                tenant: rift_cluster::TenantId::new(rift_cluster::FLEET_SCOPE),
                name: "rift-prod-eu".to_owned(),
            },
        })
        .await
        .expect("the fleet name commits");

    let victim = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|id| Some(*id) != cluster.leader().map(RaftNode::id))
        .expect("a follower");
    cluster.restart(victim).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader after the restart");

    let restarted = cluster
        .member(victim)
        .node
        .as_ref()
        .expect("the restarted node");
    let start = Instant::now();
    while restarted.fleet_name().expect("read") != Some("rift-prod-eu".to_owned())
        && start.elapsed() < CONVERGE_DEADLINE
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        restarted.fleet_name().expect("read fleet name"),
        Some("rift-prod-eu".to_owned()),
        "a node that came back without the fleet's name would answer `Unnamed` to every console \
         reading the fleet through it, with nothing logged to say why"
    );

    cluster.shutdown_all().await;
}

/// `voter_count_sizes_the_journal_shard`: the journal's shard cap divides fleet capacity
/// by the applied membership, so this pins the two halves that only exist together — that
/// `RaftNode::voter_count` reports the committed voter set, and that binding a journal to
/// a node actually re-sizes its shards.
///
/// The unit tests inject a fixed voter count; nothing there proves the real accessor
/// agrees with real membership, which is the half that would silently drift.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn voter_count_sizes_the_journal_shard() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;

    for node in cluster.live() {
        assert_eq!(
            node.voter_count(),
            3,
            "node {} does not see the full voter set",
            node.id()
        );
    }

    let leader = cluster
        .leader_handle()
        .expect("a converged cluster has a leader");
    let journal = ClusterJournal::new(leader.id());
    assert_eq!(
        journal.shard_cap(),
        10_000,
        "an unbound journal sizes as a single writer, preserving single-node behaviour"
    );

    journal.bind(leader);
    assert_eq!(
        journal.shard_cap(),
        3_333,
        "binding re-sizes the shard to its share of fleet capacity"
    );

    cluster.shutdown_all().await;
}

// -- large log entries and snapshot catch-up (#411, #428, #430, #431) ----------------------------

/// #430: a leader that loses its term while an 8 MiB entry is in flight keeps its replication
/// cores alive for a moment; the new leader's conflict truncates the old leader's uncommitted
/// index, and a stale core then reads a now-empty range. openraft 0.9.24 `unwrap()`ed that and
/// took the worker down; 0.9.25 treats the empty read as a heartbeat.
///
/// The condition that produces it is CPU starvation (the 2-vCPU CI runner; here, one spinning
/// thread per core but one), which makes followers time out and churn leadership. Whether the
/// *write* survives that churn is #431's silent-window fix and is not asserted here. What this
/// pins is narrower and must hold regardless: **no replication worker panics**, ever, on the
/// empty read. A panic hook counts them because a panic inside a tokio task does not fail the
/// test on its own — it is exactly the kind of failure that hides in a log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leadership_change_under_load_never_panics_a_replication_worker() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let _serial = TEST_LOCK.lock().await;

    let replication_panics = Arc::new(AtomicUsize::new(0));
    let counter = replication_panics.clone();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let from_replication = info
            .location()
            .is_some_and(|l| l.file().contains("openraft") && l.file().contains("replication"));
        if from_replication {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    }));

    // Starve the runtime the way the CI runner does. `available_parallelism` minus one spinner
    // leaves the tokio workers fighting for what is left, which is the condition under which
    // followers time out mid-transfer.
    let stop = Arc::new(AtomicBool::new(false));
    let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
    let spinners: Vec<_> = (0..cores.saturating_sub(1).max(1))
        .map(|_| {
            let stop = stop.clone();
            std::thread::spawn(move || while !stop.load(Ordering::Relaxed) {})
        })
        .collect();

    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    // The outcome of the write is deliberately not asserted: under this load it may park or
    // fail with "not the leader" until #431 lands. The panic count is the claim.
    let _ = tokio::time::timeout(
        Duration::from_secs(30),
        cluster.leader().expect("leader").submit(bulky_request(
            19501,
            "big",
            8 * 1024 * 1024 - 1_100,
        )),
    )
    .await;
    // Let any stale core that is mid-read finish its read and (on 0.9.24) panic.
    tokio::time::sleep(Duration::from_secs(2)).await;
    cluster.shutdown_all().await;

    stop.store(true, Ordering::Relaxed);
    for s in spinners {
        let _ = s.join();
    }
    std::panic::set_hook(previous);

    assert_eq!(
        replication_panics.load(Ordering::SeqCst),
        0,
        "a replication worker panicked on an empty log read; openraft must treat it as a heartbeat"
    );
}

/// #431's acceptance test: a voter that restarts *behind a purged log* is caught up by snapshot,
/// and its term never runs ahead of the leader's while that happens.
///
/// Nodes 1+2 keep committing while the victim is down; every node snapshots every 2 entries and
/// purges to the tip, so by the time the victim returns the entries it needs no longer exist
/// anywhere. Before the fix this livelocked — the victim's term climbed 3 → 66 over 58 s while
/// the leader sat at term 1 — for reasons that only showed under instrumentation: the leader's
/// own health tracker refused to heartbeat the restarted peer for its cooldown, the voter timed
/// out and campaigned, and a leader never adopts a term from a candidate it rejects. The term
/// assertion is the one that pins the mechanism; convergence alone would pass by accident.
///
/// Pins D-22: the leader's liveness probes reach a restarted voter *through* the health gate,
/// so its term never runs ahead while it is caught up by snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_voter_behind_a_purged_log_catches_up_by_snapshot() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start_with_snapshots(3, 2).await;
    let leader_id = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let victim: NodeId = if leader_id == 3 { 2 } else { 3 };

    let r = cluster
        .leader()
        .expect("leader")
        .submit(bulky_request(19510, "d0", 512 * 1024))
        .await
        .expect("d0");
    assert_eq!(r.outcome, rift_cluster::ControlOutcome::Applied);
    cluster.kill(victim).await;

    for i in 1..=8 {
        let r = cluster
            .leader()
            .expect("leader")
            .submit(bulky_request(19510 + i, &format!("d{i}"), 512 * 1024))
            .await
            .expect("commit while the victim is down");
        assert_eq!(r.outcome, rift_cluster::ControlOutcome::Applied);
    }

    cluster.restart(victim).await;
    let start = std::time::Instant::now();
    let mut converged = None;
    while start.elapsed() < Duration::from_secs(60) {
        let leader = cluster.leader();
        let v = cluster.member(victim).node.as_ref().expect("live");
        if let Some(l) = leader {
            assert!(
                v.raft_term() <= l.raft_term(),
                "the restarted voter's term ({}) ran ahead of the leader's ({}): it campaigned",
                v.raft_term(),
                l.raft_term()
            );
        }
        let target = leader.and_then(|l| l.status().last_applied);
        let mine = v.status().last_applied;
        if target.is_some() && mine >= target {
            converged = Some(start.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let st = cluster.member(victim).node.as_ref().expect("live").status();
    cluster.shutdown_all().await;
    assert!(
        converged.is_some(),
        "the restarted voter must be caught up by snapshot; stuck at {:?}",
        st.last_applied
    );
}

/// #431's restart grace: a member of a multi-voter cluster that comes back and hears no leader
/// holds off campaigning for `RESTART_ELECTION_GRACE`, then campaigns normally — so a restart
/// never bumps the term of a healthy fleet, and a genuinely leaderless one is still recovered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarting_member_holds_elections_until_it_hears_a_leader_or_the_grace_expires() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    // No leader can exist: two of three voters are gone. Node 3 is still a
    // plain running voter while they go down and campaigns freely — that is
    // normal and not what this test is about. The grace governs the process
    // that comes back, so the baseline is its term immediately after restart.
    cluster.kill(1).await;
    cluster.kill(2).await;
    cluster.restart(3).await;
    let term_before = cluster.member(3).node.as_ref().expect("live").raft_term();

    tokio::time::sleep(Duration::from_millis(1500)).await;
    let during_grace = cluster.member(3).node.as_ref().expect("live").raft_term();
    assert_eq!(
        during_grace, term_before,
        "a restarting member must not campaign inside the grace even with no leader audible"
    );

    tokio::time::sleep(
        rift_cluster::RaftNode::RESTART_ELECTION_GRACE + Duration::from_millis(1500),
    )
    .await;
    let after_grace = cluster.member(3).node.as_ref().expect("live").raft_term();
    cluster.shutdown_all().await;
    assert!(
        after_grace > term_before,
        "once the grace expires a leaderless member must campaign (term {after_grace} vs {term_before})"
    );
}

/// Issue #428: a fleet that has snapshotted and purged catches a fresh node up **over the wire**.
///
/// Every byte of committed config rides the state machine, so a fleet holding a few large
/// imposters has a snapshot measured in MiB. openraft bounds each snapshot *chunk* by
/// `install_snapshot_timeout` and
/// abandons the whole transfer — back to offset 0 — when one misses, so at its defaults (3 MiB
/// chunks, 200 ms) the transfer could never finish: chunks ride the JSON cluster port at ~4× their
/// raw size, which is ~900 ms for a default chunk on loopback alone. Measured before the fix on
/// this exact shape: the joiner never converged in 60 s.
///
/// This test once carried a second claim — a write submitted mid-install commits once the joiner
/// catches up — because admission promoted the joiner to voter at once, the fleet entered the
/// joint configuration `{1},{1,2}`, and a snapshot that never landed took the leader's ability to
/// commit anything with it. Two-phase admission (#433) removed that failure mode by construction:
/// a joiner is promoted in-call only if it is current within `ADMIT_CURRENCY_WAIT`, which a
/// multi-MiB install never is, so it is a **learner for the whole install** and its ack is required
/// for nothing. The probe became a write that could not fail, so it is gone; what replaced it is
/// the two observations that make the new shape checkable — no voterhood during the install,
/// voterhood after it.
///
/// **The fixture size is load-bearing, and is measured here rather than asserted in prose (#492).**
/// The install must outlast the 500 ms window, and it had stopped doing so: #436 (binary,
/// file-backed snapshots) and #440 (a KiB manifest plus an out-of-band fetch, both since removed
/// by D-72) each cut the install time while this fixture stayed at 8 × 512 KiB, until it landed at
/// 577–592 ms locally and 502–1219 ms
/// on CI. Against a 500 ms window that is a coin flip — ~55% failure in CI, 0% locally across four
/// attempts, which is why it read as an infrastructure flake for a day. `MIN_INSTALL_MARGIN` now
/// checks the margin on every run, so the next time something makes installs faster this fails
/// with the remedy named instead of flaking.
///
/// `snapshot_log_entries: Some(2)` (with the `max_in_snapshot_log_to_keep: 0` it implies) is what
/// makes the catch-up a *snapshot* rather than log replication: by the time the joiner arrives the
/// log it would need has been purged, so `install_snapshot` is openraft's only route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joiner_is_caught_up_by_a_multi_mebibyte_snapshot() {
    /// 32 MiB of state, sized so the install outlasts `ADMIT_CURRENCY_WAIT` by a wide margin on
    /// the *fastest* environment measured, not the average one. See `MIN_INSTALL_MARGIN` below
    /// and #492.
    const ENTRIES: usize = 8;
    const PER_ENTRY_BYTES: usize = 4 * 1024 * 1024;
    const CONVERGE_BY: Duration = Duration::from_secs(60);

    /// The install must outlast the admission window by this factor for the learner assertion
    /// below to be measuring two-phase admission rather than a coin flip. Checked, not assumed:
    /// #492 was exactly this margin silently going to zero.
    ///
    /// **Enlarging the fixture has limited headroom.** The write is already ~0.35 s/MiB of test
    /// time, and a single log entry has its own ceiling (#411, and the measurement recorded in
    /// D-23 — superseded by D-72, but the numbers behind it are why this bound exists).
    /// If another change makes installs 3x faster again, the answer is a different mechanism —
    /// more entries rather than bigger ones — not a bigger `PER_ENTRY_BYTES`.
    const MIN_INSTALL_MARGIN: u32 = 3;

    let _serial = TEST_LOCK.lock().await;
    let ports = reserve_ports(2);
    let addr1: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().expect("addr");
    let addr2: SocketAddr = format!("127.0.0.1:{}", ports[1]).parse().expect("addr");
    let dir1 = TempDir::new().expect("tempdir");
    let dir2 = TempDir::new().expect("tempdir");

    let leader = spawn_with_snapshot_policy(1, addr1, dir1.path(), Some(2)).await;
    leader.cluster_init().await.expect("bootstrap node 1");
    let deadline = Instant::now() + LEADER_DEADLINE;
    while !leader.status().is_leader {
        assert!(Instant::now() < deadline, "node 1 never became leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut last_revision = 0;
    for i in 0..ENTRIES {
        let response = leader
            .submit(bulky_request(
                19520 + i as u16,
                &format!("d{i}"),
                PER_ENTRY_BYTES,
            ))
            .await
            .unwrap_or_else(|e| panic!("entry d{i} commits: {e}"));
        last_revision = response.revision;
    }
    assert!(
        leader
            .await_applied(last_revision, CONVERGE_DEADLINE)
            .await
            .is_empty()
    );

    // Wait for the snapshot policy to run before the joiner arrives, so the log it would
    // otherwise be caught up *from* has been purged and `install_snapshot` is openraft's only
    // route. Polled, not slept: this fixture is 32 MiB, and a fixed settle sized for a smaller
    // one would let the joiner arrive before the purge — at which point it catches up by log
    // replication, the install this test measures never happens, and the failure looks like a
    // margin problem instead of a route problem (#492).
    let deadline = Instant::now() + CONVERGE_BY;
    loop {
        let applied = leader.status().last_applied;
        if applied.is_some()
            && leader.snapshot_index() == applied
            && leader.purged_index() == applied
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the leader never snapshotted and purged within {CONVERGE_BY:?}, so the joiner would \
             be caught up by log replication rather than an install: applied={applied:?} \
             snapshot={:?} purged={:?}",
            leader.snapshot_index(),
            leader.purged_index()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let joiner = spawn_with_snapshot_policy(2, addr2, dir2.path(), Some(2)).await;
    let seed = Authority::from(addr1);
    // Timed from *before* the call: the install races the admission window from here, so this is
    // the same quantity `ADMIT_CURRENCY_WAIT` is compared against inside `admit`.
    let join_started = Instant::now();
    // Admission commits the membership entry and returns (#433); the install it used to have to
    // outlast is no longer on this call's path, so the first attempt succeeds.
    joiner
        .join_via(&seed)
        .await
        .expect("admission returns once the membership entry commits, ahead of the install");
    let join_took = join_started.elapsed();

    // Everything this test can observe at the instant an assertion reads it. Carried into every
    // failure message below so a future failure classifies itself instead of needing a rerun with
    // instrumentation — which is what #492 cost, and what #460 added to the chaos tier for the
    // same reason.
    let state = |stage: &str| {
        let leader_status = leader.status();
        // `is_leader` is here because `replication_matching()` renders "not leading" and "leading
        // with nothing replicating" identically as `[]`. Without it a failure after an unexpected
        // election reads as a replication stall, which is the wrong thing to go and investigate.
        format!(
            "[{stage}] join_via took {join_took:?}; leader is_leader={} voters={:?} \
             matching={:?}; joiner snapshot={:?} last_applied={:?}",
            leader_status.is_leader,
            leader_status.voters,
            leader.replication_matching(),
            joiner.snapshot_index(),
            joiner.status().last_applied,
        )
    };

    // The joiner is a member now — and, for the whole install, a learner. A multi-MiB snapshot
    // cannot be current inside the admission's currency window, so no joint configuration exists
    // yet and the leader owes this node nothing. That is what makes a never-landing snapshot
    // unable to wedge the fleet, and it is checkable: no voterhood before catch-up.
    //
    // This rests on the *fixture*, not only on the code, and that dependence is no longer left to
    // a comment — the margin guard below measures it. Do not weaken this assertion to make a
    // failure go away: it guards two-phase admission (#433), and a failure here means the install
    // finished inside the window, which the guard will name.
    assert!(
        !leader.status().voters.contains(&2),
        "a joiner that must install a multi-MiB snapshot is admitted as a learner, not a voter \
         (requires the install to outlast the {ADMIT_CURRENCY_WAIT:?} admission currency window; \
         if the fixture was shrunk, in-call promotion is the correct outcome and this test needs \
         a bigger one — see #492). {}",
        state("learner")
    );

    // The margin the assertion above depends on, measured rather than assumed. #492: #436 (binary,
    // file-backed snapshots) and #440 (KiB manifest + blob fetch) each made the install faster
    // while this fixture stayed at 8 x 512 KiB, until the install landed at ~0.5 s against a
    // 500 ms window and the test failed ~55% of the time in CI and never once locally. Erosion
    // must fail loudly, with the remedy named, instead of flaking.
    //
    // `snapshot_index().is_some()` means "holds a snapshot", not "installed one" — the joiner runs
    // the same snapshot policy and would eventually build its own. This reads as an *install*
    // measurement only because the purge poll above forecloses log replication, so the joiner's
    // first snapshot can only have arrived over the wire. The two are one argument: remove that
    // poll and this measurement silently stops meaning anything.
    let deadline = Instant::now() + CONVERGE_BY;
    while joiner.snapshot_index().is_none() {
        assert!(
            Instant::now() < deadline,
            "the joiner never installed a snapshot within {CONVERGE_BY:?}. {}",
            state("install-poll")
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let install_took = join_started.elapsed();
    assert!(
        install_took >= MIN_INSTALL_MARGIN * ADMIT_CURRENCY_WAIT,
        "the install took {install_took:?} against a {ADMIT_CURRENCY_WAIT:?} admission window — \
         the fixture no longer produces a slow install, so the learner assertion above is a coin \
         flip rather than a check on two-phase admission. Enlarge `PER_ENTRY_BYTES` (see #492 \
         and `MIN_INSTALL_MARGIN`'s note on the headroom); do not loosen the assertion. {}",
        state("margin")
    );

    let last_port = 19520 + (ENTRIES - 1) as u16;
    let deadline = Instant::now() + CONVERGE_BY;
    let mut converged = false;
    while Instant::now() < deadline {
        if joiner
            .imposter_config(DEFAULT_TENANT, last_port)
            .expect("read")
            .is_some()
        {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        converged,
        "the joiner was never caught up by snapshot within {CONVERGE_BY:?} (install measured at \
         {install_took:?}). {}",
        state("converge")
    );

    // The other half of the new shape: once current, the leader's promotion sweep makes the
    // joiner a voter with no further part played by the joiner itself.
    let deadline = Instant::now() + CONVERGE_DEADLINE;
    while !leader.status().voters.contains(&2) {
        assert!(
            Instant::now() < deadline,
            "a caught-up learner must be promoted to voter by the leader's sweep. {}",
            state("promote")
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The install carried the configs themselves, not a reference to them: the joiner must hold
    // every port the leader does, from its own applied state.
    let ports = joiner
        .configured_ports()
        .expect("read the joiner's configs");
    assert_eq!(
        ports.len(),
        ENTRIES,
        "the joiner must hold every imposter the snapshot carried: {ports:?}"
    );

    joiner.shutdown().await.ok();
    leader.shutdown().await.ok();
}

/// Issue #428, the fan-out half: catch-up in a fleet that already holds a quorum without the
/// joiner, so the install cannot be masked by the joiner's ack being required anyway.
///
/// In the test above the joiner is a learner for its whole install (#433), so no commit ever waits
/// on it; here nodes 1 and 2 hold a quorum without node 3 regardless of its role, so a disrupted
/// install can only show up as a stalled or restarted transfer, never as a wedged fleet.
///
/// **What `leaders_seen` does and does not prove.** It pins that leadership stays put across a
/// multi-MiB install, which is worth having. It is *not* evidence that chunk size keeps a
/// follower's election timer quiet — a follower receiving a snapshot gets no lease refresh at all
/// (`Raft::install_snapshot` only reaches the engine on the final chunk, and openraft sends no
/// AppendEntries to a peer while its snapshot streams). A *fresh* joiner cannot campaign for an
/// unrelated reason: it has applied nothing, so its own effective membership does not list it as a
/// voter, and `handle_tick_election` returns early for a non-voter. The case where a node is
/// already a voter and *can* campaign mid-install was a real hole — measured, filed as #431 and
/// closed by it; `a_restarted_voter_behind_a_purged_log_catches_up_by_snapshot` pins the fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_catch_up_does_not_disturb_a_fleet_that_already_has_quorum() {
    const CONVERGE_BY: Duration = Duration::from_secs(30);
    const REJOIN_INTERVAL: Duration = Duration::from_secs(5);

    let _serial = TEST_LOCK.lock().await;
    let ports = reserve_ports(3);
    let addrs: Vec<SocketAddr> = ports
        .iter()
        .map(|p| format!("127.0.0.1:{p}").parse().expect("addr"))
        .collect();
    let dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().expect("tempdir")).collect();

    let n1 = spawn_with_snapshot_policy(1, addrs[0], dirs[0].path(), Some(2)).await;
    n1.cluster_init().await.expect("bootstrap node 1");
    let deadline = Instant::now() + LEADER_DEADLINE;
    while !n1.status().is_leader {
        assert!(Instant::now() < deadline, "node 1 never became leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let seed = Authority::from(addrs[0]);
    // Node 2 joins while there is nothing to catch up on, so the two-voter quorum below is formed
    // without exercising the path under test.
    let n2 = spawn_with_snapshot_policy(2, addrs[1], dirs[1].path(), Some(2)).await;
    n2.join_via(&seed)
        .await
        .expect("node 2 joins an empty fleet");

    let mut last_revision = 0;
    for i in 0..8u16 {
        last_revision = n1
            .submit(bulky_request(19540 + i, &format!("m{i}"), 512 * 1024))
            .await
            .unwrap_or_else(|e| panic!("entry m{i} commits: {e}"))
            .revision;
    }
    assert!(
        n1.await_applied(last_revision, CONVERGE_DEADLINE)
            .await
            .is_empty()
    );
    // See the sibling test: no public signal for "snapshot built and purged", so this is a settle.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let n3 = spawn_with_snapshot_policy(3, addrs[2], dirs[2].path(), Some(2)).await;
    let mut joined = n3.join_via(&seed).await.is_ok();
    let mut last_attempt = Instant::now();
    let deadline = Instant::now() + CONVERGE_BY;
    let mut converged = false;
    let mut leaders_seen = BTreeSet::new();
    while Instant::now() < deadline {
        if let Some(leader) = n1.status().current_leader {
            leaders_seen.insert(leader);
        }
        if n3
            .imposter_config(DEFAULT_TENANT, 19547)
            .expect("read")
            .is_some()
        {
            converged = true;
            break;
        }
        if !joined && last_attempt.elapsed() >= REJOIN_INTERVAL {
            last_attempt = Instant::now();
            joined = n3.join_via(&seed).await.is_ok();
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let n3_status = n3.status();
    n3.shutdown().await.ok();
    n2.shutdown().await.ok();
    n1.shutdown().await.ok();

    assert!(
        converged,
        "the joiner never caught up within {CONVERGE_BY:?}: {n3_status:?}"
    );
    assert_eq!(
        leaders_seen,
        BTreeSet::from([1]),
        "leadership must not move while a joiner installs its snapshot"
    );
}

/// The chaos suite's C5 first roll, in-process: the departing leader stays
/// alive after `leave` returns — the container's drain window — and the
/// survivors must elect a successor promptly *during* that window, because
/// C5's first post-leave write is asserted with no retry.
///
/// This caught the liveness ticker speaking past step-down: openraft keeps an
/// ex-leader's idle replication clients, the ticker filled the drain's silence
/// with the old (still highest) vote, every survivor's leader lease stayed
/// fresh, and no election happened while the process lived. The ticker now
/// falls silent when the node stops leading; this pins the handover itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survivors_elect_while_the_departed_leader_still_runs() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    // Leave WITHOUT shutting the process down: this is the drain window.
    cluster
        .member(leader)
        .node
        .as_ref()
        .expect("running")
        .leave(Duration::from_secs(5))
        .await
        .expect("the leader must be able to leave gracefully");

    let t0 = tokio::time::Instant::now();
    let survivors: Vec<NodeId> = cluster
        .members
        .iter()
        .map(|m| m.id)
        .filter(|&id| id != leader)
        .collect();
    let handover = loop {
        let elected = survivors.iter().any(|&id| {
            let s = cluster.member(id).node.as_ref().expect("running").status();
            s.current_leader.is_some() && s.current_leader != Some(leader)
        });
        if elected {
            break t0.elapsed();
        }
        if t0.elapsed() > Duration::from_secs(8) {
            cluster.shutdown_all().await;
            panic!(
                "survivors never elected while the departed leader's process was \
                 still alive — the C5 graceful-leave handover is broken"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    eprintln!("handover took {handover:?}");
    cluster.shutdown_all().await;
    assert!(
        handover < Duration::from_millis(2000),
        "handover took {handover:?}; the first post-leave write races this"
    );
}

/// #433's acceptance: a fresh node whose catch-up is a multi-MiB snapshot
/// joins a fleet with a purged log, and **startup succeeds immediately** —
/// admission commits the membership entry and returns, with catch-up left to
/// replication and promotion left to the leader's sweep.
///
/// Three claims in one scenario, each pinned separately:
/// 1. `join_via` returns `Ok` on the *first* call, fast — under the old
///    one-phase admission this call rode the full snapshot catch-up inside a
///    1.5 s wait and failed by construction (the seed loop then retried into
///    its 30 s deadline).
/// 2. The joiner needs no further part in its own promotion: after the one
///    join call it is never spoken for again (C5's criterion — the seed
///    connection is gone), yet the leader's sweep promotes it to voter once
///    caught up.
/// 3. The end state is a four-voter fleet with the joiner converged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joiner_behind_a_purged_log_starts_as_learner_and_the_leader_promotes_it() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start_with_snapshots(3, 2).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    // Put enough on the log — snapshotting every 2 entries, purging to the
    // tip — that a fresh joiner can only be caught up by a multi-MiB
    // snapshot, never by log replay.
    for i in 0..8u16 {
        let r = cluster
            .leader()
            .expect("leader")
            .submit(bulky_request(19560 + i, &format!("j{i}"), 512 * 1024))
            .await
            .expect("the bulky entry commits");
        assert_eq!(r.outcome, rift_cluster::ControlOutcome::Applied);
    }

    // A fourth node, exactly as `start_full` would build it.
    let port = reserve_ports(1)[0];
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let dir = TempDir::new().expect("tempdir");
    let joiner =
        spawn_with_snapshot_policy(4, addr, dir.path(), cluster.snapshot_log_entries).await;

    let seed = Authority::from(cluster.member(1).addr);
    let asked = tokio::time::Instant::now();
    let outcome = joiner
        .join_via(&seed)
        .await
        .expect("the first join call must succeed: admission no longer includes catch-up");
    let admission_took = asked.elapsed();
    assert!(
        admission_took < Duration::from_secs(5),
        "admission took {admission_took:?}; it must commit a membership entry, \
         not wait out a snapshot catch-up"
    );

    // Track the joiner in the harness so shutdown covers it. From here on,
    // nothing calls anything on its behalf: promotion is the leader's job.
    cluster.members.push(Member {
        id: 4,
        addr,
        dir,
        node: Some(joiner),
    });

    let everyone: BTreeSet<NodeId> = BTreeSet::from([1, 2, 3, 4]);
    assert!(
        cluster
            .wait_voters(&everyone, Duration::from_secs(60))
            .await,
        "the leader's promotion sweep must make the caught-up joiner a voter \
         without the joiner asking again (admitted as {:?}, catching_up: {})",
        outcome.role,
        outcome.catching_up
    );

    // And the member the fleet gained is a real one: it converges on the data.
    let target = cluster
        .leader()
        .expect("leader")
        .status()
        .last_applied
        .expect("leader applied something");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mine = cluster
            .member(4)
            .node
            .as_ref()
            .expect("live")
            .status()
            .last_applied;
        if mine >= Some(target) {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            cluster.shutdown_all().await;
            panic!("the promoted joiner never converged (at {mine:?}, leader at {target})");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.shutdown_all().await;
}
