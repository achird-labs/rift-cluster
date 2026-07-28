//! In-process 3-node integration + failover harness for the Raft control plane
//! (issue #11, Phase-1 subset).
//!
//! This drives real [`RaftNode`]s over real localhost TCP through the public
//! crate API — so it doubles as a check that the API is enough to stand up,
//! join, replicate, kill, and restart a cluster. It is deliberately *in-process*:
//! it covers the Phase-1 exit tests that need only nodes and a network, and NOT
//! the container-based chaos suite (Envoy + toxiproxy partitions, admin-API /
//! Prometheus assertions), which depends on the `rift-ee-server` binary (#10) and
//! the HTTP config/metrics surface (#9) and lands when those exist. See
//! `tests/README.md` for the split and how to add a scenario.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rift_cluster::{Authority, NodeConfig, NodeId, RaftNode, Router};
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
    /// `Arc` because the node-bound subsystems (`SourcePuller`, and the flow
    /// and pull-on-miss bridges in the composed server) take a `&Arc<RaftNode>`
    /// so they can hold a `Weak` back to it without keeping it alive.
    node: Option<Arc<RaftNode>>,
}

async fn spawn(id: NodeId, addr: SocketAddr, dir: &Path) -> Arc<RaftNode> {
    let config = NodeConfig {
        node_id: id,
        bind: addr,
        advertise: Some(Authority::from(addr)),
        data_dir: dir.to_path_buf(),
        secret: Some(SECRET.to_owned()),
        routes: Router::new(),
        engine: None,
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
}

impl TestCluster {
    /// Start `n` nodes, bootstrap node 1, and seed-join the rest through it, so
    /// the returned cluster is one converged group of `n` voters.
    async fn start(n: usize) -> Self {
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

        let n1 = spawn(members[0].id, members[0].addr, members[0].dir.path()).await;
        n1.cluster_init().await.expect("bootstrap node 1");
        members[0].node = Some(n1);

        let seed = Authority::from(members[0].addr);
        for member in members.iter_mut().skip(1) {
            let node = spawn(member.id, member.addr, member.dir.path()).await;
            node.join_via(&seed)
                .await
                .unwrap_or_else(|e| panic!("node {} join: {e}", member.id));
            member.node = Some(node);
        }

        let cluster = Self { members };
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
                    n.get_imposter(port)
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
        let node = spawn(mid, addr, &dir).await;
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

// ---------------------------------------------------------------------------
// Issue #134: sources as control-plane objects
// ---------------------------------------------------------------------------

/// A source that counts how many times it was fetched, so the fetch-once
/// criterion is an assertion rather than an argument.
struct CountingSource {
    fetches: Arc<std::sync::atomic::AtomicUsize>,
    body: std::sync::Mutex<Vec<rift_ee::seams::ImposterConfig>>,
    version: std::sync::Mutex<String>,
}

impl rift_ee::seams::ImposterSource for CountingSource {
    fn schemes(&self) -> &'static [&'static str] {
        &["counting"]
    }

    fn fetch<'a>(
        &'a self,
        _r: &'a rift_ee::seams::SourceRef,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = anyhow::Result<rift_ee::seams::FetchedImposters>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.fetches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(rift_ee::seams::FetchedImposters {
                configs: self.body.lock().expect("body lock").clone(),
                intercept: None,
                routes: None,
                meta: rift_ee::seams::SourceMeta {
                    version: Some(self.version.lock().expect("version lock").clone()),
                    fetched_at: std::time::SystemTime::now(),
                },
                unchanged: false,
            })
        })
    }
}

fn source_config(port: u16, name: &str) -> rift_ee::seams::ImposterConfig {
    serde_json::from_value(serde_json::json!({
        "port": port,
        "protocol": "http",
        "name": name,
    }))
    .expect("test config parses")
}

/// C-#134: one pull fetches exactly once no matter how many nodes are in the
/// fleet, and every node converges on what that single fetch produced.
///
/// This is the property that justifies fetch-then-submit over "each node
/// fetches for itself": the fetched bytes enter the log once, so replicas
/// cannot disagree about what the source said.
#[tokio::test]
async fn source_pull_fetches_exactly_once_and_converges_the_fleet() {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    assert!(cluster.wait_for_leader(LEADER_DEADLINE).await.is_some());

    let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let source = Arc::new(CountingSource {
        fetches: Arc::clone(&fetches),
        body: std::sync::Mutex::new(vec![source_config(9401, "from-source-v1")]),
        version: std::sync::Mutex::new("v1".to_owned()),
    });
    let mut registry = rift_ee::seams::SourceRegistry::new();
    registry
        .register(Arc::clone(&source) as Arc<dyn rift_ee::seams::ImposterSource>)
        .expect("register the counting source");
    let puller = rift_cluster::SourcePuller::new(registry);
    // Bound to the leader here; the write path forwards from any node, so which
    // node holds the puller is not what makes the fetch single.
    puller
        .bind(cluster.leader_handle().expect("a leader"))
        .expect("bind the puller");

    let report = puller
        .declare_and_pull(
            "mocks",
            "counting://host/i.json",
            rift_cluster::OnDrift::Overwrite,
        )
        .await
        .expect("declare and pull");
    assert!(!report.unchanged);
    assert_eq!(report.changed, vec![9401]);
    assert_eq!(
        fetches.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a 3-node fleet must fetch the source exactly once: followers apply, they do not fetch"
    );

    assert!(
        cluster
            .wait_converged(9401, "from-source-v1", CONVERGE_DEADLINE)
            .await,
        "every node must converge on the config the single fetch produced"
    );
    // And every node can answer for the source itself, not just its imposters.
    for node in cluster.live() {
        let record = node
            .source("mocks")
            .expect("read source")
            .expect("the source is replicated, not node-local");
        assert_eq!(record.last_version.as_deref(), Some("v1"));
        assert_eq!(record.ports, vec![9401]);
        assert!(!record.drifted);
    }

    // Re-pulling unchanged content fetches again (the provider is the only one
    // who can tell) but writes nothing: the applied index must not move.
    let before = cluster
        .leader()
        .expect("a leader")
        .status()
        .last_applied
        .expect("an applied index");
    let report = puller.pull("mocks", None).await.expect("re-pull");
    assert!(report.unchanged, "identical content is not a change");
    assert!(report.changed.is_empty());
    assert_eq!(
        fetches.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the re-pull did fetch"
    );
    assert_eq!(
        cluster
            .leader()
            .expect("a leader")
            .status()
            .last_applied
            .expect("an applied index"),
        before,
        "unchanged content must produce no log entry at all"
    );

    // A real change does move the fleet.
    *source.body.lock().expect("body lock") = vec![source_config(9401, "from-source-v2")];
    *source.version.lock().expect("version lock") = "v2".to_owned();
    let report = puller.pull("mocks", None).await.expect("pull v2");
    assert!(!report.unchanged);
    assert!(
        cluster
            .wait_converged(9401, "from-source-v2", CONVERGE_DEADLINE)
            .await,
        "a changed document must reach every node"
    );

    cluster.shutdown_all().await;
}

/// The secret-hygiene criterion, asserted where it matters: a credential-bearing
/// URI is refused *before* anything is written, so it never reaches the log —
/// not even as a committed refusal, which would keep the secret on every
/// replica's disk and in every snapshot.
#[tokio::test]
async fn a_credential_bearing_source_uri_never_reaches_the_log() {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(1).await;
    assert!(cluster.wait_for_leader(LEADER_DEADLINE).await.is_some());

    let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = rift_ee::seams::SourceRegistry::new();
    registry
        .register(Arc::new(CountingSource {
            fetches: Arc::clone(&fetches),
            body: std::sync::Mutex::new(vec![]),
            version: std::sync::Mutex::new("v1".to_owned()),
        }))
        .expect("register");
    let puller = rift_cluster::SourcePuller::new(registry);
    puller
        .bind(cluster.leader_handle().expect("a leader"))
        .expect("bind");

    let before = cluster
        .leader()
        .expect("a leader")
        .status()
        .last_applied
        .expect("an applied index");
    let err = puller
        .declare_and_pull(
            "leaky",
            "counting://user:hunter2@host/i.json",
            rift_cluster::OnDrift::Overwrite,
        )
        .await
        .expect_err("a credential-bearing uri must be refused");
    assert!(
        matches!(&err, rift_cluster::PullError::BadRequest(detail) if detail.contains("auth_ref")),
        "{err}"
    );
    assert_eq!(
        cluster
            .leader()
            .expect("a leader")
            .status()
            .last_applied
            .expect("an applied index"),
        before,
        "the refused uri must not have produced a log entry"
    );
    assert!(
        cluster
            .leader()
            .expect("a leader")
            .sources()
            .expect("read sources")
            .is_empty()
    );
    assert_eq!(
        fetches.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a refused source is never fetched"
    );

    cluster.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Issue #135: the leader-only tracking poll scheduler
// ---------------------------------------------------------------------------

/// Declare a tracking source and start a scheduler bound to `node`.
async fn start_scheduler(
    node: &Arc<RaftNode>,
    source: &Arc<CountingSource>,
) -> (
    Arc<rift_cluster::SourcePuller>,
    Arc<rift_cluster::PollStatus>,
    tokio::task::JoinHandle<()>,
) {
    let mut registry = rift_ee::seams::SourceRegistry::new();
    registry
        .register(Arc::clone(source) as Arc<dyn rift_ee::seams::ImposterSource>)
        .expect("register the counting source");
    let puller = Arc::new(rift_cluster::SourcePuller::new(registry));
    puller.bind(node).expect("bind the puller");
    // The task handle is returned, not dropped: the supervisor holds an
    // `Arc<RaftNode>` while waiting on the leadership watch, so a test that
    // forgot to abort it would keep the node alive past `shutdown_all` and the
    // next test's bind would fail.
    let (status, task) =
        rift_cluster::SourceScheduler::spawn(&tokio::runtime::Handle::current(), node, &puller);
    (puller, status, task)
}

fn tracking_put(id: &str, uri: &str, poll_secs: u64) -> rift_cluster::ControlRequest {
    rift_cluster::ControlRequest {
        op_id: uuid::Uuid::new_v4(),
        principal: None,
        issued_at_secs: 0,
        expected_revision: None,
        op: rift_cluster::ControlOp::SourcePut {
            tenant: rift_cluster::TenantId::default(),
            id: id.to_owned(),
            uri: uri.to_owned(),
            mode: rift_cluster::SourceMode::Tracking,
            auth_ref: None,
            on_drift: rift_cluster::OnDrift::Overwrite,
            poll_secs: Some(poll_secs),
        },
    }
}

/// The property the whole design rests on: **one poller fleet-wide**, not one
/// per node.
///
/// Every node runs a scheduler — the real deployment shape — but each is given
/// its **own** counting source with its **own** counter. That turns the claim
/// into an exact assertion (`followers fetched 0 times`) rather than a timing
/// bound, which matters: an earlier version of this test asserted a ceiling on
/// the *total* fetch count and a mutant that ignored leadership slipped under
/// it, because two pollers at a 5s cadence over 12s land right at the bound.
/// Per-node counters cannot be fudged by cadence, jitter, or a slow runner.
#[tokio::test]
async fn tracking_polls_run_on_the_leader_only() {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(2).await;
    let leader_id = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    // One scheduler per node, each with its own counter.
    let mut counters: Vec<(NodeId, Arc<std::sync::atomic::AtomicUsize>)> = Vec::new();
    let mut schedulers = Vec::new();
    for member in &cluster.members {
        let node = member.node.as_ref().expect("running");
        let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source = Arc::new(CountingSource {
            fetches: Arc::clone(&fetches),
            body: std::sync::Mutex::new(vec![source_config(9501, "tracked-v1")]),
            version: std::sync::Mutex::new("v1".to_owned()),
        });
        schedulers.push(start_scheduler(node, &source).await);
        counters.push((member.id, fetches));
    }

    let leader = cluster.leader_handle().expect("a leader").clone();
    leader
        .submit(tracking_put("tracked", "counting://cfg/i.json", 5))
        .await
        .expect("declaring a tracking source commits");

    // Long enough that a follower running its own timer would certainly have
    // fired at a 5s cadence.
    tokio::time::sleep(Duration::from_secs(14)).await;

    for (id, fetches) in &counters {
        let observed = fetches.load(std::sync::atomic::Ordering::SeqCst);
        if *id == leader_id {
            assert!(
                observed >= 1,
                "node {id} is the leader and must poll; saw {observed} fetches"
            );
        } else {
            assert_eq!(
                observed, 0,
                "node {id} is a follower and must never fetch — a follower that polls is the \
                 duplicate-fetch bug the whole fetch-then-submit design exists to prevent"
            );
        }
    }

    // And the polled content really did converge through the log.
    assert!(
        cluster
            .wait_converged(9501, "tracked-v1", CONVERGE_DEADLINE)
            .await,
        "a scheduled poll must apply through the log like any other pull"
    );

    // Abort before shutdown: dropping a `JoinHandle` does not cancel the task,
    // and a live supervisor holds the node alive past `shutdown_all`.
    for (_, _, task) in &schedulers {
        task.abort();
    }
    cluster.shutdown_all().await;
}

/// Unchanged content costs **zero log growth**, however long the fleet polls.
/// This is what makes tracking mode affordable, and it is the first thing a
/// careless refactor of the digest short circuit would break.
#[tokio::test]
async fn polling_unchanged_content_never_grows_the_log() {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(1).await;
    assert!(cluster.wait_for_leader(LEADER_DEADLINE).await.is_some());

    let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let source = Arc::new(CountingSource {
        fetches: Arc::clone(&fetches),
        body: std::sync::Mutex::new(vec![source_config(9502, "static")]),
        version: std::sync::Mutex::new("v1".to_owned()),
    });
    let node = cluster.leader_handle().expect("a leader").clone();
    let (_puller, _status, task) = start_scheduler(&node, &source).await;

    node.submit(tracking_put("static", "counting://cfg/i.json", 5))
        .await
        .expect("declaring commits");
    // Let the first poll apply the content, then pin the index.
    assert!(
        cluster
            .wait_converged(9502, "static", CONVERGE_DEADLINE)
            .await
    );
    let settled = node.status().last_applied.expect("an applied index");
    let fetches_at_settle = fetches.load(std::sync::atomic::Ordering::SeqCst);

    tokio::time::sleep(Duration::from_secs(12)).await;

    assert!(
        fetches.load(std::sync::atomic::Ordering::SeqCst) > fetches_at_settle,
        "the scheduler must still be polling — otherwise this proves nothing"
    );
    assert_eq!(
        node.status().last_applied.expect("an applied index"),
        settled,
        "polling unchanged content must not write a single log entry"
    );

    task.abort();
    cluster.shutdown_all().await;
}

/// A credentialed provider that counts fetches — the same shape as
/// `CountingSource` above, but registered through
/// `SourceProviders::register_credentialed` / `CredentialedSource`, the
/// enterprise seam issue #136 adds. `ImposterSource::fetch` has no `auth_ref`
/// to give it, so exercising the digest short circuit through *this* trait is
/// what proves it fires on the path the real `git+https:`/`s3:`/`registry:`
/// providers actually use, not merely on the upstream-only path
/// `source_pull_fetches_exactly_once_and_converges_the_fleet` above already
/// covers.
struct CountingCredentialedSource {
    fetches: Arc<std::sync::atomic::AtomicUsize>,
    body: std::sync::Mutex<Vec<rift_ee::seams::ImposterConfig>>,
    version: std::sync::Mutex<String>,
}

impl rift_cluster::sources::CredentialedSource for CountingCredentialedSource {
    fn schemes(&self) -> &'static [&'static str] {
        &["counting-cred"]
    }

    fn fetch_with_auth<'a>(
        &'a self,
        _r: &'a rift_ee::seams::SourceRef,
        _auth_ref: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = anyhow::Result<rift_ee::seams::FetchedImposters>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.fetches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(rift_ee::seams::FetchedImposters {
                configs: self.body.lock().expect("body lock").clone(),
                intercept: None,
                routes: None,
                meta: rift_ee::seams::SourceMeta {
                    version: Some(self.version.lock().expect("version lock").clone()),
                    fetched_at: std::time::SystemTime::now(),
                },
                unchanged: false,
            })
        })
    }
}

/// Issue #136 review, B6.1: the digest short circuit (#134) exercised through
/// the *actual shipped path* — a credentialed provider, pulled twice through
/// a real `SourcePuller` bound to a real `RaftNode` — rather than the
/// tautological version this replaces (two `digest_of(...)` calls compared
/// directly, in `sources/provider_tests.rs`, which would still pass even if
/// the short circuit in `SourcePuller::pull` were deleted outright, since it
/// never drove a pull through that code at all).
#[tokio::test]
async fn a_credentialed_source_short_circuits_on_unchanged_content() {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(1).await;
    assert!(cluster.wait_for_leader(LEADER_DEADLINE).await.is_some());

    let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let source = Arc::new(CountingCredentialedSource {
        fetches: Arc::clone(&fetches),
        body: std::sync::Mutex::new(vec![source_config(9504, "cred-static")]),
        version: std::sync::Mutex::new("v1".to_owned()),
    });
    let mut providers =
        rift_cluster::sources::SourceProviders::new(rift_ee::seams::SourceRegistry::new());
    providers
        .register_credentialed(source as Arc<dyn rift_cluster::sources::CredentialedSource>)
        .expect("register the credentialed counting source");
    let puller = rift_cluster::SourcePuller::new(providers);
    puller
        .bind(cluster.leader_handle().expect("a leader"))
        .expect("bind the puller");

    let first = puller
        .declare_and_pull(
            "cred-mocks",
            "counting-cred://host/i.json",
            rift_cluster::OnDrift::Overwrite,
        )
        .await
        .expect("first pull");
    assert!(!first.unchanged, "the first pull is a real change");
    assert_eq!(fetches.load(std::sync::atomic::Ordering::SeqCst), 1);

    let before = cluster
        .leader()
        .expect("a leader")
        .status()
        .last_applied
        .expect("an applied index");

    let second = puller.pull("cred-mocks", None).await.expect("second pull");
    assert!(
        second.unchanged,
        "identical content through a credentialed provider must short circuit"
    );
    assert_eq!(
        fetches.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the re-pull did fetch — the provider is the only one who can tell content is unchanged"
    );
    assert_eq!(
        cluster
            .leader()
            .expect("a leader")
            .status()
            .last_applied
            .expect("an applied index"),
        before,
        "unchanged content through a credentialed provider must produce no log entry at all"
    );

    cluster.shutdown_all().await;
}

/// The task set follows the source table without a restart: deleting a tracking
/// source stops its poller.
#[tokio::test]
async fn deleting_a_tracking_source_stops_its_poller() {
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(1).await;
    assert!(cluster.wait_for_leader(LEADER_DEADLINE).await.is_some());

    let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let source = Arc::new(CountingSource {
        fetches: Arc::clone(&fetches),
        body: std::sync::Mutex::new(vec![source_config(9503, "temp")]),
        version: std::sync::Mutex::new("v1".to_owned()),
    });
    let node = cluster.leader_handle().expect("a leader").clone();
    let (_puller, _status, task) = start_scheduler(&node, &source).await;

    node.submit(tracking_put("temp", "counting://cfg/i.json", 5))
        .await
        .expect("declaring commits");
    tokio::time::sleep(Duration::from_secs(8)).await;
    let polled = fetches.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        polled >= 1,
        "the source must have been polled at least once"
    );

    node.submit(rift_cluster::ControlRequest {
        op_id: uuid::Uuid::new_v4(),
        principal: None,
        issued_at_secs: 0,
        expected_revision: None,
        op: rift_cluster::ControlOp::SourceDelete {
            tenant: rift_cluster::TenantId::default(),
            id: "temp".to_owned(),
        },
    })
    .await
    .expect("delete commits");

    // Give the supervisor a reconcile window, then confirm the rate stopped.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let after_delete = fetches.load(std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(
        fetches.load(std::sync::atomic::Ordering::SeqCst),
        after_delete,
        "a deleted source must stop being polled without restarting the node"
    );

    task.abort();
    cluster.shutdown_all().await;
}
