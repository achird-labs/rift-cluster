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
    Authority, ControlRequest, DEFAULT_TENANT, NodeConfig, NodeId, RaftNode, Router,
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
    /// `Arc` because the node-bound subsystems (`SourcePuller`, and the flow
    /// and pull-on-miss bridges in the composed server) take a `&Arc<RaftNode>`
    /// so they can hold a `Weak` back to it without keeping it alive.
    node: Option<Arc<RaftNode>>,
}

async fn spawn(
    id: NodeId,
    addr: SocketAddr,
    dir: &Path,
    audit_retention_secs: u64,
) -> Arc<RaftNode> {
    let config = NodeConfig {
        node_id: id,
        bind: addr,
        advertise: Some(Authority::from(addr)),
        data_dir: dir.to_path_buf(),
        secret: Some(SECRET.to_owned()),
        routes: Router::new(),
        engine: None,
        audit_retention_secs,
        // These in-process tests drive `build_snapshot`/`install_snapshot` directly, so they need
        // no help provoking one; the knob exists for the container tier, which cannot (issue #183).
        snapshot_log_entries: None,
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
    /// Retained so `restart` brings a node back with the same retention it was
    /// started with. A node that silently reverted to the 30-day default on
    /// restart would look like a GC bug rather than a harness bug.
    audit_retention_secs: u64,
}

impl TestCluster {
    /// Start `n` nodes, bootstrap node 1, and seed-join the rest through it, so
    /// the returned cluster is one converged group of `n` voters.
    async fn start(n: usize) -> Self {
        Self::start_with_audit_retention(n, rift_cluster::DEFAULT_AUDIT_RETENTION_SECS).await
    }

    /// [`Self::start`] with an explicit audit retention window, for the tests
    /// that need GC to actually run within a test's lifetime.
    async fn start_with_audit_retention(n: usize, audit_retention_secs: u64) -> Self {
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

        let n1 = spawn(
            members[0].id,
            members[0].addr,
            members[0].dir.path(),
            audit_retention_secs,
        )
        .await;
        n1.cluster_init().await.expect("bootstrap node 1");
        members[0].node = Some(n1);

        let seed = Authority::from(members[0].addr);
        for member in members.iter_mut().skip(1) {
            let node = spawn(
                member.id,
                member.addr,
                member.dir.path(),
                audit_retention_secs,
            )
            .await;
            node.join_via(&seed)
                .await
                .unwrap_or_else(|e| panic!("node {} join: {e}", member.id));
            member.node = Some(node);
        }

        let cluster = Self {
            members,
            audit_retention_secs,
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
        let node = spawn(mid, addr, &dir, self.audit_retention_secs).await;
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
        rift_cluster::DEFAULT_AUDIT_RETENTION_SECS,
    )
    .await;
    let b = spawn(
        2,
        format!("127.0.0.1:{}", ports[1]).parse().unwrap(),
        db.path(),
        rift_cluster::DEFAULT_AUDIT_RETENTION_SECS,
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
    let rejoined = spawn(departed, addr, new_dir.path(), cluster.audit_retention_secs).await;
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
    let rejoined = spawn(departed, addr, &dir, cluster.audit_retention_secs).await;
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
    body: std::sync::Mutex<Vec<rift_cluster_base::seams::ImposterConfig>>,
    version: std::sync::Mutex<String>,
}

impl rift_cluster_base::seams::ImposterSource for CountingSource {
    fn schemes(&self) -> &'static [&'static str] {
        &["counting"]
    }

    fn fetch<'a>(
        &'a self,
        _r: &'a rift_cluster_base::seams::SourceRef,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = anyhow::Result<rift_cluster_base::seams::FetchedImposters>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.fetches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(rift_cluster_base::seams::FetchedImposters {
                configs: self.body.lock().expect("body lock").clone(),
                intercept: None,
                routes: None,
                meta: rift_cluster_base::seams::SourceMeta {
                    version: Some(self.version.lock().expect("version lock").clone()),
                    fetched_at: std::time::SystemTime::now(),
                },
                unchanged: false,
            })
        })
    }
}

fn source_config(port: u16, name: &str) -> rift_cluster_base::seams::ImposterConfig {
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
    let mut registry = rift_cluster_base::seams::SourceRegistry::new();
    registry
        .register(Arc::clone(&source) as Arc<dyn rift_cluster_base::seams::ImposterSource>)
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
            .source(DEFAULT_TENANT, "mocks")
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
    let report = puller
        .pull(DEFAULT_TENANT, "mocks", None)
        .await
        .expect("re-pull");
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
    let report = puller
        .pull(DEFAULT_TENANT, "mocks", None)
        .await
        .expect("pull v2");
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
    let mut registry = rift_cluster_base::seams::SourceRegistry::new();
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
            .sources(DEFAULT_TENANT)
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
    let mut registry = rift_cluster_base::seams::SourceRegistry::new();
    registry
        .register(Arc::clone(source) as Arc<dyn rift_cluster_base::seams::ImposterSource>)
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
/// cluster seam issue #136 adds. `ImposterSource::fetch` has no `auth_ref`
/// to give it, so exercising the digest short circuit through *this* trait is
/// what proves it fires on the path the real `git+https:`/`s3:`/`registry:`
/// providers actually use, not merely on the upstream-only path
/// `source_pull_fetches_exactly_once_and_converges_the_fleet` above already
/// covers.
struct CountingCredentialedSource {
    fetches: Arc<std::sync::atomic::AtomicUsize>,
    body: std::sync::Mutex<Vec<rift_cluster_base::seams::ImposterConfig>>,
    version: std::sync::Mutex<String>,
}

impl rift_cluster::sources::CredentialedSource for CountingCredentialedSource {
    fn schemes(&self) -> &'static [&'static str] {
        &["counting-cred"]
    }

    fn fetch_with_auth<'a>(
        &'a self,
        _r: &'a rift_cluster_base::seams::SourceRef,
        _auth_ref: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = anyhow::Result<rift_cluster_base::seams::FetchedImposters>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.fetches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(rift_cluster_base::seams::FetchedImposters {
                configs: self.body.lock().expect("body lock").clone(),
                intercept: None,
                routes: None,
                meta: rift_cluster_base::seams::SourceMeta {
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
    let mut providers = rift_cluster::sources::SourceProviders::new(
        rift_cluster_base::seams::SourceRegistry::new(),
    );
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

    let second = puller
        .pull(DEFAULT_TENANT, "cred-mocks", None)
        .await
        .expect("second pull");
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

// ---------------------------------------------------------------------------
// Issue #163 — the audit stream and leader-side quotas across a real cluster.
//
// The state-machine unit tests in `raft/store.rs` already pin the projection's
// shape. What can only be asserted here is the claim that makes the endpoint
// fan-out-free: every replica derives the *same* rows from the same log, so any
// node answers for the fleet. Narrating that property is not the same as
// checking it, so these tests query all three nodes and compare.
// ---------------------------------------------------------------------------

/// Poll, bounded, until every live node's audit stream is byte-identical.
/// Returns the agreed rows, or `None` on timeout — never a synthetic pass.
async fn wait_audit_agreed(
    cluster: &TestCluster,
    deadline: Duration,
) -> Option<Vec<rift_cluster::AuditRow>> {
    let start = Instant::now();
    loop {
        let per_node: Vec<Vec<rift_cluster::AuditRow>> = cluster
            .live()
            .map(|n| n.audit_since(0, None, 10_000).expect("read audit"))
            .collect();
        if let Some(first) = per_node.first()
            && !first.is_empty()
            && per_node.iter().all(|rows| rows == first)
        {
            return Some(first.clone());
        }
        if start.elapsed() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
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

/// AC1: every write appears exactly once, with the same revision, on every node
/// — asserted by querying all three and comparing, which is the whole of the
/// "no fan-out needed" claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_audit_stream_is_identical_on_every_node() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    assert!(cluster.wait_for_leader(LEADER_DEADLINE).await.is_some());

    let ports = [18081u16, 18082, 18083];
    for (i, port) in ports.iter().enumerate() {
        cluster.write_on_leader(*port, &format!("cfg-{i}")).await;
    }
    for (i, port) in ports.iter().enumerate() {
        assert!(
            cluster
                .wait_converged(*port, &format!("cfg-{i}"), CONVERGE_DEADLINE)
                .await
        );
    }

    let rows = wait_audit_agreed(&cluster, CONVERGE_DEADLINE)
        .await
        .expect("every node must derive the same audit rows from the same log");

    for port in ports {
        let matching: Vec<_> = rows
            .iter()
            .filter(|r| r.resource == port.to_string())
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "each write is audited exactly once, not once per node and not \
             twice on a replay: {matching:?}"
        );
        assert_eq!(matching[0].action, "imposter.write");
    }

    let mut revisions: Vec<u64> = rows.iter().map(|r| r.revision).collect();
    let unique = revisions.len();
    revisions.sort_unstable();
    revisions.dedup();
    assert_eq!(
        revisions.len(),
        unique,
        "revisions are unique per row: {rows:?}"
    );

    cluster.shutdown_all().await;
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

    let rows = wait_audit_agreed(&cluster, CONVERGE_DEADLINE)
        .await
        .expect("the audit stream must agree across nodes");

    let refusal: Vec<_> = rows.iter().filter(|r| r.resource == "18092").collect();
    assert_eq!(refusal.len(), 1, "the refusal is audited once: {rows:?}");
    assert_eq!(
        refusal[0].revision, refused.revision,
        "the audited refusal sits at the revision the write returned"
    );
    assert_eq!(
        refusal[0].outcome, refused.outcome,
        "the committed outcome is the audited one, verbatim"
    );
    assert_eq!(refusal[0].tenant, rift_cluster::TenantId::new("acme"));

    // And the refusal really is the same decision everywhere, not just the same
    // row shape: every node reports the identical outcome at that revision.
    for node in cluster.live() {
        let node_rows = node.audit_since(refused.revision, None, 10).expect("read");
        let row = node_rows
            .iter()
            .find(|r| r.revision == refused.revision)
            .expect("every node holds the refusal");
        assert_eq!(row.outcome, refused.outcome);
    }

    cluster.shutdown_all().await;
}

/// AC4, the restart half: audit history is committed state, so it survives every
/// node going down and coming back — not just the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_rows_survive_a_full_cluster_restart() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    assert!(cluster.wait_for_leader(LEADER_DEADLINE).await.is_some());

    cluster.write_on_leader(18095, "before-restart").await;
    assert!(
        cluster
            .wait_converged(18095, "before-restart", CONVERGE_DEADLINE)
            .await
    );
    let before = wait_audit_agreed(&cluster, CONVERGE_DEADLINE)
        .await
        .expect("the audit stream agrees before the restart");

    for id in [1, 2, 3] {
        cluster.restart(id).await;
    }
    assert!(
        cluster.wait_for_leader(LEADER_DEADLINE).await.is_some(),
        "the cluster must re-elect after a full restart"
    );

    let after = wait_audit_agreed(&cluster, CONVERGE_DEADLINE)
        .await
        .expect("the audit stream agrees after the restart");
    assert_eq!(
        after, before,
        "audit history is committed state and must survive a full-cluster \
         restart unchanged"
    );

    cluster.shutdown_all().await;
}

/// AC3 across a real cluster.
///
/// The store-level test is the one that proves the *mechanism* is clock-agnostic
/// — it uses timestamps decades in the past, so a `SystemTime::now()`-based GC
/// would sweep everything and fail it. The criterion's literal phrasing ("a node
/// whose local clock is skewed by a week") cannot be staged in-process for
/// exactly the reason the feature is correct: nothing in the retention path
/// reads a local clock, so there is no local clock to skew.
///
/// What is worth asserting here is the consequence: retention GC, running inside
/// apply on every replica, leaves the three streams **in agreement**. A GC that
/// read node-local state would diverge them, and this is where that would show.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_gc_leaves_every_node_holding_the_same_rows() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start_with_audit_retention(3, 100).await;
    assert!(cluster.wait_for_leader(LEADER_DEADLINE).await.is_some());

    let leader = cluster.leader().expect("a leader");
    let imposter = |port: u16| {
        serde_json::from_value(serde_json::json!({
            "port": port,
            "protocol": "http",
            "host": "127.0.0.1",
        }))
        .expect("test config parses")
    };

    // Three writes on an old logical clock, then two that advance it well past
    // the retention window — the second of which triggers the sweep.
    for (op_id, port, ts) in [
        (1u128, 19201u16, 1_000u64),
        (2, 19202, 1_050),
        (3, 19203, 1_090),
        (4, 19204, 5_000),
        (5, 19205, 5_001),
    ] {
        leader
            .write(submit_request(
                op_id,
                ts,
                rift_cluster::ControlOp::PutImposter {
                    tenant: rift_cluster::TenantId::default(),
                    config: Box::new(imposter(port)),
                },
            ))
            .await
            .expect("write commits");
    }

    let rows = wait_audit_agreed(&cluster, CONVERGE_DEADLINE)
        .await
        .expect("every node must agree after retention GC has run");

    assert!(
        rows.iter().all(|r| r.ts_secs >= 4_901),
        "rows outside the retention window must be gone on every node: {rows:?}"
    );
    assert!(
        rows.iter().any(|r| r.resource == "19205"),
        "and the recent ones must remain: {rows:?}"
    );

    // Belt and braces: compare the three streams element-wise, not just their
    // agreed-upon length.
    let per_node: Vec<Vec<rift_cluster::AuditRow>> = cluster
        .live()
        .map(|n| n.audit_since(0, None, 10_000).expect("read audit"))
        .collect();
    for stream in &per_node {
        assert_eq!(
            stream, &rows,
            "a node that GC'd differently would show up here"
        );
    }

    cluster.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Issue #164 — the audit export sink: leader-only, checkpointed, at-least-once.
// ---------------------------------------------------------------------------

/// A collector that counts what actually arrived.
///
/// Deliberately a real HTTP listener rather than an injected fake transport:
/// the criteria are about what reaches a sink across a failover, and a fake
/// that the exporter calls directly would not exercise the framing, the batch
/// boundary, or the failure path that a 500 produces.
struct CountingSink {
    addr: std::net::SocketAddr,
    received: Arc<std::sync::Mutex<Vec<(u64, String)>>>,
    requests: Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl CountingSink {
    /// `status` is what every request gets: 200 for the happy path, 500 for the
    /// permanently-failing-sink scenario.
    async fn start(status: u16) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind counting sink");
        let addr = listener.local_addr().expect("sink addr");
        let received: Arc<std::sync::Mutex<Vec<(u64, String)>>> = Arc::default();
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (rx_rows, rx_reqs) = (Arc::clone(&received), Arc::clone(&requests));

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let (rows, reqs) = (Arc::clone(&rx_rows), Arc::clone(&rx_reqs));
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 8192];
                    // Read headers, then exactly Content-Length bytes of body.
                    let body = loop {
                        let Ok(n) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        let Some(split) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                            continue;
                        };
                        let head = String::from_utf8_lossy(&buf[..split]).to_lowercase();
                        let len: usize = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse().ok())
                            .unwrap_or(0);
                        if buf.len() >= split + 4 + len {
                            break buf[split + 4..split + 4 + len].to_vec();
                        }
                    };
                    reqs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if status < 400 {
                        let mut guard = rows.lock().expect("sink rows lock");
                        for line in String::from_utf8_lossy(&body).lines() {
                            if line.trim().is_empty() {
                                continue;
                            }
                            let row: serde_json::Value = serde_json::from_str(line)
                                .expect("the sink ships one row per line");
                            guard.push((
                                row["revision"].as_u64().expect("row carries a revision"),
                                row["opId"]
                                    .as_str()
                                    .expect("row carries an opId")
                                    .to_owned(),
                            ));
                        }
                    }
                    let reason = if status < 400 { "OK" } else { "Server Error" };
                    let response =
                        format!("HTTP/1.1 {status} {reason}\r\ncontent-length: 0\r\n\r\n");
                    stream.write_all(response.as_bytes()).await.ok();
                    stream.flush().await.ok();
                });
            }
        });

        Self {
            addr,
            received,
            requests,
            task,
        }
    }

    fn uri(&self) -> String {
        format!("http://{}/audit", self.addr)
    }

    fn rows(&self) -> Vec<(u64, String)> {
        self.received.lock().expect("sink rows lock").clone()
    }

    fn request_count(&self) -> usize {
        self.requests.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for CountingSink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn export_context() -> Arc<rift_cluster::audit_export::ExportContext> {
    Arc::new(rift_cluster::audit_export::ExportContext {
        resolver: Arc::new(rift_cluster::sources::auth::StandardResolver::new(None)),
        s3: rift_cluster::sources::s3::S3Config {
            endpoint: None,
            region: "us-east-1".to_owned(),
        },
    })
}

/// Attach an exporter to every live node. Every node runs one, exactly as in
/// production — which is the only way the leader-only claim is actually under
/// test rather than assumed by the harness.
fn attach_exporters(cluster: &TestCluster) -> Vec<tokio::task::JoinHandle<()>> {
    attach_exporters_with_status(cluster).1
}

/// The statuses are returned alongside the tasks because they are the only
/// observable surface for "the exporter noticed it was failing". Discarding
/// them (as this harness first did) leaves a 500-forever scenario asserting
/// only that writes still work — which passes just as well against an exporter
/// that silently gave up.
#[allow(clippy::type_complexity)]
fn attach_exporters_with_status(
    cluster: &TestCluster,
) -> (
    Vec<Arc<rift_cluster::audit_export::ExportStatus>>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let context = export_context();
    cluster
        .members
        .iter()
        .filter_map(|m| m.node.as_ref())
        .map(|node| {
            rift_cluster::audit_export::AuditExporter::spawn(
                &tokio::runtime::Handle::current(),
                node,
                Arc::clone(&context),
            )
        })
        .unzip()
}

/// Read a Prometheus counter out of the process-global default registry.
///
/// `None` when the family has not been emitted yet. The registry is global and
/// this harness is serialized by `TEST_LOCK`, but counters still accumulate
/// across scenarios in one binary — so callers must compare against a baseline
/// taken in the same test, never against an absolute value.
fn counter_value(name: &str) -> Option<f64> {
    prometheus::gather()
        .into_iter()
        .find(|family| family.get_name() == name)?
        .get_metric()
        .first()
        .map(|m| m.get_counter().get_value())
}

async fn declare_sink(cluster: &TestCluster, uri: &str) {
    declare_sink_with_batch(cluster, uri, 50).await;
}

async fn declare_sink_with_batch(cluster: &TestCluster, uri: &str, batch_max_rows: u32) {
    let leader = cluster.leader().expect("a leader to accept the sink");
    let response = leader
        .submit(ControlRequest {
            op_id: uuid::Uuid::new_v4(),
            principal: None,
            issued_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
            expected_revision: None,
            op: rift_cluster::ControlOp::AuditSinkPut {
                tenant: rift_cluster::TenantId::new(rift_cluster::FLEET_SCOPE),
                uri: uri.to_owned(),
                auth_ref: None,
                batch_max_rows,
            },
        })
        .await
        .expect("the sink declaration commits");
    assert_eq!(
        response.outcome,
        rift_cluster::ControlOutcome::Applied,
        "a valid sink declaration must apply"
    );
}

/// Commit a checkpoint directly, standing in for a leader that shipped a batch.
async fn submit_checkpoint(cluster: &TestCluster, revision: u64) {
    cluster
        .leader()
        .expect("a leader")
        .submit(ControlRequest {
            op_id: uuid::Uuid::new_v4(),
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op: rift_cluster::ControlOp::AuditCheckpointPut {
                tenant: rift_cluster::TenantId::new(rift_cluster::FLEET_SCOPE),
                revision,
            },
        })
        .await
        .expect("a checkpoint commits");
}

/// Poll until the leader's export checkpoint reaches `want`, or the deadline
/// passes. Returns whatever it last read, so the caller asserts on the value.
async fn wait_checkpoint(cluster: &TestCluster, want: u64, deadline: Duration) -> u64 {
    let start = Instant::now();
    loop {
        let seen = cluster
            .leader()
            .and_then(|n| n.audit_checkpoint().ok())
            .unwrap_or(0);
        if seen >= want || start.elapsed() > deadline {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll until the sink has seen at least `want` distinct rows, or the deadline
/// passes. Returns what it saw either way, so the caller asserts on content
/// rather than on this helper's verdict.
async fn wait_rows(sink: &CountingSink, want: usize, deadline: Duration) -> Vec<(u64, String)> {
    let start = Instant::now();
    loop {
        let rows = sink.rows();
        let distinct: BTreeSet<_> = rows.iter().cloned().collect();
        if distinct.len() >= want || start.elapsed() > deadline {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// AC1: off by default. With no sink record the exporter must not read the
/// audit table, must not build a transport, and must not reach any network.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_export_is_inert_without_a_sink_record() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    // A sink exists and is listening, but nothing points at it.
    let sink = CountingSink::start(200).await;
    let tasks = attach_exporters(&cluster);

    for port in [19_001, 19_002, 19_003] {
        cluster.write_on_leader(port, "inert").await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert_eq!(
        sink.request_count(),
        0,
        "with no sink record declared, the exporter must never reach the network"
    );
    for member in &cluster.members {
        if let Some(node) = &member.node {
            assert_eq!(
                node.audit_sink().expect("read sink"),
                None,
                "node {} must hold no sink record",
                member.id
            );
            assert_eq!(
                node.audit_checkpoint().expect("read checkpoint"),
                0,
                "node {} must not have checkpointed anything",
                member.id
            );
        }
    }

    for task in tasks {
        task.abort();
    }
    cluster.shutdown_all().await;
}

/// AC2: exactly one copy of each audit row reaches the sink across a 3-node
/// fleet. Every node runs an exporter; only the leader may ship.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_audit_row_reaches_the_sink_exactly_once() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    let sink = CountingSink::start(200).await;
    declare_sink(&cluster, &sink.uri()).await;
    let tasks = attach_exporters(&cluster);

    let mut written = Vec::new();
    for port in 19_100..19_106 {
        written.push(cluster.write_on_leader(port, "shipped").await);
    }

    let rows = wait_rows(&sink, written.len(), Duration::from_secs(20)).await;
    let distinct: BTreeSet<(u64, String)> = rows.iter().cloned().collect();

    // The strong assertion: no duplicates at all. Three nodes each derive these
    // rows, so if leadership were not gating the export this would be 3×.
    assert_eq!(
        rows.len(),
        distinct.len(),
        "without a failover there is no duplicate window; got {} rows, {} distinct: {rows:?}",
        rows.len(),
        distinct.len()
    );
    let shipped_revisions: BTreeSet<u64> = distinct.iter().map(|(rev, _)| *rev).collect();
    for revision in &written {
        assert!(
            shipped_revisions.contains(revision),
            "revision {revision} was committed but never shipped; shipped: {shipped_revisions:?}"
        );
    }

    // …and the checkpoint catches up, so a failover would resume rather than
    // re-ship from zero.
    //
    // Polled rather than asserted outright, and the reason is the design under
    // test: the checkpoint is committed *after* the batch is on the wire, so
    // between the sink recording the last row and the checkpoint landing there
    // is a real window. That window is the at-least-once guarantee; a test that
    // asserted the checkpoint the instant the rows arrived would be asserting
    // exactly-once, which this feature deliberately does not provide.
    let want = *written.last().expect("a write");
    let checkpoint = wait_checkpoint(&cluster, want, CONVERGE_DEADLINE).await;
    assert!(
        checkpoint >= want,
        "the checkpoint must catch up to everything that shipped: {checkpoint} < {want}"
    );

    for task in tasks {
        task.abort();
    }
    cluster.shutdown_all().await;
}

/// AC2b: a leader kill mid-export. Duplicates are permitted **only** across the
/// failover boundary, and every row must still arrive at least once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_kill_duplicates_only_across_the_failover_boundary() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let first_leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    let sink = CountingSink::start(200).await;
    declare_sink(&cluster, &sink.uri()).await;
    let tasks = attach_exporters(&cluster);

    let mut written = Vec::new();
    for port in 19_200..19_206 {
        written.push(cluster.write_on_leader(port, "before-failover").await);
    }
    wait_rows(&sink, written.len(), Duration::from_secs(20)).await;

    cluster.kill(first_leader).await;
    let new_leader = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a new leader after the kill");
    assert_ne!(new_leader, first_leader, "leadership must actually move");

    // The surviving nodes already have exporters attached from `attach_exporters`.
    for port in 19_210..19_216 {
        written.push(cluster.write_on_leader(port, "after-failover").await);
    }
    let rows = wait_rows(&sink, written.len(), Duration::from_secs(30)).await;

    let distinct: BTreeSet<(u64, String)> = rows.iter().cloned().collect();
    let shipped_revisions: BTreeSet<u64> = distinct.iter().map(|(rev, _)| *rev).collect();

    // At-least-once: nothing is lost across the boundary.
    for revision in &written {
        assert!(
            shipped_revisions.contains(revision),
            "revision {revision} was committed but never shipped across the failover; \
             shipped: {shipped_revisions:?}"
        );
    }

    // The duplicate *set* is asserted, not hand-waved: every duplicate must be
    // dedupable by `(revision, op_id)` — i.e. a repeat of an identical pair,
    // never two different rows claiming the same revision.
    let mut counts: std::collections::BTreeMap<(u64, String), usize> =
        std::collections::BTreeMap::new();
    for row in &rows {
        *counts.entry(row.clone()).or_default() += 1;
    }
    let mut by_revision: std::collections::BTreeMap<u64, BTreeSet<String>> =
        std::collections::BTreeMap::new();
    for (revision, op_id) in &distinct {
        by_revision
            .entry(*revision)
            .or_default()
            .insert(op_id.clone());
    }
    for (revision, op_ids) in &by_revision {
        assert_eq!(
            op_ids.len(),
            1,
            "revision {revision} arrived with {} different op_ids, so `(revision, op_id)` \
             would not dedup it: {op_ids:?}",
            op_ids.len()
        );
    }
    let duplicated: Vec<_> = counts.iter().filter(|(_, n)| **n > 1).collect();
    assert!(
        duplicated.len() <= 50,
        "duplicates must be bounded by the in-flight batch (50 rows), got {}: {duplicated:?}",
        duplicated.len()
    );

    for task in tasks {
        task.abort();
    }
    cluster.shutdown_all().await;
}

/// AC3: a sink that returns 500 forever must not stall admin writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_permanently_failing_sink_does_not_stall_admin_writes() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    let sink = CountingSink::start(500).await;
    declare_sink(&cluster, &sink.uri()).await;
    let failures_before = counter_value("rift_cluster_audit_export_failures_total").unwrap_or(0.0);
    let (statuses, tasks) = attach_exporters_with_status(&cluster);

    // The write path must be entirely unaffected. Timed, because "does not
    // stall" is a latency claim: a write that eventually succeeds after the
    // exporter's backoff would satisfy a bare success assertion and still be
    // the bug.
    let start = Instant::now();
    let mut revisions = Vec::new();
    for port in 19_300..19_310 {
        revisions.push(cluster.write_on_leader(port, "still-writable").await);
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "10 admin writes took {elapsed:?} against a dead sink; the export path must never \
         appear in the write path"
    );

    // The fleet still converges, and the audit rows still exist locally.
    assert!(
        cluster
            .wait_converged(19_309, "still-writable", CONVERGE_DEADLINE)
            .await,
        "the fleet must stay writable and converge with the sink down"
    );

    // The sink was genuinely attempted and genuinely failed — otherwise this
    // test would pass just as well against an exporter that never ran.
    let start = Instant::now();
    while sink.request_count() == 0 && start.elapsed() < Duration::from_secs(20) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        sink.request_count() > 0,
        "the exporter must have attempted to ship; otherwise this proves nothing"
    );
    assert!(
        sink.rows().is_empty(),
        "a 500 must not be recorded as a successful ship"
    );

    // AC3's second half: the failure must be *visible*. Without these two
    // assertions this test passes against an exporter that swallowed the 500,
    // and against one that stopped trying after the first failure.
    let start = Instant::now();
    let mut observed = None;
    while start.elapsed() < Duration::from_secs(20) {
        let failing = statuses
            .iter()
            .map(|s| s.snapshot())
            .find(|s| s.consecutive_failures > 0);
        if let Some(snapshot) = failing {
            observed = Some(snapshot);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let observed = observed.expect(
        "some node's exporter must record the sink failure; a 500 that leaves \
         consecutive_failures at 0 is a swallowed error",
    );
    assert!(
        observed.last_error.is_some(),
        "a failing sink must leave a readable reason, not just a count: {observed:?}"
    );
    assert_eq!(
        observed.shipped_rows, 0,
        "nothing was accepted, so nothing may be counted as shipped: {observed:?}"
    );
    let failures_after = counter_value("rift_cluster_audit_export_failures_total")
        .expect("the failure counter family must exist once an export has been attempted");
    assert!(
        failures_after > failures_before,
        "rift_cluster_audit_export_failures_total must grow while the sink is down: \
         {failures_before} -> {failures_after}"
    );

    // Nothing was checkpointed: ship-then-checkpoint means a failed ship leaves
    // the checkpoint where it was, so the batch is retried rather than skipped.
    let leader = cluster.leader().expect("a leader");
    assert_eq!(
        leader.audit_checkpoint().expect("checkpoint"),
        0,
        "a failed ship must never advance the checkpoint — that would silently drop the batch"
    );
    assert!(!revisions.is_empty());

    for task in tasks {
        task.abort();
    }
    cluster.shutdown_all().await;
}

/// The monotonicity rule, asserted directly: a late checkpoint from a deposed
/// leader must not rewind the stream and re-ship a delivered window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_checkpoint_never_moves_backwards() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    submit_checkpoint(&cluster, 50).await;
    assert_eq!(
        cluster
            .leader()
            .expect("a leader")
            .audit_checkpoint()
            .expect("read"),
        50
    );

    submit_checkpoint(&cluster, 20).await;
    assert_eq!(
        cluster
            .leader()
            .expect("a leader")
            .audit_checkpoint()
            .expect("read"),
        50,
        "a late checkpoint from a deposed leader must be a no-op, not a rewind"
    );

    submit_checkpoint(&cluster, 70).await;
    assert_eq!(
        cluster
            .leader()
            .expect("a leader")
            .audit_checkpoint()
            .expect("read"),
        70,
        "forward progress must still be recorded"
    );

    // Every replica must agree — the `max` runs at apply, so this is a claim
    // about determinism, not just about the leader's copy.
    for member in &cluster.members {
        if let Some(node) = &member.node {
            let start = Instant::now();
            while node.audit_checkpoint().expect("read") != 70
                && start.elapsed() < CONVERGE_DEADLINE
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert_eq!(
                node.audit_checkpoint().expect("read"),
                70,
                "node {} disagrees about the checkpoint",
                member.id
            );
        }
    }

    cluster.shutdown_all().await;
}

/// The sink and checkpoint survive a node restart.
///
/// **Restart, not snapshot install** — the name said otherwise until it was
/// measured. openraft here runs the default `LogEntries(5000)` snapshot policy,
/// and this harness commits a few dozen entries, so no snapshot is ever built:
/// the restarted node restores from its own redb. The chaos README records the
/// same correction for C18 and C22. The snapshot round trip is gated in process
/// by `the_audit_export_sink_checkpoint_and_gc_watermark_survive_a_snapshot_install`
/// in `raft/store.rs`, which drives `build_snapshot`/`install_snapshot`
/// directly; what this scenario covers is durability across a process death.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sink_and_checkpoint_survive_a_node_restart() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    let sink = CountingSink::start(200).await;
    declare_sink(&cluster, &sink.uri()).await;
    let tasks = attach_exporters(&cluster);
    for port in 19_400..19_404 {
        cluster.write_on_leader(port, "snapshotted").await;
    }
    wait_rows(&sink, 4, Duration::from_secs(20)).await;
    for task in tasks {
        task.abort();
    }

    let expected_sink = cluster
        .leader()
        .expect("a leader")
        .audit_sink()
        .expect("read sink")
        .expect("a sink is declared");
    let expected_checkpoint = cluster
        .leader()
        .expect("a leader")
        .audit_checkpoint()
        .expect("read checkpoint");
    assert!(
        expected_checkpoint > 0,
        "the checkpoint must have advanced before this test means anything"
    );

    // Restart a follower: it reloads from its own persisted state machine, and
    // catches the rest up from the leader.
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
    // At least, not exactly: `task.abort()` above does not stop an exporter
    // mid-iteration, so one already in flight can still advance the checkpoint
    // after `expected_checkpoint` was sampled. The invariant here is that the
    // restart did not *lose* ground — an overshoot means more was exported and
    // less will be re-shipped, which is the safe direction. Demanding equality
    // made this fail under load with left > right, a passing state read as a
    // failure (seen at 13 vs 8).
    let start = Instant::now();
    while restarted.audit_checkpoint().expect("read") < expected_checkpoint
        && start.elapsed() < CONVERGE_DEADLINE
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        restarted.audit_sink().expect("read sink"),
        Some(expected_sink),
        "a node that came back without the sink record would stop exporting the moment it \
         won an election, silently"
    );
    assert!(
        restarted.audit_checkpoint().expect("read checkpoint") >= expected_checkpoint,
        "a node that came back without the checkpoint would re-ship the entire retained \
         history to the customer's bucket on its first election"
    );

    cluster.shutdown_all().await;
}

/// AC5: a backlog that ages past retention is counted and logged, never
/// silently dropped.
///
/// This test previously asserted only storage state and never attached an
/// exporter — so the counter and the error log it claims to cover were never
/// executed, and deleting both would have left it green. It now runs the real
/// exporter against a dead sink and asserts the counter moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backlog_aged_past_retention_is_counted_not_dropped() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start_with_audit_retention(3, 1).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    // A dead sink, so the backlog genuinely ages instead of being shipped.
    let dead = CountingSink::start(500).await;
    declare_sink(&cluster, &dead.uri()).await;
    let skipped_before =
        counter_value("rift_cluster_audit_export_skipped_revisions_total").unwrap_or(0.0);
    let (_statuses, tasks) = attach_exporters_with_status(&cluster);

    for port in 19_500..19_506 {
        cluster.write_on_leader(port, "will-age-out").await;
    }

    // Let the one-second retention window pass, then keep writing until GC has
    // actually run.
    //
    // **Two** writes are the minimum, and the reason is easy to get wrong:
    // `gc_audit` runs at the top of `apply`, before the batch's entries are
    // folded in, so it sees the logical clock as of the *previous* apply. One
    // write after the sleep therefore GCs against a clock that has not moved
    // yet and removes nothing. (The earlier version of this test asserted
    // `oldest > 1` as its proof that GC had run — which passes vacuously on
    // bootstrap revisions, so it proved nothing and hid exactly this.)
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut recent = 0;
    let mut port = 19_510;
    let start = Instant::now();
    while cluster
        .leader()
        .expect("a leader")
        .audit_gc_watermark()
        .expect("read watermark")
        == 0
        && start.elapsed() < Duration::from_secs(20)
    {
        recent = cluster.write_on_leader(port, "survives").await;
        port += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let leader = cluster.leader().expect("a leader");

    // GC recorded what it removed. This is the exporter's only evidence that
    // rows were *lost* rather than never written, and it must be replicated:
    // a node that forgot it would report a clean stream over a hole.
    let watermark = leader.audit_gc_watermark().expect("read watermark");
    assert!(
        watermark > 0,
        "retention must actually have removed rows for this test to mean anything"
    );

    let surviving = leader.audit_since(0, None, 10_000).expect("read audit");
    let oldest = surviving
        .first()
        .expect("at least the recent write survives")
        .revision;
    assert!(
        oldest > watermark,
        "everything at or below the watermark was removed, so the oldest survivor must sit \
         above it: oldest={oldest} watermark={watermark}"
    );
    assert!(
        surviving.iter().any(|r| r.revision == recent),
        "the recent write must survive its own retention window"
    );
    for member in &cluster.members {
        if let Some(node) = &member.node {
            let start = Instant::now();
            while node.audit_gc_watermark().expect("read") != watermark
                && start.elapsed() < CONVERGE_DEADLINE
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert_eq!(
                node.audit_gc_watermark().expect("read"),
                watermark,
                "node {} disagrees about how far retention has reached",
                member.id
            );
            let rows = node.audit_since(0, None, 10_000).expect("read audit");
            assert_eq!(
                rows.first().map(|r| r.revision),
                Some(oldest),
                "every replica must drop the same rows; node {} disagrees",
                member.id
            );
        }
    }

    // The assertion this test exists for: the loss is COUNTED, not passed over.
    let start = Instant::now();
    let mut skipped_after = skipped_before;
    while start.elapsed() < Duration::from_secs(20) {
        skipped_after =
            counter_value("rift_cluster_audit_export_skipped_revisions_total").unwrap_or(0.0);
        if skipped_after > skipped_before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        skipped_after > skipped_before,
        "rows aged out before the sink accepted them, so \
         rift_cluster_audit_export_skipped_revisions_total must grow: \
         {skipped_before} -> {skipped_after}. A silent gap in an exported audit trail is the \
         worst failure this feature has."
    );

    for task in tasks {
        task.abort();
    }
    cluster.shutdown_all().await;
}

/// The counterpart to the test above, and the one that matters more in
/// practice: a **healthy** fleet must never report a gap.
///
/// The first implementation derived loss from revision arithmetic
/// (`first.revision > checkpoint + 1`). That fires constantly in steady state,
/// because the exporter's own unaudited `AuditCheckpointPut` — plus every
/// election's blank entry and every membership change — consumes a revision
/// without producing an audit row. Ship a batch, let the checkpoint land, write
/// once more, and the next pass would claim permanent data loss on a cluster
/// that had lost nothing. That turns the one counter operators are told to
/// alert on into a rising false positive, which is worse than not having it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_healthy_fleet_never_reports_a_retention_gap() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    let sink = CountingSink::start(200).await;
    declare_sink(&cluster, &sink.uri()).await;
    let skipped_before =
        counter_value("rift_cluster_audit_export_skipped_revisions_total").unwrap_or(0.0);
    let tasks = attach_exporters(&cluster);

    // Ship a batch and let its checkpoint commit — the checkpoint op itself
    // takes a revision and writes no audit row, which is the trap.
    let first = cluster.write_on_leader(19_700, "healthy-one").await;
    wait_rows(&sink, 1, Duration::from_secs(20)).await;
    wait_checkpoint(&cluster, first, CONVERGE_DEADLINE).await;

    // Now write again, across the gap the checkpoint entry left.
    let second = cluster.write_on_leader(19_701, "healthy-two").await;
    wait_rows(&sink, 2, Duration::from_secs(20)).await;
    wait_checkpoint(&cluster, second, CONVERGE_DEADLINE).await;

    // And once more, so at least two checkpoint entries sit between audited ops.
    let third = cluster.write_on_leader(19_702, "healthy-three").await;
    wait_rows(&sink, 3, Duration::from_secs(20)).await;
    wait_checkpoint(&cluster, third, CONVERGE_DEADLINE).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let skipped_after =
        counter_value("rift_cluster_audit_export_skipped_revisions_total").unwrap_or(0.0);
    assert_eq!(
        skipped_after, skipped_before,
        "nothing aged out and nothing was lost, so the skipped-revisions counter must not \
         move: {skipped_before} -> {skipped_after}. Retention GC never ran here; any increase \
         is the counter reporting ordinary unaudited revisions as permanent data loss."
    );
    assert_eq!(
        cluster
            .leader()
            .expect("a leader")
            .audit_gc_watermark()
            .expect("read watermark"),
        0,
        "no GC ran, so the watermark must still be zero"
    );

    for task in tasks {
        task.abort();
    }
    cluster.shutdown_all().await;
}

/// The export loop must keep going, batch after batch — not ship once and park.
///
/// Every other scenario here writes fewer rows than one batch holds, so a loop
/// that ran exactly once would satisfy all of them. This one sets the batch to
/// three rows and writes well past that, so it fails unless the loop iterates,
/// re-reads from the advanced checkpoint, and ships the remainder.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_exporter_ships_batch_after_batch_rather_than_once() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("a leader");

    let sink = CountingSink::start(200).await;
    declare_sink_with_batch(&cluster, &sink.uri(), 3).await;
    let tasks = attach_exporters(&cluster);

    let mut written = Vec::new();
    for port in 19_600..19_612 {
        written.push(cluster.write_on_leader(port, "batched").await);
    }

    let rows = wait_rows(&sink, written.len(), Duration::from_secs(30)).await;
    let shipped: BTreeSet<u64> = rows.iter().map(|(rev, _)| *rev).collect();
    for revision in &written {
        assert!(
            shipped.contains(revision),
            "revision {revision} never shipped; a loop that runs once would stop after the \
             first {} rows. shipped: {shipped:?}",
            3
        );
    }
    assert!(
        sink.request_count() >= 4,
        "12+ rows at 3 per batch must take at least 4 requests, got {}",
        sink.request_count()
    );

    // The checkpoint must have followed the last batch, not the first.
    let want = *written.last().expect("a write");
    let checkpoint = wait_checkpoint(&cluster, want, CONVERGE_DEADLINE).await;
    assert!(
        checkpoint >= want,
        "the checkpoint must advance with every batch: {checkpoint} < {want}"
    );

    for task in tasks {
        task.abort();
    }
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
