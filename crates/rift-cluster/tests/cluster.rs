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
use std::time::{Duration, Instant};

use rift_cluster::{NodeConfig, NodeError, NodeId, RaftNode};
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
    node: Option<RaftNode>,
}

async fn spawn(id: NodeId, addr: SocketAddr, dir: &Path) -> RaftNode {
    let config = NodeConfig {
        node_id: id,
        bind: addr,
        advertise: Some(addr),
        data_dir: dir.to_path_buf(),
        secret: Some(SECRET.to_owned()),
    };
    // A restart can momentarily race the previous instance's async teardown
    // releasing the redb file lock: a real process restart frees it on exit, but
    // in-process the old node's Raft core drops its storage a beat after
    // `shutdown()` returns. Retry briefly on exactly that transient rather than
    // treating a test artifact as a failure.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match RaftNode::start(config.clone()).await {
            Ok(node) => return node,
            Err(e) if is_lock_contention(&e) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("start node {id}: {e}"),
        }
    }
}

/// Whether a start failure is the transient redb file lock still held by a
/// just-stopped previous instance (see [`spawn`]).
fn is_lock_contention(e: &NodeError) -> bool {
    matches!(e, NodeError::Storage(m) if m.contains("already open") || m.contains("acquire lock"))
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

        let seed = members[0].addr;
        for member in members.iter_mut().skip(1) {
            let node = spawn(member.id, member.addr, member.dir.path()).await;
            node.join_via(seed)
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
        self.members.iter().filter_map(|m| m.node.as_ref())
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

    /// Poll, bounded, until every live node's applied config for `port` equals
    /// `want`. Returns false on timeout (never a synthetic pass).
    async fn wait_converged(&self, port: u16, want: &str, deadline: Duration) -> bool {
        let start = Instant::now();
        loop {
            let mut live = self.live().peekable();
            let converged = live.peek().is_some()
                && live
                    .all(|n| n.get_imposter(port).expect("read config").as_deref() == Some(want));
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

    /// Write `body` for `port` on the current leader, returning its revision.
    async fn write_on_leader(&self, port: u16, body: &str) -> u64 {
        let leader = self.leader().expect("a leader to accept the write");
        leader
            .put_imposter(port, body.to_owned())
            .await
            .expect("leader commits the write")
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
