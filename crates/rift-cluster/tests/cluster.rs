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
    // Most of these tests drive `build_snapshot`/`install_snapshot` directly, so they need no help
    // provoking one. The exception is #428's catch-up test, which needs a real snapshot to cross
    // the wire and so passes the knob explicitly.
    spawn_with_snapshot_policy(id, addr, dir, audit_retention_secs, None).await
}

async fn spawn_with_snapshot_policy(
    id: NodeId,
    addr: SocketAddr,
    dir: &Path,
    audit_retention_secs: u64,
    snapshot_log_entries: Option<u64>,
) -> Arc<RaftNode> {
    spawn_full(
        id,
        addr,
        dir,
        audit_retention_secs,
        snapshot_log_entries,
        false,
    )
    .await
}

/// [`spawn_with_snapshot_policy`] plus the #481 capability knob: `advertise_as_digest_only_incapable`
/// makes this one node's blob route claim it cannot apply a digest-only `ControlOp`, pretending
/// to be a pre-#481 build — the only way this in-process harness can put a version-skewed member
/// in a cluster without two binary versions.
async fn spawn_full(
    id: NodeId,
    addr: SocketAddr,
    dir: &Path,
    audit_retention_secs: u64,
    snapshot_log_entries: Option<u64>,
    advertise_as_digest_only_incapable: bool,
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
        snapshot_log_entries,
        advertise_as_digest_only_incapable,
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
        Self::start_full(n, audit_retention_secs, None, None).await
    }

    /// [`Self::start`] with every node snapshotting every `entries` log entries and purging to
    /// the tip, so a member that falls behind must be caught up by `install_snapshot`.
    async fn start_with_snapshots(n: usize, entries: u64) -> Self {
        Self::start_full(
            n,
            rift_cluster::DEFAULT_AUDIT_RETENTION_SECS,
            Some(entries),
            None,
        )
        .await
    }

    /// [`Self::start`] with `incapable`'s blob route advertising `applies_digest_only: false`
    /// (#481) — that one member's build pretends to predate D-49, the digest-only `ControlOp`
    /// shape. The only way this in-process harness puts a version-skewed member in a cluster
    /// without standing up two binary versions.
    async fn start_with_one_member_digest_only_incapable(n: usize, incapable: NodeId) -> Self {
        Self::start_full(
            n,
            rift_cluster::DEFAULT_AUDIT_RETENTION_SECS,
            None,
            Some(incapable),
        )
        .await
    }

    async fn start_full(
        n: usize,
        audit_retention_secs: u64,
        snapshot_log_entries: Option<u64>,
        digest_only_incapable: Option<NodeId>,
    ) -> Self {
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

        let n1 = spawn_full(
            members[0].id,
            members[0].addr,
            members[0].dir.path(),
            audit_retention_secs,
            snapshot_log_entries,
            digest_only_incapable == Some(members[0].id),
        )
        .await;
        n1.cluster_init().await.expect("bootstrap node 1");
        members[0].node = Some(n1);

        let seed = Authority::from(members[0].addr);
        for member in members.iter_mut().skip(1) {
            let node = spawn_full(
                member.id,
                member.addr,
                member.dir.path(),
                audit_retention_secs,
                snapshot_log_entries,
                digest_only_incapable == Some(member.id),
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
        let node = spawn_with_snapshot_policy(
            mid,
            addr,
            &dir,
            self.audit_retention_secs,
            self.snapshot_log_entries,
        )
        .await;
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
/// Wait until every revision in `want` has reached the sink.
///
/// Not [`wait_rows`] with `want.len()`: the sink receives a row for *every*
/// auditable control op, including ones this test did not write — declaring the
/// sink itself, and whatever the engine happens to do at startup. Counting rows
/// therefore returns as soon as `n` arrive, which may be `n` rows that are not
/// the `n` this test is about, and the assertion that follows then reports a
/// committed-but-unshipped revision that was merely still in flight. Waiting for
/// the specific revisions makes the test immune to an unrelated op appearing
/// before them — which is exactly what an upstream change did (vendor bump to
/// `4b4f841`: one extra revision is consumed before the writes, so the first six
/// rows started one too early).
async fn wait_for_revisions(
    sink: &CountingSink,
    want: &[u64],
    deadline: Duration,
) -> Vec<(u64, String)> {
    let start = Instant::now();
    loop {
        let rows = sink.rows();
        let shipped: BTreeSet<u64> = rows.iter().map(|(rev, _)| *rev).collect();
        if want.iter().all(|rev| shipped.contains(rev)) || start.elapsed() > deadline {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

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

    let rows = wait_for_revisions(&sink, &written, Duration::from_secs(20)).await;
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
    wait_for_revisions(&sink, &written, Duration::from_secs(20)).await;

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
    let rows = wait_for_revisions(&sink, &written, Duration::from_secs(30)).await;

    let distinct: BTreeSet<(u64, String)> = rows.iter().cloned().collect();
    let shipped_revisions: BTreeSet<u64> = distinct.iter().map(|(rev, _)| *rev).collect();

    // At-least-once: nothing is lost across the boundary.
    for revision in &written {
        assert!(
            shipped_revisions.contains(revision),
            "revision {revision} was committed but never shipped across the failover. \
             missing: {:?}; written: {written:?}; shipped: {shipped_revisions:?}",
            written
                .iter()
                .filter(|r| !shipped_revisions.contains(r))
                .collect::<Vec<_>>()
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
    let mut written = Vec::new();
    for port in 19_400..19_404 {
        written.push(cluster.write_on_leader(port, "snapshotted").await);
    }
    wait_for_revisions(&sink, &written, Duration::from_secs(20)).await;

    // Rows at the sink do **not** mean the checkpoint has advanced: the exporter
    // ships first and checkpoints second, and the checkpoint is a replicated
    // `AuditCheckpointPut` — a whole consensus round after the bytes leave. So
    // wait for it before aborting the exporters, or the abort freezes it at
    // whatever it happened to be. Measured on this tree before the fix: the
    // checkpoint read **0** at the moment the old row-count wait returned, on 2 of
    // 3 local runs, which is exactly the `must have advanced` failure (#495).
    let last_written = *written.iter().max().expect("the test wrote something");
    let expected_checkpoint = wait_checkpoint(&cluster, last_written, CONVERGE_DEADLINE).await;
    assert!(
        expected_checkpoint >= last_written,
        "the exporter never checkpointed the rows it shipped within {CONVERGE_DEADLINE:?} — \
         nothing below can be measured until it has: checkpoint={expected_checkpoint} \
         last_written={last_written} written={written:?} sink_rows={}",
        sink.rows().len()
    );

    for task in tasks {
        task.abort();
    }

    let expected_sink = cluster
        .leader()
        .expect("a leader")
        .audit_sink()
        .expect("read sink")
        .expect("a sink is declared");

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

/// The fleet's name survives a process death (issue #373).
///
/// **Restart, not snapshot install** — the same correction this file already records for the
/// audit sink and checkpoint above, and for chaos C18/C22. `snapshot_round_trips_the_fleet_name`
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

    // The revisions written, not a row count: `wait_rows` returns as soon as *any* twelve
    // rows arrive, and under load those can be twelve that are not these (the same shape
    // `every_audit_row_reaches_the_sink_exactly_once` was cured of at the `4b4f841` bump).
    let rows = wait_for_revisions(&sink, &written, Duration::from_secs(30)).await;
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

// -- datasets on the control plane (RFC-005 D1, issue #285) --------------------------------------

fn dataset_digest_hex(csv: &str) -> String {
    use sha2::{Digest as _, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(csv.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// A truthful `DatasetPut` for `csv`, keyed on its first column, under the default tenant.
fn dataset_put(name: &str, csv: &str) -> ControlRequest {
    let mut lines = csv.lines();
    let columns: Vec<String> = lines
        .next()
        .unwrap_or_default()
        .split(',')
        .map(|c| c.trim().to_owned())
        .collect();
    let rows = lines.count() as u64;
    ControlRequest {
        op_id: uuid::Uuid::new_v4(),
        principal: None,
        issued_at_secs: 0,
        expected_revision: None,
        op: rift_cluster::ControlOp::DatasetPut {
            tenant: rift_cluster::TenantId::default(),
            record: rift_cluster::control::DatasetRecord {
                name: name.to_owned(),
                digest: rift_cluster::control::Digest::new(dataset_digest_hex(csv)),
                key_columns: vec![columns[0].clone()],
                delimiter: ',',
                columns,
                rows,
                bytes: csv.len() as u64,
            },
            csv: Some(csv.to_owned()),
            origin: 0,
        },
    }
}

fn spool_file(dir: &Path, csv: &str) -> std::path::PathBuf {
    dir.join("datasets")
        .join(format!("{}.csv", dataset_digest_hex(csv)))
}

/// A CSV of at least `bytes` bytes with a unique first column — the quota-ceiling payload the
/// RFC §11 check wants openraft/redb exercised at.
fn big_csv(bytes: usize) -> String {
    tagged_csv(bytes, "")
}

/// Put `csv`'s bytes into `node`'s blob transport store, mirroring the fan-out the production
/// admin-front write path (`fan_out_then_submit`, D-49) performs before proposing — which the
/// low-level `submit` these harness tests use deliberately bypasses.
///
/// Manifest-snapshot catch-up (#440, D-50) fetches each referenced blob from a holder's transport
/// store rather than carrying the bytes in the snapshot, so a holder must exist for the joiner to
/// fetch from. Production guarantees one on a quorum via fan-out (D-18); a `submit`-driven test
/// never fans out, so it establishes the same precondition directly on the accepting node.
fn seed_blob_store(node: &RaftNode, csv: &str) {
    let digest = rift_cluster::blobs::digest_of_bytes(csv.as_bytes());
    node.blobs()
        .store_whole(&digest, csv.as_bytes())
        .expect("seed the accepting node's blob transport store");
}

/// Issue #285: the bytes ride the log, so once the leader's write barrier has answered, every
/// member holds the spool file — byte-identical to the upload — with no fetch and no readiness
/// handshake. This is the fleet-level "on disk before the 2xx" the front's barrier will surface.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dataset_upload_is_byte_identical_on_every_node_before_the_write_returns() {
    let _serial = TEST_LOCK.lock().await;
    let cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let csv = "id,name,tier\n1,ada,gold\n2,bob,silver\n";

    let leader = cluster.leader().expect("leader");
    let response = leader
        .submit(dataset_put("customers", csv))
        .await
        .expect("the put commits");
    assert_eq!(response.outcome, rift_cluster::ControlOutcome::Applied);
    let unapplied = leader
        .await_applied(response.revision, CONVERGE_DEADLINE)
        .await;
    assert!(
        unapplied.is_empty(),
        "the barrier must reach every member: {unapplied:?}"
    );

    for member in &cluster.members {
        let path = spool_file(member.dir.path(), csv);
        let on_disk = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("node {} has no spool file at {path:?}: {e}", member.id));
        assert_eq!(
            on_disk,
            csv.as_bytes(),
            "node {} holds different bytes",
            member.id
        );
        let node = member.node.as_ref().expect("live");
        let record = node
            .dataset(DEFAULT_TENANT, "customers")
            .expect("read")
            .expect("every node lists the dataset");
        assert_eq!(record.version, 1);
        assert_eq!(record.digest, dataset_digest_hex(csv));
        assert_eq!(
            node.spool_path(&record.digest),
            Some(path),
            "the node names the file it holds"
        );
        let listed = node.datasets(DEFAULT_TENANT).expect("list");
        assert_eq!(
            listed
                .iter()
                .map(|d| (d.name.as_str(), d.version))
                .collect::<Vec<_>>(),
            [("customers", 1)],
            "node {} lists exactly the one dataset",
            member.id
        );
    }

    let mut cluster = cluster;
    cluster.shutdown_all().await;
}

/// Issue #285: an upload commits, converges, and survives every node going down and coming
/// back. One member also loses its spool directory while down, and gets the file back from its
/// own state machine on restart — never from a peer. Parameterised on the payload size because of
/// #411 (see the two callers below).
async fn dataset_survives_a_full_cluster_restart_and_a_lost_spool(bytes: usize) {
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let csv = big_csv(bytes);

    let leader = cluster.leader().expect("leader");
    let response = leader
        .submit(dataset_put("big", &csv))
        .await
        .expect("the entry commits");
    assert_eq!(response.outcome, rift_cluster::ControlOutcome::Applied);
    let unapplied = leader
        .await_applied(response.revision, Duration::from_secs(30))
        .await;
    assert!(unapplied.is_empty(), "{unapplied:?}");
    for member in &cluster.members {
        assert_eq!(
            std::fs::read(spool_file(member.dir.path(), &csv))
                .expect("spool file")
                .len(),
            csv.len(),
            "node {} spool",
            member.id
        );
    }

    let ids: Vec<NodeId> = cluster.members.iter().map(|m| m.id).collect();
    let victim = ids[ids.len() - 1];
    cluster.shutdown_all().await;
    // A node that lost its derived state: only the spool directory, never the state machine.
    std::fs::remove_dir_all(cluster.member(victim).dir.path().join("datasets"))
        .expect("wipe the victim's spool dir");
    for id in &ids {
        cluster.restart(*id).await;
    }
    assert!(
        cluster.wait_for_leader(LEADER_DEADLINE).await.is_some(),
        "no leader after the cold restart"
    );
    // Startup repair runs from `reconcile_engine`, which the composed server calls on start; the
    // in-process harness has no composition, so call it the way `compose` does.
    for member in &cluster.members {
        member
            .node
            .as_ref()
            .expect("live")
            .reconcile_engine()
            .await
            .expect("reconcile");
    }
    for member in &cluster.members {
        let on_disk = std::fs::read(spool_file(member.dir.path(), &csv)).unwrap_or_else(|e| {
            panic!(
                "node {} lost its spool file across the restart: {e}",
                member.id
            )
        });
        assert_eq!(on_disk.len(), csv.len(), "node {}", member.id);
        assert_eq!(
            on_disk[..64],
            csv.as_bytes()[..64],
            "node {} bytes differ",
            member.id
        );
        let record = member
            .node
            .as_ref()
            .expect("live")
            .dataset(DEFAULT_TENANT, "big")
            .expect("read")
            .expect("the record survived");
        assert_eq!(record.bytes, csv.len() as u64);
    }
    cluster.shutdown_all().await;
}

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
    let csv = big_csv(8 * 1024 * 1024 - 1_100);
    // The outcome of the write is deliberately not asserted: under this load it may park or
    // fail with "not the leader" until #431 lands. The panic count is the claim.
    let _ = tokio::time::timeout(
        Duration::from_secs(30),
        cluster
            .leader()
            .expect("leader")
            .submit(dataset_put("big", &csv)),
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

    let csv = big_csv(512 * 1024);
    let r = cluster
        .leader()
        .expect("leader")
        .submit(dataset_put("d0", &csv))
        .await
        .expect("d0");
    assert_eq!(r.outcome, rift_cluster::ControlOutcome::Applied);
    // d0..d8 are all the same bytes (one digest), so one seed on the leader covers the whole
    // manifest the restarted voter will fetch on install (D-50); production's fan-out would have
    // left this holder behind.
    seed_blob_store(cluster.leader().expect("leader"), &csv);
    cluster.kill(victim).await;

    for i in 1..=8 {
        let r = cluster
            .leader()
            .expect("leader")
            .submit(dataset_put(&format!("d{i}"), &csv))
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

/// #411's other shipped ceiling: a spec document at the RFC-004 S2 maximum (4 MiB, #278) is one
/// log entry and must commit on a real 3-node cluster.
///
/// The dataset case below proves the 8 MiB path; this proves the other quota the front already
/// accepts. Both were validated, accepted, and then unable to commit — openraft dropped every
/// AppendEntries attempt at the 50 ms `heartbeat_interval` and restarted the body, so the fleet's
/// true ceiling was "whatever replicates in one heartbeat" (a few hundred KiB on loopback)
/// rather than either documented quota.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_four_mebibyte_spec_document_commits_on_a_three_node_cluster() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    // At the ceiling, not merely near it: `MAX_SPEC_BYTES` is inclusive, and the point is that
    // the largest document the front accepts is the largest the log can carry.
    let document = big_spec_document(rift_cluster::control::MAX_SPEC_BYTES);
    assert_eq!(
        document.len(),
        rift_cluster::control::MAX_SPEC_BYTES,
        "the document must sit exactly on the documented ceiling"
    );

    let leader = cluster.leader().expect("leader");
    let response = leader
        .submit(spec_put("big-spec", &document))
        .await
        .expect("a 4 MiB spec entry commits");
    assert_eq!(response.outcome, rift_cluster::ControlOutcome::Applied);
    let unapplied = leader
        .await_applied(response.revision, Duration::from_secs(30))
        .await;
    assert!(unapplied.is_empty(), "{unapplied:?}");

    // Every member, not just the leader: the whole claim is that the bytes rode the log.
    for member in &cluster.members {
        let record = member
            .node
            .as_ref()
            .expect("live")
            .spec(DEFAULT_TENANT, "big-spec")
            .expect("read")
            .expect("the spec record replicated");
        assert_eq!(
            record.digest,
            dataset_digest_hex(&document),
            "node {} must hold the document's own digest",
            member.id
        );
    }
    cluster.shutdown_all().await;
}

/// A spec document of exactly `bytes` bytes. Content is irrelevant to this crate — it stores
/// bytes and a digest and never parses the document — so this pads to an exact length rather
/// than pretending to be a valid OpenAPI file.
fn big_spec_document(bytes: usize) -> String {
    let head = "openapi: 3.0.0\ninfo:\n  title: big\n  version: 1.0.0\npaths: {}\n# ";
    let mut doc = String::with_capacity(bytes);
    doc.push_str(head);
    while doc.len() < bytes {
        doc.push('x');
    }
    doc.truncate(bytes);
    doc
}

/// A truthful `SpecPut` for `document` under the default tenant.
fn spec_put(id: &str, document: &str) -> ControlRequest {
    ControlRequest {
        op_id: uuid::Uuid::new_v4(),
        principal: None,
        issued_at_secs: 0,
        expected_revision: None,
        op: rift_cluster::ControlOp::SpecPut {
            tenant: rift_cluster::TenantId::default(),
            id: id.to_owned(),
            meta: rift_cluster::control::SpecMeta {
                format: rift_cluster::control::SpecFormat::Yaml,
                digest: rift_cluster::control::Digest::new(dataset_digest_hex(document)),
                source: rift_cluster::control::SpecSource::Inline,
                size: document.len() as u64,
            },
            document: Some(document.to_owned()),
            origin: 0,
        },
    }
}

/// The restart + lost-spool proof at a small size (128 KiB), kept as the fast sibling of the
/// 8 MiB case below — the two differ only in payload, so a failure in one and not the other
/// points straight at size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dataset_survives_a_full_cluster_restart_and_a_lost_spool() {
    let _serial = TEST_LOCK.lock().await;
    dataset_survives_a_full_cluster_restart_and_a_lost_spool(128 * 1024).await;
}

/// RFC-005 §11's check: an upload at the per-dataset ceiling (8 MiB) is one log entry, and the
/// fleet must be comfortable with it.
///
/// #411's half of this is done: openraft 0.9 bounds every AppendEntries RPC by
/// `heartbeat_interval` (50 ms here) and drops the future when that fires, so an entry that could
/// not transfer and fsync inside one round trip restarted from byte 0 forever — ≥1 MiB never
/// committed and ~512 KiB took 23-548 s. The transfer is now single-flighted in the network
/// adapter, so it outlives the RPC deadline and a re-send attaches to it rather than restarting
/// it, with the timers untouched. Unloaded, this test passes in ~9.4 s.
///
/// It was `#[ignore]`d for a *different* and pre-existing reason (#430), now closed:
/// `try_get_log_entries` could be asked for a range whose entries openraft had counted but
/// `append` had not yet made readable; it answered with an empty vec, and openraft 0.9.24 did
/// `logs.first().….unwrap()` on that (`replication/mod.rs:399`), panicking the replication
/// workers and costing the leader its leadership. The window scales with entry size, so #411 is
/// what made it reachable. #446 fixed that; #449 then fixed the restarted-voter livelock (#431)
/// that kept the write failing afterwards. Both issues are closed, so the attribute is gone and
/// this runs.
///
/// It is the end-to-end gate for the #411 → #430 → #431 ladder, and the only test that asserts
/// the whole claim: an 8 MiB dataset commits, survives a full-cluster restart, and is rebuilt
/// after its spool is lost. What #446 shipped alongside it is deliberately narrower — it counts
/// panics located in openraft's replication module and does not assert the write's own result.
/// The 4 MiB `SpecPut` case below is #411's own CI-green proof.
///
/// Read its verdict off CI, not off a developer machine. The failure this guards against was
/// load-dependent: it reproduced on GitHub's 2-vCPU runners and never once locally, unloaded or
/// loaded, on either openraft version. A local pass says the blocker is cleared; it is not
/// evidence the test is CI-stable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_eight_mebibyte_dataset_survives_a_full_cluster_restart_and_a_lost_spool() {
    let _serial = TEST_LOCK.lock().await;
    let bytes = 8 * 1024 * 1024 - 1_100;
    let csv = big_csv(bytes);
    assert!(
        csv.len() <= 8 * 1024 * 1024,
        "at or under the default ceiling: {}",
        csv.len()
    );
    assert!(
        csv.len() > 8 * 1024 * 1024 - 2_200,
        "close to the ceiling: {}",
        csv.len()
    );
    dataset_survives_a_full_cluster_restart_and_a_lost_spool(bytes).await;
}

// ---- #438: quorum blob fan-out before propose ---------------------------

/// Pins D-18: completeness is established by the write path — the fan-out puts
/// the blob on every member *before* the referencing op is proposed, so a commit
/// implies quorum-durability and the origin dying after it leaves holders behind.
///
/// Acceptance 1. The accepting node stores the blob and every other member ends
/// up holding it, so a commit implies the blob is quorum-durable and the origin
/// dying afterwards does not matter.
///
/// Asserted on `bytes_sent` rather than on the return being `Ok`: a fan-out that
/// silently sent nothing and one that moved the whole payload are
/// indistinguishable from a status, and only the byte count separates them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blob_fanned_out_before_propose_reaches_every_member() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    let csv = "id,name\n1,ada\n2,bob\n";
    let digest = rift_cluster::blobs::digest_of_bytes(csv.as_bytes());
    let leader = cluster.leader().expect("leader");

    let (outcome, _pin) = leader
        .fan_out_blob(&digest, csv.as_bytes())
        .await
        .expect("fan-out");

    assert!(
        outcome.quorum,
        "3 live voters must be a joint-consensus quorum"
    );
    assert!(!outcome.joint, "no membership change is in flight");
    assert_eq!(
        outcome.acks.len(),
        3,
        "every member acked: {:?}",
        outcome.acks
    );
    assert!(outcome.skewed.is_empty());
    assert!(
        outcome.bytes_sent > 0,
        "the fan-out must actually put bytes on the wire, not merely return Ok"
    );

    for node in cluster.live() {
        assert!(
            node.blobs().stat(&digest).expect("stat").have,
            "node {} does not hold the fanned-out blob",
            node.id()
        );
    }

    // Released here as the real write path releases it: once the op that
    // references the blob has committed.
    drop(_pin);
    cluster.shutdown_all().await;
}

/// Pins D-19: with 1 of 3 voters holding the blob there is no majority of either
/// configuration, so the fan-out reports no quorum and the write parks.
///
/// Acceptance 2. With only one reachable peer there is no majority of either
/// configuration, so the fan-out reports no quorum and the write path parks
/// rather than committing an op whose blob is on one node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fan_out_without_a_reachable_quorum_reports_no_quorum() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    let leader_id = cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    // Kill both peers, leaving the accepting node alone in a 3-voter config.
    for id in [1, 2, 3] {
        if id != leader_id {
            cluster.kill(id).await;
        }
    }

    let csv = "id,name\n1,ada\n";
    let digest = rift_cluster::blobs::digest_of_bytes(csv.as_bytes());
    let leader = cluster
        .member(leader_id)
        .node
        .as_ref()
        .expect("leader node");

    let (outcome, _pin) = leader
        .fan_out_blob(&digest, csv.as_bytes())
        .await
        .expect("fan-out completes even when peers are unreachable");

    assert!(
        !outcome.quorum,
        "1 of 3 voters is not a majority; acks were {:?}",
        outcome.acks
    );
    assert_eq!(
        outcome.acks,
        BTreeSet::from([leader_id]),
        "only the accepting node holds it"
    );
    // The blob is still stored locally — the shortfall is about durability, not
    // about the write having been lost. That is what makes the parked intent
    // replayable rather than a dead end.
    assert!(leader.blobs().stat(&digest).expect("stat").have);

    drop(_pin);
    cluster.shutdown_all().await;
}

/// The replay path's precondition: re-running a fan-out after a shortfall must
/// be idempotent, or the 503-and-replay contract would re-send the whole payload
/// on every attempt. `BlobTransfer::put` probes `stat` first, so a peer that
/// already holds the digest costs one round trip and no bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn re_running_a_fan_out_sends_no_bytes_to_a_peer_that_already_has_it() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    let csv = "id,name\n1,ada\n2,bob\n3,cleo\n";
    let digest = rift_cluster::blobs::digest_of_bytes(csv.as_bytes());
    let leader = cluster.leader().expect("leader");

    let (first, first_pin) = leader
        .fan_out_blob(&digest, csv.as_bytes())
        .await
        .expect("first fan-out");
    assert!(first.bytes_sent > 0);
    // Release it, so the second call exercises a fresh pin rather than
    // inheriting this one's protection.
    drop(first_pin);

    let (second, _pin) = leader
        .fan_out_blob(&digest, csv.as_bytes())
        .await
        .expect("second fan-out");

    assert!(second.quorum, "the peers still hold it");
    assert_eq!(
        second.bytes_sent, 0,
        "a replayed fan-out must re-send nothing"
    );

    drop(_pin);
    cluster.shutdown_all().await;
}

/// The pin a fan-out returns must outlive the fan-out itself, because the window
/// it guards runs from "a quorum holds it" to "the op that references it
/// commits" — and the submit happens after the fan-out returns. A pin released
/// on return would leave the blob unpinned *and* unreferenced for exactly that
/// stretch.
///
/// Regression test for a real defect in this change's first draft, which dropped
/// the guard inside `fan_out_blob`. It survived the other tests because the
/// grace window is measured from the blob's mtime and a freshly-written blob is
/// young — so the bug was invisible until the blob was older than the grace,
/// which is precisely the replayed-intent case this issue's 503 contract
/// creates. Asserting on GC directly is what makes it visible now.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pin_a_fan_out_returns_protects_the_blob_until_it_is_dropped() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    let csv = "id,name\n1,ada\n2,bob\n3,cleo\n4,dai\n";
    let digest = rift_cluster::blobs::digest_of_bytes(csv.as_bytes());
    let leader = cluster.leader().expect("leader");

    let (outcome, pin) = leader
        .fan_out_blob(&digest, csv.as_bytes())
        .await
        .expect("fan-out");
    assert!(outcome.quorum);

    // Nothing references the digest yet — no op has been proposed — and `now` is
    // far past any grace, so the pin is the only thing that can save it.
    let unreferenced = std::collections::HashSet::new();
    let far_future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
        + 10_000_000;

    let outcome = leader
        .blobs()
        .gc(
            &unreferenced,
            &std::collections::HashMap::new(),
            0,
            0,
            far_future,
            3600,
        )
        .expect("gc");
    assert_eq!(
        outcome.removed, 0,
        "the returned pin must still be protecting it"
    );
    assert!(leader.blobs().stat(&digest).expect("stat").have);

    drop(pin);
    let outcome = leader
        .blobs()
        .gc(
            &unreferenced,
            &std::collections::HashMap::new(),
            0,
            0,
            far_future,
            3600,
        )
        .expect("gc");
    assert_eq!(
        outcome.removed, 1,
        "released, it is collectable like any other blob"
    );

    cluster.shutdown_all().await;
}

/// Acceptance 3. An 8 MiB dataset — the documented per-dataset ceiling — fans
/// out to a 3-node cluster on loopback, and the elapsed time is recorded.
///
/// Read the number off CI, not off a developer machine: the transfer is 4 MiB-
/// chunked, so this is round-trip-bound rather than bandwidth-bound, and a
/// 2-vCPU runner is the honest measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_eight_mebibyte_blob_fans_out_to_three_nodes() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    let csv = big_csv(8 * 1024 * 1024 - 1_100);
    let digest = rift_cluster::blobs::digest_of_bytes(csv.as_bytes());
    let leader = cluster.leader().expect("leader");

    let started = Instant::now();
    let (outcome, _pin) = leader
        .fan_out_blob(&digest, csv.as_bytes())
        .await
        .expect("fan-out");
    let elapsed = started.elapsed();

    assert!(outcome.quorum);
    assert_eq!(outcome.acks.len(), 3);
    // Two peers, each receiving the whole payload.
    assert_eq!(outcome.bytes_sent, 2 * csv.len() as u64);
    for node in cluster.live() {
        assert!(node.blobs().stat(&digest).expect("stat").have);
    }

    println!(
        "#438 acceptance 3: {} bytes fanned out to 3 nodes in {elapsed:?} ({} bytes on the wire)",
        csv.len(),
        outcome.bytes_sent
    );

    drop(_pin);
    cluster.shutdown_all().await;
}

/// Issue #285: a node that joins after the upload materialises the spool file from what it
/// receives through the log/snapshot — nothing is fetched from a peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_joining_after_the_upload_materialises_the_spool_file() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(2).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let csv = "id,name\n1,ada\n2,bob\n";
    let leader = cluster.leader().expect("leader");
    let response = leader
        .submit(dataset_put("customers", csv))
        .await
        .expect("commits");
    assert!(
        leader
            .await_applied(response.revision, CONVERGE_DEADLINE)
            .await
            .is_empty()
    );

    // A brand-new third member on a fresh directory.
    let port = reserve_ports(1)[0];
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let dir = TempDir::new().expect("tempdir");
    let seed = cluster.leader().expect("leader").advertise();
    let joiner = spawn(3, addr, dir.path(), cluster.audit_retention_secs).await;
    joiner.join_via(seed).await.expect("join");

    let deadline = Instant::now() + CONVERGE_DEADLINE;
    let path = spool_file(dir.path(), csv);
    loop {
        if std::fs::read(&path).ok().as_deref() == Some(csv.as_bytes()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the joiner never materialised the spool file at {path:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        joiner
            .dataset(DEFAULT_TENANT, "customers")
            .expect("read")
            .is_some()
    );
    joiner.shutdown().await.ok();
    cluster.shutdown_all().await;
}

/// A CSV of at least `bytes` bytes whose content is unique to `tag`.
///
/// Distinct content per dataset is the point: a dataset's blob is addressed by digest, so
/// uploading the same bytes under N names stores one blob and the snapshot stays small.
fn tagged_csv(bytes: usize, tag: &str) -> String {
    let mut csv = String::from("id,payload\n");
    let mut i = 0u64;
    while csv.len() < bytes {
        csv.push_str(&format!("{i},{tag}{}\n", "x".repeat(1_000)));
        i += 1;
    }
    csv
}

/// Issue #428: a fleet that has snapshotted and purged catches a fresh node up **over the wire**.
///
/// Every byte of a dataset rides the state machine, so a fleet at RFC-005's quotas has a snapshot
/// measured in MiB. openraft bounds each snapshot *chunk* by `install_snapshot_timeout` and
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
/// file-backed snapshots) and #440 (a KiB manifest plus a blob fetch) each cut the install time
/// while this fixture stayed at 8 × 512 KiB, until it landed at 577–592 ms locally and 502–1219 ms
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
    /// 32 MiB of state — under RFC-005's 8 MiB per-dataset and 64 MiB per-tenant ceilings, and
    /// sized so the install outlasts `ADMIT_CURRENCY_WAIT` by a wide margin on the *fastest*
    /// environment measured, not the average one. See `MIN_INSTALL_MARGIN` below and #492.
    const DATASETS: usize = 8;
    const PER_DATASET_BYTES: usize = 4 * 1024 * 1024;
    const CONVERGE_BY: Duration = Duration::from_secs(60);

    /// The install must outlast the admission window by this factor for the learner assertion
    /// below to be measuring two-phase admission rather than a coin flip. Checked, not assumed:
    /// #492 was exactly this margin silently going to zero.
    ///
    /// **Enlarging the fixture has roughly 2x left, and then it stops working.** RFC-005 caps a
    /// dataset at `DEFAULT_MAX_DATASET_BYTES` (8 MiB) and a tenant at
    /// `DEFAULT_MAX_DATASET_TOTAL_BYTES` (64 MiB), so 8 x 8 MiB is the ceiling and the write is
    /// already ~0.35 s/MiB of test time. If another change makes installs 3x faster again, the
    /// answer is a different mechanism — spreading across tenants, or the deterministic variant
    /// #492 defers to #486 (seed the blob store after the join so the install parks) — not a
    /// bigger `PER_DATASET_BYTES`.
    const MIN_INSTALL_MARGIN: u32 = 3;

    let _serial = TEST_LOCK.lock().await;
    let ports = reserve_ports(2);
    let addr1: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().expect("addr");
    let addr2: SocketAddr = format!("127.0.0.1:{}", ports[1]).parse().expect("addr");
    let dir1 = TempDir::new().expect("tempdir");
    let dir2 = TempDir::new().expect("tempdir");
    let retention = rift_cluster::DEFAULT_AUDIT_RETENTION_SECS;

    let leader = spawn_with_snapshot_policy(1, addr1, dir1.path(), retention, Some(2)).await;
    leader.cluster_init().await.expect("bootstrap node 1");
    let deadline = Instant::now() + LEADER_DEADLINE;
    while !leader.status().is_leader {
        assert!(Instant::now() < deadline, "node 1 never became leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut last_revision = 0;
    let mut last_csv = String::new();
    for i in 0..DATASETS {
        let csv = tagged_csv(PER_DATASET_BYTES, &format!("d{i}-"));
        let response = leader
            .submit(dataset_put(&format!("d{i}"), &csv))
            .await
            .unwrap_or_else(|e| panic!("dataset d{i} commits: {e}"));
        last_revision = response.revision;
        // The joiner will fetch this blob over the transport on install (D-50); seed the holder
        // the fan-out would have created in production.
        seed_blob_store(&leader, &csv);
        last_csv = csv;
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

    let joiner = spawn_with_snapshot_policy(2, addr2, dir2.path(), retention, Some(2)).await;
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
         flip rather than a check on two-phase admission. Enlarge `PER_DATASET_BYTES` (see #492 \
         and `MIN_INSTALL_MARGIN`'s note on the quota ceiling); do not loosen the assertion. {}",
        state("margin")
    );

    let last_name = format!("d{}", DATASETS - 1);
    let deadline = Instant::now() + CONVERGE_BY;
    let mut converged = false;
    while Instant::now() < deadline {
        if joiner
            .dataset(DEFAULT_TENANT, &last_name)
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

    // Installing a snapshot must also materialise the blobs' spool files, or the joiner holds a
    // dataset record it cannot serve a lookup from.
    let spool = spool_file(dir2.path(), &last_csv);
    let on_disk = std::fs::read(&spool)
        .unwrap_or_else(|e| panic!("the joiner never materialised {spool:?}: {e}"));
    assert_eq!(on_disk.len(), last_csv.len());

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
    let retention = rift_cluster::DEFAULT_AUDIT_RETENTION_SECS;

    let n1 = spawn_with_snapshot_policy(1, addrs[0], dirs[0].path(), retention, Some(2)).await;
    n1.cluster_init().await.expect("bootstrap node 1");
    let deadline = Instant::now() + LEADER_DEADLINE;
    while !n1.status().is_leader {
        assert!(Instant::now() < deadline, "node 1 never became leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let seed = Authority::from(addrs[0]);
    // Node 2 joins while there is nothing to catch up on, so the two-voter quorum below is formed
    // without exercising the path under test.
    let n2 = spawn_with_snapshot_policy(2, addrs[1], dirs[1].path(), retention, Some(2)).await;
    n2.join_via(&seed)
        .await
        .expect("node 2 joins an empty fleet");

    let mut last_revision = 0;
    for i in 0..8 {
        let csv = tagged_csv(512 * 1024, &format!("m{i}-"));
        last_revision = n1
            .submit(dataset_put(&format!("m{i}"), &csv))
            .await
            .unwrap_or_else(|e| panic!("dataset m{i} commits: {e}"))
            .revision;
        // n3 fetches each blob from a joint voter on install (D-50); seed the accepting node the
        // fan-out would have populated in production.
        seed_blob_store(&n1, &csv);
    }
    assert!(
        n1.await_applied(last_revision, CONVERGE_DEADLINE)
            .await
            .is_empty()
    );
    // See the sibling test: no public signal for "snapshot built and purged", so this is a settle.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let n3 = spawn_with_snapshot_policy(3, addrs[2], dirs[2].path(), retention, Some(2)).await;
    let mut joined = n3.join_via(&seed).await.is_ok();
    let mut last_attempt = Instant::now();
    let deadline = Instant::now() + CONVERGE_BY;
    let mut converged = false;
    let mut leaders_seen = BTreeSet::new();
    while Instant::now() < deadline {
        if let Some(leader) = n1.status().current_leader {
            leaders_seen.insert(leader);
        }
        if n3.dataset(DEFAULT_TENANT, "m7").expect("read").is_some() {
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
    let csv = big_csv(512 * 1024);
    for i in 0..8 {
        let r = cluster
            .leader()
            .expect("leader")
            .submit(dataset_put(&format!("j{i}"), &csv))
            .await
            .expect("dataset commits");
        assert_eq!(r.outcome, rift_cluster::ControlOutcome::Applied);
    }
    // j0..j7 are all the same bytes (one digest); the snapshot-joining fourth node fetches that
    // one blob on install (D-50), so seed the holder production's fan-out would have created.
    seed_blob_store(cluster.leader().expect("leader"), &csv);

    // A fourth node, exactly as `start_full` would build it.
    let port = reserve_ports(1)[0];
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let dir = TempDir::new().expect("tempdir");
    let joiner = spawn_with_snapshot_policy(
        4,
        addr,
        dir.path(),
        cluster.audit_retention_secs,
        cluster.snapshot_log_entries,
    )
    .await;

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

// ---------------------------------------------------------------------------
// Blob transfer (#437, epic #432 child 2)
//
// The store's own rules are unit-tested in `src/blobs/mod.rs`; what can only be
// proven here is that the bytes cross a real signed connection between two real
// nodes, at the size the epic's quotas actually permit.
// ---------------------------------------------------------------------------

/// sha256 of `b"hello"` — a digest no node in these tests ever receives.
const ABSENT_DIGEST: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

fn blob_transfer() -> rift_cluster::blobs::BlobTransfer {
    let client = Arc::new(rift_cluster::rpc::RpcClient::new(
        Some(rift_cluster::rpc::Signer::new(SECRET)),
        Arc::new(rift_cluster::rpc::AlwaysHealthy),
        rift_cluster::rpc::RpcClientConfig::default(),
    ));
    rift_cluster::blobs::BlobTransfer::new(client)
}

#[tokio::test]
async fn blob_of_the_per_tenant_ceiling_crosses_between_nodes() {
    // Acceptance criterion 1. 64 MiB is RFC-005 §4's per-tenant total — the size
    // the epic measured as un-replicable *through the log*, which is the whole
    // reason this transport exists. A smaller payload would not test the claim.
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;

    let bytes = vec![b'Z'; 64 * 1024 * 1024];
    let digest = rift_cluster::blobs::digest_of_bytes(&bytes);
    let target = cluster.members[1].addr;
    let transfer = blob_transfer();

    let outcome = transfer
        .put(target, &digest, &bytes)
        .await
        .expect("put 64 MiB to a peer");
    assert_eq!(outcome.resumed_from, 0, "nothing was staged beforehand");
    assert_eq!(outcome.bytes_sent, bytes.len() as u64);

    // The have/lack probe criterion 1 names. It is `?stat=1` rather than the
    // `HEAD` the issue asks for: a HEAD response carries no body, so it can
    // report neither a size nor a distinguishable 404 (see `blobs::routes`).
    assert!(
        transfer.have(target, &digest).await.expect("have"),
        "the receiver must report the blob it just accepted"
    );
    let stat = transfer.stat(target, &digest).await.expect("stat");
    assert!(stat.have);
    assert_eq!(stat.size, bytes.len() as u64);

    // And the bytes that come back are the bytes that went out.
    let fetched = transfer
        .get(target, &digest)
        .await
        .expect("get 64 MiB back");
    assert_eq!(fetched.len(), bytes.len());
    assert_eq!(rift_cluster::blobs::digest_of_bytes(&fetched), digest);

    cluster.shutdown_all().await;
}

#[tokio::test]
async fn an_interrupted_transfer_resumes_from_its_offset() {
    // Acceptance criterion 1, second half. The observable is `bytes_sent`: a
    // resumed transfer that quietly restarted from zero would still leave a
    // correct blob behind, so "the blob arrived" cannot distinguish the two.
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;

    let bytes = vec![b'Q'; 9 * 1024 * 1024];
    let digest = rift_cluster::blobs::digest_of_bytes(&bytes);
    let target = cluster.members[1].addr;
    let transfer = blob_transfer();

    // Deliver a prefix and stop, exactly as a killed origin would.
    let partial = transfer
        .put_prefix(target, &digest, &bytes, 4 * 1024 * 1024)
        .await
        .expect("partial put");
    assert_eq!(partial, 4 * 1024 * 1024);
    assert!(
        !transfer.have(target, &digest).await.expect("have"),
        "a partial transfer must not be visible as a blob"
    );

    let outcome = transfer.put(target, &digest, &bytes).await.expect("resume");
    assert_eq!(
        outcome.resumed_from,
        4 * 1024 * 1024,
        "the sender must pick up from what the receiver already held"
    );
    assert_eq!(
        outcome.bytes_sent,
        bytes.len() as u64 - 4 * 1024 * 1024,
        "the resumed transfer must not re-send the bytes already staged"
    );
    assert!(transfer.have(target, &digest).await.expect("have"));

    cluster.shutdown_all().await;
}

#[tokio::test]
async fn a_zero_byte_blob_crosses_the_wire_like_any_other() {
    // `BlobTransfer::put` special-cases `total == 0` (there is no chunk for the
    // loop to send, so the commit has to be driven explicitly). Nothing else
    // exercises that branch over a real connection.
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;

    let target = cluster.members[1].addr;
    let transfer = blob_transfer();
    let digest = rift_cluster::blobs::digest_of_bytes(&[]);

    let outcome = transfer
        .put(target, &digest, &[])
        .await
        .expect("put empty blob");
    assert_eq!(outcome.bytes_sent, 0);

    assert!(
        transfer.have(target, &digest).await.expect("have"),
        "an empty blob is still a blob the receiver holds"
    );
    assert_eq!(transfer.stat(target, &digest).await.expect("stat").size, 0);
    assert_eq!(
        transfer.get(target, &digest).await.expect("get"),
        Vec::<u8>::new()
    );

    cluster.shutdown_all().await;
}

#[tokio::test]
async fn a_partially_staged_blob_is_not_served_and_still_completes_on_resume() {
    // Two things at once, both about the staging/committed boundary as seen
    // over the wire: a `.part` must never be readable, and a resume onto one
    // must still commit.
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;

    let bytes = vec![b'P'; 6 * 1024 * 1024];
    let digest = rift_cluster::blobs::digest_of_bytes(&bytes);
    let target = cluster.members[1].addr;
    let transfer = blob_transfer();

    transfer
        .put_prefix(target, &digest, &bytes, 4 * 1024 * 1024)
        .await
        .expect("partial put");

    let err = transfer.get(target, &digest).await;
    assert!(
        matches!(err, Err(rift_cluster::rpc::RpcError::NotFound { .. })),
        "a staged-but-unverified blob must not be readable, got {err:?}"
    );

    transfer.put(target, &digest, &bytes).await.expect("resume");
    assert_eq!(
        transfer
            .get(target, &digest)
            .await
            .expect("get after resume"),
        bytes
    );

    cluster.shutdown_all().await;
}

#[tokio::test]
async fn fetching_a_blob_the_node_lacks_is_a_typed_not_found() {
    // Acceptance criterion 3: a 404 the caller can act on, never a 500. #439's
    // fetch-on-apply has to tell "this peer does not have it, ask another" from
    // "this peer is broken", and a 500 collapses that distinction.
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;

    let target = cluster.members[1].addr;
    let transfer = blob_transfer();
    let digest = rift_cluster::blobs::BlobDigest::parse(ABSENT_DIGEST).expect("digest");

    let err = transfer.get(target, &digest).await;
    assert!(
        matches!(err, Err(rift_cluster::rpc::RpcError::NotFound { .. })),
        "expected RpcError::NotFound, got {err:?}"
    );
    assert!(!transfer.have(target, &digest).await.expect("have"));
    let stat = transfer
        .stat(target, &digest)
        .await
        .expect("stat is not an error");
    assert!(!stat.have);
    assert_eq!(stat.staged, 0);

    cluster.shutdown_all().await;
}

#[tokio::test]
async fn a_chunk_that_does_not_match_the_digest_leaves_no_blob_on_the_receiver() {
    // Acceptance criterion 2, over the wire rather than in the store: the
    // receiver, not the sender, is what must refuse.
    let _guard = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;

    let bytes = vec![b'M'; 1024];
    let honest = rift_cluster::blobs::digest_of_bytes(&bytes);
    let target = cluster.members[1].addr;
    let transfer = blob_transfer();

    // Claim one digest, send the bytes of another.
    let lie = rift_cluster::blobs::BlobDigest::parse(ABSENT_DIGEST).expect("digest");
    let err = transfer.put(target, &lie, &bytes).await;
    // The concrete class, not merely `is_err()`: "you sent bytes that are not
    // what you named them" is a 400 the sender must not retry identically. An
    // implementation that answered 500 would pass an `is_err()` check while
    // telling every caller the fault was the receiver's, and retryable.
    assert!(
        matches!(err, Err(rift_cluster::rpc::RpcError::BadRequest(_))),
        "expected BadRequest for a digest mismatch, got {err:?}"
    );

    assert!(!transfer.have(target, &lie).await.expect("have"));
    assert!(!transfer.have(target, &honest).await.expect("have"));

    cluster.shutdown_all().await;
}

// ---- #439: fetch-on-apply — the bytes leave the log (D-23, D-48, D-49) --------------------

/// `dataset_put`, with the bytes taken off the op and `origin` stamped — the shape the admin
/// front submits once a quorum holds the blob (D-49). The harness submits directly, so the
/// fan-out is the test's own job: call `fan_out_blob` first.
fn digest_only_dataset_put(name: &str, csv: &str, origin: NodeId) -> ControlRequest {
    let mut request = dataset_put(name, csv);
    if let rift_cluster::ControlOp::DatasetPut {
        csv,
        origin: accepted_by,
        ..
    } = &mut request.op
    {
        *csv = None;
        *accepted_by = origin;
    }
    request
}

fn dataset_delete(name: &str) -> ControlRequest {
    ControlRequest {
        op_id: uuid::Uuid::new_v4(),
        principal: None,
        issued_at_secs: 0,
        expected_revision: None,
        op: rift_cluster::ControlOp::DatasetDelete {
            tenant: rift_cluster::TenantId::default(),
            name: name.to_owned(),
        },
    }
}

/// Poll `node` until it no longer lists `name`, or `deadline` passes.
///
/// Only `Ok(None)` counts as gone. `wait_for_dataset`'s `.ok().flatten()` is safe for the
/// *presence* question — a read error there degrades to "not yet", and the loop keeps polling —
/// but the polarity is flipped here, so the same idiom would let a transient read failure report
/// a deletion that never happened. Restart and recovery windows, which is exactly when this
/// helper is used, are also when such a transient is likeliest.
async fn wait_for_dataset_gone(node: &RaftNode, name: &str, deadline: Duration) -> bool {
    let started = Instant::now();
    loop {
        if matches!(node.dataset(DEFAULT_TENANT, name), Ok(None)) {
            return true;
        }
        if started.elapsed() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `node` until its blob transport store holds `digest`, or `deadline` passes.
async fn wait_for_blob(
    node: &RaftNode,
    digest: &rift_cluster::blobs::BlobDigest,
    deadline: Duration,
) -> bool {
    let started = Instant::now();
    loop {
        if node.blobs().stat(digest).is_ok_and(|stat| stat.have) {
            return true;
        }
        if started.elapsed() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `node` until it lists `name`, or `deadline` passes.
async fn wait_for_dataset(node: &RaftNode, name: &str, deadline: Duration) -> bool {
    let started = Instant::now();
    loop {
        if node.dataset(DEFAULT_TENANT, name).ok().flatten().is_some() {
            return true;
        }
        if started.elapsed() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Fan `csv` out from the leader with `absent` down, then commit the digest-only op. Returns
/// the leader's id (the op's origin) and the digest.
async fn commit_digest_only_with_a_member_down(
    cluster: &TestCluster,
    absent: NodeId,
    csv: &str,
) -> (NodeId, rift_cluster::blobs::BlobDigest) {
    let digest = rift_cluster::blobs::digest_of_bytes(csv.as_bytes());
    let leader = cluster.leader().expect("two of three still lead");
    let (outcome, pin) = leader
        .fan_out_blob(&digest, csv.as_bytes())
        .await
        .expect("fan-out");
    assert!(outcome.quorum, "two of three hold it");
    assert!(
        !outcome.acks.contains(&absent),
        "the killed node cannot have acked"
    );
    let response = leader
        .submit(digest_only_dataset_put("customers", csv, leader.id()))
        .await
        .expect("commits");
    assert_eq!(response.outcome, rift_cluster::ControlOutcome::Applied);
    drop(pin);
    for node in cluster.live() {
        assert!(
            wait_for_dataset(node, "customers", Duration::from_secs(30)).await,
            "live node {} applies from its own store",
            node.id()
        );
    }
    (leader.id(), digest)
}

/// Pins D-23 and D-49: a follower that was down for the fan-out never received the bytes, and
/// the entry it later replicates carries only the digest — so it must fetch on apply from a
/// member that holds the blob, and end up with the same spool file every other node has.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_down_during_fan_out_fetches_the_blob_on_apply() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let leader_id = cluster.leader().expect("leader").id();
    let absent = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader_id)
        .expect("a follower");
    cluster.kill(absent).await;

    let csv = "id,name\n1,ada\n2,bob\n";
    let (_origin, digest) = commit_digest_only_with_a_member_down(&cluster, absent, csv).await;

    cluster.restart(absent).await;
    {
        let member = cluster.member(absent);
        let node = member.node.as_ref().expect("restarted");
        assert!(
            wait_for_dataset(node, "customers", Duration::from_secs(30)).await,
            "the restarted follower applies the digest-only entry"
        );
        assert!(
            node.blobs().stat(&digest).expect("stat").have,
            "fetched into its own store"
        );
        assert_eq!(
            std::fs::read(spool_file(member.dir.path(), csv)).expect("spool file"),
            csv.as_bytes(),
            "the spool file is materialised from the fetched bytes"
        );
        assert_eq!(
            node.blob_fetch_stall(),
            None,
            "a fetch that succeeded is not a stall"
        );
    }
    cluster.shutdown_all().await;
}

/// Pins D-48 (origin first, *then any member*) and epic #432's acceptance 5: killing the node
/// that accepted the write does not stop a member that missed the fan-out from applying it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_origin_is_not_needed_to_apply_a_digest_only_entry() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let leader_id = cluster.leader().expect("leader").id();
    let absent = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader_id)
        .expect("a follower");
    cluster.kill(absent).await;

    let csv = "id,name\n1,ada\n2,bob\n3,cleo\n";
    let (origin, digest) = commit_digest_only_with_a_member_down(&cluster, absent, csv).await;
    assert_eq!(origin, leader_id);

    // The origin dies before the absent member comes back: only the third node holds the blob.
    cluster.kill(origin).await;
    cluster.restart(absent).await;
    assert!(
        cluster.wait_for_leader(LEADER_DEADLINE).await.is_some(),
        "two of three elect"
    );
    {
        let node = cluster.member(absent).node.as_ref().expect("restarted");
        assert!(
            wait_for_dataset(node, "customers", Duration::from_secs(60)).await,
            "applies by fetching from the surviving holder"
        );
        assert!(node.blobs().stat(&digest).expect("stat").have);
        assert_eq!(node.blob_fetch_stall(), None);
    }
    cluster.shutdown_all().await;
}

/// Pins D-51 (#486): a member serves a **referenced** blob out of applied state when its own
/// transport store does not have the bytes.
///
/// The setup is the pre-fan-out shape the issue names, reproduced exactly: every live member has
/// the `sm_dataset_blobs` row (it applied the entry) and none has the bytes in a blob transport
/// store — which is the state a node reaches by applying an op that carried its bytes on the log,
/// since that arm writes redb and never `store_whole`s. Before this fix the joiner's fetch found
/// no holder and parked forever (D-48), applying nothing thereafter; now applied state answers.
///
/// This is the same setup as `a_blob_no_member_holds_parks_apply_and_recovers_when_a_holder_returns`
/// below, with the opposite outcome — which is the fix, stated as a diff between two tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blob_only_applied_state_holds_is_served_to_a_member_that_must_fetch_it() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let leader_id = cluster.leader().expect("leader").id();
    let absent = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader_id)
        .expect("a follower");
    cluster.kill(absent).await;

    let csv = "id,name\n1,ada\n2,bob\n";
    let (_origin, digest) = commit_digest_only_with_a_member_down(&cluster, absent, csv).await;

    // Erase every transport-store copy. The dataset is still live, so every live member still
    // holds the referenced `sm_dataset_blobs` row — bytes in applied state, held by nobody in a
    // transport store.
    for node in cluster.live() {
        std::fs::remove_file(node.blobs().path_of(&digest)).expect("erase the transport copy");
        assert!(
            !node.blobs().stat(&digest).expect("stat").have,
            "node {} must not hold the bytes in its transport store",
            node.id()
        );
    }

    cluster.restart(absent).await;
    {
        let member = cluster.member(absent);
        let node = member.node.as_ref().expect("restarted");
        assert!(
            wait_for_dataset(node, "customers", Duration::from_secs(60)).await,
            "applies by fetching from a member that holds it only in applied state"
        );
        assert_eq!(
            node.blob_fetch_stall(),
            None,
            "a fetch that succeeded is not a stall"
        );
        assert!(
            node.blobs().stat(&digest).expect("stat").have,
            "the fetched bytes land in the joiner's own transport store, so it is now an \
             ordinary holder"
        );
        assert_eq!(
            std::fs::read(spool_file(member.dir.path(), csv)).expect("spool file"),
            csv.as_bytes(),
            "the spool file is materialised from the bytes applied state served"
        );
    }
    cluster.shutdown_all().await;
}

/// Pins D-52 (#480): blob GC retains an unreferenced digest until this node's log is purged past
/// the index that unreferenced it. End to end: a blob whose last reference is deleted while a voter is
/// down stays on the holders' stores until the log is purged past the index that unreferenced it,
/// so the returning voter can still fetch it and apply the entry that references it.
///
/// This is the failure #480 describes: the delete makes the digest unreferenced fleet-wide, the
/// 60 s GC tick would reap it on every member (the grace is measured from the blob's *mtime*, not
/// from when it became unreferenced), and the returning replica then replays the original
/// digest-only `PUT` and finds no holder anywhere. It cannot be rescued by compaction either —
/// openraft's state-machine worker runs `apply` and `install_snapshot` on one sequential loop, so
/// the snapshot that would skip the blob queues behind the parked apply and never runs.
///
/// Since D-55 (#504) the sweep below retains for a second reason as well: the lagging voter is
/// *down* when it runs, so the fleet applied floor is unknown and rule C holds every tombstoned
/// blob regardless of the purge point. Rule A still holds it on its own — the purge point here
/// is far below the tombstone — so this test's claim is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blob_deleted_while_a_voter_lags_is_retained_until_the_log_is_purged_past_it() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let leader_id = cluster.leader().expect("leader").id();
    let absent = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader_id)
        .expect("a follower");
    cluster.kill(absent).await;

    let csv = "id,name\n1,ada\n2,bob\n";
    let (_origin, digest) = commit_digest_only_with_a_member_down(&cluster, absent, csv).await;

    // The delete unreferences the digest on every live member.
    cluster
        .leader()
        .expect("two of three still lead")
        .submit(dataset_delete("customers"))
        .await
        .expect("commits");
    for node in cluster.live() {
        assert!(
            wait_for_dataset_gone(node, "customers", Duration::from_secs(30)).await,
            "live node {} applies the delete",
            node.id()
        );
    }

    // Sweep as the 60 s tick would, but **at a `now` past the mtime grace**. Sweeping at the real
    // clock would prove nothing: the blob was written seconds ago, so `BLOB_GC_GRACE_SECS` alone
    // keeps it whether or not retention works, and the test would pass against an implementation
    // that had no tombstone rule at all. Past the grace, the tombstone is the only thing left that
    // can keep it — its index is far above these nodes' purge point, so it must.
    let past_the_grace = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
        + 10_000;
    for node in cluster.live() {
        node.run_blob_gc_now(past_the_grace).await.expect("sweep");
        assert!(
            node.blobs().stat(&digest).expect("stat").have,
            "node {} must retain a blob its log has not been purged past",
            node.id()
        );
    }

    // So the returning voter still finds a holder and applies the entry it was missing.
    cluster.restart(absent).await;
    {
        let node = cluster.member(absent).node.as_ref().expect("restarted");
        assert!(
            wait_for_blob(node, &digest, Duration::from_secs(60)).await,
            "the returning voter fetches the retained blob"
        );
        assert_eq!(
            node.blob_fetch_stall(),
            None,
            "retention means it never had to stall"
        );
        assert!(
            wait_for_dataset_gone(node, "customers", Duration::from_secs(30)).await,
            "and it applies the delete behind the entry it was parked on"
        );
    }
    cluster.shutdown_all().await;
}

/// Pins D-55 (#504): blob GC retains a tombstoned digest until **every member has applied past
/// it** — rule C — because the purge point alone (D-52 rule A) does not protect a replica whose
/// log is ahead of its applied index. The issue's exact sequence, end to end:
///
/// 1. A voter is down for a digest-only `PUT` at `p` and the `DatasetDelete` at `t` behind it.
/// 2. The bytes are reaped from every live member; the voter returns, replays `p` from the log,
///    finds no holder and **parks** (D-48) while replication keeps filling its log. This is the
///    state rule A cannot see: its log runs past `t` — and past the leader's purge point, once
///    the log compacts — so it will never be offered a snapshot; its applied index is below `p`.
/// 3. The request grace lapses (rule B no longer holds anything), one holder is restored, and
///    that holder sweeps with its log purged past `t`. **Under D-52 alone this reaps the blob**
///    — `t <= purged`, nobody asked within the grace — and the parked voter, still replaying `p`
///    from its own log against a fleet that holds nothing, wedges forever. Under rule C the
///    floor is the parked voter's own applied index, below `p`, so the blob stays.
/// 4. The parked voter's next probe finds the retained holder: it fetches, applies `p` and then
///    the delete at `t`. Now every member has applied past `t`, and the same sweep reaps it.
///
/// Both halves are asserted; the first is the discriminator. The request grace is expired by
/// hand for the same reason the sweep runs at a synthetic `now`: the loop's real clocks would
/// keep the blob for an hour whether or not retention works, and the test would prove nothing.
/// The issue's step 4 — the voter *restarts* between the delete and the sweep — is the same
/// state seen from the holders (no request inside the grace), and is deliberately not staged
/// here: keeping the voter alive lets this test sweep while the fleet floor is **known and below
/// `p`**, which is a stronger pin of rule C than the fail-closed "a member is unreachable" arm a
/// kill would exercise. Restarting a parked node in-process is itself possible since D-56
/// (#513); `a_parked_node_shuts_down_cleanly_and_its_data_directory_reopens` below covers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blob_deleted_while_a_replica_is_parked_is_retained_until_every_member_has_applied_past_it()
 {
    let _serial = TEST_LOCK.lock().await;
    // Snapshot (and purge to the tip) every 24 entries: late enough that nothing is purged before
    // the returning voter has caught its log up past `t`, early enough that a few dozen fill
    // writes push every live member's purge point past `t` inside the test.
    let mut cluster = TestCluster::start_with_snapshots(3, 24).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let leader_id = cluster.leader().expect("leader").id();
    let parked = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader_id)
        .expect("a follower");
    cluster.kill(parked).await;

    // (1) `PUT` at `p`, delete at `t`, with the voter down for both.
    let csv = "id,name\n1,ada\n2,bob\n";
    let (_origin, digest) = commit_digest_only_with_a_member_down(&cluster, parked, csv).await;
    let p = cluster
        .leader()
        .expect("leader")
        .status()
        .last_applied
        .expect("applied the put");
    let t = cluster
        .leader()
        .expect("two of three still lead")
        .submit(dataset_delete("customers"))
        .await
        .expect("commits")
        .revision;
    assert!(t > p);
    for node in cluster.live() {
        assert!(
            wait_for_dataset_gone(node, "customers", Duration::from_secs(30)).await,
            "live node {} applies the delete, dropping its blob row",
            node.id()
        );
        // Reap the bytes so the returning voter has nothing to fetch (D-51's applied-state
        // holder is gone with the delete) and parks — the D-48 setup.
        std::fs::remove_file(node.blobs().path_of(&digest)).expect("reap the blob from a holder");
    }

    // (2) The voter returns, replays `p` from its log and parks; replication keeps filling its
    // log. Fill until every live member's log is purged past `t`, one write at a time, waiting
    // for the parked voter's log to reach each entry before the next: the leader purges the
    // moment it snapshots, and the parked voter must be at the tip when that happens or openraft
    // switches it to a snapshot — the *other* case, which D-52 already covers.
    cluster.restart(parked).await;
    let fill_deadline = Instant::now() + Duration::from_secs(120);
    let mut fill = 0_u32;
    loop {
        let purged_past_t = cluster
            .live()
            .filter(|node| node.id() != parked)
            .all(|node| node.purged_index().is_some_and(|purged| purged >= t));
        if purged_past_t {
            break;
        }
        assert!(
            Instant::now() < fill_deadline,
            "the live members never purged past t={t} (fills: {fill})"
        );
        let revision = cluster
            .leader()
            .expect("leader")
            .submit(dataset_put(
                &format!("fill{fill}"),
                &format!("id,v\n{fill},x\n"),
            ))
            .await
            .expect("fill commits")
            .revision;
        fill += 1;
        let parked_node = cluster.member(parked).node.as_ref().expect("restarted");
        assert!(
            wait_for_log_index(parked_node, revision, Duration::from_secs(30)).await,
            "the parked voter's log must keep up with the tip (wanted {revision}, at {:?})",
            parked_node.last_log_index()
        );
    }
    let purge_floor = cluster
        .live()
        .filter(|node| node.id() != parked)
        .map(|node| node.purged_index().expect("purged past t"))
        .max()
        .expect("two live members");
    {
        let node = cluster.member(parked).node.as_ref().expect("restarted");
        assert!(
            wait_for_log_index(node, purge_floor, Duration::from_secs(30)).await,
            "log ahead of the purge point: the voter is caught up by entries, never a snapshot"
        );
        let applied = node.status().last_applied.unwrap_or(0);
        assert!(
            applied < p,
            "parked below p={p}: applied {applied} — the bytes must be unobtainable"
        );
        assert!(
            node.dataset(DEFAULT_TENANT, "customers")
                .expect("the node still answers")
                .is_none(),
            "parked, not applied"
        );
    }

    // (3) Restore ONE holder, let the request grace lapse, and sweep past the mtime grace — all
    // three within the same few milliseconds. Restoring the holder is also what lets the parked
    // voter's next fetch round succeed, so the window between the restore and the sweep's own
    // floor probe must be small against the voter's round interval: one holder keeps the window
    // to this node's probe (a restore on a second holder would let the voter fetch, apply past
    // `t`, and lift the floor before the second sweep). The voter's backoff doubles from
    // `FETCH_BACKOFF_MIN` to its 5 s cap in about 6 s of parking; waiting that out first puts the
    // window at a few ms in 5 s. A lost race reads as a voided premise below, not as retention
    // failing.
    tokio::time::sleep(Duration::from_secs(7)).await;
    let past_the_grace = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
        + 10_000;
    let holder = cluster
        .live()
        .find(|node| node.id() != parked)
        .expect("a live member other than the parked voter");
    let purged = holder.purged_index().expect("purged");
    assert!(
        purged >= t,
        "precondition: rule A alone would reap at {purged}"
    );
    holder
        .blobs()
        .store_whole(&digest, csv.as_bytes())
        .expect("restore the blob on a holder");
    holder.blobs().expire_requests();
    holder.run_blob_gc_now(past_the_grace).await.expect("sweep");
    let still_parked = cluster
        .member(parked)
        .node
        .as_ref()
        .expect("live")
        .status()
        .last_applied
        .is_none_or(|applied| applied < p);
    assert!(
        still_parked,
        "premise void, not a retention failure: the voter fetched and applied past p={p} inside \
         the restore→sweep window"
    );
    assert!(
        holder.blobs().stat(&digest).expect("stat").have,
        "node {} must retain a blob a member parked below p={p} still needs — \
         under D-52 alone (purged {purged} >= t {t}, nobody asked within the grace) this is reaped",
        holder.id()
    );

    // (4) The parked voter's next probe finds a holder: it fetches, applies `p` and the delete.
    {
        let node = cluster.member(parked).node.as_ref().expect("live");
        assert!(
            wait_for_blob(node, &digest, Duration::from_secs(60)).await,
            "the parked voter fetches the retained blob"
        );
        assert!(
            wait_for_dataset_gone(node, "customers", Duration::from_secs(30)).await,
            "and applies the delete behind the entry it was parked on"
        );
        assert!(
            wait_for_applied_index(node, t, Duration::from_secs(30)).await,
            "applied past t={t}: at {:?}",
            node.status().last_applied
        );
        assert_eq!(
            node.blob_fetch_stall(),
            None,
            "the stall clears on recovery"
        );
    }
    // Every member has now applied past `t`: the floor lifts and the same sweep reaps.
    for node in cluster.live().filter(|node| node.id() != parked) {
        node.blobs().expire_requests();
        node.run_blob_gc_now(past_the_grace).await.expect("sweep");
        assert!(
            !node.blobs().stat(&digest).expect("stat").have,
            "node {} reaps once every member has applied past the tombstone",
            node.id()
        );
    }
    cluster.shutdown_all().await;
}

/// Poll `node` until its log reaches `index`, or `deadline` passes.
async fn wait_for_log_index(node: &RaftNode, index: u64, deadline: Duration) -> bool {
    let started = Instant::now();
    loop {
        if node.last_log_index().is_some_and(|last| last >= index) {
            return true;
        }
        if started.elapsed() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `node` until its applied index reaches `index`, or `deadline` passes.
async fn wait_for_applied_index(node: &RaftNode, index: u64, deadline: Duration) -> bool {
    let started = Instant::now();
    loop {
        if node.status().last_applied.is_some_and(|last| last >= index) {
            return true;
        }
        if started.elapsed() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Pins D-48: a blob no reachable member holds parks the follower's apply — it does not halt
/// the node, does not apply an empty document, and past `BLOB_FETCH_ESCALATE_AFTER` reports
/// the stall on the health surface; it applies the moment a holder returns, and the stall
/// clears. The setup is the failure D-48 exists for: the blob reaped from every holder before
/// a member that missed the fan-out could fetch it.
///
/// **Reaching that state now takes a delete.** Since D-51 (#486) applied state is itself a
/// holder, so erasing the transport files leaves every live member still able to serve the
/// referenced `sm_dataset_blobs` row — see
/// `a_blob_only_applied_state_holds_is_served_to_a_member_that_must_fetch_it` above, which is
/// this same setup with the opposite outcome. Deleting the dataset first drops that row on
/// every live member (`gc_dataset_blob_if_unreferenced`, in the delete's own transaction), so
/// nothing in the fleet holds the bytes and D-48's park is reachable again. That the parked
/// entry's own `DatasetDelete` sits *behind* it in the log — making the park permanent until a
/// holder returns or compaction intervenes — is the residual #480 tracks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blob_no_member_holds_parks_apply_and_recovers_when_a_holder_returns() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let leader_id = cluster.leader().expect("leader").id();
    let absent = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader_id)
        .expect("a follower");
    cluster.kill(absent).await;

    let csv = "id,name\n1,ada\n";
    let (origin, digest) = commit_digest_only_with_a_member_down(&cluster, absent, csv).await;

    // Unreference it, so applied state stops being a holder (D-51), then reap the bytes.
    cluster
        .leader()
        .expect("two of three still lead")
        .submit(dataset_delete("customers"))
        .await
        .expect("commits");
    for node in cluster.live() {
        assert!(
            wait_for_dataset_gone(node, "customers", Duration::from_secs(30)).await,
            "live node {} applies the delete, dropping its blob row",
            node.id()
        );
        std::fs::remove_file(node.blobs().path_of(&digest)).expect("reap the blob from a holder");
    }

    cluster.restart(absent).await;
    {
        let node = cluster.member(absent).node.as_ref().expect("restarted");
        assert!(
            !wait_for_dataset(node, "customers", Duration::from_secs(3)).await,
            "must not apply without the bytes"
        );
        assert_eq!(
            node.blob_fetch_stall(),
            None,
            "not yet a stall: the escalation window has not passed"
        );

        // Polled, not slept: the escalation window starts when the *fetch* starts, and the
        // fetch starts only once the restarted node has caught its log up — which on a 2-vCPU
        // runner can be seconds after `restart` returns. A fixed sleep of window + 3 s passed
        // locally and failed on CI for exactly that reason.
        let escalation_deadline = Instant::now()
            + rift_cluster::blobs::BLOB_FETCH_ESCALATE_AFTER
            + Duration::from_secs(45);
        let stall = loop {
            if let Some(stall) = node.blob_fetch_stall() {
                break stall;
            }
            assert!(
                Instant::now() < escalation_deadline,
                "never escalated to a stall"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        assert_eq!(stall.digest, digest.as_str());
        assert!(stall.stalled_for() >= rift_cluster::blobs::BLOB_FETCH_ESCALATE_AFTER);
        assert_eq!(stall.origin, origin);
        assert!(
            stall.tried.contains(&origin),
            "the origin was asked: {:?}",
            stall.tried
        );
        assert!(stall.skewed.is_empty(), "no member is version-skewed");
        assert_eq!(
            stall.last_error, None,
            "every member merely lacks the blob; nobody refused"
        );
        assert!(
            node.dataset(DEFAULT_TENANT, "customers")
                .expect("the node still answers")
                .is_none(),
            "parked, not applied"
        );
    }

    // A holder returns.
    cluster
        .member(origin)
        .node
        .as_ref()
        .expect("live")
        .blobs()
        .store_whole(&digest, csv.as_bytes())
        .expect("restore the blob on the origin");
    {
        let member = cluster.member(absent);
        let node = member.node.as_ref().expect("live");
        // The fetch landing in this node's own store is what proves the parked apply drained:
        // it is reachable only through the apply that was parked. Asserting on the dataset
        // instead would prove nothing here, because the `DatasetDelete` queued behind the
        // parked entry removes it again moments later.
        assert!(
            wait_for_blob(node, &digest, Duration::from_secs(30)).await,
            "the parked apply drains and fetches once a holder returns"
        );
        assert_eq!(
            node.blob_fetch_stall(),
            None,
            "the stall clears on recovery"
        );
        assert!(
            wait_for_dataset_gone(node, "customers", Duration::from_secs(30)).await,
            "and it keeps going past the parked entry, applying the delete behind it"
        );
    }
    cluster.shutdown_all().await;
}

/// Pins D-56 (#513): a node parked on a blob nobody holds still shuts down **cleanly**, and its
/// data directory can be reopened in the same process.
///
/// Before D-56 this was impossible, and the failure was silent in the worst way. A parked apply
/// occupies openraft's state-machine worker, which awaits `apply` inline; the worker therefore
/// never returns to its command channel, never drops its `RedbStateMachine` clone, and
/// `RaftNode::shutdown`'s storage-release wait times out and returns `Err` — while the redb file
/// lock stays held by the stuck task. `TestCluster::kill` discards that `Err`, so the next
/// `RaftNode::start` on the same directory failed with `redb: Database already open`, several
/// steps away from the cause. D-56's shutdown signal ends the parked fetch, the apply fails, the
/// worker exits, and the handle drops.
///
/// The park is D-48's, built exactly as
/// `a_blob_no_member_holds_parks_apply_and_recovers_when_a_holder_returns` builds it: a member
/// that missed the fan-out, a delete that drops applied state as a holder (D-51), and the bytes
/// removed from every live member's transport store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_node_shuts_down_cleanly_and_its_data_directory_reopens() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");
    let leader_id = cluster.leader().expect("leader").id();
    let parked = cluster
        .members
        .iter()
        .map(|m| m.id)
        .find(|&id| id != leader_id)
        .expect("a follower");
    cluster.kill(parked).await;

    let csv = "id,name\n1,ada\n";
    let (_origin, digest) = commit_digest_only_with_a_member_down(&cluster, parked, csv).await;
    cluster
        .leader()
        .expect("two of three still lead")
        .submit(dataset_delete("customers"))
        .await
        .expect("commits");
    for node in cluster.live() {
        assert!(
            wait_for_dataset_gone(node, "customers", Duration::from_secs(30)).await,
            "live node {} applies the delete, dropping its blob row",
            node.id()
        );
        std::fs::remove_file(node.blobs().path_of(&digest)).expect("reap the blob from a holder");
    }

    // The member returns and parks: it replays the `PUT` from its own log and no member can
    // supply the bytes.
    cluster.restart(parked).await;
    let node = cluster
        .member_mut(parked)
        .node
        .take()
        .expect("restarted, and taken so this test owns the shutdown rather than `kill`");
    let escalation_deadline =
        Instant::now() + rift_cluster::blobs::BLOB_FETCH_ESCALATE_AFTER + Duration::from_secs(45);
    while node.blob_fetch_stall().is_none() {
        assert!(
            Instant::now() < escalation_deadline,
            "precondition: the node never parked, so this proves nothing about a parked shutdown"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        node.dataset(DEFAULT_TENANT, "customers")
            .expect("the node still answers")
            .is_none(),
        "parked, not applied"
    );

    // The claim. Under the old behaviour this is `Err(Runtime("raft core did not release
    // storage within 2s"))` after the full timeout.
    let stopped = tokio::time::timeout(Duration::from_secs(30), node.shutdown())
        .await
        .expect("shutdown must not hang");
    assert!(
        stopped.is_ok(),
        "a parked node must shut down cleanly, got {stopped:?}"
    );
    drop(node);

    // And the consequence that made this worth fixing: the directory is usable again. `restart`
    // panics inside `spawn_full` on `Database already open`, so reaching this line is most of the
    // claim; the status read is what proves the reopened node is actually serving.
    cluster.restart(parked).await;
    let restarted = cluster.member(parked).node.as_ref().expect("reopened");
    assert_eq!(
        restarted.status().node_id,
        parked,
        "the reopened node answers as itself"
    );

    cluster.shutdown_all().await;
}

// ---- #481: a byte quorum is not a decode quorum -----------------------------------------

/// Pins the half of D-53 (#481) that only a real fleet can ask: that a **peer** running a build which
/// cannot apply digest-only ops is probed over the wire and classified, rather than merely acked.
///
/// One member advertises `applies_digest_only: false` (the hidden knob). As a full voter it still
/// receives the blob during fan-out, so a byte quorum forms exactly as before (`outcome.quorum`) —
/// D-19 is untouched. What must differ is the fan-out's *verdict*: not every member is confirmed
/// capable, so it is not safe to strip, and the incapable member is named rather than merged into
/// a bare boolean an operator could not act on.
///
/// Deliberately stops at the verdict. The strip itself lives in
/// `rift_cluster_server::admin_front::fan_out_then_submit`, which this crate cannot depend on, and
/// a test that re-implemented the gate here would be asserting against its own copy of the logic —
/// green no matter what production did. That gate is pinned where it lives, by
/// `fan_out_then_submit_keeps_the_bytes_when_a_member_cannot_apply_digest_only`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_that_cannot_apply_digest_only_is_probed_and_named() {
    let _serial = TEST_LOCK.lock().await;
    let incapable: NodeId = 3;
    let mut cluster = TestCluster::start_with_one_member_digest_only_incapable(3, incapable).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    let csv = "id,name\n1,ada\n2,bob\n";
    let digest = rift_cluster::blobs::digest_of_bytes(csv.as_bytes());
    {
        let leader = cluster.leader().expect("leader");
        let (outcome, pin) = leader
            .fan_out_blob(&digest, csv.as_bytes())
            .await
            .expect("fan-out");
        assert!(
            outcome.quorum,
            "the byte quorum is unaffected: an incapable member still stores the blob (D-19)"
        );
        assert!(
            !outcome.sideload_safe,
            "one member is not confirmed capable, so stripping is not safe: {:?} / {:?}",
            outcome.sideload_incapable, outcome.sideload_unobserved
        );
        assert!(
            outcome.sideload_incapable.contains(&incapable),
            "the incapable member is named so the warning can say who: {:?}",
            outcome.sideload_incapable
        );
        assert!(
            outcome.skewed.is_empty(),
            "it has blob routes — it is not the pre-#437 skew case"
        );
        drop(pin);
    }
    cluster.shutdown_all().await;
}

/// The mirror: with every member on this build, the same fan-out reports it *is* safe to strip.
/// Without this, `sideload_safe` could be hard-wired to `false` — closing #481's wedge by
/// disabling sideloading altogether, which is the epic's whole point undone — and the test above
/// would still pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fleet_of_capable_members_is_safe_to_strip() {
    let _serial = TEST_LOCK.lock().await;
    let mut cluster = TestCluster::start(3).await;
    cluster
        .wait_for_leader(LEADER_DEADLINE)
        .await
        .expect("leader");

    let csv = "id,name\n1,carol\n2,dan\n";
    let digest = rift_cluster::blobs::digest_of_bytes(csv.as_bytes());
    {
        let leader = cluster.leader().expect("leader");
        let (outcome, pin) = leader
            .fan_out_blob(&digest, csv.as_bytes())
            .await
            .expect("fan-out");
        assert!(outcome.quorum);
        assert!(
            outcome.sideload_safe,
            "every member is this build, so nothing should be held back: {:?} / {:?}",
            outcome.sideload_incapable, outcome.sideload_unobserved
        );
        drop(pin);
    }
    cluster.shutdown_all().await;
}
