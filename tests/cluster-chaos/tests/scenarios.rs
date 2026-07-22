//! Container-tier chaos scenarios (issue #11).
//!
//! Every one of these needs a real process to die, so none of them can live in
//! the in-process harness. Run with:
//!
//! ```sh
//! cargo test -p cluster-chaos -- --ignored --test-threads=1
//! ```
//!
//! `--ignored` because they need a container runtime; `--test-threads=1`
//! because the compose file publishes fixed host ports, so two stacks cannot
//! coexist. The harness holds a process-wide lock as well, so forgetting the
//! flag costs time rather than correctness.
//!
//! House rules, inherited from the issue's design bars:
//! - assertions read the admin API and Prometheus metrics, **never** log output;
//! - convergence is polled against a real surface, never slept-and-hoped;
//! - a scenario that fails is a bug to file, not a flake to retry.

use std::time::Duration;

use cluster_chaos::{
    CONVERGE_TIMEOUT, Cluster, NODES, imposter_ports, probe, put_imposter, wait_converged,
    wait_converged_on, wait_single_leader, wait_voters,
};

/// The imposter port a scenario configures. Inside the container network
/// nothing else binds it, and each scenario gets a fresh stack.
const IMPOSTER_PORT: u16 = 6001;

/// A write accepted by one node is servable by every node.
///
/// This is R1, the whole point of config-sync: with the default write barrier a
/// 2xx means the fleet has it, not merely that the leader does.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_config_sync_converges() {
    let _cluster = Cluster::up().await.expect("fleet comes up");

    let status = put_imposter(NODES[0].admin, IMPOSTER_PORT, "converged")
        .await
        .expect("admin write");
    assert_eq!(status, 201, "the write must be accepted by rift-1");

    wait_converged(u64::from(IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("every node serves the imposter the write created");
}

/// A node killed outright rejoins and catches up on what it missed.
///
/// `kill -9`, not a stop: the node gets no chance to leave, so this is the
/// dead-peer path rather than the graceful one.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_node_rejoin() {
    let cluster = Cluster::up().await.expect("fleet comes up");
    let survivors: Vec<_> = NODES.iter().filter(|n| n.name != "rift-2").collect();

    cluster.kill("rift-2").expect("kill rift-2");

    // The survivors keep taking writes while it is gone — a dead follower must
    // not cost the cluster its quorum.
    let status = put_imposter(NODES[0].admin, IMPOSTER_PORT, "written-while-down")
        .await
        .expect("admin write survives a dead follower");
    assert_eq!(status, 201);
    wait_converged_on(&survivors, u64::from(IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("the two survivors converge without the third");

    cluster.start("rift-2").expect("restart rift-2");
    cluster
        .wait_all_ready(Duration::from_secs(90))
        .await
        .expect("the killed node comes back ready");

    wait_converged(u64::from(IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("the rejoined node catches up on the write it missed");
}

/// SIGTERM removes the node from the membership, not just from the balancer.
///
/// This is the container proof of issue #6: the graceful leave has to be
/// answered by a real signal handler in a real process, and the *survivors*
/// have to observe the voter set shrink. In-process tests cannot show that the
/// signal path is wired at all.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_graceful_leave() {
    let cluster = Cluster::up().await.expect("fleet comes up");
    let survivors: Vec<_> = NODES.iter().filter(|n| n.name != "rift-3").collect();

    wait_voters(&NODES[0], 3.0, CONVERGE_TIMEOUT)
        .await
        .expect("three voters before the leave, or the assertion after proves nothing");

    cluster.stop("rift-3").expect("SIGTERM rift-3");

    wait_voters(&NODES[0], 2.0, CONVERGE_TIMEOUT)
        .await
        .expect("a graceful leave must shrink the voter set the survivors see");

    // And the survivors are still a working cluster, not merely a smaller one.
    let status = put_imposter(NODES[0].admin, IMPOSTER_PORT, "after-leave")
        .await
        .expect("admin write after a graceful leave");
    assert_eq!(status, 201);
    wait_converged_on(&survivors, u64::from(IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("the remaining two still converge");
}

/// A full-fleet restart restores configuration from disk.
///
/// The redb state directories survive the stop, so this proves durability
/// through real process exit and re-open — not through an in-process handle
/// that was never actually closed.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_cold_start() {
    let cluster = Cluster::up().await.expect("fleet comes up");

    let status = put_imposter(NODES[0].admin, IMPOSTER_PORT, "durable")
        .await
        .expect("admin write");
    assert_eq!(status, 201);
    wait_converged(u64::from(IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("converges before the restart");

    for node in &NODES {
        cluster.kill(node.name).expect("kill the whole fleet");
    }
    for node in &NODES {
        cluster.start(node.name).expect("restart the fleet");
    }
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the fleet comes back from cold");

    wait_converged(u64::from(IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("configuration survives a full-cluster restart");
}

/// C14: killing the leader mid-write loses no acknowledged write.
///
/// Every write that returned 2xx before the kill must still be present after a
/// new leader settles — an acknowledgement the cluster later forgets is the
/// worst failure this system can have.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c14_leader_kill_keeps_every_acknowledged_write() {
    let cluster = Cluster::up().await.expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("exactly one leader");

    // Acknowledged before the kill: whatever else happens, these must survive.
    let mut acknowledged = Vec::new();
    for offset in 0..5 {
        let port = IMPOSTER_PORT + offset;
        if put_imposter(NODES[leader].admin, port, "pre-kill")
            .await
            .is_ok_and(|s| s == 201)
        {
            acknowledged.push(u64::from(port));
        }
    }
    assert!(
        !acknowledged.is_empty(),
        "the leader accepted nothing, so the scenario would prove nothing"
    );

    cluster
        .kill(NODES[leader].name)
        .expect("kill the leader outright");

    let survivors: Vec<_> = NODES
        .iter()
        .filter(|n| n.name != NODES[leader].name)
        .collect();
    let new_leader_deadline = Duration::from_secs(30);
    let survivor_admin = survivors[0].admin;

    // A new leader has to appear, and then every acknowledged write has to be
    // there. Polling the survivor's admin API is the assertion; the leader
    // gauge only tells us when to expect an answer.
    tokio::time::timeout(new_leader_deadline, async {
        loop {
            if let Ok(ports) = imposter_ports(survivor_admin).await
                && acknowledged.iter().all(|p| ports.contains(p))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("a write acknowledged before the leader died was lost: expected {acknowledged:?}")
    });

    for node in &survivors {
        assert_eq!(
            probe(node.probe, "/healthz")
                .await
                .expect("probe reachable"),
            200,
            "{} stopped serving after the leader died",
            node.name
        );
    }
}

/// C15: `kill -9` the whole fleet under load; nothing acknowledged is lost.
///
/// The difference from `test_cold_start` is the absence of any cooperation —
/// no drain, no leave, no flush. Whatever was acknowledged was durable at the
/// moment it was acknowledged, or it was never really acknowledged.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c15_hard_kill_of_the_whole_fleet_keeps_acknowledged_writes() {
    let cluster = Cluster::up().await.expect("fleet comes up");

    let mut acknowledged = Vec::new();
    for offset in 0..5 {
        let port = IMPOSTER_PORT + offset;
        if put_imposter(NODES[0].admin, port, "pre-hard-kill")
            .await
            .is_ok_and(|s| s == 201)
        {
            acknowledged.push(u64::from(port));
        }
    }
    assert!(!acknowledged.is_empty(), "nothing was acknowledged");

    for node in &NODES {
        cluster.kill(node.name).expect("hard-kill the fleet");
    }
    for node in &NODES {
        cluster.start(node.name).expect("restart the fleet");
    }
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the fleet comes back after a hard kill");

    for port in &acknowledged {
        wait_converged(*port, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("imposter {port} was acknowledged and then lost: {e}"));
    }
}

/// C5: a rolling SIGTERM restart keeps the cluster serving throughout.
///
/// This is the shape a real deploy takes, and the one the graceful leave from
/// issue #6 exists for: each node leaves, restarts, and rejoins while the other
/// two hold quorum. The bar is that a write is accepted at every point in the
/// roll — a window where the fleet takes nothing is an outage, however brief.
///
/// **Currently fails, deliberately committed as the reproduction.** A node that
/// gracefully left cannot rejoin when it restarts with its state directory
/// intact — which is exactly what a rolling restart does, since the volume
/// outlives the container. `join_or_bootstrap` short-circuits on
/// `is_initialized()`, and before issue #6 that was sound because nothing ever
/// removed a node from membership; now the node resumes into a configuration
/// the cluster has moved past, never seed-joins, and sits at `/readyz` 503
/// forever. The in-process `test_rejoin_after_leave` misses it because it
/// rejoins on a *fresh* directory.
///
/// Kept rather than deleted: this scenario is the bug report. Restore the
/// plain "needs a container runtime" reason once the fix lands, so it runs with
/// the rest of the tier.
#[tokio::test]
#[ignore = "KNOWN FAILURE — reproduces a real defect: a graceful leave prevents rejoin when the \
            state directory is retained, i.e. every rolling restart"]
async fn c5_rolling_restart_never_stops_accepting_writes() {
    let cluster = Cluster::up().await.expect("fleet comes up");

    for (i, rolled) in NODES.iter().enumerate() {
        cluster.stop(rolled.name).expect("SIGTERM the node");

        // Write through a node that is NOT the one being rolled, so this
        // measures the cluster's availability rather than one node's.
        let other = &NODES[(i + 1) % NODES.len()];
        let port = IMPOSTER_PORT + u16::try_from(i).expect("three nodes fit in a u16");
        let status = put_imposter(other.admin, port, "mid-roll")
            .await
            .unwrap_or_else(|e| panic!("no write accepted while {} was down: {e}", rolled.name));
        assert_eq!(
            status, 201,
            "the fleet stopped accepting writes while {} was rolling",
            rolled.name
        );

        cluster.start(rolled.name).expect("bring the node back");
        cluster
            .wait_all_ready(Duration::from_secs(90))
            .await
            .unwrap_or_else(|e| panic!("{} did not rejoin after its roll: {e}", rolled.name));

        wait_voters(other, 3.0, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("voter set did not recover after {}: {e}", rolled.name));
    }

    // Everything written during the roll is present everywhere at the end.
    for i in 0..NODES.len() {
        let port = u64::from(IMPOSTER_PORT) + i as u64;
        wait_converged(port, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("a write taken mid-roll was lost: {e}"));
    }
}
