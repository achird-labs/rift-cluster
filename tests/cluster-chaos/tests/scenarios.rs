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

use tokio::task::JoinSet;

use cluster_chaos::{
    CONVERGE_TIMEOUT, Cluster, FRONT_PORT, NODES, add_toxic, backend_failing_health_check,
    clear_toxics, config_revision, exec_probe, get_json, imposter_ports, metric, probe,
    put_imposter, put_imposter_with_key, put_stubs, toxic_count, wait_admin_reachable,
    wait_backend_ejected, wait_converged, wait_converged_on, wait_revisions_agree,
    wait_revisions_agree_on, wait_single_leader, wait_voters,
};

/// The imposter port a scenario configures. Inside the container network
/// nothing else binds it, and each scenario gets a fresh stack.
const IMPOSTER_PORT: u16 = 6001;

/// How long the fleet may take to converge with the write barrier off.
///
/// From the issue's normative table. It is a *product* bound, not a harness
/// tolerance: replication is a Raft append plus an apply, so 5s is generous by
/// an order of magnitude on a healthy LAN. If this starts failing, the question
/// is what got slow, not whether the number should go up.
const UNBARRIERED_CONVERGE_BOUND: Duration = Duration::from_secs(5);

/// Writes in C14's storm, per the issue's normative table.
///
/// Each one binds a distinct imposter port from `IMPOSTER_PORT` upward, so the
/// range must stay clear of the ports other scenarios use.
const C14_STORM_WRITES: u16 = 100;

/// How long the fleet may take to accept writes again after its leader is
/// killed — the operator-visible form of the issue's "new leader <= 3s".
///
/// **Why this and not the leader gauge or `/_cluster/members`.** The gauge
/// (`rift_cluster_members{state="leader"}`) is resampled on a ~5s timer and so
/// cannot resolve a three-second bound at all; reading a quantity coarser than
/// the bound is the mistake #94 fixed in C6. `GET /_cluster/members` *does*
/// serve openraft's live metrics, but it rides the **cluster port** behind the
/// HMAC credential (docs/rift-ee-server.md, "These ride the cluster port"), so
/// the harness cannot reach it. A write is what is left, and it is also what a
/// client actually experiences.
///
/// **Derived, and deliberately larger than 3s.** A post-kill write pays the
/// election *and* the write barrier: with the default `ready-nodes` barrier the
/// new leader waits on the dead node's applied index until
/// `--cluster-write-barrier-timeout` (2s) expires, then answers 201 with a
/// warnings header. So the client-visible budget is election (<= 3s per the
/// issue) + barrier timeout (2s) = 5s. Timing the write against a bare 3s would
/// fail a perfectly healthy fleet for doing exactly what it is configured to
/// do. The 3s the issue names is the election component, and it is the part
/// that would have to change for this bound to change.
const FAILOVER_WRITE_BOUND: Duration = Duration::from_secs(5);

/// Poll until an admin write is accepted again, returning how long it took.
///
/// `503`/`504` are the documented answers while no leader is available, so they
/// are retry signals here rather than failures. Any other non-201 is returned
/// as an error rather than retried, so a permanently broken request fails fast
/// and says what it saw instead of spinning out the whole budget.
async fn time_until_writes_resume(admin: u16, port: u16, body: &str) -> Result<Duration, String> {
    let started = std::time::Instant::now();
    let deadline = started + FAILOVER_WRITE_BOUND * 3;
    let mut last = String::from("no attempt completed");
    while std::time::Instant::now() < deadline {
        match put_imposter(admin, port, body).await {
            Ok(201) => return Ok(started.elapsed()),
            Ok(503 | 504) => last = "503/504 (no leader yet)".to_owned(),
            Ok(other) => {
                return Err(format!(
                    "write answered {other}, which is not a failover state"
                ));
            }
            Err(e) => last = format!("transport error: {e}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!(
        "no write accepted within {:?}; last: {last}",
        deadline - started
    ))
}

/// Rungs in `test_graceful_leave`'s write ladder, each on its own port.
const LADDER_RUNGS: u16 = 20;

/// First port of the ladder. Clear of `IMPOSTER_PORT` and of C14's storm range.
const LADDER_BASE_PORT: u16 = 6200;

/// Ceiling on observed leadership transitions in C6's 60s toxic window.
///
/// Derived, not tuned. C6 injects 100±100ms each direction against openraft's
/// randomized election timeout (150ms to `ELECTION_TIMEOUT_MAX_MS` = 300ms,
/// heartbeat 50ms, all in `rift-cluster`'s `raft/node.rs`), so heartbeat
/// arrival gaps routinely exceed a timeout draw from the low half of that
/// range: occasional elections are *in spec* for a correct fleet under these
/// toxics, not evidence of a fault. What separates correct from flapping is the
/// rate.
///
/// The leader gauge resamples on a ~5s timer, so the 60s window yields at most
/// ~12 samples and therefore at most 11 observable transitions. A fleet
/// re-electing continuously shows a different leader in nearly every sample --
/// 8-11 -- while near-threshold elections under these toxics show 0-4. This
/// bound is the top of the in-spec regime, which leaves it a wide margin below
/// the flapping floor and none above: a 5th election in one window is treated
/// as flapping, deliberately.
///
/// If `node.rs`'s timeouts or C6's toxics change, re-derive from the new
/// arithmetic; do not nudge it upward to silence a failure.
const C6_MAX_LEADER_TRANSITIONS: usize = 4;

/// Observed leadership transitions in a sequence of distinct leader samples.
///
/// Shared by C6 and its bound test so the two cannot drift: a change to how a
/// transition is counted is felt by the gate, not only by the container tier.
fn leader_transitions(samples: &[usize]) -> usize {
    samples.len().saturating_sub(1)
}

/// The C6 bound admits a correct fleet's near-threshold elections and still
/// rejects a flapping one.
///
/// Runs in ordinary CI: C6 itself needs a container runtime, so without this
/// the bound's arithmetic would only ever be exercised by the nightly tier.
#[test]
fn c6_bound_admits_near_threshold_but_rejects_flapping() {
    // The sequence C6 actually failed on in PR #92 (run 29973215820, attempt 1),
    // which attempt 2 passed on the same SHA -- a healthy fleet, not a fault.
    let near_threshold = [0, 1, 2, 1];
    assert_eq!(leader_transitions(&near_threshold), 3);
    assert!(
        leader_transitions(&near_threshold) <= C6_MAX_LEADER_TRANSITIONS,
        "the observed same-SHA-passing sequence must not fail the bound"
    );

    // A fleet re-electing continuously: a different leader in every ~5s sample
    // across the 60s window, which is the most the sampler can observe.
    let flapping: Vec<usize> = (0..12).map(|i| i % 3).collect();
    assert!(
        leader_transitions(&flapping) > C6_MAX_LEADER_TRANSITIONS,
        "a leader change in nearly every sample must still fail the bound"
    );

    // Pin the threshold itself, so an off-by-one edit to the constant or to the
    // comparison is caught rather than absorbed by the gap between 3 and 11.
    let at_bound: Vec<usize> = (0..=C6_MAX_LEADER_TRANSITIONS).collect();
    assert_eq!(leader_transitions(&at_bound), C6_MAX_LEADER_TRANSITIONS);
    assert!(leader_transitions(&at_bound) <= C6_MAX_LEADER_TRANSITIONS);

    let one_over: Vec<usize> = (0..=C6_MAX_LEADER_TRANSITIONS + 1).collect();
    assert!(leader_transitions(&one_over) > C6_MAX_LEADER_TRANSITIONS);
}

/// The barrier-none overlay really does turn the barrier off, on every node.
///
/// `test_config_sync_converges_without_barrier` passes identically against a
/// fleet still running the default `ready-nodes` barrier — that fleet converges
/// well inside 5s too. So a typo'd key, a renamed env var, or a deleted service
/// block would leave the scenario green while it silently stopped testing the
/// thing it is named for. Checking the overlay's text is cheap and catches all
/// three; it runs un-ignored because it needs no container.
#[test]
fn barrier_none_overlay_disables_the_barrier_fleet_wide() {
    let overlay = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/compose/barrier-none.overlay.yml"
    ))
    .expect("read the barrier-none overlay");

    for node in &NODES {
        let block = overlay
            .split(&format!("{}:", node.name))
            .nth(1)
            .unwrap_or_else(|| panic!("overlay has no block for {}", node.name));
        let env = block
            .split_once("RIFT_CLUSTER_WRITE_BARRIER")
            .map(|(_, rest)| rest)
            .unwrap_or_else(|| {
                panic!(
                    "overlay does not set RIFT_CLUSTER_WRITE_BARRIER for {} -- \
                     the scenario that uses it would silently run with the \
                     default barrier and still pass",
                    node.name
                )
            });
        assert!(
            env.trim_start().starts_with(": \"none\""),
            "{} sets RIFT_CLUSTER_WRITE_BARRIER to something other than \"none\"",
            node.name
        );
    }
}

/// An all-window leaderless fleet must not pass the transition bound vacuously.
#[test]
fn c6_bound_is_vacuous_on_a_leaderless_fleet() {
    assert_eq!(
        leader_transitions(&[]),
        0,
        "zero samples yields zero transitions, which clears the bound -- C6 \
         therefore has to assert the sequence is non-empty separately"
    );
}

/// A write accepted by one node is servable by every node.
///
/// This is R1, the whole point of config-sync: with the default write barrier a
/// 2xx means the fleet has it, not merely that the leader does.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_config_sync_converges() {
    let _cluster = Cluster::up().await.expect("fleet comes up");

    let (status, headers, _) = put_imposter_with_key(
        NODES[0].admin,
        IMPOSTER_PORT,
        "converged",
        "converge-at-2xx",
    )
    .await
    .expect("admin write");
    assert_eq!(status, 201, "the write must be accepted by rift-1");

    // A barrier that *timed out* also answers 201 -- with a Rift-Cluster-Warnings
    // header naming the nodes that had not applied. Without this check a slow
    // fleet would fail the no-retry assertion below as a bare "did not serve
    // it", which reads as a lost write rather than as a slow barrier. Asserting
    // the header's absence turns that into the precise failure it actually is.
    assert!(
        !headers.contains_key("rift-cluster-warnings"),
        "the write barrier timed out (Rift-Cluster-Warnings: {:?}); the fleet is \
         slow rather than broken, but a 201 no longer means every node applied",
        headers.get("rift-cluster-warnings")
    );

    // At 2xx-return, not eventually. Polling with a timeout here would pass on
    // a fleet whose barrier does nothing at all, because convergence would
    // arrive on its own a moment later -- the scenario would then be asserting
    // eventual consistency while claiming to prove read-your-write. So every
    // node is asked exactly once, with no retry: the only thing between the
    // 201 and the question is one HTTP round trip.
    let want = u64::from(IMPOSTER_PORT);
    for node in &NODES {
        let ports = imposter_ports(node.admin)
            .await
            .unwrap_or_else(|e| panic!("read imposters from {}: {e}", node.name));
        assert!(
            ports.contains(&want),
            "{} did not serve {want} at the moment the write returned 201 -- \
             with --cluster-write-barrier=ready-nodes a 2xx means the fleet has \
             it, not merely that the leader does (R1)",
            node.name
        );
    }
}

/// R1's other half: with the barrier off, a 2xx promises less — so convergence
/// has to be *fast* instead of immediate.
///
/// Separated from the scenario above rather than folded into it, because the
/// two assert different contracts. With the barrier on, "eventually" is a bug;
/// with it off, "eventually" is the contract and the only question is the
/// bound. Running both is what stops the barrier from being a no-op that
/// nothing would notice.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_config_sync_converges_without_barrier() {
    let _cluster = Cluster::up_with_barrier_none()
        .await
        .expect("fleet comes up with the write barrier off");

    let started = std::time::Instant::now();
    let status = put_imposter(NODES[0].admin, IMPOSTER_PORT, "unbarriered")
        .await
        .expect("admin write");
    assert_eq!(status, 201, "the write must be accepted by rift-1");

    wait_converged(u64::from(IMPOSTER_PORT), UNBARRIERED_CONVERGE_BOUND)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "with --cluster-write-barrier=none the fleet must still converge \
                 within {UNBARRIERED_CONVERGE_BOUND:?}, measured from the write: {e}"
            )
        });

    let elapsed = started.elapsed();
    assert!(
        elapsed <= UNBARRIERED_CONVERGE_BOUND,
        "converged, but in {elapsed:?} -- past the {UNBARRIERED_CONVERGE_BOUND:?} bound"
    );
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
// multi_thread because the write ladder below must be polled while the main
// task is blocked inside a synchronous `docker compose stop`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a container runtime"]
async fn test_graceful_leave() {
    let cluster = Cluster::up().await.expect("fleet comes up");
    let survivors: Vec<_> = NODES.iter().filter(|n| n.name != "rift-3").collect();

    wait_voters(&NODES[0], 3.0, CONVERGE_TIMEOUT)
        .await
        .expect("three voters before the leave, or the assertion after proves nothing");

    // Drive writes *across* the leave rather than after it: a leave drops an
    // acknowledged write, if it drops one at all, in the window where
    // membership is changing. A scenario that writes once the dust has settled
    // cannot see the failure it exists to catch.
    //
    // Each rung takes its own port, so "was this acknowledged write kept?" is
    // answered per write by asking whether the port is served. The config
    // revision cannot answer it: `rift_cluster_config_revision` is the Raft log
    // index that last wrote a config -- global and monotone, already past the
    // ladder's length from bootstrap and bumped by the leave's own membership
    // change -- so `revision >= acked` would hold even if most rungs vanished.
    let writer = tokio::spawn(async move {
        let mut acked = Vec::new();
        let mut errors = Vec::new();
        for rung in 0..LADDER_RUNGS {
            let port = LADDER_BASE_PORT + rung;
            match put_imposter_with_key(
                NODES[0].admin,
                port,
                "rung",
                &format!("leave-ladder-{rung}"),
            )
            .await
            {
                Ok((201, _, _)) => acked.push(u64::from(port)),
                // 503/504 with an op-id is the documented degraded answer while
                // membership changes: the write was never acknowledged, so it
                // is not something the fleet promised to keep.
                Ok((503 | 504, _, _)) => {}
                Ok((other, _, _)) => errors.push(format!("rung {rung}: status {other}")),
                Err(e) => errors.push(format!("rung {rung}: {e}")),
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        (acked, errors)
    });

    // Stop mid-ladder so the leave lands while writes are in flight.
    //
    // `stop` is synchronous -- it shells out to `docker compose` and does not
    // return until the drain completes. On the default current-thread runtime
    // that would block the whole runtime, so the ladder could not be polled
    // during exactly the window this scenario exists to cover, and the writes
    // would all land before or after the leave. The `multi_thread` flavour on
    // this test is what keeps the writer on another worker; it is load-bearing,
    // not decoration.
    tokio::time::sleep(Duration::from_millis(600)).await;
    cluster.stop("rift-3").expect("SIGTERM rift-3");

    wait_voters(&NODES[0], 2.0, CONVERGE_TIMEOUT)
        .await
        .expect("a graceful leave must shrink the voter set the survivors see");

    let (acked, errors) = writer.await.expect("the write ladder task ran");
    assert!(
        errors.is_empty(),
        "survivors returned data-plane errors across a graceful leave: {errors:?}"
    );
    assert!(
        acked.len() >= LADDER_RUNGS as usize / 2,
        "only {} of {LADDER_RUNGS} rungs were acknowledged; the ladder did not \
         really run across the leave",
        acked.len()
    );

    // Every acknowledged write is still served by both survivors. This is the
    // "zero lost acknowledged writes" the table asks for, per write.
    for port in &acked {
        wait_converged_on(&survivors, *port, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| {
                panic!("write to port {port} was acknowledged and then lost across the leave: {e}")
            });
    }
    wait_revisions_agree_on(&survivors, acked[0], CONVERGE_TIMEOUT)
        .await
        .expect("survivors disagree on the config revision after the leave");
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

/// The other half of cold start: a fleet whose state directories are empty
/// comes back **empty**, not with yesterday's config.
///
/// Without this, `test_cold_start` is nearly vacuous — it would pass just the
/// same if the config were being restored from the image, from a stray
/// `--datadir` write-through, or from anything else that outlives a container.
/// Wiping the volumes is what makes the restore in that scenario attributable
/// to redb: same restart, same fleet, only the durable state removed, and the
/// config must then be gone.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn empty_state_dirs_cold_start_empty() {
    let want = u64::from(IMPOSTER_PORT);

    {
        let cluster = Cluster::up().await.expect("fleet comes up");
        let status = put_imposter(NODES[0].admin, IMPOSTER_PORT, "should-not-survive")
            .await
            .expect("admin write");
        assert_eq!(status, 201);
        wait_converged(want, CONVERGE_TIMEOUT)
            .await
            .expect("converges before the wipe, or the wipe proves nothing");
        drop(cluster);
    }

    // What wipes the state is container *destruction*: the compose file
    // declares no volumes, so each node's state dir lives in its container's
    // writable layer and `down` takes it with the container. That is precisely
    // the difference from `test_cold_start`, which kills and restarts the same
    // containers and so keeps its state dirs.
    let _cluster = Cluster::up().await.expect("fleet comes back up empty");

    for node in &NODES {
        let ports = imposter_ports(node.admin)
            .await
            .unwrap_or_else(|e| panic!("read imposters from {}: {e}", node.name));
        assert!(
            !ports.contains(&want),
            "{} served {want} after its state directory was wiped -- the config \
             came from somewhere other than redb, which means test_cold_start \
             was not proving durability",
            node.name
        );
    }
}

/// C14: killing the leader mid-write loses no acknowledged write.
///
/// Every write that returned 2xx before the kill must still be present after a
/// new leader settles — an acknowledgement the cluster later forgets is the
/// worst failure this system can have.
// multi_thread because the storm must keep being polled while the main task is
// blocked inside a synchronous `docker kill`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a container runtime"]
async fn c14_leader_kill_keeps_every_acknowledged_write() {
    let cluster = Cluster::up().await.expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("exactly one leader");

    // A 100-write storm that is genuinely *in flight* when the leader dies.
    //
    // Issuing the writes sequentially and awaiting each would settle every one
    // through the barrier before the kill, so the storm would only ever
    // exercise the quiet path -- 100 settled writes where there used to be 5,
    // and still nothing in the window that matters. Driving them concurrently
    // from a spawned task, and killing partway through, is what puts writes
    // mid-commit when the leader goes away.
    let leader_admin = NODES[leader].admin;
    let storm = tokio::spawn(async move {
        let mut inflight = JoinSet::new();
        for offset in 0..C14_STORM_WRITES {
            let port = IMPOSTER_PORT + offset;
            inflight.spawn(async move { (port, put_imposter(leader_admin, port, "storm").await) });
            // A trickle rather than a thundering herd: 100 simultaneous
            // connections would mostly measure the admin listener's accept
            // queue rather than the write path.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut acked = Vec::new();
        while let Some(joined) = inflight.join_next().await {
            if let Ok((port, Ok(201))) = joined {
                acked.push(u64::from(port));
            }
        }
        acked
    });

    // Kill partway through the storm, not after it.
    tokio::time::sleep(Duration::from_millis(700)).await;
    cluster
        .kill(NODES[leader].name)
        .expect("kill the leader outright");

    let survivors: Vec<_> = NODES
        .iter()
        .filter(|n| n.name != NODES[leader].name)
        .collect();
    let survivor_admin = survivors[0].admin;

    // Failover speed, as a client experiences it. See FAILOVER_WRITE_BOUND for
    // why this is a write rather than the leader gauge or /_cluster/members,
    // and why the budget is larger than the issue's bare 3s.
    let resumed = time_until_writes_resume(
        survivor_admin,
        IMPOSTER_PORT + C14_STORM_WRITES + 1,
        "post-kill",
    )
    .await
    .unwrap_or_else(|e| {
        panic!("the fleet never accepted a write after the leader was killed: {e}")
    });
    assert!(
        resumed <= FAILOVER_WRITE_BOUND,
        "writes resumed only after {resumed:?} following a leader kill, past the \
         {FAILOVER_WRITE_BOUND:?} budget (election + barrier timeout) -- this is \
         the window in which the front door sheds traffic"
    );

    let acknowledged = storm.await.expect("the storm task ran");
    assert!(
        !acknowledged.is_empty(),
        "no write in the storm was acknowledged, so the scenario proves nothing"
    );

    // Every write acknowledged before or during the kill is still there.
    tokio::time::timeout(CONVERGE_TIMEOUT, async {
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
        panic!(
            "a write acknowledged around the leader's death was lost: {} ports \
             were acknowledged and the survivor never listed them all",
            acknowledged.len()
        )
    });

    // The table's "zero duplicates" is discharged by construction rather than
    // by an assertion here: the admin API renders imposters from a port-keyed
    // map, so one port appearing twice is unrepresentable in the response and
    // a count would always find exactly one -- it could never fail. What *can*
    // show a double-apply is the revision: a replayed intent applied twice
    // would leave the survivors disagreeing on a stormed port.
    wait_revisions_agree_on(&survivors, acknowledged[0], CONVERGE_TIMEOUT)
        .await
        .expect("survivors disagree on a stormed port's revision after failover");

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
/// This scenario was committed failing, as the reproduction for issue #72: a
/// node that gracefully left could not rejoin when it restarted with its state
/// directory intact, because `join_or_bootstrap` resumed on `is_initialized()`
/// alone. Fixed by the departure marker and the membership check, so it now
/// guards that fix rather than reporting the bug.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c5_rolling_restart_never_stops_accepting_writes() {
    let cluster = Cluster::up().await.expect("fleet comes up");

    for (i, rolled) in NODES.iter().enumerate() {
        // Whether this roll takes the leader down decides what the timing
        // below means, so establish it before the stop rather than inferring
        // it after.
        let leader_before = wait_single_leader(CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("no settled leader before rolling {}: {e}", rolled.name));
        let rolling_the_leader = NODES[leader_before].name == rolled.name;

        cluster.stop(rolled.name).expect("SIGTERM the node");

        // Write through a node that is NOT the one being rolled, so this
        // measures the cluster's availability rather than one node's.
        let other = &NODES[(i + 1) % NODES.len()];
        let port = IMPOSTER_PORT + u16::try_from(i).expect("three nodes fit in a u16");
        // Asserted as zero interruption rather than as a recovery bound. A
        // graceful leave hands leadership over *during* the drain, and the
        // drain happens inside the synchronous `stop` above -- so by the time
        // any timer here could start, a leader already exists and a "recovered
        // within N seconds" bound would be satisfied by one HTTP round trip no
        // matter how bad the handover was. The stronger and actually
        // measurable claim is that the very first write after the leave is
        // accepted, with no retry at all.
        let status = put_imposter(other.admin, port, "mid-roll")
            .await
            .unwrap_or_else(|e| panic!("no write accepted while {} was down: {e}", rolled.name));
        assert_eq!(
            status,
            201,
            "the first write after {} left was answered {status}; a graceful \
             leave transfers leadership before the process exits, so the fleet \
             should never have stopped accepting writes at all{}",
            rolled.name,
            if rolling_the_leader {
                " (this roll took the leader)"
            } else {
                " (this roll took a follower, which should be invisible)"
            }
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

/// Issues #69 and #72 together: a graceful stop of the whole fleet, then a cold
/// start, converges on its own.
///
/// This is the composed invariant the two issues share, and neither one proves
/// it alone. A `docker compose stop` SIGTERMs every node, so each one in turn
/// tries to leave: #69's voter floor is what stops that walking the membership
/// down to a single authoritative volume, and #72's marker-and-rejoin is what
/// gets the node that *did* depart back in afterwards. Get either half wrong
/// and the fleet either cold-starts on one voter or never re-forms at all.
///
/// Deliberately a graceful stop rather than C15's hard kill: a kill leaves the
/// membership untouched, so it exercises none of this.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn whole_fleet_sigterm_then_cold_start_converges() {
    let cluster = Cluster::up().await.expect("fleet comes up");

    let port = IMPOSTER_PORT;
    let status = put_imposter(NODES[0].admin, port, "pre-teardown")
        .await
        .expect("write before the teardown");
    assert_eq!(status, 201, "the pre-teardown write must be acknowledged");
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the pre-teardown write converges");

    // SIGTERM every node, in order, exactly as `docker compose stop` does.
    for node in &NODES {
        cluster.stop(node.name).expect("SIGTERM the node");
    }
    for node in &NODES {
        cluster.start(node.name).expect("restart the node");
    }

    // Generous: a node whose departure landed rejoins through its seeds, and it
    // may need a restart-policy retry if it boots before any quorum exists.
    cluster
        .wait_all_ready(Duration::from_secs(180))
        .await
        .expect("the whole fleet must come back after a graceful stop");

    for node in &NODES {
        wait_voters(node, 3.0, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("{} did not converge on 3 voters: {e}", node.name));
    }
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the write taken before the teardown must survive it");
}

// ---------------------------------------------------------------------------
// Slice 2 (issue #73): the scenarios that need degraded links, a real network
// partition, and a front door. All of these run on the chaos overlay, so every
// cluster link transits toxiproxy and every node also sits on `mgmt`.
// ---------------------------------------------------------------------------

/// C4 — a partitioned minority parks its writes and replays them on heal.
///
/// The property is that a write to a node that cannot reach a leader is neither
/// served nor lost: it is refused with a receipt (`rift-cluster-op-id`), parked
/// durably, and replayed when a leader comes back — and a duplicate of it that
/// reached the majority meanwhile collapses instead of applying twice.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c4_partition_parks_minority_writes_and_replays_on_heal() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");

    // Partition a follower. Isolating the leader would be a different scenario
    // (the majority elects a new one); this one is about the minority side.
    let minority = NODES
        .iter()
        .enumerate()
        .find(|(i, _)| *i != leader)
        .map(|(_, n)| n)
        .expect("a non-leader exists");
    let majority: Vec<_> = NODES.iter().filter(|n| n.name != minority.name).collect();

    let first = 6001_u16;
    assert_eq!(
        put_imposter(majority[0].admin, first, "before")
            .await
            .expect("admin write"),
        201,
        "the pre-partition write must be accepted"
    );
    wait_converged(u64::from(first), CONVERGE_TIMEOUT)
        .await
        .expect("the fleet converges before anything is broken");

    cluster
        .partition(minority.name)
        .expect("cut the minority off");

    // The whole scenario depends on this: `mgmt` must keep the isolated node
    // reachable from the host, or none of the assertions below can be made at
    // all. Fail loudly and specifically rather than as a confusing timeout.
    wait_admin_reachable(minority.admin_via_mgmt, Duration::from_secs(30))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "{} must stay assertable over `mgmt` while partitioned ({e})",
                minority.name
            )
        });

    let parked = 6002_u16;
    let key = "c4-duplicate-key";
    let (status, headers, envelope) =
        put_imposter_with_key(minority.admin_via_mgmt, parked, "parked", key)
            .await
            .expect("the minority answers rather than hanging");
    // Both are correct parks: no reachable quorum answers 503 immediately, and
    // a forward that hangs to the write deadline answers 504. Either way the
    // intent is durable and the receipt is the proof.
    assert!(
        status == 503 || status == 504,
        "a minority write must be refused, got {status}"
    );
    assert!(
        headers.contains_key("rift-cluster-op-id"),
        "a refused write must carry the op-id that proves it was parked"
    );
    let slug = envelope["errors"][0]["type"]
        .as_str()
        .or_else(|| envelope["type"].as_str())
        .unwrap_or_default()
        .to_owned();
    assert!(
        slug == "unavailable" || slug == "timeout",
        "the error envelope must name the typed slug, got {slug:?} from {envelope}"
    );

    let pending = metric(minority.metrics_via_mgmt, "rift_cluster_intents_pending")
        .await
        .expect("the intent gauge is published");
    assert!(
        pending >= 1.0,
        "the parked intent must be pending, got {pending}"
    );

    // The same write, same key, through the majority: this is the duplicate
    // that dedup has to collapse when the parked copy replays.
    let (dup_status, _, _) = put_imposter_with_key(majority[0].admin, parked, "parked", key)
        .await
        .expect("the majority is still writable");
    assert_eq!(dup_status, 201, "the majority must still accept writes");

    cluster.heal(minority).expect("heal the partition");

    wait_converged(u64::from(parked), CONVERGE_TIMEOUT)
        .await
        .expect("the parked write must be present fleet-wide after heal");

    // The parked copy drains, and the fleet agrees on one applied revision for
    // the port -- which is what "the duplicate collapsed" looks like from
    // outside. Two applications would leave different revisions behind.
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let pending = metric(minority.metrics_via_mgmt, "rift_cluster_intents_pending")
            .await
            .unwrap_or(f64::INFINITY);
        if pending == 0.0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the parked intent never drained: pending={pending}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let dedup = metric(NODES[leader].metrics, "rift_cluster_dedup_hits_total")
        .await
        .expect("the dedup counter is published");
    assert!(
        dedup >= 1.0,
        "the replayed duplicate must have been collapsed, dedup_hits={dedup}"
    );

    for port in [first, parked] {
        wait_revisions_agree(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("nodes disagree on port {port} after heal: {e}"));
    }

    // Exactly one imposter for the deduplicated port, on every node.
    for node in &NODES {
        let ports = imposter_ports(node.admin).await.expect("list imposters");
        let copies = ports.iter().filter(|p| **p == u64::from(parked)).count();
        assert_eq!(
            copies, 1,
            "{} has {copies} copies of the deduplicated write",
            node.name
        );
    }
}

/// C6 — heavy latency, jitter and connection resets do not flap membership or
/// lose an acknowledged write.
///
/// Toxiproxy is an L4 proxy and cannot drop packets, so "lossy" is modelled as
/// latency+jitter on every byte plus `reset_peer` on a third of connections —
/// the TCP-level analogue of loss bursts.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c6_loss_and_jitter_do_not_flap_or_lose_writes() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");
    wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles before the links degrade");

    for node in &NODES {
        add_toxic(
            node.proxy,
            serde_json::json!({
                "type": "latency",
                "stream": "upstream",
                "toxicity": 1.0,
                "attributes": { "latency": 100, "jitter": 100 }
            }),
        )
        .await
        .expect("add upstream latency");
        add_toxic(
            node.proxy,
            serde_json::json!({
                "type": "latency",
                "stream": "downstream",
                "toxicity": 1.0,
                "attributes": { "latency": 100, "jitter": 100 }
            }),
        )
        .await
        .expect("add downstream latency");
        add_toxic(
            node.proxy,
            serde_json::json!({
                "type": "reset_peer",
                "stream": "upstream",
                "toxicity": 0.3,
                "attributes": { "timeout": 0 }
            }),
        )
        .await
        .expect("add connection resets");
    }

    // Confirm the links are actually degraded before concluding anything from
    // the stability that follows. Without this the scenario passes just as
    // happily against a cluster nobody perturbed -- "no flapping under load"
    // asserted over an untouched fleet.
    for node in &NODES {
        let attached = toxic_count(node.proxy)
            .await
            .unwrap_or_else(|e| panic!("read toxics on {}: {e}", node.proxy));
        assert_eq!(
            attached, 3,
            "{} should carry latency-up, latency-down and reset_peer; the toxic \
             window means nothing if they did not land",
            node.proxy
        );
    }

    // Every port whose write was acknowledged. Only these are asserted on: a
    // write refused with a park receipt is allowed to be absent until it
    // replays, and asserting on it here would be asserting the wrong contract.
    let mut acked: Vec<u16> = Vec::new();
    let window = Duration::from_secs(60);
    let started = std::time::Instant::now();
    let mut next_write = std::time::Instant::now();
    let mut leader_samples: Vec<usize> = Vec::new();

    while started.elapsed() < window {
        for node in &NODES {
            let voters = metric(node.metrics, r#"rift_cluster_members{state="voter"}"#)
                .await
                .unwrap_or(f64::NAN);
            assert_eq!(
                voters, 3.0,
                "{} saw the voter set change to {voters} under load -- membership \
                 must not flap just because links are slow",
                node.name
            );
        }

        // Who currently claims leadership. The gauge is resampled on a 5s timer,
        // so this bounds resolution rather than catching every transition; it is
        // enough to catch a fleet that is re-electing continuously.
        let mut leaders = Vec::new();
        for (i, node) in NODES.iter().enumerate() {
            if metric(node.metrics, r#"rift_cluster_members{state="leader"}"#)
                .await
                .is_ok_and(|v| v == 1.0)
            {
                leaders.push(i);
            }
        }
        if let [only] = leaders[..]
            && leader_samples.last() != Some(&only)
        {
            leader_samples.push(only);
        }

        if std::time::Instant::now() >= next_write {
            let port = 6100 + acked.len() as u16;
            if let Ok((status, _, _)) =
                put_imposter_with_key(NODES[0].admin, port, "under-load", &format!("c6-{port}"))
                    .await
                && status == 201
            {
                acked.push(port);
            }
            next_write = std::time::Instant::now() + Duration::from_secs(5);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Ordered before the rate bound: zero samples yields zero transitions, so a
    // fleet that never had a leader at all would clear the bound vacuously.
    assert!(
        !leader_samples.is_empty(),
        "no node ever reported leadership during the toxic window -- the \
         transition bound would pass vacuously over a leaderless fleet"
    );

    let transitions = leader_transitions(&leader_samples);
    assert!(
        transitions <= C6_MAX_LEADER_TRANSITIONS,
        "leadership changed {transitions} times in the toxic window (sequence \
         {leader_samples:?}); occasional near-threshold elections are in spec \
         under C6's jitter, but at ~5s sampling a continuously re-electing \
         fleet shows 8+ -- this rate means the fleet is flapping"
    );
    assert!(
        !acked.is_empty(),
        "no write was acknowledged during the toxic window -- the scenario proved nothing"
    );

    for node in &NODES {
        clear_toxics(node.proxy).await.expect("clear toxics");
    }

    for port in &acked {
        wait_converged(u64::from(*port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("acknowledged write {port} was lost: {e}"));
        wait_revisions_agree(u64::from(*port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("nodes disagree on port {port} after healing: {e}"));
    }

    for node in &NODES {
        let pending = metric(node.metrics, "rift_cluster_intents_pending")
            .await
            .unwrap_or(0.0);
        assert_eq!(pending, 0.0, "{} still has parked intents", node.name);
    }
    drop(cluster);
}

/// C7 — a node joining with an empty state directory serves nothing until its
/// reconciliation gate opens, then serves exactly what everyone else does.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c7_joining_node_serves_nothing_until_reconciled() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");

    let port = 6200_u16;
    assert_eq!(
        put_imposter(NODES[0].admin, port, "reconciled")
            .await
            .expect("admin write"),
        201
    );
    // Several more, so the joiner has real reconciliation work to do. With a
    // single imposter the gated window can close faster than it can be
    // observed, and the scenario then fails on its own "never saw the gate"
    // guard rather than on anything about the product.
    for extra in 1..5 {
        assert_eq!(
            put_imposter(NODES[0].admin, port + extra, "reconciled")
                .await
                .expect("admin write"),
            201
        );
    }
    wait_converged(u64::from(port + 4), CONVERGE_TIMEOUT)
        .await
        .expect("the fleet converges before the join");

    let joiner = &NODES[2];
    cluster
        .recreate(joiner.name)
        .expect("replace the node with an empty one");

    // Catch it before its gate opens. The observation is required rather than
    // best-effort: if the gated state is never seen, the scenario never watched
    // a join and would pass just as happily against a node with no gate at all.
    //
    // Two things keep that window catchable. The poll is cheap and tight --
    // reading `/readyz` only -- because a `docker exec` costs a few hundred ms
    // and doing one per poll burns the very window being watched. And the probe
    // runs once, on the first gated reading, rather than every time.
    // What must never happen is a node answering the data plane out of state it
    // does not have. Note this is NOT the same as "does not serve until ready":
    // a node legitimately binds its imposters *before* flipping the readiness
    // gate -- that ordering is the safe one, since the reverse would have a load
    // balancer routing to ports that are not listening yet. Asserting on the
    // gate alone therefore fails intermittently on correct behaviour.
    //
    // `rift_cluster_config_revision{port}` is the precise question: it is absent
    // until this node has applied that config. Serving while it is absent means
    // answering out of empty state, which is the actual defect C7 guards.
    let mut saw_gated = false;
    let mut served_unapplied = false;
    let mut probed = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    while std::time::Instant::now() < deadline {
        match get_json(joiner.probe, "/readyz").await {
            Ok((503, body)) if body["pending"].to_string().contains("cluster-reconciled") => {
                saw_gated = true;
                let unapplied = config_revision(joiner.metrics, u64::from(port))
                    .await
                    .is_err();
                if unapplied && !probed {
                    probed = true;
                    let served = exec_probe(joiner.name, &format!("http://127.0.0.1:{port}/"));
                    // Re-read afterwards: the probe costs a few hundred ms, long
                    // enough for the node to apply the config underneath it, and
                    // a node that applied mid-probe is allowed to serve.
                    let still_unapplied = config_revision(joiner.metrics, u64::from(port))
                        .await
                        .is_err();
                    served_unapplied = served && still_unapplied;
                }
            }
            Ok((200, _)) => break,
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !served_unapplied,
        "{} answered on the data plane for a config it had not applied -- a \
         joining node must not serve out of empty state",
        joiner.name
    );
    assert!(
        saw_gated,
        "never observed {} gated on `cluster-reconciled` -- the scenario did not \
         witness a join and would pass even if the gate did not exist",
        joiner.name
    );

    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the joiner becomes ready");

    assert!(
        exec_probe(joiner.name, &format!("http://127.0.0.1:{port}/")),
        "{} must serve the imposter once reconciled",
        joiner.name
    );
    wait_revisions_agree(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the joiner lands on the same applied revision as everyone else");
}

/// Applying a config must not rebuild imposters it did not change.
///
/// Recorded requests are the visible proxy for that: they live in the running
/// imposter, so a sibling write that recreated it would reset the count to
/// zero. This is the incrementality property, asserted from outside.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_reconcile_preserves_state() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");
    let port = 6300_u16;

    let body = serde_json::json!({
        "port": port,
        "protocol": "http",
        "recordRequests": true,
        "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "recorded" } }] }]
    });
    let status = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/imposters", NODES[0].admin))
        .timeout(Duration::from_secs(30))
        .json(&body)
        .send()
        .await
        .expect("create the recording imposter")
        .status()
        .as_u16();
    assert_eq!(status, 201);
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the recording imposter converges");

    assert!(
        exec_probe(NODES[0].name, &format!("http://127.0.0.1:{port}/")),
        "the imposter must answer on the data plane"
    );

    let recorded = |admin: u16| async move {
        get_json(admin, &format!("/imposters/{port}"))
            .await
            .map(|(_, b)| b["numberOfRequests"].as_u64().unwrap_or(0))
    };
    let before = recorded(NODES[0].admin).await.expect("read the imposter");
    assert_eq!(before, 1, "the data-plane request must have been recorded");

    // A sibling write: it changes the config set, but not this imposter.
    let sibling = 6301_u16;
    assert_eq!(
        put_imposter(NODES[1].admin, sibling, "sibling")
            .await
            .expect("sibling write"),
        201
    );
    wait_converged(u64::from(sibling), CONVERGE_TIMEOUT)
        .await
        .expect("the sibling converges");

    let after = recorded(NODES[0].admin).await.expect("read the imposter");
    assert_eq!(
        after, before,
        "the sibling write recreated the untouched imposter -- recorded requests \
         went from {before} to {after}"
    );

    for node in &NODES {
        let failures = metric(node.metrics, r#"rift_cluster_bind_failures{port="0"}"#)
            .await
            .unwrap_or(0.0);
        assert_eq!(failures, 0.0, "{} reported a bind failure", node.name);
    }
    drop(cluster);
}

/// Reordering an imposter's stubs propagates fleet-wide and preserves state.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_reconcile_reorder() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");
    let port = 6400_u16;

    let body = serde_json::json!({
        "port": port,
        "protocol": "http",
        "recordRequests": true,
        "stubs": [
            { "responses": [{ "is": { "statusCode": 200, "body": "first" } }] },
            { "responses": [{ "is": { "statusCode": 201, "body": "second" } }] }
        ]
    });
    let status = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/imposters", NODES[0].admin))
        .timeout(Duration::from_secs(30))
        .json(&body)
        .send()
        .await
        .expect("create the two-stub imposter")
        .status()
        .as_u16();
    assert_eq!(status, 201);
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the imposter converges");
    assert!(exec_probe(
        NODES[0].name,
        &format!("http://127.0.0.1:{port}/")
    ));

    let reversed = serde_json::json!([
        { "responses": [{ "is": { "statusCode": 201, "body": "second" } }] },
        { "responses": [{ "is": { "statusCode": 200, "body": "first" } }] }
    ]);
    let status = put_stubs(NODES[0].admin, port, reversed)
        .await
        .expect("reorder the stubs");
    assert_eq!(status, 200, "the stub replacement must be accepted");

    // Every node must show the new order -- read from the admin API, so this is
    // the committed config rather than one node's local memory.
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut agreed = 0;
        for node in &NODES {
            if let Ok((_, body)) = get_json(node.admin, &format!("/imposters/{port}")).await
                && body["stubs"][0]["responses"][0]["is"]["body"].as_str() == Some("second")
            {
                agreed += 1;
            }
        }
        if agreed == NODES.len() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "only {agreed}/{} nodes show the reordered stubs",
            NODES.len()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let (_, body) = get_json(NODES[0].admin, &format!("/imposters/{port}"))
        .await
        .expect("read the imposter");
    assert_eq!(
        body["numberOfRequests"].as_u64().unwrap_or(0),
        1,
        "reordering stubs must not rebuild the imposter and lose its recorded requests"
    );
    wait_revisions_agree(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the reorder lands at the same revision everywhere");
    drop(cluster);
}

/// A node whose seeds are unreachable stays live but never ready, and never
/// accepts a write.
///
/// The failure this guards against is a node that gives up joining and quietly
/// founds a cluster of one — which would look healthy and serve divergent state.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_no_seeds_not_ready() {
    // rift-2 seeds from rift-1, which is not running.
    let _cluster = Cluster::up_isolated("rift-2")
        .await
        .expect("start one node");
    let node = &NODES[1];

    // Give it time to boot far enough to answer at all.
    let boot = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < boot {
        if probe(node.probe, "/healthz").await.is_ok_and(|s| s == 200) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let observe = std::time::Instant::now() + Duration::from_secs(30);
    let mut checks = 0;
    while std::time::Instant::now() < observe {
        let health = probe(node.probe, "/healthz").await;
        assert_eq!(
            health.ok(),
            Some(200),
            "a seedless node must stay live -- it is running, just not joined"
        );
        let ready = probe(node.probe, "/readyz").await;
        assert_eq!(
            ready.ok(),
            Some(503),
            "a node that never reached its seeds must never report ready"
        );
        let write = put_imposter(node.admin, 6500, "must-not-apply").await;
        assert_ne!(
            write.ok(),
            Some(201),
            "a node that never joined must not accept a write -- doing so means \
             it founded a cluster of one"
        );
        checks += 1;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert!(
        checks >= 5,
        "only {checks} observations made; the window did not run"
    );
}

/// The front door stops routing to a backend that is no longer ready.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_front_routes_around_an_unready_node() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");
    let port = 6600_u16;

    let status = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{FRONT_PORT}/imposters"))
        .timeout(Duration::from_secs(30))
        .json(&serde_json::json!({
            "port": port,
            "protocol": "http",
            "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "front" } }] }]
        }))
        .send()
        .await
        .expect("write through the front")
        .status()
        .as_u16();
    assert_eq!(
        status, 201,
        "the front must accept a write while all backends are up"
    );
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the write converges");

    let dead = &NODES[1];
    cluster.kill(dead.name).expect("kill a backend");

    // Wait for the front to actually eject it. Round-robin keeps offering the
    // dead backend its share until the health check trips, so writing before
    // this would measure Envoy's check interval, not its routing.
    wait_backend_ejected(dead.ip, Duration::from_secs(60))
        .await
        .expect("envoy must notice the dead backend");

    for i in 0..10 {
        let status = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{FRONT_PORT}/imposters"))
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({
                "port": 6610 + i,
                "protocol": "http",
                "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "f" } }] }]
            }))
            .send()
            .await
            .expect("write through the front")
            .status()
            .as_u16();
        assert_eq!(
            status, 201,
            "write {i} through the front hit the ejected backend"
        );
    }

    cluster.start(dead.name).expect("restart the backend");
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the fleet comes back");

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if !backend_failing_health_check(dead.ip).await.unwrap_or(true) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "envoy never marked the restarted backend healthy again"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the fleet reconverges after the backend returns");
}
