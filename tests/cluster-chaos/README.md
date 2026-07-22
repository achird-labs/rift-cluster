# Container-tier chaos harness

Real `rift-ee-server` processes, in containers, killed and restarted for real.

```sh
cargo test -p cluster-chaos -- --ignored --test-threads=1
```

`--ignored` because these need a container runtime — a workspace `cargo test` on
a machine without Docker must still pass. `--test-threads=1` because the compose
file publishes fixed host ports, so two stacks cannot coexist; the harness also
holds a process-wide lock, so forgetting the flag costs time rather than
correctness.

## Why two tiers

| | in-process (`crates/rift-cluster/tests/cluster.rs`) | container (here) |
|---|---|---|
| What runs | real `RaftNode`s over localhost TCP | real `rift-ee-server` processes |
| Covers | consensus, membership, convergence | **process death**, signals, cold start, the admin API as an operator reaches it |
| Speed | seconds, runs in PR CI | minutes, needs Docker |
| Land new scenarios here first | ✅ | only when it genuinely needs a process |

The split matters because a whole class of bug is invisible in-process: a
SIGTERM handler that is never wired up, a redb file that was never really
closed, a readiness gate that only looks right because the object was still in
memory. `test_graceful_leave` here is the case in point — it is the only test
that proves the signal path exists at all.

## Topology

`deploy/compose/docker-compose.yml` **itself**, not a copy. The artifact that
ships and the artifact that gets tested cannot drift apart, which is the whole
reason to reuse it rather than write a second compose file here.

Three nodes on a fixed subnet; node 1 founds the cluster and 2/3 seed-join.
Published host ports:

| node | admin | probes | metrics |
|---|---|---|---|
| rift-1 | 12525 | 12526 | 19090 |
| rift-2 | 22525 | 22526 | 29090 |
| rift-3 | 32525 | 32526 | 39090 |

## House rules

- **Assertions read the admin API and Prometheus metrics, never log output.** A
  log line is not an interface; a scenario that greps one fails the day someone
  rewords it.
- **Convergence is polled against a real surface, never slept-and-hoped.** The
  fleet gauges are sampled on a 5 s timer, so asserting immediately races the
  sampler and fails on a healthy cluster.
- **Exactly one leader, not at least one.** A split brain must fail rather than
  pass as "a leader exists".
- **An invariant violation is a bug to file, not a flake to retry.** Infra
  failures (image pull, daemon not running) may be retried once; a scenario that
  says an acknowledged write went missing never is.

## Interpreting a failure

1. The harness tears the stack down on drop, including on a panic — so by the
   time you read the output the containers are gone. Re-run the single scenario
   with `--nocapture` and, while it runs, `docker compose -f
   deploy/compose/docker-compose.yml logs -f`.
2. `only N/3 nodes became ready` is usually a seed-join problem, not a chaos
   problem: check `docker logs rift-2` for the join retry loop.
3. A convergence timeout names the port and how many nodes had it. One node
   short is a replication or readiness-gate issue; zero is a write-path issue.

## Scenario status

Implemented and passing: `test_config_sync_converges`, `test_node_rejoin`,
`test_graceful_leave`, `test_cold_start`, `c14_leader_kill_keeps_every_acknowledged_write`,
`c15_hard_kill_of_the_whole_fleet_keeps_acknowledged_writes`.

`c5_rolling_restart_never_stops_accepting_writes` is committed **failing**, as
the reproduction for a real defect it found (#72): a node that gracefully left
cannot rejoin when its state directory is retained, which is every rolling
restart. CI skips it by name; drop the `--skip` in `.github/workflows/ci.yml`
when #72 is fixed, and the scenario starts guarding the fix instead of
reporting the bug.

Not yet implemented — they need toxiproxy and an Envoy front the compose file
does not carry yet: **C4** (partition during writes → 503 + parked intent →
heal → replay), **C6** (30% loss + 100 ms jitter on the cluster port), **C7**
(joining node with stale state serves nothing until Ready), plus
`test_reconcile_preserves_state`, `test_reconcile_reorder` and
`test_no_seeds_not_ready`.
