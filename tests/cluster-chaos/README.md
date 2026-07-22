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

### The chaos overlay

Scenarios that need degraded links, a real partition, or a front door layer
`tests/cluster-chaos/compose/chaos.overlay.yml` over the shipped file — again by
`-f`, never by copying it:

```sh
docker compose -f deploy/compose/docker-compose.yml \
               -f tests/cluster-chaos/compose/chaos.overlay.yml up -d
```

`Cluster::up_with_chaos()` does this; `Cluster::up()` stays on the base file, so
slice-1 scenarios are unaffected. It adds:

| service | host port | what it is for |
|---|---|---|
| toxiproxy | 48474 (API) | every cluster link is re-pointed through it, so C6 can add latency, jitter and resets to real Raft traffic |
| toxiproxy | 45251-3 (admin), 45261-3 (metrics) | observation paths into each node over `mgmt` — see below |
| envoy | 42525 (front), 49901 (admin) | round-robins the three admin APIs with an active `/readyz` health check |

Two things about it are easy to get wrong and are load-bearing:

- **Relative paths in an overlay resolve against the *first* `-f` file's
  directory**, not the overlay's own. The mounts therefore read
  `../../tests/cluster-chaos/compose/...`. A sibling-looking `./envoy.yaml`
  silently means `deploy/compose/envoy.yaml` and fails to mount.
- **A partitioned node is observed through `mgmt`, never on its own published
  ports.** Docker programs a published port's DNAT against a single container
  IP, so `docker network disconnect rift <node>` takes that node's published
  admin and metrics ports down with it — and C4 needs both *while* the node is
  cut off. Attaching `mgmt` first (via `priority`) does usually keep the DNAT
  alive, but it was measured flaking on Docker 29.5.2 under sustained
  create/destroy churn, which is the worst kind of CI red: real, rare, and
  unreproducible. So the harness does not rely on it. `Node::admin_via_mgmt` and
  `Node::metrics_via_mgmt` go through toxiproxy, which is published from a
  container that is never disconnected and hops to the node over `mgmt`; neither
  leg touches `rift`. No toxic is ever attached to those listeners — they are
  how the harness watches, not what it perturbs.

### Why partitions are not toxiproxy

Toxiproxy is used for C6 and *not* for C4, for two independent reasons:

- **It cannot express a partition.** Every peer dials a node at its single
  advertised address, so one listener carries all of them. Disabling that
  listener cuts inbound only — the "partitioned" node keeps campaigning through
  its still-open outbound path, which destabilises the majority and is the
  opposite of what C4 asserts. Per-link proxies would need per-source
  addressing, i.e. hostname advertise, which is #68. `docker network disconnect`
  gives whole-node isolation symmetrically, and for a 3-node fleet that is the
  only partition that matters.
- **It is an L4 TCP proxy and cannot drop packets.** C6's "30% loss" is
  therefore modelled as `latency{latency:100, jitter:100}` on both streams plus
  `reset_peer` at toxicity 0.3 — 30% of connections reset, the TCP-level analogue
  of loss bursts.

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
`c15_hard_kill_of_the_whole_fleet_keeps_acknowledged_writes`,
`c5_rolling_restart_never_stops_accepting_writes`,
`whole_fleet_sigterm_then_cold_start_converges`.

`c5_rolling_restart_never_stops_accepting_writes` was committed **failing**, as
the reproduction for a real defect this tier found (#72): a node that gracefully
left could not rejoin when its state directory was retained, which is every
rolling restart. Fixed, un-ignored, and no longer skipped in CI — it now guards
that fix. It remains the example worth copying: a scenario that reproduces a
defect belongs in the tree, failing and named, not deleted.

`whole_fleet_sigterm_then_cold_start_converges` covers the invariant #69 and #72
share and neither proves alone — a graceful stop of the *whole* fleet, then a
cold start, converging without operator action. It is deliberately a SIGTERM
rather than C15's hard kill: a kill leaves the membership untouched and so
exercises neither the voter floor nor the rejoin path.

Slice 2 (#73) added the scenarios that need the overlay:
`c4_partition_parks_minority_writes_and_replays_on_heal`,
`c6_loss_and_jitter_do_not_flap_or_lose_writes`,
`c7_joining_node_serves_nothing_until_reconciled`,
`test_reconcile_preserves_state`, `test_reconcile_reorder`,
`test_no_seeds_not_ready`, `test_front_routes_around_an_unready_node`.

The scenario table from #11 is now fully implemented.

## Quarantine convention

A scenario that is persistently flaky or known-failing **keeps its code** and
gets a tag naming an **open** tracking issue:

```rust
#[ignore = "quarantined: #123 -- one line of why"]
```

Deletion is never the remedy: a deleted scenario stops being evidence, and
nothing then remembers the behaviour was ever covered. `c5_...` is the worked
example — it was committed failing as the reproduction for #72, and un-ignored
when #72 was fixed.

`scripts/chaos-quarantine.sh` makes the tag the single source of truth:

- `list` emits `--skip <fn>` flags, which both `cluster-smoke` and the nightly
  use. Nothing is hardcoded in a workflow, so a skip cannot outlive its reason.
- `check` fails if a tag names no issue, and — where a `GH_TOKEN` exists, i.e.
  the nightly — if the issue it names is already **closed**. A quarantine on a
  closed issue means the bug was fixed and the scenario was never turned back
  on.

## Nightly soak

`.github/workflows/nightly-chaos.yml` runs one job per scenario (so the wall
clock is the slowest scenario, not the sum), each iterating per a table sized to
land under ~100 min of a 120 min cap. PR-time `cluster-smoke` runs each scenario
once, which catches a broken scenario but not a *flaky* one; only iteration
does, and iteration does not fit a PR's latency budget.

On failure the harness writes `docker compose ps` and `logs` to `$CHAOS_LOG_DIR`
**before** teardown, and the job uploads them as an artifact — a 3am failure that
tore its own evidence down is a failure nobody can act on. Set `CHAOS_LOG_DIR`
locally to get the same dump; unset, teardown behaves exactly as before.

`workflow_dispatch` takes a `scenario` filter and an `iterations` override, so a
single suspect scenario can be soaked on demand.
