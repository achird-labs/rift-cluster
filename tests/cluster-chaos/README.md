# Container-tier chaos harness

Real `rift-ee-server` processes, in containers, killed and restarted for real.

```sh
cargo test -p cluster-chaos -- --ignored --test-threads=1
```

`--ignored` because the scenarios need a container runtime — a workspace
`cargo test` on a machine without Docker must still pass. (A few pure unit tests
over the scenarios' own assertion arithmetic do run un-ignored; see "Why C6
bounds a rate".) `--test-threads=1` because the compose file publishes fixed
host ports, so two stacks cannot coexist; the harness also holds a process-wide
lock, so forgetting the flag costs time rather than correctness.

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

### Why C6 bounds a rate, not a count

C6's injected jitter **overlaps the election timeout by design**: 100±100 ms each
direction against a randomized 150–300 ms timeout means heartbeat arrival gaps
routinely exceed a timeout draw from the low half of the range. So an occasional
election during C6's window is a correct fleet behaving correctly, not a fault —
and the scenario's original `transitions <= 1` was asserting a lucky draw rather
than a property (#94). It failed intermittently for that reason, including a
same-SHA fail-then-pass on PR #92.

What actually separates a correct fleet from a flapping one is the **rate**, so
C6 bounds transitions against `C6_MAX_LEADER_TRANSITIONS`, derived from the
~5 s leader-gauge resolution rather than tuned. If C6 fails on that bound, the
question is whether the fleet is genuinely re-electing continuously — **do not
raise the constant to make it pass.** The derivation lives in the doc comment on
the constant; changing `raft/node.rs`'s timeouts or C6's toxics means re-deriving
it there. The bound's arithmetic is pinned by un-ignored unit tests
(`c6_bound_admits_near_threshold_but_rejects_flapping`) so it stays honest even
though C6 itself only runs in the container tier.

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

`c16_pull_on_miss_rescues_lagging_follower` (#102) is the end-to-end proof the
pull-on-miss safety net (#49) shipped without: its decision table was covered
exhaustively at the unit level against a scripted `ClusterView`, so the *logic*
was proven and the *wiring* — manager construction, `bind` on the node, the seam
actually being consulted — was not.

Two things make it deterministic rather than raced. The lag is **injected**: a
250 ms latency toxic on the follower's inbound cluster link puts a floor under
how far behind it is, while the hook's 500 ms budget puts a ceiling on how long
it waits, so floor-below-ceiling means the rescue happens by construction.
Constant latency with **zero jitter**, deliberately — jitter creates the gaps
between heartbeats that C6 exists to bound, whereas a constant shift preserves
the heartbeat rate and leaves leadership alone. And the evidence is
**self-proving**: `rift-cluster-pull-on-miss: rescued-wait` is only ever set on
the path where the node found itself behind and then caught up, so the header is
the assertion that it lagged — no separate (and inherently racy) "is it lagging
yet?" precondition.

It is the one scenario that reads a **data-plane response header**, which is why
`compose/pull-on-miss.overlay.yml` publishes one imposter port: `exec_probe`
runs the binary's `healthcheck` subcommand inside the container and reports only
success or failure, discarding headers. The port is published on all three nodes
because the node that must lag has to be a follower, and the scenario asks who
the leader is at run time rather than assuming it.

It was verified to *fail* as well as pass: with `pull_on_miss.bind` removed, it
fails on the header assertion. A scenario that has never been seen red is a
scenario that has not been shown to test anything.

Slice 3 (#11) closed the gap between *having* every row and each row asserting
what the table actually specifies — five scenarios were passing on materially
weaker properties:

- `test_config_sync_converges` now asserts convergence **at 2xx-return**, asking
  each node exactly once with no retry. Polling with a timeout would have passed
  against a fleet whose write barrier did nothing, since convergence arrives
  moments later anyway — it would have been asserting eventual consistency while
  claiming to prove read-your-write.
- `test_config_sync_converges_without_barrier` is its counterpart on the
  `barrier-none.overlay.yml` stack, where "eventually" *is* the contract and the
  question is the ≤ 5 s bound. Running both is what keeps the barrier from being
  a no-op nothing would notice.
- `test_graceful_leave` drives a write ladder **across** the leave rather than
  after it, and checks the surviving config revision against the number of
  acknowledged writes. A leave loses writes in the window where membership is
  changing, which a post-hoc write cannot see.
- `empty_state_dirs_cold_start_empty` wipes the state volumes and asserts the
  config is **gone**, which is what makes `test_cold_start`'s restore
  attributable to redb rather than to anything else that outlives a container.
- C5 and C14 gained the failover bound (`WRITES_RESUME_BOUND`), and C14 the
  100-write storm and a zero-duplicates check.

**Failover is measured as write availability, not off the leader gauge.** The
gauge is resampled on a ~5 s timer, so it cannot resolve a 3 s bound at all — a
scenario polling it would be reading a quantity coarser than the thing it claims
to measure, the same mistake #94 fixed in C6. A write returning 201 proves a
leader exists, timestamped when it mattered, and is what a client experiences.

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

Because `list` decides what does *not* run, `scripts/chaos-quarantine.sh` is
itself watched by the tier's path filter (`scripts/cluster-smoke-paths.sh`,
issue #107): a bug in it shrinks the tier fleet-wide while the job still reports
success, and `check` only validates tag syntax. The filter script is watched for
the same reason — its self-test cannot catch a PR that edits pattern and case
table together, but a real tier run can. `.github/workflows/` is deliberately
*not* watched; the rationale, and what compensates for it, is written down in
the filter script's header rather than left to memory.

## Nightly soak

`.github/workflows/nightly-chaos.yml` runs one job per scenario (so the wall
clock is the slowest scenario, not the sum), each iterating per a table sized to
land under ~100 min of a 120 min cap. PR-time `cluster-smoke` runs each scenario
once, which catches a broken scenario but not a *flaky* one; only iteration
does, and iteration does not fit a PR's latency budget.

### Two accepted deviations from #11's design bars

Both are deliberate, and recorded here rather than left to be re-discovered as
"the CI doesn't match the spec".

- **`cluster-smoke` runs 1 iteration, not the specified 3.** Flake detection is
  the nightly soak's job, and it does it far better: 60–100 iterations of each
  scenario against 3 on a PR. Tripling the most expensive job on every
  cluster-touching PR — roughly 25 min to 70+ — buys very little the soak does
  not already catch, and it buys it by taxing every change. If a scenario is
  suspected flaky, soak that one on demand via `workflow_dispatch` rather than
  making every PR pay for the general case.
- **The nightly iterates 60–100 per scenario, not a flat 100.** 100× everything
  does not fit the 2 h cap: C6 alone carries an irreducible 60 s toxic window,
  which puts it at ~3.6 h by itself. The table is sized to the cap, with the
  cheapest scenarios at 100 and the longest at 60. The first nightly publishes
  measured per-iteration wall clock as a step summary, so the table can be tuned
  against real numbers rather than estimates.

On failure the harness writes `docker compose ps` and `logs` to `$CHAOS_LOG_DIR`
**before** teardown, and the job uploads them as an artifact — a 3am failure that
tore its own evidence down is a failure nobody can act on. Set `CHAOS_LOG_DIR`
locally to get the same dump; unset, teardown behaves exactly as before.

### `cluster-smoke` is a required check

Since #104, `master` carries a ruleset (`.github/rulesets/master.json`) that
makes `build`, `public-api` and `cluster-smoke` required status checks: a PR
cannot merge until they have **completed**, not merely started. This closes the
hole that motivated it — PR #101 merged while its `cluster-smoke` job was still
`in_progress`, so the tier never validated the change that landed, and nothing
objected.

This composes with the 1-iteration cadence above rather than fighting it. That
decision was about how many times a scenario runs *within* a job, and it stands
untouched; this one is about whether a merge may outrun the job at all. The
per-PR cost is unchanged.

It is also cheaper than it sounds. The job runs on every pull request and only
its heavy steps sit behind the path filter, so a docs-only PR completes it in
seconds. Requiring it delays exactly the changes it exists to guard.

`workflow_dispatch` takes a `scenario` filter and an `iterations` override, so a
single suspect scenario can be soaked on demand.
