# Container-tier chaos harness

Real `rift-cluster-server` processes, in containers, killed and restarted for real.

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
| What runs | real `RaftNode`s over localhost TCP | real `rift-cluster-server` processes |
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

**Adding a published port? Check where it lands.** Linux allocates ephemeral
source ports from 32768–60999, so a published port inside that window can be
handed to an unrelated outbound connection — including this harness polling
these very endpoints — and `compose up` then fails to bind it with an
unattributable `address already in use` (issue #117; it cost PR #113 a
22-minute re-run). Ports in that range must be listed in the
`net.ipv4.ip_local_reserved_ports` step that `ci.yml` and `nightly-chaos.yml`
both run, which takes them out of the pool the kernel allocates *from* while
leaving them bindable. `ci_reserves_every_published_port_that_linux_could_hand_out`
fails the build if you forget, so this is a note about *why*, not a thing to
remember.

Below 32768 (the base tier's own ports, and 16300/26300, 14790/24790) there is
nothing to do.

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

### The front-door overlay

C17 and C18 need every node's own `--front-door` listener bound and reachable
from the host — real dispatch through it, not an admin-API lookup, is the
whole point of both (issue #131's correction: there is no `/front-door/resolve`
to read instead). `front-door.overlay.yml` adds that:

```sh
docker compose -f deploy/compose/docker-compose.yml \
               -f tests/cluster-chaos/compose/front-door.overlay.yml up -d
```

`Cluster::up_with_overlays(&["front-door.overlay.yml"])` does this.

| node | front door |
|---|---|
| rift-1 | 12527 |
| rift-2 | 22527 |
| rift-3 | 32527 |

Not in the shipped base file, for the same reason `flow-state.overlay.yml`
isn't: `--front-door` is a real operator choice that opens a listener, and the
base file is read as the reference deployment. All three nodes are published,
not one — C17 and C18 both assert dispatch through nodes that never received
the admin write directly, which is what "converges" and "survives a restart"
actually mean here.

Imposter data ports are never published by this overlay: the front door
dispatches to them in-process (`dispatch_to_port` against the local
`ImposterManager`, never a socket), so every C17/C18 assertion goes through a
front-door port and never touches an imposter port directly.

### The sources overlay

C20–C23 need two things the shipped topology deliberately does not give them, so
`sources.overlay.yml` adds both:

```sh
docker compose -f deploy/compose/docker-compose.yml \
               -f tests/cluster-chaos/compose/sources.overlay.yml up -d
```

`Cluster::up_with_overlays(&["sources.overlay.yml"])` does this.

| what | host port | why |
|---|---|---|
| rift-1/2/3 cluster port | 14790 / 24790 / 34790 | `/admin/sources*` rides the **cluster port**, not the admin API — a source is a control-plane object authenticated with the cluster credential |
| `source-origin` admin API | 46525 | where the fetch counter is read, and where the served document is changed mid-scenario |

**The counting server is a fourth `rift-cluster-server`, not a new image.** C20's
claim is that a pull fetches the source *exactly once fleet-wide*, and asserting
that needs something that counts requests exactly. So the origin is a rift node
run **un-clustered**, whose imposter's response body *is* the config document
the fleet fetches — and its Mountebank-compatible admin API then hands the
harness the counter for free (`GET /imposters/6600` → `numberOfRequests`,
maintained whether or not request recording is on). "Fetched once" becomes `== 1`
against a first-class API value rather than a log scrape.

A rift container rather than a small static-file image, deliberately: this tier
pins `toxiproxy` and `envoy` by digest, and a new public image means a new digest
to pin, review and rotate — a supply-chain surface added to serve a counter the
binary already in this repo publishes. It also buys, for nothing, the ability to
**change** what the origin serves mid-scenario (`PUT /imposters/6600/stubs`),
which C21's content change and C23's repair pull both need and which a static
file could not do without a container restart.

Un-clustered is load-bearing too: the origin has to be a plain HTTP host the
fleet fetches *from*, entirely outside the replicated log. A fourth voter would
change the quorum arithmetic every one of these scenarios depends on, and C21
kills a leader.

**This is the one place the harness depends on a first-party crate.** Everything
else here is driven over plain HTTP — an imposter config, a route table and a
source declaration are all built as JSON at the call site, the way an operator's
`curl` would. The cluster port is the exception: it is HMAC-authenticated per
request over a length-prefixed canonical form and negotiates a protocol version
(RFC-001 §11.2). `cluster_api` therefore uses `rift_cluster::rpc::RpcClient`
rather than re-implementing that framing, with `max_retries: 0` — the default
client retries three times, and a retried `POST .../pull` fetches again, which
would quietly turn C20's `== 1` into whatever the transport happened to do.

### Why the snapshot round trip needed a knob

`build_snapshot` / `install_snapshot` name openraft's wire path for catching a
lagging follower up wholesale rather than entry-by-entry. Three scenarios'
issues each named it as a mutation target — C18 ("rides the snapshot"), C22
("`sm_sources` omitted from the snapshot"), C26 ("the `audit` table omitted
from `SnapshotPayload`") — and each, applied and measured against this tier's
plain full-fleet restart, **survived**. Not because the scenarios were weak:
openraft's default `snapshot_policy` (`LogsSinceLast(5000)`,
`crates/rift-cluster/src/raft/node.rs`) never triggers on the few dozen
entries a chaos stack commits, and `stop`/`start` on every node restores each
one from its own redb, which needs nothing from a peer. `install_snapshot`
exists to catch up a peer that is missing log entries the leader no longer
has — nothing in a synchronized full-fleet restart is ever in that state.

`RIFT_CLUSTER_SNAPSHOT_LOG_ENTRIES=10` is the knob that closes the gap (issue
#183): it sets `snapshot_policy = LogsSinceLast(10)` **and**
`max_in_snapshot_log_to_keep = 0` (`NodeConfig::snapshot_log_entries`) — both
are required together, and why is pinned by two unit tests next to the field in
`raft/node.rs` rather than re-derived here. A testability knob, not operator
tuning: `None`, the only value any shipped configuration produces, leaves
openraft's defaults untouched.

**It lives in its own `snapshot-install.overlay.yml`, stacked only by C26 — not
in `chaos.overlay.yml`.** That distinction was learned the expensive way. The
knob purges the log the moment a snapshot covers it, so it changes how *any*
lagging node catches up; `chaos.overlay.yml` backs `Cluster::up_with_chaos()`,
which most of this tier uses, so setting it there applied it everywhere. C4 (a
healed partition replaying parked writes), C6 (a lossy link) and C7 (a joining
node reconciling) all went red — each is *about* a node falling behind and
catching up, and each suddenly needed a snapshot install where it had been
replaying the log. A knob that changes the catch-up mechanism belongs only on
the scenario whose subject is catch-up-by-snapshot.

A full-fleet restart still cannot exercise the wire path even with the knob on,
which is why C26 also grew a lag-behind phase: it stops one follower, commits
past the window so the leader snapshots and purges, then restarts it, leaving
the leader no way to catch it up but `install_snapshot`. That the RPC really
fired is asserted from `rift_cluster_snapshots_installed_total` rather than
inferred — without it the scenario would prove only that a snapshot install
*should* have been needed, and would stay green if a regression quietly restored
catch-up-by-log.

`c26_audit_chain_survives_a_full_cluster_restart` is the one scenario built on
top of it: it stops one follower, commits more than 10 entries through the
other two so the leader snapshots and purges what the follower would need,
then restarts it — forcing a real `install_snapshot` rather than assuming the
config alone proves the wire path ran. See its doc comment for the mechanism
and for what the container tier can and cannot observe about `install_snapshot`
directly. C18 and C22 still run the plain full-fleet restart, so their named
snapshot mutants still survive here for the reason above — the round trip for
routes and sources is gated in process instead, as their own entries below say.

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

### Why libfaketime (C12), not the clock itself

The same shape of reasoning, for time instead of the network: the containers
share the host kernel's clock, so there is no per-container clock to set — the
only way one node can honestly disagree with another about the time is to lie
to that node's *process*, which is what `faketime.overlay.yml`'s `LD_PRELOAD`
of libfaketime does (via the `runtime-faketime` build target, the production
`runtime` stage plus the library — no shipped image ever carries it). The
scenario proves the lie took hold before asserting anything: the two extreme
nodes' `Date` headers must disagree by most of the ±5 s spread, so a broken
overlay fails loudly instead of passing every clock-free-clears probe on a
secretly synchronized fleet.

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
`c15_flow_state_survives_a_full_cluster_restart`,
`c5_rolling_restart_never_stops_accepting_writes`,
`whole_fleet_sigterm_then_cold_start_converges`, `c17_routes_converge`,
`c18_routes_survive_a_full_cluster_restart`,
`c20_source_pull_converges_and_fetches_once`,
`c21_tracking_poll_is_leader_only_and_survives_failover`,
`c22_sources_survive_a_full_cluster_restart`,
`c23_drift_flags_and_pull_overwrites`,
`c24_rbac_enforcement_is_identical_through_any_node`,
`c25_key_revocation_survives_a_partition`,
`c26_audit_chain_survives_a_full_cluster_restart`,
`c27_tenancy_isolates_ownership_but_not_the_data_plane`,
`c28_fleet_journal_is_exact_under_node_kill`,
`c29_partial_reads_answer_within_budget_and_count_themselves`,
`c30_vector_cursor_walk_survives_membership_change`,
`c10_proxy_once_survives_owner_and_leader_kills`,
`c11_concurrent_recording_loses_nothing`,
`c12_clears_are_exact_under_clock_skew`.

## C28–C30 + C10–C12: the verification plane (#228)

The M3 exit bar's in-anger tier: fleet `savedRequests`/counts/clears/cursors
under kill, partition and skew, plus exactly-once proxy recording under owner
and leader death. (#223's smoke-level partition scenario,
`journal_partition_is_declared_on_both_sides_and_heals`, was deleted when C29
landed, exactly as its own note here used to direct: C29 is the same property
measured properly.)

None of the journal scenarios needs a new overlay — traffic goes through
`front-door.overlay.yml`'s already-published per-node listeners and every
merged read is an *admin* call on a port the base file publishes; imposter data
ports stay unpublished. The proxy pair borrows `sources.overlay.yml`'s
standalone `source-origin` server as its counting origin (in-network by service
name, single-node admin already published and port-reserved), so the whole
family adds exactly one piece of compose surface: `faketime.overlay.yml` for
C12, which publishes nothing.

| scenario | asserts | vacuity guard |
|---|---|---|
| `c28_fleet_journal_is_exact_under_node_kill` | survivors answer **exactly N** with the dead shard cache-served, honestly stamped partial while its writer is gone; fleet count exact; the stamp clears with the same N when the node returns | the pre-kill convergence gate: the exact tagged set must merge unstamped across three genuinely distinct shards before anything is broken |
| `c29_partial_reads_answer_within_budget_and_count_themselves` | both partition sides answer **within the 2 s peer budget** (measured, printed as the run artifact); `rift_cluster_journal_partial_reads_total` moves; heal clears the stamp and converges the sets | the metric delta — a fleet that stamped headers without counting them fails, as does one that never stamped at all |
| `c30_vector_cursor_walk_survives_membership_change` | the `?since=` walk is gapless and duplicate-free across a kill and the node's return (tallied by unique request path); `x-rift-truncated` appears **iff** a presented position predates a shard's eviction watermark | delivered-set equality against the sprayed set at every phase — a walk that skipped or repeated anything fails the set comparison, and the truncation probe asserts both directions |
| `c10_proxy_once_survives_owner_and_leader_kills` | zero wedged signatures across an owner kill *and* a leader kill (every signature ends Recorded on every node); replay adds nothing at the origin; max upstream calls per signature measured and printed, loosely bounded | the replay-freeze check — a fleet that "recovered" by silently re-proxying forever fails the origin-count freeze |
| `c11_concurrent_recording_loses_nothing` | exactly one recorded stub per proxyOnce signature fleet-wide after a 3-node race; zero origin calls once Recorded; proxyAlways never replays and keeps reaching the origin | the post-settle round from every node — one lost recording shows as an origin call, one duplicate as a second stub |
| `c12_clears_are_exact_under_clock_skew` | a fast-clock clear erases fleet-wide; every post-clear append survives with counts exact; racing clears from the two clock extremes converge | the `Date`-header spread assertion — a broken faketime overlay (synchronized fleet) fails before any clock-free probe can pass vacuously |

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

`c15_flow_state_survives_a_full_cluster_restart` (#121) closes the #16 epic: a
scripted imposter advances four independent flows, each step deliberately
landing on a **different node**, and every counter resumes at exactly the next
integer after the whole fleet stops and starts. Two properties in one scenario —
ownership routing (a per-process store would repeat a value before the restart
is even reached) and the durable tier (a reset to 1 afterwards is the bug it
exists to catch).

It needs its own overlay for two reasons, both scoped there rather than in the
shipped topology: the imposter's data port must be reachable from the host,
because the counter is only observable in a response body; and scripts are
gated behind `--allowInjection`, which the base file deliberately leaves off.

**This scenario earned its keep before it merged.** Run first against
`durability: "none"` — the mutation that must fail — it *passed*, because
replication pushes were hardcoded to persist at `Async` no matter what the
imposter chose, so both replicas wrote to disk state the imposter had asked to
keep in memory, and a restart adopted it back. The push now carries the write's
own durability and repairs never persist at all, so the three modes mean the
same thing fleet-wide; with the fix in place the mutation fails on
`resumed at 1`, as it always should have.

`c17_routes_converge`, `c18_routes_survive_a_full_cluster_restart` (#132) and
`c19_front_door_routes_around_bind_divergence` (#143) close #19's
cluster-level acceptance list for the front door (#131): the route table is a
replicated control-plane object exactly like the imposter config set, so it
gets the same two container-tier proofs C15/C16 already gave imposter
configs — a write's barrier contract, and survival of a real restart — plus a
third the config set never needed until #143 gave it one: a node whose own
bind failed still routes to the imposter it could not bind, dispatched
in-process rather than through the socket that lost the race.

Both need every node's own `--front-door` listener reachable from the host —
see "The front-door overlay" above — because there is no `/front-door/resolve`
to assert against instead (issue #131's correction): "the fleet has the
write" has to be proven by a real request through the front door, not an
admin-API lookup.

`c17_routes_converge` is `test_config_sync_converges` reprised for the route
table, with that stronger dispatch-based check. It writes twice — first an
empty table becoming one route, then the same route id retargeted to a
different imposter — and after each write, immediately (no polling: the
barrier's return **is** the assertion) dispatches through the two nodes that
did not receive the write. Verified red: commenting out the `ArcSwap` store in
`RedbStateMachine::drive_engine`'s `EngineAction::SyncRoutes` arm
(`crates/rift-cluster/src/raft/store.rs`) fails both nodes on the first write
with

```
rift-2 did not route /svc the moment the write returned 200 -- with
--cluster-write-barrier=ready-nodes a 2xx means the fleet has the
new table, not merely that the leader does (R1)
```

`c18_routes_survive_a_full_cluster_restart` follows the C15 restart pattern
exactly (`stop` then `start` every node, `wait_all_ready` + `wait_cluster_formed`,
never `recreate`) against a table of three routes — an exact host, a wildcard
host, and a path prefix, each naming a different imposter — and checks two
things per node afterward: `GET /front-door/routes` (the *stored* table) and a
real dispatch of all three shapes through that node's own front door (the
*in-memory* compiled table a restarted process has to rebuild before it can
serve anything). The two checks exist separately on purpose — a node can pass
the first and still 404 every request if the rebuild is skipped, which is
exactly what mutation-proving this scenario found:

**Correction to the issue's premise.** The issue named `build_snapshot` /
`install_snapshot` as C18's mutation target ("rides the snapshot"); measured,
it survives here for the reason "Why the snapshot round trip needed a knob"
above gives. Durability across this restart runs through `RaftNode::reconcile_engine`'s
post-join re-derivation from `sm_routes` instead (`crates/rift-cluster/src/raft/store.rs`,
called from the readiness reconciler `compose.rs` spawns to satisfy
`GATE_RECONCILED`) — the real cold-start projection, and the same one
`test_cold_start` exercises for imposter configs. That is the line this
scenario is mutation-proven against: dropping the routes action from the vec
`reconcile_engine` hands to `drive_engine` leaves `GET /front-door/routes`
matching (the stored table on disk is untouched) while every post-restart
dispatch 404s:

```
rift-1: exact-host route did not dispatch
  left: 404
 right: 200
```

(the `GET /front-door/routes` check immediately above it, for the same node,
still passed — the stored table survived the restart; only the in-memory
compiled table failed to come back.) The same mutant leaves `c17_routes_converge`
green — it does not touch a restart, so this is confirmation the mutation is
specific to the cold-start path, not a duplicate of C17's.

`c19_front_door_routes_around_bind_divergence` runs the same collision
`crates/rift-cluster-server/tests/bind_divergence.rs` already proves in-process,
across a real three-node stack instead: `bind-squat.overlay.yml` runs an
`alpine/socat` sidecar inside **rift-2's own network namespace**
(`network_mode: "service:rift-2"`), so an imposter's port is held by a
process outside rift-2 entirely before rift-2's own `ImposterManager` ever
tries to bind it — the way an unrelated deployment on the same host would
hold it. rift-1 and rift-3 share no namespace with the squatter, so their
binds are untouched.

The squat has to provably precede the imposter write or this is a race that
passes by luck. Compose cannot express "rift-2 waits on the squatter": the
squatter needs rift-2's network namespace to attach to, which only exists
once rift-2's own container has started, so the dependency can only run
rift-2 → squatter. The scenario closes the loop from the other side: it polls
the squatter's own healthcheck over `docker inspect` and does not write the
imposter config until it reports healthy, so the port is confirmed held —
not merely scheduled to be — before the write that depends on it.

Four checks, in order. The write itself returns `201`, not the `404` a
bind-failed node answered before #143, because every node now constructs the
imposter and claims the port in its map regardless of the local bind outcome.
`wait_converged` — reading `GET /imposters`, the map, not the socket —
converges fleet-wide despite the squat: convergence of the config, not of the
bind. rift-2's own `rift_cluster_bind_failures{port="6520"}` gauge reads `1`,
because serving unbound is not the same as pretending to be healthy. And
finally the dividend: a route to the squatted port dispatches `2xx` with the
imposter's body through **rift-2's own front door** — the node whose bind
failed. A last check against rift-1's front door, whose bind succeeded, is
what makes this a proof of divergence rather than of a stack that is
uniformly broken.

## C20–C23: imposter sources

Issue #137 closes #20's cluster acceptance list for imposter sources (#134) and
their tracking scheduler (#135). All four ride `sources.overlay.yml` — see "The
sources overlay" above for why the counting server is a fourth rift container.

| scenario | asserts | mutation story |
|---|---|---|
| `c20_source_pull_converges_and_fetches_once` | one pull converges fleet-wide (`wait_converged` + `wait_revisions_agree` + provenance on every node) **and** the origin served exactly **one** request for it; a second, unchanged pull fetches once more and writes nothing | `SourcePuller::pull` fetching once per voter instead of once — a stand-in for the fetch-in-apply design #134 rejected — went red **only** on the counter: `left: 3, right: 1`. Every convergence assertion still passed, which is the point |
| `c21_tracking_poll_is_leader_only_and_survives_failover` | the origin's request **rate** matches one node's cadence, not three; after the leader is `kill -9`ed and the fleet re-elects, the rate resumes at one node's cadence and a content change converges fleet-wide | deleting the `if !is_leader { … }` arm from `SourceScheduler::supervise` went red on the first window: **22 fetches in 40s** against a one-poller bound of 3–12 (three pollers is ≥ 21, which is what the bound's derivation predicts) |
| `c22_sources_survive_a_full_cluster_restart` | after a full-fleet SIGTERM/restart, every node still holds the source records, the provenance stamped on their ports, and the replicated `drifted` flag — and a post-restart pull still short-circuits on the unchanged digest without moving `last_applied` | emptying `sm_sources` whenever the store is opened went red immediately after the restart: `404 unknown route: GET /admin/sources/clean`. **The issue's named mutant does not apply here** — see below |
| `c23_drift_flags_and_pull_overwrites` | a hand edit of a source-owned imposter is visible as `drifted: true` on **every** node, and the next pull (`onDrift: overwrite`) restores the declared content in every node's committed config | making `RedbStateMachine::mark_drifted` a no-op went red on the post-edit check with all three nodes reporting `false` — while the repair pull and the content afterwards still passed, which is exactly the shape of the regression worth catching |

**Fetched-once is asserted as an equality, never an inequality.** `>= 1` would
pass against a fleet that had quietly gone back to per-node fetching, which is
the whole regression C20 exists to catch. The reason the fetch may be
non-deterministic I/O at all is that it happens *once*: the receiving node
fetches, canonicalizes, hashes and submits an ordinary control op, and every
replica applies those same bytes. Two nodes fetching the same URI a second apart
can legitimately get different bytes, and a fleet that applied *different*
configs from the "same" op would have diverged with nothing to point at.

C20 also pins the other half of that contract, which is easy to misread: a pull
**always** fetches — it cannot know the content is unchanged without asking. What
the digest short circuit removes is the *write*. So the second pull moves the
counter by one and answers `unchanged: true`.

**C21 bounds a rate, for the same reason C6 does.** A single count cannot tell a
slow box from a second poller. The window is 40 s at the enforced 5 s poll floor,
±10 % jitter, so one poller completes 6–10 sleeps on an idle box; the bound is
widened to 3–12 (down for a stretched sleep and the leaderless gap after the
kill, up for ~50 % of runner headroom) and three pollers cost ≥ 21. If it fails,
the question is how many nodes are polling — do not move the numbers. The
scenario also reads `rift_cluster_source_polls_total` fleet-wide as an
independent second opinion: only the leader increments it, so the sum counts each
poll once and must equal what the origin served.

**Correction to C22's named mutation.** The issue names "`sm_sources` omitted
from the snapshot"; applied and measured, that mutant **survives** here for
the reason "Why the snapshot round trip needed a knob" gives above — not
because the scenario is weak. The snapshot round trip **is** gated, in
process, by `provenance_is_reported_and_survives_snapshot_restore` in
`crates/rift-cluster/src/raft/store.rs`, which drives `build_snapshot` /
`install_snapshot` directly. What C22 exercises is the restart path, and the
mutant that kills it is the one named in the table above. Recorded here rather
than left to be re-derived: a mutant that survives is evidence about where a
property is enforced, not a gap to paper over.

**C23 runs only the `overwrite` arm.** `on_drift` has three, and all three are
already covered in process over real HTTP by #134's suite —
`a_skipped_pull_does_not_short_circuit_the_pull_that_resolves_it` in
`crates/rift-cluster-server/tests/sources.rs` for `skip`, and the state machine's own
`drifted_source_fails_when_asked` for `fail`. What containers add is process
death and the operator-facing surface, neither of which differs between the arms,
so triplicating a ~40 s scenario would buy a third copy of the same evidence.
`overwrite` is the arm run here because it is the default and the only one whose
effect is visible in committed content rather than only in a report field.

The demo these four back is `deploy/compose/sources-demo.yml` — one
`RIFT_IMPOSTERS` variable, three nodes serving the same mocks, and one
`POST /admin/sources/:id/pull` rolling the fleet onto new content. See
`deploy/README.md`'s "Imposter sources demo".

## C24–C27: tenancy, RBAC and audit

Issue #165 closes #146's acceptance list. These four are the **only** scenarios in
this tier that run against a *closed* admin plane: every other one relies on
RFC-002 §3.4 leaving the plane open while the fleet holds no principal, which is
why they send no credential. `tenancy.overlay.yml` boots the fleet with
`MB_APIKEY`, so a fleet-admin credential exists from the first request — see that
file's header for why a scenario cannot bootstrap one over HTTP itself.

| scenario | asserts | mutation story |
|---|---|---|
| `c24_rbac_enforcement_is_identical_through_any_node` | one Viewer bound in a non-default tenant (`acme`), the §4.1 action matrix through all three nodes, every verdict identical **including the response body** — plus vacuity guards requiring the matrix to contain a `200`, a `403` and a `404`, so "everyone agreed" cannot be satisfied by a fleet that refuses everything | `RaftNode::principal_bindings` returning empty on a non-leader (a leader-only authorizer) went red on the follower: `node rift-2 never applied the viewer's binding: 404` while the leader answered `200` |
| `c25_key_revocation_survives_a_partition` | (a) a partitioned minority cannot itself perform an authorization write (`503`/`504`), and (b) the **first** request through the previously-minority node after the heal is refused, with the convergence window measured and bounded | a 60 s TTL cache over `principal_bindings` went red on the majority side immediately: `the side that committed the revocation must refuse the revoked key at once` |
| `c26_audit_chain_survives_a_full_cluster_restart` | a session spanning `tenant.manage` and `imposter.write` through all three nodes; after a full-fleet stop/start every node's `(revision, action, resource)` projection is byte-identical to its own pre-restart one **and** to every other node's | clearing `sm_audit` whenever the store is opened — rows behaving as if held in memory — went red at `node rift-1 lost or reordered audit rows across the restart` |
| `c27_tenancy_isolates_ownership_but_not_the_data_plane` | two tenants, one imposter each; each tenant's Editor reads and manages (`AddStub`) its own imposter (`2xx`) and is refused the other's with a `404` **byte-identical** to a port that does not exist; the tenancy surface refuses cross-tenant reads the same way; and both imposters answer unauthenticated data-plane traffic — even a bogus credential — through every node | requiring an `authorization` header in `handle_request_inner` went red at `alpha's imposter must answer unauthenticated traffic through rift-1 — RFC-002 §7` |

**C24 now runs its matrix in a non-default tenant (`acme`), and that is the
point.** Issue #161's fail-closed guard used to make this unrunnable:
`admin_front::authorize_action` answered §8.4's 404 for every non-`default`
tenant, because `raft::store`'s `desired_configs`/`desired_routes` skipped
non-default tenants when binding the local engine, so a matrix run in `acme`
was **every** probe 404 regardless of role — the vacuity guards are what
caught it. Issue #182 replaced that blanket guard with a narrower per-resource
ownership gate in the same choke point (an authorized action is still refused
if the addressed port belongs to a *different* tenant), so a tenant other than
`default` is genuinely served, and C24 exercises that: the role still
discriminates inside `acme` (`imposter.read`/`write`/`delete`), and the 404
half of the split now comes purely from the *fleet-scoped* routes (`GET
/admin/tenants` and `GET /admin/audit/sink` both scope to `FLEET_SCOPE`),
which a tenant-bound principal holds no binding for regardless of which
tenant it is bound to. Every request the viewer sends carries an explicit
`X-Rift-Tenant: acme` — unlike `default`, `acme` is not what an omitted
header falls back to.

**C27 now constructs the issue's literal shape.** "Two tenants, one imposter
each" was not buildable before #182: an imposter could not be created in any
tenant but `default`, so both lived there and the hidden-resource assertion
had to be smuggled through `alpha`'s Editor holding no binding in `default` —
a real refusal, but not really a *cross-tenant* one. Now `alpha` and `beta`
each own their own imposter, each tenant's Editor can read and manage
(`AddStub`) its own and is refused the other's — a genuine ownership-gate
`404`, since the editor names its own tenant explicitly and the gate is what
catches the mismatched port — byte-identical to the never-existed refusal.
The old *bound-but-unservable* probe (`X-Rift-Tenant: alpha` reading `alpha`'s
own port, previously pinned at `404` as "not yet constructible") is gone as a
separate case: it is now simply the "own imposter" `2xx` read every editor
performs. The data-plane assertion is strengthened to also send a bogus
`authorization` header, proving the data plane has no authentication to fail
rather than merely none presented.

**C26 closes the gap the other two record.** The issue named "the `audit`
table omitted from `SnapshotPayload`" as its mutation target and asked for
"the snapshot-install path, not only restart-and-replay" — see "Why the
snapshot round trip needed a knob" above for why the plain full-fleet restart
this scenario used to run could never supply that. It now runs two phases: one
that deliberately lags a follower past `RIFT_CLUSTER_SNAPSHOT_LOG_ENTRIES` and
restarts it, forcing a real `install_snapshot`, and the original full-fleet
restart, kept because it separately guards "clearing `sm_audit` whenever the
store is opened" — a different bug on the ordinary cold-start path that the
snapshot phase does not touch. Both mutant stories, and how the container
tier observes `install_snapshot` actually running (from
`rift_cluster_snapshots_installed_total` — this file's own house rule, that
assertions read the admin API and Prometheus metrics and never log output,
rules out a log line), are recorded in the scenario's own doc comment rather
than repeated here.

**`GET /admin/whoami` is not a revocation probe, and neither is it a binding
probe.** It classifies no action (§4.3's `None` case), so it answers `200` to
anyone who authenticates — including a principal whose every binding has just been
revoked, since revoking a binding does not delete the key. C25 uses
`GET /admin/tenants/acme/principals` (`TenantManage`, path-scoped) against a
`tenant-admin`, giving a sharp `200` → `404`; C24's convergence gate uses the
Viewer's own read of the seeded imposter, because whoami would go green on a node
that had replicated the principal row but not the binding.

**Two harness gaps these four exposed**, both fixed in `src/lib.rs`:
`imposter_ports` sent no credential *and* ignored the status, so on a closed plane
a `401` read as "this node has no imposters" and `wait_converged` reported a
*convergence* failure for a *missing credential*; and `wait_admin_reachable` had
the same shape, reporting a reachable partitioned node as an unreachable one. Both
now have `_with_key` variants, a non-2xx is a real error, and the last read error
is carried into the timeout message.

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

- `list` emits `--skip <fn>` flags — **one token per line** — which both
  `cluster-smoke` and the nightly use. Nothing is hardcoded in a workflow, so a
  skip cannot outlive its reason. One token per line so the caller can read them
  into an array and pass `"${skips[@]}"`; the older space-separated form forced an
  unquoted `$skips`, which left `SC2086` pointing at the call site inviting an
  edit that stops the tier running (issue #116).
- Both modes take an optional path to the scenarios file, so the parser is
  exercised against a fixture rather than against whatever the tree happens to
  hold — which today is nothing quarantined at all.

### A run that tested nothing is not a pass

`libtest` exits **0** when a filter matches no test: `--exact` naming a scenario
that has since been renamed reports `0 passed; 0 failed` and succeeds. The
nightly matrix names scenarios as literal strings, so that is one rename away at
all times, and the soak would go green having soaked nothing indefinitely.

Both tiers therefore pipe their run through `scripts/assert-scenarios-ran.sh`,
which fails unless at least one test passed. It has a `--self-test` (run from the
`build` job) covering the empty run, the multi-binary sum, and the nightly's
one-scenario floor. Since #104 made `cluster-smoke` a required check, a
green-but-empty run is worse than a red one — the ruleset then certifies what did
not run.
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
  cluster-touching PR buys very little the soak does not already catch, and it
  buys it by taxing every change. If a scenario is suspected flaky, soak that
  one on demand via `workflow_dispatch` rather than making every PR pay for the
  general case.
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

### How `cluster-smoke` is shaped (D-58)

Three jobs, because the required check's name has to stay put while the work
fans out:

| job | what it does |
|---|---|
| `cluster-smoke-prepare` | runs the path filter, then builds **both** image flavors once and uploads them |
| `cluster-smoke-shard` (×4) | loads those images and runs a quarter of the scenarios each |
| `cluster-smoke` | runs nothing; judges the other two — this is the required check |

Two numbers explain the shape, both measured rather than assumed. The tier's
cost fits **≈ 627 s + 33.2 s × N** over ten runs as N grew 17 → 36. The constant
was a cold image build happening *inside* the first scenario's `compose up
--build`, where nothing timed it; it is now `cluster-smoke-prepare`, paid once
and shared. The slope is one full fleet lifecycle per scenario, which sharding
divides rather than removes.

Three things about this are easy to get wrong, so they are all guarded:

- **The shard list is derived**, from `--list` through `scripts/chaos-shard.sh`,
  never written in the workflow. The nightly matrix below is the counter-example
  — it names 24 scenarios where the tier has 36.
- **An empty shard is a failure, not an empty shard.** libtest reads *no*
  filters as "run everything", so a partition that silently emitted nothing
  would run the whole tier in every shard and still look fine. Both the script
  and its caller refuse it, because `mapfile` reading a process substitution
  does not see the script's exit status.
- **The gate is a whitelist.** `scripts/cluster-smoke-gate.sh --self-test` pins
  which combinations pass; `skipped` counts as success only when the filter said
  the tier was not needed.

Per-scenario cost is now recorded (`CHAOS_TIMING_LOG`) and rendered as a step
summary, which is what the count-balanced partition will be replaced by once
there are enough numbers to pack against. Measured: **17–18.5 min** wall clock,
against 35.5 before.

### Where the tier's time actually goes

The first run with `CHAOS_TIMING_LOG` on, over 37 stacks — and it is not what
`start_stack` reads like:

| phase | total | mean | share |
|---|--:|--:|--:|
| scenario bodies | 787 s | — | 49 % |
| **teardown** | **576 s** | **15.6 s** | **36 %** |
| `wait_cluster_formed` | 181 s | 5.0 s | 11 % |
| `up` + `ready` + `down` + ports-free | 66 s | 1.8 s | 4 % |

The per-scenario floor is a **teardown** cost, not a startup one. Two numbers
there are exact enough to name a mechanism, and both are open (D-58):

- teardown clusters at 13–17 s on a `stop_grace_period: 15s`, where a node that
  drained inside its 5 s leave timeout should cost ~5–6 s. Most containers look
  like they ride the grace period out and are SIGKILLed. Worth a diagnosis
  before a faster teardown flag, since the same shape would cost a Kubernetes
  rolling update the full grace period per pod. **Not D-56's mechanism**: two
  runs of this branch differing only by #513 under the second measured
  teardown at 15.6 s and 15.2 s, 37 stacks each — no change. Still
  unaccounted for.
- `wait_cluster_formed` is 5.0 s on every one of 36 stacks — the voter gauge's
  resample interval, which that function's own doc comment names. It measures
  the sampler, not convergence.
