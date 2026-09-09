# Chapter 12 — Testing & Correctness

A distributed mock server has one unforgivable failure mode: silently telling
a test suite something false. So correctness here is not a QA phase — it is a
set of machine-checkable exit criteria per phase, a chaos suite that attacks
the exact windows Chapters 6 and 9 documented, and standing gates that keep
the single-node experience sacred.

## The harness

`tests/cluster-chaos/` (issue #11; its `README.md` is the harness's own
guide): the shipped `deploy/compose/docker-compose.yml` itself, stacked with
overlays that add an Envoy front, `toxiproxy` between nodes, a front door,
tenancy — driven by a Rust integration binary
(`tests/scenarios.rs`) with a scenario DSL:

```rust
let c = cluster.start(3).await;
c.node("a").post_imposter(cfg).await?;
c.assert_converged(8080, within_secs(5)).await?;
c.partition(&["a"], &["b", "c"]).await?;      // toxiproxy
c.kill("b").await?;                            // SIGKILL
c.sigterm("b").await?;                         // graceful leave
c.heal().await?;
```

Two rules with teeth: **every assertion reads the admin API or the correctness
instrumentation, never logs** (logs are for humans; contracts are for machines) —
membership, leadership and bind state come from `GET /_fleet/members`, and the
`rift_cluster_*` counters that survive D-71 (#548) carry what no state endpoint
can answer, a count of things that happened — and **invariant
violations never auto-retry** — an infra flake retries once, but a violated
invariant files a bug, and persistently flaky scenarios get quarantined behind
an issue rather than deleted. CI budget: at PR time every scenario runs
**once**, across four `cluster-smoke-shard` jobs that share one prebuilt image
(D-58); `cluster-smoke` itself is the required status check (#104 — a merge may
not outrun it) and does no testing, it judges whether the shards ran. The
nightly soak (`nightly-chaos.yml`) iterates each scenario 60–100× under a 2 h
cap. Both cadences are deliberate deviations from RFC-001 §12's 3×/100× bars,
recorded with their reasoning in the harness README.

## Phase exit criteria (functional)

Phase 1 — membership + config-sync (the write path of Chapter 4):

| Test | Pins down |
|---|---|
| `test_config_sync_converges` | R1: `POST` on A → served on B/C within the barrier |
| `test_reconcile_preserves_state` | Incremental apply: sibling-port change preserves scenario state |
| `test_reconcile_reorder` | Stub order is match priority — reorders apply as moves, not resets |
| `test_node_rejoin` | Kill → rejoin → catch-up, fleet unaffected |
| `test_no_seeds_not_ready` | A node that can't join never tells the LB it's healthy |
| `test_cold_start` | R3 for config: full restart restores everything incl. deletions |
| `test_graceful_leave` | Rolling restart: zero survivor errors, **zero lost acknowledged writes** (a CAS-ladder driven across each leave) |

Phase 2 adds the flow-state suite (`test_scenario_cluster_linear` — 10k
round-robin transitions with zero illegal/lost updates,
`test_scenario_handoff`, `test_flow_read_strong_default`,
`test_flow_state_survives_full_restart`, `test_flow_state_async_loss_bound`);
Phases 4–5 the strict sequencing and proxyOnce suites
(`test_sequence_redis_strict`, `test_proxy_once_*` including the documented
duplicate bound under owner kill). These phase-2+ names are RFC-001 §10's
*planned* names and no test exists under them: the claims landed in the
container tier as C15 (flow state) and C10–C11 (proxyOnce) below. All four of
RFC-001 §10's phase-3 journal names (`test_journal_merge_exact`,
`test_journal_clear`, `test_count_merge`, `test_journal_cursor_merge`) are
permanently unallocated — D-74 (#552) removed the fleet journal merge they were
to pin, `test_count_merge`'s claim (`numberOfRequests` = N on every node) is the
one D-74 reverses outright, and a per-node journal is upstream's own suite's
business. What replaced them is per-node and lives in
`crates/rift-cluster-server/tests/write_path.rs`:
`number_of_requests_is_the_answering_nodes_own_count_not_a_fleet_sum`,
`a_proxied_clear_empties_only_the_journal_of_the_node_it_reached` and the two
`Rift-Cluster-Partial` pins beside them.
The sequencing claims landed as **C33** (#476), which is the
container-tier counterpart to the whole of `rift-cluster`'s `tests/sequencer.rs`
gate; `test_sequence_redis_strict` itself stays unwritten, and now permanently
so — D-15 killed the Redis-backed design it names, and D-47 shipped
owner-routing in its place.

## The chaos suite

> **Amended by D-66** (2026-08-29, #529): C10's duplicate-upstream acceptance is now one origin
> call per **chargeable** fire — every fire except those the claim gate refused with a `503`,
> which are never forwarded. The former `1 + refires` bound scaled with the length of the outage
> rather than with anything the contract promises.

Each scenario targets a specific claim made earlier in this guide. The
registry below is rebuilt from the harness — `tests/cluster-chaos/tests/scenarios.rs`
is the source of truth, and every scenario it knows has a row. Status lives in
the ID cell: **✅ implemented** (the scenario function is named in the Attack
cell), **⛔ quarantined** (`#[ignore = "quarantined: #n -- why"]`, harvested by
`scripts/chaos-quarantine.sh` into the tier's `--skip` list; nothing is
quarantined today), **📋 planned** (the issue or RFC section that owns it).
C2, C3 and C9 are RFC-001 §12 numbers never carried into this chapter (C3's
claim is C11's; **C2's — "kill sequence-owner mid-traffic" — is C33's**, minus
the "(gossip mode)" its wording assumed, which died with D-15 and was replaced
by ring ownership in D-47; C9's fork class stopped being a scenario that can
fail under D-15) and stay unallocated.

| ID | Attack | Invariant it must fail to break |
|---|---|---|
| C1 📋 planned (RFC-001 §12; no issue filed) | Partition during scenario traffic, heal at 30 s | No illegal FSM transition either side; reachable-side keys stay serialized; rejected ops are 503s, not stale answers |
| C4 ✅ | Config writes on both sides of a partition (#73: `c4_partition_parks_minority_writes_and_replays_on_heal`) | Minority: 503 + parked op-id; majority commits; heal replays with **zero lost acks, zero double-applies** (op-dedup) |
| C5 ✅ | Rolling SIGTERM restarts under load (#72/#11: `c5_rolling_restart_never_stops_accepting_writes`) | Zero survivor data-plane errors; zero owner-unreachable windows (graceful leave); CAS ladder loses nothing; writes resume within `WRITES_RESUME_BOUND` after each roll |
| C6 ✅ | 30% connection resets + 100 ± 100 ms jitter on the cluster port via toxiproxy, 60 s (#73/#94: `c6_loss_and_jitter_do_not_flap_or_lose_writes`) | Leadership transitions bounded by **rate** (`C6_MAX_LEADER_TRANSITIONS`), not count — the jitter overlaps the election timeout by design; no false membership changes; zero lost acknowledged writes |
| C7 ✅ | Node joins with stale/empty disk (#73: `c7_joining_node_serves_nothing_until_reconciled`) | Serves nothing until caught up; then byte-identical config |
| C8 📋 planned (RFC-001 §12; no issue filed) | Round-robin scenario traffic, healthy fleet, no affinity | Zero stale-read matches — the owner-read guarantee under the worst LB |
| C10 ✅ | Kill claim-owner AND config-leader at proxyOnce's two critical moments (#228: `c10_proxy_once_survives_owner_and_leader_kills`) | Duplicate upstream calls ≤ **1 + ownership changes in the phase** — the contract's own bound, not a bound derived from the outage; refusals during the outage are counted and printed as the run's artifact (D-66); **zero wedged signatures**; failed publication releases the claim; a replaying signature shows its stub on every node |
| C11 ✅ | Concurrent proxy recording on 3 nodes (#228: `c11_concurrent_recording_loses_nothing`) | Exactly one recorded stub per proxyOnce signature fleet-wide; zero upstream calls once Recorded; proxyAlways never replays and merges every recording (an `InFlight` racer forwards-without-recording *by upstream design*, so the racing-window call count is measured, not pinned). "Loses nothing" is asserted **per node** since D-74 (#552): each node's own `GET /imposters/:port/requests` accounts for every request that node served, rather than the three nodes' logs being diffed against one fleet-merged answer, which no longer exists. A raced `proxyOnce` request answers `200` **or** `503` — the latter when the cluster could not serialize its claim in that instant (D-66), which under load is a normal outcome and not a failure; `proxyAlways` stays strictly `200` as the control |
| ~~C12~~ | ±5 s clock skew across nodes, asserting that journal clears were generation-based and therefore clock-free — **removed by D-74 (#552)** with the clear generations it exercised. It was the only user of `faketime.overlay.yml` and of the `runtime-faketime` image | — |
| C13 📋 planned (RFC-001 §12; no issue filed) | Owner black-holes while 20% of load is stateful | **Stateless p99 < 5 ms throughout** — the bridge + fast-fail firewall |
| C14 ✅ | Kill the Raft leader during a 100-write admin storm (#11: `c14_leader_kill_keeps_every_acknowledged_write`) | Every write acked-and-present or 503-with-op-id-then-present; zero duplicates; writes resume within `WRITES_RESUME_BOUND` (measured as write availability, not off the ~5 s leader gauge) |
| C15 ✅ | `kill -9` the entire fleet under load, restart (#11: `c15_hard_kill_of_the_whole_fleet_keeps_acknowledged_writes`; #121: `c15_flow_state_survives_a_full_cluster_restart`) | Configs/tenancy/intents identical to last ack; flow state per durability level (`sync` = exact; `async` ≤ one fsync interval) — four flows stepped across different nodes resume at exactly the next integer |
| C16 ✅ | 250 ms constant-latency toxic on one follower's inbound cluster link, then data-plane reads through that follower (#102: `c16_pull_on_miss_rescues_lagging_follower`) | The pull-on-miss safety net (#49) rescues the read within its 500 ms budget: the `rift-cluster-pull-on-miss: rescued-wait` header is only set on the lagged-then-caught-up path, so it is the proof the node lagged; zero jitter keeps leadership untouched |
| C17 ✅ | Route-table write on one node, twice — create, then retarget the same route id (#132: `c17_routes_converge`) | Dispatch through the two nodes that never saw the write succeeds **the moment the write returns 2xx**, no polling — the barrier's return is the assertion (R1 for routes) |
| C18 ✅ | Full-fleet stop/start with a three-route table: exact host, wildcard host, path prefix (#132: `c18_routes_survive_a_full_cluster_restart`) | After restart every node passes two separate checks: `GET /front-door/routes` (the stored table) **and** a real dispatch of all three shapes through its own front door (the rebuilt in-memory table) |
| C19 ✅ | A `socat` sidecar squats an imposter's port inside rift-2's network namespace, confirmed held before the write (#143: `c19_front_door_routes_around_bind_divergence`) | The write is 201; config converges fleet-wide; `bind_failures` on rift-2's `GET /_fleet/members` names the squatted port; a route to the squatted port dispatches 2xx through **rift-2's own** front door, and through rift-1's |
| ~~C24~~ · ~~C25~~ · ~~C27~~ | The RBAC ladder, key revocation across a partition, and tenancy isolation — **removed by D-73 (#550)** with the tenancy they asserted. The one claim of C27's that outlived tenancy (the data plane is never credentialed) is now `the_gateway_stays_open_and_never_carries_the_admin_key` in `tests/fleet_session.rs` | — |
| C26 ✅ | Write imposters through every node, then lag a follower past `RIFT_CLUSTER_SNAPSHOT_LOG_ENTRIES` with fifteen more so the leader snapshots and purges, restart it (a real `install_snapshot`, asserted from `rift_cluster_snapshots_installed_total`); then full-fleet stop/start (#165/#183, re-targeted by D-71: `c26_replicated_imposters_survive_a_full_cluster_restart_by_snapshot_install`) | The restarted follower converges to the live nodes' `(port, revision, stubs)` imposter rows; after the full-fleet restart every node's rows are byte-identical to its own pre-restart ones and to every other node's |
| ~~C28~~ · ~~C29~~ · ~~C30~~ | Journal exactness under a node kill, partial-read latency and counting under partition, and the vector-cursor walk across a membership change — **removed by D-74 (#552)** with the fleet journal merge they asserted. A per-node journal has no merge to be exact about, no fan-out to be partial, and no vector cursor to walk | — |
| C33 ✅ | Owner-mode sequencing sprayed round-robin across all three nodes; SIGKILL the cursor's owner (found by killing — a non-owner's death must change nothing), then restart it (#476: `c33_owner_mode_sequencing_cycles_fleet_wide_and_degrades_on_owner_kill`) | Strict `A, B, C` cycling with **zero fallbacks** on a healthy fleet; with the owner dead every response is still a 2xx and carries `rift-cluster-sequence: local-fallback`, and the **fallback counter** — not the returned index — is what moves; after the owner returns the fallbacks stop and strict cycling resumes from wherever the new cursor started (D-8 permits the reset) |

C14 and C15 are the direct tests of R4 and R3; C8 is the direct test of R2's
LB-independence; C4+C5 together are R1 under adversity.

## Standing gates (never waived, any phase)

- **Single-node fidelity**: the entire upstream test suite runs against
  `rift-cluster-server` with `--cluster` off — byte-identical behavior required.
- **Hot-path performance**: `matcher_bench` within 2% of the pre-seam
  baseline; clustering compiled in but disabled must be free.
- **SDK conformance**: the four language SDKs' conformance suites pass against
  a clustered fleet — the admin API contract (envelopes, cursors, SSE,
  `Rift-Cluster-*` headers as additive-only) holds from a client that didn't
  read this guide.
- **Degraded-ops zero**: strict-mode harness runs assert
  `rift_cluster_degraded_ops_total == 0` and no `Rift-Cluster-Partial` — the
  cluster may only degrade when something is actually wrong. (That family is
  designed but not registered — Chapter 10 — so this gate is not yet
  enforced; C13, the scenario that would drive it, is unbuilt.)

## Where a measured figure ends up

> **Amended by D-67** (2026-08-29, #534): the rows above have said "printed as the run's artifact"
> since the first of them was written, and on a **passing** run none of those figures was printed
> anywhere a human could read. libtest captures a passing test's stdout and `cluster-smoke` passed
> no `--nocapture`, so an artifact surfaced only when its scenario *failed* — the one case where the
> number is worthless, the run having aborted before settling it. This section is new, and states
> where a measured figure actually lands now.

Several rows above say a bound is *measured and printed* rather than asserted
against a guessed constant — C10's duplicate-upstream ceiling, C11's racing-window
call counts. Those scenarios call `chaos_artifact!`, which does two things:

- **Prints the line.** `cluster-smoke` runs the tier with `--nocapture`, so it
  lands in the job log beside the scenario that produced it. Until #534 the flag
  was missing and libtest captured a *passing* test's stdout — so the figure
  reached a human only when the scenario **failed**, which is when it is least
  useful, the run having aborted before settling the measurement.
- **Appends it to `$CHAOS_ARTIFACT_LOG`.** Rendered as a step summary table per
  shard and uploaded as `chaos-artifacts-<shard>`, alongside the `CHAOS_TIMING_LOG`
  it mirrors.

The second is not a convenience. A bound that drifts *inside* its own ceiling —
duplicate upstream calls creeping from 2 to 4 while still under C10's assertion —
is invisible to the assertion by construction, and that early warning is the whole
reason these figures are printed rather than merely checked. Reading a trend needs
the numbers side by side across runs, which a thirty-minute log is not.

Unset, the collector is a clock read and a `println!` and nothing else, so a local
run is unchanged: `cargo test -p cluster-chaos -- --ignored --nocapture --exact <name>`.

## Verification philosophy, in one paragraph

Every guarantee in this guide names the test that would catch its violation,
and every documented window (adoption staleness, async fsync interval,
duplicate-claim bound) is *measured* by the suite, not just asserted in prose.
When an invariant and an implementation disagree, the invariant wins and the
implementation changes; when a window turns out wider than documented, the
documentation changes loudly. That contract — tested honesty — is the actual
product.
