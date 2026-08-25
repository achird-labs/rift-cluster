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
sources, tenancy, clock skew — driven by a Rust integration binary
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

Two rules with teeth: **every assertion reads the admin API or metrics, never
logs** (logs are for humans; contracts are for machines), and **invariant
violations never auto-retry** — an infra flake retries once, but a violated
invariant files a bug, and persistently flaky scenarios get quarantined behind
an issue rather than deleted. CI budget: the PR-time `cluster-smoke` job runs
each scenario **once** (a required status check since #104 — a merge may not
outrun it), and the nightly soak (`nightly-chaos.yml`) iterates each scenario
60–100× under a 2 h cap. Both are deliberate deviations from RFC-001 §12's
3×/100× bars, recorded with their reasoning in the harness README.

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
Phase 3 the journal suite (`test_journal_merge_exact`, clock-skew-immune
`test_journal_clear`, `test_journal_cursor_merge` for vector cursors); Phases
4–5 the strict sequencing and proxyOnce suites (`test_sequence_redis_strict`,
`test_proxy_once_*` including the documented duplicate bound under owner
kill). These phase-2+ names are RFC-001 §10's *planned* names and no test
exists under them: the claims landed in the container tier as C15 (flow
state), C28–C30 (journal — the scenarios' own doc comments name which
`test_journal_*` claim each is "in anger") and C10–C11 (proxyOnce) below.
The sequencing names have no counterpart yet.

## The chaos suite

Each scenario targets a specific claim made earlier in this guide. The
registry below is rebuilt from the harness — `tests/cluster-chaos/tests/scenarios.rs`
is the source of truth, and every scenario it knows has a row. Status lives in
the ID cell: **✅ implemented** (the scenario function is named in the Attack
cell), **⛔ quarantined** (`#[ignore = "quarantined: #n -- why"]`, harvested by
`scripts/chaos-quarantine.sh` into the tier's `--skip` list; nothing is
quarantined today), **📋 planned** (the issue or RFC section that owns it).
C2, C3 and C9 are RFC-001 §12 numbers never carried into this chapter (C3's
claim is C11's; C9's fork class stopped being a scenario that can fail under
D-15) and stay unallocated.

| ID | Attack | Invariant it must fail to break |
|---|---|---|
| C1 📋 planned (RFC-001 §12; no issue filed) | Partition during scenario traffic, heal at 30 s | No illegal FSM transition either side; reachable-side keys stay serialized; rejected ops are 503s, not stale answers |
| C4 ✅ | Config writes on both sides of a partition (#73: `c4_partition_parks_minority_writes_and_replays_on_heal`) | Minority: 503 + parked op-id; majority commits; heal replays with **zero lost acks, zero double-applies** (op-dedup) |
| C5 ✅ | Rolling SIGTERM restarts under load (#72/#11: `c5_rolling_restart_never_stops_accepting_writes`) | Zero survivor data-plane errors; zero owner-unreachable windows (graceful leave); CAS ladder loses nothing; writes resume within `WRITES_RESUME_BOUND` after each roll |
| C6 ✅ | 30% connection resets + 100 ± 100 ms jitter on the cluster port via toxiproxy, 60 s (#73/#94: `c6_loss_and_jitter_do_not_flap_or_lose_writes`) | Leadership transitions bounded by **rate** (`C6_MAX_LEADER_TRANSITIONS`), not count — the jitter overlaps the election timeout by design; no false membership changes; zero lost acknowledged writes |
| C7 ✅ | Node joins with stale/empty disk (#73: `c7_joining_node_serves_nothing_until_reconciled`) | Serves nothing until caught up; then byte-identical config |
| C8 📋 planned (RFC-001 §12; no issue filed) | Round-robin scenario traffic, healthy fleet, no affinity | Zero stale-read matches — the owner-read guarantee under the worst LB |
| C10 ✅ | Kill claim-owner AND config-leader at proxyOnce's two critical moments (#228: `c10_proxy_once_survives_owner_and_leader_kills`) | Duplicate upstream calls ≤ measured bound (printed as the run's artifact); **zero wedged signatures**; failed publication releases the claim; a replaying signature shows its stub on every node |
| C11 ✅ | Concurrent proxy recording on 3 nodes (#228: `c11_concurrent_recording_loses_nothing`) | Exactly one recorded stub per proxyOnce signature fleet-wide; zero upstream calls once Recorded; proxyAlways never replays and merges every recording (an `InFlight` racer forwards-without-recording *by upstream design*, so the racing-window call count is measured, not pinned) |
| C12 ✅ | ±5 s clock skew across nodes via `faketime.overlay.yml` (#228: `c12_clears_are_exact_under_clock_skew`) | Clears exact (generation-based, clock-free): a fast-clock clear erases fleet-wide, every post-clear append survives, racing skewed clears converge; the skew itself is proven real before any probe runs |
| C13 📋 planned (RFC-001 §12; no issue filed) | Owner black-holes while 20% of load is stateful | **Stateless p99 < 5 ms throughout** — the bridge + fast-fail firewall |
| C14 ✅ | Kill the Raft leader during a 100-write admin storm (#11: `c14_leader_kill_keeps_every_acknowledged_write`) | Every write acked-and-present or 503-with-op-id-then-present; zero duplicates; writes resume within `WRITES_RESUME_BOUND` (measured as write availability, not off the ~5 s leader gauge) |
| C15 ✅ | `kill -9` the entire fleet under load, restart (#11: `c15_hard_kill_of_the_whole_fleet_keeps_acknowledged_writes`; #121: `c15_flow_state_survives_a_full_cluster_restart`) | Configs/tenancy/intents identical to last ack; flow state per durability level (`sync` = exact; `async` ≤ one fsync interval) — four flows stepped across different nodes resume at exactly the next integer |
| C16 ✅ | 250 ms constant-latency toxic on one follower's inbound cluster link, then data-plane reads through that follower (#102: `c16_pull_on_miss_rescues_lagging_follower`) | The pull-on-miss safety net (#49) rescues the read within its 500 ms budget: the `rift-cluster-pull-on-miss: rescued-wait` header is only set on the lagged-then-caught-up path, so it is the proof the node lagged; zero jitter keeps leadership untouched |
| C17 ✅ | Route-table write on one node, twice — create, then retarget the same route id (#132: `c17_routes_converge`) | Dispatch through the two nodes that never saw the write succeeds **the moment the write returns 2xx**, no polling — the barrier's return is the assertion (R1 for routes) |
| C18 ✅ | Full-fleet stop/start with a three-route table: exact host, wildcard host, path prefix (#132: `c18_routes_survive_a_full_cluster_restart`) | After restart every node passes two separate checks: `GET /front-door/routes` (the stored table) **and** a real dispatch of all three shapes through its own front door (the rebuilt in-memory table) |
| C19 ✅ | A `socat` sidecar squats an imposter's port inside rift-2's network namespace, confirmed held before the write (#143: `c19_front_door_routes_around_bind_divergence`) | The write is 201; config converges fleet-wide; `rift_cluster_bind_failures{port}` reads 1 on rift-2; a route to the squatted port dispatches 2xx through **rift-2's own** front door, and through rift-1's |
| C20 ✅ | One `POST /admin/sources/:id/pull` against a counting origin, then an unchanged second pull (#137: `c20_source_pull_converges_and_fetches_once`) | Converges fleet-wide with provenance on every node **and** the origin served exactly **one** request (`== 1`, never `>= 1`); the second pull fetches once more, writes nothing, answers `unchanged: true` |
| C21 ✅ | `kill -9` the leader while a `tracking` source polls (#137: `c21_tracking_poll_is_leader_only_and_survives_failover`) | Origin request **rate** matches one poller (3–12 per 40 s window), not three; after re-election the rate resumes and a content change converges; `rift_cluster_source_polls_total` summed fleet-wide equals what the origin served |
| C22 ✅ | Full-fleet SIGTERM/restart with declared sources (#137: `c22_sources_survive_a_full_cluster_restart`) | Every node keeps its source records, port provenance and the replicated `drifted` flag; a post-restart pull short-circuits on the unchanged digest without moving `last_applied` |
| C23 ✅ | Hand-edit a source-owned imposter on one node (#137: `c23_drift_flags_and_pull_overwrites`) | `drifted: true` on **every** node; the next pull (`onDrift: overwrite`) restores the declared content in every node's committed config |
| C24 ✅ | One Viewer bound in tenant `acme`; RFC-002 §4.1 action matrix through all three nodes (#165: `c24_rbac_enforcement_is_identical_through_any_node`) | Every verdict identical **including the body**; vacuity guards require the matrix to contain a 200, a 403 and a 404 |
| C25 ✅ | Revoke a key's binding while one node is partitioned, then heal (#165: `c25_key_revocation_survives_a_partition`) | The minority cannot perform the authorization write (503/504); the **first** request through the healed node is refused; the convergence window is measured and bounded |
| C26 ✅ | Lag a follower past `RIFT_CLUSTER_SNAPSHOT_LOG_ENTRIES` so the leader snapshots and purges, restart it (a real `install_snapshot`, asserted from `rift_cluster_snapshots_installed_total`); then full-fleet stop/start (#165/#183: `c26_audit_chain_survives_a_full_cluster_restart`) | Every node's `(revision, action, resource)` audit projection is byte-identical to its own pre-restart one and to every other node's |
| C27 ✅ | Two tenants, one imposter each; each tenant's Editor reads/manages both (#165: `c27_tenancy_isolates_ownership_but_not_the_data_plane`) | Own imposter 2xx; the other tenant's 404 **byte-identical** to a port that does not exist; both imposters answer unauthenticated data-plane traffic through every node, even with a bogus credential |
| C28 ✅ | SIGKILL a follower after a fleet-wide spray (#228: `c28_fleet_journal_is_exact_under_node_kill`) | Every survivor answers **exactly N** with the dead shard cache-served — honestly stamped partial while its writer is unreachable; fleet count exact throughout; on return the stamp clears with the survivors still exact — the returned node itself converges on N minus its own lost shard, unstamped (the #349 honesty gap, pinned as-is) |
| C29 ✅ | Partition one node mid-traffic (#228: `c29_partial_reads_answer_within_budget_and_count_themselves`) | Both sides answer **within the 2 s peer budget** (measured, printed); `rift_cluster_journal_partial_reads_total` moves; heal clears the stamp and converges the sets |
| C30 ✅ | Kill and restart a node mid-cursor-walk; overflow a shard past its cap (#228: `c30_vector_cursor_walk_survives_membership_change`) | The `?since=` walk stays gapless and duplicate-free across the kill and the return; `x-rift-truncated` appears **iff** a presented position predates a shard watermark (baseline reads never truncate) |
| C31 📋 planned (#283) | Deploy a spec and flip its validation policy; full-cluster `kill -9` + restart (RFC-004 §9, S7) | Both converge on every node and are identical after recovery; a retried, unchanged `PUT /specs/:id` during recovery grows the log by zero |
| C32 📋 planned (#283) | Mode `hard`, round-robin across all nodes while a follower restarts (RFC-004 §9, S7) | An off-contract request is rejected with the same status and shape through **any** node; an on-contract one is served through any node; violation rows land on the serving node and the merged `validationFailures` read reflects them, `Rift-Cluster-Partial`-honest if a node is down |

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

## Verification philosophy, in one paragraph

Every guarantee in this guide names the test that would catch its violation,
and every documented window (adoption staleness, async fsync interval,
duplicate-claim bound) is *measured* by the suite, not just asserted in prose.
When an invariant and an implementation disagree, the invariant wins and the
implementation changes; when a window turns out wider than documented, the
documentation changes loudly. That contract — tested honesty — is the actual
product.
