# Chapter 12 — Testing & Correctness

A distributed mock server has one unforgivable failure mode: silently telling
a test suite something false. So correctness here is not a QA phase — it is a
set of machine-checkable exit criteria per phase, a chaos suite that attacks
the exact windows Chapters 6 and 9 documented, and standing gates that keep
the single-node experience sacred.

## The harness

`tests/cluster/` (issue #11): a 3-node docker-compose fleet with an Envoy
front and `toxiproxy` between nodes, driven by a Rust integration binary with
a scenario DSL:

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
an issue rather than deleted. CI budget: a ~20-minute PR smoke (3 iterations
of each phase-relevant scenario, parallel stacks) and a nightly 100-iteration
soak.

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
kill).

## The chaos suite

Each scenario targets a specific claim made earlier in this guide:

| ID | Attack | Invariant it must fail to break |
|---|---|---|
| C1 | Partition during scenario traffic, heal at 30 s | No illegal FSM transition either side; reachable-side keys stay serialized; rejected ops are 503s, not stale answers |
| C4 | Config writes on both sides of a partition | Minority: 503 + parked op-id; majority commits; heal replays with **zero lost acks, zero double-applies** (op-dedup) |
| C5 | Rolling SIGTERM restarts under load | Zero survivor data-plane errors; zero owner-unreachable windows (graceful leave); CAS ladder loses nothing |
| C6 | 30% packet loss on the cluster port, 60 s | No leader flapping, no false membership changes, zero degraded data-plane ops |
| C7 | Node joins with stale/empty disk | Serves nothing until caught up; then byte-identical config |
| C8 | Round-robin scenario traffic, healthy fleet, no affinity | Zero stale-read matches — the owner-read guarantee under the worst LB |
| C10 | Kill claim-owner AND config-leader at proxyOnce's two critical moments | Duplicate upstream calls ≤ documented bound; **zero wedged signatures**; failed publication releases the claim |
| C11 | Concurrent proxy recording on 3 nodes | Zero lost recorded stubs (they ride the Chapter 4 write path) |
| C12 | ±5 s clock skew across nodes | Clears exact (generation-based, clock-free); HMAC window behaves per spec |
| C13 | Owner black-holes while 20% of load is stateful | **Stateless p99 < 5 ms throughout** — the bridge + fast-fail firewall |
| C14 | Kill the Raft leader during a 100-write admin storm | Every write acked-and-present or 503-with-op-id-then-present; zero duplicates; new leader ≤ 3 s |
| C15 | `kill -9` the entire fleet under load, restart | Configs/tenancy/intents identical to last ack; flow state per durability level (`sync` = exact; `async` ≤ one fsync interval) |

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
  cluster may only degrade when something is actually wrong.

## Verification philosophy, in one paragraph

Every guarantee in this guide names the test that would catch its violation,
and every documented window (adoption staleness, async fsync interval,
duplicate-claim bound) is *measured* by the suite, not just asserted in prose.
When an invariant and an implementation disagree, the invariant wins and the
implementation changes; when a window turns out wider than documented, the
documentation changes loudly. That contract — tested honesty — is the actual
product.
