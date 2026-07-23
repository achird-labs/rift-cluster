# RFC-001 — Self-Clustering Distributed Rift (v3.1)

| | |
|---|---|
| **Status** | v3.1 (re-grounded at v0.15.0; control plane decided by ADR-001) — implementation-ready |
| **Tracking issue** | [achird-labs/rift-enterprise#1](https://github.com/achird-labs/rift-enterprise/issues/1) |
| **Canonical location** | `rift-enterprise:docs/rfc/RFC-001-self-clustering-rift.md` |
| **Ground truth** | All code citations resolve against `vendor/rift` @ `aaa6042` (v0.15.0). `imposter/core.rs` was split upstream into the `imposter/core/{mod,matching,lifecycle,recording,responses,proxy}.rs` module tree and the crate renamed `rift-core` → `rift-mock-core`; line-number citations below are approximate against v0.15.0. |
| **Author** | Mohsen Zainalpour |
| **Date** | 2026-07-01 (v3: 2026-07-21; v3.1: 2026-07-22) |

**Changelog v3 → v3.1** (normative surfaces re-aligned with the issues that implement them)

v3 put supersession banners on §7.1/§7.2/§7.4 but left two normative surfaces carrying
v2-era gossip semantics that contradicted decisions already recorded in the implementation
issues — an implementer reading only the RFC would have built the wrong thing.

- **§7.6 aligned with ADR-001, #9 and #16.** The blanket *"this design has no quorum"* is
  corrected — the **control plane** has one, the data plane does not, and the section now
  says which is which. **Both** config-write rows drop the `local` / `(g,revision,origin)`
  heal-merge option: admin writes have exactly **one** degraded behaviour, `503` + op-id,
  durably parked, auto-replayed (#9) — and a **proxy recording is a config write**, so its
  row now says so instead of offering a merge. The **flow-KV read** row becomes
  owner-authoritative by default with `readConsistency: "local"` as a per-imposter opt-in
  (#16 Gap C); §7.2.4's cross-text follows. `--cluster-degraded-mode` is scoped to
  **data-plane features** (flow state, Phase-4 sequences, Phase-5 proxyOnce) — matching
  #9's flag decisions, and explicitly not config writes.
- **§7.5.1 gained the journal vector cursor and SSE design**, which v3 omitted entirely: a
  `(node_id → shard_seq)` map as the **opaque** `since` token (the admin layer round-trips
  it and never parses it), clear-generation staleness detection, eviction-overtakes-cursor
  handling, partial-shard handling, and the two acceptable SSE shapes with their honest
  one-anti-entropy-interval latency bound. This needs a method U-4 does not carry, so
  **U-13 (`RequestJournal::read_since`) is recorded as a queued seam** in Appendix B rather
  than assumed. New Phase-3 exit criterion `test_journal_cursor_merge` (§10), stated **per
  clear generation** — a cross-clear concatenation is supposed to exceed a final full read,
  so asserting equality across a clear would have been unsatisfiable by construction.
- **§11.3 states the sequencer cost honestly:** one `next` **plus up to a few `peek`s** per
  request, since a template referencing the sequence several times pays per reference. The
  RPC fan-out is stated as **inherent** — the only cache that could remove a round trip
  would sit on the calling node, where it would reintroduce the stale read the design
  exists to prevent; an owner-side cache bounds owner-side work, not RPC count.
- **§12 re-synced with the harness (#11), which inherits its inventory from here.** C4 is
  rewritten per ADR-001 (park-and-replay, not merge-arbitration); C5 gains the
  leader-hand-off bound (≤ 3 s per roll) it was missing; C6 is redefined from gossip-era
  UDP drop to toxiproxy TCP loss/jitter, bounding the leadership **rate** rather than the
  count (#94); **C14/C15 added** from #9. The harness description is corrected to the
  two-tier split that shipped, including why partitions are not toxiproxy.
- **§10's Phase-1 row was malformed** — six cells in a five-column table, whose duplicate
  exit-criteria cell listed a *different* chaos set (no C14/C15). Repaired, so §10 and §12
  now show one Phase-1 chaos list rather than two.
- **§7.3 gained a supersession banner** scoped to its two *config* rows — it was the last
  section on the config path without one, and this pass made the contradiction sharper by
  having §7.6 say "forward to the Raft leader" two pages after §7.3 still said
  "port-config owner … assign `(g, revision)`". Its data-plane rows are untouched.
- **§7.5.3 pins the proxy-recording `op_id` to `(port, signature)`.** Making the config
  path park-and-replay changed a proxyOnce invariant: a parked write has not *failed*, so
  releasing the claim and letting a retry re-record would produce two op-ids for one
  signature — two upstream calls, which #9's dedup would not collapse and §7.5.3's stated
  duplicate bound does not cover. A signature-derived op-id makes retry and replay the same
  operation.
- **§11.1 marks three metrics as not surviving ADR-001** —
  `rift_cluster_config_conflicts_total` (counts a merge conflict now unrepresentable),
  `rift_cluster_settle_waits_total`, `rift_cluster_gossip_lag_seconds` — rather than
  leaving a dashboard silently reading zero.
- **Small verifies:** achird-labs/rift#800 recorded as a dependency-not-blocker for the
  backend-error door (§7.4 banner); the SDK conformance suite (upstream epic #458) named in
  the §10 Phase-1 exit criteria and §12 regression bars; `--cluster-off` parity tracked
  as #37.

**Changelog v2 → v3** (re-grounding + control-plane decision)

- **The requirements are now explicit (R1–R4).** Fleet-wide read-after-write config visibility
  (R1), immediate cross-node flow-state visibility (R2), full-cluster-restart durability of
  configs *and* flow state (R3), and no-lost-admin-requests (R4). §1.1 states them; they are
  what drove the control-plane decision below.
- **Control plane decided — ADR-001 (accepted).** The cluster control plane is an **embedded
  Raft group** (`openraft`, in-process over the HMAC cluster port) with `redb` for durability;
  flow state stays off consensus (HRW + WAL). **This supersedes the gossip mechanism in §7.1,
  §7.2 and §7.4** (and decisions D-1, D-2). Those three sections carry a superseded-by-ADR-001
  banner and are retained for context and for the parts that survive unchanged (the ownership
  *contract*, the partition table, the seam usage); their *mechanism* is ADR-001's. New decision
  entries D-15/D-16/D-17 in Appendix C.
- **Phase 0 is complete.** All eight upstream seams shipped as `achird-labs/rift#311–#318`
  (v0.14.0); Appendix B is now a *filed-and-merged* mapping, not a to-file list. The Phase-0
  kill gate is discharged. The transport substrate (internal RPC + sync/async bridge, #8) is
  merged (`rift-enterprise#21`).
- **Re-grounded at v0.15.0.** Crate rename and module split applied throughout (see Ground
  truth). Two new upstream facts folded in: the per-core runtime topology (RFC-712) is
  incompatible with the sync bridge and is **rejected at startup** (D-14); the error envelope
  gained a stable `type` slug (#797), so §7.6's degraded responses use the existing
  `ErrorKind` slugs (`unavailable`, `timeout`) rather than inventing cluster error types.
- **Two new capabilities from the wrapper (Mimemo/Solo) analysis:** the **front door** —
  single-port content-based routing to imposters (upstream seam U-11, #19) — and the
  **imposter source SPI** — pluggable providers with cluster-correct pulls (U-12, #20). Added
  to §2/§6.3 and the Appendix A seam inventory.
- **RFC-002 carved out.** Multi-tenancy + RBAC + audit is its own RFC (#17), referenced from
  §11.2; it was absent from v2 and from upstream.

**Changelog v1 → v2**

- Corrected three factual claims: hot-reload is *not* atomic (§3.4); Redis `set_ttl` is a
  logging no-op (§3.2); tracking issue is rift-enterprise#1, not rift#301.
- Added the missing cornerstone: an **OSS seam plan** (§8, Appendix A/B) — v1 named a trait
  family but no injection path exists in rift today; without upstream seams the enterprise
  repo cannot plug in clustered backends at all.
- Replaced "config-sync via existing atomic hot-reload" (which would reset all cluster state
  on every change) with **owner-serialized writes + digest gossip + anti-entropy fetch +
  two-level incremental reconcile** (§7.4).
- Specified previously hand-waved areas: ownership handoff & fencing (§7.2), partition
  decision table (§7.6), sync/async bridge (§7.7), stable stub identity & sequence keys
  (§8.3), port-bind divergence (§7.4.6), CRDT types & GC (§7.5), rolling upgrade / version
  skew (§11.4), chaos verification (§12), kill criteria (§13.3).
- Added enterprise composition: `rift-ee-server` binary and crate layout (§9).

**Changelog v2 review cycle 1** (5-reviewer adversarial pass; blockers fixed)

- **Honest affinity model** (was overclaimed): real LBs hash headers onto *their own* ring,
  not ours — header affinity gives per-flow stickiness, not owner co-location. Zero-hop is
  the exception (~1/N), one LAN RPC per stateful op is the norm (§6.2, §7.7, §11.3).
- **Scenario reads are owner-serialized** — local-replica reads for the FSM match gate would
  serve stale states under round-robin; reads for scenario matching now go through the
  owner (§7.2.4, §7.6).
- **Fencing redefined**: epoch is a pure function of the converged live set (comparable
  across nodes); ownership settle delay + per-key ownership generations close the
  rejoin dual-owner fork (§7.2).
- **Config writes serialized at a port owner** — the previous whole-config LWW destroyed
  concurrent stub mutations (esp. proxy recordings) even without a partition (§7.4.2).
- **proxyOnce claims are a Pending(owner-local)/Recorded(replicated) state machine** — the
  prior monotone G-set could not support claim release and could wedge a signature
  permanently (§7.5.3).
- **Clears/resets are generation bumps** (clock-free, monotone by construction), not
  wall-clock epoch markers (§7.5.2).
- **Flow-KV replication moved off gossip** onto owner→successor RPC (gossip budget could
  not hold realistic scenario state) (§7.2.3).
- **Cold start, graceful leave, seeds-unreachable, and incarnation** specified; persisted
  cluster-state dir added (§7.1.2–7.1.3, §7.4.5).
- **Kubernetes deployment section** added; LB compatibility table added (§11.5, §6.2).
- **New upstream seams**: U-7 embeddable-server (bootstrap/metrics are bin-private today),
  U-8 response-decoration + typed backend-unavailable error (the cluster's client-visible
  contract previously had no emission path); U-3/U-4/U-5 surfaces revised per
  implementability review (port-scoping, `note_request`, scoped clears, bulk resets).
- **Plan restructured for value**: Phase 0 split (0a blocks Phase 1; 0b parallel);
  verification ships before sequencing; strict sequencing/proxyOnce ship **Redis-backed
  first** with gossip-native as a demand-gated follow-on; commercial kill gates added
  (§10, §13.3, Appendix C D-12).

**Changelog v2 review cycle 2** (re-verification pass; residual blockers fixed)

- **Factual correction (code-grounded):** stub replace by id/index *preserves* the
  slot's cycling state today (`imposter/core.rs:1186-1197,1223-1229`) — cycle-1 text
  claimed it resets. `SequenceKey` gains a `slot` token so `LocalSequencer` stays
  byte-identical; the clustered keyless-edit divergence is documented (§3.3, §8.3, U-3).
- **proxyOnce two-owner ordering closed:** `Pending → Recorded` transitions only after the
  recorded stub's config write is acknowledged (claim owner ≠ config owner); failure
  releases the claim; claim tokens reject stale completions (§7.5.3, §7.3, U-5, C10).
- **Flow-KV ordering completed:** `(g, v, origin)` total order; adoption generation
  floored by replica/persisted maxima; post-Dead partition ownership clarified (§7.2).
- **Graceful-leave handoff sequenced** (adopt from the leaver or after its
  handoff-complete marker); C5 gains a lost-acknowledged-write assertion (§7.1.2, §12).
- **Kubernetes cold-start deadlock fixed:** `publishNotReadyAddresses: true` +
  `podManagementPolicy: Parallel` mandatory; gossip join independent of readiness;
  allow-solo explicitly rejected as the k8s workaround; emptyDir scoped; all-empty
  cold-start rule added (§11.5, §7.4.5).
- **U-8 completed:** per-request task-local op annotations carry degraded/partial flags to
  the decorator; bind warnings become a header (headers-only contract kept); scenario
  store errors stop being swallowed for fallible backends; U-8 moved into Phase 0a
  (§7.2.4, §7.4.6, §8.2, §10, Appendix A/B).
- Error channels added to U-3/U-5 (`Result`) so §7.6's reject defaults are expressible;
  U-4 `record` carries the resolved flow id so `clear_flow` is implementable.

---

## 1. Summary

Run Rift as a **fully distributed, self-clustering application** — a fleet of active-active
nodes behind a load balancer that share one imposter set and behave correctly for stateful
features cluster-wide, **without any mandatory external dependency**. The control plane
(membership, configs, tenancy, admin intents) is an **embedded Raft group** (`openraft`,
in-process; ADR-001); the data plane's flow state stays off consensus (single-writer
rendezvous-hash ownership + write-ahead durability). Redis remains an optional backend behind
the same traits — the **supported path for customers who require strict sequencing /
exactly-once semantics** (Appendix C D-12).

> **v2 → v3 note.** v2 coordinated the *whole* cluster via embedded gossip. v3 keeps gossip's
> instinct for the data plane (no consensus between a request and its response) but replaces
> the gossip **control plane** with Raft, because the four requirements below demand strong
> consistency there — see ADR-001 and §1.1. The §7.1/§7.2/§7.4 gossip mechanism is superseded;
> those sections are retained for the parts that survive (the ownership *contract*, the
> partition table, seam usage).

This is an **open-core feature**: a small set of *generic* extension seams goes upstream to
Apache-2.0 Rift (Appendix A); everything cluster-aware lives in the proprietary
`rift-enterprise` repo (`rift-cluster`, `rift-ee-server`). Phased so the high-value,
low-risk slice (membership + config-sync) ships first and each later phase is gated on
demonstrated demand.

## 1.1 Requirements (R1–R4)

The design is shaped by four requirements; each eliminates a class of simpler designs, and
together they are why the control plane is Raft (ADR-001).

- **R1 — read-after-write config visibility.** A config change acknowledged on any node is
  servable from *every* node by the time the client receives its 2xx. (§7.4 write barrier.)
- **R2 — immediate cross-node flow state.** A flow-state change made while serving a request is
  visible to the very next request, whichever node serves it. (§7.2.4 owner-authoritative
  reads — independent of the load balancer.)
- **R3 — full-restart durability.** Nothing is lost on a full-cluster restart: imposter configs
  *and* flow state (at the chosen durability level). (ADR-001 + §7.4.5, #16.)
- **R4 — no lost admin requests.** An accepted admin request is never lost, even if the node
  handling it dies mid-flight. (§7.4.2 durable intent log + op-id dedup.)

## 2. Motivation

Rift today is single-process: each instance binds one TCP port per imposter, matches stubs
in-process, and keeps all runtime state in memory. A single node already sustains ~20–40k
RPS, so raw throughput is rarely the driver — four goals justify a cluster:

1. **Throughput** beyond one node (Rift as the bottleneck in load/perf tests).
2. **HA / resilience** — an always-on shared mock environment that survives a node dying.
3. **Cluster-wide stateful correctness** — scenarios, response cycling, request
   verification, and proxy recordings correct even when a test's requests are sprayed
   across nodes.
4. **Config-sync ergonomics** — an admin change (`POST /imposters`) on any node reaches the
   whole fleet.

### 2.1 Non-goals

- **Not a database.** Durability covers configs, tenancy, admin intents, and — at the chosen
  per-imposter level (#16) — flow state. Response cursors, the recorded-request journal, and
  in-flight proxy claims stay deliberately volatile (v3: flow state is *no longer* volatile,
  correcting v2 — see R3, ADR-001, and #16; the survival matrix is architecture-guide Ch. 9).
- **Not cross-region.** Single failure domain / one LAN; WAN is out of scope (the Raft election
  and heartbeat timeouts assume LAN RTT).
- **Not linearizable end-to-end.** The **control plane is linearizable** (Raft); the **data
  plane is single-writer-per-key** with bounded, counted, flagged degradation (§7.6). A minority
  partition rejects control-plane writes rather than diverging (ADR-001).
- **Not unbounded scale.** Design target 3–9 **voters**, documented ceiling 16 nodes (extra
  nodes join as non-voting learners).
- **No enterprise concepts upstream.** The OSS surface stays generic (gate B3).
- **No owner-affinity guarantee at the LB.** Header-hash affinity gives stickiness, not
  owner co-location (§6.2); the design budgets one LAN RPC per stateful op.
- **No intercept mode, injection gate, or TUI in cluster mode** (v3). These are single-node
  OSS surfaces; `--cluster` with intercept mode is rejected at startup, alongside the per-core
  runtime rejection (D-14).
- **No multi-tenancy in this RFC.** Tenant-scoped configs, per-user RBAC, and audit are
  **RFC-002** (#17) — orthogonal, and absent from both v2 and upstream.

## 3. Current state (grounding — verified at `aaa6042` (v0.15.0))

All paths relative to `vendor/rift/crates/`.

### 3.1 Data plane

hyper 1.5 + tokio; **one `TcpListener` per imposter**. `ImposterManager` holds
`imposters: RwLock<HashMap<u16, Arc<Imposter>>>` (`rift-mock-core/src/imposter/manager.rs:88`).
Stateless matching is fully in-process. Auto-assigned ports scan the dynamic range
**49152–65535** for the lowest bindable port (`manager.rs:316`), deliberately not `bind(0)`.

### 3.2 The one existing state abstraction: `FlowStore`

`rift-mock-core/src/extensions/flow_state.rs:10`:

```rust
pub trait FlowStore: Send + Sync {
    fn get(&self, flow_id: &str, key: &str) -> Result<Option<Value>>;
    fn set(&self, flow_id: &str, key: &str, value: Value) -> Result<()>;
    fn exists(&self, flow_id: &str, key: &str) -> Result<bool>;
    fn delete(&self, flow_id: &str, key: &str) -> Result<()>;
    fn increment(&self, flow_id: &str, key: &str) -> Result<i64>;
    fn set_ttl(&self, flow_id: &str, ttl_seconds: i64) -> Result<()>;
}
```

Impls: `NoOpFlowStore` (same file), `InMemoryFlowStore` (`rift-mock-core/src/backends/inmemory.rs`),
`RedisFlowStore` (`rift-mock-core/src/backends/redis.rs`, feature `redis-backend`). Scenario FSM
state and flow KV route through it (`rift-mock-core/src/imposter/core.rs:369-412`). Lua scripting
binds it synchronously, executing on dedicated non-tokio worker threads
(`rift-mock-core/src/scripting/script_pool.rs:78`, `thread::Builder`). **This is the template to
generalize** — but note two problems:

- **No injection seam.** The imposter's store is built inside the *private*
  `Imposter::create_flow_store` (`imposter/core.rs:152`), a hardcoded
  `"inmemory" | "redis" | _ => NoOp` match on the imposter's `_rift.flowState` config. There
  is no builder parameter, registry, or factory through which an external crate can supply
  an implementation. (Corrected from v1, which implied the trait alone was enough.)
- **Scenario transitions are get-then-set** (`imposter/core.rs:369-397`) — not atomic even
  single-node under concurrency. The trait has no compare-and-set.

Also corrected from v1: `RedisFlowStore::set_ttl` is a **logging no-op**
(`backends/redis.rs:200-205`); per-key TTLs are set on write instead.

### 3.3 Everything else is per-node, in-memory, no abstraction

- **Response cyclers** — `RuleCycler(AtomicU64)` packs `resp_idx`/`repeat_idx` into
  high/low 32 bits (`rift-mock-core/src/behaviors/cycler.rs:10,20,56`). Cursor lifetime = the
  stub's `StubState` *slot*: **stub replace (by id or index) swaps the stub in place and
  deliberately keeps the slot's cycling state** (`imposter/core.rs:1186-1197,1223-1229`);
  only stub deletion, bulk stub replacement, and imposter replace drop the state.
- **Recorded requests** — `recorded_requests: RwLock<Vec<RecordedRequest>>` capped at
  `MAX_RECORDED_REQUESTS = 10_000` with oldest-first eviction (`imposter/core.rs:96,8`).
  The request *count* (`numberOfRequests`) is a separate `AtomicU64` incremented on every
  request even when recording is off; clearing saved requests resets it, targeted `retain`
  does not (`imposter/core.rs:1130-1142`).
- **Proxy recordings + proxyOnce dedup** — `RecordingStore` with
  `pending: Mutex<HashSet<RequestSignature>>` for exactly-once claims
  (`rift-mock-core/src/recording/store.rs:25`); caps 1 000/signature, 10 000 signatures
  (`store.rs:13,16`). `RequestSignature` is **port-less** (`recording/types.rs`) —
  uniqueness today comes from each imposter owning its own store. Known bug at the pin: a
  failed upstream call leaves the signature stuck in `pending` (no release on the error
  path) — U-5 fixes this upstream.
- **Imposter registry** — the `RwLock<HashMap>` above; only persistence is `--datadir`
  write-through JSON per port (`persist_imposter_checked` `manager.rs:538`,
  `persist_imposter` `manager.rs:561`).

### 3.4 Config & hot-reload (v1 correction)

Config comes from `--configfile` or `--datadir`. `POST /admin/reload`
(`rift-http-proxy/src/admin_api/handlers/system.rs:124`, routed at
`admin_api/router.rs:170`) re-reads the retained `ConfigSource` and calls
`ImposterManager::reload` (`manager.rs:410`). **This is not an atomic swap**: it validates
the new set, then `delete_all()` (`manager.rs:385`, tearing down every listener) and
recreates each imposter. Its own doc comment states: *"Reload resets all imposter state
(recorded requests, scenario state, response cyclers)"*, and a post-teardown bind failure
can leave a partial set. **Cluster config-sync therefore cannot be built on `reload()`** —
every `POST /imposters` on node A would reset all runtime state on B and C. §7.4 replaces
it with incremental reconciliation.

### 3.5 What already helps

- **Space-based isolation (issue #223) is implemented**: `Stub.space`
  (`rift-mock-core/src/imposter/types.rs:181`) gates matching on the request's resolved
  `flow_id`; `flowIdSource` supports `"imposter_port"` (default) or `"header:<Name>"`
  (`types.rs:795`). `teardown_space` exists (`manager.rs:485`).
- **Stable stub identity (issue #202) is implemented**: `Stub.id: Option<String>`
  (`types.rs:185`) with uniqueness enforced by `add_stub` and by-id CRUD
  (`manager.rs:437-480`: `add_stub`, `replace_stub_by_id`, `delete_stub_by_id`,
  `get_stub_by_id`). §8.3 builds sequence keys on it.
- **A single-port gateway exists**: `/__rift/:port/<path>` on the admin API dispatches
  in-process to the imposter on `:port` (issue #212, `admin_api/router.rs:94,158-170`).
  The gateway-fronted mode (§6.3) promotes this pattern to the data plane.
- **rift-mock-core is an embeddable, CLI-free library** (issue #203); `rift-http-proxy` is
  lib+bin, re-exporting `admin_api` and `config_loader`, so the admin API
  (`AdminApiServer::new(addr, manager, api_key)`, `admin_api/server.rs:27`) is reusable
  from an enterprise binary. Caveat: the metrics server and the bootstrap composition
  (`run_mountebank_mode`, `rift-http-proxy/src/main.rs:277,304`) are **bin-private** —
  U-7 moves them into the lib so the enterprise binary composes instead of forking.

## 4. Central design tension (and resolution)

Embedded gossip (SWIM-style) is **AP / eventually consistent** — ideal for membership,
config dissemination, and *mergeable* state, but it cannot by itself give correct
**response cycling** (never hand out the same index twice) or **proxyOnce** (record exactly
once) under a load balancer spraying requests across nodes; those need a single
authoritative writer per key.

**Resolution — ownership by rendezvous hashing over the gossip ring (no external
consensus):**

- Gossip gives every node an (eventually) identical membership roster. Derive a
  deterministic owner per key via **rendezvous (HRW) hashing** over live members. Each
  strongly-consistent key (a sequence cursor, a proxyOnce signature, a scenario FSM entry,
  a port's config revision counter) has **exactly one owner node** → single-writer without
  Raft/etcd.
- **Mergeable** state (recorded requests, counters) uses **CRDTs** — per-writer shards
  merged on read; no owner needed.
- Ownership moves on membership change; that handoff window is the bounded HA cost, and
  §7.2 specifies it (settle delay, ownership generations, per-key-type adopt-or-reset).

Stateless stubs never touch the coordination layer — they stay 100% in-process on every
node. Stateful ops cost **one LAN RPC to the owner in the common case** (§6.2 explains why
LB affinity does not eliminate this); the design budgets for that rather than pretending
it away.

## 5. Architecture

```
   L7 LB (space-based: single/few ports; header affinity optional — stickiness only)
   [gateway-fronted port mode: /__rift-style dispatch, port carried in request]
   [port-based fallback: L4 range proxy over a statically declared port set]
   ┌────┴───────────────────────────────────────────┐
   ▼                   ▼                    ▼
 Node A              Node B               Node C   (rift-ee-server)
 - data plane: binds ALL imposter ports; in-process stub matching (unchanged OSS code)
 - gossip layer ⇆ SWIM membership + small KV (config digests, bind status, clear
   generations, journal watermarks) ⇆
 - ring: HRW owner(key) → single writer for sequence cursors / proxyOnce / scenario FSM /
   per-port config revisions
 - internal RPC (hyper, HMAC-authenticated, dedicated cluster port): owner-forwarded state
   ops, config-body fetch, flow-KV replication to successors, journal anti-entropy
 - reconciler: gossiped desired-state → anti-entropy fetch → incremental apply
 - persisted cluster-state dir: desired-state (configs, revisions, tombstones, generations)
```

Every node runs identical roles (no leader). The OSS engine (`rift-mock-core`) is unmodified at
runtime; cluster behavior enters exclusively through the seams of Appendix A, implemented
by `rift-cluster` and wired by `rift-ee-server` (§9).

## 6. Isolation model ↔ LB topology

Rift supports two isolation models with very different load-balancer consequences.

### 6.1 Port-based isolation (default, "PerInstance")

One imposter = one listener; N services = N ports; ports can be minted at runtime
(`POST /imposters` with no port scans 49152–65535, `manager.rs:316`). **This does not
cluster nicely behind a managed LB**: (1) the port set is a moving target — an NLB/ALB
needs a listener + target group per port, cannot discover runtime-minted ports, and hits
listener quotas (~50 on an ALB); (2) L4 cannot read headers, so no flow stickiness. It
clusters only with a **statically declared** port set fronted by a range-capable L4 proxy
(Envoy/HAProxy), with per-port health checks so the proxy avoids nodes whose bind failed
(§7.4.6).

### 6.2 Space-based isolation — the clustering-native topology (with honest affinity)

Many isolated environments multiplexed onto one port; a stub carries `space: "X"`
(`types.rs:181`) and matches only when the request's resolved `flow_id` equals `X`, where
`flow_id` comes from a header (`flowIdSource`, `types.rs:795`). Adding a tenant/flow = a
header value, not a port. Scaling isolation scales header cardinality, not ports.

**What LB header affinity actually buys (correction from v1):** an L7 LB hashing the
flow-id header maps flows onto *its own* hash ring over *its own* endpoint view — it does
**not** compute Rift's HRW function, so the sticky node coincides with the HRW owner only
by chance (~1/N). Affinity therefore gives: (a) per-flow request ordering through one node
(useful for connection reuse and warm caches), (b) stable owner-RPC fan-out patterns — but
**not** zero-hop stateful ops. The design consequently treats **one LAN RPC per stateful
op as the normal cost** (§11.3); `owner == self` is an opportunistic fast path, not the
plan of record. (A future "sticky-owner lease" optimization — first-touch ownership so the
LB-chosen node becomes the owner — is recorded as future work in Appendix C D-13; it is
not part of this design.)

**LB compatibility for header hashing / stickiness:**

| LB | Header-based affinity | Notes |
|---|---|---|
| Envoy | ✅ `ring_hash`/`maglev` on header | recommended in-cluster tier |
| nginx | ✅ `hash $http_x_flow_id consistent` | |
| HAProxy | ✅ `balance hdr(...)` | |
| AWS ALB | ❌ (cookie stickiness only) | put an Envoy/nginx tier behind it |
| AWS NLB / any L4 | ❌ | port-based/L4 mode only, no affinity |

**Decision unchanged:** clustering targets **space-based isolation as primary** — the
grounds are port-cardinality, runtime port minting, and single-front L7 operability (not
the retracted zero-hop claim). Port-based remains a constrained secondary mode.

### 6.3 Gateway-fronted port mode (middle path)

Keep existing port-based configs verbatim while collapsing to a single L7 entry point: the
target port is carried **in the request** and dispatched in-process — exactly the pattern
of the existing `/__rift/:port/<path>` admin gateway (issue #212, `router.rs:94`).
Addressing schemes in order of transparency:

| Scheme | Example | Path rewrite? | Client change? |
|---|---|---|---|
| Header | `X-Rift-Port: 8080` | none | must set header |
| Subdomain | `p-8080.mocks.example.com` | none (vhost) | wildcard DNS/TLS |
| Path prefix | `/__rift/8080/orders` | strip prefix | URL only |

Prefer header or subdomain — nothing to strip, so path/host predicates, `savedRequests`,
and proxy `recorded_from` all see the true downstream request. Path-prefix requires clean
stripping (the #212 gateway already does this) or those three break.

Because dispatch is in-process against the existing `HashMap<u16, Arc<Imposter>>`, a
freshly minted port is addressable immediately with zero LB reconfiguration. Port and
flow-id stay orthogonal. **On Kubernetes this mode is effectively mandatory** (§11.5).
The plain gateway listener is upstreamed as part of U-7 (it is a generic single-node
convenience — promotion of #212; decision D-11); the cluster-aware parts (bind-failure
fallback dispatch, §7.4.6) stay enterprise.

## 7. Cluster runtime design

### 7.1 Membership & gossip

> ⚠️ **Superseded by ADR-001 (v3).** Membership is now a value in the **Raft log**, not gossip:
> at any log index every node computes byte-identical membership (and therefore ownership).
> chitchat, node incarnations, and the versioned-KV budget below are **removed**; bootstrap is
> `--cluster-init` (not `--cluster-allow-solo`), identity is a leader-minted `u64`, and join is
> `add_learner` → snapshot catch-up → auto-promote to voter (< 9 voters). The lifecycle
> *contract* (Joining→Ready gate, graceful-leave handoff, never-serve-stale) is unchanged and
> carried by the Raft membership; only the mechanism moved. See ADR-001 §Membership and issue
> #6. The text below is retained for the surviving contract and for context.

- **Library:** [`chitchat`](https://github.com/quickwit-oss/chitchat) (MIT; Quickwit's
  SWIM-with-phi-accrual + versioned key-value gossip). It covers membership *and*
  small-value dissemination in one dependency. Fallback if chitchat proves unsuitable:
  [`foca`](https://github.com/caio/foca) + a hand-rolled versioned KV (decision Appendix C,
  D-2). Either way it is wrapped behind `rift-cluster::membership`.
- **Node identity:** `node_id = <name>@<advertise_addr>#<incarnation>`; name defaults to
  hostname, overridable `--cluster-node-name`. **Incarnation** = 
  `max(persisted_incarnation + 1, unix_time_secs)` persisted in the cluster-state dir
  (§7.4.5); on a fresh volume (e.g. rescheduled pod with empty state) the wall-clock term
  still produces a higher incarnation than any prior boot, and a changed advertise address
  simply yields a new node identity — both safe.
- **CLI (Phase 1):**
  - `--cluster` — master switch; **everything below is inert without it**.
  - `--cluster-bind <ip:port>` — **required with `--cluster`** (no default): UDP gossip +
    TCP internal RPC (same port number, two sockets). Binding `0.0.0.0` requires the
    explicit `--cluster-bind-public-ok` acknowledgment.
  - `--cluster-advertise <host:port>` — address peers should use (NAT/container). A hostname
    is accepted, as well as a literal address (IPv6 literals must be bracketed,
    `[::1]:4790`), and is **re-resolved on every send** — not just at join time — so it
    tracks a DNS record that changes underneath it.
  - `--cluster-seeds <addr,addr,...>` — DNS names allowed and **re-resolved on every
    (re)join attempt** (required for Kubernetes, §11.5). Mutual seeding supported.
  - `--cluster-allow-solo` — explicit opt-in to form/serve a cluster of one (see §7.1.3).
  - `--cluster-secret <string>` / `--cluster-secret-file <path>` — **required**; refuse to
    start clustered without it unless `--cluster-insecure` (§11.2).
  - `--cluster-degraded-mode <reject|local>` — global partition-behavior override (§7.6).
  - `--cluster-features <list>` — enable stateful features selectively
    (`config-sync,flow-state,sequencing,journal,proxy`; default: all shipped phases).
    This is the per-phase rollback switch (§10).
  - `--cluster-state-dir <path>` — persisted desired-state (default `<datadir>/_cluster`,
    or a mandatory explicit path when no datadir).
- **Timing defaults:** gossip interval 1 s; phi-accrual failure detection target: node
  marked Dead in ≤ 10 s after crash; Suspect within ~3 s.
- **Gossip payload budget:** gossip KV carries only small entries — per-port config
  pointers (§7.4.1), per-(port,node) bind status, per-port/flow clear generations
  (§7.5.2), journal watermarks. Values ≤ 512 B, total node state ≤ 64 KiB, enforced with a
  startup-computed ceiling: ≈ `ports × (pointer + N×bind) + active_flows_with_clears`.
  Config bodies and flow-KV data **never** enter gossip (§7.4.1, §7.2.3).

#### 7.1.2 Node lifecycle & graceful leave

States: `Joining → Ready → Live ⇄ Suspect → Dead | Leaving → Left`.

- **Joining:** contact seeds, sync membership, run initial config reconcile (§7.4.5).
  The node publishes **no desired-state entries** and serves **no data-plane traffic**
  until reconciled (readiness gate, §11.1).
- **Graceful leave (SIGTERM / preStop):** node publishes `Leaving`; the leaver stops
  accepting new owner ops, drains in-flight ones, pushes final flow-KV values to its
  successors, then publishes a per-key-class **handoff-complete marker** (all bounded by
  `--cluster-leave-timeout`, default 10 s) and exits. Peers recompute ownership
  immediately on seeing `Leaving` **bypassing the settle delay** (voluntary leave is
  unambiguous), but adopting owners take their snapshot **from the leaver itself while it
  is alive** (it is the freshest replica) or wait for its handoff-complete marker before
  adopting from successors — this ordering is what prevents an acknowledged write from
  losing to the adopter's generation bump. If the leaver dies mid-drain, the normal
  crash path (settle delay + successor adoption) applies. This is what makes rolling
  restarts non-disruptive (§11.4) — crash-detection latency applies only to actual
  crashes. C5's exit criteria include a CAS-ladder lost-update assertion across the leave.
- **Permanent removal:** `Left`/`Dead` entries are GC'd from membership after 24 h (and
  from chitchat's own dead-node list per its config); a scale-down runbook documents
  draining via SIGTERM.

#### 7.1.3 Seeds unreachable

`--cluster` with configured seeds that are all unreachable: the node retries with backoff
**and stays not-Ready indefinitely** — it must never serve an empty/stale imposter set to
an LB as if healthy. Operators opt into serving alone with `--cluster-allow-solo`
(single-node bootstrap, e.g. first node of a new cluster seeded from its own datadir).
Phase-1 exit test: "unreachable seeds ⇒ never Ready" (§10).

### 7.2 Ring, ownership, epochs, handoff

> ⚠️ **Partly superseded by ADR-001 (v3).** The ownership *contract* survives verbatim — one
> authoritative owner per key by rendezvous (HRW) hashing, owner-serialized writes, and the
> §7.2.4 owner-authoritative reads that make R2 hold under any load balancer. What is **removed**
> is the machinery that existed only because gossip membership was not agreed: the ring epoch
> `xxh3(roster)` + `EPOCH_MISMATCH` retries, the 3 s settle delay, and per-key-class ownership
> generations + persisted floors. Under Raft the ring is a pure function of the **applied**
> membership, and the fencing token is `m_idx` (the log index of the last applied membership
> change); the one residual window is closed by the **isolated-owner rule** (a node that has not
> heard a leader heartbeat within 3× the election timeout rejects owner-side ops). See ADR-001
> §Ring & fencing and issue #7. Read `(g, v, origin)` below as `(m_idx, v, origin)`.

- **Ring:** rendezvous (HRW) hashing: `owner(key) = argmax_{n ∈ eligible} h(n.node_id, key)`
  with xxhash64, where `eligible = Live ∪ Suspect − Leaving` (Suspect nodes keep ownership
  until confirmed Dead — "sticky grace" — to avoid flapping during GC pauses). No virtual
  nodes; O(N) per lookup cached per key until the view changes.
- **Ring epoch (fencing token):** `epoch = xxh3(sorted eligible node_ids)` — a **pure
  deterministic function of the converged view**, so any two nodes with the same view
  compute the same epoch (v1's `view_version` component was per-node and not comparable;
  removed). Every owner-forwarded RPC carries the caller's epoch; the owner requires
  equality, rejecting with `EPOCH_MISMATCH` + its own epoch; the caller refreshes its view
  and retries (max 3, backoff 50/100/200 ms + jitter). Once views converge, epochs are
  equal by construction — mismatch is always transient.
- **Settle delay (dual-owner prevention):** epoch equality proves caller/owner *agreement*,
  not *currency* — during asymmetric view propagation (e.g. a node rejoins and some peers
  see it before others) two nodes can transiently compute themselves owner of the same
  key, each agreeing with its own callers. Mitigation: a node that *becomes* owner of a
  key class due to a view change serves ownership only after
  `T_settle = 2 × gossip dissemination bound` (default 3 s) has elapsed since it observed
  the change, while a node that *loses* ownership stops serving immediately upon observing
  it. Under the stated dissemination bound (≤ T_settle/2 for N ≤ 16 on a LAN) the serving
  windows cannot overlap. If the bound is violated (pathological network), forks are
  ordered by **ownership generation** (below) and the loss is bounded and observable —
  never silent (chaos C9 measures it).
- **Ownership generation:** each owner, on adopting a key class after a view change,
  increments a per-key-class generation `g` (floored by the maximum generation found on
  any successor/replica *and* by its own persisted floor, so an adopt-found-nothing owner
  cannot restart generations below a lingering deposed owner's). All versioned values
  carry **`(g, v, origin_node_id)` ordered lexicographically** — writes from a deposed
  owner (lower `g`) can never overwrite an adopting owner's writes, and a same-`(g, v)`
  fork (both sides of a partition adopting concurrently) resolves deterministically by
  origin; such merge losses are counted in `rift_cluster_cas_conflicts_total`, never
  silent. Note: once the failure detector confirms Dead across a partition, *each side*
  owns every key class within its own view — the "other side's keys" rows in §7.6
  describe only the pre-confirmation window.

#### 7.2.3 Flow-KV ownership, replication & adoption

Scenario FSM state and flow KV are owner-serialized **for writes and for scenario-gate
reads** (see §7.2.4), with replication for handoff continuity:

- The owner holds the authoritative copy in memory. On each accepted write it
  asynchronously pushes `(flow_id, key) → (g, v, value)` to its **k = 2 HRW successors**
  over RPC (fire-and-forget + a 5 s anti-entropy pull by successors). Replication is
  **not** via gossip — realistic scenario cardinality (hundreds–thousands of concurrent
  flows) exceeds any sane gossip budget.
- **Adoption:** a new owner (post-settle) pulls the key range from the former successors /
  any replica, taking the highest `(g, v, origin)`. Staleness bound: one replication round
  (typically ≤ 1 s) behind the failed owner's last accepted write. **Adopt-found-nothing**
  (all replicas lost, or entry evicted): the entry is treated as unset — for a scenario
  this means the FSM restarts; the response is flagged `Rift-Cluster-Degraded: kv-adopt`
  and counted. This is a documented bounded loss, listed in §7.6.
- **Eviction:** owners and replicas apply per-flow TTL (from `set_ttl`/flowState config,
  default 1 h) and a max-entries bound (default 100 k entries/node, LRU by flow);
  hitting the bound sheds whole least-recently-used *flows* (never single keys, to avoid
  torn scenario state) and counts `rift_cluster_kv_evicted_flows_total`.
- **Handoff semantics per key type:**

| Key type | On ownership change | Rationale |
|---|---|---|
| Sequence cursors | **Reset to 0** at the new owner (generation bump) | Cursor is test-run-scoped; replicating every advance would put a network write on the hottest stateful path (D-8). Documented: a membership change mid-test may restart response sequences. |
| Scenario state / flow KV | **Adopt** highest `(g, v)` from replicas | See above; ≤ 1 replication round staleness; adopt-nothing ⇒ documented reset, flagged. |
| proxyOnce | `Recorded` entries adopt (replicated, monotone); `Pending` claims are owner-local and die with the owner → re-claim allowed (§7.5.3) | Duplicates bounded by ownership changes, stated honestly: ≤ 1 + (ownership changes while a claim is in flight). |
| CRDT data (journal, counters) | N/A — no owner | Merge-on-read everywhere. |

- **In-flight ops at ownership change:** owner ops are single-shot atomic operations
  (CAS / INCR / claim). An op accepted by the old owner either completed and replicated,
  or the caller got an error and retries against the new owner. CAS re-validates; claims
  are re-claimable (Pending) or monotone (Recorded); INCR after a cursor reset is a
  documented sequence restart.

#### 7.2.4 Scenario reads go through the owner

The scenario FSM gate is a **read during stub matching** (`imposter/core.rs:369-412`
region): a stub with `required_scenario_state` matches only if the flow's state equals it.
Reading a local gossip/replica copy here is wrong under spraying — request 2 of a flow can
reach node B milliseconds after node A's transition, long before any replication —
so **scenario-state reads used for matching are forwarded to the owner** (one RPC),
exactly like writes; `owner == self` short-circuits to memory. Generic flow-KV reads that
do not gate matching (scripting `flow_store:get`) are **also owner-authoritative by
default** (#16 Gap C): scripts drive response content and fault decisions off flow state,
so a stale read is a wrong answer just as a stale match is. A per-imposter
`readConsistency: "local"` opts a given imposter back into the local replica (bounded
staleness, flagged when the owner is unreachable) — same polarity as
`--cluster-degraded-mode reject`: correct by default, fast by choice. The cost argument
that once justified replica reads is void, because scripts run on the dedicated
script-pool threads §7.7 already sizes with their own larger permit pool precisely so they
can park on RPC. The split is per §7.6. Transitions
(`willSetStateTo`) execute as CAS at the owner and return the authoritative new state, so
match-and-transition for a single request is one round trip total.

Implementation note (carried by U-8's PR, whose error-mapping path it completes): today
`Imposter::scenario_state`
**swallows** store errors and defaults to the initial state (`imposter/core.rs` ~368-378) —
acceptable for infallible built-ins, but a fallible injected backend defaulting to
"Started" would produce a *wrong match* instead of an error. The upstream change
propagates store errors from the scenario read/transition path so a `BackendUnavailable`
becomes the §7.6 503, never a silent wrong match. OSS behavior is unchanged in practice
(built-in stores don't fail).

### 7.3 Internal RPC

> **Config endpoints superseded by ADR-001 (v3.1).** The transport, auth and epoch/proto
> framing below are current (merged as #8). The two **config** rows are not: config writes
> go to the **Raft leader** as a `ControlOp` (§7.6, #9), not to a per-port config owner
> assigning `(g, revision)`, and ADR-001 deletes the content-addressed
> `GET /internal/v1/config/{port}/{digest}` fetch along with the digest-gossip mechanism it
> served. Retained for context, like §7.1/§7.2/§7.4. The KV, sequence, proxy and journal
> rows are unaffected — they are data-plane, which stays off consensus.

Hyper (already a dependency) over TCP on the cluster port. All bodies JSON; all requests
carry `X-Rift-Cluster-Auth` (§11.2), `X-Rift-Cluster-Epoch`, and the sender's proto
version (§11.4).

| Endpoint | Semantics | Idempotency |
|---|---|---|
| `POST /internal/v1/seq/next` `{port, stub_key, scope, response_count, repeats}` | owner: atomic advance, returns index | Not idempotent → callers retry only on connect/timeout-before-send, else surface per §7.6 |
| `POST /internal/v1/kv/cas` `{flow_id, key, expected, new}` | owner: compare-and-set, returns `{ok, current, g, v}` | Idempotent (CAS re-validates) |
| `POST /internal/v1/kv/get` `{flow_id, key, for_match: bool}` | owner-authoritative read (scenario gate) | Idempotent |
| `POST /internal/v1/kv/put` / `delete` / `incr` | owner-serialized KV ops | put/delete idempotent; incr as seq/next |
| `POST /internal/v1/kv/replicate` `{entries: [(flow_id,key,g,v,value)]}` | owner → successor push | Idempotent (versioned) |
| `POST /internal/v1/proxy/claim` `{port, signature}` | owner: Pending-claim attempt, returns `{outcome: claimed\|in_flight\|recorded, token}` | Idempotent per state machine (§7.5.3) |
| `POST /internal/v1/proxy/complete` / `release` `{port, signature, token, ...}` | owner: Pending→Recorded (after config-write ack, §7.5.3) / Pending→Unclaimed; stale token rejected | Idempotent |
| `POST /internal/v1/config/write` `{port, body \| stub_patch \| delete}` | **port-config owner**: validate, assign `(g, revision)`, publish | Idempotent via client op-id |
| `GET /internal/v1/config/{port}/{digest}` | content-addressed config body fetch | Idempotent, cacheable |
| `GET /internal/v1/journal/{port}?since=<seq-vector>&gen=<g>` | pull journal shard deltas ≥ watermark | Idempotent |

Timeouts: connect 500 ms, request 2 s. **Fast-fail:** if the local view already marks the
owner Suspect/Dead, skip the RPC and resolve immediately per the §7.6 owner-unreachable
row (no 2.5 s burn). Retries: 3 with backoff+jitter, subject to the idempotency column.
Connection pooling per peer.

### 7.4 Config replication

> ⚠️ **Superseded by ADR-001 (v3).** Config is now a **Raft state machine**: an admin write is a
> `ControlOp` that is validated on the leader, appended, replicated to a fsync'd majority
> (commit = R3 durability), applied everywhere via the incremental `apply_config` (#316), and
> gated by a **read-after-write barrier** (leader waits for every Ready node's applied index ≥
> the entry's, R1). `op_id` dedup in the state machine gives retries exactly-once *effect* (R4),
> and an admin write that cannot reach quorum is durably parked and replayed on heal rather than
> lost. This **removes** the gossip pointers, content-addressed body fetch/anti-entropy,
> per-port `(g, revision)` counters, and tombstone ack-vector GC below; `revision` becomes the
> Raft log index. The client-visible contract (revision/warning headers, degraded semantics via
> §7.6) and the bind-divergence handling (§7.4.6) survive. Degraded/failed responses use the
> upstream v0.15.0 error slugs (`unavailable`, `timeout`; #797) via `error_response_typed` — no
> cluster-specific error types. **Dependency, not a blocker:** achird-labs/rift#800 — the
> backend-error door still serves the pre-#797 envelope shape, so a degraded-response
> assertion aimed at *that* door must either wait for #800 or assert the headers only.
> Everything else already carries the stable `type`. See ADR-001 §Write path and issue #9.
> Text retained for context and the surviving contract.

**Design rules (v2, superseded mechanism): gossip carries pointers, not payloads; writes are
serialized at a per-port owner** (v1's accept-anywhere LWW destroyed concurrent stub mutations —
notably proxy recordings landing on two nodes within one gossip round — even without a
partition).

#### 7.4.1 Desired-state pointers in gossip KV

One entry per imposter port: `cfg/<port> → {g, revision, origin, digest, deleted}` where
`digest = sha256(canonical_json(ImposterConfig))`, `revision` is assigned **only by the
port's config owner** (HRW key `cfg:<port>`, generation `g` per §7.2), monotone and
persisted (§7.4.5). Tombstones (`deleted`) carry the same `(g, revision)` ordering.

#### 7.4.2 Write path (owner-serialized)

Any node's admin API accepts the mutation, forwards it to the port's config owner
(`config/write` RPC; `owner == self` short-circuits). The owner validates, assigns the
next revision, applies locally, publishes the gossip pointer, and responds; the accepting
node then applies locally (via the same reconcile path) before answering the client with
`Rift-Cluster-Revision: <port>:<g>.<revision>`, so the client observes its own write on
that node. Proxy-recorded stubs follow the identical path (each recording is a stub-append
forwarded to the config owner — recording is already an upstream-latency path, one more
LAN hop is immaterial).

**Owner unreachable:** per §7.6 — default `reject` (503) for config writes too; `local`
mode accepts locally with `(g_local, revision)` and reconciles by `(g, revision, origin)`
on heal (lost-update window, counted in `rift_cluster_config_conflicts_total`). During a
partition each side's HRW yields its own config owner, so each side stays internally
serialized; heal merges by `(g, revision, origin)` deterministically.

#### 7.4.3 Anti-entropy fetch

A reconciler seeing an unknown digest fetches the body from the origin or any node
advertising it (`GET /internal/v1/config/{port}/{digest}`), verifying content-address on
receipt.

#### 7.4.4 Two-level incremental reconcile

Upstream U-6; explicitly *not* `manager.reload()`:

- **Port level:** diff desired vs `ImposterManager` actual. New port → `create_imposter`;
  tombstone → `delete_imposter`; digest change → descend. Untouched ports are never
  touched.
- **Stub level:** compute an **order-aware edit script** (LCS over the stub-key sequence —
  order is semantic: first-match-wins) and apply via by-id/positional CRUD
  (`add_stub(index)`, `replace_stub_by_id`, `delete_stub_by_id`, plus U-6's `move_stub`).
  Same-key in-place replacements preserve sibling runtime state; a pure reorder applies as
  moves. If the edit script degenerates (>50 % of stubs change), fall back to
  whole-imposter replace (that imposter's runtime state resets — same as a single-node
  `PUT`). Imposter-level field changes (protocol, TLS, `recordRequests`, `flowState`…)
  always replace wholesale.

**Convergence:** typically 1–2 gossip rounds + one body fetch (≤ ~3 s at N ≤ 9). Test
harnesses await `GET /_cluster/config` → `converged: true` or poll the per-node applied
revision.

#### 7.4.5 Persistence, cold start, and tombstone safety

- Every node persists the **replicated desired-state** — configs (bodies), `(g, revision)`
  per port, tombstones, clear generations, its own incarnation — to
  `--cluster-state-dir`, updated write-through on reconcile. This subsumes the datadir
  role in cluster mode (the OSS `--datadir` write-through stays enabled for
  single-node-compatible snapshots but is not the replication source of truth).
- **Cold start (full-cluster restart):** nodes load persisted state, join, and merge by
  `(g, revision, origin)`; the highest persisted revision per port wins; persisted
  tombstones are honored even if older peers rejoin later. A node whose state dir is
  empty publishes nothing (Joining gate, §7.1.2) — an empty node can never GC the fleet's
  config.
- **Stale rejoin:** a node whose persisted state is older than the tombstone TTL (24 h)
  or whose state dir is missing **discards its config KV**, reconciles fully from peers,
  and only then resumes publishing — closing the resurrection path (a deleted imposter
  cannot be revived by a returning laggard, because the laggard defers to the fleet
  before speaking).
- **All-empty cold start:** if *every* node starts with an empty state dir (e.g. a fleet
  on `emptyDir` restarted simultaneously), no node may publish and none becomes Ready —
  the fleet must not silently converge on an empty imposter set behind an LB. Recovery is
  explicit: start (or restart) exactly one node with `--cluster-allow-solo` **and** a
  config source (`--configfile` / `--datadir`) to re-seed; the rest reconcile from it.
  Deployments must either persist the state dir on ≥ 1 node or keep config re-seedable
  from outside (§11.5).
- **Tombstone GC:** a tombstone is removed only when (a) older than 24 h **and** (b)
  acknowledged by every currently-live member (ack vector piggybacked in gossip).
  Revisions never restart: the persisted per-port `(g, revision)` floor survives tombstone
  GC, so a post-deletion re-create continues the revision sequence instead of restarting
  at 1 (no stale-state ABA).

#### 7.4.6 Port-bind divergence

Every node binds every imposter port; binds can fail on some nodes (port taken by an
unrelated process). Semantics:

- Bind status is gossiped per `(port, node)`: `bind/<port>/<node> → Bound | Failed(reason)`.
- The imposter **exists cluster-wide regardless**; nodes that failed to bind still serve it
  via the gateway listener (§6.3) if enabled, and still participate as owners for its keys.
- Admin `POST /imposters` returns `201` with a `Rift-Cluster-Warnings` **header** listing
  nodes that failed to bind (best-effort within a 2 s wait; late failures visible via
  `GET /_cluster/imposters`). A header — not a body field — because the U-8 decoration
  seam is deliberately headers-only; clients wanting detail follow the header to
  `/_cluster/imposters`.
- **Auto-assigned ports:** minted by the config owner during `config/write` (its local
  49152–65535 scan); the minted port is fixed in the replicated config — other nodes do
  *not* re-mint on bind failure (port is identity). In L4/port-based mode, per-port LB
  health checks must be configured so traffic avoids nodes with a failed bind (§6.1).
  Operators running clusters should prefer explicit ports or the gateway mode; documented.

### 7.5 Mergeable state (journal, counters) & clears

#### 7.5.1 Recorded-request journal

- Per port, a **grow-only log sharded by writer**: each node appends locally to its own
  shard, entries keyed `(node_id, seq, clear_gen)` with `seq` a per-node monotone counter.
  Merge = union of shards (idempotent under redelivery); read = k-way merge by recorded
  timestamp (ties: `(node_id, seq)`).
- **Propagation:** pull-on-read — `GET /imposters/:port/requests` on any node pulls peer
  deltas (`journal` RPC, 2 s budget; unreachable peers ⇒ partial result flagged
  `Rift-Cluster-Partial: true` via U-8) — plus a background anti-entropy pull every 5 s so
  reads are usually warm. Journal data never transits gossip.
- **Caps (writer-local):** each node caps **its own shard** at
  `max(500, 10_000 / N_live)` entries per port (age cap 1 h default), evicting oldest and
  advancing a per-shard `evicted_below_seq` **watermark** exchanged in anti-entropy
  responses; readers drop cached peer entries below the watermark so all nodes converge on
  the same visible set. Peer-cache memory is bounded separately (LRU, correctness-neutral
  — anything evicted is re-pullable). Note the honest consequence: with flow-affine
  traffic concentrating a port's load on one node, the effective cluster cap for that port
  approaches the single-shard cap; `test_journal_merge_exact` sizes N below
  `max(500, 10_000/N_live)` per written shard or uses uniform spraying (stated in the
  test).
- **`numberOfRequests`:** a per-node G-counter slot (incremented via U-4's
  `note_request`, which fires even when body recording is off — matching today's
  `AtomicU64` semantics); read = sum of slots (partial under partition, flagged).

**Incremental reads — the vector cursor.** A sharded log has no single sequence number, so
a scalar `since` cannot express "everything I have already seen": entries from different
shards interleave by recorded timestamp, and a peer that was unreachable last poll may
supply entries that sort *before* ones already returned. The cursor is therefore a
**vector** — a map `(node_id → shard_seq)` naming the high-water mark consumed per shard —
serialised into a single **opaque** token:

```
since = base64url(cbor({ "v": 1, "gen": <clear_gen>, "m": { "<node_id>": <shard_seq>, … } }))
```

- `read_since(cursor) -> (entries, next_cursor)` — a **new upstream method on
  `RequestJournal`**, and therefore a queued seam (**U-13**, Appendix B), not part of the
  already-merged U-4 (`achird-labs/rift#314`). Phase 3 cannot ship the fleet-wide form
  until it lands; the interim below is what makes that a schedule constraint rather than a
  blocker. Entries are the union of per-shard tails above each mark, k-way merged as for a
  full read.
- **`shard_seq` is monotone per shard for the life of the node and does *not* reset on a
  clear** — clears advance `clear_gen`, never rewind `seq` (§7.5.2 is clock-free precisely
  so nothing has to). The `gen` in the token therefore does not exist to prevent skipping;
  it exists so a cursor minted before a clear is not silently used to *resume* into a
  generation whose earlier entries have been discarded, which would return a suffix while
  looking like a complete page.
- **Eviction can overtake a lagging cursor.** If a shard's `evicted_below_seq` watermark
  has passed the cursor's mark for that shard, the entries between are gone — the honest
  answer is to report it, not to paper over it: the response carries
  `Rift-Cluster-Cursor-Lapsed: <node_id>` and resumes from the watermark. A consumer that
  must not miss entries polls faster than the shard cap turns over; one that only needs
  recency ignores the header. Silently resuming from the watermark would manufacture
  exactly the gap the Phase-3 criterion forbids.
- **The admin layer treats the token as opaque** — it round-trips it and never parses it.
  That is what lets the encoding change (a shard added, a node id retired, `v` bumped)
  without an API break, and it is why the token carries `gen`: a cursor minted before a
  clear (§7.5.2) is detected as stale and answered with a full re-read plus
  `Rift-Cluster-Cursor-Reset: true`, rather than silently skipping the entries a naive
  scalar comparison would drop.
- Unreachable shards are omitted rather than guessed: the response carries
  `Rift-Cluster-Partial: true` and `next_cursor` keeps the old mark for those shards, so
  the entries arrive on a later poll instead of being lost.

**Streaming (`GET /events`, `…/savedRequests/stream`).** SSE is fed by the same 5 s
anti-entropy pull, so its honest latency bound is **one anti-entropy interval, not
real-time**, and that must be documented on the endpoint rather than implied away. Two
acceptable shapes, decided at implementation time:

1. **Full fleet stream** — the vector cursor drives the pull; each SSE event carries the
   `next_cursor` so a reconnecting client resumes exactly (`Last-Event-ID`).
2. **Phase-3 interim** — stream **local shard only**, every event stamped
   `Rift-Cluster-Partial: true`, until the vector cursor lands. Honest and useful; it must
   not be described as a fleet stream.

Phase-3 exit criterion: **`test_journal_cursor_merge`** — spray across 3 nodes while
polling with the returned cursor. Stated per generation, because a clear deliberately
discards data and a cross-clear concatenation is therefore *supposed* to exceed a final
full read: within one `clear_gen`, the concatenation of pages equals a full read of that
generation, with no duplicate and no gap **for a run sized below the shard cap** (as
`test_journal_merge_exact` already requires) — above it, eviction legitimately outruns a
lagging cursor and the run must observe `Rift-Cluster-Cursor-Lapsed` rather than a silent
hole; a cursor spanning a clear yields
`Rift-Cluster-Cursor-Reset` **exactly once** and then resumes cleanly in the new
generation; a node going unreachable mid-sequence yields `Rift-Cluster-Partial` and its
entries on a later poll rather than never.

#### 7.5.2 Clears are generation bumps (clock-free)

`DELETE .../savedRequests`, count resets, and `teardown_space` do **not** delete replicated
data by timestamp (wall clocks skew). Instead:

- Gossip carries per-port (and per-`(port, flow)` for teardown_space) **clear generations**:
  monotone integers, merged by max. Every journal entry and counter increment is tagged
  with the generation current at its writer when written. Per-`(port, flow)` entries are
  TTL'd (default 1 h, matching the flow-KV TTL) once all live nodes have merged them, so
  long-lived clusters don't erode the gossip budget.
- Merge/read keep only max-generation data; stale-generation shards/slots are ignored and
  locally GC'd. Two racing clears merge to the same max harmlessly; a clear during
  partition applies to the minority's post-heal reads as soon as generations merge
  (minority appends made *after* the minority-side clear-gen bump survive; appends tagged
  with the pre-clear generation do not — deterministic, no clocks).
- `retain`-style targeted deletion (match clauses) remains node-local per shard: the
  reading node applies the predicate to the merged view; writer shards apply it locally on
  request (`clear_flow`/predicate push), acknowledging best-effort — exact targeted
  cluster-wide deletion is not guaranteed under partition (documented; the full-clear
  generation mechanism is the guaranteed path).
- `teardown_space(port, space)` in cluster mode = bump `(port, space)` clear generation
  (journal + counters + sequence cursors for that space) **and** owner-side deletion of the
  space's flow-KV entries with replicated per-entry delete markers `(g, v, deleted)`
  (same 24 h GC + all-live-ack rule as config tombstones).

#### 7.5.3 proxyOnce: Pending/Recorded state machine (replaces v1's G-set)

A pure monotone claim set cannot support claim *release* (failed upstream call) — a
released claim would resurrect on any merge, and an unreleased failed claim would wedge
the signature forever (claimed, but nothing recorded). Instead, per `(port, signature)`
at the owner:

```
Unclaimed ──try_claim──► Pending(holder, deadline) ──complete──► Recorded(response)
                              │ release / deadline expiry
                              ▼
                          Unclaimed  (re-claimable)
```

- `Pending` is **owner-local only** (never replicated) and carries a per-claim token
  returned by `try_claim`; `complete`/`release` must present the token, so a late
  `complete` arriving after deadline expiry and re-claim by another holder is rejected
  (stale token), not misattributed. If the owner dies, Pending dies with it; the next
  request re-claims at the new owner → possible duplicate upstream call, bounded by
  `1 + (ownership changes while claims in flight)` — stated honestly.
- **Ordering rule (claim owner ≠ config owner):** the recorded stub's config write goes to
  the *port-config* owner (§7.4.2) while the claim lives at the *(port, signature)* owner —
  two different nodes in general, so "one logical publication" must be sequenced, not
  assumed: the claim owner transitions `Pending → Recorded` **only after the config write
  is acknowledged** by the config owner. If the config write does not land — no leader
  reachable, so it returns `503` with a **parked** intent (§7.6) — the claim is `release`d
  so the signature stays retryable. **The recording's `op_id` is therefore derived from
  `(port, signature)`, not minted per attempt.** This matters because park-and-replay is
  not failure: the parked write is queued to *succeed*. With a per-attempt op-id, releasing
  the claim would let a retry record the signature a second time and the parked intent
  would later replay the first — two op-ids, so #9's dedup would not collapse them, and one
  signature would produce two upstream calls and two stub appends. Deriving the op-id from
  the signature makes the replay and the retry **the same operation**, which dedup does
  collapse. (The alternative — hold the claim and poll `GET /_cluster/ops/:op_id` to a
  terminal state before releasing — trades that duplicate for a claim pinned for the whole
  partition, and is rejected for it.) With the derived op-id,
  "recorded-but-stubless" is unrepresentable. `Recorded(port, signature, response_digest)`
  then replicates as the monotone fact; adopters accept only complete `Recorded` entries.
  Two conflicting `Recorded` publications for one signature (partition, both sides
  completing) merge by `(g, v, origin)` like any versioned value.
- Deadline default: `2 × upstream timeout`. All transitions idempotent; `release` of a
  non-held claim is a no-op. Chaos C10 includes killing the *config* owner between
  upstream completion and stub publication (claim must release, no wedge).

### 7.6 Partition & failure decision table

> **Aligned with ADR-001 and #9 (v3.1).** The rows below were written for the v2 gossip
> design, in which nothing had a quorum. Since ADR-001 the **control plane is a Raft group**
> and therefore *does* have one; only the **data plane** (flow state, sequences, journal —
> HRW-owned keys, off consensus) remains quorum-free. Three rows changed substantively as a
> result: **config write** (one degraded behaviour, not two — #9), **proxy recording write**
> (which *is* a config write, so it no longer offers a merge), and **flow-KV read**
> (owner-authoritative by default — #16 Gap C).

Global default `--cluster-degraded-mode reject` — correctness-critical ops fail fast and
honestly rather than silently diverging; per-feature defaults below chosen for a mock
server's practical needs. `local` mode trades correctness for availability and stamps
responses `Rift-Cluster-Degraded: <feature>` (emitted via U-8). `--cluster-degraded-mode`
arrives with **#16** and governs **data-plane features only** — flow state (#16), Phase-4
sequences, Phase-5 proxyOnce. It has **no effect on config writes** (including proxy
recordings, which are config writes): those go through Raft, so their degraded behaviour is
fixed rather than chosen — see the config-write row.

**Quorum, precisely.** The *control plane* has a quorum: config writes are Raft-committed,
so a minority side cannot commit at all. The *data plane* does not: for HRW-owned keys
"minority" is descriptive rather than privileged, and each side simply serializes the keys
whose owner is on that side.

| Feature / op | Owner reachable (normal) | Owner unreachable (fast-fail on Suspect/Dead) | During partition (either side) |
|---|---|---|---|
| Scenario read (match gate) | Owner RPC (`for_match`); local if self | **reject**: 503 cluster/owner-unreachable; `local`: local replica, flagged degraded | Keys owned on this side: normal; other side's keys: as owner-unreachable |
| Scenario CAS (transition) | Owner RPC; returns authoritative state | **reject** / `local` CAS on replica, flagged; `(g,v)`-merged on heal | Same pattern |
| Flow KV write / incr | Owner RPC | **reject** / `local`, flagged | Same |
| Flow KV read (scripting, non-gate) | **Owner RPC** (default); local replica only with per-imposter `readConsistency: "local"` (#16 Gap C) | **reject** / `local`, flagged | Same |
| Sequence next | Owner RPC; local if self | **local** (default for this feature): node-local cursor so test traffic flows; duplicates possible across sides; flagged. `reject` available | Same |
| Sequence reset / peek | Owner RPC (reset via generation bump; peek reads owner cursor) | reset: applied as generation bump (converges on heal); peek: local estimate, flagged | Same |
| proxyOnce claim/complete/release | Owner RPC per §7.5.3 | **reject** (default): 503 rather than risk duplicate side effects; `local`: local Pending, duplicates possible | Same |
| Proxy recording write (stub append) | Leader-serialized config write (`PatchStubs` `ControlOp`) | **A config write, and governed by the config-write row below:** `503` + op-id, durably parked, auto-replayed. No mode choice, no heal-merge. The op-id is derived from `(port, signature)` so a replay and a retry dedup to one operation (§7.5.3) | Same as config write |
| Journal append / `note_request` | Local shard (always) | Local (unaffected) | Local |
| Journal read / count read | Pull-on-read merge, complete | Merge of reachable shards, `Rift-Cluster-Partial: true` | Same |
| Journal clear / count reset | Generation bump (gossip) | Bump propagates on heal; local effect immediate | Same |
| `teardown_space` | Generation bump + owner KV deletes | Same as clear + KV-write rules above | Same |
| Config write (admin, recordings) | Forward to the Raft leader; 2xx after the write barrier | **One** behaviour, not a mode choice: `503` + `Rift-Cluster-Op-Id`, intent **durably parked**, auto-replayed when a leader returns (#9) | Majority side commits normally; minority side parks as above. Heal replays the parked intents; op-id dedup collapses duplicates. No `(g,revision,origin)` merge — that vocabulary is deleted |
| Config read | Local (converged) | Local (possibly stale; convergence gauge exposes lag) | Same |

Client-visible contract: degraded/partial responses always carry a `Rift-Cluster-*` header
(via the U-8 decoration seam — the OSS handlers themselves stay cluster-ignorant), and
every degraded op increments `rift_cluster_degraded_ops_total{feature}` — test harnesses
can assert zero degraded ops for strict runs.

### 7.7 Sync/async bridge

Problem: `FlowStore` (and the new traits) are **synchronous** — called from async request
handlers and from Lua/JS script execution — while clustered implementations perform
network RPC.

**Decision: dedicated bridge runtime in `rift-cluster`; no upstream trait signature
changes** (async-ifying `FlowStore` would ripple through the scripting engines and every
call site — Appendix C D-9).

- `rift-cluster` owns a small private tokio runtime (`cluster-io`, 2 threads default) used
  exclusively for gossip, RPC, replication, and reconciliation.
- Clustered trait impls bridge sync→async by submitting the op to `cluster-io` and parking
  the calling thread on a **`std::sync::mpsc::sync_channel` with `recv_timeout`** (never
  `tokio::sync::oneshot::blocking_recv`, which panics inside an async context) bounded by
  the op deadline (connect 500 ms + request 2 s; worst ~2.5 s), after which the op
  resolves to the §7.6 owner-unreachable row. **Fast-fail:** if the owner is already
  Suspect/Dead in the local view, resolve immediately without parking.
- **Thread-occupancy bound (corrected from v1):** parked callers block real threads. Lua/JS
  scripts run on dedicated non-tokio worker threads (`script_pool.rs:78`) — parking there
  is harmless, and script-originated ops use a **separate, larger permit pool** so heavy
  scripting can never starve the data-plane bridge. Data-plane tokio workers are the
  scarce resource: their bridge semaphore is
  therefore sized to **`max(2, worker_threads / 2)` permits** (not a large constant), and
  ops beyond it shed immediately with the owner-unreachable outcome — so at least half the
  data-plane workers are always available to the stateless hot path even when an owner is
  black-holing. Combined with fast-fail (which turns the common failure into a
  no-park path within one detection interval), a partition stalls bounded worker capacity
  for bounded time. `rift_cluster_bridge_inflight` / `_rejected_total` expose it; chaos
  C13 asserts stateless p99 during owner loss.
- **Deadlock analysis:** callers never run cluster-io work and cluster-io never calls back
  into the data plane synchronously — the wait graph between executors is acyclic.
- Fast path: when `owner(key) == self`, impls skip the bridge entirely and hit local
  memory. With N nodes this is ~1/N of stateful ops (plus any operator-arranged
  alignment); the design does not depend on it (§6.2).

## 8. Abstraction layer & OSS seams

The full seam inventory with justifications and API sketches is **Appendix A** (normative
for gate B). Summary:

### 8.1 Trait family (upstream, generic; `rift-mock-core` unless noted)

| Trait / seam (crate::module) | Supersedes | Cluster impl (enterprise) |
|---|---|---|
| `FlowStore` + `compare_and_set` (`extensions::flow_state`) | itself | `ClusteredFlowStore` (owner-serialized, successor-replicated) |
| `FlowStoreProvider` (`extensions::flow_state`) | private `create_flow_store` match (`imposter/core.rs:152`) | provider returning clustered stores |
| `ResponseSequencer` (`behaviors::sequencer`) | `RuleCycler`/`StubState` cursor call sites | `ClusteredSequencer` (owner INCR); `RedisSequencer` |
| `RequestJournal` (`imposter::journal`) | `RwLock<Vec<RecordedRequest>>` + count `AtomicU64` | `ClusteredJournal` (sharded G-log) |
| `ProxyRecordingStore` (`recording::store`) | concrete `RecordingStore` | `ClusteredProxyStore` (owner state machine); `RedisProxyStore` |
| `ImposterEventListener` + `apply_config` + `move_stub` + `stub_key` (`imposter::manager`, `imposter`) | `reload()` for sync purposes | config publisher + reconciler |
| Embeddable server pieces (`rift-http-proxy`): bootstrap builder, metrics server, gateway dispatch | bin-private `main.rs` | `rift-ee-server` composition |
| `ResponseDecorator` + `BackendUnavailable` (`extensions::decorate`) | — (new) | stamps `Rift-Cluster-*` headers/warnings |

Design rules for the upstream surface (gate B3): names, doc comments, and config keys are
generic ("pluggable backend", "provider", "listener", "decorator") — no
gossip/cluster/owner/CRDT vocabulary; every trait ships with a `Local` impl that is the
*current code moved behind the trait* (behavior-preserving; hot path bench-pinned), living
in the trait's own module; `Local` remains the default so OSS behavior is unchanged.

**Monetization boundary (deliberate):** OSS gets the traits + `Local` impls + the existing
`RedisFlowStore` (including its U-1 CAS — withholding an atomicity fix from an existing
OSS backend would be bad-faith open-core). **Redis implementations of the *new* traits
(sequencer/journal/proxy) and all gossip/fleet machinery are enterprise** (`rift-cluster`).
The honest consequence and the actual moat are recorded in Appendix C D-6: OSS + shared
Redis can DIY scenario/flow-KV multi-instance correctness; what stays commercial is
zero-dependency clustering, config-sync/membership/HA, cluster-merged verification, and
fleet operations.

### 8.2 Injection seams (upstream)

- `ImposterManager::with_flow_store_provider(Arc<dyn FlowStoreProvider>)` — consulted by
  `Imposter::create_flow_store` before its builtin match (`Imposter::new`'s only
  production call site is `manager.rs:209`, so an additive constructor parameter reaches
  it). A manager-scoped provider also resolves the construction-time caveat
  (`imposter/core.rs:148-151`): late-added scenario stubs no longer silently hit
  `NoOpFlowStore` (Appendix C D-7).
- `ImposterManager::with_sequencer / with_request_journal / with_proxy_store /
  with_event_listener / with_response_decorator` — builder-style, mirroring
  `with_datadir` (`manager.rs:104`) and `with_tls_defaults` (`manager.rs:116`).
- `ImposterManager::apply_config(Vec<ImposterConfig>) -> ApplyReport` — the two-level,
  order-aware incremental reconcile of §7.4.4; also fixes OSS `POST /admin/reload`'s
  reset-the-world behavior (`system.rs:124` switches to it; standalone OSS win).
- `AdminApiServer` construction is unchanged (`server.rs:27` takes the manager), but its
  handlers gain two generic behaviors (part of U-8): mapping the typed
  `BackendUnavailable` error to a 503 with structured body, and invoking the manager's
  `ResponseDecorator` with the request's **operation annotations** (a task-local
  annotation set that backend impls append to during the request — tokio task-locals
  follow the task across `.await`s and threads, and the sync bridge call runs inside the
  same task, so the carrier works; annotations from script-pool threads are best-effort
  and documented as such). This is how revision headers, bind-warning headers, and
  degraded/partial flags are emitted without the OSS handlers knowing about clusters.
- Sequencer plumbing note: cursor call sites (`StubState::get_next_response/peek_response`
  and their callers in `core.rs`/`handler.rs`) need `flow scope` and precomputed
  `repeats: &[u32]` threaded through ~8 signatures — acknowledged in U-3's PR scope, not
  hidden.

### 8.3 Stable stub identity & sequence keys

- **Stub key:** `stub.id` if set (unique-enforced, issue #202); otherwise
  `"~" + xxh3(canonical_json(stub \ {id, _verify}))[..16] + "#" + k` where `k` ≥ 1 is the
  occurrence index among byte-identical siblings (the first occurrence is always `#1`).
  The `~` prefix keeps generated keys disjoint from user-supplied ids. Deterministic
  across nodes because it derives only from replicated config bytes. Upstreamed as
  `rift_mock_core::imposter::stub_key(&Stub, occurrence)` (U-6 needs it for keyless-stub
  diffing; enterprise reuses it — one definition).
- **Sequence key** (cursor identity): `SequenceKey { port, slot, stub_key, scope }` where
  `scope = stub.space.clone().unwrap_or_default()` — **per-stub, not per-flow** (an
  unscoped stub matched by many flows shares one cursor, exactly today's semantics) —
  and `slot` is a per-imposter opaque token minted when a stub is inserted and **kept
  across in-place replaces** (mirroring the `StubState` slot lifetime,
  `imposter/core.rs:1186-1197`).
  - `LocalSequencer` keys by `(port, slot, scope)` → **byte-identical to today**,
    including the fact that replacing a stub (by id or index) *preserves* its cursor
    (corrected in review cycle 2: today's replace swaps in place and deliberately keeps
    cycling state — it does not reset).
  - `ClusteredSequencer` keys by `(port, stub_key, scope)` — `slot` is node-local and
    cannot be a cluster key. **Documented divergence:** editing a *keyless* stub's
    content changes its `stub_key`, so its cluster cursor restarts, whereas a single-node
    in-place edit preserves it. Stubs relying on cross-node sequencing should carry an
    explicit `id` (then edit-preserves-cursor holds in both modes).
- **Stability semantics:** editing stub X never touches stub Y's cursor (order-aware
  reconcile, §7.4.4). Deleting a stub, bulk-replacing stubs, or replacing the imposter
  drops the cursor (today's behavior). Admin "peek" without request context peeks the
  stub's own `scope`.
- `ResponseSequencer::reset_scope(port, stub_key: Option<&str>)` covers bulk resets
  (imposter delete/replace, teardown_space via scope) and GC — keyed cursor maps
  otherwise accumulate stale entries from keyless content edits (bounded; freed on
  port-level reset).
- Flow KV / scenario keys: `(flow_id, key)` exactly as `FlowStore` today. proxyOnce keys:
  `(port, RequestSignature)` — port-scoped because `RequestSignature` itself is port-less
  and only per-imposter store instances disambiguate it today.

## 9. Enterprise composition (`rift-enterprise` repo)

```
crates/
  rift-ee            # facade (exists): re-exports rift_mock_core/rift_types AND the seam traits;
                     #   rift-cluster/rift-ee-server import ONLY rift-ee — enforced
                     #   structurally: their Cargo.tomls drop the direct rift-mock-core/
                     #   rift-types deps they carry today (Cargo, not lints, is the fence)
  rift-cluster       # all cluster logic (proprietary)
    src/membership/  #   chitchat wrapper (DNS re-resolving seeds, Leaving state), identity
    src/ring.rs      #   HRW, epochs, settle delay, generations, owner cache
    src/rpc/         #   hyper server+client, HMAC auth, endpoints (§7.3), fast-fail
    src/bridge.rs    #   cluster-io runtime + sync bridge (mpsc) + sized semaphore (§7.7)
    src/stores/      #   ClusteredFlowStore, ClusteredSequencer, ClusteredJournal,
                     #   ClusteredProxyStore; RedisSequencer/RedisJournal/RedisProxyStore
    src/configsync/  #   owner-serialized writes, publisher, reconciler, persisted state dir
    src/crdt/        #   sharded G-log + watermarks, G-counters, clear generations
    src/decorate.rs  #   ResponseDecorator impl stamping Rift-Cluster-* (via U-8)
    src/admin.rs     #   /_cluster/* observability endpoints (§11.1)
  rift-ee-server     # binary (new): clap CLI = OSS flags + --cluster* superset; composes
                     #   the U-7 bootstrap builder with providers/backends from
                     #   rift-cluster; runs OSS AdminApiServer + metrics server (from U-7)
                     #   + cluster admin routes on the cluster port; --gateway-port
                     #   listener reusing U-7 gateway dispatch with cluster-aware fallback
```

Consumption mechanics (per `docs/dev-workflow.md`): each upstream seam PR (Appendix A)
merges to `achird-labs/rift` first → `vendor/rift` submodule bump → the corresponding
enterprise phase unblocks. Workspace mechanics: add `rift-http-proxy` to
`[workspace.dependencies]` as a path dep (missing today); `rift-mock-core` is consumed with its
default features; feature unification with `rift-http-proxy`'s `default-features = false`
core dep is verified by the existing CI `cargo check --workspace`.

Single-node/OSS users are unaffected: without `--cluster`, `rift-ee-server` wires the same
`Local` impls the OSS binary uses; the OSS `rift` binary never links `rift-cluster` at all.

## 10. Phased plan

Ordering rationale (review cycle 1): verification (now Phase 3) ships before sequencing —
it is the stateful feature users actually file bugs about; strict sequencing/proxyOnce
ship **Redis-backed first** against the same traits (weeks, low risk), with gossip-native
single-writer versions as demand-gated follow-ons (Appendix C D-12). Phase 0 is split so
Phase 1 is not blocked by seams it doesn't need.

| Phase | Deliverable | Preconditions (upstream) | Machine-checkable exit criteria | Rollback |
|---|---|---|---|---|
| **0a — enabling seams** ✅ **DONE** | U-6 (#316), U-7 (#317), U-8 (#318) — **merged upstream v0.14.0** | — | OSS suite green; `matcher_bench` within 2 % of pre-seam baseline | Additive, default-off |
| **0b — backend seams** ✅ **DONE** | U-1…U-5 (#311–#315) — **merged upstream v0.14.0** | — | Same bars per PR | Same |
| **1 — Membership + config-sync** (v3: **Raft**, ADR-001) | `--cluster*` CLI; Raft membership incl. graceful leave; `ControlOp` config writes + read-after-write barrier + durable intent log + op-id dedup (replaces the v2 gossip mechanism); redb log/vote/snapshot + cold start; `/_cluster/{members,config,health,imposters,ops}`; `/readyz`. Transport substrate (#8) merged. | 0a ✅ | 3-node harness: `POST /imposters` on A visible & serving on B/C ≤ 5 s (`test_config_sync_converges`); kill B mid-run → A/C unaffected, B rejoin converges (`test_node_rejoin`); sibling-port config change preserves scenario state (`test_reconcile_preserves_state`); stub reorder converges order-correct (`test_reconcile_reorder`); unreachable seeds ⇒ never Ready (`test_no_seeds_not_ready`); full-cluster cold restart restores config incl. tombstones (`test_cold_start`); SIGTERM leave under load → zero data-plane errors on survivors AND zero lost acknowledged writes (`test_graceful_leave`). Chaos: C4, C5, C6, C7, **C14, C15**. SDK conformance suite (upstream epic achird-labs/rift#458) green against a clustered fleet — an SDK must not be able to tell a 3-node fleet from a single node, which is R1 as a client sees it; `--cluster`-off parity of the full OSS suite tracked as #37. | `--cluster` off → OSS single node. **Truth scope:** a de-clustered node serves the full fleet config only if it ran with `--datadir` (the OSS write-through) — with `--configfile`-only deployments, export a snapshot from `--cluster-state-dir` first. Rollback is per-fleet: mixed on/off nodes behind one LB diverge immediately |
| **2 — Scenario/flow state** | `ClusteredFlowStore`: owner-serialized reads (match gate) + CAS, successor replication, adoption; `/_cluster/kv/{flow_id}`; stuck-scenario & split-brain runbooks | U-1, U-2 (+0a) | multi-step scenario round-robin across 3 nodes at 10 ms pacing: transitions linear per flow, zero illegal transitions, zero lost updates over 10 k iterations (`test_scenario_cluster_linear`); owner kill mid-scenario → adopt within 1 replication round or flagged reset, never an illegal transition (`test_scenario_handoff`). Chaos: C1, C8, C9, C12, C13 | `--cluster-features` without `flow-state` → local stores |
| **3 — Recorded-request verification** | `ClusteredJournal`: sharded log, watermarks, pull-on-read, generation clears; count G-counter | U-4 (+0a); **U-13** for the vector-cursor/streaming form (§7.5.1) | spray N (< shard-cap) requests across 3 nodes → `GET .../requests` on each node returns exactly N (`test_journal_merge_exact`); `DELETE savedRequests` clears cluster-wide ≤ 5 s incl. concurrent appends, clock-skew-immune (`test_journal_clear`); `numberOfRequests` = N on every node (`test_count_merge`); incremental reads with the returned vector cursor concatenate, **within one clear generation**, to exactly a full read of that generation — no duplicate, no gap — with `Rift-Cluster-Cursor-Reset` exactly once across a clear and `Rift-Cluster-Partial` for a node unreachable mid-sequence (`test_journal_cursor_merge`, §7.5.1; needs seam U-13) | `--cluster-features` without `journal` → local Vec |
| **4 — Response sequencing (strict = Redis first)** | `RedisSequencer` (strict, requires `--cluster-redis <url>`); `ClusteredSequencer` (gossip-native, experimental flag) | U-3 (+0a); **named customer request on file for gossip-native strict** | Redis mode: cyclic stub sprayed across nodes → global sequence no dup/skip incl. during single-node kill (`test_sequence_redis_strict`); gossip mode: no dup/skip while membership stable, documented reset on handoff (`test_sequence_no_dup_no_skip`, `test_sequence_handoff_reset`). Chaos: C2, C13 | feature flag off → per-node cursors (today's behavior) |
| **5 — Proxy + proxyOnce (strict = Redis first)** | `RedisProxyStore` (strict claims); `ClusteredProxyStore` (Pending/Recorded, experimental); recordings via config-owner writes | U-5 (+0a); same demand gate for gossip-native | Redis mode: 3 nodes, concurrent first-hits, 100-run soak incl. node kill → upstream called exactly once (`test_proxy_once_redis_strict`); gossip mode: exactly-once while membership stable, duplicates ≤ documented bound under owner kill, measured (`test_proxy_once_gossip_bound`); recorded stubs appear on all nodes (`test_recording_replicates`); concurrent recordings on 3 nodes, no partition → zero lost stubs (`test_recording_no_loss`). Chaos: C3, C10, C11 | feature flag off → local store (today) |

**Phase 1 must land and be validated with a design partner before 2–5 proceed** (also a
kill-criteria gate, §13.3). Every phase ships behind `--cluster` +
`--cluster-features`, and each lists its chaos scenarios (§12) as part of "green".

## 11. Cross-cutting

### 11.1 Observability

Cluster admin endpoints (served on the cluster port, auth'd; JSON):

- `GET /_cluster/members` — roster: node, state (Joining/Ready/Live/Suspect/Dead/Leaving),
  incarnation, epoch.
- `GET /_cluster/ring?key=<k>&type=<seq|kv|proxy|cfg>` — computed owner + epoch +
  generation ("who owns K, at which generation").
- `GET /_cluster/config` — per port: `(g, revision)`, digest, origin, per-node applied
  revision + bind status; `converged: bool`.
- `GET /_cluster/imposters` — per-(port,node) bind status (§7.4.6).
- `GET /_cluster/kv/{flow_id}` — per key: owner node, owner value `(g, v)`, local replica
  `(g, v)` — the "why is my scenario stuck" endpoint (Phase 2 ships a stuck-scenario and a
  split-brain runbook alongside it).
- `GET /_cluster/health` — full diagnostics (auth'd).

**Probes (unauthenticated, data-plane/admin port):** `GET /readyz` — Joined + initial
reconcile complete (what LBs and kubelet must probe); `GET /healthz` — process liveness.
The existing OSS `/health` stays untouched.

Prometheus (existing registry pattern, `extensions/metrics.rs`; served by the U-7 metrics
server on `--metrics-port`): `rift_cluster_members{state}`, `rift_cluster_ring_epoch`,
`rift_cluster_owner_forwards_total{op}`, `rift_cluster_owner_failures_total{op,reason}`,
`rift_cluster_settle_waits_total` †, `rift_cluster_generation{key_class}`,
`rift_cluster_gossip_lag_seconds` †, `rift_cluster_config_revision{port}`,
`rift_cluster_config_converged` (0/1), `rift_cluster_config_conflicts_total` †,
`rift_cluster_cas_conflicts_total`, `rift_cluster_degraded_ops_total{feature}`,
`rift_cluster_partial_reads_total`, `rift_cluster_kv_evicted_flows_total`,
`rift_cluster_bind_failures{port}`, `rift_cluster_bridge_inflight`,
`rift_cluster_bridge_rejected_total`, `rift_cluster_insecure` (0/1).

† Three of those are v2-era and do not survive ADR-001 as written:
`rift_cluster_config_conflicts_total` counted a config-write merge conflict, which the
rewritten §7.6 config-write row makes unrepresentable — the control plane has a quorum, so
there is nothing to arbitrate; `rift_cluster_settle_waits_total` counted the settle delay
ADR-001 deletes rather than mitigates; and `rift_cluster_gossip_lag_seconds` names a
transport the control plane no longer uses. The Phase-1 replacements are the
intent/replay counters #9 actually ships — `rift_cluster_intents_pending`,
`rift_cluster_intents_replayed_total`, `rift_cluster_ops_deduplicated_total` — plus
`rift_cluster_config_converged`. Listed here rather than silently dropped, because a
dashboard built on the old names should be told they will read zero forever.

### 11.2 Security

- Cluster traffic (gossip UDP + RPC TCP) on a **dedicated, explicitly configured bind**
  (§7.1 — no default; `0.0.0.0` needs `--cluster-bind-public-ok`), intended for a private
  network; never multiplexed with the public data plane.
- **Shared secret required** (`--cluster-secret[-file]`): RPC requests carry
  `X-Rift-Cluster-Auth: t=<unix_ts>,n=<nonce>,mac=HMAC-SHA256(secret, ts‖nonce‖method‖path‖body)`;
  receivers enforce ±30 s clock skew and a nonce cache sized
  `expected_peak_rpc_rate × 60 s` (default 100 k entries; overflow = reject, fail-closed).
  Gossip payloads are wrapped in the same keyed MAC via a chitchat custom-transport
  wrapper (chitchat has no built-in auth); UDP replay within the MAC window is contained
  by chitchat's version monotonicity (replayed old versions are ignored by the KV merge).
  This authenticates and integrity-protects but does **not encrypt**; confidentiality
  requires network-level isolation (private VPC/namespace, WireGuard/mesh). mTLS between
  nodes is a Phase-2+ hardening item, not a Phase-1 blocker.
- `--cluster-insecure` (explicit) is the only way to run without a secret; it logs a
  startup warning and sets `rift_cluster_insecure 1`.
- Admin API auth is unchanged (existing `--api-key`); `/_cluster/*` requires the cluster
  secret's derived bearer or the admin api-key. `/readyz`/`/healthz` are unauthenticated
  by design (probe targets, no state exposure).

### 11.3 Performance guardrails

- Stateless hot path: **zero cluster code** — `Local` trait impls are the moved current
  code; Phase-0 exit criterion pins bench regressions ≤ 2 %.
- Stateful ops: budget **one LAN RPC (sub-millisecond typical) per op** — scenario-gated
  requests cost one `kv/get(for_match)` (+ one CAS when transitioning, same round trip);
  sequence advances one `seq/next`; proxyOnce one claim + one complete. `owner == self`
  (~1/N) short-circuits to memory. There is no zero-hop assumption (§6.2).
- **Sequencer honesty:** "one RPC per op" holds for `next`, but a response body that
  interpolates the cursor without advancing it costs a `peek`, so the real budget is
  **one `next` plus up to a few `peek`s per request** — a stub template referencing the
  sequence several times pays per reference. State plainly what is and is not mitigable:
  the **RPC fan-out is inherent**, because the only cache that could remove a round trip
  would live on the *calling* node, and a non-owner cache reintroduces exactly the stale
  read the owner-authoritative design exists to prevent. An optional short-lived (≈ 50 ms)
  peek cache **on the owner** bounds owner-side cursor-store work under a peek-heavy
  template — it does not reduce the number of RPCs. Templates that reference a sequence
  many times per response are therefore a known cost, to be measured rather than designed
  away — and no existing chaos scenario measures it (C13 loads the *stateless* path against
  a black-holing owner, which is a different question), so Phase 4 owes a peek-amplification
  benchmark rather than a citation.
- Bridge capacity: semaphore `max(2, workers/2)`; fast-fail on Suspect/Dead owners keeps
  the parked-thread window to roughly one detection interval after a crash.
- Sizing rules of thumb (per node): journal ≤ ports × shard-cap × avg-entry;
  flow-KV ≤ 100 k entries (LRU-shed above); config bodies = fleet config size;
  gossip state ≤ 64 KiB (§7.1 formula gives the imposter-count ceiling for a given N —
  at N = 9 with default caps, ≈ 1 500 imposters).

### 11.4 Rolling upgrade & version skew

- Node gossip state carries `proto=<major>.<minor>`; RPC requests carry the same; **major
  mismatch → rejection** (op resolves per §7.6 owner-unreachable) and
  `JOIN_REFUSED_VERSION` at join for skew beyond one adjacent major. Within a major, all
  gossip keys and RPC fields are additive-only.
- Rolling restart procedure (runbook): one node at a time; SIGTERM triggers graceful leave
  (§7.1.2 — settle delay bypassed, drains, pushes final replicas), so surviving nodes see
  zero owner-unreachable windows for voluntary restarts; the restarting node rejoins with
  a higher incarnation and passes the readiness gate before receiving traffic. Sequence
  cursors still reset on ownership moves — schedule upgrades between test runs.
- Config schema: content-addressed bodies make skew safe to transport; a node that cannot
  deserialize a newer config field fails that port's apply, reports it in
  `/_cluster/config` (per-node applied revision lags), and serves its last good config —
  additive-field discipline in `ImposterConfig` keeps this rare.

### 11.5 Deployment topologies

**Kubernetes (primary):**

- **StatefulSet + headless Service** for the fleet: stable node names (identity,
  runbook-friendly), stable DNS seed names (`rift-0.rift-hs`, `rift-1.rift-hs`, …);
  `--cluster-seeds` uses those names and the membership wrapper **re-resolves DNS on every
  join/retry** (pod IPs churn; a fleet restart with cached IPs would brick the cluster —
  hard requirement on `rift-cluster::membership`).
- **Two manifest facts are mandatory or a fresh fleet deadlocks:** the headless Service
  must set `publishNotReadyAddresses: true` (readiness = reconciled, so on a full restart
  every pod is not-Ready; a default headless Service would publish no seed DNS and no pod
  could ever join) and the StatefulSet must use `podManagementPolicy: Parallel` (the
  default `OrderedReady` never starts pod 1 until pod 0 is Ready, which requires pod 1).
  Gossip join is deliberately independent of readiness. **`--cluster-allow-solo` is NOT
  the k8s workaround** — fleet-wide allow-solo lets a pod that misses its seeds during a
  blip form a serving cluster of one behind the Service, exactly the stale-serving
  failure §7.1.3 exists to prevent.
- **Gateway-fronted mode is effectively mandatory** (§6.3): Service ports are static, so
  per-imposter runtime-minted ports cannot be exposed; expose the gateway port + admin
  port + metrics port only. Port-based mode on k8s requires a statically declared port
  list mirrored in the Service — supported but discouraged.
- Probes: readiness = `/readyz` (unauthenticated, §11.1); liveness = `/healthz`.
  `preStop`: SIGTERM + `terminationGracePeriodSeconds ≥ 2 × cluster-leave-timeout`.
- Disruption: `PodDisruptionBudget maxUnavailable: 1` (scale-independent), rollout
  `maxUnavailable: 1` — each disruption is one graceful leave (no crash-detection
  window), but sequence cursors reset per §7.2; schedule rollouts between test runs.
- Cluster port stays ClusterIP-internal (never on the Ingress); secret via k8s Secret →
  `--cluster-secret-file`. **`--cluster-state-dir` on a small PVC for at least one node.**
  `emptyDir` is acceptable *only* when config is re-seedable from outside (a ConfigMap
  `--configfile`, or ≥ 1 PVC-backed node): a simultaneous full-fleet restart on all-emptyDir
  loses the fleet config, and the all-empty rule (§7.4.5) then correctly holds every node
  not-Ready until an operator re-seeds — availability loss by design, not silence.

**VMs / bare metal:** static `--cluster-seeds` (or DNS), systemd `ExecStop` = SIGTERM
(graceful leave), Envoy/HAProxy front for header affinity or L4 range mode; per-port
health checks in L4 mode (§6.1).

## 12. Verification & chaos plan

Harness: `rift-enterprise` repo, **two tiers** (#11). *In-process* (`crates/rift-cluster`
tests) drives real `RaftNode`s over localhost TCP — fast, deterministic, runs in PR CI.
*Container* (`tests/cluster-chaos/`) runs 3× `rift-ee-server` containers behind an Envoy
front with **toxiproxy between the nodes** — the only tier that can test real process
death, partitions, and the admin write path end to end. Every scenario asserts on the
admin API + Prometheus metrics, **never** log output, and never on the frozen legacy
`code` field — degraded-behaviour assertions read the stable `type` slug (`unavailable`,
`timeout`, upstream #797) plus the `Rift-Cluster-*` headers.

Note on partitions: toxiproxy cannot express one. Every peer dials a node at its single
advertised address, so disabling that listener cuts inbound only and leaves the
"partitioned" node campaigning through its still-open outbound path — the opposite of what
C4 asserts. Whole-node isolation (`docker network disconnect`) is symmetric, and for a
3-node fleet that is the only partition that matters. Toxiproxy is used for C6's
loss/jitter, where L4 latency injection is exactly the right tool.

Functional exit criteria are named per phase in §10. Chaos scenarios (fault-injected,
timing-sensitive — **not** claimed deterministic):

| ID | Scenario | Invariant asserted |
|---|---|---|
| C1 | Partition A\|BC during scenario traffic, heal after 30 s | No illegal FSM transition on either side; keys owned on the reachable side stay serialized; other-side ops rejected (default) or flagged degraded; converges on heal; conflicts counted, never silent |
| C2 | Kill sequence-owner mid-traffic (gossip mode) | No dup/skip before kill; documented reset after handoff; no wedged requests (complete or 503 within deadline) |
| C3 | Concurrent proxyOnce first-hits on all 3 nodes, 100 signatures, membership stable | Upstream sees exactly 100 calls |
| C4 (rewritten per ADR-001) | Config write during partition on both sides, heal | Minority answers `503` + `Rift-Cluster-Op-Id` with the intent **durably parked**; majority commits; on heal the parked intents **replay** and op-id dedup collapses duplicates; every node ends at an identical applied index. No `(g,revision,origin)` winner — the control plane has a quorum, so there is no merge to arbitrate |
| C5 | Rolling restart under load (SIGTERM, one node at a time) | Zero data-plane errors on survivors; zero owner-unreachable rejections (graceful leave path); **zero lost acknowledged writes** (CAS ladder driven across each leave — catches handoff-ordering races); **leadership hand-offs bounded: a new leader within 3 s of each roll** (per #11 — a latency bound, measured directly, unlike C6's rate bound which exists because its quantity is only observable at the sampling resolution) |
| C6 (redefined — was 30 % UDP drop, a gossip-transport scenario) | toxiproxy 30 % loss + 100 ms jitter on the **cluster TCP port** for 60 s | Voter set never changes; **leadership transitions bounded by rate, not count** — the injected jitter overlaps the 150–300 ms election timeout by design, so occasional elections are in spec and only a *continuously* re-electing fleet fails (#94); zero acknowledged-write loss; fleet converges when the toxics lift |
| C7 | Joining node with stale/empty state | Serves no traffic until Ready; then identical config; publishes nothing while Joining |
| C8 | **Round-robin scenario traffic, healthy cluster, 10 ms pacing, no affinity** | Zero illegal transitions, zero stale-read matches (owner-read path) |
| C9 | **Node rejoin under CAS load (asymmetric views)** | No forked `(g,v)` histories: generation ordering yields one winner; any acknowledged-write loss ≤ documented settle-violation bound and counted |
| C10 | **Owner kill during proxyOnce storm** — variants: kill the claim owner; kill the *config* owner between upstream completion and stub publication | Duplicates ≤ 1 + ownership changes (measured against the documented bound); zero permanently wedged signatures; failed config write ⇒ claim released, signature retryable |
| C11 | **Concurrent proxy recording on 3 nodes, no partition** | Zero lost recorded stubs (owner-serialized config writes) |
| C12 | **±5 s clock skew across nodes** | Journal clears exact (generation-based); HMAC window behavior per spec; no age-GC anomalies |
| C13 | **Owner black-hole + 20 % stateful load** | Stateless p99 < 5 ms throughout (bridge semaphore + fast-fail); bounded rejected-op count |
| C14 (from #9) | **Leader docker-kill mid 100-write storm** | Every write is either acked-and-present, or `503`-with-op-id and present after replay; zero duplicates; a new leader within 3 s |
| C15 (from #9) | **`kill -9` all three nodes under load** | After restart, configs *and* parked intents are identical to the last acknowledgement — R3/R4 end to end |

CI budget: **PR smoke** = 3 iterations of each phase-relevant scenario (~20 min,
parallelized compose stacks); **nightly full** = 100 iterations across parallel stacks
(wall target ≤ 2 h on 8 stacks). Flake policy: infrastructure failures auto-retry once;
**invariant violations never retry** — they file as bugs; persistently flaky scenarios get
quarantined behind an issue, not deleted.

Regression: entire existing `rift-mock-core`/`rift-http-proxy` test suite runs against
`rift-ee-server` with `--cluster` **off** → byte-identical behavior (tracked as #37);
hot-path micro-benches (`matcher_bench`) within 2 %. The **SDK conformance suite**
(upstream epic achird-labs/rift#458) runs against a clustered fleet as a Phase-1 exit
criterion: an SDK cannot tell a clustered `rift-ee-server` from a single node, which is the
externally visible form of R1. Pipeline: `cargo fmt`, `cargo clippy -- -D warnings`,
`cargo test` (both repos).

## 13. Risks, alternatives, kill criteria

### 13.1 Honest risk assessment

- **Multi-quarter effort.** Phase 0a+1 is the high-value ~25 % and alone makes stateless
  Rift horizontally scalable + HA with synced config.
- **Strong stateful correctness with no external store is the ambitious part.** The design
  is AP with bounded staleness: settle-delay + generations mitigate but cannot eliminate
  every asymmetric-propagation fork (§7.2); the residual is bounded, counted, and
  chaos-measured (C9), never silent. Quorum consensus would eliminate it and is explicitly
  out of scope (Appendix C D-1); customers needing strict semantics **today** get the
  Redis-backed implementations (D-12).
- **chitchat dependency risk:** MIT-licensed, maintained by Quickwit, but not designed as
  a stable public API; pinned + wrapped behind `rift-cluster::membership` so a swap to
  `foca` stays contained (Appendix C D-2).
- **Upstream coupling:** enterprise velocity depends on OSS seam PRs (Phase 0). Mitigation:
  seams are small, additive, independently useful (Appendix A), the repos share a
  maintainer today, and §13.3 puts a clock on it.

### 13.2 When NOT to cluster (documented guidance)

Decision table (any single row sufficing means: don't cluster):

| Situation | Cheaper answer |
|---|---|
| Sustained load < ~20 k RPS, restart-in-minutes availability is acceptable | Single node + supervisor |
| Teams have disjoint imposter sets | Shard by port-range/DNS across independent nodes — zero new code |
| Mocks live and die with a CI job | One Rift sidecar per app/test-runner |
| Traffic for any one logical flow can be pinned to one node | **Sticky/affinity LB over independent nodes**: per-flow scenarios, cycling, and verification are then correct today with zero cluster code — the cluster only earns its keep when one flow's requests genuinely spray (serverless callers, many concurrent clients per flow) or fleet-wide verification/config is required |
| You already run Redis and accept it as a dependency | Independent nodes + the OSS Redis flow-state backend cover shared scenario state; the enterprise Redis-strict sequencer/proxy backends (Phases 4–5) extend that without gossip |
| No on-prem/perimeter constraint | Hosted mock SaaS (WireMock Cloud et al.) removes ops entirely; Rift-EE clustering targets customers whose mock env must sit inside their network (data residency, load-test network path) — validate this constraint with each design partner |

Cluster when ≥ 2 of: >1-node sustained throughput; zero-downtime always-on SLO;
cross-node flows; fleet-wide config + verification.

### 13.3 Kill criteria (checked at each phase gate)

- **Phase 0 gate:** ✅ **discharged.** All seams merged upstream in v0.14.0 (#311–#318); no
  patched-submodule debt was incurred. The Phase-0 timeline risk is closed.
- **Phase 1 gate:** if no design partner runs a ≥ 3-node Phase-1 cluster against real
  workloads within one quarter of release, **pause 2–5; resume only when a named design
  partner commits to a paid pilot** (config-sync + HA may be the whole sellable feature).
- **Commercial gate (standing):** if by the end of Phase 2 no customer has signed (or
  LOI'd) for cluster mode at target price, stop Phases 4–5 permanently and productize
  Phases 1–3 as the clustering offering.
- **Gossip-strict gate (Phases 4–5):** the gossip-native strict sequencer/claims ship only
  against a named customer requirement for coordination-free strict mode; absent that,
  Redis-strict remains the supported strict path and gossip-native stays experimental.
- **Standing:** any phase whose `Local`-path benchmark regression exceeds 2 % blocks until
  resolved — the single-node experience is never sacrificed.

---

## Appendix A — OSS-boundary matrix (normative)

Rules: every item is additive, default-off/behavior-preserving, generically named (no
cluster vocabulary), and independently justifiable to an OSS maintainer. Signatures are
drafts — final shape negotiated in upstream review. U-2…U-5 and U-8 share one umbrella
framing when filed: *"pluggable runtime-state backends & response decoration for
embedders (#203)"* — so five trait-extraction PRs from one author read as a coherent
embeddability program, not an unexplained seam campaign.

### U-1 — `FlowStore::compare_and_set` (crate `rift-mock-core`, `extensions::flow_state`)

```rust
pub enum CasOutcome { Applied, Conflict(Option<Value>) }

pub trait FlowStore: Send + Sync {
    // ...existing six methods...
    /// Atomically set `key` to `new` iff its current value equals `expected`
    /// (`None` = "not present"). Returns the winning current value on conflict.
    /// Default implementation is a non-atomic get-then-set fallback, provided
    /// for backward compatibility; real backends should override.
    fn compare_and_set(&self, flow_id: &str, key: &str,
                       expected: Option<&Value>, new: Value)
                       -> Result<CasOutcome> { /* default: get+set */ }
}
```

*OSS justification:* scenario FSM transitions are get-then-set today
(`imposter/core.rs:369-397`) — racy under concurrent requests on a single node; CAS fixes
an OSS correctness bug. `InMemoryFlowStore` overrides under its write lock;
`RedisFlowStore` via a Lua script. *Compat:* provided default → no downstream breakage.

### U-2 — `FlowStoreProvider` + manager injection (`extensions::flow_state`, `imposter::manager`)

```rust
pub trait FlowStoreProvider: Send + Sync {
    /// Return a store for this imposter, or `None` to defer to built-ins.
    fn provide(&self, config: &ImposterConfig) -> Option<Arc<dyn FlowStore>>;
}
impl ImposterManager {
    pub fn with_flow_store_provider(self, p: Arc<dyn FlowStoreProvider>) -> Self;
}
```

`Imposter::create_flow_store` (`imposter/core.rs:152`) consults the provider first;
`Imposter::new`'s single production call site (`manager.rs:209`) threads it through.
*OSS justification:* rift-mock-core is an embeddable library (issue #203) with FFI consumers;
embedders supply custom stores (own persistence, test fakes). Also fixes the documented
construction-time caveat (`imposter/core.rs:148-151`): a manager-scoped provider serves
stores to imposters whose scenario stubs arrive after creation. *Compat:* no provider →
exactly today's match.

### U-3 — `ResponseSequencer` (`behaviors::sequencer`)

```rust
pub struct SequenceKey<'a> {
    pub port: u16,
    /// Per-imposter slot token, minted at stub insertion, kept across in-place
    /// replaces (mirrors the internal StubState slot lifetime).
    pub slot: u64,
    pub stub_key: &'a str,
    pub scope: &'a str,
}

pub trait ResponseSequencer: Send + Sync {
    /// Atomically advance and return the response index for `key`,
    /// honoring per-response repeat counts. `Err` = backend unavailable.
    fn next(&self, key: SequenceKey<'_>, response_count: usize, repeats: &[u32])
        -> Result<usize>;
    fn peek(&self, key: SequenceKey<'_>, response_count: usize, repeats: &[u32])
        -> Result<usize>;
    /// Reset cursors: a specific stub's, or (None) every cursor on the port.
    fn reset_scope(&self, port: u16, stub_key: Option<&str>);
}
impl ImposterManager { pub fn with_sequencer(self, s: Arc<dyn ResponseSequencer>) -> Self; }
```

`LocalSequencer` (same module) wraps the existing `RuleCycler` packing
(`behaviors/cycler.rs`), keyed by `(port, slot, scope)` — **byte-identical to today**,
including cursor preservation across in-place stub replaces (`imposter/core.rs:1186-1197`;
corrected in review cycle 2 — replace does *not* reset). `Local` never returns `Err`.
Backends that key by `stub_key` instead of `slot` (for cross-process sharing) document the
keyless-content-edit divergence (§8.3). **Declared PR scope:** threading the key parts +
precomputed `repeats` through `StubState::get_next_response/peek_response` and ~8 caller
signatures in `core.rs`/`handler.rs`; admin response-preview peeks the stub's own scope.
*OSS justification:* pluggable sequencing enables persistent cursors across restarts,
deterministic seeding for reproducible runs, and shared cursors for embedders (#203).
*Compat:* default = `LocalSequencer`, hot path bench-pinned.

### U-4 — `RequestJournal` (`imposter::journal`)

```rust
pub struct JournalRead { pub entries: Vec<RecordedRequest>, pub complete: bool }

pub trait RequestJournal: Send + Sync {
    /// Called for EVERY request (even when body recording is off) — backs
    /// `numberOfRequests`, matching the existing counter semantics.
    fn note_request(&self, port: u16);
    /// `flow_id` is the request's resolved flow (per the imposter's `flowIdSource`) —
    /// carried here so scoped clears don't require re-deriving it from stored headers.
    fn record(&self, port: u16, flow_id: &str, req: RecordedRequest);
    /// `complete = false` signals the backend could not reach all storage.
    fn read(&self, port: u16) -> JournalRead;
    /// Clears entries AND resets the request count (documented contract).
    fn clear(&self, port: u16);
    /// Targeted deletion; does NOT reset the count (documented contract).
    fn retain(&self, port: u16, keep: &dyn Fn(&RecordedRequest) -> bool);
    /// Declarative scoped clear (per resolved flow) — remotable, unlike `retain`.
    fn clear_flow(&self, port: u16, flow_id: &str);
    fn count(&self, port: u16) -> u64;
}
impl ImposterManager { pub fn with_request_journal(self, j: Arc<dyn RequestJournal>) -> Self; }
```

`LocalJournal` = the current `RwLock<Vec>` + 10 k cap + `AtomicU64` count moved behind the
trait (clear-resets-count / retain-does-not, exactly as `imposter/core.rs:1130-1142`).
`teardown_space` calls `clear_flow`. *OSS justification:* retention is hardcoded today
(10 k, no age cap — `imposter/core.rs:8`); the trait allows configurable retention,
spill-to-disk, and external sinks for verification at scale. *Compat:* default =
`LocalJournal`.

### U-5 — `ProxyRecordingStore` (`recording::store`)

```rust
pub struct ClaimToken(u64);
pub enum ClaimOutcome { Claimed(ClaimToken), InFlight, AlreadyRecorded }

pub trait ProxyRecordingStore: Send + Sync {
    /// First caller per (port, signature) wins — the "record once" gate.
    /// `Err` = backend unavailable (mapped to 503 per the backend-error contract).
    fn try_claim(&self, port: u16, sig: &RequestSignature) -> Result<ClaimOutcome>;
    /// Releases a claim after a failed upstream call so the signature is retryable.
    /// Stale tokens (expired + re-claimed) are ignored.
    fn release_claim(&self, port: u16, sig: &RequestSignature, token: ClaimToken);
    fn record(&self, port: u16, sig: RequestSignature, token: ClaimToken,
              resp: RecordedResponse) -> Result<()>;
    fn lookup(&self, port: u16, sig: &RequestSignature) -> Option<RecordedResponse>;
    fn clear(&self, port: u16);
}
impl ImposterManager { pub fn with_proxy_store(self, s: Arc<dyn ProxyRecordingStore>) -> Self; }
```

All methods are **port-scoped** because `RequestSignature` carries no port — today
uniqueness comes solely from per-imposter store instances. `LocalProxyStore` (same module)
= current `RecordingStore` behind the trait, preserving the pending-set TOCTOU fix.
*OSS justification:* formalizes the implicit pending-set contract and **fixes a real bug
at the pin**: a failed upstream call currently leaves the signature stuck in `pending`
(no release on the error path) — `release_claim` closes it. `clear` backs the existing
DELETE endpoint. *Compat:* default unchanged (plus the bug fix, noted in the PR).

### U-6 — `apply_config` + events + stub identity (`imposter::manager`, `imposter`)

```rust
pub struct ApplyReport { pub created: Vec<u16>, pub replaced: Vec<u16>,
                         pub stub_patched: Vec<u16>, pub deleted: Vec<u16>,
                         pub failed: Vec<(u16, ImposterError)> }
impl ImposterManager {
    /// Reconcile toward `desired` incrementally: per-port diff, then an order-aware
    /// per-stub edit script (stub identity = id or content key) applied in place.
    /// Untouched imposters keep all runtime state. Unlike `reload()`, never tears
    /// down unchanged listeners.
    pub async fn apply_config(&self, desired: Vec<ImposterConfig>) -> ApplyReport;
    /// Positional move for order-aware reconcile (stub order is match priority).
    pub async fn move_stub(&self, port: u16, from: usize, to: usize)
        -> Result<(), ImposterError>;
}
/// Stable stub identity: explicit `id`, else `"~" + hash + "#" + occurrence`.
pub fn stub_key(stub: &Stub, occurrence: usize) -> String;

pub enum ImposterEvent { Created(u16), Replaced(u16), StubsChanged(u16), Deleted(u16),
                         AllDeleted }
pub trait ImposterEventListener: Send + Sync { fn on_event(&self, ev: &ImposterEvent); }
impl ImposterManager { pub fn with_event_listener(self, l: Arc<dyn ImposterEventListener>) -> Self; }
```

*OSS justification:* `POST /admin/reload` currently resets **all** imposter state even for
a one-line change (`manager.rs:410` doc comment) — switching it to `apply_config` is a
straight OSS improvement. `stub_key` extends issue #202's identity to keyless stubs
(by-content admin addressing). Event listeners serve audit logging, persistence hooks,
webhooks. *Compat:* `reload()` kept (deprecated); listener default = none.

### U-7 — Embeddable server (`rift-http-proxy` lib)

Move three bin-private pieces of `main.rs` into the library (no behavior change):

```rust
pub struct ServerBuilder { /* manager, admin addr, api key, config source, tls, datadir */ }
impl ServerBuilder {
    pub fn from_cli(cli: Cli) -> Self;   // today's run_mountebank_mode split; the bin's
                                         // `Cli` struct moves into the lib (public-api addition)
    pub fn manager(self, m: Arc<ImposterManager>) -> Self; // inject a pre-built manager
    pub async fn run(self) -> anyhow::Result<()>;
}
pub async fn run_metrics_server(addr: SocketAddr) -> anyhow::Result<()>;
    // was main.rs:555 (today `(port: u16)` with hardcoded 0.0.0.0 — signature delta noted)
pub async fn dispatch_to_port(manager: &ImposterManager, port: u16, req: Request<Incoming>)
    -> Response<Full<Bytes>>;                             // the #212 gateway core, reusable
```

*OSS justification:* completes issue #203 (embeddable engine) at the server layer — custom
binaries compose the real bootstrap instead of forking `main.rs`; the gateway dispatch
becomes reusable for anyone fronting many ports with one listener (promotion of #212).
*Compat:* the `rift` binary becomes a thin caller of the same functions.

### U-8 — Response decoration + typed backend-unavailable (`extensions::decorate`)

```rust
/// Attached by backends to a failed op; admin/data-plane handlers map it to 503
/// with a structured JSON body instead of a generic 500.
#[derive(Debug, thiserror::Error)]
#[error("backend unavailable: {feature}: {detail}")]
pub struct BackendUnavailable { pub feature: &'static str, pub detail: String }

/// Request-scoped operation annotations: a tokio task-local, append-only set the
/// server initializes per request. Backend impls append (key, value) notes during
/// the request; the decorator reads them at response time. Task-locals follow the
/// task across .await points, and synchronous backend calls execute inside the
/// same task, so the carrier is reliable on the request path. (Ops originating on
/// script-pool threads annotate best-effort.)
pub fn annotate(key: &'static str, value: String);   // no-op outside a request task

pub enum ResponsePhase { DataPlane, Admin }
pub trait ResponseDecorator: Send + Sync {
    /// Inspect/annotate an outgoing response (headers only; body untouched).
    fn decorate(&self, phase: ResponsePhase, req_port: Option<u16>,
                annotations: &[(&'static str, String)], headers: &mut http::HeaderMap);
}
impl ImposterManager { pub fn with_response_decorator(self, d: Arc<dyn ResponseDecorator>) -> Self; }
```

Handlers (data plane + admin) downcast op errors to `BackendUnavailable` → 503
`{"error":"backendUnavailable","feature":...}`, and invoke the decorator on every
response with the request's collected annotations. This PR also declares one behavior
change in its scope: the scenario read/transition path stops swallowing store errors
(§7.2.4) so a failing injected backend produces the 503, not a silent wrong match
(built-in stores are infallible in practice — OSS behavior unchanged). *OSS
justification:* embedders with custom backends (U-2…U-5) need backend outages to surface
as 503-with-context rather than opaque 500s, and need a per-request annotation channel +
response hook (op timing, build info, storage provenance) — all generic. *Compat:* no
decorator, no custom backends → byte-identical responses.
*(This seam is how all `Rift-Cluster-*` headers — degraded/partial flags, revision and
bind-warning headers — are emitted: enterprise backends `annotate(...)`, the enterprise
decorator translates annotations into `Rift-Cluster-*` headers. The OSS handlers never
learn cluster vocabulary.)*

### Enterprise-only inventory (never upstreamed)

Everything in `rift-cluster` and `rift-ee-server` (§9): chitchat integration and
membership lifecycle, HRW ring/epochs/settle/generations, internal RPC + HMAC, flow-KV
replication/adoption, config-owner serialization + persisted state dir + reconciler
driver, journal shards/watermarks/generation clears, proxyOnce Pending/Recorded machine,
Redis impls of U-3/U-4/U-5, the `ResponseDecorator` impl, `/_cluster/*` endpoints,
`--cluster*` CLI, cluster-aware gateway fallback, k8s manifests, chaos harness.

## Appendix B — upstream seams (FILED AND MERGED)

> **v3 status: Phase 0 complete.** All eight seams below shipped upstream in v0.14.0 as
> `achird-labs/rift#311–#318` — this is now a *merged mapping*, not a to-file list:
> U-1→#311 (`compare_and_set`), U-2→#312 (`FlowStoreProvider`), U-3→#313 (`ResponseSequencer`),
> U-4→#314 (`RequestJournal`), U-5→#315 (`ProxyRecordingStore`), U-6→#316 (`apply_config` +
> events + `stub_key`), U-7→#317 (embeddable `ServerBuilder`/gateway/metrics), U-8→#318
> (`ResponseDecorator` + `BackendUnavailable`). Three further seams are queued for later
> phases: **U-11** the front-door route table (#19), **U-12** the `ImposterSource` provider
> trait (#20), and **U-13** `RequestJournal::read_since` — the incremental-read method the
> §7.5.1 vector cursor needs, which U-4 (#314, already merged) does not carry, so Phase 3's
> fleet-wide streaming form is gated on it landing upstream. RFC-002 adds **U-9** (admin
> authorizer) + **U-10** (principal on events). The original drafts are retained below for
> provenance.

Filed on `achird-labs/rift` (generic wording; no enterprise references). One
umbrella issue — *"Pluggable runtime-state backends & embeddable server (#203
follow-up)"* — then one PR per seam:

1. **"Add compare_and_set to FlowStore; use it for scenario transitions"** — fixes racy
   get-then-set FSM transitions under concurrency; default method keeps third-party impls
   compiling; InMemory overrides under its lock, Redis via Lua script. (U-1)
2. **"Support custom flow-store providers for embedders"** — `FlowStoreProvider` +
   `with_flow_store_provider`; also fixes late-added scenario stubs getting
   `NoOpFlowStore`. (U-2)
3. **"Extract ResponseSequencer trait from cycler internals"** — persistent /
   deterministic / shared sequencing for embedders; `LocalSequencer` preserves behavior +
   benches; declared plumbing scope (threading scope+repeats through the response path).
   (U-3)
4. **"Pluggable RequestJournal for recorded requests"** — retention is hardcoded (10 k
   cap, no age cap); trait enables configurable retention and external sinks; explicit
   count contract (`note_request`, clear-resets, retain-doesn't). (U-4)
5. **"Formalize proxy recording behind ProxyRecordingStore; fix pending-claim leak on
   failed upstream calls"** — port-scoped claim/release/record/lookup/clear; the release
   path fixes a real stuck-pending bug. (U-5)
6. **"Incremental apply_config + imposter change events + stable stub keys; use in
   /admin/reload"** — stop resetting all imposter state on reload; order-aware per-stub
   reconcile (`move_stub`, `stub_key`); event hook for audit/persistence. (U-6)
7. **"Make the server embeddable: ServerBuilder, metrics server, gateway dispatch as
   library functions"** — #203 at the server layer; `rift` binary becomes a thin caller.
   (U-7)
8. **"Typed BackendUnavailable error, per-request op annotations + ResponseDecorator
   hook"** — custom backends surface outages as structured 503s; task-local annotation
   channel + response-decoration hook; stops swallowing scenario-store errors for
   fallible backends. (U-8)

## Appendix C — decision log

| # | Decision | Alternatives rejected & why |
|---|---|---|
| ~~D-1~~ | ~~No consensus layer; AP + single-writer-by-ownership + settle/generations~~ **SUPERSEDED by D-15 (ADR-001).** | The four requirements R1–R4 (§1.1) are a request for a strongly consistent *control plane*, which D-1 declined. D-1's premise — "quorum ops on the request path" — was the error: Raft carries only the control plane (human/CI-frequency), never the data path. Retained for history. |
| ~~D-2~~ | ~~chitchat (MIT) for membership + small-KV gossip~~ **SUPERSEDED by D-15/D-16 (ADR-001).** | Membership is now a Raft-log value (`openraft`); the versioned-KV gossip it provided is replaced by the Raft state machine. Retained for history. |
| D-3 | HRW hashing, no vnodes | Consistent-hash rings with vnodes shine at N≫16 and weighted nodes; HRW is simpler, minimal churn on membership change, O(N) fine at our scale |
| D-4 | Config bodies via content-addressed RPC fetch, not gossip | Gossiping full configs blows the SWIM payload budget and re-floods every round; digests converge fast and bodies transfer once per node |
| D-5 | Two-level, order-aware reconcile (LCS edit script) on top of by-id/positional stub CRUD | Whole-imposter replace per change resets runtime state cluster-wide; set-diff (v2 draft 1) missed reorders and reordered keyless edits — order is match priority, so the edit script must be order-aware |
| D-6 | Redis impls of the *new* traits are enterprise; existing `RedisFlowStore` (incl. U-1 CAS) stays OSS. **Accepted erosion:** OSS + shared Redis can DIY multi-instance scenario/flow-KV correctness. **The moat is not "coordination"** — any Redis impl of these small traits is community-reproducible in days — it is zero-dependency clustering, config-sync/membership/HA, cluster-merged verification, and fleet operations | Withholding CAS from an existing OSS backend would be bad-faith open-core and raise more upstream suspicion than shipping it; pretending trait-impl code is the moat mis-prices the product |
| D-7 | Manager-scoped store via provider resolves the construction-time caveat | Per-imposter stores kept for OSS compat; a provider returning a shared store is strictly more flexible |
| D-8 | Sequence cursors reset on ownership change | Replicating cursors puts a network write on the hottest stateful path; a documented reset matches test-run-scoped data |
| D-9 | Sync traits + enterprise-side bridge runtime (std mpsc park, sized semaphore) | Async-ifying `FlowStore` ripples into Lua/JS engines and every call site — huge OSS churn benefiting only clustering |
| D-10 | Default `--cluster-degraded-mode reject` (except sequencing = local) | Silent local fallback for CAS/proxyOnce converts partitions into wrong test results — the one thing a verification tool must never do; sequencing degrades by default because blocking all cyclic responses during a blip is worse than a possible duplicate index, and it's flagged |
| D-11 | Plain gateway listener upstreams with U-7 (promotion of #212); only cluster-aware dispatch (bind-failure fallback) stays enterprise | Keeping a generic single-node convenience enterprise-only has bad optics, zero moat (community can promote #212 trivially), and weakens U-7's story |
| D-12 | Strict sequencing/proxyOnce ship **Redis-backed first**; gossip-native single-writer versions are demand-gated experimental follow-ons | Gossip-exact semantics are the hardest engineering in the RFC aimed at the least-demanded guarantee; the trait seams make the backend invisible to customers; target customers already operate Redis. The zero-dependency premise stays intact for Phases 1–3 (membership, config-sync, scenario state, verification) |
| D-13 | LB header affinity treated as stickiness only; owner co-location is NOT assumed (one LAN RPC per stateful op is the budget) | v1/v2-draft claimed "receiving node is usually the owner" — false: LBs hash onto their own ring. A future sticky-owner lease (first-touch ownership) could align them but is a separate design with its own fencing story; recorded as future work, not assumed |
| D-14 | `--cluster` + `--runtime per-core` rejected at startup; `--cluster` + intercept mode likewise | Upstream RFC-712's per-core topology runs single-threaded pinned worker runtimes; the §7.7 sync bridge parks caller threads, and a per-core worker has only one thread to park, so a single owner outage would stall every connection pinned to it. Enforced in the #8 config guard. |
| **D-15** | **Embedded Raft (`openraft`) control plane over gossip (ADR-001).** Membership + configs + tenancy + admin intents in a Raft log; flow state stays off consensus. | Bolting a barrier + persist-before-ack + intent log + dedup onto v2 gossip = four hand-rolled protocols atop the settle/generation machinery that only existed because membership wasn't agreed — a worse consensus by hand. External Temporal/etcd violates the zero-dependency premise (revisit only as an *optional* integration, D-12 pattern). **Supersedes D-1, D-2.** |
| **D-16** | **`redb` for all cluster durability** (Raft log/vote/snapshot + the #16 flow WAL). | Hand-rolled WAL is error-prone; `sled` rejected on maintenance; `fjall` kept as the LSM fallback if write-amplification bites. Pure Rust — static-musl/`FROM scratch` safe. `Durability` is `Immediate`-only since redb 2.0, so #16's `async` flow durability is group-commit (batch one `Immediate` per interval), not an `Eventual` mode. |
| **D-17** | **Flow state stays off consensus** (HRW ownership + successor replication + WAL); ownership derived from *committed* membership. | A quorum write per scenario transition at 20–40k RPS is an outage. D-8 (cursor reset on ownership move) and D-12 (Redis-strict path) both still stand. |
