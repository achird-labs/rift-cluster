# Decision register

**This file is the only place a design decision (`D-n`) is defined.** RFCs propose, ADRs argue,
the architecture guide explains, issues carry acceptance criteria, the vault holds analyses — but
when any of them disagree about *what was decided*, the entry here wins, and the others have a
bug. `scripts/design-check.py` enforces the parts of that which can be checked mechanically; the
rest is discipline, described in [`docs/process/design-code-sync.md`](../process/design-code-sync.md).

## How to read an entry

```
### D-16 — `redb` for all cluster durability
- **Status:** amended            active | amended | superseded | pending
- **Decided:** 2026-07-21 · ADR-001 · #14
- **Supersedes:** D-1, D-2       decisions this one retires (they get Superseded by: D-16)
- **Superseded by:** —
- **Amends:** RFC-001 §7.4       spec sections this decision changes — each MUST carry a
                                 "> Amended by D-16" callout (checked)
- **Implemented by:** #436       PRs/issues that landed it; an open one means the decision is
                                 ahead of the code, which is allowed but must be visible
- **Code:** crates/…/store.rs    where the decision lives; paths are checked to exist
```

`pending` means decided but not yet built — the code may still do the old thing. A `pending`
decision must list the open issue that builds it.

## Citation grammar (what code, tests and docs may reference)

| Token | Defined in | Resolved by `design-check` |
|---|---|---|
| `D-<n>` | this file | yes — must exist; citing a *superseded* one from code is flagged |
| `RFC-00N §x.y` | `docs/rfc/RFC-00N-*.md`, heading `### x.y …` | yes — the section must exist |
| `ADR-00N` | `docs/adr/ADR-00N-*.md` | yes |
| `U-<n>` | `docs/architecture/11-upstream-boundary.md` (upstream seams) | yes |
| `R1`…`R4` | RFC-001 §1.1 / `docs/architecture/01-overview.md` (the four load-bearing requirements) | no (fixed set) |
| `C<n>` | `docs/architecture/12-testing.md` (chaos scenarios) | no |
| `docs/<path>.md` | the file | yes — path must exist |
| `#<n>` | GitHub issue/PR | no |

Upstream Rift's own RFCs are a different numbering space; cite them as `rift RFC-712`, never bare.

**Pinning a decision in a test.** Any of the tokens above in the doc comment (`///`) or the
comment lines directly above a `#[test]`/`#[tokio::test]` attribute counts as a pin — that test is
then listed as evidence for the decision. Write the *claim* the test discriminates, not just the ID:

```rust
/// Pins D-19: the fan-out quorum is a majority of BOTH the committed and the effective voter
/// configuration — a committed-only majority would commit an op whose blob 2 of 5 nodes hold.
#[tokio::test]
async fn quorum_is_joint_over_committed_and_effective_voters() { … }
```

## Adding or changing a decision

1. A decision reached anywhere else — an issue thread, a review, a session with an agent, the
   vault — **is not made until it has a `D-n` here.** Open the PR that records it; the code PR
   may be the same PR.
2. Never edit a decision's meaning in place. Amend it (add an **Amendment** paragraph, set
   `Status: amended`) or supersede it (new entry, `Supersedes:` / `Superseded by:` on both).
   Superseded text stays, struck through in the title, because code and history cite it.
3. If it changes what an RFC or chapter says, list the section under `Amends:` and put
   `> **Amended by D-n** (date): <one line>` at the top of that section. `design-check` fails
   without it.
4. Cite it from the code that embodies it, and pin it with at least one test.

---

## Register

### ~~D-1 — No consensus layer; AP + single-writer-by-ownership + settle/generations~~
- **Status:** superseded
- **Decided:** 2026-07-01 · RFC-001 v2
- **Superseded by:** D-15

The four requirements R1–R4 (RFC-001 §1.1) are a request for a strongly consistent *control
plane*, which D-1 declined. D-1's premise — "quorum ops on the request path" — was the error:
Raft carries only the control plane (human/CI-frequency), never the data path. Retained for
history.

### ~~D-2 — chitchat (MIT) for membership + small-KV gossip~~
- **Status:** superseded
- **Decided:** 2026-07-01 · RFC-001 v2
- **Superseded by:** D-15, D-16

Membership is now a Raft-log value (`openraft`); the versioned-KV gossip it provided is replaced
by the Raft state machine. Retained for history.

### D-3 — HRW hashing, no vnodes
- **Status:** active
- **Decided:** 2026-07-01 · RFC-001 v2
- **Code:** crates/rift-cluster/src/raft/ring.rs

Consistent-hash rings with vnodes shine at N≫16 and weighted nodes; HRW is simpler, minimal churn
on membership change, O(N) fine at our scale.

### D-4 — Config bodies via content-addressed RPC fetch, not gossip
- **Status:** amended
- **Decided:** 2026-07-01 · RFC-001 v2
- **Code:** crates/rift-cluster/src/raft/store.rs, crates/rift-cluster/src/blobs/mod.rs

Gossiping full configs blows the SWIM payload budget and re-floods every round; digests converge
fast and bodies transfer once per node.

**Amendment (D-15, 2026-07-21):** with gossip gone, *config* bodies ride the Raft log as small
JSON entries. The content-addressed principle survives for *blobs* (datasets, specs) — see D-18,
D-23.

### D-5 — Two-level, order-aware reconcile (LCS edit script) on top of by-id/positional stub CRUD
- **Status:** amended
- **Decided:** 2026-07-01 · RFC-001 v2
- **Code:** crates/rift-cluster/src/control.rs, vendor/rift/crates/rift-mock-core/src/imposter/reconcile.rs

Whole-imposter replace per change resets runtime state cluster-wide; set-diff (v2 draft 1) missed
reorders and reordered keyless edits — order is match priority, so the edit script must be
order-aware.

**Amendment (2026-08-25, verification pass):** the mechanism is not an LCS. Level 1 is the
explicit `StubEditScript` (`Add`/`ReplaceById`/`DeleteById`/`Move`), applied all-or-nothing by
`apply_edit` and the `PatchStubs` arm. Level 2 is upstream's `reconcile_stub_states` (U-6): stubs
are matched by `stub_key` (explicit id, else an occurrence-counted content hash), surviving keys
keep their slot state, a pure reorder costs nothing, and a change touching more than half the
stubs falls back to a wholesale replace. Order-awareness holds; "LCS" does not.

### D-6 — Redis impls of the new traits are cluster; existing `RedisFlowStore` (incl. U-1 CAS) stays OSS
- **Status:** amended
- **Decided:** 2026-07-01 · RFC-001 v2
- **Code:** crates/rift-cluster-base/src/lib.rs

**Accepted erosion:** OSS + shared Redis can DIY multi-instance scenario/flow-KV correctness.
**The moat is not "coordination"** — any Redis impl of these small traits is
community-reproducible in days — it is zero-dependency clustering, config-sync/membership/HA,
cluster-merged verification, and fleet operations.

*Rejected:* withholding CAS from an existing OSS backend would be bad-faith open-core and raise
more upstream suspicion than shipping it; pretending trait-impl code is the moat mis-prices the
product.

**Amendment (2026-08-25, verification pass):** the first clause is moot — no Redis implementation
of any seam was built on the cluster side (durability is redb, D-16; the Redis-strict path is
D-12, demand-gated, #466). What stands is the second clause: U-1's CAS ships in upstream's
`rift-store-redis::RedisFlowStore`, and the facade withholds nothing Redis-shaped.

### D-7 — Manager-scoped store via provider resolves the construction-time caveat
- **Status:** active
- **Decided:** 2026-07-01 · RFC-001 v2
- **Code:** crates/rift-cluster/src/stores/flow.rs

Per-imposter stores kept for OSS compat; a provider returning a shared store is strictly more
flexible.

### D-8 — Sequence cursors reset on ownership change
- **Status:** active
- **Decided:** 2026-07-01 · RFC-001 v2
- **Implemented by:** #466 (D-47's owner-routed sequencer)
- **Code:** crates/rift-cluster/src/stores/sequencer.rs

Replicating cursors puts a network write on the hottest stateful path; a documented reset matches
test-run-scoped data. A cursor lives only on its owner, so a membership change hands the key to a
node that starts it at zero — the reset is the contract, not a fault.

### D-9 — Sync traits + cluster-side bridge runtime (std mpsc park, sized semaphore)
- **Status:** active
- **Decided:** 2026-07-01 · RFC-001 v2
- **Code:** crates/rift-cluster/src/stores/shard.rs

Async-ifying `FlowStore` ripples into Lua/JS engines and every call site — huge OSS churn
benefiting only clustering.

### D-10 — Degraded reads reject by default (except sequencing = local); per-imposter `readConsistency`
- **Status:** active
- **Decided:** 2026-07-01 · RFC-001 v2 · shipped as per-imposter `readConsistency` (#120), not the `--cluster-degraded-mode` flag first sketched
- **Code:** crates/rift-cluster/src/stores/flow_config.rs

Silent local fallback for CAS/proxyOnce converts partitions into wrong test results — the one
thing a verification tool must never do; sequencing degrades by default because blocking all
cyclic responses during a blip is worse than a possible duplicate index, and it's flagged.

### D-11 — Plain gateway listener upstreams with U-7 (promotion of rift #212); only cluster-aware dispatch stays cluster
- **Status:** active
- **Decided:** 2026-07-01 · RFC-001 v2
- **Code:** crates/rift-cluster-base/src/lib.rs

Keeping a generic single-node convenience cluster-only has bad optics, zero moat (community can
promote #212 trivially), and weakens U-7's story.

### ~~D-12 — Strict sequencing/proxyOnce ship Redis-backed first; gossip-native single-writer versions are demand-gated~~
- **Status:** superseded
- **Superseded by:** D-47
- **Decided:** 2026-07-01 · RFC-001 v2
- **Amends:** RFC-001 §7.5.3
- **Code:** crates/rift-cluster/src/stores/proxy.rs, crates/rift-cluster-server/src/compose.rs

Gossip-exact semantics are the hardest engineering in the RFC aimed at the least-demanded
guarantee; the trait seams make the backend invisible to callers; teams that need this already
operate Redis. The zero-dependency premise stays intact for Phases 1–3 (membership, config-sync,
scenario state, verification).

**Amendment (2026-08-25, verification pass):** proxyOnce did **not** ship Redis-first. #226 built
the zero-dependency form directly — owner-local `Pending` claims on the HRW ring
(`KeyClass::Proxy`) and one consensus `ProxyRecorded` op (D-40). No Redis proxyOnce backend
exists or is planned, and `backend: "redis"` is deliberately not honoured under `--cluster`
(`tests/manager_parity.rs`). The Redis-first ordering now applies to **strict sequencing only**,
which is unbuilt and demand-gated (D-8).

**Scope note (ADR-001, D-18):** D-12 is the pattern "zero-dependency by default, external system
by choice" for *flow state*. It is **not** a precedent for tiering the blob corpus to an object
store — a bucket outage would become a request-path failure (rift-cluster#458).

### D-13 — LB header affinity is stickiness only; owner co-location is NOT assumed
- **Status:** active
- **Decided:** 2026-07-01 · RFC-001 v2

One LAN RPC per stateful op is the budget. v1/v2-draft claimed "receiving node is usually the
owner" — false: LBs hash onto their own ring. A future sticky-owner lease (first-touch ownership)
could align them but is a separate design with its own fencing story; recorded as future work,
not assumed.

### D-14 — `--cluster` + `--runtime per-core` rejected at startup; `--cluster` + intercept mode likewise
- **Status:** active
- **Decided:** 2026-07-01 · RFC-001 v2 · enforced in the #8 config guard
- **Code:** crates/rift-cluster-server/src/cli.rs

Upstream rift RFC-712's per-core topology runs single-threaded pinned worker runtimes; the RFC-001
§7.7 sync bridge parks caller threads, and a per-core worker has only one thread to park, so a
single owner outage would stall every connection pinned to it.

### D-15 — Embedded Raft (`openraft`) control plane over gossip
- **Status:** active
- **Decided:** 2026-07-21 · ADR-001 · #14
- **Supersedes:** D-1, D-2
- **Amends:** RFC-001 §7.1, RFC-001 §7.2, RFC-001 §7.4
- **Code:** crates/rift-cluster/src/raft/node.rs, crates/rift-cluster/src/raft/store.rs

Membership + imposter configs + the `enabled` bit + tenancy/RBAC records + admin intents in one
Raft log; **flow state stays off consensus** (D-17). Putting membership itself into the log is the
move that pays for everything else: the roster becomes a linearizable value, so at any log index
every node computes byte-identical membership and therefore byte-identical ownership. The settle
delay, the generations, the epoch-mismatch retries are deleted, not mitigated.

*Rejected:* bolting a barrier + persist-before-ack + intent log + dedup onto v2 gossip = four
hand-rolled protocols atop the settle/generation machinery that only existed because membership
wasn't agreed — a worse consensus by hand. External Temporal/Restate/DBOS/etcd violates the
zero-dependency premise (revisit only as an *optional* integration, the D-12 pattern).

*Paid:* a minority partition cannot write config (`503 + Retry-After + op-id`, intent parked and
replayed on heal); elections (~1–3 s) pause admin writes invisibly; every voter needs a real disk.

### D-16 — `redb` for all cluster durability
- **Status:** amended
- **Decided:** 2026-07-21 · ADR-001
- **Implemented by:** #436
- **Code:** crates/rift-cluster/src/raft/store.rs

Raft log, vote and snapshot *metadata*, plus the flow WAL (#16), live in `redb` (`Durability::Immediate`
only — the `Eventual` mode assumed in early drafts was removed in redb 2.0, so #16's `async` flow
durability is group commit, not an `Eventual` mode). `sled` rejected on maintenance; `fjall` kept
as the LSM fallback if write amplification bites. Pure Rust — static-musl / `FROM scratch` safe.

**Amendment (2026-08-24, #436):** the snapshot *payload* is a plain file beside redb —
`SNAPSHOT_TABLE` keeps only `{meta, file}`. Inlining it as a redb value cost ~3.7× the bytes it
carried and read the whole payload on every send. The decision stands for the log, the vote and
the snapshot's metadata; only where the payload bytes are written changed.

### D-17 — Flow state stays off consensus
- **Status:** active
- **Decided:** 2026-07-21 · ADR-001
- **Implemented by:** #465 (the isolated-owner rule, for flow KV), #472 (the measured cost)
- **Code:** crates/rift-cluster/src/stores/flow.rs, crates/rift-cluster/src/raft/ring.rs

**Paid, honestly — measured (2026-08-28, #472; supersedes the #465 estimate).** Enforcing the
isolated-owner rule puts one consensus-shaped pause on the otherwise consensus-free flow path:
`is_isolated()` is `true` whenever a node's `current_leader` is unknown, so a node that has lost
sight of the leader refuses owner-side flow writes and `strong` reads until a leader is
re-established. The cost is **not** the "sub-second typically, up to the ~1–3 s D-15 accepts"
figure this entry carried from #465 — that was reasoned from the election *timeout*, never
measured, and it is ~30× too high. Measured on a 3-node cluster (leader killed, survivors sampled
at ~1 ms, 10 rounds / 20 observations):

- a **follower** isolates **450–600 ms** after it last heard the leader — openraft sets
  `leader_lease = election_timeout_max` and campaigns only after `leader_lease +
  rand(election_timeout_min..max)`, so followers already have a lease-shaped grace and this
  primitive only bites after it;
- a **leader** isolates **900 ms** after its last quorum ack (`ISOLATION_WINDOW_MS`);
- a routine election isolates each node for **tens of milliseconds** — the election round trip
  plus the new leader's first quorum-ack, because `current_leader` is `None` exactly while the
  node's vote is uncommitted — plus 150–300 ms per extra round on a split vote (0 of 10 rounds).
  The #472 probe measured **13–31 ms**; re-measuring while writing this entry's guard test, on
  different hardware and with a coarser ~8 ms sampler, gave **32–40 ms**. Both are the same
  quantity and both are two orders of magnitude below the figure this entry used to state; take
  the band as **~13–40 ms, hardware-dependent**, and do not quote either end as exact.

State the asymmetry plainly: the follower grace is openraft's 450–600 ms, the leader grace is our
900 ms. At ~25 ms per node per election this is stricter than this entry's own
"3 × election_timeout" wording, which would ride out a routine election; the primitive fails closed
immediately and that is what ships. `local` reads (D-10) are unaffected.

*Rejected (#472, on these numbers):* a follower-side grace matching the literal wording — it buys
~25 ms and pays ~900 ms, letting a partitioned minority owner serve for ~1.4 s before refusing,
which is the "CAS succeeded, then vanished" outcome D-10 exists to forbid. *If* election-time
`503`s are ever observed in a real deployment, the shape to reach for is a **bounded wait** in
`owner_write` and the owner branch of a `strong` read (delay the decision rather than refuse it;
zero safety regression, since the node still never serves while leaderless) — that would change
what an election costs the data path from errors to latency, so it gets its own `D-n` rather than
being folded in here.

HRW ownership + successor replication + WAL; ownership is *derived from committed membership*. A
quorum write per scenario transition at 20–40k RPS is an outage, not a design. D-8 (cursor reset
on ownership move) and D-12 (Redis-strict path) both still stand. The residual window (a
partitioned node that has not applied the entry deposing it) is closed by the isolated-owner rule:
no leader heartbeat within `3 × election_timeout` ⇒ reject owner-side stateful ops.

### D-18 — Every member holds every live blob
- **Status:** active
- **Decided:** 2026-08-24 · ADR-001 · #432, rift-cluster#458
- **Amends:** RFC-005 §3.2
- **Implemented by:** #437, #438 (merged), #439 (open), #440 (open), #441 (open — the RFC/chapter revision)
- **Code:** crates/rift-cluster/src/blobs/mod.rs, crates/rift-cluster-server/src/admin_front.rs

The content-addressed blob store (#437) is quorum-complete on each node; an object store (#448)
is an opt-in cache/backup tier that is never consulted on the serving path and never a condition
for apply. D-12 covers flow state, not the blob store.

Refined by D-51: "holds" means **can serve**. A member answers a blob read from applied state
when its own transport store misses, so completeness is a property of the state machine rather
than of how the bytes happened to arrive.

The store itself replicates nothing — two nodes holding different blob sets is normal, not
divergence. Completeness is established by the **write path** (#438): the accepting node stores
the blob, fans it out to the members, and proposes the referencing op only once a quorum
acknowledges the digest, so a commit implies quorum-durability — the guarantee the log itself
provided while the bytes were still on it. **Until D-23 (#439) lands** the guarantee at commit is
quorum-completeness, not every-member completeness — a member the fan-out did not reach still
receives the bytes from the log entry, so nothing is lost, but "every member holds every live
blob" is the target state, not yet the invariant. *As of #439 it is the invariant:* a member the
fan-out did not reach fetches the blob on apply (D-48), so completeness is the write path plus
fetch-on-apply, and a member that cannot fetch parks rather than diverges.

*Rejected:* object-store tiering of the corpus (evict cold datasets locally, fetch from a bucket
on demand). A bucket outage, credential rotation or lifecycle deletion becomes a request-path
failure, and a blob no member holds can never apply, stalling the log fleet-wide. RFC-005 §3.2
bounds dataset bytes by quota precisely so the corpus is consensus-worthy small and fully
replicated; a corpus that outgrows a voter's disk is a redesign with numbers, not a tier.

### D-19 — The blob fan-out quorum is joint consensus
- **Status:** active
- **Decided:** 2026-08-24 · ADR-001 · #438
- **Implemented by:** #438
- **Code:** crates/rift-cluster/src/raft/network.rs, crates/rift-cluster-server/src/admin_front.rs

A majority of *both* the committed and the effective voter configuration, read in a single
`with_raft_state` closure so the pair cannot be assembled from two membership epochs. Neither
set alone is sound: a cluster growing 3→5 with the new config uncommitted has a committed
majority of 2, which would commit an op whose blob is on 2 of the 5 nodes now in force; and
effective membership can carry an uncommitted entry from a deposed leader that later truncates.

A majority of both configurations is a set no single membership change can empty — precisely the
precondition #439's fetch-on-apply needs in order to find a holder at all. An ack from a node
outside a configuration does not count toward that configuration, and a member whose build cannot
serve blobs counts toward neither.

### D-20 — Only a flow has an owner; imposters, stubs and config own nothing
- **Status:** amended
- **Decided:** 2026-08-09 · #359 (corrected), `docs/design/console/README.md`
- **Code:** crates/rift-cluster/src/raft/ring.rs, crates/rift-cluster/src/stores/flow_config.rs, crates/rift-cluster/src/stores/proxy.rs

Imposters, stubs and config go through Raft (D-15): a write propagates from the leader to every
node, every node serves any imposter. There is no "port owner" and no `OWNER` column for an
imposter — a port has as many owners as it has flows. A flow has exactly one owner
(`KeyClass::FlowKv`, HRW over the *applied* membership) plus `REPLICAS` successors; a misrouted
write answers `NotOwner{owner}`.

**The ring key is scoped, not the bare flow id:** `ContextScope::prefix_for(port)` — `Imposter`
(default) → `i{port}:`, `Fleet` → `f:`. Under `Fleet`, two imposters' same-named spaces are one
flow with one owner. Any code deriving an owner from the bare id names the wrong node.
`KeyClass::Config` is vestigial (the gossip-era config owner) and must never be renumbered — tags
are hash inputs, so moving one silently reassigns live flows.

**Amendment (2026-08-25, verification pass):** "only a flow has an owner" is precise for *config*
vs *state*, but there are two owned key classes, not one: flow state (`KeyClass::FlowKv`) and
proxyOnce claims (`KeyClass::Proxy`, key `(port, signature)`, D-40). Read the title as "only
*state* has an owner; replicated config never does".

*Why it is registered:* #358 shipped an `OWNER` column on the imposter table and #359 asked to
fill it in; both were built on the wrong premise. An issue whose facts all check out can still be
wrong at the intent level — this entry is what a triage checks it against.

### D-21 — Cluster membership changes only via a node joining or leaving
- **Status:** active
- **Decided:** 2026-08-10 · #366 (rejected by design)
- **Code:** crates/rift-cluster/src/raft/node.rs

Adding or removing a learner, a voter, or any member happens **only** by starting a node that
joins (`join_via` → the HMAC-signed cluster port → `admit`, two-phase since #450) or by a node
leaving. The console must not offer it and the admin API must not expose a route for it.

*Why:* membership is the cluster's trust boundary. Admission is initiated by the joining node, so
what can enter the fleet is bounded by what an operator chose to *start*. An admin-API "add
learner" taking an advertise address would be a second, weaker entry point — operator input
written straight into the replicated membership log. Snapshot/compaction actions (#365) are a
separate question; this entry does not settle them.

### D-22 — Liveness probes bypass the peer-health gate
- **Status:** active
- **Decided:** 2026-08-24 · #431
- **Implemented by:** #431, #442, #449
- **Code:** crates/rift-cluster/src/rpc/client.rs

`TrackedPeerHealth` refuses calls to a peer for its cooldown after a failure. A heartbeat or
keepalive routed through that gate is refused for the whole cooldown after the peer restarts, so
the restarted voter never hears the leader, campaigns, and diverges its term — the fleet
livelocks. Any liveness mechanism therefore uses `RpcClient::probe`, which bypasses `is_healthy`
and clears the mark on success; never `call`/`call_once`. A caller's own deadline expiring is not
evidence the peer is down (#442).

*Why it is registered:* three earlier fixes (keepalive during chunks, ticker, grace) each looked
correct and each failed for this unmeasured reason. Instrument both ends before changing timers.

### D-23 — The bytes leave the log: blobs are sideloaded, ops carry digests
- **Status:** active
- **Decided:** 2026-08-24 · #432 (epic), RCA "Bytes on the Log"
- **Amends:** RFC-005 §3.2, RFC-004 §4.1
- **Implemented by:** #436, #437, #438, #439, #440; prose revision #441 (this PR)
- **Code:** crates/rift-cluster/src/blobs/mod.rs, crates/rift-cluster/src/raft/store.rs, crates/rift-cluster/src/raft/blob_source.rs

RFC-004/005 put 4–64 MiB blobs through a Raft log and snapshot whose timers, health tracker,
snapshot encoding and admission protocol all assume KiB entries; openraft 0.9 opens a silent
no-heartbeat window during a large entry or snapshot, a voter campaigns, and leader stickiness
never reconciles (#411, #430, #431, #433). The reason RFC-005 §3.2 gave for keeping bytes on the
log — "at that scale a log entry is unremarkable" — is measured to be false at exactly the quota
sizes those RFCs set.

Target: `SpecPut`/`DatasetPut` carry `{digest, size, meta}`; apply requires the digest locally
and a follower that lacks it fetches (origin first, then any member) before applying; snapshots
are manifests of digests. RFC-005's ordering argument survives intact — log order still
guarantees the bytes are on disk before any config referencing them applies. Do not re-argue the
reversal; #441 revised the prose (RFC-005 §3.2, RFC-004 §4.1, ch.3, ch.9).

Refined by D-48 (what a node does when no member can supply a blob) and D-49 (the wire shape
that keeps every existing log replayable, and where the bytes leave the op).

### D-24 — The cluster maintains itself: no snapshot or log-compaction admin actions
- **Status:** active
- **Decided:** 2026-08-10 · #365 (rejected by design)
- **Code:** crates/rift-cluster/src/raft/store.rs

Snapshots are taken at openraft's log-entry threshold (`LogsSinceLast`, default 5 000 — there is
no size threshold) and the log is purged behind them; no
admin route or console panel triggers either. openraft *does* expose `trigger().snapshot()` and
`purge_log` — recorded here so nobody re-derives "it is possible" into "it is wanted". Reading
the durability and write-path settings back (#394) is a separate, accepted request.

### D-25 — Leader-enforced voter floor of two; no orchestrator signal
- **Status:** active
- **Decided:** 2026-08 · #69
- **Code:** crates/rift-cluster/src/raft/node.rs, crates/rift-cluster/src/raft/network.rs

A graceful leave that would drop the voter set below two is refused by the leader. The fleet is
not told whether a departure is a rolling restart or a teardown — a Kubernetes annotation was
rejected as orchestrator-specific — so the residual raciness of a fast roll is accepted rather
than papered over with a signal only one platform can send.

### D-26 — The `departed` marker and the join-or-bootstrap table; no state wiping
- **Status:** amended
- **Decided:** 2026-08 · #72
- **Code:** crates/rift-cluster-server/src/compose.rs, crates/rift-cluster/src/raft/node.rs

A node that left gracefully writes a `departed` marker beside its state; on start, the marker,
the presence of a Raft vote, and the reachability of seeds decide between *join*, *rejoin* and
*bootstrap* by a fixed table (operator guide, "departed marker"). Wiping state to force a clean
join was rejected: it turns an operator mistake into data loss and hides the case the table is
there to make explicit.

**Amendment (2026-08-25, verification pass):** the node id is minted by the node itself at first
start — from `--cluster-node-name` when set, otherwise from the clock — never by the leader; the
join request carries it. A redeployed pod with the same name and a wiped state dir therefore
returns as the *same* node, which is the one case where "no state wiping" and "a new node"
coincide.

### D-27 — `MAX_AUTO_VOTERS` is a soft ceiling; promotion only ever adds voter ids
- **Status:** active
- **Decided:** 2026-08 · #55
- **Code:** crates/rift-cluster/src/raft/node.rs, crates/rift-cluster/src/raft/network.rs

Automatic promotion stops at `MAX_AUTO_VOTERS`; beyond it a node stays a learner. Membership
changes are always `AddVoterIds`, never `ReplaceAllVoters`, so a promotion cannot silently evict.
An operator-driven `change_membership` may race the soft gate — by design, the ceiling bounds
what the *fleet* does on its own, not what an operator chooses.

### D-28 — Dial every resolved address; do not prefer IPv4
- **Status:** active
- **Decided:** 2026-08 · #79
- **Code:** crates/rift-cluster/src/rpc/client.rs

A peer's hostname is re-resolved on every send and every returned address is tried in the
resolver's (RFC 6724) order. A prefer-IPv4 knob was rejected: it encodes one network's bug into
every deployment and hides dual-stack misconfiguration instead of surfacing it.

### D-29 — Deleting a source orphans its imposters; never cascades
- **Status:** active
- **Decided:** 2026-08 · #253
- **Code:** crates/rift-cluster/src/control.rs, crates/rift-cluster/src/sources/mod.rs

`SourceDelete` removes the source record and stops polling; imposters it created stay, now
unowned by any source. Cascading would delete live mocks on an admin's bookkeeping change. No
separate `Action::SourceWrite` — sources are authorized under the existing config actions, by
precedent.

### D-30 — Object-store offload is untrusted, opt-in, and never on the serving path
- **Status:** pending
- **Decided:** 2026-08-24 · #448 (tracking), #456, #457
- **Implemented by:** #456 (open), #457 (open)
- **Code:** crates/rift-cluster/src/blobs/mod.rs

Refines D-18. Bytes read back from a bucket are untrusted until their digest is verified — an
acceptance criterion, not an optimization. Cache (mirror) and backup want opposite retention
policies and are separate tiers. Restoring a fleet from a snapshot backup requires
`--as-new-fleet`: it rewrites membership to a single voter and bumps the term, so a restored
node can never be mistaken for a member of the fleet it was copied from.

### D-31 — `PollStatus` is node-local; `SourceRecord` is fleet-replicated
- **Status:** active
- **Decided:** 2026-08 · #233, #239
- **Code:** crates/rift-cluster/src/sources/scheduler.rs, crates/rift-cluster/src/sources/mod.rs, crates/rift-cluster-server/src/admin_front.rs

What a source *is* (URL, credentials ref, interval) is a Raft value; when *this node* last polled
it and what it saw is not. A response that flattens the two into one shape would present a
node-local observation as fleet state — the past-state-as-present error. They stay separate
fields with separate provenance.

### D-32 — Fleet request tail over a capped, declared coverage set
- **Status:** active
- **Decided:** 2026-08 · #362
- **Code:** crates/rift-cluster-server/src/admin_front.rs, crates/rift-cluster-server/src/cli.rs

The fleet-wide request tail carries a `port → JournalCursor` map over a coverage set capped by
`fleet_journal_port_cap` (default 100) and reports `coverage: {covered, total, omitted}` on every
response, so a partial view is never mistaken for the whole. A `(timestamp, tiebreak)` watermark
was rejected — clocks are not ordered across nodes (Ch.7) — and a fourth, hybrid shape was
rejected *visibly* here so it is not rediscovered.

### D-33 — An unclustered node is indistinguishable from the open-source binary
- **Status:** amended
- **Decided:** 2026-08 · #297
- **Code:** crates/rift-cluster-server/src/console.rs

Without `--cluster`, `rift-cluster-server` serves exactly what `rift` serves: `/console` answers
404, no `Rift-Cluster-*` headers, no fleet routes. It is a design invariant, not observed
behaviour — the binary must be a drop-in for the OSS one so a single-node deployment gains
nothing and risks nothing by using it.

**Amendment (2026-08-25, verification pass):** the invariant has two independent halves. Without
`--cluster` there is no console regardless of build; and a build without the `console` feature
serves no console even when clustered. `tests/passthrough.rs` pins the first, `tests/console_off.rs`
the second — same decision, two gates.

### D-34 — `git+` sources are a detected capability in the `-static` image
- **Status:** active
- **Decided:** 2026-08 · #270
- **Code:** crates/rift-cluster/src/sources/git.rs

The musl/`FROM scratch` image cannot carry a git binary. A `git+` source on such a node fails
loudly at source creation with a capability error rather than at the first poll; the capability
is probed, not assumed from the build.

### D-35 — Portable release artifacts are the Helm chart and the GHCR image
- **Status:** amended
- **Decided:** 2026-08 · #264 (epic)
- **Code:** deploy/helm, .github/workflows/release.yml

Non-goals, stated so they are not re-proposed: no Terraform / CloudFormation / Bicep modules, no
OS package-manager packages, no auto-update. Chapter 14/15 reference deployments consume the chart
and the image; anything else is the operator's composition.

**Amendment (2026-08-25, verification pass):** per-platform binary tarballs + `SHA256SUMS` on the
GitHub Release (`release.yml` jobs `binaries`/`release`) also ship, as the no-container path. They
are not a third deployment shape: the chart and the image remain the only artifacts the reference
deployments consume.

### D-36 — Flow-eviction ties are broken by a monotone touch sequence
- **Status:** active
- **Decided:** 2026-08-18 · #408
- **Code:** crates/rift-cluster/src/stores/shard.rs

LRU eviction keyed on a millisecond timestamp alone evicts the wrong flow when several are touched
within the same millisecond; a process-wide monotone sequence (`static TOUCH_SEQ`) stamped on
every touch breaks the tie so LRU holds at any rate.

### D-37 — The journal is per-writer shards, merged on read
- **Status:** active
- **Decided:** 2026-08 · #223, #224 (RFC-001 §7.5.1 as built)
- **Code:** crates/rift-cluster/src/stores/journal.rs, crates/rift-cluster/src/stores/journal_net.rs

Every node appends only to its own `(port, node_id)` shard; a read k-way-merges the shards by
recorded timestamp with `(node_id, seq)` breaking ties. Caps are writer-local with an
`evicted_below_seq` watermark; an unreachable peer yields `Rift-Cluster-Partial`, never a stall.

*Rejected:* owner-routed or consensus-carried journaling — a mock request must never wait on
another node to be recorded.

### D-38 — Clears are generation bumps, never timestamps
- **Status:** active
- **Decided:** 2026-08 · #223 (RFC-001 §7.5.2 as built)
- **Amends:** RFC-001 §7.5.2
- **Code:** crates/rift-cluster/src/control.rs, crates/rift-cluster/src/stores/journal.rs

A monotone per-port (and per-`(port, space)`) clear generation rides the Raft log as
`ControlOp::JournalClearGen`; entries and counter slots carry their writer's generation, and the
merge drops anything from an older one. RFC-001 §7.5.2's "gossip carries clear generations" and
its `teardown_space` markers were never built — the generation is a committed value.

*Rejected:* timestamped deletion (clocks are not ordered across nodes); `retain` predicates stay
best-effort per shard.

### D-39 — The journal cursor is a vector, opaque by contract
- **Status:** active
- **Decided:** 2026-08 · #225, #348 (RFC-001 §7.5.1 as built)
- **Amends:** RFC-001 §7.5.1
- **Code:** crates/rift-cluster/src/stores/journal.rs, crates/rift-cluster/src/stores/journal_net.rs

`since` is `v1 {gen, pos: node_id → seq}`, base64url-JSON; per-shard filtering, monotone advance,
dead shards frozen rather than rewound; a bare `u64` is read as `{this_node: seq}` for the upgrade
window. `x-rift-truncated` replaces the RFC's `Rift-Cluster-Cursor-Lapsed`; `Cursor-Reset` is
carried but not acted on. The fleet-wide form is `port → JournalCursor` with its own scope tag
(D-32 builds on it).

*Rejected:* a scalar cursor — it cannot name a position across writers.

### D-40 — proxyOnce is Pending/Recorded with one committed op and a fixed claim TTL
- **Status:** active
- **Decided:** 2026-08 · #226 (RFC-001 §7.5.3 as built)
- **Amends:** RFC-001 §7.5.3
- **Code:** crates/rift-cluster/src/stores/proxy.rs, crates/rift-cluster/src/control.rs

A `Pending` claim is owner-local (`KeyClass::Proxy`, HRW over `(port, signature)`) and dies with
its owner; `Recorded` and its replayable stub are one `ControlOp::ProxyRecorded`, so there is no
crash window between the recording and the config write. The claim TTL is a fixed 60 s — U-16
carries no timeout context, so "2× the upstream timeout" was not derivable. A partitioned owner
refuses claims. No Redis-backed proxyOnce was built or is planned (see D-12's amendment).

*Rejected:* two ops (`PatchStubs` then a marker) — a crash between them duplicates the recording;
an op-id derived from `(port, signature)` — dedup comes from the owner-validated claim token plus
the committed row instead.

### D-41 — `cluster-smoke` runs every scenario once as a required check; flake detection is the nightly soak
- **Status:** active
- **Decided:** 2026-07 · #104, #11
- **Amends:** RFC-001 §12
- **Code:** .github/workflows/ci.yml, .github/workflows/nightly-chaos.yml

PR-time `cluster-smoke` runs each container chaos scenario once and is a required status check;
`nightly-chaos.yml` iterates 60–100× per scenario under a 2 h cap and is where flakes surface.

*Rejected:* RFC-001 §12's three iterations per PR (~25 → 70+ min per cluster-touching PR for
little the soak does not catch) and a flat 100× nightly (C6's 60 s toxic window alone is ~3.6 h).

### D-42 — C6 bounds an election *rate*; election timers are not an operator knob
- **Status:** active
- **Decided:** 2026-07 · #94
- **Code:** tests/cluster-chaos/tests/scenarios.rs, crates/rift-cluster/src/raft/node.rs

C6's injected jitter overlaps the 150–300 ms election timeout by design, so occasional elections
are in spec; the scenario bounds leadership transitions by `C6_MAX_LEADER_TRANSITIONS` (derived
from the ~5 s gauge resolution), never by a fixed count.

*Rejected:* widening the election timeout so a count bound holds — the timers stay fixed in
`raft/node.rs`; making them a `NodeConfig` knob needs its own design pass and has no operator
requirement behind it.

### D-43 — `--cluster-snapshot-log-entries` is a hidden testability knob, not operator tuning
- **Status:** active
- **Decided:** 2026-08 · #183
- **Code:** crates/rift-cluster-server/src/cli.rs, crates/rift-cluster/src/raft/node.rs

Sets `snapshot_policy = LogsSinceLast(N)` and `max_in_snapshot_log_to_keep = 0` together so the
container tier can force a real `install_snapshot`; `hide = true`, unset by every shipped
configuration, and present only in `snapshot-install.overlay.yml`.

*Rejected:* documenting it for operators (trades log retention for nothing — no tuning
requirement exists) and putting it in the shared `chaos.overlay.yml` (it changed every catch-up
path and broke C4/C6/C7).

### D-44 — The first principal closes the open admin plane
- **Status:** active
- **Decided:** T2 (#161) · RFC-002 §3.4
- **Code:** crates/rift-cluster-server/src/principal.rs

A fleet with neither `--api-key` nor any stored principal keeps the pre-tenancy open admin plane,
so an upgrade denies nobody. The moment the first principal is committed — or a key is
configured — every request must authenticate; there is no grace window and no per-node flag to
reopen it (`should_bypass` is `api_key.is_none() && !has_any_principals()`).
`rift_cluster_no_principals` reports the open state for audit. Bootstrap therefore goes through
`MB_APIKEY` (legacy key = `tenant-admin@default` + `fleet-admin@*`), then minted principals.

*Rejected:* requiring a key whenever `--cluster` is set — breaks every existing keyless fleet on
upgrade.

### D-45 — Cross-tenant and unowned-port probes answer one indistinguishable 404
- **Status:** active
- **Decided:** T2 (#161) · RFC-002 §8.4 · narrowed by #182
- **Code:** crates/rift-cluster-server/src/admin_front.rs

A tenant the principal is not bound to, a port owned by another tenant, and a port owned by
nobody all answer the same terse `404`; `403` is reserved for "bound here, role insufficient".
The gate refuses unowned ports too, because upstream's descriptive 404 names the port and would
otherwise let a tenant map which ports other tenants hold.

*Rejected:* letting unowned ports fall through to upstream's 404.

### D-46 — The legacy `--api-key` cannot hold a console session
- **Status:** active
- **Decided:** C2 (#185)
- **Code:** crates/rift-cluster-server/src/admin_front.rs

`POST /session` with the legacy key answers `400`, never a cookie: the synthetic
`legacy:api-key` identity has no principal row, so a session minted for it could never resolve
back to a principal — a `200` there is the silent-fallback shape. Operators mint a real principal
and log in with its key; the legacy key is a curl/bootstrap credential only.

*Rejected:* minting the cookie and letting later requests fail `401` — indistinguishable from a
rotated key or a skewed clock.

### D-47 — Strict sequencing is owner-routed on the ring, opt-in per imposter, and degrades rather than fails
- **Status:** active
- **Decided:** 2026-08-26 · #466
- **Supersedes:** D-12
- **Amends:** RFC-001 §11.3
- **Implemented by:** #466
- **Code:** crates/rift-cluster/src/stores/sequencer.rs, crates/rift-cluster-server/src/compose.rs

A response cursor is owned by one node — HRW over the applied membership under
`KeyClass::Sequence` — so `responses: [A, B, C]` cycles once fleet-wide instead of once per node
behind a round-robin load balancer. Opt-in per imposter via `_rift.sequencing.mode: "owner"`;
absent or `"local"` keeps per-process cursors, byte-identical to a single-node rift.

**No Redis backend, and none planned.** D-12's reason for Redis-first — that gossip-exact
single-writer semantics were the hardest engineering in the RFC — died with D-15: ownership is now
a deterministic function of committed Raft membership, and the owner-routed pattern already exists
end to end in `FlowNet` and was reused for proxyOnce (D-40). A Redis sequencer would be the only
external dependency on the data path, for the feature with the weakest consistency need of the
three.

**A cluster failure is a fallback, never an error.** D-10 already settled that sequencing is the
one stateful op where availability beats consistency: an unreachable, isolated or fenced owner
means the decision is served from this node's own cursor and the response is annotated, counted by
`rift_cluster_sequence_fallbacks_total`. A `503` here would block every cyclic response during a
leadership blip, which is worse than a possible duplicate index. The consequence worth stating: a
wiring bug looks exactly like a degradation, so the counter — not the returned index — is what
tells the two apart, and the acceptance test asserts it does *not* move on a healthy fleet.

**Keyed by `stub_key`, not the engine's `slot`** (RFC-001 §8.3): `slot` is node-local and cannot
be a cluster key. Documented divergence: editing a *keyless* stub changes its `stub_key` and so
restarts its cluster cursor, where a single-node `LocalSequencer` preserves it. A stub relying on
cross-node sequencing should carry an explicit `id`.

*Deferred, not done:* the peek-amplification benchmark RFC-001 §11.3 asks Phase 4 for, and a
container chaos scenario — both additive verification on a working feature (#476).

### D-48 — A blob no member can supply parks apply and reports degraded; the node never halts
> **Amended by D-56** (2026-08-28, #513): "never gives up" holds while the node is **up**. A
> shutdown ends a parked fetch, because the park holds the storage handle the node must release
> to stop.
- **Status:** active
- **Decided:** 2026-08-25 · #439 (user ruling)
- **Implemented by:** #439
- **Code:** crates/rift-cluster/src/raft/blob_source.rs, crates/rift-cluster-server/src/cluster_api.rs, crates/rift-cluster-server/src/fleet.rs

Refines D-23. Fetch-on-apply asks the write's origin first, then every other joint voter
(D-19's set — the one no single membership change can empty), and **never gives up**: after
`BLOB_FETCH_ESCALATE_AFTER` (30 s) the node logs at error level, sets
`rift_cluster_blob_fetch_stalled` to `1`, counts `rift_cluster_blob_fetch_stalls_total`, and
reports the stall on `/_cluster/health` as `blob_fetch_stall` (`/_fleet/health` rolls it up as
`blob_fetch_stalls_fleet`) — then keeps asking, with capped backoff, and clears all of it the
moment a holder returns.

**Degraded is not not-ready.** `ready` and `state` are untouched and the node stays in the load
balancer: pulling it would only widen whatever partition caused the stall. Every committed write
behind the parked entry is unapplied on that node until it clears, which is the failure mode
D-18's rejected alternative names — made visible rather than silent.

*Rejected:* the issue's original `StorageError` after a bounded retry. An error out of `apply` is
fatal to the openraft state machine, so a partition longer than the bound would take a healthy
node down with no self-heal. *Also rejected:* refusing the op (`Ok(Err(..))`) — the entry is
already committed and "do I have these bytes" is node-local, so holders would apply and
non-holders refuse, which is replica divergence. A peer whose build cannot serve blobs at all
(`UnknownRoute`/`VersionSkew`, both 404 on the wire) is reported as *skewed*, separately from
one that merely lacks the blob, so an upgrade in progress is not misread as a partition; and a
peer that *refused* (credential, request shape, mismatched bytes) has its refusal carried on the
stall as `last_error`, never flattened into "no member holds the blob".

The retry loop is entered from the **replay path too**: `compose::drain_parked_intents` runs a
parked blob write through the same fan-out-then-strip as a fresh one (D-49), so a replay never
puts the payload back on the log.

**Amended by D-52 (2026-08-27): a parked apply cannot be rescued by compaction.** openraft's
state-machine worker is a single sequential loop over one command channel
(`openraft-0.9.25/src/core/sm/worker.rs`): `CommandPayload::Apply` is awaited **inline**, and
`CommandPayload::InstallFullSnapshot` is a sibling arm of the same `match` — nothing is spawned
(unlike `BuildSnapshot`, which is). Since `apply` awaits `resolve_blobs`, which awaits a fetch that
retries forever under this entry, a parked apply blocks the worker: **any snapshot install queued
behind it never runs.** So a park ends only when a holder returns — never by the node being handed
a snapshot that would skip the blob. This is why D-52's rule B (holders keep a blob that is being
actively requested) is not a redundancy but the sole recovery path for a replica that has already
parked, and why "wait for compaction" was never a real remedy.

**Amended by D-56 (2026-08-28): the same inline-await is why a parked node could not stop.** The
worker that is blocked here owns the `RedbStateMachine` clone, so while a fetch retries, nothing
drops the redb handle: `RaftNode::shutdown`'s storage-release wait timed out and returned `Err`
with the file lock still held, and reopening the data directory in the same process failed with
`Database already open`. D-56 signals the fetch on shutdown so the worker can exit.

### D-49 — Payload fields stay optional on the wire; the bytes leave the op at submit, after the quorum
> **Amended by D-53** (2026-08-27): a quorum ack is no longer sufficient to strip — every member
> must also be known to apply a digest-only op.
- **Status:** active
- **Decided:** 2026-08-25 · #439
- **Implemented by:** #439
- **Code:** crates/rift-cluster/src/control.rs, crates/rift-cluster/src/raft/store.rs, crates/rift-cluster-server/src/admin_front.rs

Refines D-23. `SpecPut.document` and `DatasetPut.csv` become `Option<String>` with
`#[serde(default)]`, plus `origin: NodeId` — **not removed**. Raft log entries are plain
`serde_json` with no envelope version, so every `SpecPut`/`DatasetPut` already committed in
every existing cluster carries these fields; a build that could not deserialize them could not
replay its own log. `Some` therefore means "a pre-#439 entry" and applies exactly as before,
with no fetch; `None` is everything this build writes. `SpecMeta` gains `size`, because the
quota was measured from the very bytes that are leaving.

**The strip happens at submit, not at mint.** Ops reach the admin front carrying their bytes, so
`control::validate` still proves digest⇔bytes on the full op there; `fan_out_then_submit` fans
the bytes out, and only once a joint quorum holds them (D-19) strips the payload, stamps
`origin`, and submits — while holding the GC pin, and owning the submit so no caller is ever
handed a guard it could drop early (#438's disclosed pin-hold gap, closed structurally). The
parked intent is the un-stripped copy, so a replay re-fans. A minted entry is < 4 KiB.

**Fetch-on-apply is a pre-transaction pass.** `apply` opens one redb write transaction for the
whole batch; a fetch is up to 17 round trips and holding the transaction across it would block
every other write — the leadership-costing stall #444 closed. So every digest-only op's bytes
are resolved *before* `begin_write()`, and the apply arms treat absent-from-both-op-and-resolution
as a hard error rather than a default: a replica that applied an empty document while its peers
applied the real one is exactly the divergence content addressing exists to prevent.

### D-50 — Snapshots carry a manifest of digests; the joiner fetches the bytes on install
- **Status:** active
- **Decided:** 2026-08-26 · #440 (#432 child 5)
- **Implemented by:** #440
- **Code:** crates/rift-cluster/src/raft/store.rs

Refines D-23 (the bytes leave the log) — the snapshot half of it, as D-49 is the log-entry half.
`SnapshotPayload`'s `spec_blobs`/`dataset_blobs` become the **manifest** — `(digest hex, byte
size)` per referenced blob — not the bytes. `build_snapshot` records each blob's byte length
(1.00× the raw bytes since #436, == `SpecMeta.size`); a fleet holding 64 MiB of datasets snapshots
to KiB (#440 AC1).

`install_snapshot` fetches every manifest digest this node lacks **before** it opens the redb
write transaction, through the one existing fetch path (`PeerBlobSource`, D-48 — no second path),
then writes `sm_spec_blobs`/`sm_dataset_blobs` and materialises spool files from the fetched bytes.
The fetch is a **pre-pass**, not a branch inside the write, for `resolve_blobs`' reason (D-49,
#444): a fetch is up to 17 round trips per blob and holding the write transaction across it would
block every other write. `origin` is `0` — a snapshot has no single accepting node, so the source
asks every joint voter. A digest no member can supply parks the install and retries forever (D-48)
rather than failing a committed catch-up; only a malformed digest, a non-UTF-8 blob, or (in a node
with no source attached) a non-empty manifest fails it — never a silent empty table, the snapshot
analogue of D-49's divergence guard.

The invariant this relies on is D-18: every live blob is on a quorum's node-local transport store,
established by the write-path fan-out (#438) — so a joiner always finds a holder. A blob referenced
by live state is never GC'd (`gc` respects the reference set), so the manifest can never name a
digest the fleet has reaped.

> **Amended by D-51** (2026-08-27): the precondition below is closed — a member serves any
> referenced blob from applied state, so a pre-fan-out row always has a holder.

**Precondition — the invariant holds for fan-out-minted blobs only.** Every write this build
produces goes through `fan_out_then_submit` (D-49), so its bytes are on a quorum's transport store
before the referencing op commits, and a follower that applies a digest-only op fetches into its
own transport store — so in any fleet formed by this build, every `sm_*_blobs` row has a transport
holder. The one shape without one is a **pre-fan-out** blob: an op that rode its bytes on the log
before #438 existed, applied straight into `sm_*_blobs` with no `store_whole`. The manifest names
such a row like any other, but no member can serve it, so its install would park (D-48) with no
holder able to appear. No such fleet exists — the blob transport (#437/#438) predates any release,
so there was no pre-fan-out log to replay. **D-51 (#486) closes this**: applied state serves the
row, so a pre-fan-out digest has a holder on every member that references it, and a manifest can
no longer name a blob nobody can supply. The separate rolling-upgrade concern — an un-upgraded
member wedging on a digest-only op it cannot decode — is **#481**, and is tracked there.

*Not changed:* D-23 stays `pending` — #441 (the RFC/architecture prose revision) flips it to
`active`. The `install_snapshot_timeout` / `snapshot_max_chunk_size` knobs stay (#428): a KiB-sized
install removes the pressure on the deadline, not the restart-from-offset-0 correctness argument.

### D-51 — A member serves a referenced blob from applied state when its transport store misses
- **Status:** active
- **Decided:** 2026-08-27 · #486 (#432/#440 follow-up); amended 2026-08-28 · #501
- **Refines:** D-18, D-48, D-50
- **Implemented by:** #486, #501
- **Code:** crates/rift-cluster/src/blobs/routes.rs, crates/rift-cluster/src/raft/store.rs, crates/rift-cluster/src/raft/node.rs

`GET /internal/v1/blob/{digest}` answers a chunk read from `sm_spec_blobs`/`sm_dataset_blobs`
when this node's blob transport store does not have the bytes. Applied state is therefore a
holder of last resort on every member, and D-18's "every member holds every live blob" means
**can serve** — true by construction of the state machine, not by the provenance of the bytes.

What this closes is the shape D-50's precondition named: a **pre-fan-out** blob, applied straight
into `sm_*_blobs` from an op that carried its bytes on the log, with no `store_whole` and so no
transport holder anywhere. A manifest names such a row like any other and its install would park
forever (D-48) with no holder able to appear. It also retires the out-of-band repair D-48
documents ("write the bytes back into any member's `<data-dir>/blobs/`") as the *only* path back:
any *peer* that still references the blob can serve it, even after its transport store is wiped.

**Amended (2026-08-28, #501) — a node now serves itself from applied state too.** As shipped this
entry was peers-only: `resolve_blobs` went straight to `BlobSource::load`, and `PeerBlobSource`
checks the local *transport* store and then filters this node out of the peer sweep, so the one
member certain to hold a referenced row — the node doing the applying — was the holder it could not
reach. That mattered because the blob tables are shared by digest and their own docs call
byte-sharing common: a tenant re-putting identical bytes under a second dataset name produces a
digest-only op whose bytes the applying node already has in `sm_dataset_blobs`, and it went to the
network for them anyway. Closed: `resolve_blobs` (apply) and `resolve_snapshot_blobs` (the D-50
install pre-pass) both consult `applied_blob_text` — the same two-table lookup the route fallback
uses — **before** the source, and skip it entirely on a hit. One redb read against up to 17 round
trips, and it is content-addressed, so a hit under `d` is by construction the bytes `validate`
proved for `d` (D-4/D-49): nothing to re-verify, no ordering question. A lookup that *fails* fails
the batch or the install; it is not read as a miss and does not fall through to the network, the
same fail-closed reading this entry gave the route. The install's `size` check applies to a
self-served row exactly as to a fetched one — a local row is not privileged evidence.

*The `blob_source == None` guard stays ahead of both reads.* A node applying digest-only ops with
no source attached still fails the batch or the install, even when it holds every digest they name.
That is deliberate, not the same gap one level down: no source attached is a construction-time
misconfiguration, and serving yourself from applied state is a fast path *in front of* the source,
not a substitute for having one. Pinned by
`a_digest_only_op_with_no_blob_source_is_an_error_even_when_this_node_holds_the_bytes`.

**A self-serve hit does not write the transport store.** No `store_whole`, for the `?stat` reason
below: the fallback serves reads, it does not claim to hold. Stated plainly, because it is a real
consequence rather than an oversight — after a self-serve hit the digest still has no transport
holder on this node, so D-52's rule B does not see requests for it and the next fan-out of the same
bytes will `put` it again (`?stat` answers `have: false`). Both are correct: D-18's "holds" means
*can serve* since this entry.

The fallback can only ever answer for the **referenced** set — `gc_spec_blob_if_unreferenced` and
its dataset twin drop the row in the same write transaction that drops the last reference — which
is exactly the set a manifest names. So it can never serve a reaped or stale blob, and it needs no
pin. The bytes are identical to what a transport holder would serve: the key is the sha256 hex
`validate` proved over exactly these bytes (D-4/D-49), and `BlobTransfer::get` re-verifies the
whole assembly against the digest it asked under, so a mixed-source read is safe by construction.

**`?stat` deliberately does not consult applied state.** `BlobTransfer::put` skips sending to any
peer whose stat reports `have`, and that peer's ack counts toward the fan-out quorum
`fan_out_then_submit` strips on (D-19/D-49). A stat that answered from applied state would let a
member ack a fan-out without ever receiving the bytes into its transport store — resting D-18's
quorum durability on a redb row a later delete can drop, which is a new hole in the invariant this
entry exists to close. `?stat` stays a pure transport-store probe: the fallback serves reads, it
does not claim to hold. The fetch path is unaffected either way — `PeerBlobSource` calls
`BlobTransfer::get`, which stats nothing and reads chunks until one comes back empty.

A fallback lookup that **fails** answers 500, never 404. A read this node could not perform is not
evidence the blob is absent, and a 404 would have the fetching peer cross this member off
(`FetchStep::NextPeer`) over a transient error and lose the reason; as a refusal it is carried on
the stall record instead.

*Rejected:* backfilling at apply (`store_whole` from the legacy carried-bytes arm, plus a one-time
sweep at open). It puts filesystem writes inside the redb write transaction — the thing D-49/#444
keep out of `apply` — duplicates every legacy row's bytes on every member, and still cannot help a
member that applied before the backfill build shipped. This is **not** what the #501 amendment
does, and "rejected backfill" should not be read as covering it: a self-serve *read* in
`resolve_blobs` writes nothing, sits outside the write transaction, and helps every member that
already holds the row — only the first of the three grounds above touches it at all, and it is the
one that argues *for* reading rather than duplicating. *Also rejected:* leaning on the object-store
mirror tier (#448/#456) for this. Its upload queue and its completeness sweep are both anchored on
the node-local `BlobStore`, so a blob that was never in one is never mirrored either — the same
provenance gap, one tier out — and D-30 makes the bucket opt-in, while this is a correctness gap in
the default build.

**Residual:** a blob that is *unreferenced* has no holder in applied state either, by design. A
replica parked on a `PUT` whose `DELETE` sits behind it in the log therefore still parks — that is
**#480**, and it is what `a_blob_no_member_holds_parks_apply_and_recovers_when_a_holder_returns`
now has to construct deliberately in order to pin D-48 at all.

### D-52 — Blob GC retains an unreferenced digest until this node's log is purged past it, and never while a peer is asking for it
> **Amended by D-55** (2026-08-28, #504): a third rule, C, retains a tombstoned digest until
> every member has applied past it. The residual below — a follower whose log is ahead of its
> applied index — is closed by it, and the "no channel" premise the rejection rested on is
> corrected in place.
- **Status:** active
- **Decided:** 2026-08-27 · #480 (#432 follow-up)
- **Refines:** D-18, D-48, D-50
- **Implemented by:** #480
- **Code:** crates/rift-cluster/src/blobs/mod.rs, crates/rift-cluster/src/raft/store.rs, crates/rift-cluster/src/raft/node.rs

Two local rules. No new gossip, no leader-driven GC, no fleet-minimum index to disseminate.

**A — tombstone plus this node's own purge point.** When apply drops a digest's last reference it
writes `sm_blob_tombstones[digest] = <the log index of the entry that unreferenced it>` in the same
write transaction; re-referencing the digest clears it. GC reaps a committed blob only if it is
unreferenced **and** unpinned **and** past the mtime grace **and** (carries no tombstone — a
never-referenced fan-out leftover, the pre-#480 rule — or its tombstone index is at or below this
node's `RaftMetrics.purged`).

Why a node's *own* purge point is the right index, when the replica at risk is elsewhere: the node
that decides whether a lagging follower is caught up by *entries* or by a *snapshot* is the leader,
using its own purge point. If the leader has not purged past `d` it still holds the blob under this
rule, and the follower's fetch — which asks every joint voter (D-19/D-48) — finds it. If the leader
has purged past `d`, the follower's next needed index is below the purge point, so it receives a
snapshot at `>= d`, whose manifest (D-50) omits a digest that was already unreferenced at `d`.
Leadership changes re-run the argument for whoever leads. A follower that purged earlier and reaped
is harmless, because the leader has not.

`purged` unknown (`None`, before the log has ever been compacted) reads as `0`, which protects every
tombstoned digest: `0` is never a genuine purge boundary, and a node that cannot say where its own
log begins must not be the one deciding a blob is expendable.

**B — a blob a peer is asking for is not garbage.** `BlobStore` keeps an in-memory
`last_requested`, bumped by `handle_get` (both the `?stat` and chunk branches); GC skips any digest
requested within the grace, tombstone or not. **This is not defence in depth — for an
already-parked replica it is the only recovery path**, and that is the finding recorded on D-48
below: a replica parked mid-apply can never install the snapshot rule A's argument hands it, because
the install queues behind the parked apply and never runs. Its fetch rounds hit every voter every
`<= FETCH_BACKOFF_MAX` (5 s), far inside the grace, so the holders keep the blob for exactly as long
as somebody still needs it. Precisely: rule B protects a holder that **answers**, not one that is
merely asked — a request lost to a timeout, a transport error or load shedding never reaches
`handle_get` and so refreshes nothing. Sustained shedding for a whole grace window would expose the
blob, which is the same "not asking" residual below seen from the holder's side. The map is pruned each sweep so a departed peer's one probe cannot pin
an entry forever.

**Snapshot install clears the tombstone table.** After an install this node's log position *is* the
snapshot boundary, so every pre-existing tombstone index is at or below it — already "purged past"
by rule A, i.e. carrying them forward would preserve rows the rule can never again act on. A
brand-new joiner has the property for a simpler reason: it fetches only the blobs the fresh
manifests name. The table is opened and emptied rather than skipped, so it is never silently absent
from an installed database (the `#[serde(default)]` lesson on `SnapshotPayload`). The cross-node
case this leaves — a node that just installed reaping a blob some *other*, already-parked replica
needs — is exactly what rule B covers.

`BLOB_GC_GRACE_SECS` is the **never-referenced** grace only: it is measured from the blob file's
mtime, so it was never a retention window for a blob that was live and then deleted. Before this
entry, such a blob was reaped on the next 60 s tick on every member — the window in #480 was 60
seconds, not an hour.

*Rejected:* a leader-published fleet-minimum applied index — it watches the wrong index: a parked
node's *matched* index keeps advancing while its applied index does not, so the leader's replication
view could never see the case. That half stands, and it is why D-55's rule C **pulls** each
member's applied index rather than reading the leader's replication view.

> **Amended by D-55** (2026-08-28, #504): closed by rule C — the channel is
> `POST /internal/v1/applied`, in-crate; what was missing was plumbing, not a channel or a layer.

**Correction (#504), twice over.** This rejection originally also claimed no channel existed for
disseminating the *applied* index. It then said the obstacle was layering — that blob GC lives in
`rift-cluster` while the fleet fan-out lives in `rift-cluster-server`. **Both are wrong.**
`raft::network::CLUSTER_APPLIED_PATH` (`/internal/v1/applied`) reports how far a node's state
machine has applied, and `RaftNode` already fans it out across members for the write barrier
(`node.rs`, issue #9) — inside `rift-cluster`, on the signed cluster port, in the same type that
owns `spawn_blob_gc_loop`. A fleet-minimum applied index is that existing call aggregated with
`min`, not a new mechanism. What it actually costs is a fan-out per sweep, a fail-closed rule when
a member does not answer, and the learner-vs-voter question D-53 also had to settle — all three
are settled by D-55. *Also rejected:*
leader-only GC (followers grow without bound); accepting the gap (the window is 60 s, not an hour);
and retaining the redb row instead of the transport blob (the fetch path reads the transport store,
and D-51's fallback serves *referenced* rows only — a blob in this state is by definition not one).

**Residual — a replica that is not *asking*.** Rule B protects a replica for exactly as long as it
keeps fetching; rule A protects it only while some holder's log has not passed the index. A replica
that stops asking for longer than the grace — partitioned, shut down, or restarting — and comes back
after every voter has purged past the unreferencing index finds no holder, and must be repaired out
of band.

> **Amended by D-55** (2026-08-28, #504): the shape below is closed. Rule C keeps the blob until
> every member's *applied* index has passed the tombstone, which is exactly the condition under
> which no member can still replay the `PUT` from its own log. What remains of this residual is
> narrower and named in D-55: a member that parked, was evicted, and rejoined with its retained
> state dir.

One shape of this was worth naming because it is not exotic: **a follower whose log is ahead of its
applied index**. openraft chooses snapshot-versus-entries on a follower's *matching/log* position,
not its applied index (`progress::entry`: "every candidate matching position is purged"), and rift
does not implement `save_committed`. So a node that parked (D-48) while replication kept filling its
log, then restarted, replays the `PUT` **from its own log** and is never handed the snapshot rule A's
argument above assumes — while the holders, whose `purged` has passed the tombstone, have already
reaped. Rule A's snapshot argument is therefore sound for a replica that was simply *down*
(`log == applied`, so its `matching` is below the purge point and it does receive a snapshot), and
not for one whose apply lagged its log. Such a replica never applies anything again and is
recoverable only out of band (D-48's repair), because it is never *offered* a snapshot — its
`matching` is above the purge point, so openraft replicates by logs. Closing it needed retention
keyed to a *fleet-minimum applied* index — which this entry had rejected on the false "no channel"
premise corrected above — and D-55 is that rule.

**Tombstones are reclaimed, not accumulated.** Each sweep drops rows at or below this node's
`purged`. That is information-free — a purge point only advances, so such a row can never again
satisfy `index > purged` — and it is what stops the table growing by a permanent row per delete on a
node that never installs a snapshot (which is the normal case for a long-lived leader). Node-local,
like the rest of this entry's state: the table is already excluded from the snapshot payload and
cleared on install, so its contents differ between members by design. A prune failure is logged and
the sweep continues; losing a row costs disk, while losing the sweep costs every blob it would have
freed.

**Observability:** `rift_cluster_blob_gc_retained` (gauge) — committed blobs this node actually held
back under rule A, counted by `gc` itself rather than re-derived, so the gauge cannot drift from the
rule it reports on.

### D-53 — The bytes leave the op only when every member is known to apply a digest-only one
- **Status:** active
- **Decided:** 2026-08-27 · #481 (#432 follow-up)
- **Refines:** D-19, D-23, D-49
- **Implemented by:** #481
- **Code:** crates/rift-cluster/src/raft/node.rs, crates/rift-cluster-server/src/admin_front.rs

`fan_out_then_submit` strips a `DatasetPut`/`SpecPut`'s payload only when every member of the
committed ∪ effective configuration is **known** to apply digest-only ops. Otherwise it submits the
op unchanged — the pre-D-49 shape every build can decode — counts
`rift_cluster_blob_sideload_deferred_total{reason}`, and warns naming the members responsible. A
mixed fleet degrades to bytes-on-the-log for the duration of a roll, and wedges nobody.

**What the prose rule it replaces got wrong.** `10-operations.md` told operators to upgrade the
fleet before the first sideloaded write, and described the failure as an old node that "fails closed
at apply". It does not. Log entries are decoded in `RedbLogStore::try_get_log_entries` with
`serde_json::from_slice(..).map_err(|e| StorageIOError::read_logs(&e))?`, and a `StorageError` out of
the **log store** is fatal to openraft's core: the node's Raft runtime stops. It is not a refused
apply, and if the un-upgraded members are a majority, one routine spec or dataset write costs the
fleet quorum mid-roll. That is the reason this is a mechanism rather than a sentence in a runbook.

**The capability is learned, not configured.** `BlobStat` gains `applies_digest_only`, set `true` by
this build's stat handler and `#[serde(default)]` so an old build's response — which omits the field
— decodes as `false`. The fan-out already stat-probes before sending, so the signal costs nothing.
`FanOutOutcome` carries the evidence (`sideload_safe`, plus which members were *incapable* and which
were merely *unobserved*), not a bare verdict, so the warning can name who.

**The probes run concurrently with the byte fan-out**, in a second `JoinSet` spawned before either
is awaited. Serially they cost the *sum* over learners, and since an unreachable member is never
remembered, that bill is paid on every write — enough to push a write past its caller's timeout on a
fleet with a blackholed learner. Concurrently the cost is the *max*, and in a healthy fleet it is
free: a stat round trip finishes long before a peer being sent megabytes. Learners are re-probed
every round rather than skipped once remembered, because skipping is exactly what would let a stale
`true` outlive the build it described.

**The probe set is every member; the byte quorum is still joint voters.** A learner applies the log
too, so it is asked the capability question — but bytes still go only to joint voters, and D-19 is
untouched. An **empty** membership is never safe to strip: `sideload_safe` is false on it rather
than vacuously true, which is what an `all()` over an empty set would have given.

**Observed capability is remembered** for as long as a member is in the membership, and pruned when
it leaves — but **a fresh `false` evicts a remembered `true`**. Without that eviction the set is
grow-only, and the memory becomes the very hazard it was meant to close: a node id is chosen by the
operator, so replacing a machine and rejoining under the *same* id from an older image is an
ordinary move, and the prune cannot catch it (the prune only runs inside a fan-out, and there may be
no write at all during the absence). The stale `true` would then authorise a strip, and the rejoined
member's Raft core would stop at log read — #481's own failure, reached through #481's mechanism.
An explicit `false` observed now is strictly better evidence than a `true` observed earlier, so
honouring it is never less safe; it also makes "a rolling downgrade in place is out of contract" a
statement of intent rather than a gap the code leans on.

**The cost, stated accurately.** Bytes ride the log while any member is not known capable. That is
*not* — as this issue's triage had it — "one write's worth, then it is remembered": a member this
leader has **never** observed, because it was down when the leader's first fan-out ran, is never
added to the set, so every write carries its bytes for as long as it stays away. Worse, since
membership changes only via join or leave (D-21), a member that is down *permanently* keeps the
fleet on bytes-on-the-log indefinitely — silently undoing the epic this sits in. The counter and the
member-naming warning are therefore part of this decision, not observability garnish: they are what
makes that state visible, and the operator closes it by removing the dead member. A leader failover
empties the set, so the first write after one re-probes; that degrades, it never wedges.

*Rejected:* capability in membership metadata — needs a custom openraft `Node` type and a join-time
stamp that an in-place restart onto an older binary silently invalidates. *Rejected:* gating on
*acked* members only — the member that will replay the entry is exactly the one that was down.
*Rejected:* a replicated "digest-only enabled" ratchet — a one-way door, and a persisted
state-machine field for a transient upgrade window, whose own op has the same decode problem unless
smuggled onto an existing variant.

**Replays are covered by construction:** `compose::drain_parked_intents` runs a parked write through
this same function, so it cannot bypass the gate.

### D-54 — Gateway addressing is the path prefix; the header and subdomain schemes are withdrawn in favour of front-door routes

- **Status:** active
- **Decided:** 2026-08-28 · #491
- **Amends:** RFC-001 §6.3
- **Implemented by:** #491
- **Code:** vendor/rift/crates/rift-http-proxy/src/gateway.rs, vendor/rift/crates/rift-http-proxy/src/front_door/route_table.rs, web/src/screens/Routes.tsx

RFC-001 §6.3 listed three gateway addressing schemes and recommended two of them. Only the third
was ever built: upstream's `gateway.rs` parses `/__rift/:port/<path>`, and the front door uses that
same form as its no-route fallback. The header (`X-Rift-Port`) and subdomain (`p-8080.…`) schemes
are **withdrawn**, not deferred.

**What §6.3 wanted, and what delivers it now.** Its stated reason for preferring header or subdomain
was transparency — *"nothing to strip, so path/host predicates, `savedRequests`, and proxy
`recorded_from` all see the true downstream request."* The front door (#19/#130, U-11, chapter 13)
delivers exactly that by a different mechanism: a content-based route table on the same listener.
`RouteMatch.host` takes an exact host or one leading wildcard label, so `p-8080.mocks.example.com →
:8080` is a host route; `RouteMatch.headers` is a list of exact `(name, value)` matches, so
`X-Rift-Port: 8080 → :8080` is a header route; and `RouteTarget.strip_prefix` defaults to **false**,
whose own doc reads "predicates and recorded requests see the true path unless the route asks
otherwise". Transparency-by-default is already the route table's rule.

**Why withdrawn rather than "not yet".** Both rows are expressible *today*, per imposter, as
operator-authored routes — with two properties a hard-wired scheme could not have had: they are
tenant-scoped (routes belong to tenants and are compiled in per `routes_installed_for`, chapter 8)
and they are replicated control-plane state (`ControlOp::PutRoutes`, R1/R3). All a built-in scheme
would add over a route is the *implicit* any-port mapping — no route per imposter — and that
implicit form is precisely what the path prefix already provides as the no-route fallback
(`gateway::dispatch_gateway_path`). A second and third implicit scheme would be three spellings of
one thing, and each one is another path the tenancy rule has to account for beside the single
fallback it has now.

D-11 is untouched and is not a dependency in either direction: the plain listener *and* the route
table are both upstream already, so nothing has to move first. (§6.3 says only that the plain
listener is upstream, which remains true; "D-11 would have to move first" was #491's own inference.)

*Rejected:* keeping them as a deferred preference. Because the code is equally consistent with "not
yet" and "never", a preference nobody is building is a claim the docs cannot keep true — #467, #489
and this issue are three corrections of the same one sentence, in three different places.

### D-55 — Blob GC retains a tombstoned digest until every member has applied past it
- **Status:** active
- **Decided:** 2026-08-28 · #504 (D-52 residual)
- **Refines:** D-52; also D-18, D-48, D-50, D-53
- **Implemented by:** #504
- **Code:** crates/rift-cluster/src/blobs/mod.rs, crates/rift-cluster/src/raft/network.rs, crates/rift-cluster/src/raft/node.rs, crates/rift-cluster/src/raft/store.rs

D-52 keyed retention on this node's own purge point (rule A) and on a peer actively asking (rule B),
and recorded as a residual the one replica neither rule sees: **a follower whose log is ahead of its
applied index**. That is the steady state of any node that parked once (D-48) and restarted —
replication kept filling its log while apply was stuck — and openraft chooses snapshot-versus-entries
on the log position, so such a node is never offered a snapshot: it replays the `PUT` from the log it
already holds, finds every holder has reaped, and wedges permanently, recoverable only out of band.
Every step of that sequence is documented behaviour and the trigger is ordinary, so it is not an
acceptable residual. A third rule closes it.

**C — the fleet applied floor.** A committed blob that is unreferenced, unpinned, past the mtime
grace and carries a tombstone at index `t` is reaped only if **all** of:

- **A** (D-52, unchanged): `t <= this node's purged`. Covers the *snapshot* side — any snapshot a
  holder that reaped could send has a manifest built at `>= t`, which omits the digest (D-50).
- **C** (new): `t <= fleet_min_applied`, the minimum `last_applied` over **every** node in the
  committed ∪ effective membership, voters *and* learners (a learner applies the log too; the
  widening D-53 made). Covers the *log* side — a member with `last_applied >= t` has applied the
  unreferencing entry and will never replay the `PUT` below it from its own log.
- **B** (D-52, unchanged): not requested within the grace.

Why A ∧ C and not C alone: C says nothing about a *future* member. A joiner takes a snapshot from
the leader, and A on the leader is what guarantees that snapshot's manifest omits the digest. Why A
alone is not enough is the residual above.

**Pulled, not leader-published.** openraft carries no applied index on the wire —
`AppendEntriesResponse` reports `matching`, which keeps advancing on a parked node while
`last_applied` does not — so the leader's replication view can never see this case (the half of
D-52's rejection that stands). The floor is read by each node's GC sweep over the channel the write
barrier already uses: `POST /internal/v1/applied` (`raft::network::CLUSTER_APPLIED_PATH`, issue
#9), in `rift-cluster`, on the signed cluster port. This node answers from its own metrics; each
peer is asked concurrently, any of its resolved addresses answering being the peer answering (D-28),
under a 2 s per-member budget. No new endpoint, no new credential, no `rift-cluster-server`
dependency — what was missing was plumbing, not a channel or a layer.

**Fail closed, and loud.** Any member unreachable, timing out, undecodable or answering
`applied: None` makes the floor unknown, which reads as `0` — the convention `purged == 0` already
carries (real indices start at 1) — and every tombstoned blob is retained that sweep. The sweep then
`warn!`s once, naming the members it could not read, so the retention is attributable rather than
silent (the D-53 shape: carry the evidence, name who). The trade accepted is the one #481/D-53 made
explicit: **one unreachable member pins every tombstoned blob until it answers or leaves the
membership.** What that costs, stated honestly: it is disk, and it is **not** bounded by the
dataset quota (D-18) — the quota bounds the *live* corpus, and a blob held under C is by definition
no longer in it. While a member is unreachable, every delete or overwrite adds one more retained
blob, so the exposure is delete churn × outage duration; and because the prune bound is
`min(purged, floor)` with the floor unknown, **tombstone rows stop being reclaimed for the same
window** (they are the evidence C still needs), so `sm_blob_tombstones` and the 60 s scan over it
grow with the churn too. Under D-52 alone both were transient, bounded by the purge cadence; under
this entry they last exactly as long as the member is neither answering nor evicted. Both lift on
their own the moment it is — when the member returns and applies the deletes it missed, or when it
is evicted (D-21/D-26), which removes it from the set rule C consults. The bound is therefore the
operator's tolerance for a down member, which is the bound the membership already has: a member
that will never return has to be evicted anyway, and the once-a-minute warning names it.

**Tombstones are pruned on the lower of the two indices**, `min(purged, fleet_min_applied)`, never
on the purge point alone. A row this log has passed but some member has not yet applied past still
protects its blob under C; pruning it would turn that blob into a never-referenced leftover, reaped
by the plain grace rule on the next sweep with nothing left to say otherwise. With `0` as unknown,
`min` is itself fail-closed.

*Rejected:* bounding the exposure with a wall-clock floor (never reap a tombstoned blob younger than
some multiple of the snapshot cadence) — a guess the sequence outruns, since the wedge needs only a
restart at the wrong time; making a parked apply preemptible so the node could take a snapshot past
the missing entry — the node is never *offered* one here (its `matching` is above the purge point),
so D-48 would be reopened for nothing; accepting the residual — a silent-until-noticed wedge with an
out-of-band-only repair, on an ordinary trigger. The object-store mirror tier (#456/#448) mitigates
but does not close it: opt-in (D-30), with its own wall-clock GC.

**Remaining residual — depart-then-rejoin.** A member that parked, then *left* (evict, D-21) and
rejoined under D-26 with its retained state dir is not in the membership during the window and so is
not consulted; it replays its own log after rejoining and parks. Repair is D-48's out-of-band
write-back, or wiping the state dir before rejoin so it takes a snapshot instead. Far narrower than
the sequence above — it needs a parked node to be gracefully evicted — and `blob_fetch_stall`
surfaces it.

### D-56 — Shutdown ends a parked blob fetch; the entry re-applies on restart
- **Status:** active
- **Decided:** 2026-08-28 · #513 (found while implementing D-55/#504)
- **Refines:** D-48; also D-16, D-23
- **Implemented by:** #513
- **Code:** crates/rift-cluster/src/raft/blob_source.rs, crates/rift-cluster/src/raft/node.rs, crates/rift-cluster/src/blobs/mod.rs, crates/rift-cluster/src/raft/store.rs

D-48 says a blob no member can supply parks the apply and the node **never gives up**. That is
right while the node is running, and it is what makes the park recoverable at all (D-52 rule B is
fed by exactly those repeated requests). But it also made a parked node impossible to stop.

**The mechanism, which is D-48's own amendment read from the other side.** openraft's
state-machine worker awaits `Apply` inline (`openraft-0.9.25/src/core/sm/worker.rs`), and that
worker owns the `RedbStateMachine` clone. `Raft::shutdown` joins only the RaftCore task and the
tick handle — never the sm worker — so a worker parked inside `resolve_blobs` keeps the redb
handle for as long as the fetch retries, which under D-48 is forever. `RaftNode::shutdown`'s
storage-release wait (#41) therefore timed out and returned `Err`, with the file lock still held
by an orphaned task in the same process; a `RaftNode::start` on that directory then failed with
`redb: Database already open`, several steps from the cause.

**The rule.** A parked fetch ends on one of two events: a holder returns (D-48, unchanged), or
**this node is shutting down**. `RaftNode` owns a `tokio::sync::watch<bool>`; `shutdown()` sends
it *before* asking the Raft core to stop, and `Drop` sends it too. `PeerBlobSource` races it
against **both** halves of the loop — the fetch round and the backoff sleep — so the signal is
observed at the next poll rather than at the end of whichever is in flight. The fetch returns
`BlobError::ShuttingDown`, `apply` fails, the worker exits, the handle drops, and shutdown
completes normally.

Racing the *round*, not only the sleep, is what makes the guarantee real rather than typical. One
round walks every member and, within each, every resolved address; a single unreachable peer costs
`replication_deadline` — the 2 s request timeout plus a 4 MiB allowance, about 6 s — so a round
against a black-holed fleet can run for tens of seconds, against a `STORAGE_RELEASE_TIMEOUT` of
2 s. A version that waited for the round to drain would therefore still have failed shutdown in
exactly the partition-shaped case D-48 exists for, while appearing to work in every test whose
peers answer promptly. Cancelling mid-round is safe: a round only reads from peers, and committing
fetched bytes to the store is the caller's step.

**Why failing the apply is safe here, and only here.** `RedbStateMachine::apply` resolves every
digest-only op's bytes **before** it opens its write transaction, so a parked apply has written
nothing: `last_applied` on disk is still the entry before it. Failing it during shutdown produces
exactly the state a `kill -9` produces, reached cooperatively — the entry replays on restart. This
is *not* a licence to fail an apply on a timer, which is what D-48 rejected and what would take a
healthy node down during a long partition: the trigger is the node stopping, not the fetch being
slow.

**`ShuttingDown` is its own error, never `NotFound`.** Absence is a domain answer about the
*fleet*; a shutdown is a fact about this process. Collapsing them would let a caller conclude
"no member holds this blob" from "this node is stopping". The `BlobSource::load` contract is
amended to say so.

*Rejected:* aborting the sm worker task — openraft owns it and exposes no handle; a fetch timeout
or a maximum round count — reopens exactly what D-48 refused, and is the wall-clock guess D-55
also declined; draining the park by installing a snapshot — cannot run, since the install queues
behind the parked apply (D-52's amendment to D-48).

**The snapshot-install path too.** `resolve_snapshot_blobs` calls the same `BlobSource::load`, so
a node parked while installing a snapshot is covered by the same signal. Its safety argument is
the sibling of the apply one and was checked separately: the install's write transaction has not
opened when the fetch runs, so a failed install leaves the previous state intact and the node
takes the snapshot again from the leader on restart.

**Residual.** A node whose apply is parked on something *other* than a blob fetch is not covered
by this entry; no such path exists today. The `Drop` send makes the guarantee hold for a plain
drop as well as for `shutdown()`, so the pre-existing
`drop_without_shutdown_eventually_releases_storage` property is now unconditional rather than
true-unless-parked.
