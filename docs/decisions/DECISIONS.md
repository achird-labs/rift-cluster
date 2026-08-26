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
- **Implemented by:** #465 (the isolated-owner rule, for flow KV)
- **Code:** crates/rift-cluster/src/stores/flow.rs, crates/rift-cluster/src/raft/ring.rs

**Paid, honestly (2026-08-25, #465).** Enforcing the isolated-owner rule puts one
consensus-shaped pause on the otherwise consensus-free flow path: `is_isolated()` is `true`
whenever a node's `current_leader` is unknown, so a leader election makes every node that has lost
sight of the leader refuse owner-side flow writes and `strong` reads until a leader is
re-established — sub-second typically, up to the ~1–3 s D-15 accepts for admin writes when
contended. This is stricter than this entry's own "3 × election_timeout" wording, which would ride
out a routine election; the primitive fails closed immediately and that is what ships. Whether the
flow path should take the looser grace is #472. `local` reads (D-10) are unaffected.

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
- **Status:** pending
- **Decided:** 2026-08-24 · #432 (epic), RCA "Bytes on the Log"
- **Amends:** RFC-005 §3.2, RFC-004 §4.1
- **Implemented by:** #436, #437, #438, #439, #440 (open), #441 (open)
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
reversal; #441 revises the prose.

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

### D-49 — Payload fields stay optional on the wire; the bytes leave the op at submit, after the quorum
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
