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

**Callout verbs.** A spec section a decision changes carries a callout at its head, and the verb
is one of exactly four. `design-check` reads these and nothing else, so a callout that uses
another word is invisible to it:

| Callout | Means |
|---|---|
| `> **Amended by D-n**` | the section still holds, with the stated change |
| `> **Superseded by D-n**` | the section is replaced by something else, named |
| `> **Reversed by D-n**` | the section's conclusion was wrong and the opposite is now true |
| `> **Retired by D-n**` | the section's subject was removed and has no successor; the text stays as history |

`Retired by` was added on 2026-09-09 (#554). RFC-007's children had already written it about
twenty times — it is what a removal does to a section, and neither "amended" nor "superseded" says
that — but it was not in the grammar and the checker did not accept it. Those callouts passed only
because no `Amends:` field named their sections, so nothing looked. Naming the relation the
project actually has is the smaller change; rewriting twenty true callouts into "superseded",
which promises a successor that does not exist, would have been the larger and the falser one.

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
   `> **Amended by D-n** (date): <one line>` — or the `Superseded`/`Reversed`/`Retired` form, per
   the callout table above — at the top of that section. `design-check` fails without it.
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
- **Code:** crates/rift-cluster/src/raft/store.rs

Gossiping full configs blows the SWIM payload budget and re-floods every round; digests converge
fast and bodies transfer once per node.

**Amendment (D-15, 2026-07-21):** with gossip gone, *config* bodies ride the Raft log as small
JSON entries. The content-addressed principle survives for *blobs* (datasets, specs) — see D-18,
D-23.

**Amendment (D-72, 2026-09-07, #549):** the blob half of that is gone with the blob store, so
nothing content-addressed remains. D-18 and D-23 are superseded; every config body — imposter,
stubs, route table — rides the Raft log as a small JSON entry, and there is no second transport.
The entry stays `amended` rather than `superseded` because its surviving claim is the one that
built the system: config travels as a body on the log, not as a gossip digest.

### D-5 — Two-level, order-aware reconcile (LCS edit script) on top of by-id/positional stub CRUD
- **Status:** amended
- **Decided:** 2026-07-01 · RFC-001 v2
- **Implemented by:** #565 (the delete-path amendment)
- **Code:** crates/rift-cluster/src/control.rs, vendor/rift/crates/rift-mock-core/src/imposter/reconcile.rs, crates/rift-cluster/src/raft/store.rs, crates/rift-cluster/src/stores/flow.rs

Whole-imposter replace per change resets runtime state cluster-wide; set-diff (v2 draft 1) missed
reorders and reordered keyless edits — order is match priority, so the edit script must be
order-aware.

**Amendment (2026-08-25, verification pass):** the mechanism is not an LCS. Level 1 is the
explicit `StubEditScript` (`Add`/`ReplaceById`/`DeleteById`/`Move`), applied all-or-nothing by
`apply_edit` and the `PatchStubs` arm. Level 2 is upstream's `reconcile_stub_states` (U-6): stubs
are matched by `stub_key` (explicit id, else an occurrence-counted content hash), surviving keys
keep their slot state, a pure reorder costs nothing, and a change touching more than half the
stubs falls back to a wholesale replace. Order-awareness holds; "LCS" does not.

**Amendment (2026-09-08, #565 — the delete path):** "a replicated write never resets an untouched
imposter's runtime state" has an inverse that was never stated, and the fleet had it wrong: **an
imposter's state is the imposter's, and a deleted imposter has none.** A committed
`DeleteImposter` or `DeleteAll` drops the deleted port's imposter-scoped flow state (`i<port>:`,
D-20's ring key) on **every node**, from the apply loop — each node clears its own shard when its
engine reports the port removed, so the clear is deterministic, once per node per committed
delete, and covers a delete replayed on join or installed by a snapshot; `reconcile_engine`
additionally drops any `i<port>:` namespace the tables no longer name, which is what covers a
delete committed while the node was down. Single-node Rift gets this for free by dropping the
imposter's store instance; one shared `FlowNet` per node (D-7) has to do it explicitly. What is
*not* cleared, by the same rule: a `PutImposter` over an existing port (a config change keeps its
state — the first paragraph of this entry), the `fleet`-scoped (`f:`) context (shared by
construction, not any one imposter's to drop), and any other port's namespace (ports are
fleet-unique, so `i<port>:` names exactly one imposter). Sequencer cursors already go with the
imposter via upstream's `reset_scope` hook (D-8, D-57); proxyOnce markers via the apply arm (#226).

**Amendment (2026-09-09, the #567 and #573 reviews — what "deleted" means to the clear):** the rule
above was applied to a wider set than "a deleted imposter" in two places, and — once narrowed — to
a set that was then too narrow in a third.

*The live clear is filtered against the desired set.* Upstream's `replace_imposter` tears the old
imposter down and re-creates it; when the re-create is refused at staging, the port is reported
`deleted` *and* `failed`. The engine is truthfully serving nothing there — but the config set still
names the port, so this is a failed **edit** on one node, not a removal: the next successful sync
re-creates the imposter, and nothing would have put its state back. A port the fleet still wants
keeps its state; only a port the applied set omits is cleared.

*…and unioned with the ports carrying a recorded apply failure.* That filter alone leaks. The
refused re-create has already taken the port out of the engine's map, so when the operator gives up
and deletes it, upstream computes the removal set (`map ∖ desired`) from a map that no longer names
the port: nothing is reported deleted, the state kept by the paragraph above survives for the life
of the process, and an identically re-created imposter meets yesterday's scenario — the #565 bug,
reached through a failed edit. The per-port apply-failure map is already reaped on exactly this
ground ("a bind-failed port that is later deleted keeps its stale entry forever"); the flow state
leaves with it. Both halves of the union stay gated by the desired set, so a port that is merely
failing keeps everything.

*The reconcile sweep measures the tables after the sync, not the snapshot the sync was driven from,
and reads its two sets in that order: the namespaces it holds first, the desired ports second.* The
original claim — that no imposter can be created in the window because the node is not yet `Ready`
— was false: the ring is Raft membership, so a restarted voter is an HRW owner the whole time it
catches up, and `compose` binds the flow net long before it spawns the reconciler. The read that
fed the engine sync and the sweep were a whole `apply_config` apart (seconds, on a cold start with
listeners to bind) while the apply loop ran concurrently, so an imposter committed in between was
alive on every node with its flow namespace swept on this one. Re-reading `sm_configs` after the
sync closes most of that; reading it *after* the held set closes the rest. Neither read is
instantaneous and no barrier separates them, so one of the two is necessarily the older
observation — and it must be the accusation, never the acquittal. Apply commits `sm_configs`
before it drives the engine, so a `PutImposter{P}` landing between the reads is durably applied and
served fleet-wide: a desired set read *first* would convict it on a held set read second. The
comparison is against the **tables, not the engine's imposter list** — the two differ exactly on
ports the engine failed to stage, which by the paragraphs above keep their state. The residual,
stated: a flow that lands for an imposter this node has not yet applied, before the desired-set
read, is dropped. That is all of it — an imposter this node has applied by that read is in the set
and is kept, whenever its flow arrived — and what bounds it is that `compose` reconciles only once
`last_applied` has reached the leader's applied index: one apply round-trip, not seconds.

**Amendment (2026-09-09, #574 — two handles drive one engine, and nothing separated them):** the
paragraphs above are all about *what a set means*. This one is about **who may drive one at the same
time**, which nothing above constrained and — the part that was missed — nothing upstream constrained
either. Two handles drive one engine: the apply loop has openraft's clone of the state machine,
`compose`'s reconciler has the reader clone, and `apply_config` (U-6) deletes every live imposter the
set it is given omits.

**Upstream takes no lock across `apply_config` at all** — not manager-wide, not around the delete and
create passes (`manager.rs`, `apply_config`). It says so itself, in the bind path: *"two `apply_config`
calls can run against the same engine concurrently — the raft apply loop and the startup reconcile
poll drive it through separate state-machine handles with no lock between them."* So the two calls
were not merely computing their arguments at unsynchronised times; their **bodies interleaved**, the
reconcile's delete pass running against the apply's create pass, with only `try_claim`'s
check-then-store and a `PortInUse` loser to arbitrate a collision.

The concrete loss: a reconcile that read its desired set before an apply committed port P, and drove
the engine after that apply had driven it, applied a set that never named P. P is torn down, reported
`deleted`, and — by the 2026-09-08 amendment above — its flow state is cleared. Fleet-wide P is alive
and served; on this node it is gone, and the imposter returns only on the next committed config op
(each carries the whole set), while the flow state does not return at all.

Every engine drive therefore takes an engine-drive lock, and the reconcile holds it **across its
desired-set read and the drive that set feeds**, not merely around the drive. That is strictly more
than serialising the two calls: apply builds its set from a mid-write-transaction view, so it is
always at least as fresh as any concurrent reader's, applies are strictly sequential on openraft's
state-machine worker, and the reconcile's read is inside the same critical section as its drive —
so the drive that lands *last* always carries the *freshest* set, with no residual ordering hole.
It releases that lock **before the orphan sweep**: the
sweep's correctness is the read *order* established above, not exclusion, and the apply loop must
be free to make progress across it — which is what the sweep's own seam exists to exercise. The
residual stated in the previous amendment is unchanged; this closes a different window, in the sync
half rather than the sweep.

*Rejected:* a generation counter re-checked before acting on `report.deleted`. It can only detect
the teardown after `apply_config` has performed it, so the imposter is still momentarily gone and
the node still answers 404 for it until something re-drives — detection where exclusion was
available. *Also rejected:* routing the reconcile through the log, which would need a leader and a
replicated entry for a node-local operation.

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
- **Decided:** 2026-07-01 · RFC-001 v2
- **Superseded by:** D-47
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
- **Status:** amended
- **Decided:** 2026-07-21 · ADR-001 · #14
- **Supersedes:** D-1, D-2
- **Amends:** RFC-001 §7.1, RFC-001 §7.2, RFC-001 §7.4
- **Code:** crates/rift-cluster/src/raft/node.rs, crates/rift-cluster/src/raft/store.rs

Membership + imposter configs + the `enabled` bit + tenancy/RBAC records + admin intents in one
Raft log; **flow state stays off consensus** (D-17). Putting membership itself into the log is the
move that pays for everything else: the roster becomes a linearizable value, so at any log index
every node computes byte-identical membership and therefore byte-identical ownership. The settle
delay, the generations, the epoch-mismatch retries are deleted, not mitigated.

**Amendment (D-73, 2026-09-08, #550/#566):** tenancy and RBAC records are no longer among the
log's contents — D-73 removed both, and no `ControlOp` carries either. What the log agrees today
is membership, imposter configuration (the `enabled` bit with it), the route table and admin
intents. The route table is named here for completeness only: it is not part of what D-15 decided
and arrived later, as `ControlOp::PutRoutes` (D-54, D-68). The decision itself is untouched —
membership is agreed rather than gossiped, and everything that must be byte-identical at a log
index rides the same log.

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

### ~~D-18 — Every member holds every live blob~~
- **Status:** superseded
- **Decided:** 2026-08-24 · ADR-001 · #432, rift-cluster#458
- **Superseded by:** D-72
- **Amends:** RFC-005 §3.2
- **Implemented by:** #437, #438 (merged), #439 (open), #440 (open), #441 (open — the RFC/chapter revision)

Superseded by D-72 (#549): the blob store is gone. It existed to carry stored specs and datasets,
and with both removed there is no content-addressed tier for a completeness rule to hold over.
Retained for history.

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

### ~~D-19 — The blob fan-out quorum is joint consensus~~
- **Status:** superseded
- **Decided:** 2026-08-24 · ADR-001 · #438
- **Superseded by:** D-72
- **Implemented by:** #438

Superseded by D-72 (#549): there is no blob fan-out. `QuorumTargets`, `joint_voters` and
`joint_members` went with it — they had no other caller. The joint-consensus *reasoning* is not
wrong and is worth re-reading if the fleet ever grows another out-of-band transfer; it is the
mechanism, not the argument, that this entry loses. Retained for history.

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

> **Refined by D-59**: the promotion sweep is *joining* machinery, and a leaving node never
> presents as a joiner — a departing voter is never a learner in committed membership.

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

### ~~D-23 — The bytes leave the log: blobs are sideloaded, ops carry digests~~
- **Status:** superseded
- **Decided:** 2026-08-24 · #432 (epic), RCA "Bytes on the Log"
- **Superseded by:** D-72
- **Amends:** RFC-005 §3.2, RFC-004 §4.1
- **Implemented by:** #436, #437, #438, #439, #440; prose revision #441

Superseded by D-72 (#549) — by removing the payloads rather than by putting them back. Only
`SpecPut` and `DatasetPut` ever carried a blob, and both ops are gone; every remaining op is
metadata-sized JSON, so the sideload path, the digest-only shape and `/internal/v1/blob/{digest}`
have nothing left to carry. **The measurement behind this entry stands and is the reason it must
not be undone casually:** 4–64 MiB entries broke openraft 0.9's heartbeat, snapshot and admission
assumptions (#411, #430, #431, #433), and the single-large-entry ceiling that finding established
(RFC-001 §7.3) still applies to anything that would put bulk on the log again. Retained for history.

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

### ~~D-29 — Deleting a source orphans its imposters; never cascades~~
- **Status:** superseded
- **Decided:** 2026-08 · #253
- **Superseded by:** D-72

Superseded by D-72 (#549): there are no source records to delete. The half of this entry that
outlived it is the instinct behind it — a bookkeeping change must not delete live mocks — which
is why the one-shot import replaced tracking rather than being bolted onto it. Retained for
history.

### ~~D-30 — Object-store offload is untrusted, opt-in, and never on the serving path~~
- **Status:** superseded
- **Decided:** 2026-08-24 · #448 (tracking), #456, #457
- **Superseded by:** D-72
- **Implemented by:** #456, #457 — both closed as out of scope on 2026-09-06

Superseded by D-72 (#549) before it was ever built: it refined D-18's blob-store completeness
rule, and there is no blob store. The `--as-new-fleet` reasoning it records is about snapshot
restore, not about blobs, and would need re-deciding on its own terms if that work returns.
Retained for history.

Refines D-18. Bytes read back from a bucket are untrusted until their digest is verified — an
acceptance criterion, not an optimization. Cache (mirror) and backup want opposite retention
policies and are separate tiers. Restoring a fleet from a snapshot backup requires
`--as-new-fleet`: it rewrites membership to a single voter and bumps the term, so a restored
node can never be mistaken for a member of the fleet it was copied from.

### ~~D-31 — `PollStatus` is node-local; `SourceRecord` is fleet-replicated~~
- **Status:** superseded
- **Decided:** 2026-08 · #233, #239
- **Superseded by:** D-72

Superseded by D-72 (#549): neither shape exists — there is no poll and no source record. **The
rule it states is general and still binds every surface this fleet publishes:** a node-local
observation must never be flattened into a shape a reader will take for fleet state. It is the
past-state-as-present error, and the `nodeLocal` split it introduced is the pattern the remaining
fleet reads (`/_fleet/members`, `/_cluster/health`) still follow. Retained for history, and worth
reading before adding any new fan-out.

What a source *is* (URL, credentials ref, interval) is a Raft value; when *this node* last polled
it and what it saw is not. A response that flattens the two into one shape would present a
node-local observation as fleet state — the past-state-as-present error. They stay separate
fields with separate provenance.

### ~~D-32 — Fleet request tail over a capped, declared coverage set~~
- **Status:** superseded
- **Decided:** 2026-08 · #362
- **Superseded by:** D-74

Superseded by D-74 (#552): the fleet-wide tail is gone with the merge it streamed. There is no
coverage set to declare because no read speaks for more than one node; a client that wants the
fleet's tail opens upstream's own `savedRequests/stream` on each node. Retained for history.

The fleet-wide request tail carries a `port → JournalCursor` map over a coverage set capped by
`fleet_journal_port_cap` (default 100) and reports `coverage: {covered, total, omitted}` on every
response, so a partial view is never mistaken for the whole. A `(timestamp, tiebreak)` watermark
was rejected — clocks are not ordered across nodes — and a fourth, hybrid shape was
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

### ~~D-34 — `git+` sources are a detected capability in the `-static` image~~
- **Status:** superseded
- **Decided:** 2026-08 · #270
- **Superseded by:** D-72

Superseded by D-72 (#549): there is no `git+` provider on either flavor, so the two images no
longer differ in what they can fetch and the runtime stage no longer installs git. The rule that
survives — **fail loudly at declaration, not at first use** — is why `--imposters git+…` is
refused at startup by name rather than left to error on a fetch nobody watches. Retained for
history.

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

### ~~D-37 — The journal is per-writer shards, merged on read~~
- **Status:** superseded
- **Decided:** 2026-08 · #223, #224 (RFC-001 §7.5.1 as built)
- **Superseded by:** D-74

Superseded by D-74 (#552): the shards, the k-way merge and the anti-entropy pull were removed
in full. The journal is upstream Rift's own, per node, and a read answers for the node it reached.
Retained for history.

Every node appends only to its own `(port, node_id)` shard; a read k-way-merges the shards by
recorded timestamp with `(node_id, seq)` breaking ties. Caps are writer-local with an
`evicted_below_seq` watermark; an unreachable peer yields `Rift-Cluster-Partial`, never a stall.

*Rejected:* owner-routed or consensus-carried journaling — a mock request must never wait on
another node to be recorded.

### ~~D-38 — Clears are generation bumps, never timestamps~~
- **Status:** superseded
- **Decided:** 2026-08 · #223 (RFC-001 §7.5.2 as built)
- **Superseded by:** D-74
- **Amends:** RFC-001 §7.5.2

Superseded by D-74 (#552): `ControlOp::JournalClearGen` and the `sm_journal_gens` table are
gone. A clear generation is observable only through a reader that consults it, and the only
reader was the merge; `DELETE savedRequests` is now a proxied clear of the reached node's own
journal. Retained for history.

A monotone per-port (and per-`(port, space)`) clear generation rides the Raft log as
`ControlOp::JournalClearGen`; entries and counter slots carry their writer's generation, and the
merge drops anything from an older one. RFC-001 §7.5.2's "gossip carries clear generations" and
its `teardown_space` markers were never built — the generation is a committed value.

*Rejected:* timestamped deletion (clocks are not ordered across nodes); `retain` predicates stay
best-effort per shard.

### ~~D-39 — The journal cursor is a vector, opaque by contract~~
- **Status:** superseded
- **Decided:** 2026-08 · #225, #348 (RFC-001 §7.5.1 as built)
- **Superseded by:** D-74
- **Amends:** RFC-001 §7.5.1

Superseded by D-74 (#552): with one writer per journal there is no position across writers to
name. `?since=` is upstream's own scalar cursor again, with upstream's `x-rift-next-index` and
`x-rift-truncated`; the `JournalCursor`/`FleetCursor` codecs went with the merge. Retained for
history.

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
- **Status:** amended
- **Decided:** 2026-07 · #94
- **Code:** tests/cluster-chaos/tests/scenarios.rs, crates/rift-cluster/src/raft/node.rs

C6's injected jitter overlaps the 150–300 ms election timeout by design, so occasional elections
are in spec; the scenario bounds leadership transitions by `C6_MAX_LEADER_TRANSITIONS` (derived
from the ~5 s gauge resolution), never by a fixed count.

**Amendment (D-71, 2026-09-07, #548):** the `rift_cluster_members` gauge that supplied the samples
is retired with the operator observability pack. The harness now samples **which node claims
leadership** — `is_leader` on `GET /_fleet/members`, read through `claims_leadership` — at
`C6_LEADER_SAMPLE_INTERVAL`, deliberately the same ~5 s cadence the gauge was resampled at, so the
derivation of `C6_MAX_LEADER_TRANSITIONS` is unchanged. The bound is a rate over that sampling
window, exactly as before; only the sample's source moved.

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

### ~~D-44 — The first principal closes the open admin plane~~
- **Status:** superseded
- **Decided:** T2 (#161) · RFC-002 §3.4
- **Superseded by:** D-73

A fleet with neither `--api-key` nor any stored principal keeps the pre-tenancy open admin plane,
so an upgrade denies nobody. The moment the first principal is committed — or a key is
configured — every request must authenticate; there is no grace window and no per-node flag to
reopen it (`should_bypass` is `api_key.is_none() && !has_any_principals()`).
`rift_cluster_no_principals` reports the open state for audit. Bootstrap therefore goes through
`MB_APIKEY` (legacy key = `tenant-admin@default` + `fleet-admin@*`), then minted principals.

*Rejected:* requiring a key whenever `--cluster` is set — breaks every existing keyless fleet on
upgrade.

**Superseded by D-73 (2026-09-07, #550).** Principals are gone, so the "first principal" half of
this rule has nothing left to count. What survives is its *other* half, unchanged and now the
whole rule: `--api-key` set closes the admin plane, unset leaves it open, and an upgrade denies
nobody. `should_bypass`, `has_any_principals` and the `rift_cluster_no_principals` gauge went with
the principals; the rejected alternative is still rejected for its original reason.

### ~~D-45 — Cross-tenant and unowned-port probes answer one indistinguishable 404~~
- **Status:** superseded
- **Decided:** T2 (#161) · RFC-002 §8.4 · narrowed by #182
- **Superseded by:** D-73

A tenant the principal is not bound to, a port owned by another tenant, and a port owned by
nobody all answer the same terse `404`; `403` is reserved for "bound here, role insufficient".
The gate refuses unowned ports too, because upstream's descriptive 404 names the port and would
otherwise let a tenant map which ports other tenants hold.

*Rejected:* letting unowned ports fall through to upstream's 404.

**Superseded by D-73 (2026-09-07, #550).** There is no cross-tenant probe to defeat: one
credential means one view of the fleet, and a port either has an applied imposter or it does not.
Upstream's own descriptive 404 is what a caller sees again, because the enumeration it used to
enable — *which other tenant holds this port* — is not a question the system can be asked.

### ~~D-46 — The legacy `--api-key` cannot hold a console session~~
- **Status:** superseded
- **Decided:** C2 (#185)
- **Superseded by:** D-73

`POST /session` with the legacy key answers `400`, never a cookie: the synthetic
`legacy:api-key` identity has no principal row, so a session minted for it could never resolve
back to a principal — a `200` there is the silent-fallback shape. Operators mint a real principal
and log in with its key; the legacy key is a curl/bootstrap credential only.

*Rejected:* minting the cookie and letting later requests fail `401` — indistinguishable from a
rotated key or a skewed clock.

**Superseded by D-73 (2026-09-07, #550).** Reversed, not merely retired: the `--api-key` **is**
the credential `POST /session` accepts, because it is the only credential the fleet has. The
reasoning here was sound for its world — a token naming an identity with no row behind it can
never resolve — and it stops applying the moment the token names no identity at all. A fleet
running with no key has nothing to exchange and answers `400`, which is this entry's
silent-fallback rule surviving its own decision.

### D-47 — Strict sequencing is owner-routed on the ring, opt-in per imposter, and degrades rather than fails
> **Amended by D-57** (2026-08-28, #514): a cursor **reset** is delivered per-peer, retried, and
> reported when it does not land. "Degrades rather than fails" governs a *decision* on the hot
> path; it never licensed losing a config-time reset.
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

### ~~D-48 — A blob no member can supply parks apply and reports degraded; the node never halts~~
> **Amended by D-56** (2026-08-28, #513): "never gives up" holds while the node is **up**. A
> shutdown ends a parked fetch, because the park holds the storage handle the node must release
> to stop.
- **Status:** superseded
- **Decided:** 2026-08-25 · #439 (user ruling)
- **Superseded by:** D-72
- **Implemented by:** #439

Superseded by D-72 (#549): nothing fetches on apply, so apply cannot park. `/_cluster/health`'s
`blob_fetch_stall` and the fleet roll-up `blob_fetch_stalls_fleet` are gone with it —
`parked_intents` and `parked_intents_fleet` are a **different** mechanism (the R4 write ledger,
which predates blobs and covers every write path) and are untouched. Retained for history.

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

### ~~D-49 — Payload fields stay optional on the wire; the bytes leave the op at submit, after the quorum~~
> **Amended by D-53** (2026-08-27): a quorum ack is no longer sufficient to strip — every member
> must also be known to apply a digest-only op.
- **Status:** superseded
- **Decided:** 2026-08-25 · #439
- **Superseded by:** D-72
- **Implemented by:** #439

Superseded by D-72 (#549): the two ops whose payloads this made optional no longer exist, so
there is no strip and no `origin`. This entry's compatibility argument — a Raft log entry is
plain `serde_json` with no envelope version, so a field cannot simply vanish from a shape already
on disk — **still governs every op that remains**, and is exactly why #549 is a deliberate
log-format break (a fleet upgrading across it starts from a fresh `cluster-state-dir`) rather
than a silent one. Retained for history.

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

### ~~D-50 — Snapshots carry a manifest of digests; the joiner fetches the bytes on install~~
- **Status:** superseded
- **Decided:** 2026-08-26 · #440 (#432 child 5)
- **Superseded by:** D-72
- **Implemented by:** #440

Superseded by D-72 (#549): `SnapshotPayload` no longer carries `spec_blobs`/`dataset_blobs` — nor
`sources`, `specs` or `datasets` — so there is no manifest and no fetch pre-pass on install. What
this entry established and #549 keeps: a snapshot install must not hold the redb write transaction
across network I/O, and it must never silently write an empty table where a populated one was
expected. Retained for history.

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

### ~~D-51 — A member serves a referenced blob from applied state when its transport store misses~~
- **Status:** superseded
- **Decided:** 2026-08-27 · #486 (#432/#440 follow-up); amended 2026-08-28 · #501
- **Superseded by:** D-72
- **Refines:** D-18, D-48, D-50
- **Implemented by:** #486, #501

Superseded by D-72 (#549): there is no blob route and no `sm_*_blobs` table to fall back to.
Retained for history.

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

### ~~D-52 — Blob GC retains an unreferenced digest until this node's log is purged past it, and never while a peer is asking for it~~
> **Amended by D-55** (2026-08-28, #504): a third rule, C, retains a tombstoned digest until
> every member has applied past it. The residual below — a follower whose log is ahead of its
> applied index — is closed by it, and the "no channel" premise the rejection rested on is
> corrected in place.
- **Status:** superseded
- **Decided:** 2026-08-27 · #480 (#432 follow-up)
- **Superseded by:** D-72
- **Refines:** D-18, D-48, D-50
- **Implemented by:** #480

Superseded by D-72 (#549): there is no blob store to collect, no `sm_blob_tombstones` table and no
GC sweep. Worth keeping in view for its own sake: this entry's "no channel exists" premise was
wrong twice (corrected by #504/#505 — `/internal/v1/applied` already carried it), which is the
standing reminder to grep the internal routes before designing a new one. Retained for history.

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

The callout above covers a rejection that was wrong twice over, and #504 corrected both halves.
This rejection originally also claimed no channel existed for
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

### ~~D-53 — The bytes leave the op only when every member is known to apply a digest-only one~~
- **Status:** superseded
- **Decided:** 2026-08-27 · #481 (#432 follow-up)
- **Superseded by:** D-72
- **Refines:** D-19, D-23, D-49
- **Implemented by:** #481

Superseded by D-72 (#549): no op carries a payload to strip, so there is no capability to probe.
The rolling-upgrade hazard it guarded against — handing a member an entry its build cannot decode
— is not gone in general, and #549 answers it the blunt way for this change: removing the nine op
variants is a deliberate log-format break, so a fleet upgrading across it starts from a fresh
`cluster-state-dir` rather than negotiating. Retained for history.

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

- **Status:** amended
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

**Amendment (D-73, 2026-09-08, #550/#566):** the first of those two properties is gone. There is
one fleet-wide route table, `routes_installed_for` no longer exists in the code, and every stored
route is compiled into the listener (`crates/rift-cluster/src/raft/store.rs::desired_routes`,
which filters nothing); chapter 8's tenancy surface is retired and the definition now lives in
[chapter 13](../architecture/13-router.md). The withdrawal of the header and subdomain schemes
stands on the replication property alone, which is the half that was doing the work — a
hard-wired scheme still could not be replicated control-plane state, and the closing sentence's
"another path the tenancy rule has to account for" is now simply another path.

D-11 is untouched and is not a dependency in either direction: the plain listener *and* the route
table are both upstream already, so nothing has to move first. (§6.3 says only that the plain
listener is upstream, which remains true; "D-11 would have to move first" was #491's own inference.)

*Rejected:* keeping them as a deferred preference. Because the code is equally consistent with "not
yet" and "never", a preference nobody is building is a claim the docs cannot keep true — #467, #489
and this issue are three corrections of the same one sentence, in three different places.

### ~~D-55 — Blob GC retains a tombstoned digest until every member has applied past it~~
- **Status:** superseded
- **Decided:** 2026-08-28 · #504 (D-52 residual)
- **Superseded by:** D-72
- **Refines:** D-52; also D-18, D-48, D-50, D-53
- **Implemented by:** #504

Superseded by D-72 (#549): there is no blob GC, so no retention rule. `network::fleet_applied_floor`
and `FleetAppliedFloor` — this entry's whole mechanism — went with it; blob GC was their only
caller. `/internal/v1/applied` itself stays, because the write barrier uses it. Retained for
history: the fail-closed reading it settled (a member that cannot be asked withholds the floor
rather than being counted as caught up) is the shape any future fleet-minimum aggregate should
take.

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

### ~~D-56 — Shutdown ends a parked blob fetch; the entry re-applies on restart~~
- **Status:** superseded
- **Decided:** 2026-08-28 · #513 (found while implementing D-55/#504)
- **Superseded by:** D-72
- **Refines:** D-48; also D-16, D-23
- **Implemented by:** #513

Superseded by D-72 (#549): apply awaits nothing, so it cannot park and there is no shutdown signal
to send — the `watch<bool>` and its receiver are gone. **The mechanism it documents is not gone
and is worth keeping in view:** openraft's state-machine worker awaits `Apply` inline and owns the
redb handle, so anything that makes `apply` block indefinitely will again make a node impossible
to stop and its data directory impossible to reopen. Retained for history, as the standing reason
`apply` stays synchronous and infallible.

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

### D-57 — A cursor reset is delivered to every member, retried, and named when it is not
- **Status:** active
- **Decided:** 2026-08-28 · #514 (found while verifying #504)
- **Refines:** D-47; also D-8, D-10
- **Implemented by:** #514
- **Code:** crates/rift-cluster/src/stores/sequencer.rs, crates/rift-cluster/src/metrics.rs

`ClusteredSequencer::reset_scope` is the engine's GC hook for response cursors — stub delete, bulk
replace, imposter teardown. It clears this node's own cursors and fans the reset out to every other
member, because the port-wide form names no single key and so has no owner.

**Where it is called from, which the fix turns on.** Two paths, and only one of them was obvious.
The direct one is an embedder or an admin op on the node in hand. The other is the **apply loop**:
`EngineAction::Sync` runs on *every* member (`store.rs`, `drive_engine`/`drive_one`) and calls
`engine.apply_config`, which fires this hook (`vendor/rift`'s `ImposterManager`). So each member
normally drops its own cursors as it applies the committed op, and the fan-out is a **backstop** —
for the direct call, and for a member whose engine refused the apply.

**What was wrong.** The fan-out ran as one `bridge.call(CallerClass::DataPlane, SEQ_OP_DEADLINE, …)`
wrapping a *sequential* loop over peers under a **single 2 s deadline shared by all of them**, each
per-peer error at `debug!`, the aggregate `let _ =`-discarded. Three consequences:

- **It blocked the apply loop.** `Bridge::call` parks the *calling thread* on
  `recv_timeout` — and on the apply path that thread is openraft's state-machine worker. A bulk
  `apply_config` that removed K stubs could stall apply for up to 2 s per stub. This is the most
  serious of the three and the one that most justifies the change.
- **It could be shed whole.** `DataPlane` permits are capped precisely so cursor traffic cannot
  starve the stateless path (RFC-001 §11.3), so under load the fan-out could be dropped before it
  started.
- **It could be lost silently.** One slow peer consumed the shared budget, leaving later peers
  never asked; nothing retried; every failure was `debug!` and the result discarded.

The last of these is what #514 surfaced, as an intermittent failure of
`reset_scope_clears_the_cursor_on_every_member` under load: the test calls `reset_scope` **directly**
(no committed op, so no apply-path reset anywhere), the cursor lives on the ring owner rather than
on the caller, and a lost fan-out left the owner answering `2` for the whole converge window.

**The rule.** `forget()` still runs synchronously — that is what the caller's next decision reads.
The fan-out is then **spawned** on the bridge's runtime instead of running under a sheddable
data-plane permit, so it never blocks apply. Delivery is per-peer and concurrent, each retried with
exponential backoff from **1 s** to `RESET_MAX_ATTEMPTS` (5, ≈15 s), the whole sweep bounded by
`RESET_TOTAL_BUDGET` (60 s). A peer that never accepts is **named** in a `warn!` and counted by
`rift_cluster_sequence_resets_incomplete_total`.

Three numbers that are not arbitrary:

- **1 s, not 200 ms.** `RpcClient` marks a peer unhealthy after three consecutive liveness failures
  and fast-fails it for a 5 s cooldown, and it already spends four internal attempts per call — so
  a shedding peer trips that breaker inside our *first* attempt. A 200 ms schedule would spend
  every remaining attempt inside the cooldown, answering "not healthy" without ever reaching the
  wire: a retry loop that cannot retry, in exactly the case it was built for.
- **Five attempts.** Enough to outlast that cooldown twice; deliberately not enough to chase a
  member that is down, which re-keys on the next membership change (D-8) and applies the op itself.
- **A 60 s ceiling.** Spawning dropped the deadline `bridge.call` used to impose, and one attempt
  sweeps every resolved address at a 2 s request timeout with the client's retries on top. The
  budget also bounds how long the spawned task holds its `Arc<RaftNode>` — the sequencer holds a
  `Weak` everywhere else precisely so it never outlives the node.

**A departed member is not a failure.** If a peer has left the membership by the time its turn
comes, delivery reports `NotAMember`: not retried, not named, not counted. It holds no cursors this
reset is about, and naming it would send an operator after a node that is correctly gone.

**Not a change to routing.** The reset still goes to every member rather than to an owner:
ownership can move between the reset and the next decision, and the port-wide form has no owner at
all. Only delivery changed.

*Rejected:* keeping the fan-out inside `bridge.call` with a longer deadline — still sheddable, and
still blocking the apply loop; retrying forever — chases a down member for cursors D-8 treats as
disposable; replicating cursors so no fan-out is needed — D-8 rejected that for the hot path and
this does not reopen it.

**Residual.** A member that is unreachable for the whole retry window keeps its stale cursors until
it is asked again, membership changes, or — on the apply path — it applies the committed op itself.
The last of those is why this is a narrow window in practice rather than the indefinite staleness
the fan-out alone would imply. What this entry adds is that the gap is now *reported* rather than
assumed away.

### D-58 — The chaos tier builds its image once and runs sharded; the required check is a gate
- **Status:** amended
- **Decided:** 2026-08-28
- **Refines:** D-41
- **Implemented by:** #516
- **Code:** .github/workflows/ci.yml, scripts/chaos-shard.sh, scripts/cluster-smoke-gate.sh, tests/cluster-chaos/src/lib.rs, deploy/compose/docker-compose.yml, deploy/Dockerfile

`cluster-smoke` took 35–37 min. Measured, by regressing the tier's own reported wall clock against
its scenario count over ten runs (2026-07-23 → 2026-08-28, N from 17 to 36):
**total ≈ 627 s + 33.2 s × N**. Two costs, not one, and they want different remedies.

**The 627 s constant is a cold image build**, and it was invisible. Every scenario brought its stack
up with `compose up --build`, so the first one paid for a full `deploy/Dockerfile` — wasm-pack
release, pnpm/vite, `cargo build --locked --release` over the workspace and vendored `rift` — with
no BuildKit cache mount, no `cache_from`, and a fresh runner's empty layer cache. Nothing timed it,
because it happened inside a test. It is now its own job (`cluster-smoke-prepare`), which is what
makes it both measurable and cacheable, and it is built **once per run** and handed to every shard
as an artifact rather than rebuilt per shard.

**The 33.2 s slope is one full fleet lifecycle per scenario** — `down -v` → ports-free → `up` →
readiness → `wait_cluster_formed` → teardown. It is not attacked here. Serial execution *within* a
runner stays forced by the fixed published ports the tier shares with the shipped compose file
(which is the point of sharing it), so the tier is parallelised on the only free axis: **four
shards, four runners**, partitioned by `scripts/chaos-shard.sh` from `--list` output.

**Measured after the change**, over the two runs that landed it:

| | before | after |
|---|--:|--:|
| wall clock | 35.5 min | **17–18.5 min** |
| runner-minutes | 35.5 | **38–44** |

The trade is stated honestly because the first draft of this entry got it wrong: runner-minutes go
**up ~20 %**, not "roughly unchanged". Four shards each pay their own checkout, toolchain, image
load and `cargo` compile — ~108 s apiece, ~530 s in total — which the single job paid once. Wall
clock is what a required check costs a person waiting to merge, and that halves.

The image build itself measured **478 s / 460 s**, against the 627 s the regression put on the
intercept. The intercept was not only the build: it also held the 36 per-scenario `compose up
--build` context re-transfers, which this change removes as well.

> **Corrected by D-60** (#519): the sentence below claimed the GHA layer cache was worth ~4 %. It
> was worth nothing — the cache never ran at all, and the two numbers it rested on were runner
> variance. Left in place, struck through, because a wrong measurement is worth seeing next to how
> it was made.

~~**The GHA layer cache is worth ~4 %** — 478 s cold, 460 s warm on the next run. Predicted, and now
measured.~~ **Wrong, on both counts.** A third run put the build back at 478 s with a `cargo build`
layer of 404.4 s against the first run's 404.3 s — identical, because it was cold every time. Two
samples had been read as cold-then-warm when they were two draws from the same distribution: the
same "one sample per side is not a rate" error this register has recorded elsewhere, made in the
write-up of the change that recorded it. What is true is the mechanism, which was never in doubt:
`COPY . .` sits before `cargo build` in `deploy/Dockerfile`, so any source edit invalidates the
dependency layer. D-60 fixes both the cache and that ordering.

`ignore-error=true` on the export: the cache is an optimisation and this is a required check, so a
cache backend that is down must cost minutes, never a red merge gate. It also, as D-60 found, hid
the fact that the cache was doing nothing.

**`cluster-smoke` keeps its name and stops doing the testing.** It is a required status check
(#104), so the name is load-bearing in `.github/rulesets/master.json`; it is now a gate job that
judges the prepare job's filter verdict against the shards' aggregate result. That makes the gate
the whole check, so it is a whitelist — a combination must be recognised as *good* to pass —
self-tested in `scripts/cluster-smoke-gate.sh`. `skipped` is the result that reads like success
while meaning nothing ran, and it is accepted only when the path filter itself said the tier was not
needed. This repo has produced that failure twice by other routes (#93, a filter failing open into
skipping; #101, a merge outrunning the job).

**Flavors are tagged apart, and that is a correctness fix, not a saving.** Dropping `--build` was
only safe once it was. `faketime.overlay.yml` overrides `build.target` but inherited the image
*name*, so the clock-lying flavor and the production one landed on the same compose-derived tag
(`rift-cluster-rift-1`); what kept the bytes matching the overlay in play was that every scenario
passed `--build` and so re-tagged on the way up. A real invariant resting on an argument nobody
would think to preserve — and C12 running on a truthful clock, or a later scenario inheriting a
lying one, would both have passed quietly. The two tags are now declared with their targets and
pinned by `compose_images_are_tagged_by_flavor`, so a third flavor fails a test rather than sharing.

**Shards are balanced by count, not by cost**, and those differ more than expected: the first run's
shards came in at 361–571 s, a 58 % spread. A weight table written before the numbers existed would
have been a guess, and would rot the way the nightly's hand-maintained matrix has (it names 24
scenarios where the tier has 36). So the precondition was built instead: `CHAOS_TIMING_LOG` records
per-phase and per-scenario wall clock, rendered as a step summary and kept as an artifact. Pack by
measured cost once there is measured cost — there now is.

**What the first measurement says, and it is not what this entry assumed.** Across 37 stacks, the
tier's 1611 s of stack time divides as: scenario bodies 787 s (49 %), **teardown 576 s (36 %)**,
`wait_cluster_formed` 181 s (11 %), and everything else — `up`, `ready`, `down`, ports-free —
66 s (4 %). The floor is a *teardown* cost, not a startup one, which is the opposite of what the
"full fleet lifecycle" framing above suggests and what any reading of `start_stack` would predict.

Two of those numbers are exact enough to name a mechanism rather than a symptom, and both are left
for their own change:

- **Teardown clusters at 13–17 s, on a `stop_grace_period: 15s`.** A node that drained inside its
  `RIFT_CLUSTER_LEAVE_TIMEOUT: 5` and exited should cost ~5–6 s; a 15.6 s mean says most containers
  ride out the grace period and are SIGKILLed. If that is what is happening it is not only a CI
  cost — the same shape makes a Kubernetes rolling update spend the full
  `terminationGracePeriodSeconds` per pod, and D-24's "the cluster maintains itself" assumes
  otherwise. It needs a diagnosis, not a `-t 1`, which would hide it.

  **It is not D-56's mechanism, and that is measured rather than argued.** D-56 (#513) landed the
  same day and fixed one concrete way a node could fail to stop — a parked blob fetch held the
  state-machine worker, so the storage-release wait timed out — which was close enough to this
  shape to be worth ruling out rather than assuming either way. Two tier runs of this branch,
  identical but for #513 sitting under the second, 37 stacks each: teardown **15.6 s → 15.2 s**,
  and the tier's whole stack time 1610.7 s → 1607.2 s. No change. Whatever holds these containers
  for the full grace period is still unaccounted for.
- **`wait_cluster_formed` is 5.0 s on every one of 36 stacks**, never more and never less. That is
  the voter gauge's resample interval, which the function's own doc comment names; it is measuring
  the sampler, not convergence. A membership read that is not gauge-derived removes it outright.

*Rejected:* restructuring `deploy/Dockerfile` with `cargo-chef` so a source change reuses a
dependency layer — it is where the remaining minutes are, and the 478 s → 460 s cache measurement
above is now the evidence for that rather than the prediction it was, but it is surgery on the
artifact the release lane ships and belongs in its own change. Building the image per shard — same wall
clock, four times the runner-minutes, and four images that are only *probably* identical. Reusing
one stack across scenarios — the isolation hazard is documented in `start_stack` (a leftover stack
makes `test_cold_start` pass for the wrong reason); it trades a latency problem for a correctness
one. Cutting scenarios or widening the path filter's skip set — D-41 already settled the
coverage-for-latency trade at one iteration per scenario, and this entry buys the latency back
without reopening it.

**Amendment (D-74, 2026-09-08, #552):** the tier builds **one** image, not two. The second was the
`faketime` flavor, whose `LD_PRELOAD` lied about the clock for the one scenario (C12) that proved
journal clears consulted no timestamp; that scenario left with the clear generations it was
proving clock-free, so the flavor, its overlay and the `runtime-faketime` Dockerfile stage went
with it. Nothing about the decision changes — `BUILT_IMAGES` is still the declared list, and
`compose_images_are_tagged_by_flavor` still fails a build target that arrives without a tag of its
own, which is the invariant this entry exists to keep. `runtime` is the Dockerfile's last stage
again, and every build site still pins `target:` anyway: the ordering is not a thing a compose
file should have to know.

### D-59 — A voter departs by one `RemoveVoters(retain = false)`; a leaving node is never a learner
- **Status:** active
- **Decided:** 2026-08-28
- **Refines:** D-21, D-27
- **Implemented by:** #496
- **Code:** crates/rift-cluster/src/raft/network.rs

`evict` was demote-then-remove: `RemoveVoters(retain = true)`, then `RemoveNodes`. Between those two
committed changes the departing node was a **caught-up learner that was still a member** — which is
exactly `promote_ready_learners`' promotion criterion. A departure and the promotion sweep could
therefore interleave, and the sweep would vote a leaving node back into the quorum.

Not hypothetical: #495 reproduced it deterministically by widening the leadership-takeover window in
`evict_completes_from_a_new_leader_after_partial_departure` by 3 s — the condition a loaded CI runner
produces on its own — and observed `voters=[1, 2, 3]` with n3 mid-departure. It had been failing
intermittently on CI before that, reported as the demote's no-op guard appearing to break, which sent
the first diagnosis to the wrong place entirely.

**The window is narrower than it looks, and that is what decides the remedy.** On one leader the
sweep cannot interleave at all: `evict` holds the membership gate across the whole departure and
`promote_ready_learners` takes the same gate per candidate. The only real window is the #71 case —
the demote commits on leader A, leadership moves, and B's sweep runs before the leaver's retry
reaches B's `evict`. B's gate is a *different lock*, so no amount of sequencing on A closes it.

**So the fix is to the shape of the departure, not to the sweep.** A voter leaves by a single
`change_membership(RemoveVoters({id}), retain = false)`. Verified against `openraft-0.9.25`:

- The public `Raft::change_membership` (`raft/impl_raft_blocking_write.rs`) commits the **joint**
  configuration and then, when the result is still joint, re-submits the same change to reach the
  **uniform** one. One call, exactly two committed membership entries.
- `next_coherent(goal, retain = false)` (`membership/membership.rs`) removes from `nodes` only the
  ids in `old_voter_ids − new_voter_ids`, both computed over the **joint union**. In the joint step
  that difference is empty, so the leaver stays in `nodes` — and `voter_ids()` is itself the joint
  union, so it is still a **voter**. In the uniform step the difference is the leaver, and it is
  removed outright.

Across both entries the departing node is a voter, then gone. **There is no committed membership in
which it is a learner**, so the sweep's `voters.contains(&id)` skip covers the entire departure — on
any leader, including one that takes over mid-departure. The bad state is unrepresentable rather than
guarded against.

#71's property is unaffected: from the half-finished state (joint committed, uniform not),
`RemoveVoters({id})` is coherent with the joint config and resolves straight to uniform, so
`leave_inner`'s retry against the new leader still completes the departure — and in that state the
leaver is still a voter, so the new leader's sweep cannot touch it either.

A **learner** still leaves by `RemoveNodes`. For a non-voter `last.difference({id}) == last`, so
`RemoveVoters` resolves to the same configuration and would commit an entry that removes nothing.

*Known, and unchanged by this entry:* `held_by_floor` counts `voter_ids()`, which is the joint
union, so in the joint half-state a departure sees the pre-departure voter count and D-25's floor is
evaluated one voter high. The old demote-then-remove shape opened the same window, but the joint
state is now the only residual half-state and so is exactly where a #71 retry lands. Not fixed here
— fixing it means deciding what the floor means mid-change, which is its own decision.

The **leaving leader** is a separate asymmetry worth stating, because the obvious reading is wrong:
the joint entry commits under the joint quorum, in which the leaver still counts, but
`rebuild_progresses` upgrades the leader's progress to the *effective* membership's quorum set as
soon as the uniform entry is appended, so that entry commits under the survivors alone. No transfer
step is needed regardless, because openraft keeps a removed leader replicating until the entry
removing it is committed (`leader_step_down`).

*Rejected:*

- **A departing set consulted by the sweep** (`evict` marks, remove clears). Per-leader in-memory
  state, so it is empty on the new leader — the only node where the race actually exists. It cannot
  cover the one window it was designed for.
- **Holding the admission gate across both halves of `evict`.** Already the case, and the gate is
  per node; it does not reach the next leader.
- **Tolerating the bounce and recording it.** `evict` does converge by retry, but a node the fleet
  has already decided is leaving re-enters the quorum, and each occurrence writes membership entries
  nobody asked for. The final state being right does not make the intermediate one acceptable, and
  "we knew about it" does not make the entries less of a fact in the log.
- **`SetNodes` or a custom `Node` type marking a departure.** openraft warns that incorrect
  `SetNodes` use can split the brain, and D-53 already rejected carrying metadata in membership.

*Consequence for tests:* the sweep interference #495 suppressed with an auto-voter ceiling pin is
gone at the source, so that pin comes out. `evict_completes_from_a_new_leader_after_partial_departure`
could no longer construct its half-finished state — that state does not exist — and is rewritten as
`evict_completes_from_a_new_leader`, which pins what #71 actually guarantees: a retried departure
from a new leader completes and appends nothing.

### D-60 — The image's dependency build is a cached layer; `type=gha` needs an action, not a shell step
- **Status:** active
- **Decided:** 2026-08-28
- **Refines:** D-58
- **Implemented by:** #519
- **Code:** deploy/Dockerfile, .github/workflows/ci.yml

Two defects, one symptom. `cluster-smoke-prepare` builds the node image in ~460 s, of which
`cargo build --locked --release -p rift-cluster-server --features console` is **404 s** — measured
from buildx's own step timings, not inferred. D-58 deferred this; here it is, and the deferral was
resting on a claim that turned out to be false.

**The cache was never running.** D-58 passed `--cache-from type=gha` / `--cache-to type=gha` to
`docker buildx build` from a `run:` step. Across three runs: zero `importing cache manifest` lines,
zero `exporting cache` lines, and a cargo layer of 404.3 s / 404.4 s — cold every time. The reason
is not the flags but where they were used: `type=gha` authenticates with `ACTIONS_RUNTIME_TOKEN`
and `ACTIONS_CACHE_URL`, which GitHub injects into **JS actions** and not into `run:` steps, so the
backend could not authenticate and silently did nothing. `ignore-error=true`, added so a cache
outage could not redden a required check, is what kept it quiet. `release.yml` had it right all
along by using `docker/build-push-action@v6`; this is the same fix one lane over.

**Nothing to cache, either.** `COPY . .` sat immediately before `cargo build`, so even a working
cache could only ever return the apt and pnpm layers: editing a chaos scenario rebuilt all 371
crates. `cargo-chef` splits that in two — a `planner` stage that reduces the tree to a recipe of
manifests and lockfile, and a dependency layer keyed on that recipe rather than on the source.

**What the split is worth, from the same measurements.** Of the 404 s: 144 s to reach the first
path crate (third-party dependencies), then 260 s across the nine path crates, of which the last
174 s is `rift-cluster` and `rift-cluster-server`. Only those two, plus `rift-cluster-base` and
`rift-cluster-spec`, change on an ordinary PR. Everything before them belongs in the cached layer.

**`vendor/rift` is copied before `cook`, and that is not incidental.** Verified rather than assumed,
because it decides whether the split is worth 144 s or 230 s: `cargo chef prepare` writes a skeleton
of the **workspace members only** — the recipe carries the five `crates/`+`tests/` manifests and
nothing from `vendor/rift`, which is a separate workspace consumed as path dependencies. So `cook`
cannot resolve without the real directory present; and with it present, `cook` compiles all five
vendored crates as the dependencies they are (run locally against the real recipe: `rift-types`,
`rift-http-proxy`, `rift-lint`, `rift-mock-core`, `rift-store-redis`, alongside dummy `v0.0.1`
stand-ins for this repo's own four). Its own `COPY` layer, so a submodule pin bump invalidates the
dependency build — correct, they are dependencies — and nothing else does.

**`builder-static` is left alone.** Only `release.yml` builds it, on a tag, so the split would buy
no PR latency and a mistake in it would surface for the first time during a release. The stage worth
restructuring is the one every cluster-touching PR exercises.

**Measured, cold then warm, on the two runs that landed this:**

| | before | cold (this change) | warm |
|---|--:|--:|--:|
| `cargo build` layer | 404.3 s | 439 s¹ | **184.7 s** |
| "Build the node image" step | 478 s | 581 s | **314 s** |
| `cluster-smoke-prepare` job | ~462–507 s | 615 s | **357 s** |
| `cluster-smoke` wall clock | 17–18.5 min | — | **~14 min** |

¹ cold is the sum of `cargo install cargo-chef` (58.9 s) + `cook` (240.7 s) + `cargo build`
(139.5 s); all three are cached on a warm run except the last. A cold build is genuinely slower,
which is the trade: it happens once per dependency change, and the warm path is what every PR pays.

> **Corrected by D-64** (#524): "the warm path is what every PR pays" is the error in the footnote
> above. A GitHub cache is scoped to the ref that wrote it, and every job here runs only on
> `pull_request` — so nothing wrote a cache `master` could share, and **every PR's first run was
> cold**, at 19.4 min against the 16.5 min this change inherited. The warm figure was real, but it
> was the second-run case presented as the general one. D-64 seeds from `master` and makes it
> general.

`importing cache manifest` and `preparing build cache for export` appear in the log for the first
time, and `cargo install cargo-chef`, `cargo chef prepare` and `cargo chef cook` all report `CACHED`
on the warm run. That is the whole claim, and it is now checkable rather than asserted.

**Still on the table, and measured rather than guessed: ~100 s.** The warm `cargo build` recompiles
the five vendored crates — the log shows `rift-types`, `rift-http-proxy`, `rift-lint`,
`rift-mock-core` and `rift-store-redis` starting at 0.4 s, i.e. *after* the third-party graph came
back from cache but before this repo's own four. `cook` built them, so cargo should have found them
fresh; what dirties them is the blanket `COPY . .` after `cook`, which rewrites `vendor/rift` with
new mtimes and so busts cargo's fingerprint. Copying only what the build actually needs after that
point (`Cargo.toml`, `Cargo.lock`, `crates/`, `tests/`) would leave the submodule untouched. Not
done here: it trades a blanket copy that cannot omit anything for an explicit list that can, and it
belongs behind its own run rather than bundled into the change that made it visible.

*Rejected:* `crazy-max/ghaction-github-runtime` to export the tokens into the existing `run:` step —
it works, but it adds a publisher to the supply chain to keep a shell loop that the action replaces
outright, and `release.yml` already establishes the action as this repo's way. A `lukemathwalker/
cargo-chef` base image instead of `cargo install cargo-chef --locked --version` — a third-party
image on the path to the shipped binary, where the pinned crates.io install has the provenance the
dependency graph already trusts and costs a minute only on a cold build. Dropping
`ignore-error=true` so a broken cache is loud — it would redden a required check on a GitHub
outage; the tell is documented at the call site instead (no import line, cargo back at ~400 s).

### D-61 — A peer that answered is never reported as unreachable; a relayed refusal keeps the peer's error

- **Status:** active
- **Decided:** 2026-08-28
- **Refines:** D-17, D-28, D-40
- **Implemented by:** #471
- **Code:** crates/rift-cluster/src/raft/node.rs, crates/rift-cluster/src/stores/flow.rs, crates/rift-cluster/src/stores/proxy.rs

Two rules, one subject: what a caller is told when the peer it needed said no.

**1. Wording.** `call_any` sweeps the addresses an authority resolves to and, on failure, describes
the outcome. It may say *unreachable* only when every attempt was a liveness failure
(`RpcError::is_liveness_failure` — `Timeout | Transport | Shed`). A peer that answered — even to
refuse — was reachable, and its own reason is the message. This is not cosmetic: it is the
difference between on-call checking the network and on-call checking quorum state, and the refusal
exists precisely to be that signal.

**2. Sweep.** A peer that answers ends the sweep; only a liveness failure is worth trying the next
address for. The justification is that an answer is more informative than a further address's
silence — **not** that the addresses are the same node. Under D-28 every resolved address is
dialled in resolver order, and while a dual-stack advertise authority is one node, a multi-A
headless service (what `--cluster-seeds` may point at, reaching `sweep_addresses` through
`join_via`) is not: there, a non-liveness answer from the first pod stops the sweep before a
healthy second pod is tried. That is the behaviour `call_any_typed` has had since #391 — a
`NotLeader` hint recovered from one address must not be overwritten by a later address's transport
error — and this decision keeps it and extends it to `call_any` rather than revisiting it. A seed
that answers is a seed that can redirect; a seed that is still booting and answers `Handler` is the
case where the rule costs something, and it is accepted.

**3. Relay.** A forwarding hop that carries an *owner's* refusal uses `call_member_typed` and
propagates the owner's `RpcError` rather than restating it as this hop's `Transport`. Classifying a
peer's considered refusal as a transport fault is wrong at the point it is made, independently of
who reads it later.

**What this does and does not change, measured rather than assumed.** The observable effect is the
**message text** and the early stop. It is *not* an HTTP status change: at all six rewired sites the
typed error is flattened within one or two frames — the flow store wraps it as
`anyhow!("flow store: {e}")`, proxy claim/complete as `ProxyStoreError::Unavailable(String)`,
release logs it, lookup discards it with `.ok()?` — so no `RpcError::status()` is ever consulted on
these paths. A forwarded isolated-owner read answers **500** on the data plane before and after
(`backend_error_response` finds no `BackendUnavailable` to downcast), or **200** with an empty token
under `{{ state.k }}` with `RIFT_DEBUG` unset. An earlier draft of this entry claimed 502 → 503;
that was false in both halves and is corrected here. Giving isolation its own 503 means carrying
`BackendUnavailable` out of the clustered store — a data-plane contract change, and a separate
decision.

**Scope.** Only the flow store's `GET_PATH` forward relays a refusal the peer expressed *as* an
`RpcError`. `WRITE_PATH` and proxy's `CLAIM`/`COMPLETE`/`LOOKUP`/`RELEASE` refuse **in band** —
`WriteReply::Error { reason }`, `ClaimReply::Error { reason }` inside a `200` — which no
transport-level change affects; they move to `call_member_typed` for the refusals they *can* carry
(a shedding bridge, a `NotLeader`, a handler fault), not for isolation. Whether an in-band refusal
should instead be a typed error is a separate question this decision does not settle.
`sequencer.rs`'s forward keeps the `Transport` re-wrap deliberately: its result is `.ok()?`-ed into
the local-cycle fallback, so no text or class it produces is ever read.

Rejected: fixing only the flow store's call sites and leaving `call_any`'s wording. The wording is
wrong for all sixteen `call_member` callers, and every one of them is operator-facing.

### D-62 — The builder copies named roots, not the tree; `vendor/rift` is not re-copied after `cook`
- **Status:** active
- **Decided:** 2026-08-28
- **Refines:** D-60
- **Implemented by:** #523
- **Code:** deploy/Dockerfile, tests/cluster-chaos/tests/scenarios.rs

D-60 left ~100 s on the table and said where. `cook` compiles the five vendored crates, but the
`COPY . .` that followed it rewrote `vendor/rift` with fresh mtimes, so cargo's fingerprint saw them
as dirty and rebuilt all five. Measured, from the warm build's own log: `rift-types`,
`rift-http-proxy`, `rift-lint`, `rift-mock-core` and `rift-store-redis` all start compiling at 0.4 s
— *after* the third-party graph came back from cache, which is exactly the signature of "the
dependency layer worked and then something dirtied part of it".

**The builder now copies named roots and leaves the submodule alone.** `Cargo.toml`, `Cargo.lock`,
`crates/`, `tests/`, `docs/`. `vendor/rift` is copied once, before `cook`, and never again.

**A second effect, not incidental.** The layer's cache key is now the compiler's actual inputs, so
editing `web/`, `scripts/` or `.github/` no longer invalidates the build at all. Under D-60 a
workflow-only change still paid a full `cargo build`; under this entry it pays nothing.

**`docs/` is a build input, not documentation**, and this is the part that makes the trade real
rather than free. `crates/rift-cluster-server/src/openapi.rs` does
`include_str!("../../../docs/api/openapi-ee.yaml")` — the OpenAPI contract is compiled into the
binary. A copy list written from "what a Rust build obviously needs" omits it, and the first draft
of this change did exactly that.

That is the whole risk of naming roots: a blanket copy cannot omit anything, and a list can. The
failure mode is at least loud — a missing file breaks the image build, on the PR that introduces it,
in a job that runs on every cluster-touching change. But loud is not the same as caught, and it
lands on whoever adds the next `include_str!` rather than on whoever shortened the list. So
`the_builder_copies_every_root_the_crates_compile_in` reads the roots **out of the Dockerfile** —
restating them in the test would be the same rot one file over — walks every `.rs` under `crates/`,
resolves each `include_str!`/`include_bytes!` literal, and asserts its root is copied. It refuses to
pass vacuously in both directions: zero parsed roots and zero found references are each an assertion
failure. Verified by mutation — deleting `COPY docs docs` fails it, naming the file and the resolved
path.

*Rejected:* `COPY --exclude=vendor/rift . .`, which keeps blanket semantics and would need no list at
all — it requires the `dockerfile:1.7-labs` syntax directive, which pulls a frontend image at build
time and changes the parser for every stage in the file, to avoid a list that a test now pins.
Restoring mtimes after the copy — there is no declarative way to do it, and a `touch` sweep would be
a second thing to keep correct. Copying `docs/api` alone rather than `docs/` — narrower, but it
makes the next `include_str!` into `docs/` a build break for no gain; after `.dockerignore` strips
`**/*.md`, the whole directory is three files.

### D-63 — Sequencing costs one `next` per decision and never peeks; there is no amplification, and no cache

- **Status:** active
- **Decided:** 2026-08-28
- **Amends:** RFC-001 §11.3
- **Refines:** D-47
- **Implemented by:** #476
- **Code:** crates/rift-cluster/src/stores/sequencer.rs, crates/rift-cluster/src/metrics.rs

RFC-001 §11.3 budgeted **"one `next` plus up to a few `peek`s per request"** and reasoned from
there to a mitigation: an optional ≈ 50 ms peek cache on the owner, to bound cursor-store work
under a peek-heavy template. D-47 carried the claim forward and deferred the measurement to #476.

**The premise is false. There is no template path that peeks.** Verified against source at
`3c0ca47` / vendor `9fb5a1a`, in both directions:

- `ResponseSequencer::peek` reaches the seam from exactly one engine site — `peek_stub_response`
  (`vendor/rift/crates/rift-mock-core/src/imposter/core/responses.rs:22`), whose only caller is
  `get_response_preview` (:102), whose only caller is the **debug preview** response
  (`imposter/handler.rs:2046`). It is not on any serving path.
- The serving path is `next_stub_response` → `via_sequencer(.., advance: true)`
  (`responses.rs:11`, :33), reached from `handler.rs:1025`: **one `next` per matched stub with
  responses, and nothing else.**
- No template variable exposes the cursor index, so the body that §11.3 imagined — one that
  interpolates the sequence several times — cannot be written today.

So the honest cost is **one `next` RPC per decision on a non-owner, zero on the owner, zero
`peek`s**. That is *cheaper* than the documented budget, which is exactly why it is worth pinning
rather than quietly correcting: an amplification introduced later would still sit inside the
RFC's stated envelope and would pass unnoticed.

**The peek cache is withdrawn.** It was a mitigation for a cost that does not exist, and it is not
free — a cursor read served from a 50 ms cache is a *stale* read of the one structure the whole
owner-routing design exists to keep authoritative. If a peeking serving path is ever introduced, it
is a new decision with its own numbers, not a licence already granted here.

**Unchanged:** the RPC fan-out is still inherent, and a caller-side cache is still refused — that
half of §11.3 was reasoning about `next`, and it survives intact. A non-owner cache would
reintroduce precisely the stale read owner routing prevents (D-47, D-9).

**Observability.** `rift_cluster_sequence_decisions_total{op,path}` counts every decision exactly
once, `op` ∈ `next` / `peek` and `path` ∈ `owner` / `forward` / `local` / `fallback` — mirroring
`rift_cluster_flow_reads_total{path}` (#120) so the two stateful ops read the same way. The label
values are a closed Rust enum (`DecisionPath`) rather than free text, so they cannot drift from the
paths `route` actually has. `rift_cluster_sequence_fallbacks_total` is unchanged and still the
degradation signal; `path="fallback"` is the same event seen through the new metric.

**Measured** (the figure §11.3 owed), from `owner_hop_latency_figure`, 1 000 decisions each over
loopback TCP on a 3-member in-process fleet, Apple M4 / 10 cores, debug build:

| | p50 | p99 |
|---|--:|--:|
| owner, no hop | 16–23 µs | 46–59 µs |
| non-owner, one RPC | 181 µs | 374 µs |

The owner hop costs ~160 µs on loopback, comfortably inside §11.3's "one LAN RPC
(sub-millisecond typical) per op". This is a *floor*, not a production figure: a real LAN adds its
own round trip, and a debug build is not a release one. It is recorded to give the RFC's cost model
a number rather than a citation, and the figure is reproduced by running the ignored test, not
asserted — a latency assertion on CI's shared 2-vCPU runners would be a flake generator.

Rejected: writing the bench §11.3 asked for. `n = 1, 4, 16 sequence references per response` cannot
be constructed, because a response cannot reference the sequence at all. Building the surface in
order to measure it would be inventing the cost the RFC feared.

### D-64 — Only `master` writes the image build cache; PRs read it
- **Status:** active
- **Decided:** 2026-08-28
- **Refines:** D-60
- **Implemented by:** #524
- **Code:** .github/workflows/ci.yml

D-60 fixed a cache that had never run, and measured it warm. It never asked *which refs can read
it*, and the answer undid most of the win.

**A GitHub Actions cache is scoped to the ref that wrote it.** A pull request reads its own
`refs/pull/<n>/merge` entries and the default branch's, and nothing else. Every job that builds this
image runs `if: github.event_name == 'pull_request'`, so nothing ever wrote a cache under
`refs/heads/master`. Measured, from the cache API rather than inferred: **93 buildkit entries across
two PR refs, zero under `master`.** Each PR could only warm itself.

**So D-60 was a regression for any PR that runs once.** The chef install (~60 s) and `cook` (~246 s)
are added work that only pays back off a readable cache. Measured across four runs:

| | wall clock |
|---|--:|
| D-58, before cargo-chef | 16.5 min |
| D-60/D-62, first run of a new PR | **19.4 min** |
| D-60/D-62, second run of the same PR | 14.0 min |
| D-60/D-62, run whose Docker context is unchanged | 8.9 min |

The 14 min figure was real, and it was the second-run case reported as the general one.

**`cluster-smoke-cache-seed` writes the cache, on push, and nothing else does.** No `load`, no
`push`, no scenarios — `outputs: type=cacheonly`, whose only product is cache under
`refs/heads/master` that every branch can read. Its build must match `cluster-smoke-prepare`'s
exactly (same target, flags and scope) or it warms a cache the PRs then miss, which is D-60's
failure from the other side.

**Not path-filtered, deliberately.** It reads the same cache it writes, so a push that changed
nothing the image depends on finds every layer present and finishes in about a minute. It does real
work exactly when the dependency graph moved — which is exactly when PRs would otherwise each pay
for it. A filter would add a second thing to keep correct in exchange for that minute.

**`continue-on-error: true`.** This job feeds an optimisation; a failure in it must slow the next PR
down and never turn `master` red, because a red `master` is how people learn to ignore a signal.

**The faketime flavor is not seeded.** It is `FROM runtime` plus one apt layer, so every expensive
layer it needs is already in what this produces.

*Rejected:* seeding on a schedule instead of on push — it would warm a cache for a `master` that had
already moved, which is the stalest possible thing to hand a PR. Making `cluster-smoke` itself run on
push — that runs 36 scenarios per merge to populate a cache, and the tier's whole cost is the
scenarios. Sharing one scope across flavors *and* the static builder (`release.yml` scopes per
flavor for a reason its own comment gives: `runtime` and `runtime-static` diverge at `builder`, so a
shared scope has each evict the other).

**Watch the ceiling.** Repo cache usage stood at 9.54 GB of GitHub's 10 GB when this landed, 165
entries, of which buildkit was ~2.8 GB spread across PR refs. Seeding from `master` should reduce
that — one shared copy rather than one per open PR — but eviction is LRU and repo-wide, so a
`cluster-smoke` that suddenly rebuilds from cold is worth reading as "the cache was evicted" before
it is read as "the Dockerfile changed".

### D-65 — A clustered flow-store failure caused by the cluster's state is `BackendUnavailable`; the data plane answers it 503

- **Status:** active
- **Decided:** 2026-08-28
- **Amends:** docs/architecture/09-durability-failure.md, docs/architecture/05-read-path.md
- **Refines:** D-17, D-61
- **Implemented by:** #522
- **Code:** crates/rift-cluster/src/stores/flow.rs

The design has promised this status three times over — RFC-001 §7.6's table (*"reject: 503
cluster/owner-unreachable"*), Chapter 9's degradation table, and Chapter 5, which stated outright
that the isolation refusal "reaches the caller as the same `503` an unreachable owner produces".
The code delivered none of it. Upstream's contract (#318) is that a backend attaches
`BackendUnavailable { feature, detail }` as the *source* of its `anyhow::Error` for an outage, and
`backend_error_response` downcasts to it — 503 — or answers 500; `rift-store-redis` does exactly
that for every Redis failure. The clustered store never constructed it: every `RpcError` → `anyhow`
boundary in `stores/flow.rs` was a Display-flatten, so isolation, an unreachable owner, a shed
bridge and a not-yet-started cluster were all generic 500s on the data plane. D-61 measured that
and deferred the decision; this is the decision.

**The rule.** A clustered flow-store failure caused by the **cluster's state**, not by the request,
carries `BackendUnavailable { feature: "flowState" }`. Cluster state means:

- the D-17 isolation refusal — the typed `RpcError::Unavailable` on the read path *and* the in-band
  `WriteReply::Error { reason: ISOLATED_REFUSAL }` on the write path;
- a liveness failure reaching the owner: `RpcError::is_liveness_failure()` — `Timeout`,
  `Transport`, `Shed`;
- any other `RpcError::Unavailable`;
- a fenced or misrouted write (`WriteReply::Fenced` / `NotOwner`): the op did not happen and a
  retry against a rebuilt ring succeeds;
- the not-ready states — no bridge yet, no applied membership yet, no owner in the ring, node
  shut down — wherever they are met: at this node's pre-flight, inside the bridge closure, or on
  the owner side of a forwarded op.

Everything else stays a plain error and answers 500: `Handler`, `BadRequest`, `Unauthorized`,
`VersionSkew`, `UnknownRoute`, `BodyTooLarge`, `NotFound`, `NotLeader`, any other in-band
`WriteReply::Error` reason (the owner's own storage failing), and a reply that fails to decode. The
line is the Redis store's: corruption and faults are not unavailability, and a 503 there would
invite a retry that cannot help.

**What changes on the data plane, measured.** The status moves 500 → 503 exactly where the store's
error reaches `backend_error_response` with its chain intact: the scenario match gate
(`find_matching_stub` → `scenario_state`), the scenario transition (`apply_scenario_transition`)
and the debug match preview. It does **not** change for `{{ state.k }}` (200 with an empty token,
or a 500 `x-rift-template-error` under `RIFT_DEBUG`) or for `_rift.script`'s `ctx.state.get`
(a 500 `script error`): those doors erase the type upstream — `template_fn::render` substitutes `""`
for every failed token, `flow_result` stringifies with `{e:#}` — before any downcast can happen,
and a Redis outage behaves identically there today. #522's premise that the wrap would turn the
default template path into a hard failure was false; making those doors outage-aware is an
upstream question, not this one. `local` reads (D-10) never reach any of this.

**Wire format is unchanged, deliberately.** The in-band isolation refusal and the in-band
not-ready refusals are recognised at the store face by matching their constants
(`ISOLATED_REFUSAL`, `NOT_READY_*`) — the same discriminators the acceptance gate already matches
on — rather than by new `WriteReply` variants, which a mixed-version fleet mid-upgrade would decode
on the old side as handler errors. One cluster-port status does move with this: a node that is
not ready now answers a forwarded flow op (and its own bridge closure) with `RpcError::Unavailable`
(503) instead of `Handler` (500), so a forwarding hop relays it as unavailability under D-61's
relay rule rather than as the owner's fault. The admin listings (`COUNTS_PATH`, `SPACES_PATH`)
keep `Handler`: their callers already fold any error into `partial: true`.

**On D-61.** Its wording, sweep and relay rules stand; its "not an HTTP status change" paragraph
described the code at the time and is overtaken here. The message text D-61 chose survives inside
`BackendUnavailable`'s `detail`, so the runbook signal and the status now agree.

Rejected: the isolation-only wrap (the two `ISOLATED_REFUSAL` sites). It would leave an
*unreachable* owner at 500 while the same three documents promise 503, and RFC-001's row is
literally "owner-unreachable".

### D-66 — A proxyOnce claim the cluster cannot serialize refuses the request (503); it never forwards

- **Status:** active
- **Decided:** 2026-08-29
- **Amends:** RFC-001 §7.6, docs/architecture/06-flow-state.md, docs/architecture/09-durability-failure.md, docs/architecture/12-testing.md
- **Refines:** D-17, D-40, D-65
- **Implemented by:** #529 (EE), rift#990 (the U-17 seam)
- **Code:** crates/rift-cluster/src/stores/proxy.rs, vendor/rift/crates/rift-mock-core/src/recording/proxy_store.rs

Chapter 9's degradation table and RFC-001 §7.6 both promise `503` for a proxyOnce claim whose
signature owner is unreachable, for the stated reason that *"duplicate upstream side-effects are
worse than a failed mock call"*. The code did the opposite, and four layers had to line up for it
to happen. Upstream's `ProxyStoreError` had a single variant, `Unavailable`, documented as *"lets
the caller degrade gracefully"*, and the engine honoured it exactly: any `Err` from `try_claim`
became `claim_token = None` and the request was forwarded upstream without a claim. The clustered
store collapsed **every** failure into that one word — including `owner_claim`'s D-17 isolation
refusal, which is written to fail closed and was undone two hops later. The handler had no door
for the status anyway: its only `Err` arm answered `502` through `upstream_error_response`, and
`ProxyStoreError` carried no `BackendUnavailable` for `backend_error_response` to downcast. And
chaos C10 had institutionalised the result, calling the forward *"degrade-don't-wedge"* and
bounding origin calls at `1 + refires` — a bound defined by the length of the outage.

**The rule.** In `proxyOnce` mode this store **is** the exactly-once arbiter, so any failure to
obtain a claim answer is `ProxyStoreError::Refused(BackendUnavailable { feature: "proxyOnce" })`;
the engine fails the request through `backend_error_response` — `503` — and does **not** call the
upstream. That covers the D-17 isolation refusal, a liveness failure reaching the owner
(`Timeout`/`Transport`/`Shed`), an unsettled ring after the redirect attempts, the not-ready
states (no bridge, no applied membership, node shut down, no owner), an unresolved port identity,
and any in-band `ClaimReply::Error`. Every refusal counts
`rift_cluster_proxy_claims_total{outcome="refused"}`.

**Why the whole claim path, and not D-65's 503/500 split.** At a claim there is no request payload
that can be at fault — the input is `(port, signature)`. The two paths that are not cluster state
(a reply that fails to decode, i.e. version skew mid-upgrade; the owner's own `proxy_recorded`
read failing) are transient from the client's seat, and no test or runbook distinguishes them.
Splitting them would cost a second seam variant to serve a distinction nobody can act on.

**What keeps its behaviour, deliberately.** `ClaimOutcome::InFlight` still forwards without
recording: a claim *was* serialized there, so that duplicate is bounded by the one racing window
and is by design (Ch. 12's C11 row). `complete`/`release` keep upstream's release-and-serve — they
fail *after* the upstream call succeeded, and a `503` there would provoke the retry that is itself
the duplicate; this refines RFC-001 §7.6, whose row reads "claim/complete/release". `proxyAlways`
and `proxyTransparent` gate nothing and refuse nothing — making them fail closed would take an
imposter offline for a partition that costs it nothing. `local` mode (§7.6's *"local Pending,
duplicates possible"*) was never built and is withdrawn: no requirement exists, and D-10's
availability-wins exception is sequences alone (D-47).

**The seam is upstream's, not ours (U-17, rift#990).** `ProxyStoreError` gains `Refused` and
becomes `#[non_exhaustive]`; `Unavailable` keeps its meaning, its degrade, and its test. The
engine's `Err` arm splits, and both proxy-leg response arms — the stub proxy and `defaultForward`
— share one helper that answers `BackendUnavailable` with `503` and everything else with the
`502` it always had. A refusal is not logged as an upstream failure: none was called, and saying
so would send an operator to the wrong system. This had to be upstream because both the
degrade decision and the status choice are engine-side; a downstream store cannot reach either.

**What this costs, measured (C11, 2026-08-29).** The refusal is not confined to partitions. A
claim can fail to serialize on a *healthy* fleet for transient reasons — a readiness race against a
just-created imposter whose config a node has not applied yet, or the data-plane bridge shedding
under a burst — and those now answer `503` where they previously forwarded and answered `200`.
Chaos C11 caught this on its first run after the change: 18 simultaneous first-hits across three
nodes produced refusals, and its assertion that *"a raced proxyOnce request must still answer"*
`200` was encoding the pre-D-66 contract. It now accepts `200` or `503` and counts the refusals,
while `proxyAlways` stays strictly `200` as the control.

This is the trade Chapter 9 chose, stated in the direction that costs something: **proxyOnce trades
availability under transient claim-path failure for the absence of duplicate upstream calls.** A
client that cannot tolerate that should use `proxyAlways`/`proxyTransparent`, which gate nothing,
or retry — the signature stays claimable, and C11's settle loop is exactly that retry. The bound is
that a refused request is retryable and costs the upstream nothing, not that it never happens.

*Rejected:* keeping the degrade and correcting the docs to say *"forwards without recording"*.
The distinction that settles it is `InFlight` vs `Unavailable` — the first forwards while the
owner **is** serializing, so the duplicate is bounded by one racing window; the second forwards
while **nobody** is, so it is bounded by the outage. Only the first is a trade worth documenting.
*Rejected:* a `fails_closed()` capability flag on the trait instead of an error variant — it
describes the store rather than the failure, and the same store must not refuse for `proxyAlways`;
the 503 door needs a typed cause regardless, which the variant already carries.

### D-67 — A measured chaos figure is recorded as a run artifact, not merely printed

- **Status:** active
- **Decided:** 2026-08-29
- **Refines:** D-41, D-58
- **Amends:** docs/architecture/12-testing.md
- **Implemented by:** #534
- **Code:** tests/cluster-chaos/src/lib.rs, tests/cluster-chaos/tests/scenarios.rs, .github/workflows/ci.yml

Chapter 12's verification philosophy rests on the suite *measuring* a documented window rather
than asserting a guessed constant, and four scenario rows say so outright — C10's duplicate-upstream
ceiling "printed as the run's artifact (D-66)", C11's racing-window call counts, C12's observed
clock spread, C29's partitioned read answered "(measured, printed)".

None of those figures existed anywhere a human could read them on a **passing** run. libtest
captures a passing test's stdout and `cluster-smoke` passed no `--nocapture`, so an artifact
surfaced only when its scenario **failed** — which is when it is least useful, the run having
aborted before settling the measurement. The promise had been true in intent and false in fact
since the first row was written.

**The rule.** A scenario that measures a figure records it with `chaos_artifact!`, which prints the
line **and** appends `<scenario>\t<text>` to `$CHAOS_ARTIFACT_LOG`. `cluster-smoke` sets that
variable, runs the tier with `--nocapture`, renders the file as a per-shard step summary and
uploads it as `chaos-artifacts-<shard>`. A bare `println!` for a measured figure is a defect.

**Why both halves.** The print is what Ch. 12 promises and what puts the number beside its scenario
in the log. The file is what makes the number comparable *across* runs, and that is the property
the measurement was introduced for: a bound that drifts **inside** its own ceiling — duplicate
upstream calls creeping 2 → 4 while still under C10's assertion — is invisible to the assertion by
construction, and the printed artifact is the early warning. A trend cannot be read out of a
thirty-minute log.

Best effort on the file, as with `CHAOS_TIMING_LOG` (D-58), which this mirrors deliberately rather
than inventing a second shape: an artifact log that cannot be written costs a datum, never the run.
Unset — every local run — `chaos_artifact!` is a `println!` and nothing else.

*Rejected:* `--nocapture` alone, which is what #534 proposed. It makes the figures visible and
leaves them un-greppable across runs, which the issue itself names as the weakness; the collector
that fixes that already existed one function away.
*Rejected:* a distinct `chaos-artifact:` line prefix for grepping the log, also floated in #534.
The lines already begin `cNN artifact:`, so the prefix would read `chaos-artifact: c10 artifact:`
and would still be answering with a log what the uploaded TSV answers directly.
*Rejected:* asserting the printed figures instead of recording them. That is the guessed-constant
the philosophy rejects — C10's assertion is the contract's own bound, and the artifact is the
separate question of what the run actually measured underneath it.

### ~~D-68 — Both front-door route endpoints publish `installed`; the write says what it cannot do~~

- **Status:** superseded
- **Decided:** 2026-09-01
- **Refines:** D-54
- **Superseded by:** D-73
- **Amends:** docs/architecture/13-router.md
- **Implemented by:** #536

`routes_installed_for` is the single definition of which tenants' routes are compiled into the
shared front door, and only the default tenant's are. That rule is deliberate — the front door is
one listener with no tenant discriminator, so a unioned table would let any tenant publish a
catch-all that captures the whole fleet's traffic — and it is not what this entry changes.

The rule had **two** call sites and neither was a write: the compiler that enforces it
(`RedbStateMachine::desired_routes`) and `GET /front-door/route-hits`. So a non-default tenant's
`PUT /front-door/routes` was validated, committed to the replicated log, read back byte-for-byte
and answered `200` for a table that could never dispatch a single request, with the only signal in
the system sitting behind a *different* endpoint the caller had no reason to suspect it needed.

**The rule.** Both `PUT` and `GET /front-door/routes` answer `installed: <bool>` beside the table,
derived from `routes_installed_for` at the render site through one shared helper — so the write,
the read and the compiler cannot drift from each other, and the word means exactly what
`route-hits` already means by it. `200` continues to mean *stored*; `installed` is what says
*dispatching*.

**Why a body field and not a `Warning` header.** `Warning` is deprecated (RFC 9111 obsoleted RFC
7234's definition), so it is not a surface to put new contract on. Beyond that, headers are
routinely dropped by clients, proxies and logs, while SDK consumers treat this admin API as JSON
passthrough — a body field reaches every one of them with no SDK change, and gives the console one
concept to render rather than two.

**Why a response decoration and not a field on `RouteTable`.** `installed` is a property of the
**tenant**, not of the table. On the shared type it would enter the state machine's stored bytes
and the *request* body, where a client could assert itself installed. It is therefore a
serialize-only view (`RouteTableView`, `serde(flatten)`) over the stored table, and a `PUT` body
carrying `installed` is ignored on parse. `RouteTable` sets no `deny_unknown_fields`, so the read
body stays what its contract calls it — a config document a client `PUT`s back verbatim.

**Scope: the two whole-table endpoints, and deliberately not `DELETE /front-door/routes/{id}`.**
That route answers a single `Route` — the row it removed — not a table, so there is no table body
to decorate and no shape the field would fit. It also removes inert config rather than creating
it, which is the direction this entry is not about: the trap is being told a write will dispatch
when it cannot, and a delete promises nothing. The asymmetry is intended, not an oversight.

*Rejected:* refusing a non-default-tenant write with a `400`. The routes are stored and read back
per tenant **on purpose**, so a tenant sees what it wrote and the data survives whenever the front
door does grow a tenant dimension. Refusing would discard a deliberate property to close a
signalling gap.

*Rejected:* leaving it to documentation alone. The contract now says it too, and that half was
never in question — but the fact is derivable per-request and cheap, and a caller acting on a
`200` is not reading the spec at that moment.

**Amendment (D-71, 2026-09-06, #545):** `GET /front-door/route-hits` no longer exists — per-route
dispatch counters were removed with RFC-007 §3.2, the request log being the answer to "is this
route taking traffic". `installed` is therefore published by `GET` and `PUT /front-door/routes`
**only**, still derived from `routes_installed_for` through the one shared render helper, and the
console derives the whole not-installed treatment from the table read alone. The sentence above
comparing the word to what `route-hits` "already means by it" describes a second source that is
gone; the rule it stated — one definition, no drift between write, read and compiler — is
unchanged and now has fewer places to drift between.

**Superseded by D-73 (2026-09-07, #550).** The rule this entry existed to signal is gone with its
subject. `routes_installed_for` had exactly one input — the tenant — and there are no tenants, so
there is one fleet-wide route table and every stored route is compiled into the front door.
`installed` would be a constant `true`: a field that says nothing, on two endpoints, forever. It
is removed along with `RouteTableView`, `routes_installed_for` and the console's not-installed
banner, rank muting and "why" text.

The entry is worth reading anyway, and D-71 cites it for this: `installed` existed **because
tenancy reached the router**. A boundary added to the admin plane leaked into which routes a
data-plane request could reach, and the fix was a body field explaining that a `200` did not mean
what a `200` means. That is the strongest single argument for removing tenancy rather than
freezing it.

### D-69 — A space-scoped stub is replicated config; a space teardown deletes it fleet-wide

- **Status:** amended
- **Decided:** 2026-09-01
- **Refines:** D-5
- **Amends:** docs/api/openapi-ee.yaml (`addSpaceStub`), docs/architecture/11-upstream-boundary.md
- **Implemented by:** #537 (EE), rift#1012 (the #336 shape seam)
- **Code:** crates/rift-cluster-server/src/admin_front.rs, crates/rift-cluster/src/control.rs, crates/rift-cluster/src/raft/store.rs

`POST /imposters/{port}/spaces/{flowId}/stubs` answered `201` for a stub that existed on exactly
one node and was deleted by the next config reconcile. The classifier deliberately excluded the
three-segment shape, so the write was reverse-proxied to the receiving node's engine and never
became a `ControlOp` — absent from `sm_configs`, and therefore absent from the desired set that
`apply_config`'s `reconcile_stubs` renders each node's imposters from. Any committed op emitting
`EngineAction::Sync` — creating or deleting **any** imposter, anywhere in the fleet — re-rendered
the port and removed the stub as stale. D-5's "a replicated write never resets an untouched
imposter's runtime state" held; a space stub was not runtime state but *config the replicated layer
had never learned about*, so the diff was correct to drop it.

The asymmetry is what made it worth fixing rather than documenting: `GET /imposters/{port}/spaces`
is explicitly fleet-wide, and flow-state KV replicates, so the surface read as clustered in every
direction except the one that silently was not.

**The rule.** The `POST` terminates as an ordinary `ControlOp::PatchStubs` carrying
`StubEdit::Add`, with `space` set from the **path** (a `space` in the body is ignored, exactly as
upstream's handler did it). No new replicated shape was needed: a space stub is already an
imposter-config stub distinguished only by `Stub::space`. Reads on the shape stay proxied — correct
once the data replicates, because every node renders the same space from the same committed config.

**And the inverse, which replication creates.** `DELETE /imposters/{port}/spaces/{flow}` proxied
its teardown to the local engine and committed only `JournalClearGen`. Once the stubs replicate,
that would remove them from one engine and let the next `Sync` **resurrect them fleet-wide** — a
worse failure than the one being fixed. The teardown therefore also commits
`StubEdit::DeleteBySpace`. It is **set-addressed and idempotent**: a space stub need not carry an
`id`, so `DeleteById` cannot express this at all, and zero matches is success — a space holding
flow state but no stubs is ordinary, and erroring would turn an everyday teardown into a `500`.
That is the deliberate difference from the by-id steps, which address one named thing the caller
asserted exists.

**Validation parity is inherited, not re-implemented.** `--allowInjection` gating
(`op_uses_script_surface`) and `file:`/`ref:` script resolution (`resolve_op_scripts`) are already
generic over `ControlOp::PatchStubs`, so terminating gains both with no new code. The one gate that
did *not* survive is upstream's `reject_if_not_a_stub` (#336), which lived in the handler the
request no longer reaches — and it cannot be dropped: `Stub` deserializes through `StubRaw` where
every field is `#[serde(default)]` and unknown keys are discarded, so any JSON object parses, and
an object of only unrecognised keys becomes the vacuous stub that matches everything in its space.

*Rejected:* re-implementing that guard in EE. It would put a second copy of `STUB_FIELD_NAMES` in
another crate, which goes stale the moment upstream adds a stub field and fails the wrong way — a
legitimate stub answered `400`, silently. That is the same two-definitions-of-one-rule defect D-68
had just removed from the front door. rift#1012 exposes the decision instead, as
`not_a_stub_reason`, returning the reason rather than a `Response` so the rule stays upstream while
the rendering stays with whoever answers.

*Rejected:* refusing the write under `--cluster` with a `501`/`409`. Honest, and it cannot lose
data, but it removes a working single-node feature from clustered fleets to fix a durability gap
that turned out to be cheap to close properly.

*Rejected:* folding the stub delete into the existing `JournalClearGen` op. One op, atomic, no new
variant — but it silently changes what an already-committed `JournalClearGen` entry means, so a log
replay would start deleting stubs it never deleted when it was written.

**Amendment (D-74, 2026-09-08, #552):** `ControlOp::JournalClearGen` no longer exists — it raised a
*replicated* clear generation that only the merge-on-read consulted, and there is no merge. The
teardown commits `StubEdit::DeleteBySpace` alone; the space's recorded requests are cleared by
upstream's own `teardown_space`, on the node that took the request
(`crates/rift-cluster-server/src/admin_front.rs`). Both mentions of the op above are history. The
rejected alternative is retained because its reasoning — an op's meaning must not change under a
replay of entries already written — is the general rule, and it is why the delete got a variant of
its own.


### ~~D-70 — The console reads a structural claim from the cheapest source that carries it; absence from every source is still unknown~~

- **Status:** superseded
- **Decided:** 2026-09-01
- **Superseded by:** D-71
- **Refines:** D-68
- **Amends:** docs/design/console/README.md
- **Implemented by:** #539
- **Code:** web/src/screens/Routes.tsx, web/src/app/queries.ts

Superseded by D-71 (#545): with the route-hits endpoint removed there is one source for
`installed` and nothing to prefer between. The half of this entry that survives — a structural
claim requires a source that *positively* reported it, and a body that omitted the flag is unknown,
never `false` — is now stated in D-68's amendment and pinned there. Retained for history.

D-68 put `installed` on both `/front-door/routes` and `/front-door/route-hits`, derived from one
server function so the two cannot disagree. The console went on reading it only from `route-hits`
— which is a **cluster-wide fan-out**, while the route-table read beside it is a local read of
replicated state. So the entire not-installed treatment (banner, muted ranks, "not installed" in
the why column, stored-order rows) was gated on the slowest and most failure-prone of the two
queries, and disappeared exactly when an operator was on that screen asking why nothing dispatches.

**The rule.** Where one fact is published by more than one endpoint, the console derives it from
the cheapest and most available of them, and falls back to the others. Preference is settled by
*availability*, never by adjudicating truth: this rule is only ever applied to facts a single
server-side definition guarantees cannot differ between their sources. A screen that had to decide
which of two disagreeing endpoints to believe would have a contract problem, not a rendering
problem.

**Unknown does not weaken as sources are added.** The #369 bound-versus-unknown discipline holds
across all of them at once: a confident structural claim ("these routes can never take a request")
requires a source that *positively* reported it. Neither answering is still unknown — never a
majority of silence, and never a default. Concretely `(table ?? hits) === false`, so a body that
omitted the flag reads exactly as a read that never completed, which is what a pre-D-68 node during
a rolling upgrade looks like from the console.

**Corollary: a stronger fact outranks a missing weaker one.** The Hits cell reports "not installed"
*before* it reports the unavailable dash. A failed fan-out leaves the count unknown, but the table
read has already established that the route can take no dispatch at all — so the dash would hide a
fact the console holds, in the one state this entry exists to fix.

*Rejected:* requiring both sources to agree before rendering. It converts an availability question
into a consensus one and reintroduces the original bug — the banner would again be hostage to the
fan-out, now in every state rather than only the failing ones.

*Rejected:* folding an absent flag to `false` so the banner is never missed. That trades a missing
true statement for a confident false one, on the screen whose whole design premise is that a zero
and an unknown are different claims.

---

### D-71 — The cluster is the distributed core: membership, replicated configuration and the router; everything else is Rift's own or removed

- **Status:** active
- **Decided:** 2026-09-06 · RFC-007 · #544
- **Implemented by:** #556, #557, #558, #559, #560, #562, #563, #564, #566, #567, #568, #569, #570, #571, #572, #573, #577
- **Code:** crates/rift-cluster/src/control.rs, crates/rift-cluster-server/src/admin_front.rs, crates/rift-cluster/src/raft/node.rs, deploy/compose/smoke.sh

RiftCluster is **a replicated fleet of Rift nodes that forms and heals itself, replicates
imposters, stubs and the route table through Raft, routes a request arriving at any node to the
local imposter the route table names, and keeps a stateful mock's state cluster-wide without an
external store.** That, the admin API and console to add and manage
imposters and stubs (with a one-shot OpenAPI import), the probes and fleet reads, and the
deployment path are the whole product. RFC-007 §3 draws the boundary; §2 is the measurement
behind it.

**Removed, not flagged off.** Tenancy and RBAC, the audit projection and its exporter, the MCP
server, the operator metrics product — the fleet gauges, dashboards, recording and alert rules,
the observability overlay and the CI lanes that read them — per-route hit counters, tracking
sources and datasets and stored specs with the blob store that carried them, and the fleet journal
merge. The `rift_cluster_*` families a chaos scenario reads to pin a core claim stayed, as
correctness instrumentation rather than an operator surface
(`crates/rift-cluster/src/metrics.rs`).

Each removal is a child of #544, and each child marks the decisions it retires `superseded` in the
same PR — by **that child's own decision** (D-72 for #549, D-73 for #550, D-74 for #552), not by
this entry, which records the scope and lists the children. The one exception is D-70, which #545
retired without registering a decision of its own and which therefore names D-71 directly.
RFC-007 Appendix A is the full list.

**The flow-state tier stays.** Owner-authoritative scenarios and flow KV on the HRW ring, fencing,
the durable flow shard, the sequencer, proxyOnce claims and spaces (D-3, D-7–D-10, D-13, D-17,
D-20, D-36, D-40, D-47, D-57, D-63, D-65, D-66) remain active: a stateful mock behaving as one
mock across the fleet is part of what "behave like one" means, and the cluster's tier does it
with no external store. The request journal is per node, as in open-source Rift.

**Amended 2026-09-06, before any child landed.** As first written this entry also removed the
flow-state tier in favour of upstream's `FlowStore` with the Redis backend (#551). Withdrawn the
same day by the project owner: a Redis in the request path is precisely the external dependency
the fleet exists to avoid, and the cluster's tier is the stronger of the two. RFC-007 v1.1
records the same change.

**What landed, by child.** RFC-007 §9 carries the same mapping with the surfaces named.

| Child issue | Surface | PR |
|---|---|---|
| #555 | In-repo core smoke check | #557 |
| #545 | Route hits | #559 |
| #546 | Audit log, export loop, sink | #563 |
| #547 | MCP server | #560 |
| #548 | Cluster metric families and the observability pack | #562 |
| #549 | Tracking sources, datasets, stored specs, blob store (D-72) | #564 |
| #550 | Tenancy, RBAC, principals (D-73) | #566 |
| ~~#551~~ | ~~Clustered flow state~~ — **withdrawn**, the tier stays (see the amendment above) | — |
| #552 | Fleet journal merge (D-74) | #568 |
| #553 | Console trimmed | #569 (open) |
| #554 | Design docs retired; the router named | #577 |

The RFC itself landed as #556 and was amended to v1.1 by #558. Five PRs outside the child list
belong to the epic because they fix defects the removals exposed or the lanes they broke: #567
(issue #565 — a committed imposter delete left the port's flow state behind), #570 (the compose
verification lane, dropped when the observability overlay went), #571 (the `--imposters`
bootstrap keyed on a canonical digest, after the spec surface was reduced to one shot), #572 (the
gateway leg strips every admin credential — found reviewing #566) and #573 (the cold-start sweep
only clears ports the sync itself dropped — found reviewing #567). RFC-007 §9 lists the same five.

**All merged.** The last of them, #577, carried this entry itself; the numbers above are final.

**Verified live, before and after.** No removal merges until the surface being removed has been
driven on a running fleet and recorded, and every kept surface has been re-driven afterwards
(RFC-007 §5). The baseline on the day of decision was 57/57 on the compose fleet;
`deploy/compose/smoke.sh` (#557) is that protocol's in-repo half and is the check every child ran
before and after.

*Rejected:* freezing the peripheral surfaces in place. Frozen code still compiles, still tests,
still cites decisions, and still leaks — D-68 exists because tenancy reached the router.

*Rejected:* sharding imposters across nodes. The fleet stays replicated (D-20's core claim); the
router routes to the local imposter, never across nodes.

---

### D-72 — Imposter import is one-shot: `--imposters` and `POST /specs/compile` become ordinary `PutImposter` ops; the cluster retains no source, spec or dataset

- **Status:** amended
- **Decided:** 2026-09-07 · RFC-007 §3.2 · #549
- **Supersedes:** D-18, D-19, D-23, D-29, D-30, D-31, D-34, D-48, D-49, D-50, D-51, D-52, D-53, D-55, D-56
- **Amends:** RFC-004 §1, RFC-004 §3.4, RFC-004 §6
- **Implemented by:** #549
- **Code:** crates/rift-cluster-server/src/admin_front.rs, crates/rift-cluster-server/src/compose.rs

An imposter reaches the fleet through **one** path: `ControlOp::PutImposter` on the replicated
log. Compiling an OpenAPI document and reading a `--imposters` URI are both *ways of producing
one*, not second config planes with their own records, schedulers and replication tier.

**What is gone.** Tracking imposter sources — the `git+https:`/`git+file:`/`s3:`/`registry:`
providers, the `auth_ref` credential resolver, the leader-only poll scheduler, drift policy, the
`sm_sources` table and the whole `/admin/sources*` surface. Datasets — `sm_datasets`, the CSV
spool, the `_rift.dataset` binding compile-down, the three dataset quotas and
`/admin/tenants/{id}/datasets*`. Stored specs — `sm_specs`, the drift diff, edit-time validation
and its `Rift-Spec-Warnings` header, and `/specs`, `/specs/{id}`, `/specs/{id}/compile`,
`/specs/{id}/deploy`. And the content-addressed blob store beneath all of it: the transport store,
the fan-out, the sideload/strip, fetch-on-apply, tombstones and GC, the snapshot manifest, and
`/internal/v1/blob/{digest}`. Nine `ControlOp` variants leave with them.

**What replaces the two things anyone actually used.**

*`POST /specs/compile?port=<u16>[&name=<text>]`* takes an OpenAPI 3.0 document (JSON or YAML,
≤ `rift_cluster_spec::MAX_SPEC_BYTES` = 4 MiB) and answers the compiled imposter JSON plus the
operation index — **and stores nothing**: no record, no op, no table read. The caller `PUT
/imposters` the `imposter` field, which is the same admission gate every other config passes.
`rift-cluster-spec` itself is untouched (RFC-004 §3.1–§3.3 stay live).

*`--imposters <uri>` under `--cluster`* is a one-shot bootstrap. At startup each URI is resolved
through upstream's own `SourceRegistry` (U-12: `file:` and `http(s):`, the only schemes left), the
documents parsed by upstream's config loader, and each imposter submitted as a plain
`PutImposter`. The flag is still `take()`n from the CLI before `ServerBuilder` sees it, for the
reason it always was: upstream's `start()` would create the imposters in this node's manager,
outside the replicated log, and the reconciler would then delete them — the operator watches their
imposters appear and vanish with no error anywhere.

**`port` is required on the compile endpoint**, unlike the store-backed compile it replaces. That
one could fall back to the spec's single bound port because a stored spec had bindings; this one
has no record to infer from, and a portless compiled imposter cannot be `PUT` under `--cluster` at
all (an auto-assigned port cannot replicate). Answering with a document the very next call refuses
would be a worse default than asking.

**Idempotence is in the `op_id`, not in a record.** Each bootstrap imposter is submitted under
`Uuid::new_v5(NAMESPACE_URL, "rift-cluster/bootstrap-imposter\n{uri}\n{port}\n{digest}")`, where
`digest` is the sha256 of the fetched document's canonical config set. A restart, or a second node
booting against the same file, re-derives the same `op_id` and collapses in the state machine's
dedup table; an *edited* document hashes differently and applies. That is what the source record's
`applied_digest` short circuit used to buy, obtained without a replicated record — and it is why
the digest is over the whole document rather than per imposter: an imposter removed from the
document must change the identity of what the document declares.

**Amendment (2026-09-08, retrospective review of #564):** four clarifications, none of which
changes what is replicated.

*The digest is over a canonical rendering, not over `ImposterConfig`'s own serialization.* As
shipped, `digest` was `sha256(serde_json::to_vec(configs))`, and `ImposterConfig` transitively
holds `std::collections::HashMap`s (a response's `headers`, `_rift.scripts`) that upstream
serializes in raw iteration order — a fresh `RandomState` per map, so any document with two or
more response headers could hash differently on any read (with exactly two the orderings coincide
about half the time, and agreement collapses from there), restarts minted new `op_id`s, and the
idempotence described above never reliably engaged. The digest is now over the config set
round-tripped through `serde_json::Value`, whose map is a `BTreeMap` (`preserve_order` is off
across the workspace; a unit test on the helper fails if that changes). Raw document bytes remain
the wrong input for the reason already given — two spellings of one document, JSON where there was
YAML, must dedup — but that dedup holds **only within one URI**: the `op_id` hashes the URI
verbatim, so `file:mocks.json` and `file:mocks.yaml` are two documents to the dedup table however
equal their digests, as is one file mounted at two paths on two nodes. The URI is deliberately not
normalised; two URIs are two operator intentions.

*The whole-document digest has a blast radius, and it is accepted rather than overlooked.* Editing
one imposter changes the digest every imposter in that document is keyed on, so all of them
re-apply — and a re-applied `PutImposter` is a delete-then-recreate that also drops that port's
`proxy_recorded` markers (#226), so a stub already recorded there can be recorded again. Keeping
per-imposter digests would avoid it and give up the property above (a removed imposter would leave
the others' identities unchanged, hiding that the document moved at all); a bootstrap document is
edited at deploy time, so the trade goes this way.

*The bootstrap waits, bounded, for a leader — immediately before its first submit, not on entry.*
`RaftNode::submit` answers `Unavailable` at once when no leader is visible, and a joiner — or any
node in a fleet cold-starting together — is composed inside exactly that window. The bootstrap
waits up to `BOOTSTRAP_LEADER_DEADLINE` (30 s, the seed-join budget) for one; a fleet that never
elects still fails the start, by the deadline and saying so, and a genuine refusal is still fatal.
The *placement* is part of the decision: everything decidable on this node alone — a retired URI
scheme, an unreadable document, an `intercept` or `routes` block, a portless imposter — is refused
before the wait, so a leaderless fleet cannot mask a plain misconfiguration and cost the operator a
second deploy cycle to hear about it.

*A `routes` block is refused, like `intercept`, not warned about.* Both leave the operator with
something they configured and never got; a start-up warning is not a channel an operator reads
before sending traffic at a front door whose table is empty. Same message shape, pointing at
`PUT /front-door/routes`.

*And one thing the bootstrap deliberately does not do:* it never removes an imposter that was
dropped from the document. A one-shot import has no baseline to diff against — that baseline is
precisely the source record this decision removed — so "absent from this document" is
indistinguishable from "created through the admin API by someone else". The import is additive;
deletion is an admin action (`DELETE /imposters/{port}`).

**Authorized as `imposter.write`.** A compile is the first half of an imposter write and the only
reason to call it is to make one; putting it below that would let a reader have the fleet do a
writer's work. `Action::{SourceRead, SpecRead, SpecWrite, SpecDelete, DatasetRead, DatasetWrite,
DatasetDelete}` are removed rather than renamed.

**No warning channel.** The compiler's warnings *are* its refusals — an unsupported version, an
external `$ref`, a parse failure, its own self-check — and they render as `400`. A `200` therefore
means the output passed the contract it just emitted, and an always-empty `warnings: []` was
rejected as a field that says nothing.

**This is a deliberate log-format break.** `ControlOp` is externally-tagged `serde_json` with no
envelope version and no catch-all arm, so a node replaying a log holding any of the nine removed
variants fails to start rather than skipping them. Pre-release that is the right trade and is why
the removal is clean; a fleet upgrading across this commit starts from a fresh
`cluster-state-dir`. D-49's compatibility reasoning is what makes this a decision rather than an
oversight.

*Rejected:* keeping the `/specs` store and removing only the tracking sources. The store is what
the blob tier existed for — eleven decisions about replication, GC, snapshot manifests and
rolling-upgrade capability, all in service of holding documents the caller already has.

*Rejected:* keeping `--imposters` as sugar for a pinned source record. It preserves the source
table, the puller and the provider registry to serve a flag that is read once at boot.

*Rejected:* a `warnings` array on the compile response for symmetry with the retired
`Rift-Spec-Warnings` header. The header reported a *deployed* imposter drifting from the spec that
generated it; with nothing stored there is no baseline to drift from, so the field would be
provably empty.

---

### D-73 — One tenant, one credential: the admin plane is closed by `--api-key` or open; the console exchanges that key for a cookie; nothing in the fleet is tenant-scoped

- **Status:** active
- **Decided:** 2026-09-07 · RFC-007 §3.2 · #550
- **Supersedes:** D-44, D-45, D-46, D-68
- **Amends:** RFC-006 §5.3
- **Implemented by:** #550
- **Code:** crates/rift-cluster-server/src/admin_front.rs, crates/rift-cluster-server/src/session.rs, crates/rift-cluster-server/src/compose.rs, crates/rift-cluster/src/control.rs, crates/rift-cluster/src/raft/store.rs

A RiftCluster fleet has **one administrator**. Authentication is open-source Rift's own
`--api-key` (`MB_APIKEY`), sent as the raw `Authorization` value and compared constant-time:
**set ⇒ the whole admin plane is closed to that key; unset ⇒ open, exactly as upstream behaves.**
There is no principal, no role, no binding, no quota and no tenant anywhere in the system, and
therefore no per-resource authorization: everyone who can administer the fleet can administer all
of it. Isolation between teams is a deployment (two fleets), not a feature (RFC-007 §3.3).

`/healthz` and `/readyz` on the probe listener stay open — a liveness probe that needs a
credential is a liveness probe that fails for the wrong reason — and so does the `/__rift/*`
data-plane gateway, which upstream exempts from its own key gate for the reason the key must not
be *injected* onto that leg either: it reaches the imposter, where an `Authorization` header
would land in its predicates and its recorded request log.

**`POST /session` survives** (RFC-007 §8 Q1's default, taken). It exchanges the API key for the
`rift_session` cookie so the browser does not keep the key after login: the token format, the `kr`
key-revision kill switch, the constant-time compare, the 8-hour TTL, `SM_SESSION_KEY_TABLE`,
`ControlOp::SessionKeyPut` and the CSRF gate are all unchanged. Two things about it did change.
Its payload's `pid` is now a fixed subject (`"admin"`), and `session::verify` answers
`Result<(), _>` — there is no identity for a session to resolve *to*, and a cookie that carried
authorization data would be a second source of truth for a decision the key already settles. And
**this supersedes D-46**, which refused the legacy `--api-key` a session because the synthetic
identity it named had no principal row behind it: with one credential there is no other key, so
the key *is* what the exchange accepts. Rotating `SessionKeyPut` remains the only revocation.

**Keys lose their tenant component — a true removal, not a `default` shim.** `sm_configs` is keyed
by `u16`, `sm_routes` by route id, `sm_routes_revision` is a one-row table beside
`sm_session_key`/`sm_fleet_name`, `sm_journal_gens` is `(u16, &str)` and `sm_proxy_recorded` is
`(u16, &str)`; `sm_tenants`, `sm_principals` and `sm_bindings` are gone, and `SnapshotPayload`
shrinks with them (`routes_revisions` becomes a scalar `Option<u64>`). Every `ControlOp` variant
drops its `tenant` field and the six tenancy/RBAC variants leave entirely. **This is a deliberate
fleet-wide log-format break** — the same one #549 (D-72) declared, and for the same reason: a
changed field set changes the encoding of ops that still exist, so a fleet upgrading across this
commit starts from a fresh `--cluster-state-dir`.

**`Rift-Cluster-Revision` names its subject.** It was `default:<port>@<rev>` and
`default@<rev>` — a hardcoded segment the write path emitted and the `If-Match` parser demanded
back. It is now `<port>@<rev>` for an imposter and `routes@<rev>` for the route table. A bare
revision integer is still accepted. Keeping the token self-describing rather than reducing it to a
number is what stops an imposter's revision being fed back as a precondition on the route table.

**D-68 dissolves rather than being amended again.** Its whole subject was that only the default
tenant's routes were compiled into the shared front door, so a `200` on a non-default tenant's
`PUT /front-door/routes` meant *stored* and not *dispatching*, and `installed` was the field that
said which. With one fleet-wide table every stored route is installed: the field would be a
constant `true`, which is a field that says nothing. `installed`, `RouteTableView`,
`routes_installed_for` and the console's not-installed treatment all go.

**`ContextScope` loses `Tenant`.** `Imposter` (`i<port>:`) and `Fleet` (`f:`) remain; the
`t<tenant>:` and `t??:` prefixes, `tenant_of`, `by_tenant` and `FlowNet`'s tenant fan-out are
gone. A config declaring `"contextScope": "tenant"` is **refused at admission with a message
naming #550**, never silently aliased — folding it into either survivor would change which
imposters share flow state, which is the one thing the key decides. Relatedly, a `fleet`-scoped
space *listing* is now served rather than refused: it was `FleetAdmin`-only because `f:` carries
no tenant component and enumerating it handed one tenant another's flow ids, and there is no
longer a boundary for it to cross.

**The loopback gate is upstream's own again.** `compose` no longer clears `cli.oss.api_key`, so
the loopback admin listener runs open-source Rift's `api_key_matches`, and the U-9
`.admin_authorizer(EeAuthorizer)` seam is not installed — one comparison, implemented once, in
upstream. That has a sharp consequence the front must handle: **a cookie-authenticated request
carries no `Authorization` at all**, and upstream's raw compare will refuse it. So `admin_front`
authenticates first and then *injects the configured key* on both internal legs — `proxy` (the
proxied reads) and `fetch` (the render re-read after a committed write). Forwarding the session
token instead, which is what the code did before #550, now fails: the write commits and only the
render is refused, so the client is told `401` about a change that actually landed. Pinned by
`fleet_session.rs`'s `the_api_key_mints_a_session_and_the_cookie_is_accepted_on_every_node`.

**Quotas: none survive.** `max_flow_entries` was never enforced anywhere, and
`max_stubs_per_imposter` is a property of an imposter rather than of a tenant — if a stub ceiling
is wanted it belongs upstream, in the engine that owns the stub list, not in a cluster-only
record. `Quotas`, `TenantConfigUsage`, `quotas_for` and `quota_refusal_for_config` are removed
rather than re-homed under a fleet-wide default, because a ceiling nobody configured and nothing
reports is indistinguishable from no ceiling.

*Rejected:* keeping a `default` tenant shim — the field with one legal value. It preserves every
key shape, every `TenantId` construction and every `tenant.as_str()` call site in exchange for
nothing: the log format breaks anyway (the removed variants see to that), so the compatibility
the shim would buy does not exist, and what remains is a thousand mentions of a concept the system
no longer has. D-68 is the evidence for how that ends — tenancy reached the router, and the field
that documented the leak outlived three attempts to explain it.

*Rejected:* keeping any quota. A per-fleet `max_imposters` is a different feature from a
per-tenant one — there is no other tenant to protect capacity from — and shipping the record
without the tenant would leave a limit whose only effect is to refuse the operator their own
fleet.

*Rejected:* dropping `POST /session` and having the console send the key on every request. It is
one fewer route and one fewer replicated record, but it puts a long-lived credential in the page
for the whole session, and rotation would then have no kill switch short of changing the flag on
every node and restarting. The cookie is the only revocation this design has; removing it would
leave none.

**Amendment (2026-09-08, the #566 retrospective fix):** three operational facts the entry above
left implicit or got wrong. *First*, "a deliberate fleet-wide log-format break" is true, but not
for the reason given: dropping the `tenant` field does **not** make a surviving op undecodable —
`ControlOp` sets no `deny_unknown_fields`, so an old entry's extra key is ignored. What refuses an
old state directory is redb's per-table type check (`TableTypeMismatch` on open, because every
key shape lost its tenant component), and that guard exists on disk only; the wire has none. So
**a mixed-version fleet straddling this commit is unsupported** — nodes on either side would
exchange ops that decode on both and apply against different key shapes. Upgrade every node at
once, from a fresh `--cluster-state-dir`. *Second*, the tenancy flags are gone, not deprecated:
`--cluster-legacy-key-is-fleet-admin` (RFC-002's one-release bridge) is no longer parsed, so a
unit file or manifest that still passes it is **startup-breaking** — clap refuses the unknown flag
and the node does not start. *Third*, the session cookie is `Secure`, so **console login works
only over HTTPS** (or a `localhost` origin, the browser's one exception): over plain HTTP `POST
/session` answers `200`, the browser discards the cookie, and every request after it is `401`.
The raw `Authorization` key path is unaffected. Also in that fix: the `rift_session` cookie is
stripped from the `/__rift/*` gateway leg for the same reason the key is never injected there —
it is an admin credential the imposter must not see — and `DELETE /session` sits behind the CSRF
header like every other cookie-borne mutation.

**Amendment, continued (2026-09-09, that fix's review):** two more facts about which credentials
reach a mock. *Fourth*, "the key is never injected on the gateway leg" was never the whole rule,
because a caller can present the key itself: upstream exempts `/__rift/*` from its own key gate,
so an `Authorization` header the *client* sent — a CI script or `curl` alias that stamps the
fleet key onto every rift call — was forwarded verbatim into the imposter's `savedRequests`, its
predicates, and any proxying stub's outbound request. The gateway leg now **drops an
`Authorization` whose value is the configured key** (constant-time compare, so an unauthenticated
surface does not become a timing oracle for it) and forwards every other bearer untouched, since
an app under test legitimately authenticates to its own mock. Two behaviour notes follow: a
gateway request presenting the fleet key now reaches the imposter with no `Authorization` at all,
and — from the same fix's `bearer_verdict` change — a *present but empty or unreadable*
`Authorization` on an admin route is a refusal (`401`) rather than an absence that lets a cookie
on the same request authenticate it. No first-party client is affected; the console is
cookie-only. *Fifth*, the cookie strip has a reach the operator must supply the rest of:
**cookies are host-scoped, never port-scoped**, and the strip only runs on requests that pass
through the admin front. An imposter bound on its own port is a listener the front never sees, so
once the admin origin is HTTPS an imposter declared `"protocol": "https"` on another port of the
same hostname satisfies both `Secure` and `SameSite=Strict` and is handed a live session token.
No cookie attribute fixes this — `Domain` only widens scope and there is no `Port` attribute — so
it is a deployment rule: **the hostname serving the console must not also serve HTTPS imposters**
(`docs/architecture/10-operations.md` §`POST /session`).

### D-74 — Verification is per node: the request journal is upstream's own, `numberOfRequests` is the answering node's count, and `Rift-Cluster-Partial` is stamped only on reads that genuinely fan out

- **Status:** amended
- **Decided:** 2026-09-08 · RFC-007 §3.2 · #552
- **Supersedes:** D-32, D-37, D-38, D-39
- **Amends:** docs/architecture/05-read-path.md, RFC-007 §2.1, RFC-006 §4, RFC-001 §10, RFC-001 §12
- **Implemented by:** #552
- **Code:** crates/rift-cluster-server/src/admin_front.rs, crates/rift-cluster-server/src/openapi.rs, crates/rift-cluster/src/decorate.rs, crates/rift-cluster/src/control.rs, crates/rift-cluster/src/raft/store.rs, crates/rift-cluster/src/stores/mod.rs

A RiftCluster node's recorded requests are **upstream Rift's own**, per node.
`GET /imposters/:port/requests` — and its `savedRequests` spelling, its `?since=` cursor form and
its `DELETE` — are ordinary proxied routes to the local engine, answering for the node the caller
reached, with upstream's own scalar `x-rift-next-index` and `x-rift-truncated` and upstream's
Mountebank semantics unchanged. A test that needs fleet-wide verification pins a node or reads all
of them and adds up (RFC-007 §3.3).

**What leaves.** The whole fleet journal: `stores/{journal,journal_net,journal_seq}.rs`, the
per-writer `(node_id, seq, clear_gen)` shards and their `evicted_below_seq` watermarks, the k-way
merge-on-read, the anti-entropy pull and its replica cache, the `/_cluster/journal/{since,counts}`
RPCs, `ControlOp::JournalClearGen` with the `sm_journal_gens` table and the `journal_gens`
snapshot field, the vector cursor and its `JournalCursor`/`FleetCursor` codecs, the merged SSE
tail on `.../savedRequests/stream`, the fleet-wide `GET /admin/requests` and its stream with the
declared coverage set, `--cluster-fleet-journal-port-cap`, and
`rift_cluster_journal_partial_reads_total`. The console's Requests screen loses the merge and
names the node it read from instead.

**`numberOfRequests` is a contract change, stated as one.** On `GET /imposters` and
`GET /imposters/{port}` it is now **the answering node's own count**. It was a fleet sum: upstream
answered its local counter and the front rewrote it by fanning out to every peer. A client that
wants a fleet total reads every node and sums — and then knows which nodes it counted, which the
old answer could not tell it whenever a peer missed the budget and the sum silently became a
floor.

**`Rift-Cluster-Partial` narrows rather than leaving.** It stays on the two reads that stamp it —
`/_fleet/members` and `/_fleet/health` — and goes from every journal site. A requests read has no
peer to be partial about: it either answers for the node the caller reached, or it fails. The
contract now *declares* the header on those two operations, which it never did: every `$ref` to it
was on a journal route, so removing them would have left the component defined and referenced by
nothing. The spaces listing fans out too and keeps reporting its own incompleteness in the body
(`partial`, beside `unavailable`) — an enumeration refused by policy and one shortened by a slow
peer are different facts, and a boolean header cannot tell them apart.
`HEADER_NEXT_INDEX`/`HEADER_TRUNCATED` leave `decorate.rs` entirely; the cursor headers on the
wire are upstream's, emitted by upstream.

**A space teardown still clears that space's requests.** `DELETE /imposters/:port/spaces/:flow`
proxies to the local engine, and upstream's own `teardown_space` calls
`RequestJournal::clear_flow(port, space)` on the way through — so the entries go on the node that
took the teardown, which is the node whose journal held them. The `JournalClearGen { space }` half
this front used to commit alongside existed only to raise a *replicated* generation the merge
consulted; with no merge there is nothing for it to be replicated for. The replicated **stub**
half (D-69) is untouched and still required.

**This is a fleet-wide log-format break**, the same one #549 (D-72) and #550 (D-73) declared and
for the same reason: removing a `ControlOp` variant makes an old log entry undecodable, so a fleet
upgrading across this commit starts from a fresh `--cluster-state-dir`. Snapshot *decoding* is
deliberately tolerant of the removed `journal_gens` field — `SnapshotPayload` sets no
`deny_unknown_fields`, so a snapshot built before this still installs and its extra key is
dropped.

**Why.** The journal was a second distributed system riding inside the first: per-writer shards, a
k-way merge by recorded timestamp, an anti-entropy loop, generation clears committed through Raft,
a vector cursor and a declared coverage set — about 4,000 source lines and 5,700 test lines, built
so a test assertion could be made against any node. That is a real problem. It is Rift's to solve,
in the engine, once, for every deployment shape — and while the cluster carried it, it doubled the
surface of every read: two code paths for `GET /imposters/{port}/requests`, two cursor vocabularies,
a `numberOfRequests` that meant something different depending on which node answered and how many
peers replied in time.

*Rejected:* keeping a replicated clear generation without the merged read. It is the cheapest half
to keep — one `ControlOp`, one small redb table — and it converges a `DELETE savedRequests` across
the fleet without any fan-out on the read path. But a clear generation is only observable through
a reader that consults it, and the only reader was the merge. Kept alone it is a counter that
commits, replicates, snapshots and is compared against nothing: a fleet-wide write whose effect no
API can show, which is a worse thing to own than either the whole subsystem or none of it.

*Rejected:* keeping the fleet-sum `numberOfRequests` decoration. It is the single most useful thing
the merge produced and the cheapest to keep — one fan-out over `/_cluster/journal/counts`, no
shards, no cursor. It stays rejected because a sum is only honest if every addend arrived: under a
slow peer it silently becomes a floor, and the `Rift-Cluster-Partial` bit that says so is a header
most clients never read. A per-node count is smaller and always exactly true, and a caller that
wants the total can compute it from `/_fleet/members` and know what it counted.

**Amendment (2026-09-08, #552 review — the one data-plane change the removal carries, stated so it
is not read as a regression, plus one behaviour that only looks like one):**

*(a) Retention, in both of its dimensions.* A shard used to hold
`(fleet_capacity / voters).max(min_shard_cap).max(1)` entries per port — `max(10_000 / N, 500)` —
so a three-voter fleet kept about 3,333 each and the merged view held about 10,000. The general
form matters above 20 voters, where the `MIN_SHARD_CAP = 500` floor takes over and the fleet's
total stops dividing. Each node now keeps upstream's own `MAX_RECORDED_REQUESTS = 10_000` per port
(`rift-mock-core/src/imposter/journal.rs`), so a three-node fleet retains up to three times as many
entries in total and each node's read is cut at its own cap.

The shard also had a **second** retention dimension that upstream's journal does not:
`DEFAULT_MAX_AGE = 600 s`, an age sweep that dropped entries older than ten minutes whether or not
the cap was near. Upstream's `LocalJournal` evicts by count alone (`record_indexed` pops the front
only at `MAX_RECORDED_REQUESTS`), so recorded requests are now unbounded in *time*: a long-lived
imposter holds its last 10,000 requests however old they are, where the shard would have held none
older than ten minutes. That, not the count, is the larger memory consequence.

*(b) A wholesale replace drops the port's recorded requests — and did so before this change too.*
Stated because it looks like a consequence of moving the journal back inside the imposter core, and
is not. `replace_imposter` is `delete_imposter_inner` + `create_imposter_staged` (`manager.rs`), so
the port does start on a fresh `LocalJournal` — but `delete_imposter_inner` already cleared an
*injected* journal on the way through (`if let Some(journal) = &self.request_journal {
journal.clear(port) }`), and the removed `ClusterJournal`'s `clear` emptied that port's entries and
zeroed its count. A replace therefore dropped the port's recorded requests on every node under the
merge as well. The mechanism moved; the behaviour did not.

Which writes replace is worth stating exactly, because "a config-changing write" is wrong in both
directions. `apply_config` replaces on an imposter-level field change *other than* `enabled` — an
`enabled`-only diff toggles in place and keeps the journal (upstream #817, `manager.rs`) — and also
on a **degenerate** stub diff, `StubReconcile::Degenerate`, which fires when
`changed_slots * 2 > states.len() + desired.len()` (`imposter/reconcile.rs`). Since every stub route
terminates on the front and is applied through `apply_config` on each node, a
`DELETE /imposters/{port}/stubs/0` against a one-stub imposter is degenerate and rebuilds the core.
Below that threshold the stub set is patched in place and the journal survives, which is what the
chaos reorder scenario's `numberOfRequests == 1` assertion pins
(`tests/cluster-chaos/tests/scenarios.rs`, the assertion closing `test_reconcile_reorder`):
a reorder that rebuilt the core would read `0`. The assertion is not stronger than it was before —
under the merge a rebuild zeroed the local shard on every node, so the counterfactual read `0` then
too — it is simply the pin that keeps the in-place path in place.

---

### D-75 — The router rename is prose only: the wire path `/front-door/routes`, the `--front-door` flag, `RIFT_FRONT_DOOR` and `x-rift-front-door` keep their names

- **Status:** active
- **Decided:** 2026-09-09 · RFC-007 §6 · #554
- **Refines:** D-71
- **Implemented by:** #554
- **Code:** crates/rift-cluster-server/src/admin_front.rs, crates/rift-cluster-server/src/openapi.rs, crates/rift-cluster/src/raft/store.rs, web/src/api/paths.ts, vendor/rift/crates/rift-http-proxy/src/server.rs

RFC-007 §6 renamed the feature. In the reduced system it is the cluster's only data-plane
contribution, "front door" said nothing about what it does, and so docs, console labels and CLI
help call it the **router**. The rename stops at the prose. Every name a client types or reads
stays: `GET`/`PUT /front-door/routes` and `DELETE /front-door/routes/{routeId}`, upstream's
`--front-door` flag and its `RIFT_FRONT_DOOR` environment alias
(`vendor/rift/crates/rift-http-proxy/src/server.rs:150`), and the `x-rift-front-door` response
header. The upstream module `rift_http_proxy::front_door` is not ours to rename in any case.

**Why this is a decision and not an omission.** A reader who meets "the router" in the guide and
`/front-door/routes` on the wire will ask which one is wrong, and the answer has to be findable.
It is deferred, not refused: renaming the path is a client migration, and the epic that reduced
the system is not the place to charge one. Sixteen files in this repo spell the path — nine that
serve or describe it (the terminating front, the state machine, the raft node, the composition,
the served contract and the OpenAPI document, the console's path table, its generated schema and
its query layer) and seven tests — and none of that is the cost. The cost is every caller outside
the repo, who would need a deprecation window, both spellings served through it, and a reason
better than a noun.

*Rejected:* renaming the path in #577. The epic's own rule is that a removal must not change
what a working client sees; a rename is the same promise broken from the other direction.

*Rejected:* leaving it unrecorded, as prose in RFC-007 §6. That is what #554 first did. A future
reader hunting for why the two names disagree looks in the register, which is where this project
says decisions live, and finds nothing — the same "decided in a thread, never written down" this
register exists to prevent.
