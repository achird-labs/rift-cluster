# ADR-001 — Embedded Raft control plane (openraft + redb)

| | |
|---|---|
| **Status** | Accepted (2026-07-21) |
| **Deciders** | Mohsen Zainalpour |
| **Supersedes** | RFC-001 v2 decisions D-1 (no consensus layer) and D-2 (chitchat gossip); rewrites §7.1, §7.2, §7.4 |
| **Tracking** | [rift-cluster#14](https://github.com/achird-labs/rift-cluster/issues/14); implementation across [#6](https://github.com/achird-labs/rift-cluster/issues/6), [#7](https://github.com/achird-labs/rift-cluster/issues/7), [#9](https://github.com/achird-labs/rift-cluster/issues/9) |
| **Depends on** | `openraft` 0.9.x, `redb` 4.x (both verified 2026-07-21) |
| **Amended** | 2026-08-24, [#436](https://github.com/achird-labs/rift-cluster/issues/436): the snapshot *payload* no longer lives in redb. The decision below stands for the log, the vote and the snapshot's metadata; only where the payload bytes are written changed. |

## Context

RFC-001 v2 designed the cluster control plane on **eventually-consistent gossip**:
membership by chitchat, ownership by rendezvous hashing over whatever roster a
node currently believed, and — because two nodes could transiently believe
different rosters — a stack of compensating machinery (ring epochs, a settle
delay before a new owner may serve, per-key ownership generations, version-vector
merges on heal). Every piece of that machinery existed to *manage* disagreement
about membership rather than remove it.

Four requirements were stated after v2 was written, and together they change what
the control plane must be:

- **R1** — a config change acknowledged on any node is servable from *every* node
  by the time the client receives its 2xx.
- **R2** — a flow-state change is visible to the very next request, whichever node
  serves it.
- **R3** — nothing is lost on a full-cluster restart: imposter configs *and* flow
  state.
- **R4** — an accepted admin request is never lost, even if the node handling it
  dies mid-flight.

R1 + R3 + R4 are a request for a **strongly consistent, durable control plane**.
The gap analysis (`RFC-001-correctness-and-tenancy-gap-analysis.md` in the design
vault) worked through delivering them on the gossip foundation and found it means
hand-rolling four protocols — a fan-out barrier, a persist-before-ack path, a
durable intent log, and an op-id dedup map — layered *on top of* the
settle-delay / generation / epoch machinery that only exists because gossip
membership is not agreed. That is building a worse version of consensus by hand,
on top of the scaffolding whose reason for existing consensus removes.

The founding constraint stands: **no mandatory external dependency — Rift clusters
itself.** So the control plane must arrive as an embedded library, not a sidecar
service.

## Decision

Adopt an **embedded Raft group** — [`openraft`](https://docs.rs/openraft), in
process, over the existing HMAC-authenticated cluster port — as the control plane
for **membership, imposter configs, the `enabled` bit, tenancy/RBAC records, and
the admin intent log**. Persist the Raft log, vote, and snapshot metadata — and
the durable flow-state tier of #16 — in an embedded ACID store,
[`redb`](https://docs.rs/redb). Both are pure Rust and safe for the static-musl /
`FROM scratch` builds; neither is a service.

**Flow state stays off consensus.** A quorum write per scenario transition at
20–40k RPS is an outage, not a design. Flow state keeps single-writer HRW
ownership + successor replication + a write-ahead tier (#16); it only *derives*
ownership from committed membership. Nothing consensus-shaped ever sits between a
request and its response.

Putting membership *itself* into the Raft log is the move that pays for
everything else: the roster becomes a linearizable value, so at any log index
every node computes byte-identical membership and therefore byte-identical
ownership. The settle delay, the generations, the epoch-mismatch retries are
**deleted, not mitigated**. The same log then carries configs, tenancy, and admin
intents, so R1 (a committed entry the fleet has applied), R3 (fsynced on a
majority before ack), and R4 (the log *is* the durable intent record) come from
one primitive instead of three bespoke protocols.

### Dependencies, pinned and verified (2026-07-21)

- **`openraft` 0.9.x** — latest stable; 0.10 is alpha and not used. Uses the split
  storage API `RaftLogStorage` + `RaftStateMachine` + `RaftSnapshotBuilder`
  (verified present in 0.9.21), not the deprecated single `RaftStorage`.
- **`redb` 4.x** — `Durability` has exactly two variants, `None` and `Immediate`
  (the `Eventual` mode assumed in early drafts was removed in redb 2.0). The
  control plane uses only `Immediate` — log and vote must fsync before ack.
  Consequence for #16: its `async` flow durability is **group commit** — batch N
  transitions into one `Immediate` commit per interval — not an `Eventual` mode.

## Consequences

### Gained

- **One mechanism for R1/R3/R4.** Append → replicate to majority (fsync) → commit
  → apply → read-after-write barrier. Committed means durably held by a majority;
  a node serves reads once its apply index reaches the commit index; the log
  doubles as the durable admin-intent record and the audit trail (#17).
- **Deleted machinery.** chitchat + gossip KV + payload budget; node incarnations;
  ring epochs + `EPOCH_MISMATCH` retries; the 3 s settle delay; per-key ownership
  generations + persisted floors; config digest pointers + anti-entropy body
  fetch + per-port revision counters; tombstone ack-vector GC + resurrection
  rules; cold-start `(g,revision,origin)` merge. Net line count very likely goes
  *down*, and the dual-owner fork class (chaos C9) stops being a scenario that can
  fail.
- **Strongly consistent authorization data** for tenancy/RBAC (#17) — the right
  property for authz, which eventually-consistent RBAC is not.
- **Full-restart recovery is log + snapshot replay**, with deletions that cannot
  resurrect and an all-empty fleet that loudly refuses to serve.

### Paid, honestly

- **A minority partition cannot write config** — it returns `503 + Retry-After +
  op-id` (the intent durably parked and replayed on heal). This is a real change
  from v2's "each side stays internally serialized, heals by
  `(g,revision,origin)`". Given correctness was stated as the priority over
  availability, and §7.6 already defaulted to `reject` on owner-unreachable,
  quorum is the better answer.
- **Leader elections (~1–3 s) pause admin writes** — invisibly to clients, since
  the intent machinery parks and replays across the election.
- **Every voter needs a real disk** — the vote and log live there. Kubernetes and
  cloud deployment guidance make a persistent volume per voter mandatory, not
  advisory.
- **`openraft` is a substantial dependency** with a learning curve — offset by
  the machinery it lets us delete, and de-risked by the spike below before the
  full build-out.

### The one residual window, now principled

A node partitioned away that has not yet applied the membership entry deposing it
could briefly still believe it owns a key. Closed by the **isolated-owner rule**:
a node that has not heard a leader heartbeat within `3 × election_timeout` marks
itself *isolated* and rejects owner-side stateful ops. A new owner is by
definition on the quorum side and has applied the deposing entry, so the two
serving windows cannot overlap by more than the heartbeat bound, and the
`(m_idx, v, origin)` fencing tuple resolves anything written inside it. Enforced
at the owner, not assumed at the caller.

## Alternatives rejected

- **Bolt Gaps A/D/F onto the v2 gossip foundation.** Four hand-rolled protocols
  atop the settle/generation machinery — a worse consensus, built by hand, on the
  scaffolding consensus removes. Rejected on complexity and correctness surface.
- **External Temporal / Restate / DBOS / etcd.** A good fit for the durable-intent
  gap on its own merits, but reintroduces exactly the mandatory external service
  the product exists to avoid (server + datastore, sometimes + Elasticsearch), and
  none has an embeddable Rust form. Worth revisiting only as an *optional*
  cluster integration for customers already running one (the D-12 pattern:
  zero-dependency by default, external system by choice).
- **Raft for flow state too.** A quorum write per scenario transition at
  20–40k RPS is unacceptable. Flow state stays single-writer HRW + WAL; D-8
  (cursor reset on ownership move) and D-12 (Redis-strict path) both stand.
- **Object-store tiering of the blob corpus** (evict cold datasets/specs
  locally, fetch from a bucket on demand — rift-cluster#458, 2026-08-24). D-12's
  "external system by choice" is *not* a precedent for this: the Redis-strict
  path lets a customer swap one consistency model for another, whereas tiering
  would trade away availability the fleet already has — a bucket outage,
  credential rotation, or lifecycle deletion becomes a request-path failure, and
  a blob no member holds can never apply, stalling the log fleet-wide. RFC-005
  §3.2 bounds dataset bytes by quota precisely so the corpus is
  consensus-worthy small and fully replicated; a corpus that outgrows a voter's
  disk is a redesign with numbers, not a tier. See D-18.

## Decision-log entries (RFC-001 Appendix C)

- **D-15** — Embedded Raft (`openraft`) control plane over gossip. Supersedes D-1,
  D-2.
- **D-16** — `redb` for all cluster durability (log/vote/snapshot metadata +
  flow WAL). `sled` rejected (maintenance); `fjall` kept as the LSM fallback if
  write amplification bites. **Amended by #436:** the snapshot payload is a file
  beside redb, not a row in it — a payload inlined as a redb value was ~3.7x the
  bytes it carried and was read whole on every send.
- **D-17** — Flow state stays off consensus (HRW + WAL); ownership derived from
  committed membership.
- **D-18** — Every member holds every live blob. The content-addressed blob
  store (rift-cluster#437) is quorum-complete on each node; an object store
  (rift-cluster#448) is an opt-in cache/backup tier that is never consulted on
  the serving path and never a condition for apply. D-12 covers flow state, not
  the blob store.

## Implementation

The complete, implementation-ready specification — openraft type config, the redb
table schema, the deterministic-and-infallible apply-loop contract, the
read-after-write barrier mechanism, op-id dedup, the membership lifecycle, the
`m_idx` fencing rule, the rift-cluster module layout, and the re-scope of #6–#9 —
lives on the tracking issue,
[rift-cluster#14](https://github.com/achird-labs/rift-cluster/issues/14),
and is reflected throughout the [architecture guide](../architecture/README.md)
(chapters 3–9). The transport substrate this builds on
([#8](https://github.com/achird-labs/rift-cluster/issues/8)) is already merged.

**First gate — the spike.** Before the full #6/#9 build-out, a minimal 3-node
`openraft`-over-`redb` prototype must prove the two risks that cannot be
desk-checked: (1) the redb storage impl passes `openraft`'s own storage test
suite, and (2) a 3-node group commits a `PutImposter` and survives a leader kill
mid write-storm (writes applied-and-present or 503-with-op-id-then-present, zero
duplicates via dedup, new leader ≤ 3 s). If redb fights openraft's API, it
surfaces there — cheaply — rather than mid-#9.
