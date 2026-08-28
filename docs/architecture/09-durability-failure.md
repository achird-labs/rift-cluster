# Chapter 9 — Durability & Failure

This chapter is the system's honesty ledger: one table for *what survives
what*, one for *how each operation behaves when the cluster is degraded*, and
walkthroughs of the failure scenarios that matter. Nothing here is
open-ended — every window is bounded, counted in metrics, and visible on the
wire (`Rift-Cluster-*` headers). The design's standing rule: **when
correctness and availability conflict, reject loudly; never answer wrong
quietly.** A mock that 503s makes a test fail visibly; a mock that answers
stale makes a test pass falsely.

## The survival matrix

What state lives where, and what it survives:

| State | Store | Node crash | Full-cluster restart | Notes |
|---|---|---|---|---|
| Membership, node ids | Raft log/vote (`redb`, fsync) | ✅ | ✅ | Group re-forms from disk |
| Raft snapshot payload | file at `<cluster-state-dir>/snapshot/<id>` (temp → fsync → rename → dir fsync); `redb` holds only `{meta, file}` | ✅ | ✅ | Derived state — a missing payload is rebuilt, not mourned (#436) |
| `departed` marker | file in the state dir, fsync'd before the drain | ✅ | ✅ | Steers the next start between *resume*, *rejoin* and *bootstrap* (D-26) |
| Imposter configs + `enabled` + revisions | Raft state machine | ✅ | ✅ | Committed = fsync'd on majority |
| Tenants, principals, bindings | Raft state machine | ✅ | ✅ | Authz survives anything the configs survive |
| Admin intents + op-dedup | Raft SM + accepting node's `pending_intents` | ✅ | ✅ | R4: parked before forwarded |
| Flow state @ `sync` | FlowShard `redb`, fsync-per-ack | ✅ | ✅ **zero loss** | |
| Flow state @ `async` (default) | FlowShard, group fsync per 50 ms | ✅ (replicas live) | ✅ minus ≤ 1 interval | Loss only if **all 3** holders die inside one interval |
| Flow state @ `none` | memory | via replicas | ❌ (opted) | Throwaway CI imposters |
| Sequence cursors | memory | ❌ reset (D-8) | ❌ | Deliberate: hottest stateful path, test-run-scoped |
| Request journal + counters | memory (CRDT shards) | ❌ that shard | ❌ | In-run assertion data, bounded buffers |
| Journal per-port **seq floor** | `journal-seq-floors` in the state dir | ✅ | ✅ | Not the entries — just the counter, so a restarted writer cannot reuse `(node_id, seq)` (#351) |
| proxyOnce `Pending` claims | owner memory | ❌ re-claimable | ❌ | By design — Chapter 7 |
| proxyOnce `Recorded` + recorded stubs | via config (Raft SM) | ✅ | ✅ | Recordings are config |

The volatile rows are decisions, not gaps: each would cost hot-path writes to
preserve state whose value ends with the test run.

The seq-floor row is the one place that reasoning was applied too widely, and
#351 corrects it. The journal's *entries* are indeed test-run-scoped and stay
volatile. Its *counter* is not the same kind of thing: `node_id` is durable, so
`(node_id, seq)` is an identity the rest of the fleet keeps referring to — in
replica caches and in live cursors — after the writer that issued it is gone.
A counter that restarted at 0 would hand those identities out a second time,
which is not lost data but wrong data. Persisting it also does not cost a
hot-path write, because the floor is reserved a block at a time: one fsync per
2^20 appends per port, and nothing at all in between.

## The degradation table

Behavior per operation class when the relevant authority is unreachable.
Defaults shown; `local` overrides exist per feature and stamp
`Rift-Cluster-Degraded: <feature>` on every response they taint:

One class the word *unreachable* does not cover: an authority that is perfectly
reachable and **refuses itself**. A flow owner that cannot see a quorum reports
`is_isolated()` and declines its own owner-side writes and strong reads (D-17,
the isolated-owner rule, Chapter 6). The outcomes in the table are unchanged —
the caller still gets a fast failure rather than a stale answer — but the cause
is the owner's own quorum state, not the network between caller and owner, and
`Rift-Cluster-Degraded` does **not** fire for it: nothing degraded, the write was
refused.

What is observable differs by operation, and it is worth stating exactly:
a refused **write** increments `rift_cluster_cas_conflicts_total{reason="isolated"}`;
a refused **forwarded** read surfaces on the serving node as
`rift_cluster_rpc_failures_total{reason="unavailable"}`; a refused **local**
owner-read emits no metric at all today. That asymmetry is the reason to watch
the *condition* rather than its symptoms: a read-heavy workload can trip the
isolated-owner rule continuously and move none of the counters above.

The **message** is the same refusal wherever the read entered the cluster. A
`strong` read forwarded to an isolated owner fails with `flow store: unavailable:
flow store: owner is isolated from the cluster` through any node (D-61): the
forwarding hop relays the owner's own error instead of restating it as its own
transport failure. Before #471 the same read reported `flow store: transport
failure: <authority> unreachable (<addr>: …)`, which pointed on-call at the
network for a peer that was up and had answered — the reason survived only as a
substring of a sentence that contradicted it.

The **status** does not change, and it is worth knowing which one you get. The
store hands the engine a bare `anyhow::Error`, so the data plane's
`backend_error_response` finds no `BackendUnavailable` to downcast to and answers
**500** — or, for a `{{ state.k }}` token with `RIFT_DEBUG` unset, **200** with the
token rendered empty. Isolation is therefore not distinguishable by status on the
data plane, before or after this change; the message and the `rift_cluster_isolated`
gauge are the signals. Giving the refusal its own 503 would mean carrying
`BackendUnavailable` out of the clustered store, which is a data-plane contract
change and is not this one.

The condition is `rift_cluster_isolated` — a gauge, `1` while this node cannot
see the quorum, `0` otherwise (#470) — and the `isolated` field of
`/_cluster/health` (and `/_fleet/health`). Both are the same sample of the same
rule: `StatusReport::isolated` and `RaftNode::is_isolated` both evaluate
`isolated_from`, so the gauge an alert fires on and the field a human reads
cannot drift apart. Prefer the gauge: a JSON field is not scrapable by an alert
rule, which is what left this condition alertable only through its symptoms
until #470.

Widening `rift_cluster_cas_conflicts_total{reason="isolated"}` to cover reads
was rejected: it is documented as owner-side **write** refusals, and changing
what an existing family counts would silently redefine a shipped runbook signal.

| Operation | Authority | Unreachable ⇒ default | Rationale |
|---|---|---|---|
| Admin write (config, tenancy, enable) | Raft quorum | `503` + `Retry-After` + **op-id, durably parked, auto-replayed** | R4: refused ≠ lost |
| Scenario match-gate read | flow owner | fast-fail `503` | A stale read here = silently wrong stub |
| Scenario CAS / flow-KV write | flow owner | fast-fail `503` | Single-writer or nothing |
| Script flow-KV read (`strong`, default) | flow owner | `503` | Scripts drive responses off this |
| Script flow-KV read (`local`, opt-in) | — | local replica, flagged when owner down | Imposter chose speed |
| Sequence advance | cursor owner (opt-in, D-47) | **falls back to the node-local cursor, annotated and counted** (`rift_cluster_sequence_fallbacks_total`) | Blocking all cyclic responses during a blip is worse than a possible duplicate index — the one place availability wins (D-10). Never a `503`; the counter, not the returned index, is what distinguishes a degraded answer from a healthy one |
| proxyOnce claim | signature owner | `503` | Duplicate upstream side-effects are worse than a failed mock call |
| Journal append / count | — (always local) | unaffected | Mergeable by design |
| Journal / count read | all peers | merge of reachable shards + `Rift-Cluster-Partial: true` | Partial-and-says-so beats blocked |
| Journal read **from a crash-restarted writer** | all peers | merge, still `Rift-Cluster-Partial: true` while peers cache entries of its own lost shard (#349) | The entries are gone for good, not late — a knowingly short answer must say so |
| Admin config read | — (local applied state) | served, possibly behind; revision comparable | Staleness is measurable, not hidden |
| Admin write **carrying a large payload** (spec ≤ 4 MiB, dataset ≤ 8 MiB) | Raft quorum | commits in `O(size / link speed)`; below ~1 MiB/s the intent parks and replays | The bytes ride the log — a big entry is slow, not impossible (#411) |

## The replication ceiling

The log carries metadata; blobs are quorum-replicated out of band (epic #432,
D-23). A spec document up to 4 MiB (RFC-004 S2) and a dataset up to a
tenant-configurable 8 MiB (RFC-005 §4) no longer ride a log entry: a
`SpecPut`/`DatasetPut` commits a digest and a size — under 4 KiB — and the bytes
travel through the content-addressed blob store (`/internal/v1/blob/{digest}`,
#437), fanned out to a joint-consensus quorum *before* the referencing op is
proposed (#438, D-18/D-19) and fetched on apply by any member the fan-out missed
(#439, D-48/D-49). Snapshots became manifests of digests the joiner fetches on
install (#440, D-50). So the log entry is small again by construction, and the
large-payload concerns this section used to open with have moved off the log
onto a transport built for them.

The blob transport is **resumable**: a transfer is chunked at 4 MiB
(`BLOB_CHUNK_MAX_BYTES`), receiver-verified against the digest, and a stalled or
restarted transfer resumes from its last committed chunk rather than from byte 0
— which is what lets an 8 MiB blob cross a real link and commit (#453) where an
8 MiB *log entry* never did (below). Everything from here down describes the
log's own large-entry path (#411): it still exists, still bounds any entry the
transport cap admits, and is simply no longer the path the data quotas take.

What bounds a log entry's size is the transport's own body cap (32 MiB), which
sits above every quota. What bounds its *latency* is the link:

- A large entry commits in **`O(size / link speed)`**, not in one heartbeat.
  openraft grants each AppendEntries RPC only `heartbeat_interval` (50 ms), so
  the transfer deliberately **outlives** that deadline — it runs on its own task
  in the network adapter, and openraft's re-send attaches to the transfer in
  flight instead of restarting it from byte 0.
- **Heartbeats and failover are unaffected.** A heartbeat carries no entries and
  is sent inline, never queued behind a transfer, so the leader keeps its term
  for the whole upload. The election timers are untouched (50 / 150–300 ms), and
  ADR-001's "~1–3 s elections" still holds.
- An entry that cannot cross the link at **≥ 1 MiB/s** exceeds its transfer
  deadline. The op then takes the ordinary parked-intent path — `503`, op-id,
  durably parked, auto-replayed — and op-id dedup makes the eventual commit
  happen exactly once.
- A follower that refuses an over-cap batch answers `413`. The leader halves the
  batch and retries immediately rather than treating the peer as unreachable, so
  a lagging follower catching up across several large entries makes progress
  instead of backing off forever.

Before #411 none of this was true: every attempt was cut at 50 ms and restarted,
so a 512 KiB entry took 23–548 s and anything ≥ 1 MiB never committed at all —
the effective ceiling was "whatever replicates in one heartbeat", far below both
documented quotas.

**The silent window, and why it is closed.** A follower's election timer is
refreshed only by an AppendEntries that reaches its engine, and openraft 0.9
sends a follower nothing else while a large entry is in flight nor anything at
all during a snapshot install. A window longer than `election_timeout_min`
(150 ms) makes a **voter** campaign, and once its term has moved the leader —
which rejects a candidate without adopting its term — never reconciles with it.
Two independent fixes closed it, and #432 removed what opened it in the first
place. #431 supplies a per-peer liveness heartbeat that bypasses the health
tracker, a 50 ms reconnect backoff, and a restart grace for a node that already
belongs to a cluster, so a voter no longer campaigns through a legitimate
transfer; a stale replication core reading a range the new leader's conflict had
truncated — the openraft 0.9.24 panic behind #430 — is tolerated from 0.9.25
(#435). And because spec/dataset bytes and snapshot payloads no longer ride the
log (#439/#440), the multi-MiB entries and multi-MiB `InstallSnapshotRequest`s
that used to make the window *routine* are gone: a digest-only entry and a
manifest snapshot are both back in KiB, and their bytes cross the blob transport
without touching a follower's election timer at all.

## Scenario walkthroughs

**One voter crashes (the common case).** Raft elects within ~1–3 s if it was
the leader (admin writes pause invisibly — intents park and replay); mock
traffic unaffected on surviving nodes. Flow keys owned by the dead node are
adopted by their successors after the membership entry commits — staleness ≤
one replication round, or a flagged FSM reset if all replicas were lost too.
LB health checks drain the dead node. Nothing requires an operator.

**Network partition, 5 nodes → 3|2:**

```mermaid
flowchart TB
    subgraph M["majority side (3 voters)"]
        A[Node A — leader] --- B[Node B] --- C[Node C]
    end
    subgraph m["minority side (2 voters)"]
        D[Node D] --- E[Node E]
    end
    M ~~~ m

    W1["admin writes: ✅ normal<br/>(quorum intact)"] -.-> M
    W2["admin writes: 503 + op-id,<br/>parked & replayed on heal"] -.-> m
    F1["flow keys owned in M: ✅ normal<br/>keys owned in m: adopted at<br/>next membership commit"] -.-> M
    F2["mock traffic: stateless ✅<br/>stateful ops on M-owned keys: 503<br/>own-side owners: isolated-owner rule —<br/>followers at 450–600 ms (openraft lease),<br/>leader at 900 ms (ISOLATION_WINDOW_MS)"] -.-> m
```

The minority never diverges — it *refuses*. On heal: parked intents replay
(op-dedup makes replay exactly-once), minority nodes catch up on the log, and
the fencing tuple `(m_idx, v, origin)` disposes of anything an isolated owner
wrote inside the heartbeat window. What v2 needed conflict counters and merge
rules for, this design makes structurally unrepresentable — the one lost-update
class remaining is the flagged, opt-in `local` modes.

**Full-cluster restart (deploy, power event).** Chapter 3's cold start: redb →
group re-forms → replay. Configs, tenancy, intents: intact (R3). Flow state:
per its durability level. Journal: empty (matrix above). A CI run interrupted
mid-flight resumes against identical mocks with identical scenario states (at
`sync`/`async`), which is precisely the "always-on shared environment" promise.

**Disk loss on one node.** The node restarts empty → it is a *new* node
(identity lived on that disk — unless `--cluster-node-name` is set, which
re-derives the same id): joins as learner, snapshots the state machine,
re-syncs flow replicas. The old id is removed by runbook. No data loss —
everything it held exists on ≥ 2 other disks. **Correlated disk loss on a
majority of voters** is the honest limit of a self-contained cluster: configs
survive only as `--datadir` exports/backups (Chapter 10's backup runbook);
this is stated rather than hedged.

That catch-up moves the *whole* state machine, datasets and spec documents
included, so it is measured in MiB rather than KiB — and it costs roughly 4× its
size on the wire, because snapshot chunks ride the JSON cluster port as byte
arrays. The transfer is bounded **per chunk**, not per snapshot: a chunk that
misses its deadline abandons the entire transfer back to offset 0, so the bounds
are deliberately generous rather than tight (#428). Two apply, and the smaller
governs — the RPC's own size-aware deadline (~6 s for a 1 MiB chunk: a flat
budget plus a 1 MiB/s floor on the link) inside openraft's per-chunk
`install_snapshot_timeout` of 10 s. The join itself never rides that install:
admission is two-phase (#433) — the join RPC returns once the membership
entry commits, the node starts up as a learner, and the leader promotes it to
voter when its replication is current. However long the catch-up above takes,
it delays *promotion*, never startup; a refused, unreachable, or mis-secreted
join still fails the deployment exactly as before.

**What a snapshot costs to store, measured (#436).** The payload is written as a plain file beside
redb rather than inlined into a `redb` row as a JSON integer array, which is what made the stored
artifact ~3.7× the bytes it carried. Measured after the change, on loopback: a fleet holding 4 MiB
of datasets stores **1.00×** its raw bytes, and one holding 16 MiB likewise **1.00×**.

**Fresh-joiner catch-up, before and after (#436),** same probe on both sides, 1 voter → 2:

| fleet state | before (#436) | after file-backed (#436) | after manifest (#440) |
|---|---|---|---|
| 4 MiB | 3.1 s | **2.2 s** | seconds |
| 16 MiB | 12.3 s | **8.8 s** | seconds |
| 64 MiB | never converges | still never converges | **converges — bytes fetched out of band** |

The 64 MiB row was the honest headline until #440: binary, file-backed snapshots (#436) cut what a
snapshot costs to *store* and to *install*, but did not move the ceiling, because the payload was
still chunked onto the wire as a JSON integer array by openraft's own `InstallSnapshotRequest` — the
same silent window a multi-MiB entry opens (above), now opened by the install instead. #440 removes
the payload from that path entirely: the snapshot is a **manifest** of digests and sizes (KiB), and
the joiner fetches each blob it lacks over the resumable blob transport (#437) — a snapshot names no
single origin, so it asks every joint voter, parking and retrying if none can yet supply it (D-48) —
*before* the install commits. The `InstallSnapshotRequest` no longer
carries the dataset bytes, so the ceiling it imposed is gone; a 64 MiB fleet's catch-up is now
bounded by the same per-blob resumable transport that carries an 8 MiB blob (#453), not by a
snapshot that restarts from byte 0. Measured multi-MiB catch-up is seconds; the 64 MiB case
converges by the same construction rather than by a fresh end-to-end measurement at that size.

**Producing a snapshot costs the leader CPU, but never its runtime (#444).** The chapter above
describes what a catch-up costs the *joiner*; the other half is what building one costs the node
that serves it. A build walks every state-machine table and encodes the result — O(state) CPU with
no await in it — and an install does the same in reverse under one `Durability::Immediate`
transaction. All three of `build_snapshot`, `get_current_snapshot` and `install_snapshot` run that
work on tokio's **blocking pool**, never on a runtime worker, so heartbeats, elections and the
per-peer liveness ticker are unaffected by a build or an install of any size.

That placement is load-bearing rather than tidy. tokio cannot preempt a synchronous body, so before
#444 a build held a runtime worker for its whole duration; on a two-vCPU runner — where all three
in-process nodes' runtimes share two vCPUs — that was long enough for a follower's election timeout
(150–300 ms) to fire and for the leader to lose office while doing nothing but snapshotting. The
`worker_threads = 1` gate in `raft::store`'s tests pins it: the leader's tick gap across a build,
read and install of a ≥ 16 MiB snapshot stays under `election_timeout_min`, measured with **no
joiner present** so the leader-side cost is isolated from anything on the install path.

**The write path still has this shape, in one place.** `RaftLogStorage::append` is synchronous on a
runtime worker — it commits and fsyncs every entry before acknowledging it — and is not hoisted: a
blocking-pool hop per committed entry buys latency for nothing on the hot path, and #432's move to
digest-only entries (#439) already shrank each entry to KiB, so the serde and fsync it carries are
small. `apply` no longer carries a dataset's CSV at all: a digest-only op resolves its bytes in a
pre-transaction fetch (D-49) that runs off both the redb write transaction and the runtime worker
(the blob is stored via `spawn_blocking`, #444), and only the spool materialisation — plain file
I/O bounded by the blob's size — remains, done after the durable commit. A heartbeat gap during a
write is that residual, not a regression of the fix above.

**Catch-up no longer has a size ceiling below the documented quotas (#440).**
Before #432's snapshot child, a fleet holding a few MiB of state caught a node up
in seconds (measured: ~4 MiB in 8 s on loopback) but a fleet at RFC-005's 64 MiB
per-tenant dataset ceiling did **not** catch a node up at all — the joiner
applied nothing even given minutes, because the whole payload was chunked through
`InstallSnapshotRequest` and restarted from byte 0 on any stall. Manifest
snapshots removed that path: the joiner installs a KiB manifest and fetches the
bytes it lacks over the resumable blob transport (#437, #440), so RFC-005 §4's
quotas now describe state a replacement node can actually be brought up to hold,
and an operator near them should expect a joiner to converge in
`O(state / link speed)` rather than to fail. The per-blob transport is the only
remaining bound, and it is the same one the live write path already runs at.

A related case, and the one more likely to be met in practice: the scenario
above is a node that comes back **empty**. A node that returns still holding its
old state, having been down long enough for the fleet to snapshot and purge past
it, is caught up by snapshot too — but only since #431. Before it the leader's
own peer-health tracker refused to heartbeat the restarted voter for its
cooldown, the voter campaigned, its term ran away, and it stayed at the index it
left off forever. The liveness probes of D-22 close that window;
`a_restarted_voter_behind_a_purged_log_catches_up_by_snapshot` pins it, term
assertion included.

**Slow node (GC-pause-class stall, not dead).** The three protections:
fast-fail RPC health stops per-request timeout burn; the bounded bridge sheds
stateful ops so stateless traffic never queues behind a black hole (chaos C13
pins stateless p99 < 5 ms through owner loss); the write barrier caps at 2 s
and names the straggler in `Rift-Cluster-Warnings` rather than hanging the
admin plane.

## The client-visible contract

Everything above surfaces through five headers — `Rift-Cluster-Revision`,
`Rift-Cluster-Op-Id`, `Rift-Cluster-Warnings`, `Rift-Cluster-Degraded`,
`Rift-Cluster-Partial` — plus `rift_cluster_degraded_ops_total{feature}` and
friends in metrics. A strict test harness asserts the absence of the last
three; a lenient one ignores them. Both get the truth.
