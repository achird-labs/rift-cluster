# Chapter 7 — The Verification Plane

Mocks exist to be asserted against. After a test sprays requests across the
fleet, `GET /imposters/:port/requests` must return **all** of them — from any
node; `numberOfRequests` must be the true fleet-wide count; a `DELETE
savedRequests` must clear everywhere without clock games; and proxy recording
must capture upstream responses exactly once. This plane has a luxury the
others lack: its data is **mergeable**. Recorded requests from different nodes
never conflict — they interleave. That property buys a design with no owners,
no consensus, and no coordination on the write side at all.

## The journal: per-writer shards, merge on read

Every node appends recorded requests to **its own shard** — a local,
append-only log per port, entries keyed `(node_id, seq, clear_gen)` with `seq`
a per-node monotone counter. Appending is always local (zone 3 of the read
path): a mock request never waits on another node to be journaled.

Reads merge:

```mermaid
sequenceDiagram
    participant T as Test (assertion phase)
    participant A as Node A
    participant B as Node B
    participant C as Node C

    T->>A: GET /imposters/8080/requests
    par pull peer deltas (2s budget)
        A->>B: journal since (B, seq≥17)
        A->>C: journal since (C, seq≥42)
    end
    B-->>A: 3 new entries
    C-->>A: 5 new entries
    A->>A: k-way merge by recorded timestamp<br/>(ties: node_id, seq) — dedup by key
    A-->>T: all N requests, whichever nodes served them
```

Pull-on-read plus a 5 s background anti-entropy pull keeps reads warm; if a
peer is unreachable, the merged result still returns — stamped
`Rift-Cluster-Partial: true`, so a CI assertion can distinguish "0 requests
arrived" from "0 requests visible right now" (silently conflating those two is
how verification tools lie).

**Caps stay writer-local and honest.** Each shard caps at
`max(500, 10_000 / N)` entries per port plus an age cap, evicting oldest
and advancing an `evicted_below_seq` watermark that readers respect — so every
node converges on the same visible set even after eviction. `N` is the voter
count of the **applied membership**, not the currently-reachable node count:
membership changes only through a committed log entry, so every node derives
the same cap, and a peer flapping in and out of health cannot resize shards —
which would evict entries a test was still going to assert on.
`numberOfRequests` is a per-node G-counter slot summed on read (it counts even
when body recording is off, matching single-node semantics).

## Clears are generation bumps — never timestamps

`DELETE savedRequests`, count resets, and `teardown_space` must clear
*fleet-wide* atomically-enough, and wall clocks across nodes cannot be
trusted. So clears never delete by time: a **clear generation** — a monotone
integer per port (and per `(port, space)`) — is bumped and replicated; every
journal entry and counter increment is tagged with the generation current at
its writer; merge and read simply ignore anything from an older generation.
Racing clears merge to the same max harmlessly; a clear during a partition
takes effect on the other side the moment generations merge — deterministic,
clock-free, no coordinated deletion required. (Under the ADR-001 control
plane, generation bumps ride the Raft log like any small fleet-wide fact,
which also orders them against config changes for free.)

Targeted deletion (`retain` with a predicate) stays best-effort per shard and
is documented as such — the generation mechanism is the guaranteed path.

## Cursor reads and live streams

Upstream grew a cursor API (#603): `savedRequests?since=<cursor>` and SSE
streams (`GET /events`, `.../savedRequests/stream`), with SDKs in four
languages tailing them. A scalar cursor cannot survive a multi-writer merge —
"index 500" means nothing across three shards. The cluster answer is a
**vector cursor**: the opaque `since` token encodes `{node_id → shard_seq}`;
each pull advances per-shard positions; the merged stream stays gapless and
duplicate-free per shard even as nodes join or die (missing nodes keep their
last position; new nodes enter at 0). SSE in cluster mode rides the same
anti-entropy cadence — events from *other* nodes arrive within the pull
interval, and the stream declares that latency rather than pretending
otherwise.

### As implemented (issue #225)

The token is `v1: {gen, pos: {node_id → shard_seq}}`, serialized to JSON and
encoded unpadded base64url, so it is safe in a query string and an SSE `id:`
line alike. It is opaque **by contract**, not merely by convention: a client
round-trips it and never parses it, which is what leaves the encoding free to
change behind its version tag. An unrecognized version is refused rather than
guessed at.

`gen` is the port's clear generation (#224) at issue time. It is **carried but
not yet acted on**: a clear landing mid-walk neither re-delivers cleared
entries nor rewinds the reader, but that comes from the per-shard positions
plus the fact that a clear never resets `seq` — the merge drops superseded
generations, and the positions step over the seq range they occupied. The
field is in the `v1` format because a token that records which generation it
was minted under is the cheap enabling condition for anything that later needs
to distinguish "your cursor predates a clear" from "nothing new", and adding a
field to a versioned token afterwards costs a version bump.

The front door terminates **every** requests-read that is not `?match=`-scoped,
`?since=` included — the engine's own scalar `parse_since` never sees a vector
token. Each merged read answers `x-rift-next-index` with the next token (this
replaces #223's withheld-header convention, which existed only because there
was no value that meant the same thing on every shard) and
`x-rift-truncated: true` exactly when some shard's presented position had
fallen below that shard's eviction watermark. `evicted_below_seq` is
inclusive, so a reader *at* the watermark has seen everything eviction removed
and is not truncated.

Three rules make the walk correct, and each is pinned by its own gate test:

- **Which entries** — an entry is withheld when its own shard's position is at
  or past its `seq`. Per-shard filtering is the whole point of a vector cursor:
  the merged *order* is by recorded timestamp and therefore interleaves shards
  arbitrarily, so no single index is a position in any shard in particular.
- **Where the next page starts** — each shard advances to the highest of the
  presented position, its eviction watermark, and the highest `seq` it holds.
  Taking the eviction and clear-dropped entries into account (rather than just
  "the highest seq emitted") stops the walk re-scanning entries it can never
  serve; taking the presented position into account is what makes the token
  monotone even when a replica cache has gone backwards.
- **Membership** — a shard in the cursor with no slice this round keeps its
  position, so a dead or partitioned node freezes rather than rewinding to 0
  and replaying its history when it returns. A shard with a slice and no cursor
  entry reads as 0, so a joining node enters the walk at the beginning.

A bare `u64` is accepted for the upgrade window and read as `{this_node: seq}`:
before #225 a merged read issued no cursor at all, so any scalar a client holds
provably came from a proxied per-node read of this node. Anything that is
neither a token nor a `u64` is a typed 400 — never a defaulted position, since
defaulting either replays the whole journal or silently skips everything
recorded since.

**The merged SSE tail is not yet implemented.** `.../savedRequests/stream` and
`GET /events` both still proxy per-node, FleetAdmin-gated, as they did before
this issue; the cursor half above is what shipped. The token was designed to
serve both — an SSE `id:` line is a valid place to put it — so the terminator
is additive when it lands.

## proxyOnce: exactly-once recording via an owner claim

The one verification feature that is *not* mergeable: `proxyOnce` must call
the real upstream **once** per request signature, record the response, and
replay it forever after. "Once" under concurrent first-hits on three nodes
needs an arbiter — this is owner territory (Chapter 6's ring, keyed by
`(port, signature)`), running a small state machine at the owner:

```mermaid
stateDiagram-v2
    [*] --> Unclaimed
    Unclaimed --> Pending : try_claim → token<br/>(winner calls upstream)
    Pending --> Recorded : complete(token) —<br/>only AFTER the recorded stub's<br/>config write is acknowledged
    Pending --> Unclaimed : release(token) on upstream failure<br/>· or deadline expiry (2× upstream timeout)
    Recorded --> [*] : replicated fact —<br/>all future hits replay locally

    note right of Pending
        Pending is owner-local and dies with
        the owner: a crash makes the signature
        re-claimable, bounding duplicate upstream
        calls at 1 + ownership changes in flight.
        Stale tokens are rejected — a late
        complete() after re-claim cannot
        misattribute a recording.
    end note
```

Two ordering subtleties carry the correctness:

- **Claim owner ≠ config owner.** The recorded stub is published through the
  Chapter 4 write path, while the claim lives at the `(port, signature)`
  owner. `Pending → Recorded` transitions only after the config write is
  acknowledged; if that write fails, the claim releases and the signature
  stays retryable. "Recorded but stub-less" is unrepresentable.
- **Why not a simple replicated set?** A pure grow-only claim set cannot
  express *release*: a failed upstream call would either resurrect its claim on
  every merge or wedge the signature forever. The Pending/Recorded split — with
  only `Recorded` ever replicated — is the minimal shape that supports both
  exactly-once success and retryable failure.

Recordings themselves (multi-response proxy modes) append through the config
write path like any stub mutation, so they inherit R1/R3/R4 wholesale.

## What this plane deliberately does not promise

Journal entries are **test-run-scoped**: bounded buffers for in-run assertion,
volatile across full-cluster restarts (Chapter 9's matrix, with the segment-file
extension path noted if that ever changes). And under partition, verification
reads are *partial and say so* rather than blocking — a test harness that needs
strict completeness asserts `Rift-Cluster-Partial` is absent, which the chaos
suite does in anger.
