# Chapter 3 — The Control Plane

The control plane is where the cluster agrees on the things it cannot afford to
disagree about: **who is in the cluster, what the imposters are, what the
front door's route table says, and which admin requests have been accepted.** It
is an embedded Raft group — `openraft` running inside every `rift-cluster-server`
process, speaking over the same HMAC-authenticated cluster port as everything
else. There is no external coordinator, no sidecar, no operator-managed quorum
service. Three Rift binaries behind an LB *are* the consensus group.

## Why Raft, and why membership lives inside it

The earlier design draft (RFC-001 v2) built the control plane on gossip:
eventually-consistent membership, ownership by hashing over whatever roster a
node currently believed, and then — because two nodes could transiently
believe different rosters — a stack of compensating machinery: ring epochs,
settle delays before a new owner may serve, per-key ownership generations,
version-vector merges on heal. Every piece existed to manage disagreement
about membership.

Putting membership *itself* into a Raft log (D-15) removes the disagreement
instead of managing it. The roster is now a value in a linearizable state machine: at any
log index, every node that has applied that index computes byte-identical
membership, and therefore byte-identical ownership for every key. The settle
delay, the generations, the epoch-mismatch retry ladders — deleted, not
mitigated. The one residual window (a node partitioned away that hasn't yet
learned it lost ownership) is closed by a lease rule at the *owner* side
(Chapter 6), not by caller-side guessing.

The same log then carries configs, tenancy, and admin intents — so R1
(fleet-wide visibility), R3 (durability), and R4 (no lost requests) come from
one mechanism instead of three bespoke protocols.

## Anatomy

```mermaid
flowchart TB
    subgraph Node["each rift-cluster-server process"]
        RN["openraft node<br/>(leader OR follower/learner)"]
        SM["State machine (apply loop)"]
        DB[("redb — cluster-state-dir<br/>raft_log · raft_vote · snapshot meta<br/>sm_configs · sm_routes · sm_routes_revision<br/>sm_session_key · sm_fleet_name<br/>sm_op_dedup · pending_intents")]
        IM["ImposterManager (OSS engine)"]
        RPC["cluster RPC (hyper + HMAC)<br/>/internal/v1/raft/append · vote · snapshot"]
    end

    RN -- "append entries (fsync'd)" --> DB
    RN -- "committed entries" --> SM
    SM -- "sm_* updates" --> DB
    SM -- "apply_config / set_enabled" --> IM
    RN <-- "to peers" --> RPC
```

- **Log and vote storage** commit with `redb`'s `Durability::Immediate` —
  fsynced before acknowledged. This single property is what makes "committed"
  mean "survives a full-cluster power loss": a majority has the entry on disk
  before any client sees success.
- **The state machine** holds the applied view: imposter config records
  (`port → {config, enabled, revision}` where `revision` is simply
  the Raft log index — monotone and totally ordered fleet-wide for free),
  the front-door route table, and the op-id dedup map that gives admin retries
  exactly-once *effect*.
- **Apply is deterministic and cannot fail.** All validation happens on the
  leader *before* the entry is appended; apply only writes `sm_*` tables and
  calls the local engine (`ImposterManager::apply_config` — the incremental
  reconciler from upstream #316, which touches only what changed). A node
  where the local side-effect fails (say, a port bind conflict) still advances
  its applied index — the config exists; that node reports the bind failure as
  status (Chapter 2), preserving "one node's local problem never stalls the
  fleet's log."
- **Snapshots** serialize the `sm_*` tables on openraft's default policy —
  every 5 000 log entries since the last snapshot (`snapshot_log_entries` lowers
  it for tests only) — and purge the log behind them. Both happen on their own:
  no admin route or console panel triggers a snapshot or a compaction (D-24).
  The payload is a file at `<cluster-state-dir>/snapshot/<snapshot_id>`,
  written temp-file → fsync → rename → fsync-dir before the redb row naming it
  commits, so the row never points at a payload that is not already durable
  (#436; Chapter 9). Config bodies ride in log entries (small JSON); snapshots are the
  compaction story, replacing v2's content-addressed body fetch entirely.
  **Everything the log carries is small JSON** (D-71, #549): the one class of
  payload that was not — a multi-megabyte uploaded document — no longer enters the
  cluster at all. An OpenAPI document is compiled to imposter JSON by a stateless
  endpoint (`POST /specs/compile`, `docs/rift-cluster-server.md`) and the *result*
  is written as an ordinary `ControlOp::PutImposter`; nothing is retained but the
  imposter.

## Membership lifecycle

```mermaid
stateDiagram-v2
    [*] --> Discovering : start with --cluster-seeds
    Discovering --> Learner : leader add_learner()
    Learner --> CatchingUp : snapshot + log replay
    CatchingUp --> Voter : auto-promote when caught up<br/>(while voters < 9)
    CatchingUp --> Learner : voters full — stays learner<br/>(serves data plane, no vote)
    Voter --> Leaving : SIGTERM — flow-state handoff,<br/>removed as a voter (D-59)
    Learner --> Leaving : SIGTERM
    Leaving --> [*]
    Voter --> Dead : crash — peers elect ≤ ~1s
    Dead --> Discovering : restart (same node id,<br/>persisted in state dir)

    note right of Discovering
        Bootstrap is explicit: exactly one node,
        once, starts with --cluster-allow-solo and
        no --cluster-seeds to create the group.
        A node with peers configured
        NEVER forms its own group — this closes
        the split-brain-on-blip and all-empty
        cold-start hazards by construction.
    end note
```

Key rules, each carrying weight:

- **Node identity** is a `u64` the node mints for itself at first start —
  derived from `--cluster-node-name` when set, otherwise from the clock — and
  persists in the state dir. A pod rescheduled with its volume keeps its
  identity; one rescheduled without it returns as the same node if it carries
  the same name, and joins as a new node otherwise (the old id is removed via
  runbook). This replaces the v2 incarnation scheme outright.
- **Membership changes only by a node joining or leaving** (D-21). Admission is
  initiated by the joining node over the signed cluster port; no admin route or
  console action adds or removes a learner or a voter — membership is the
  trust boundary, and what can enter the fleet is bounded by what an operator
  chose to *start*.
- **Seeds are re-resolved through DNS on every attempt** — pod IPs churn, and a
  cached-IP join loop after a full restart would brick the fleet. Every address
  a name resolves to is dialled, in the resolver's own order; there is no
  prefer-IPv4 knob (D-28).
- **Voter cap at 9**: beyond that, nodes join as learners — full data-plane
  citizens (they bind imposters, own flow-state keys, serve traffic) with no
  election weight. Consensus latency stays flat as the fleet grows to the
  16-node ceiling. The cap is a *soft* ceiling on what the fleet does by
  itself, and a promotion only ever adds voter ids — it can never silently
  evict one (D-27).
- **Admission is two-phase** (#433, the etcd learner pattern): the join RPC
  commits the membership entry — the fast, consensus-bound fact — and returns
  `admitted` with the role and a `catching_up` estimate. Catch-up belongs to
  replication, and the **leader's** promotion sweep (1 s cadence) makes a
  caught-up learner a voter under the same admission gate and ceiling. A
  joiner never waits out its own catch-up inside an RPC deadline, and the
  diagram above is literally what the code does: `Learner → CatchingUp →
  Voter`, each transition a committed entry the joiner does not drive.
- **Readiness is a gate, not a vibe**: `/readyz` goes 200 only when the node's
  applied index has caught up to the leader's commit index observed at join
  *and* its imposters are bound-or-reported. An LB never routes to a node
  serving yesterday's config (Chapter 10).
- **Graceful leave** (SIGTERM): drain readiness, leave the membership, and let
  ownership of its flow state move with the committed entry (Chapter 6 — there is
  no pre-leave handoff; every write was already pushed to the successors) — a
  rolling restart never triggers an election or an ownership *guess*; every
  transition is a committed entry.
  The leader refuses a departure that would leave fewer than two voters
  (D-25): the refused node exits crash-equivalent and resumes on its next
  start, so a whole-fleet teardown cannot walk the membership down to a single
  volume. A node that really departed writes a `departed` marker beside its
  state, which — with the presence of a Raft vote and the reachability of its
  seeds — decides *resume*, *rejoin* or *bootstrap* on the next start; the
  state directory is never wiped to force a clean join (D-26).

## Cold start — the payoff

A full-cluster restart under v2 gossip required careful merge rules,
tombstone acknowledgment vectors, and a rule against empty nodes GC'ing the
fleet's config. Under Raft the whole scenario collapses to: every node reopens
its `redb`, the group re-forms from persisted vote + log + snapshot, elects a
leader, and replays. Deletions cannot resurrect (a deletion is a log entry —
a lagging node replays it like any other); an empty-disk node is just a
learner catching up from a snapshot; and if *every* disk is empty, there is no
group to re-form and nothing serves until an operator re-initializes — loud
refusal, exactly as R3 demands (Chapter 9 has the full restart matrix).

**A single member restarting is the case that needs care, not the full
fleet.** A voter that comes back must hear the leader within its election
timeout (150–300 ms) or it campaigns — and once its term has moved, the leader
(which rejects a candidate without adopting its term) never reconciles with it.
Two things on the leader and one on the returning node keep that from
happening (#431): the leader retries an unreachable peer every 50 ms and runs a
per-peer *liveness ticker* — an empty AppendEntries on its current vote whenever
openraft has sent that peer nothing for a heartbeat interval, which is the whole
of a snapshot install and the whole of a large entry's transfer — sent through a
probe that bypasses the peer-health tracker (D-22), because the tracker would otherwise
refuse to talk to a just-restarted peer for its cooldown. The ticker speaks only
while its node actually leads: a probe asserts "your leader is alive", and a
leader that has gracefully left (or been deposed) must fall *silent* — its
silence is what lets the survivors' leader leases lapse so a successor can win
during the drain, which is the handover a rolling restart depends on. On the returning node,
a member with persisted state holds elections for a 3 s *restart grace* until it
hears a leader; a fresh node and a single-voter fleet are unaffected, and a
genuinely dead leader is still replaced once the grace expires.

## Cluster-internal security — the peer secret

*Moved here from Chapter 8 by D-73 (#550), which retired that chapter's tenancy body. This
section was never about tenancy: it is the control plane's own transport, and it belongs beside
the RPCs it protects.*

The node-to-node surface (Raft RPCs, owner-forwarded ops, flow-state
replication) shares one model:

- **A dedicated cluster port**, explicitly configured, intended for a private
  network; binding `0.0.0.0` requires an explicit acknowledgment flag. Never
  multiplexed with data-plane or admin ports.
- **Shared-secret HMAC on every message**:
  `X-Rift-Cluster-Auth: t=<ts>,n=<nonce>,mac=HMAC-SHA256(secret, ts‖nonce‖method‖path‖body)`,
  ±30 s skew window, bounded nonce cache that **fails closed** on overflow.
  Startup refuses clustering without a secret unless `--cluster-insecure` is
  passed, which logs loudly at startup (`cluster port started WITHOUT
  authentication`, with `insecure = true`) so a fleet can be audited for it.
- **Integrity and authenticity, not confidentiality** — the threat model is
  "no unauthenticated peer joins or injects ops", with confidentiality
  delegated to network isolation (VPC/namespace/WireGuard). mTLS between nodes
  is a hardening milestone, deliberately not a Phase-1 gate.
- **Version skew**: every message carries a protocol version; majors must
  match (mismatch → clean rejection, not undefined behavior), minors are
  additive — the contract that makes rolling upgrades safe (Chapter 10).

The **admin** plane is a different credential and a different tier: one API key
(`--api-key`/`MB_APIKEY`, D-73), TLS at the load balancer plus application auth.
Probe endpoints (`/readyz`, `/healthz`) are deliberately unauthenticated and
stateless-safe, because kubelets and LBs do not hold credentials. The cluster
secret and the admin key are never the same value and never interchangeable —
the cluster port is a peer surface, not an operator one.

**The admin plane originates no outbound HTTP on a caller's behalf.** The one
route that did — `POST /admin/imposters/{port}/try` (#335), which dialled
`127.0.0.1:{port}` behind an `is_locally_bound` gate — was found reachable past
the gate on BSD/macOS (#344: the engine's `0.0.0.0` bind coexists with a
foreign `127.0.0.1` socket, and the loopback dial lands on the foreign one —
including, on a `--cluster-insecure` fleet, the cluster RPC listener). Since
#344 the try is answered **in-process**: the sample request is dispatched to
the imposter this node's engine holds, over an in-memory HTTP/1 connection,
with no socket opened. The containment is now structural rather than checked —
there is no address to get wrong — and the `is_locally_bound` gate remains only
so a bind-failed imposter is reported as such (`502`, "not bound") instead of
answered as though it were serving.

## What the control plane costs

Symmetry demands the bill. A minority partition **cannot write config** — the
two nodes on the wrong side of a 5-node split serve mock traffic from their
applied configs but return `503 + Retry-After + op-id` for admin writes (with
the intent durably parked for replay — Chapter 4). Leader elections (~1–3 s)
pause admin writes, invisibly to clients thanks to the same intent machinery.
And every voter needs a real disk (Chapter 10 makes persistent volumes
mandatory, not advisory). For a control plane that changes at human frequency,
these are the right prices; the request path pays none of them.
