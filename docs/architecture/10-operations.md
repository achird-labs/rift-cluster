# Chapter 10 — Operations

Running the cluster: bootstrap, Kubernetes, probes, observability, upgrades,
backups, and sizing. The operator experience is a design goal, not an
afterthought — the target user runs ephemeral CI fleets and perimeter-bound
on-prem environments, usually without a dedicated platform team.

## CLI surface (cluster-relevant)

```
rift-cluster-server \
  --cluster                          # master switch; everything below inert without it
  --cluster-allow-solo               # FIRST node of a NEW cluster: found it when no seeds are
                                      # given (a seedless node without it refuses to start)
  --cluster-bind 10.0.0.5:4790       # required: Raft + owner RPC (TCP, one port)
  --cluster-advertise <host:port>    # NAT/container address peers should dial; a hostname
                                      # is re-resolved on every send, IPv6 literals bracketed
  --cluster-seeds rift-0.rift-peers:4790,rift-1.rift-peers:4790   # DNS re-resolved per attempt
  --cluster-secret-file /secrets/cluster.key                # required (or --cluster-insecure)
  --cluster-state-dir /var/lib/rift  # redb: identity, raft log/vote/snapshot + flow shard;
                                      # default <datadir>/_cluster
  --cluster-write-barrier ready-nodes|none    # Ch.4; default ready-nodes
  --cluster-leave-timeout 10         # seconds (default 10); orchestrator grace ≥ 2× this
  --cluster-probe-bind 0.0.0.0:2526  # unauthenticated /readyz + /healthz (default shown)
```

Every flag has an `RIFT_CLUSTER_*` environment form (`crates/rift-cluster-server/src/cli.rs`
is the source of truth; `docs/rift-cluster-server.md` the full reference).

There is no `--cluster-degraded-mode` flag: what a node does when a flow's owner is unreachable
is a per-imposter `readConsistency` setting (D-10, Chapter 9's degradation table), not a
fleet-wide switch — the flag sketched in earlier drafts was superseded rather than built (#378).
Nor is there a `--cluster-features` flag: config sync and flow state are always on under
`--cluster` (#120 — a per-feature opt-out would reintroduce the per-imposter split-brain the
clustered flow store exists to remove).

One subcommand is not a server at all:

```
rift-cluster-server mcp --url https://fleet.example:2525 --api-key-file ~/.rift/agent.key
```

`mcp` runs a Model Context Protocol server over stdio for coding agents. It is a
**client** of the admin API named by `--url` — it binds no port, joins no ring,
and holds no state, so it is not a node and does not appear in any topology.
Full reference in `docs/rift-cluster-server.md`.

Guard rails enforced at startup: `--cluster` with `--runtime per-core` is
rejected (the sync bridge assumes a work-stealing data plane — D-14);
`--cluster` with intercept mode is rejected (out of scope); no secret and no
explicit `--cluster-insecure` is a refusal to start; a node with no seeds and
no `--cluster-allow-solo` refuses to start rather than founding a second
cluster beside the real one, and a node that already holds state decides
between *join*, *rejoin* and *bootstrap* by D-26's table — never by wiping it.

## Kubernetes deployment

StatefulSet + headless Service; every peculiarity below exists because a
default manifest deadlocks or loses data:

```mermaid
flowchart TB
    ING[Ingress / LB] --> SVC["Service (data: gateway port,<br/>admin port, metrics port)"]
    SVC --> P0 & P1 & P2
    subgraph STS["StatefulSet rift (podManagementPolicy: Parallel)"]
        P0["rift-0 (ordinal 0: RIFT_CLUSTER_ALLOW_SOLO, seeds unset)"]
        P1[rift-1]
        P2[rift-2]
    end
    HS["headless Service rift-peers<br/>publishNotReadyAddresses: true"] -.- P0 & P1 & P2
    P0 --- V0[(PVC)]
    P1 --- V1[(PVC)]
    P2 --- V2[(PVC)]
```

- **`publishNotReadyAddresses: true`** on the headless Service — readiness
  means "caught up and serving", so on a full restart *no* pod is Ready; a
  default headless Service would publish no seed DNS and nothing could ever
  join. Cluster formation is deliberately independent of readiness.
- **`podManagementPolicy: Parallel`** — `OrderedReady` waits for pod 0 to be
  Ready before starting pod 1, but a one-voter group of a three-voter cluster
  isn't Ready. Classic deadlock, designed out.
- **PVC per pod, non-negotiable** for voters: the Raft vote and log live
  there. `emptyDir` is *unsupported* for durability — a simultaneous restart
  on emptyDir is the correlated-disk-loss scenario of Chapter 9.
- **Gateway-fronted mode** for data traffic (Service ports are static;
  runtime-minted imposter ports can't be exposed) — Chapter 2.
- Probes: readiness `/readyz`, liveness `/healthz` (both unauthenticated).
  `preStop` = SIGTERM with `terminationGracePeriodSeconds ≥ 2 ×
  cluster-leave-timeout` so graceful leave (demote → flow handoff → remove)
  completes. `PodDisruptionBudget maxUnavailable: 1`.
- Cluster port stays ClusterIP-internal; secret via K8s Secret →
  `--cluster-secret-file`.

## Observability

**Endpoints** (cluster port, authenticated; probes excepted):

| Endpoint | Answers |
|---|---|
| `GET /_cluster/members` | roster: id, address, voter/learner, Ready, applied index; plus this node's own `bound_ports` / `bind_failures` (Chapter 2 divergence) |
| `GET /_cluster/config` | per port: revision @ every node, `converged: bool` — the CI wait target |
| `GET /_cluster/imposters` | per-(port, node) bind status (Chapter 2 divergence) |
| `GET /_cluster/ring?key=…` | computed owner + m_idx — "who owns this flow right now". *Designed (RFC-001 §10, phase 2); not served by this build* |
| `GET /_cluster/kv/:flow_id` | owner value vs local replica — the *why is my scenario stuck* endpoint. *Designed (RFC-001 §10, phase 2); not served by this build* |
| `GET /_cluster/ops/:op_id` | intent state: pending / applied / failed (Chapter 4) |
| `GET /_cluster/health` | rolled-up diagnostics — including `blob_fetch_stall`, non-`null` while this node's apply is parked on a sideloaded blob it cannot fetch from any member (#439). Since **D-51** (#486) a member can also serve a referenced blob out of applied state, so a stall now means the blob is referenced by *this* node's parked entry and by no live state anywhere — in practice a digest whose own delete sits behind the parked entry (#480). The fleet projection `GET /_fleet/health` adds `blob_fetch_stalls_fleet`, one row per stalled voter. A stall is *degraded*, not *not-ready*: the node stays in the load balancer and self-heals when a holder returns, but every committed write behind the parked entry is unapplied on that node until it does |
| `GET /_cluster/route-hits` | this node's per-route dispatch counts, in memory since process start — the node-local input the admin port's `GET /front-door/route-hits` sums across the fleet |

**Metrics that page** (Prometheus, served by the standard metrics port).

This list mixes families that exist today with families that were designed here
and never registered. The distinction is not pedantry: an alert rule naming an
unregistered family is not a loud failure but a silent one — the expression
evaluates to no data forever, so the alert never fires and reads as health. The
shipped alert pack in `deploy/observability/` therefore references only the
first group, and `scripts/check-observability-families.sh` enforces that.

*Registered, and alerted on by the shipped pack:*
`rift_cluster_intents_pending` (stuck > minutes = quorum loss),
`rift_cluster_insecure` (should be 0 everywhere, forever),
`rift_cluster_members{state}`, `rift_cluster_barrier_timeouts_total`,
`rift_cluster_bind_failures`, `rift_cluster_no_principals`, the
`rift_cluster_audit_export_*` family, and
`rift_cluster_source_scheduler_read_failures_total` /
`rift_cluster_source_scheduler_corrupt_rows`.

*Registered by #470, alert threshold still to be chosen:*
`rift_cluster_isolated` (gauge, `1` while this node cannot see the quorum and is
refusing owner-side operations — proxyOnce claims under D-40, flow-KV owner
writes and strong reads under D-17). This is the *condition*, not a symptom, and
it is deliberately the thing to alert on: the symptom counters are incomplete by
design (`rift_cluster_cas_conflicts_total{reason="isolated"}` counts write
refusals only, so a read-heavy workload can trip the rule continuously and move
nothing). `isolated == 1 for 2m` is the obvious rule — longer than an election,
short enough to catch a real partition — but a threshold shipped without being
tried against a real fleet's election noise is a page nobody trusts, so the
choice is left explicit rather than guessed. Note the gauge publishes `0` on a
healthy node rather than being absent, so such a rule is falsifiable from the
first scrape.

*Registered by #439, alert threshold still to be chosen:*
`rift_cluster_blob_fetch_stalled` (gauge, `1` while this node's apply is parked
on a blob no member can supply — the metric form of `blob_fetch_stall` above;
`stalled for > 5m` is the obvious rule, and a stall that outlives every
plausible transient is a lost blob, which is an operator's problem, not a
retry's) and `rift_cluster_blob_fetch_stalls_total` (counter, one per stall
onset — a rising count on a fleet with no partitions says a blob is being
reaped before its op commits, which is the #438 pin failing).

*Registered by #480, no alert:* `rift_cluster_blob_gc_retained` (gauge, the
number of unreferenced blobs this node is holding back under the tombstone
rules — because its own log has not been purged past the index at which they
stopped being referenced (**D-52** rule A), or because some member of the fleet
has not yet *applied* past it (**D-55** rule C)). Not a fault signal: a non-zero
value is retention working as designed, and it falls to zero on its own as the
log compacts and the fleet catches up. Worth a dashboard line rather than an
alert, because a value that stays high while `purged` advances *and* every
member is caught up is the one shape that would suggest the tombstone table is
not being cleared.

Rule C is read, not gossiped: each GC sweep (every 60 s) asks every member —
voters and learners — for its applied index over the cluster port, and **fails
closed** when any of them cannot be read. The sweep then logs, at `warn`,
`blob gc: fleet applied floor unknown` naming the member ids it could not reach,
and retains every tombstoned blob on this node until they answer or leave the
membership. That line repeating once a minute is the expected shape of a fleet
with a down member, not a GC fault: it clears on its own when the member returns
(and applies the deletes it missed) or is evicted (D-21/D-26). What it costs in
the meantime is disk that grows with delete churn for as long as the member is
down — every dataset or spec deleted or overwritten during the outage stays on
every holder, and the tombstone table stops being pruned for the same window —
and it is **not** capped by the dataset quota, which bounds live data only. So
treat the line as a clock, not as noise: once it has repeated for longer than
you would tolerate that member being absent, evict it. A member that will never
return has to be evicted anyway; this is one more reason not to leave it in the
membership.

Two things this changes for the `blob_fetch_stall` runbook above. A stall now
means the blob is held by nobody *and* referenced by no live state anywhere —
in practice a digest whose own delete sits behind the parked entry — because a
blob that is merely unreferenced is retained until the log passes it, and one
that is being actively requested is never reaped at all. And **compaction is not
a remedy**: a parked apply blocks openraft's state-machine worker, so the
snapshot that would let the node skip the blob queues behind the park and never
runs (D-48 as amended by D-52). Waiting does not clear a stall; a holder
returning, or the out-of-band repair, does.

*Registered by #514, no alert yet:* `rift_cluster_sequence_resets_incomplete_total`
(counter, cursor resets that did not reach every member — **D-57**). Unlike
`rift_cluster_sequence_fallbacks_total`, which counts a decision degrading as
designed, this one is a fault signal: the member named in the `sequencer reset
did not reach every member` warning beside it is still cycling responses for a
stub that was deleted or replaced, and it will keep doing so until it is asked
again or the membership changes. A non-zero value with no member down is worth
looking at; the warning names which member to look at.

*Registered, but not alerted on:* `rift_cluster_flow_wal_lag_ops` (async
durability backlog). It is a legitimate paging signal, but it appears in
neither the alert pack nor the shipped dashboards yet: nobody has chosen a
threshold that is meaningful across deployments, and an alert with an
arbitrary one trains people to ignore it.

*Aspirational — designed, not registered, deliberately absent from the pack:*
`rift_cluster_raft_leader_changes_total` (flapping = network trouble),
`rift_cluster_applied_index_lag` per node (barrier stragglers),
`rift_cluster_degraded_ops_total{feature}` (any nonzero during a strict test
run is a finding), `rift_cluster_bridge_rejected_total` (owner black-hole
shedding). Leader flapping is the sharpest of these and has no substitute today;
`RiftClusterNoLeader` catches the outage but not the flap that preceded it.

## Runbooks (sketches; full versions ship with the harness)

The `cluster …` subcommands sketched below are **not built**: the binary has
no membership or recovery subcommand today (membership changes only through a
node joining or leaving — D-25, D-26; the cluster maintains its own log and
snapshots — D-24). They are kept as the design of what a crash-retire and a
majority-loss recovery would have to look like.

- **Scale up**: start pod with seeds → auto learner → voter if < 9
  (`MAX_AUTO_VOTERS`, D-27). Nothing else.
- **Scale down / retire**: SIGTERM, wait for exit (graceful leave does the
  rest; the leader refuses a leave that would drop the voter set below two —
  D-25). Crash-retire: `rift-cluster-server cluster remove-node <id>` against
  any live node *(sketch — not built)*.
- **Restore quorum after majority loss**: last-resort
  `cluster force-recover --from-state-dir` on the best surviving node (log
  end inspected via `cluster inspect`), then rejoin others empty. Documented
  as data-loss-possible, operator-confirmed, twice *(sketch — not built)*.
- **Backup**: configs are exportable at any moment via `GET /imposters`
  (Mountebank-compatible JSON) or the core `--datadir` write-through; the
  state dir itself is snapshot-friendly (redb single file, crash-consistent).
- **Stuck scenario triage** (once `/_cluster/ring` and `/_cluster/kv` ship —
  see the endpoint table): `/_cluster/ring?key=flow` → `/_cluster/kv/:flow`
  → compare owner vs replica `(m_idx, v)` → the answer is one of: owner
  isolated (heartbeat metric), adoption reset (degraded counter), or the test
  actually didn't send the transition. Three checks, no log spelunking.

## Rolling upgrades

One node at a time, SIGTERM-driven; the invariants that make it boring:
protocol majors must match to join (clean refusal otherwise), gossip/RPC
fields are additive within a major, config bodies tolerate unknown fields
(so an old node can apply a new node's config — it reports rather than
crashes on genuinely new required semantics), and graceful leave means no
election and no ownership guess per step. Sequence cursors still reset on
ownership moves (D-8) — schedule upgrades between test runs, stated in the
docs rather than discovered in one.

**Sideloaded blobs (#439, D-49; gated since #481, D-53): nothing to do.** A
roll no longer has an ordering requirement. The write path strips a
`DatasetPut`/`SpecPut`'s bytes only once every member of the committed ∪
effective configuration is known to apply a digest-only op — a capability the
fan-out learns for free from the `?stat` probe it already makes. Until then
the op is committed **with its bytes**, the shape every build can decode, so a
mixed fleet is slower for the duration of the roll and never wedged.

*This paragraph used to say the opposite* — "upgrade the whole fleet before the
first dataset or spec write", on the grounds that an old build "fails closed at
apply". That description was wrong, and wrong in the dangerous direction. Log
entries are decoded in the **log store**, not at apply
(`RedbLogStore::try_get_log_entries`), and a decode failure there is a
`StorageError` that is fatal to openraft's core: the node's Raft runtime stops.
One routine write during a roll therefore took down every not-yet-upgraded
member, and if those were a majority, the fleet lost quorum. Recorded here
because a fleet still running a build older than #481 has that behaviour, and
the runbook that told operators it was a soft failure was the reason it looked
safe.

**Watch `rift_cluster_blob_sideload_deferred_total`.** Non-zero during a roll is
the mechanism working. Non-zero *after* one is the state worth acting on: some
member is not known capable, so every write is carrying its bytes on the log —
the load the #432 epic exists to remove. The warning that accompanies it names
the members. The usual cause is a member that is down and has never been probed
by the current leader; since membership changes only through a node joining or
leaving (D-21), a permanently dead member holds the fleet in this state until it
is removed. A leader failover also clears the learned set, so a single deferred
write immediately after one is expected and self-correcting.

A member whose build cannot serve blobs *at all* (pre-#437) is separately
reported as *skewed* in the fan-out refusal and in `blob_fetch_stall`, distinct
from a partition, so a half-finished roll still reads as what it is.

**Downgrades across this boundary are out of contract.** An observed capability
is remembered for as long as the member stays in the membership, so restarting a
member in place onto an older binary is not something the gate can catch.

## Sizing rules of thumb

3 voters for HA (survives 1), 5 for comfort (survives 2); learners beyond 9
add data-plane capacity without consensus weight. Per node: flow shard ≤ 100k
entries (LRU-shed above), journal ≤ ports × shard cap × avg entry, config SM =
fleet config size (small), Raft log bounded by snapshot cadence. Disk: state
dir on real block storage (fsync latency is the `sync`-durability floor);
tens of GB is generous. Network: everything assumes single-DC LAN — the
timeouts are wrong for WAN by design.
