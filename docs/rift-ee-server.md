# `rift-ee-server` — the enterprise binary

`rift-ee-server` is the Rift server with enterprise clustering. It is a
*composition*, not a fork: it hands the open-source `ServerBuilder` the same CLI
the `rift` binary would, and adds cluster backends through the upstream
embedding seams. With clustering off it is the open-source server, byte for
byte — the same admin API, the same imposters, the same ports, and nothing
extra bound.

That claim is verified, not merely made by construction. The `parity` CI job
(issue #37) builds `rift-ee-server` and runs upstream's own `rift-http-proxy`
process-spawning suites — `mountebank_compatibility`, `rift_extensions`,
`issue_360_script_cli`, `corpus_replay` — against it with `--cluster` off,
using the `RIFT_SERVER_BIN` override (`vendor/rift`'s `tests/support/mod.rs`)
to point them at this binary instead of their own debug build. `tests/
passthrough.rs` in this crate covers the same claim at the admin-API level and
runs on every PR regardless of what changed; `parity` is the path-gated
counterpart that checks it against upstream's own, much larger behavioural
suite whenever `vendor/rift`, `crates/rift-ee-server/`, `Cargo.lock`, or
`.github/` change. (The rest of `rift-http-proxy`'s integration tests link
`ImposterManager`/`AdminApiServer` directly and can never be pointed at an
external binary — but they also don't need to be: this crate links that exact
vendored library unmodified, so upstream's own CI already gates that code.)

```sh
# Exactly the open-source behaviour.
rift-ee-server --port 2525 --datadir ./data

# The same, as one node of a cluster.
rift-ee-server --port 2525 --datadir ./data \
  --cluster --cluster-bind 10.0.0.7:4790 \
  --cluster-secret-file /etc/rift/cluster-secret \
  --cluster-seeds rift-headless.default.svc.cluster.local:4790
```

## Identifying a build

```
$ rift-ee-server --version
rift-ee-server 0.1.0 (enterprise, rift v0.16.0)
```

Three things, all of which matter on a bug report: this build's version, the
edition (which says which code paths exist at all), and **which open-source Rift
is embedded**. That last one is the vendored submodule's pin, not a crate
version — every crate under `vendor/rift` inherits `0.1.0` from that workspace,
so their versions identify nothing. A build where the pin could not be
determined (a source tarball, an image without `git`) reports `rift unknown`
rather than a plausible-looking wrong version.

The same string is logged at startup.

## Relationship to the `rift` binary

Every open-source flag and subcommand parses here; a test in `tests/cli.rs`
fails the build if that ever stops being true.

`stop`, `restart`, `save` and `--rcfile` used to be declined with an explanatory
error, because the open-source binary implemented them in private functions of
its own `main.rs` rather than behind a library seam — copying them would have
forked behaviour that is meant to stay shared. Upstream promoted them to
`rift_http_proxy::bootstrap` (rift#807), so this binary now **calls the same
implementation** rather than reimplementing or declining it:

| Subcommand / flag | Behaviour |
|---|---|
| `--rcfile` | Mountebank-compatible JSON defaults, applied only to fields left at their defaults. A missing or malformed rcfile warns and startup continues, exactly as upstream (the warning goes to stderr immediately and is repeated through `tracing` once the subscriber exists). |
| `--pidfile` | one `global` flag, bindable on either side of the subcommand; written on the serving path only — see below |
| `stop` | SIGTERM the PID in `--pidfile` (default `rift.pid`), then remove the file |
| `restart` | `stop`, then start a new server in the same process. A missing PID file is "nothing to stop", not an error |
| `save` | fetch `GET /imposters?replayable=true` from the configured `--host`/`--port` and write it to `--savefile` |
| `replay` | start a server with `--configfile` set to the replayed file, overriding any top-level `--configfile`. Refused with `--cluster` — see below |

The steps this binary implements run in **upstream's order**, which matters
because the order is observable: `--rcfile` is applied before tracing
initialises, or an rcfile carrying `logLevel` would be silently ignored.

Two more flags now match upstream exactly, both fixed the same way upstream
fixes them:

- `--debug` sets the tracing filter to `debug` **and** sets `RIFT_DEBUG=1` for
  the engine, which reads that variable directly (cached in a `OnceLock`, so it
  is set before the runtime starts). This is what makes an unknown template
  token in a stub response a loud `500` carrying `x-rift-template-error`
  instead of silently substituting empty — `rift --debug` and
  `rift-ee-server --debug` now render the same imposter identically.
- `--log <path>` adds a file layer (via `tracing-appender`, non-blocking,
  never-rotated) alongside the console output. `--nologfile` overrides `--log`
  and suppresses the file even when one is named.

`script`, `healthcheck`, `start` and `replay` all work exactly as upstream.
`replay` is worth spelling out because it does less than the name suggests: it is
`start` with `--configfile` pointed at the replayed file, and nothing else. In
particular it performs no `removeProxies` transformation — that belongs to
`save`, which is where upstream does it too.

One deliberate divergence: **`replay` is refused with `--cluster`**. It loads a
file straight into one node's engine, so clustered those imposters would never
reach the replicated log — and the reconciler, which treats the replicated set
as authoritative, would then delete them. A saved file is already a
`PUT /imposters` body, so replay it through the admin API instead and the write
replicates.

`--configfile` is refused under `--cluster` for the same reason and by the same
error, wherever it comes from (#85). The check sits in the composition rather
than in `replay`'s dispatch, so it also covers a bare `--configfile` and an
embedder calling `compose::start` directly — `replay`'s refusal is now one
spelling of a general rule rather than the only guard.

`--datadir` is **not** refused: it legitimately anchors the cluster state
directory (below), and under `--cluster` its imposters are not loaded — the
directory's `{port}.json` files are left untouched and nothing from them is
bound or listed. A regression test pins that, because the suppression is a
property of the clustered composition rather than of an explicit guard.

### PID-file semantics (upstream behaviour, reproduced)

`--pidfile` is a single `global` flag: `--pidfile p restart` and
`restart --pidfile p` name the same file. The PID file is written on the
**serving** path only, after the subcommand dispatch — so:

- `rift-ee-server --pidfile p restart` stops the server recorded in `p` and the
  server it then starts writes its own PID back to `p`.
- A transient `save` or `healthcheck` never touches a running server's PID file.
- `stop`/`restart` fall back to `rift.pid` when `--pidfile` is absent. The
  default is applied at dispatch rather than on the flag, so a plain start still
  writes no PID file unless one was asked for.
- `stop` with no PID file is an error; `restart` with no PID file starts fresh,
  since "end up running" is already satisfiable.

These are upstream's semantics (rift#827), reproduced rather than reinvented,
because `--cluster`-off parity is the point. They replaced a set of caveats — a
`restart` that SIGTERMed itself, a `save` that clobbered a live PID file — that
this binary previously reproduced faithfully.

One deliberate divergence, on the safe side: a PID file whose contents are not a
positive integer is **refused** rather than passed to `kill(2)`, where `0` means
"every process in my process group" and `-1` means "every process I may signal".
The check is *advisory* — upstream re-reads and re-parses the file before
signalling, so it defends a truncated or hand-edited file, not a concurrent
writer. Tracked upstream as rift#822; the guard goes away when that lands.

## Cluster flags

The master switch is `--cluster`; every other `--cluster*` flag is inert without
it, so a stray flag on a single node is not an error.

| Flag | Meaning |
|---|---|
| `--cluster` | Run this node as part of a cluster |
| `--cluster-bind <ADDR>` | Address to bind the cluster port on. **Required** — there is no default, because the cluster port must be a deliberate decision |
| `--cluster-bind-public-ok` | Acknowledge that `--cluster-bind` names a publicly reachable interface |
| `--cluster-advertise <HOST:PORT>` | Address peers dial, when it differs from the bind (NAT, port mapping, a pod behind a service). Accepts a hostname as well as a literal address (IPv6 literals must be bracketed, e.g. `[::1]:4790`); a hostname is re-resolved on every send, so it stays valid as DNS changes. A name resolving to several addresses (dual-stack A + AAAA, a multi-A headless service) is dialed at each in the resolver's own order until one answers — the peer counts as unreachable only when all of them are |
| `--cluster-seeds <ADDR[,ADDR...]>` | Existing members to join through. Retried and re-resolved for up to 30s, so a node that starts before its seeds during a rolling deploy still joins |
| `--cluster-allow-solo` | Found a new single-node cluster when no seeds are given |
| `--cluster-secret <SECRET>` | Shared secret authenticating the cluster port |
| `--cluster-secret-file <FILE>` | The same, from a file (trailing whitespace trimmed) |
| `--cluster-insecure` | Run the cluster port unauthenticated |
| `--cluster-state-dir <DIR>` | Cluster state: identity, Raft log, snapshots. Defaults to `<datadir>/_cluster` |
| `--cluster-node-name <NAME>` | Operator-facing node name; seeds the first node id only |
| `--cluster-leave-timeout <SECONDS>` | Drain window after SIGTERM (default `10`) |
| `--cluster-probe-bind <ADDR>` | Address for `/readyz` and `/healthz` (default `0.0.0.0:2526`) |
| `--cluster-write-barrier <MODE>` | What a committed admin write waits for before its 2xx: `ready-nodes` (default — every Ready node has applied it, so any node serves it) or `none` (committed and applied locally) |
| `--cluster-write-barrier-timeout <SECONDS>` | How long the barrier waits (default `2`) before answering anyway with a `Rift-Cluster-Warnings: unapplied=<node,…>` header |
| `--cluster-admin-async` | Answer admin writes with an immediate `202` + op id after durably parking them; poll `GET /_cluster/ops/:id` for the outcome |
| `--cluster-flow-fsync-interval-ms <MILLIS>` | Group-fsync cadence for `durability: "async"` flow-state writes (default `50`) — the bound on what a whole-fleet crash can lose for imposters that did not choose `"sync"` or `"none"` |

Each flag also has an environment-variable spelling (`RIFT_CLUSTER_BIND`,
`RIFT_CLUSTER_SECRET_FILE`, …), which is the intended vehicle for the secret.

### Startup guards

These run **before anything binds**, and each exists because the alternative is
a fleet that looks healthy and is quietly wrong:

- `--cluster` without `--cluster-bind` → refused. The cluster port is explicit.
- `--cluster-bind` on a public or wildcard address without
  `--cluster-bind-public-ok` → refused. The threat model delegates
  confidentiality to network isolation, so anything not positively private fails
  closed.
- `--cluster` without a secret and without `--cluster-insecure` → refused.
- `--cluster-secret-file` that cannot be read, or is empty → refused, naming the
  file. An unreadable secret never degrades into "no secret".
- `--cluster` with `--runtime per-core` → refused (decision D-14). The state
  bridge parks caller threads, and a per-core worker has only one thread to
  park, so a single owner outage would stall every connection pinned to it.
- `--cluster` with the TLS-MITM intercept listener (`--intercept-port` or an
  `intercept` block in the config file) → refused. Intercept state is per-node
  and is not replicated, so a clustered fleet would answer the same client
  differently depending on which node it reached.
- `--cluster` with no seeds and no `--cluster-allow-solo` → refused, rather than
  silently founding a second cluster beside the real one.

## Probes

The probe listener is unauthenticated and, unlike the admin API, is bound only
when `--cluster` is on:

- **`GET /healthz`** — liveness. `200` for as long as the process is serving,
  including while it is still converging and while it is draining. Restarting a
  converging node only slows the convergence down.
- **`GET /readyz`** — the load-balancer gate. `200` only once every startup gate
  has reported in; `503` otherwise, with the outstanding gates named:

  ```json
  { "status": "not-ready", "pending": ["cluster-joined"] }
  ```

The latch is closed until proven open, and **draining is terminal**: once a
graceful leave begins, `/readyz` reports `{"status":"draining"}` and no late
gate can re-open it.

## Graceful leave (SIGTERM)

On SIGTERM the node, in this order:

1. **fails readiness**, so the balancer sheds it before any socket closes;
2. **leaves the Raft membership** — demote-then-remove, performed by the leader
   (the departing node asks it over the cluster port). Until that commits the
   fleet still counts this node toward quorum, so leaving before the drain is
   what keeps a rolling restart from shrinking the effective quorum. The leader
   **refuses** a departure that would leave fewer than **two voters**; the node
   then drains and exits while still a member, and its next start resumes (see
   [The voter floor](#the-voter-floor));
3. **records the departure** in the state directory (a `departed` marker file),
   so the next start knows to rejoin rather than resume — see
   [Restarting a node](#restarting-a-node). Written only when the cluster
   actually accepted the departure;
4. **drains** whatever is left of the `--cluster-leave-timeout` window;
5. **closes the listeners** and stops the control-plane node.

Closing sockets first would turn every in-flight request into a client-visible
error, which is exactly what the leave exists to avoid.

Steps 2 and 3 **share** the `--cluster-leave-timeout` budget rather than taking
it each, so the total stays inside the window the orchestrator's grace period is
sized against. A node that cannot reach a leader within the budget logs a warning
and exits anyway — the cluster then handles it as a dead node, which is strictly
better than a shutdown that hangs.

If the departing node is itself the **leader**, leadership moves as part of the
departure: openraft keeps a node leading while it is merely demoted to learner,
and hands off once the second write drops it from membership entirely.

> **Set the orchestrator's grace period to at least twice
> `--cluster-leave-timeout`** (`terminationGracePeriodSeconds` on Kubernetes).
> A shorter period kills the process mid-drain and turns a graceful leave into a
> hard one.

Manifests that encode this rule — a Dockerfile, a 3-node compose cluster, and a
Kubernetes StatefulSet with both probes wired — live in
[`deploy/`](../deploy/README.md).

### The voter floor

A departure that would leave the cluster with fewer than **two voters** is
refused. The node still drains and exits — its exit is crash-equivalent — but it
keeps its place in the membership, and its next start resumes from the durable
log rather than rejoining.

This exists because a whole-fleet teardown (`docker compose stop`,
`kubectl delete statefulset`, scale-to-0) SIGTERMs every node, and without a
floor each one leaves in turn: a three-node membership walks 3 → 2 → 1. That
ends the outage with the entire control plane — configs, dedup state, parked
intents — on a **single** authoritative volume, and a cold start that cannot
make progress until exactly that node returns, even though two other volumes
hold near-complete copies.

Consequences worth knowing:

- **A rolling restart is unaffected.** One node leaves at a time and the fleet
  never drops below two, so every departure lands as before.
- **A two-node cluster can no longer shed a voter at all.** Both of its nodes
  are load-bearing; every graceful leave is refused and every node resumes.
- **Learners are never refused.** Removing one costs no quorum member.
- **A cold start after a full teardown now needs two nodes back, not one.**
  This is the trade, and it is worth stating plainly: before the floor, a
  teardown ended with a single voter and that one node could restart and serve
  alone. Now it ends with two, and a quorum of two needs *both*. You gain a
  second authoritative copy of the control plane; you lose the ability to
  recover from a teardown with only one volume intact. If you have genuinely
  lost a volume, the surviving node's state directory is still complete — the
  recovery is to start it fresh as a new cluster and let the others seed-join,
  not to wait for a node that is never coming back.
- The floor is enforced by the leader, under the same lock that makes the
  auto-voter ceiling exact, so two nodes SIGTERMed at the same instant cannot
  both slip through. Across a *leader failover* mid-teardown it is best-effort:
  two leaders can each judge one departure. openraft's refusal to commit an
  empty voter set is the hard backstop underneath, and a node that ends up
  outside the membership rejoins per [Restarting a node](#restarting-a-node).

Refusals are logged at info, not as errors — the cluster declined on purpose:

```
still a member on exit — the cluster kept this node's vote; the next start
resumes from the durable log
```

## Restarting a node

A restarting node either **resumes** its place in the membership or **rejoins**
through its seeds, and it decides from three inputs: whether its state directory
carries a cluster (`initialized`), whether that directory holds a `departed`
marker from a graceful leave, and whether its own id is still in the membership
its durable log carries.

| initialized | `departed` marker | still in membership | somewhere to rejoin | what happens |
|---|---|---|---|---|
| no | — | — | seeds given | seed-join |
| no | — | — | no seeds | founds a cluster, but only with `--cluster-allow-solo` |
| yes | no | yes | — | **resume** from the durable log — the cold-start path |
| yes | yes | any | yes | **rejoin**, state retained |
| yes | no | no | yes | **rejoin**, state retained |
| yes | yes / not a member | — | nothing | **start is refused**, naming both recoveries |

"Somewhere to rejoin" is `--cluster-seeds` **plus every other member the node's
own durable log remembers**, seeds first. That second half is not a nicety: the
node that *founds* a cluster has no seeds by construction — there was nothing to
seed from — so after a graceful leave it would have no way back at all. Its log
still names the peers that outlived it, and that is what it asks.

Two more things that are easy to get wrong:

- **The state directory is never wiped.** It holds the applied state, parked
  intents and this node's minted identity, so a rejoin reuses the retained log —
  which is a prefix of the cluster's — and catches up by ordinary replication.
  If you genuinely want a clean node, delete the directory yourself; that takes
  the first row.
- **"Still in membership" is local knowledge and can be stale.** A departing
  node often never receives the entry that removes it, so its own log still
  lists it. That is why the marker exists, and why a node that resumes but then
  sees **no leader at all for 60 s** starts offering itself to its seeds again —
  the last-resort path for a node evicted while it was down.

A node whose start is refused shows this shape:

```
this node is no longer part of the cluster it holds state for, and its log
names no surviving peer to rejoin through; give it --cluster-seeds to rejoin,
or delete <state-dir> to start it fresh
```

## The `/_cluster/*` operator surface

These ride the **cluster port** and require the cluster credential — not the
admin API key — because they answer questions about the fleet and the cluster
port already authenticates exactly that audience. Every answer comes from *this
node's* applied state, so comparing two nodes' answers is what tells you whether
the fleet has converged.

| Endpoint | Reports |
|---|---|
| `GET /_cluster/members` | node id, leadership, current leader, last applied index, voters |
| `GET /_cluster/config` | the ports this node has a committed config for |
| `GET /_cluster/imposters` | those ports with their committed config bodies |
| `GET /_cluster/health` | readiness state and pending gates, whether this node is an isolated owner, and the ownership ring (`m_idx` + members) |

`/_cluster/ring` and `/_cluster/kv` arrive with later phases.

## Metrics

Under `--cluster` the node publishes fleet gauges on the existing metrics port
(`--metrics-port`, default 9090), alongside the open-source metrics — they are
registered into the same Prometheus registry `GET /metrics` already serves, so
there is nothing extra to scrape:

| Metric | Meaning |
|---|---|
| `rift_cluster_members{state="voter"}` | size of the effective voter set as this node sees it |
| `rift_cluster_members{state="leader"}` | `1` on the leader, `0` elsewhere — summing it across the fleet answers "is there exactly one leader?" |
| `rift_cluster_ring_epoch` | membership log index the ownership ring is derived from; two nodes reporting different epochs have not converged |
| `rift_cluster_insecure` | `1` when this node's cluster port runs unauthenticated, so a fleet can be audited for it |

These are sampled from Raft metrics every 5s rather than pushed, because
leadership and membership change without the cluster crate being called; an
event-driven gauge would silently go stale. (If you are asserting on them right
after a node reports ready, poll — the gauge can lag readiness by one sample.)

They reach `/metrics` by registering into the `prometheus` crate's global
default registry, which is what the open-source metrics server already serves.
That works only while the whole build links **one** copy of that crate; a second
one would carry its own registry and these gauges would silently reach no
endpoint. `scripts/check-single-prometheus.sh` enforces it in CI.

The config-sync families (issue #9): `rift_cluster_config_revision{port}`
(applied revision per imposter — two nodes disagreeing have not converged),
`rift_cluster_bind_failures{port}` (1 while a committed config cannot be
realized locally; resampled after every engine drive, so healing clears it),
`rift_cluster_write_forwards_total`, `rift_cluster_barrier_waits_total` /
`rift_cluster_barrier_timeouts_total`, `rift_cluster_dedup_hits_total`, and
the R4 ledger's `rift_cluster_intents_parked_total` / `_replayed_total` /
`rift_cluster_intents_pending` (resampled by every replay sweep). The
Phase-1 plan's `rift_cluster_config_converged` and
`rift_cluster_config_conflicts_total` are still pending — convergence is a
fleet-level derivation (compare `config_revision` across nodes) and conflicts
cannot exist until a non-Raft write mode does.

The pull-on-miss family (issue #49): `rift_cluster_pull_on_miss_checks_total`
(no-match requests the net evaluated), `_lagging_total` (of those, the ones
that found this node behind the leader) and `_retries_total` (requests sent
back through the matcher once). The useful reading is the ratio: `lagging /
checks` persistently high means followers are serving while behind, which is a
readiness-gate question rather than a matcher one. There is deliberately no
`rescues_total` — the hook cannot see the retry's outcome, so such a counter
would be a guess; use the response header below as rescue evidence.

## Response headers

Under `--cluster`, cluster-aware code annotates a request and the enterprise
response decorator turns those notes into `Rift-Cluster-*` headers at the
response boundary — so the open-source handlers stay entirely cluster-unaware.
The mapping is structural: an annotation `cluster.revision` becomes
`Rift-Cluster-Revision`. Repeated notes (warnings, above all) are appended as
separate header lines rather than collapsed.

`Rift-Cluster-Pull-On-Miss` is stamped when the pull-on-miss net (below) sent a
request back through the matcher: `rescued-wait` when this node caught up to the
leader within the budget, `retry-after-timeout` when it did not and the retry
happened anyway. Its absence is the normal case — the header appears only on
requests that missed *and* found this node lagging.

## The pull-on-miss safety net

A follower that falls behind **after** it has gone Ready is still in rotation,
and a request for an imposter it has not applied yet would be answered as a
no-match. The default `ready-nodes` write barrier (a 2xx implies fleet-wide
apply) and the `cluster-reconciled` readiness gate (a catching-up node takes no
traffic) both narrow that window; neither closes it.

So on a **genuine no-match, and only then**, a clustered node asks whether it is
behind the leader. If it is, it waits up to a **500 ms** total budget for the
apply and asks the matcher to try exactly once more. The budget is not
configurable: it bounds how much slower an already-failing request can get, and
a knob there would be a knob for making misses arbitrarily slow.

Three properties worth stating plainly:

- **Matched requests are untouched.** The upstream seam is consulted only after
  matching has already returned no hit, so the hot path never reaches this code.
- **Every uncertainty proceeds.** No leader known, an unreachable leader, a
  budget that expired before lag was confirmed — each answers exactly as a fleet
  without the net would. It never parks a request on a leader that cannot reply.
- **A burst costs one leader lookup, not one per request.** The catch-up target
  is cached for 250 ms and the lookup is single-flight, so concurrent missers
  queue behind one RPC rather than each issuing their own — a lagging follower
  under load does not turn its lag into an RPC storm.

One cost is **not** hidden, because it is real: a retry re-runs the whole
matching pass, so a predicate `inject` script executes a second time and its
persistent `state` mutations and `logger` output are committed twice. The honest
worst case for a rescued request is the 500 ms budget **plus** a second
`scriptEngine.timeoutMs`. For imposters whose predicates are pure this is
invisible; for imposters whose predicates mutate state through `inject`, it is a
duplicate mutation on every rescue, and worth knowing before relying on the net.

A request to a port with **no imposter at all** is still outside this net: it
reaches no imposter handler, so there is nothing to hook. Readiness gating is
what covers that window.

## The clustered admin write path

Under `--cluster`, the public admin address is served by a thin front: the
config-mutating routes (`POST/PUT/DELETE /imposters`, `DELETE
/imposters/:port`, stub CRUD, and `POST /imposters/:port/{enable,disable}`)
become replicated control ops committed through the Raft leader — submitted on
any node, forwarded automatically — and everything else (reads, scenario
state, recorded requests) is reverse-proxied to the local engine unchanged. A
pause replicates and survives restarts (upstream #817): it applies in place on
every node, so the paused imposter's scenario state is intact on resume. A 2xx from a mutating route
means the write is durable on a majority and, with the default
`--cluster-write-barrier=ready-nodes`, applied on every Ready node; if the
barrier times out the response still succeeds and names the lagging nodes in
`Rift-Cluster-Warnings`. Every mutating response carries
`Rift-Cluster-Revision` (`<tenant>:<port>@<log-index>`) and
`Rift-Cluster-Op-Id`.

Acceptance is never lost (R4): every mutation is durably parked on the
accepting node *before* it is submitted. With no reachable leader the write
answers `503` (`unavailable` type) with `Retry-After` and its
`Rift-Cluster-Op-Id` — and the parked intent is replayed automatically when a
leader returns, on startup, and on every leader change; the op-id dedup in the
state machine makes replay exactly-once-in-effect. A client-supplied
`Idempotency-Key` header becomes the op id (verbatim when it is a UUID, hashed
into one otherwise), so retrying the same key can never double-apply.
`GET /_cluster/ops/:id` on the cluster port reports
`{"state": "pending" | "applied" | "failed", "revision"?, "detail"?}` for any
accepted op (404 once the 24 h dedup window has lapsed). With
`--cluster-admin-async`, mutations answer
`202 {"opId": …, "opIds": […]}` immediately after parking and the commit
happens in the background — poll each entry of `opIds` (a multi-op mutation
such as `PUT /imposters` commits several ops; `opId` is the correlation id).

A parked op whose commit fails — in either mode — is retried by the replay
loop, which is woken as soon as that happens rather than waiting for its
periodic sweep. So a transient failure costs roughly one submit round-trip, not
the sweep interval. The sweep remains the backstop for anything that fails
without a live node to notice it (a crash between parking and submitting).

Cluster-mode divergences from a single node: an imposter must carry an explicit
`port` (an auto-assigned port cannot replicate). `_rift.script` `file:`/`ref:`
sources resolve on the node that accepts the write, under that node's
`--scripts-dir`, before replication (upstream #356) — followers receive
inline code and need no scripts dir of their own, so deploy script files to
every node that can receive admin writes. A resolution failure is upstream's
400 (`bad data` type, `Script resolution failed: …` message). What resolution
produces is then compiled before it is accepted, so a script that resolves but
does not parse is refused the same way (`Script validation failed: …`) instead
of replicating and failing at bind time on every node — upstream's admin-time
gate, applied to the clustered path. Without `--allowInjection` nothing about
either step changes — the injection gate still refuses all script surfaces
first, before resolution ever runs. Concurrent
writers to the *same* imposter are last-writer-wins by default; a
single-imposter write may carry an `If-Match` header to condition on the
record's current revision instead — either the exact `Rift-Cluster-Revision`
value (`default:<port>@<revision>`) or a bare revision integer, optionally
quoted like a normal ETag. A stale or mismatched `If-Match` is refused with
`409` (`resource conflict` type, message starting `revision conflict`); a
collection-wide mutation (`PUT /imposters`, `DELETE /imposters`) has no single
record to condition on and refuses an `If-Match` with `400` (`bad data`
type). The precondition is checked inside the state machine's `apply`, so it
holds even when the write is accepted by a follower and forwarded to the
leader. One residual window remains: the precondition guards the record's
*revision*, not the accepting node's read basis — an index-addressed stub edit
conditioned on the current revision but accepted by a node whose applied state
still lags that revision is synthesized from the stale local read and passes
the check. The default `ready-nodes` write barrier keeps that window to the
barrier timeout; route conditioned index-addressed edits to the leader (or
prefer by-id stub edits, which replicate only the edited stub) when it
matters. A keyed retry (the same `Idempotency-Key`) of a `409` dedups to that
same `409` by design — the op-id dedup returns the original response, it does
not re-evaluate the precondition — so recovering from a conflict means
re-reading the current revision and retrying with a **fresh** key. Mixed-version
rollout: a replica still running a pre-#46 binary ignores `expected_revision`
and applies unconditionally, so don't send `If-Match` from any client until
every node in the fleet has upgraded. `PUT /imposters` commits as a sequence
(upserts first, then prunes), so a write interrupted by a leadership change
can transiently leave a superset of old and new imposters — a retry converges
it.

A node is not Ready until its `cluster-reconciled` gate opens: its applied
state has caught up to the leader's and its imposters are bound (or their
failures reported on `GET /_cluster/imposters`).

## The clustered front door (#131)

Upstream's front door (`--front-door <ADDR>`, env `RIFT_FRONT_DOOR`; a
`HOST:PORT` or a bare port meaning every interface) resolves a request against
a content-based route table and dispatches it to an imposter port — U-11's
listener and matcher. Its admin CRUD was deliberately deferred upstream, so
under `--cluster` this binary provides it: the route table is a **replicated
control-plane object**, exactly like the imposter config set.

`--front-door` is not a `--cluster*` flag — `EeCli` flattens the open-source
`Cli`, so it is accepted as-is — but what it *does* changes under `--cluster`:

- **Who binds it.** Un-clustered, upstream's own `ServerBuilder::start` binds
  it from the config file's `routes` block, with a private `ArcSwap` it never
  shares. Clustered, this binary clears the flag before handing the CLI to
  `ServerBuilder` (so upstream never binds it — the same port bound twice
  would otherwise fail the second bind) and binds it itself, after the node
  has joined, against the table the state machine maintains — never the
  config file. This is the same "engine constructed before the node" ordering
  problem `PullOnMissInterceptor` and `FlowNet::bind` solve by binding late.
- **What the table is.** `PUT /front-door/routes` (body: a whole U-11
  `RouteTable`, `vendor/rift/crates/rift-http-proxy/src/front_door/route_table.rs`)
  and `DELETE /front-door/routes/:id` are terminated by the clustered admin
  front into `ControlOp::PutRoutes`/`ControlOp::DeleteRoute`, committed
  through the Raft leader exactly like an imposter config write: `control::validate`
  runs U-11's rules (unique ids, no two enabled routes with an identical match
  at the same priority, `strip_prefix` requires `path_prefix`, wildcard/method/
  prefix well-formedness) before anything commits, apply is deterministic, and
  a committed write recompiles the table and hot-swaps it into every node's
  front door — no restart, no re-read of a config file. `GET
  /front-door/routes` answers from the local state machine directly (there is
  no upstream endpoint to proxy to). `PutRoutes` is a whole-table replace, not
  a merge — the same all-or-nothing shape U-11's own `RouteTable::validate`
  uses, because ambiguity is a property of the whole set, not one route.
  `DeleteRoute` on an absent id is idempotent at the state machine (like
  `DeleteImposter`), but the admin surface still answers `404` for one that
  was never there.
- **The response headers are the imposter write path's, unchanged.** A
  successful route write carries `Rift-Cluster-Revision` (`default@<log-index>`
  — routes have no per-record port to qualify it with, so there is no
  `If-Match` support for them either) and `Rift-Cluster-Op-Id`, and follows
  the same `--cluster-write-barrier` semantics as every other mutating route.
- **Bind-divergence dividend (§7.4.6): designed, not built (#143).** The front
  door dispatches into the manager in-process (`dispatch_to_port`), so *if* a
  node's own bind failure left an entry in the imposter map, dispatch would
  reach it without touching a socket. It does not: `ImposterManager::create_imposter`
  binds first and returns `Err(BindError)` **before** the imposter is ever
  inserted into the port table, so a node whose bind failed has no map entry
  at all, and the front door 404s on that node exactly like the gateway does.
  Checked at #132's implementation time — this bullet previously claimed the
  dividend as built; it is not. See RFC-001 §7.4.6's corrected note and #143.

With `--cluster` off, none of this exists: `--front-door` behaves exactly as
it does in the open-source binary, which is what the `parity` CI job checks.

## Clustered flow state (#120)

Under `--cluster`, **every** imposter's flow state (scenarios, `ctx.state`,
flow-state templating) is served by the clustered store — configured or not,
because scenario state on a process-local store behind a round-robin LB is
wrong for every imposter, not just the ones that thought about it. Each flow
has one owner (rendezvous-hashed over the applied membership); writes are
serialized through it, and by default reads are answered by it, so a scenario
behaves correctly however the LB spreads its steps.

Two per-imposter knobs, under `_rift.flowState` (both validated at admission —
an unknown value is a `400` naming the key, never a silent default):

| Knob | Values | Meaning |
|---|---|---|
| `readConsistency` | `"strong"` (default) \| `"local"` | `strong`: every read is owner-answered — correct under any LB, at most one LAN RPC. `local`: reads stay on this node's replica — fast, at most one replication push behind the owner |
| `durability` | `"none"` \| `"async"` (default) \| `"sync"` | What a write survives: `sync` fsyncs before the ack (a full-fleet restart loses nothing), `async` is group-fsynced every `--cluster-flow-fsync-interval-ms` (bounded loss), `none` never touches disk |

The mode is **fleet-wide, not node-local**: a replication push carries the
durability the write chose, so a `none` flow is held in memory on every node
that has a copy, and a `sync` flow is persisted by each of them. Background
repair (adoption and anti-entropy) never writes to disk at all — disk copies
come from writes, so a repair cannot quietly persist state an imposter asked to
keep off disk.

The existing `flowState` fields (`backend`, `ttlSeconds`, `flowIdSource`) keep
their upstream meaning; `ttlSeconds` bounds each entry's life fleet-wide.

**Repair (#126).** Deletes replicate as *versioned tombstones*, so a delayed
replication push cannot resurrect a deleted key — the stale push loses the same
version comparison every merge uses. When a membership change moves a flow's
ownership, the new owner **adopts** on its first serve: it pulls the flow from
the surviving holders and merges before answering, so a takeover serves verified
state rather than whatever its own replica held (if *no* holder is reachable it
serves its local copy — bounded staleness, counted, retried on the next touch).
And every node runs a 5-second **anti-entropy** loop pulling the flows it holds
but does not own from their owners, so a replica that missed a push converges
within one tick.

Observability: `rift_cluster_flow_reads_total{path=owner|forward|local}` says
where reads are answered, `rift_cluster_cas_conflicts_total{reason=cas|fence|misroute}`
counts owner-side refusals, `rift_cluster_flow_adoptions_total{outcome}` makes
takeovers visible (`unreachable` is the label worth alerting on), and
`rift_cluster_flow_repairs_total` counts anti-entropy merges that actually fixed
something — steady non-zero means pushes are being missed.

## What lands later

One flag from the Phase-1 plan is deliberately **not** accepted yet, because
nothing behind it exists and this codebase refuses flags that quietly do
nothing (that is the same principle the startup guards enforce):

- `--cluster-features` — the namespace gates nothing while the clustered
  feature set is not selectable; flow state (#120) ships on for every
  `--cluster` node rather than behind a feature gate.

`--cluster-degraded-mode`, also once listed here, was superseded rather than
built: the degradation choice the table reserved it for became the
**per-imposter** `readConsistency` knob above — per-imposter because staleness
tolerance is a property of the test using the imposter, not of the node.

See [`docs/architecture/10-operations.md`](architecture/10-operations.md) for the
operational model this implements.
