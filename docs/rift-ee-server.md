# `rift-ee-server` — the enterprise binary

`rift-ee-server` is the Rift server with enterprise clustering. It is a
*composition*, not a fork: it hands the open-source `ServerBuilder` the same CLI
the `rift` binary would, and adds cluster backends through the upstream
embedding seams. With clustering off it is the open-source server, byte for
byte — the same admin API, the same imposters, the same ports, and nothing
extra bound.

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
rift-ee-server 0.1.0 (enterprise, rift v0.15.0)
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
fails the build if that ever stops being true. Three subcommands and one flag
are **declined with an explanatory error** rather than reimplemented, because
the open-source binary implements them in private functions of its own `main.rs`
rather than behind a library seam, and copying them would fork behaviour that is
meant to stay shared:

| Not supported | Use instead |
|---|---|
| `stop`, `restart` | the `rift` binary (drives a running server by PID file) |
| `save` | the `rift` binary, or `GET /imposters?replayable=true` |
| `--rcfile` | pass the equivalent flags directly |

`script`, `healthcheck`, `replay` and `start` work exactly as upstream.

## Cluster flags

The master switch is `--cluster`; every other `--cluster*` flag is inert without
it, so a stray flag on a single node is not an error.

| Flag | Meaning |
|---|---|
| `--cluster` | Run this node as part of a cluster |
| `--cluster-bind <ADDR>` | Address to bind the cluster port on. **Required** — there is no default, because the cluster port must be a deliberate decision |
| `--cluster-bind-public-ok` | Acknowledge that `--cluster-bind` names a publicly reachable interface |
| `--cluster-advertise <ADDR>` | Address peers dial, when it differs from the bind (NAT, port mapping, a pod behind a service) |
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

On SIGTERM the node fails readiness *first*, so the balancer sheds it before any
socket closes; waits `--cluster-leave-timeout` for in-flight work; and only then
closes the listeners and stops the control-plane node. Closing sockets first
would turn every in-flight request into a client-visible error, which is exactly
what the leave exists to avoid.

> **Set the orchestrator's grace period to at least twice
> `--cluster-leave-timeout`** (`terminationGracePeriodSeconds` on Kubernetes).
> A shorter period kills the process mid-drain and turns a graceful leave into a
> hard one.

Manifests that encode this rule — a Dockerfile, a 3-node compose cluster, and a
Kubernetes StatefulSet with both probes wired — live in
[`deploy/`](../deploy/README.md).

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

## Response headers

Under `--cluster`, cluster-aware code annotates a request and the enterprise
response decorator turns those notes into `Rift-Cluster-*` headers at the
response boundary — so the open-source handlers stay entirely cluster-unaware.
The mapping is structural: an annotation `cluster.revision` becomes
`Rift-Cluster-Revision`. Repeated notes (warnings, above all) are appended as
separate header lines rather than collapsed.

## The clustered admin write path

Under `--cluster`, the public admin address is served by a thin front: the
config-mutating routes (`POST/PUT/DELETE /imposters`, `DELETE
/imposters/:port`, stub CRUD) become replicated control ops committed through
the Raft leader — submitted on any node, forwarded automatically — and
everything else (reads, scenario state, recorded requests, enable/disable) is
reverse-proxied to the local engine unchanged. A 2xx from a mutating route
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

Cluster-mode divergences from a single node: an imposter must carry an explicit
`port` (an auto-assigned port cannot replicate), and `file:`/`ref:` script
sources are not yet resolved on the replicated path (they are still gated by
`--allowInjection`). Concurrent writers to the *same* imposter are
last-writer-wins for now — an expected-revision precondition on
`Rift-Cluster-Revision` is planned follow-up; serialize per-imposter writers
until it lands. `PUT /imposters` commits as a sequence (upserts first, then
prunes), so a write interrupted by a leadership change can transiently leave a
superset of old and new imposters — a retry converges it.

A node is not Ready until its `cluster-reconciled` gate opens: its applied
state has caught up to the leader's and its imposters are bound (or their
failures reported on `GET /_cluster/imposters`).

## What lands later

The data-plane pull-on-miss safety net (retry a failing match once after
waiting ≤ 500 ms for a lagging apply) needs an upstream no-match seam that
v0.15.0 does not expose; until that seam lands its window is kept small by the
default `ready-nodes` barrier (a 2xx already implies fleet-wide apply) and the
`cluster-reconciled` readiness gate (a catching-up node takes no traffic).
`SetEnabled` replication waits on the upstream imposter-enabled seam (#15).

Two flags from the Phase-1 plan are deliberately **not** accepted yet, because
nothing behind them exists and this codebase refuses flags that quietly do
nothing (that is the same principle the startup guards enforce):

- `--cluster-degraded-mode` — the degradation table gives admin writes exactly
  one behavior (`503`, durably parked, replayed), so this flag belongs to the
  flow-state features (#16) that actually have a `local` mode to choose.
- `--cluster-features` — the namespace gates nothing until a second clustered
  feature (#16) exists.

Both arrive with the code that reads them.

See [`docs/architecture/10-operations.md`](architecture/10-operations.md) for the
operational model this implements.
