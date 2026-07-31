# `rift-cluster-server` — the cluster binary

`rift-cluster-server` is the Rift server with cluster clustering. It is a
*composition*, not a fork: it hands the open-source `ServerBuilder` the same CLI
the `rift` binary would, and adds cluster backends through the upstream
embedding seams. With clustering off it is the open-source server, byte for
byte — the same admin API, the same imposters, the same ports, and nothing
extra bound.

That claim is verified, not merely made by construction. The `parity` CI job
(issue #37) builds `rift-cluster-server` and runs upstream's own `rift-http-proxy`
process-spawning suites — `mountebank_compatibility`, `rift_extensions`,
`issue_360_script_cli`, `corpus_replay` — against it with `--cluster` off,
using the `RIFT_SERVER_BIN` override (`vendor/rift`'s `tests/support/mod.rs`)
to point them at this binary instead of their own debug build. `tests/
passthrough.rs` in this crate covers the same claim at the admin-API level and
runs on every PR regardless of what changed; `parity` is the path-gated
counterpart that checks it against upstream's own, much larger behavioural
suite whenever `vendor/rift`, `crates/rift-cluster-server/`, `Cargo.lock`, or
`.github/` change. (The rest of `rift-http-proxy`'s integration tests link
`ImposterManager`/`AdminApiServer` directly and can never be pointed at an
external binary — but they also don't need to be: this crate links that exact
vendored library unmodified, so upstream's own CI already gates that code.)

```sh
# Exactly the open-source behaviour.
rift-cluster-server --port 2525 --datadir ./data

# The same, as one node of a cluster.
rift-cluster-server --port 2525 --datadir ./data \
  --cluster --cluster-bind 10.0.0.7:4790 \
  --cluster-secret-file /etc/rift/cluster-secret \
  --cluster-seeds rift-headless.default.svc.cluster.local:4790
```

## Identifying a build

```
$ rift-cluster-server --version
rift-cluster-server 0.1.0 (cluster, rift v0.16.0)
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
  `rift-cluster-server --debug` now render the same imposter identically.
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

- `rift-cluster-server --pidfile p restart` stops the server recorded in `p` and the
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
| `--cluster-legacy-key-is-fleet-admin <true\|false>` | Whether the legacy `--api-key`'s synthetic principal also gets `FleetAdmin` on the fleet scope, on top of its `TenantAdmin` binding on `default` (RFC-002 §3.4). **Default `true`** — see below |
| `--cluster-audit-retention <SECONDS>` | How long audit rows are kept (default `2592000`, i.e. 30 d; `0` = forever). **Give every node in a fleet the same value** — see "The audit stream" below |

Each flag also has an environment-variable spelling (`RIFT_CLUSTER_BIND`,
`RIFT_CLUSTER_SECRET_FILE`, …), which is the intended vehicle for the secret.

### RBAC and the legacy `--api-key` migration (issue #161)

Every admin request — terminated or proxied — is authenticated and authorized
against RFC-002's principal/role/tenant model. A request's credential resolves
to a principal one of two ways:

- **A stored principal** (`PrincipalPut`, minted via `BindingPut` into a
  tenant): its bindings, read fresh from the Raft state machine on every
  request — never cached, so a revoked binding is refused on the very next
  request through any node.
- **The legacy `--api-key`**, mapped to a synthetic principal bound
  `TenantAdmin` on `default`. This is what keeps every pre-#161 deployment
  working unchanged: the day of an upgrade, nothing observable changes.

`--cluster-legacy-key-is-fleet-admin` is the staged default that turns that
into a deprecation rather than a breaking change:

1. **This release: default `true`.** The legacy key also gets `FleetAdmin` on
   the fleet scope, so it can do everything it always could, including
   fleet-wide and cluster-admin operations.
2. **A future release: default `false`.** The legacy key stays `TenantAdmin`
   on `default` only; fleet-wide operations need a real `FleetAdmin`
   principal.
3. **A later release: the flag is removed**, along with the `FleetAdmin`
   grant.

If a fleet has **no principal defined at all and no `--api-key` configured**,
the admin plane stays fully open — the pre-#161 behavior, so an upgrade never
starts denying a fleet that never set up authorization. `GET /metrics` reports
this as `rift_cluster_no_principals` (see below), so it is something to audit
for rather than discover.

### The tenancy admin surface (issue #162)

Tenants, principals and bindings are managed over the public admin address.
Every route here is answered by the cluster's own control plane — none of it is
proxied to the core admin — so any node can serve the reads.

| Route | Method | Requires |
|---|---|---|
| `/admin/tenants` | `POST`, `GET` | `FleetAdmin` |
| `/admin/tenants/:id` | `GET`, `PUT`, `DELETE` | `FleetAdmin` |
| `/admin/tenants/:id/principals` | `POST`, `GET` | `TenantAdmin` of `:id` |
| `/admin/tenants/:id/principals/:pid` | `PUT`, `DELETE` | `FleetAdmin` |
| `/admin/tenants/:id/bindings/:pid` | `PUT`, `DELETE` | `TenantAdmin` of `:id` — `FleetAdmin` when `:id` is `*` |
| `/admin/whoami` | `GET` | any authenticated principal |

The tenant is taken from the **path**, not from `X-Rift-Tenant`. That header
selects which of your bindings you are acting under on a *resource* route; on
these routes the tenant is the record you are addressing.

Two asymmetries worth knowing before they surprise you:

- **Deleting a principal is a fleet operation, even on a tenant-shaped path.**
  Principals are fleet-global — one identity may be bound in several tenants —
  so a tenant admin deleting one would destroy a credential another tenant
  depends on. *Minting* one is tenant-scoped, because a new identity grants
  nothing outside the tenant it is bound to.
- **Binding on `*` requires `FleetAdmin`.** `*` is the reserved fleet scope, and
  the only role that may be bound there is `fleet-admin` — so a write to
  `/admin/tenants/*/bindings/:pid` is a grant of fleet privilege regardless of
  how the path reads.

A refusal on this surface is a `404`, not a `403`, whenever you hold no binding
in the tenant named — byte-identical to the answer for a tenant that does not
exist. That is deliberate (RFC-002 §8.4): a `403` would confirm which tenants
your neighbours have.

#### Bootstrapping the first fleet admin

Every route above needs a `FleetAdmin`, and a fresh fleet has none. **Use
`--api-key` to bootstrap**, not the open admin plane:

```sh
rift-cluster-server --cluster … --api-key "$BOOTSTRAP_KEY"     # fleet-admin, see below
curl -sX POST http://$ADMIN/admin/tenants/default/principals \
  -H "authorization: $BOOTSTRAP_KEY" \
  -d '{"displayName":"ops","role":"tenant-admin"}'
curl -sX PUT "http://$ADMIN/admin/tenants/*/bindings/key:…" \
  -H "authorization: $BOOTSTRAP_KEY" -d '{"role":"fleet-admin"}'
```

The legacy key is bound `FleetAdmin` on `*` while
`--cluster-legacy-key-is-fleet-admin` is on (default `true` this release, see
above), which is what makes the second call land. Drop the flag — and the
`--api-key` — once a real fleet admin exists.

The open plane (no principals *and* no `--api-key`) does not get you there, and
it is worth knowing why rather than discovering it: it stops being open the
instant the first principal exists, and a tenant-scoped mint may not ask for
`fleet-admin` — fleet privilege binds only on `*`, and binding on `*` requires
fleet privilege you do not yet have. So the very first call closes the door
behind itself and leaves you a `tenant-admin` that cannot promote anything,
including itself. That refusal is the design working (a tenant admin must never
be able to self-promote), which is precisely why bootstrapping is `--api-key`'s
job and not the open plane's.

#### Creating a tenant and issuing a key

```sh
# 1. Create the tenant (FleetAdmin).
curl -sX POST http://$ADMIN/admin/tenants \
  -H "authorization: $FLEET_KEY" \
  -d '{"id":"acme","displayName":"Acme Corp",
       "quotas":{"maxImposters":100,"maxStubsPerImposter":500,
                 "maxFlowEntries":100000},
       "journalRetentionSecs":0}'
# journalRetentionSecs sits beside `quotas`, not inside it: it is a duration
# policy rather than an object count (RFC-002 §11 Q2). See "Quotas" below.

# 2. Mint a principal in it. The response is the ONLY place the key appears.
curl -sX POST http://$ADMIN/admin/tenants/acme/principals \
  -H "authorization: $FLEET_KEY" \
  -d '{"displayName":"ci-runner","role":"editor"}'
# {"id":"key:9f86d0…","displayName":"ci-runner","role":"editor",
#  "tenant":"acme","apiKey":"rift_kZ3v…"}

# 3. Use it.
curl -s http://$ADMIN/admin/whoami -H "authorization: rift_kZ3v…"
# {"principalId":"key:9f86d0…","bindings":[{"tenant":"acme","role":"editor"}],
#  "authorizationDisabled":false}
```

**`apiKey` is shown once.** Capture it at creation or it is gone: the control
plane stores an argon2id hash and a SHA-256 fingerprint, and neither can
reproduce the key. There is no "reveal" endpoint and there will not be one —
a key that can be re-read is a key that leaks from whatever stores it. To rotate,
mint a new principal and `DELETE` the old one; the id is derived from the key, so
a new key is necessarily a new principal.

`PUT /admin/tenants/:id/principals/:pid` changes the display name and the
`disabled` flag only. Disabling is the immediate revocation lever: it is a
Raft-committed fact with no cache in front of it, so the key stops
authenticating on the very next request through **any** node.

**`whoami` is the cheapest check that authorization is wired at all.** It
reports the caller's own identity and bindings and authorizes nothing beyond
having authenticated. `"authorizationDisabled": true` with a null `principalId`
means the fleet has no principals and no `--api-key` — the open admin plane
described above, not an unbound principal.

#### The per-request cost of a key

Each authenticated admin request performs **one** argon2id verification, roughly
20–50 ms, at the pinned OWASP 2024 cost (m = 19456 KiB, t = 2, p = 1). There is
no verification cache: caching authorization data is what RFC-002 §8.5 forbids,
and a cache keyed on the credential would need invalidating on four separate op
variants with a failure mode that fails *open*. Budget for that latency on the
admin plane. It does not touch the data plane — gateway traffic under
`/__rift/` is never authenticated.

A credential that matches no principal is refused having performed **zero**
argon2id work, so an unauthenticated caller cannot use the admin port as a
memory-amplification lever.

### The audit stream (issue #163)

```sh
curl -s "http://$ADMIN/admin/audit?since=0&limit=100" \
  -H "authorization: $KEY" -H "x-rift-tenant: acme"
# [{"tsSecs":1700000000,"principal":"acme/ci-runner","tenant":"acme",
#   "action":"imposter.write","resource":"8080",
#   "opId":"3f2a…","revision":42,"outcome":"applied"}]
```

**Any node answers, and every node answers the same.** The row is derived at
apply from the committed log entry, so all replicas compute identical rows and
the read needs no fan-out — unlike `/_cluster/*` operator reads, you do not have
to reach a particular node, and you never get a partial view.

| Parameter | Meaning |
|---|---|
| `since` | First revision to return, inclusive. Default `0`. This is a **revision**, not a timestamp — page by taking the last row's `revision + 1` |
| `limit` | Rows to return. Default `500`, capped at `5000` |

`x-rift-tenant` names the tenant you are acting as, exactly as on every other
tenant-scoped route; the path carries no tenant, so there is nothing else to go
on. It defaults to `default`.

**Visibility.** `audit.read` is its own action and is **not** part of
`tenant.manage`: reading who did what and changing who may do what are different
powers. A `FleetAdmin` sees the whole fleet; a `TenantAdmin` sees exactly the
tenant it is acting as; `Editor` and below get `403`. The narrowing happens on
the server — a tenant admin is never sent another tenant's rows.

**What is and is not in the stream.**

- Every replicated **write** — including one that was **refused**. A refusal is a
  committed decision, and its row is often the one you want.
- **Reads are not audited.** Neither are the mutating operations served over the
  *proxy* path (`POST /imposters/:port/scenarios/:id/reset`,
  `DELETE …/savedRequests`, flow-state clears): they are forwarded to the
  embedded core admin and never become replicated ops, so a log-derived stream
  cannot see them. This is a **known gap** — auditing them means putting them on
  consensus, which is a future slice. Do not read their absence as "it did not
  happen".
- A fleet-wide delete records the **tenant whose imposters were destroyed** and
  `"resource": "*"`.

**Retention.** `--cluster-audit-retention` (default 30 d, `0` = forever). The
sweep runs against the cluster's replicated logical clock, never a node's local
wall clock, so a node whose clock is skewed drops exactly the rows its peers do.
That is also why **every node must be given the same value**: the GC runs inside
the replicated apply path, so nodes configured differently would drop different
rows and their audit tables would permanently diverge. Expiry lags the write that
crosses the retention boundary by one apply — the window is a floor on how long
rows are kept, not a promise to delete at the instant it passes.

Audit history survives a full-cluster restart and a node joining by snapshot
install.

### Exporting the audit stream (issue #164)

Optional, **off by default**, and it adds no dependency to the binary — the HTTP
client and SigV4 signing are the ones the `s3:` imposter-source provider already
carries. With no sink declared no export task runs, nothing is read from the
audit table, and nothing reaches the network.

Declare a sink — fleet state, so you set it once and every node (including one
that joins later) inherits it:

```sh
curl -s -X PUT "http://$ADMIN/admin/audit/sink" \
  -H "X-Rift-Key: $FLEET_ADMIN_KEY" -H 'content-type: application/json' \
  -d '{"uri":"s3://acme-audit/rift/","authRef":"audit-bucket","batchMaxRows":500}'

curl -s "http://$ADMIN/admin/audit/sink" -H "X-Rift-Key: $FLEET_ADMIN_KEY"
curl -s -X DELETE "http://$ADMIN/admin/audit/sink" -H "X-Rift-Key: $FLEET_ADMIN_KEY"
```

All three require the `cluster.admin` action, not `audit.read`: where the fleet's
audit is shipped is a fleet-scoped decision. **There is one sink, fleet-wide** —
per-tenant sinks are a stated non-goal (the resume checkpoint is a single
revision).

| Scheme | Shape | Wire format |
|---|---|---|
| `https://…` | webhook `POST` per batch | JSON Lines (`application/x-ndjson`) |
| `s3://<bucket>/<prefix>` | one object `PUT` per batch, key `<prefix>/<20-digit revision>.jsonl` | JSON Lines |

`http://` is refused **except** to a loopback host — a sidecar collector on the
same machine puts no bytes on a network, which is the only thing the cleartext
rule protects against. The S3 key is zero-padded so a lexicographic bucket
listing is in revision order.

**Credentials never enter the log.** The record carries `authRef` — the *name* of
a credential — and a URI with credentials in its authority is refused at
admission with the same error a source URI gets. Resolution is node-local at
export time, through the same secrets directory the imposter sources use
(`RIFT_SOURCE_SECRETS_DIR`); `RIFT_S3_ENDPOINT` / `RIFT_S3_REGION` apply to an
`s3://` sink as they do to an `s3:` source. A named `authRef` that fails to
resolve **fails the ship** — it never falls back to an unauthenticated request.
An S3 credential is `<access-key-id>:<secret-access-key>`; a webhook credential
is sent as `Authorization: Bearer <value>`.

**Delivery is at-least-once, not exactly-once**, and the difference is
operational, not academic. The leader ships a batch and *then* commits its
checkpoint; a leader that dies in between re-ships that batch when its successor
resumes. **Your consumer must dedup on `(revision, opId)`** — that is why both
are on every row. Duplicates are bounded to one batch and appear only across a
failover.

**A dead sink cannot stall admin writes.** Export runs in a background task
reading committed state, so nothing on the write path waits on it. Watch:

| Metric | Meaning |
|---|---|
| `rift_cluster_audit_export_shipped_total` | Rows accepted by the sink (re-ships counted — at-least-once) |
| `rift_cluster_audit_export_failures_total` | Failed ship attempts; rising while `shipped` is flat means the sink is down |
| `rift_cluster_audit_export_lag_revisions` | Applied revision minus checkpoint, on the leader |
| `rift_cluster_audit_export_skipped_revisions_total` | **Rows that aged out before they shipped** — see below |

> **`skipped_revisions_total` moving is data loss, and it is permanent.** If a
> sink stays down longer than `--cluster-audit-retention`, the GC removes rows the
> exporter never shipped. Those rows are gone from every replica, so the gap
> cannot be backfilled. The exporter counts the span and logs it at error level
> rather than passing over it quietly — a silent hole in an exported audit trail
> is the worst failure this feature could have. It is reported as a **revision
> span**, an upper bound on rows lost: revisions are not one-to-one with audit
> rows, and once the rows are GC'd there is nothing left to count exactly. Alert
> on this counter, and size `--cluster-audit-retention` against how long you can
> tolerate your collector being down.

> **A badly skewed node clock can erase history early.** The replicated clock is
> a running maximum over the `issued_at_secs` each submitting node stamps from
> its *own* wall clock. One node whose clock is a year fast can therefore advance
> the whole fleet's logical clock by a year with a single write, and the next GC
> — deterministically, on every replica — drops everything older than the new
> cutoff. The clock never goes backwards, so this is not recoverable. The same
> exposure exists for the 24 h dedup GC, where it costs a day of replay collapse;
> here it costs the retention window. **Keep cluster nodes on NTP**, and treat a
> large unexplained jump in retained history as a clock incident rather than a
> quota or storage one.

### Upgrading a fleet

`--cluster-audit-retention` and quota enforcement both take effect **inside the
replicated apply path**, which makes them upgrade-sensitive in two ways:

- **Give every node the same retention value.** Nodes configured differently drop
  different rows from the same log and their audit tables diverge permanently.
- **Finish rolling out a version before writing against its new rules.** This
  release adds two apply-time refusals (a quota ceiling, and a zero-valued quota
  at validation). A node still running the previous version applies the *same*
  committed entry without them, so during a partial rollout two nodes can reach
  different outcomes for one entry and diverge. Upgrade all nodes, then start
  using the new behaviour. (This is the same rule earlier tenancy slices
  introduced; it is written down here because this release is the first to make
  it easy to trip on a routine write.)

### Quotas (issue #163)

`quotas` on the tenant record bounds **object counts**, and is enforced on the
replicated apply path:

| Field | Enforced |
|---|---|
| `maxImposters` | Yes. Counts the tenant's existing ports *excluding* the one being written, so replacing an imposter you already own never trips the ceiling |
| `maxStubsPerImposter` | Yes — on the payload for a create/replace, and on the *result* for a stub edit |
| `maxFlowEntries` | Stored; enforced by the flow owner |
| `journalRetentionSecs` | Moved **off** `quotas` onto the tenant record itself (it is a duration policy, not a count). Stored; applied by the request shards in M3 |

> **If you set `quotas.journalRetentionSecs` on an earlier M2 build, re-set it.**
> The field moved to the top level of the tenant body; a stored record still
> carrying it under `quotas` decodes with the *new* field at its default of `0`
> (unlimited), silently. Nothing enforces the value yet — M3 (#147) is what will
> read it — so the practical window to fix this is before that lands, but the
> value is lost now rather than then.

A ceiling of `0` is refused at validation rather than stored: it makes the tenant
permanently unusable, and "unlimited" has its own spelling (a large number). A
tenant with no stored record gets generous defaults, so a fleet that never
configured tenancy is not capacity-locked.

Quotas bound object counts, **not compute**. One tenant's pathological regex
still degrades a shared node; that is a stated non-goal, not a gap.

**A quota refusal is a committed decision, not a submit-time error.** This
matters for the async/parked write path. Enforcement happens where the op
applies, so a write parked during a minority-side outage
(`--cluster-admin-async`, or a `503 + op-id`) is validated **on replay, against
the quota as it stands then**. A tenant at its ceiling can therefore be accepted
at submit and refused at replay:

```sh
curl -sX POST http://$ADMIN/imposters -H "authorization: $KEY" -d @imposter.json
# 202  {"opId":"3f2a…"}          <- parked, outcome not yet knowable

curl -s http://$ADMIN/_cluster/ops/3f2a… -H "authorization: $KEY"
# {"outcome":"failed","reason":"tenant \"acme\" is at its ceiling of 100 imposters",
#  "revision":93}
```

There is deliberately **no quota reservation at park time**: a reservation would
have to survive a leader change and expire on its own, which is more machinery
than the problem earns and a new source of divergence between nodes. Poll
`GET /_cluster/ops/:id` for the real outcome — that is the contract for every
parked write, and a quota refusal is simply one of the outcomes it can report.

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

**Calling them.** Every request on this port carries an HMAC over its
timestamp, nonce, method, path and body (RFC-001 §11.2), so plain `curl` cannot
reach it. `cargo run -q -p rift-cluster --example cluster-curl` is a one-file
client that signs one request and prints the answer — deliberately minimal, and
living in the crate that defines the format so the two cannot drift:

```sh
cargo run -q -p rift-cluster --example cluster-curl -- \
    --secret "$RIFT_CLUSTER_SECRET" GET http://127.0.0.1:4790/_cluster/members
```

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
| `rift_cluster_no_principals` | `1` when the fleet has no principal defined at all (issue #161) — the condition under which the admin plane's open-by-default bypass applies. Resampled continuously, not only at startup: a `PrincipalPut` can flip it at any moment the fleet is running |

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

Under `--cluster`, cluster-aware code annotates a request and the cluster
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
- **Bind-divergence dividend (§7.4.6): built (#143).** The front door dispatches
  into the manager in-process (`dispatch_to_port`), so a node whose own bind
  failure left an entry in the imposter map reaches it without touching a
  socket. Under `--cluster`, `cluster_manager` sets
  `ImposterManager::with_serve_unbound(true)`: an apply-path create whose
  explicit port hits `Err(BindError)` no longer drops the imposter — it
  registers it with no listener, reported under `ApplyReport::failed` (never
  `created`) and cleared by the next apply once the port frees up (rebind
  healing). A port-addressed admin read of that imposter — `GET
  /imposters/:port` through the clustered front — answers its normal `200`
  plus `rift-cluster-bind-failures: <port>=<reason>`; the response body stays
  core-shaped, so the divergence is a header only. Two things stay
  all-or-nothing regardless of the flag: `create_imposter` (the direct,
  non-apply path) and any bind failure that isn't a plain `BindError` on an
  explicit port (`PortInUse`, or an auto-assigned port — which `--cluster`
  refuses outright at `400` rather than minting one). Checked at #132's
  implementation time, this bullet previously said the dividend was designed
  but not built; #143 built it. See RFC-001 §7.4.6.

With `--cluster` off, none of this exists: `--front-door` behaves exactly as
it does in the open-source binary, which is what the `parity` CI job checks.

## Imposter sources (#134)

An **imposter source** is a URI the fleet agrees backs some of its imposters —
a config document in S3, in a git repo, on a config server. Under `--cluster`
a source is a replicated control-plane object, like the imposter set and the
route table: declared once, visible on every node, and pulled on demand.

The rule that makes this cluster-correct: **fetching never happens in the apply
path.** A fetch is I/O against a system this cluster does not control, and two
nodes fetching the same URI a second apart can legitimately get different
bytes — so the node that received the request fetches *once*, hashes what it
got, and submits the result as an ordinary control op. The fetched bytes enter
the log exactly once and every node applies those same bytes. Followers never
fetch.

### The endpoints

These ride the **cluster port**, beside `/_cluster/*` — they are an operator
surface authenticated with the cluster credential (`--cluster-secret`), not
with the admin API key:

| Endpoint | What it does |
|---|---|
| `POST /admin/sources` | Declare a source: `{ id, uri, mode?, authRef?, onDrift? }` |
| `GET /admin/sources` | Every source, with its drift flag and last pull outcome |
| `GET /admin/sources/:id` | One source |
| `DELETE /admin/sources/:id` | Stop tracking the URI |
| `POST /admin/sources/:id/pull` | Fetch it now and apply what it produced |

A pull answers `{ revision, version, digest, unchanged, skipped, changed: [ports] }`.
The two negative flags mean different things: `unchanged` is "nothing to do, no
log entry written", while `skipped` is "the log recorded a decision *not* to
apply" (a drifted source under `onDrift: skip`) — in which case the fleet does
not hold this content and `changed` is empty.

Refusals carry the usual split: `400` for something the operator can fix (a
malformed body, an unservable scheme, a drifted source under `onDrift: fail`),
`404` for an unknown source, and `503` + `Rift-Cluster-Op-Id` when the write
could not be committed — the same Chapter 4 write-path contract the admin
front uses, so a client polls `GET /_cluster/ops/:id` rather than blind-
retrying.

**What a pull's 2xx does and does not promise.** It means the op was committed
and *the node you asked* has applied it — the puller awaits its own local apply
before answering, no more (#99). `--cluster-write-barrier` is a property of the
**admin front**, and these endpoints are not on it, so a 2xx here is not the
fleet-wide read-after-write a `POST /imposters` 201 is. The rest of the fleet
follows within a replication round; compare `GET /_cluster/config` across nodes
if a script needs to know it has. Container scenario
`c20_source_pull_converges_and_fetches_once` is where that convergence is
asserted, alongside the exactly-one-fetch equality.

These endpoints are on the cluster port, so reach them with `cluster-curl` (see
"The `/_cluster/*` operator surface" above), and see `deploy/README.md`'s
"Imposter sources demo" for a three-node walkthrough that rolls the whole fleet
with one pull.

| Field | Values | Meaning |
|---|---|---|
| `mode` | `"pinned"` (default) \| `"tracking"` | `pinned`: explicit pulls only. `tracking`: the fleet re-fetches on `pollSecs` — see below |
| `pollSecs` | ≥ 5 | How often a `tracking` source is re-fetched. Required for `tracking`, refused for `pinned` |
| `onDrift` | `"overwrite"` (default) \| `"skip"` \| `"fail"` | What a pull does when the source's imposters have been hand-edited since it last applied |
| `authRef` | a credential *name* | Stored and validated as a name. Resolving it into a request header ships with the providers that need it (#136) — upstream's `HttpSource` has no header-injection seam, so a credentialed fetch needs a new provider, not a hook here |

**Secret hygiene.** A URI carrying credentials in its authority
(`https://user:pass@host/x.json`) is refused before anything is written, so the
secret never reaches the log — not even as a committed refusal, which would
keep it on every replica's disk and in every snapshot. `authRef` is the only
credential path.

### No-change pulls cost nothing

A pull whose content matches what the source last applied produces **no log
entry at all** — it answers `unchanged: true` and the applied index does not
move. Without that, a fleet re-pulling a stable document would grow its log
forever and re-churn imposter state every round. The comparison is a SHA-256
over a canonical (recursively key-sorted) encoding of the fetched configs, so
a document that only reordered itself still counts as unchanged.

Two things deliberately do **not** count as unchanged, because in both the
fleet does not hold the content:

- a **drifted** source — an operator who hand-edited an imposter and then pulls
  to restore declared truth is the ordinary repair path, and answering
  "unchanged" there would make drift unfixable except by editing the document
  upstream;
- content a previous pull **skipped** — a skip records the digest it *saw*, not
  one that was applied.

### Tracking sources: one poller, fleet-wide (#135)

A `tracking` source is re-fetched on an interval without anyone asking. The
whole difficulty is the word *fleet*: N nodes each running a timer would fetch N
times per interval, undoing the fetch-once property above with the very thing
meant to drive it. So the scheduler is **leader-only**.

- The poller is grounded on the same Raft leadership signal the forward-to-leader
  write path reads — deliberately not a second notion of leadership, because two
  independent answers to "am I the leader" is exactly how a fleet grows a second
  poller during an election.
- On losing leadership a node stops every poll task; on gaining it, it starts
  them. `SourcePut` and `SourceDelete` reconcile the running set without a
  restart.
- Intervals are jittered ±10%, so sources declared together do not arrive at the
  upstream host as a burst every interval.
- **`pollSecs` has a 5-second floor**, enforced at admission with a typed `400`.
  A mistyped `1` would turn the fleet into a request flood against someone
  else's host, and the operator would see only that their mocks update promptly.

**A poll costs no log growth when nothing changed.** Polling runs the same pull
flow as an explicit `POST .../pull`, so the digest short circuit applies: an
unchanged document writes no log entry, forever. That is what makes tracking
mode affordable at a 30-second cadence.

**A failing poll is visible without being written down.** Errors are recorded
*leader-locally* — surfaced as `lastPollError` on `GET /admin/sources/:id`, and
counted by `rift_cluster_source_polls_total{outcome="error"}`. They are
deliberately never committed: a log entry per failure would reintroduce the log
growth the short circuit exists to prevent, at the worst possible moment (an
upstream outage is exactly when you do not want fleet-wide write traffic). The
durable `last…` fields still move only when a pull actually applies or is
skipped, so a stale `lastVersion` next to a `lastPollError` reads correctly:
the fleet is holding the last good content and the source is currently
unreachable.

Observability: `rift_cluster_source_polls_total{outcome}` (`applied` /
`unchanged` / `skipped` / `error`) and `rift_cluster_source_poll_seconds`. Only
the leader increments them, so summing across the fleet counts each poll once —
which is also how you would catch a fleet that has somehow grown a second
poller.

### Provenance and drift

A pull stamps each config it applies with its source id and version. That
provenance is replicated state, which is what makes drift detection
deterministic: when an operator edits a source-owned imposter by hand — a
`PUT`/`POST` on its stubs, an enable/disable, a delete — every replica flips
that source's `drifted` flag at the same log index, for the same reason.

`GET /_cluster/config` reports the provenance alongside the ports, so the
question an operator actually asks of a source-driven fleet — "has every node
converged on the same configs, from the same source version?" — is answered by
comparing two nodes' responses.

The next pull then follows the source's declared `onDrift`:

- `overwrite` (the default, and Solo's behaviour) applies the document and
  clears the flag — but *declared*, and visible in `GET /admin/sources/:id`
  beforehand, rather than a silent clobber;
- `skip` leaves the operator's edit alone and records the attempt, so a source
  being held back is visible rather than looking idle;
- `fail` refuses the pull.

Two deliberate non-destructive choices:

- **A pull only touches what its own source owns.** Ports the document dropped
  are removed; a config it declares unchanged is not rewritten at all (the
  rewrite is what would reset that imposter's runtime state); an imposter no
  source owns is never in the blast radius. If two sources declare the same
  port — nothing forbids it, since they are fetched independently — the one
  that loses the port is marked `drifted` rather than left believing it still
  owns it.
- **Deleting a source does not delete its imposters.** "Stop tracking this URI"
  is not "tear down live traffic". The imposters keep serving and simply lose
  their provenance, so nothing is left pointing at a source that no longer
  exists.

### `--imposters` under `--cluster`

Upstream's `--imposters <uri,...>` loads imposters from source URIs at startup.
With `--cluster` **off** it behaves exactly as it does in the open-source
binary. With `--cluster` **on** it becomes sugar for declaring pinned sources:
this binary takes the flag before handing the CLI to `ServerBuilder` and
declares one source per URI, then pulls each once — so the imposters land
through the replicated log and reach every node.

Left in place, upstream's own startup would create those imposters in *this
node's* manager, outside the log, and the reconciler — which treats the
replicated set as authoritative — would then delete them again. That is the
same failure `--configfile` is refused for (see the startup guards);
`--imposters` can be desugared instead of refused because a URI is fetchable
from every node, while a local path is not.

Source ids are derived from the URI (a readable slug plus a short hash), so
they are stable: a restart, or a second node booting with the same flags,
upserts the same source rather than accumulating one per boot — and the digest
short circuit then makes the repeat pull a no-op.

A source that cannot be declared or pulled **fails the start**. An operator who
passed `--imposters` asked for those imposters to be serving; a node that comes
up healthy without them is the silently half-configured fleet this path exists
to avoid.

### What a pull does not apply

A source document may declare blocks that belong to other subsystems:

- an `intercept` block **refuses the pull** — the cluster refuses the TLS-MITM
  intercept listener fleet-wide, because its state is per-node and is not
  replicated;
- a `routes` block is **ignored, with a warning in the pull response** — the
  front door's table is its own replicated object with its own op (`PUT
  /front-door/routes`, above).

### Audit

Every applied pull writes a structured `audit`-target log event naming the
principal, the source id, the version and the applying revision — so "who moved
the payment mocks to which commit, and when" is a log query.

Container-tier chaos coverage for sources is #137.

## Source providers (#136)

Which schemes a node can fetch is **per-node** configuration — deliberately not
part of the replicated op validation, so two nodes can never disagree about a
committed source. `POST /admin/sources` refuses a scheme *this* node cannot
serve, listing the ones it can.

| Scheme | URI shape | `version` is | Credential (`authRef`) |
|---|---|---|---|
| `file:` | `file:/srv/mocks.json` | *(none — always re-applied)* | n/a |
| `http:` / `https:` | `https://host/imposters.json` | the `ETag` | n/a — use `registry:` for a token-authenticated endpoint |
| `git+https:` | `git+https://host/org/repo#<ref>:<path>` | the **commit sha** | a token, sent as an `Authorization: Basic` git `http.extraHeader` |
| `git+file:` | `git+file:/srv/repo.git#<ref>:<path>` | the **commit sha** | as above (rarely needed for a local mirror) |
| `s3:` | `s3://bucket/key` | the `ETag`, unquoted | `<access-key-id>:<secret-access-key>`, signed with SigV4 |
| `registry:` | `registry://<service-id>[,…]` | a SHA-256 of the responses | a token, sent as `Authorization: Bearer` |

Notes that matter in practice:

- **`git+…` needs a `git` binary.** These providers shell out rather than
  linking libgit2 or `gitoxide`, and the binary is probed at **startup**, so an
  image without `git` refuses to boot instead of failing on the first pull. The
  shipped `deploy/Dockerfile` installs it.
- **`<path>` may be a file or a directory.** A directory parses every file under
  it and merges them; a port declared by two documents is an error naming both,
  never a silent last-one-wins.
- **`s3:` is path-style** (`{endpoint}/{bucket}/{key}`), which is what makes a
  MinIO or in-VPC endpoint reachable. Ambient credentials (IRSA, an EC2/ECS task
  role) are **not** implemented: `authRef` static keys are the credentialed
  path, and a source with no `authRef` fetches anonymously.
- **`registry:` is only registered when its endpoint is configured** — a scheme
  with nothing to reach is a pull failure waiting to happen.
- **Interop caveat:** the SigV4 signer is covered by structural and regression
  tests against a local stub, not by a test against a real S3 or MinIO — the
  chaos tier has no S3-compatible container plumbing. Treat first use against a
  new S3-compatible endpoint as worth verifying by hand.

### What a source URI is not allowed to be

A source URI is operator-supplied data that reaches every node through the
replicated log, so the `git+` schemes are constrained tightly — at **admission**,
so a URI no node should ever fetch never reaches the log at all, and again in the
provider:

- A remote or ref beginning with `-` is **refused**. `git`'s option parser
  permutes, so `git+file:--upload-pack=/tmp/x.sh#main:y` would otherwise be
  parsed as an *option* and run `/tmp/x.sh` as the rift process on every node
  that pulls it. This is the reason the two checks exist at all.
- A remote containing `::` is **refused** — that is the `<helper>::<target>`
  transport syntax, whose purpose is running a command as the transport.
  `protocol.ext.allow=never` is also set on every invocation, so it is two
  independent gates rather than one.
- A `git+file:` remote must be an **absolute path**; a `git+https:` remote must
  parse as an `https` URL with a host.
- A ref must look like a ref: `[A-Za-z0-9._/+-]`, no `..`, no leading `/`.

Three more properties of the git subprocess worth knowing operationally:

- **Every invocation has a 30s budget** and is killed — as a process *group*, so
  the `git-remote-https` helper goes with it — when the budget passes. Without
  the group kill the helper survives holding the pipes open, and a stalled
  remote would leak one blocking-pool thread per poll until nothing on the node
  could do blocking work at all.
- **Redirects are refused** (`http.followRedirects=false`). `git` does not strip
  `http.extraHeader` when it follows one, so a remote that 302s elsewhere would
  otherwise hand that host your token.
- **Host git config is ignored** (`GIT_CONFIG_GLOBAL`/`GIT_CONFIG_SYSTEM` are
  `/dev/null`), so an `insteadOf` rewrite, a `credential.helper` or an
  `http.proxy` outside this process cannot redirect or re-credential a fetch the
  replicated URI is supposed to define completely.

### Credentials

A source **names** a credential and never carries one: a URI with credentials in
its authority is refused at admission, before it can reach a log that every
replica keeps and every snapshot copies. Resolution happens on the fetching
node, at fetch time, in this order — first hit wins:

1. environment variable `RIFT_SOURCE_AUTH_<REF>`, where `<REF>` is `authRef`
   upper-cased with every non-alphanumeric character replaced by `_` (so
   `gh-mocks` reads `RIFT_SOURCE_AUTH_GH_MOCKS`);
2. a file named exactly `<authRef>` under `RIFT_SOURCE_SECRETS_DIR` — the shape
   a Kubernetes secret mounts as (a trailing newline is stripped);
3. a cloud secret manager, when one is configured. None is wired in this build.

**Resolution fails closed.** An `authRef` that cannot be resolved is a *pull
error* surfaced in `last.outcome` — never a retry without the credential, never
a silent skip. A source configured for a private repo does not quietly start
serving whatever a public one holds because a secret mount went missing.

**An `authRef` a provider would ignore is refused, not accepted.** Only
`git+https:`, `git+file:`, `s3:` and `registry:` consume a credential; `file:`
and `http(s):` do not. Setting `authRef` on one of those is a `400` at
`POST /admin/sources`, naming the schemes that do take one. Accepting it would
mean fetching anonymously forever while the operator believed the request was
authenticated — and against an endpoint that serves public content to anonymous
callers, that is not an error the operator ever sees, it is the fleet quietly
serving the wrong corpus. (This is a node-local check, like the unknown-scheme
refusal: which schemes take a credential is per-node configuration, so it
cannot live in the replicated op validation.)

Secret material never reaches a log line, an audit row, or an error string: the
credential type has no `Display` and renders as `<redacted>` under `Debug`, the
git token travels in the subprocess environment rather than in the remote URL
(`git` echoes URLs on failure), the S3 secret key never leaves the signing
function, and no provider folds a response body into an error — an echoing
server must not be able to reflect an `Authorization` header back into a
message an operator reads.

### Provider configuration

Environment variables, not flags — this is deliberately the minimum plumbing,
not a config subsystem:

| Variable | Effect |
|---|---|
| `RIFT_SOURCE_SECRETS_DIR` | Directory of `<authRef>`-named secret files |
| `RIFT_S3_ENDPOINT` | Override the S3 endpoint (MinIO, in-VPC gateway) |
| `RIFT_S3_REGION` | SigV4 region; defaults to `us-east-1` |
| `RIFT_SOURCE_REGISTRY_ENDPOINT` | Registry base URL; **registers the `registry:` scheme** |
| `RIFT_SOURCE_REGISTRY_POINTER` | RFC 6901 pointer to the imposters array in a registry response; defaults to `/imposters` |

A `registry:` fetch issues one `GET {endpoint}/{service-id}` per id in the URI,
in order, and pulls the imposters array out of each response through the
pointer. A pointer that matches nothing is an **error**, not an empty list —
an empty list would silently delete every imposter the source owns.

## Clustered flow state (#120)

Under `--cluster`, **every** imposter's flow state (scenarios, `ctx.state`,
flow-state templating) is served by the clustered store — configured or not,
because scenario state on a process-local store behind a round-robin LB is
wrong for every imposter, not just the ones that thought about it. Each flow
has one owner (rendezvous-hashed over the applied membership); writes are
serialized through it, and by default reads are answered by it, so a scenario
behaves correctly however the LB spreads its steps.

Three per-imposter knobs, under `_rift.flowState` (all validated at admission —
an unknown value is a `400` naming the key, never a silent default):

| Knob | Values | Meaning |
|---|---|---|
| `readConsistency` | `"strong"` (default) \| `"local"` | `strong`: every read is owner-answered — correct under any LB, at most one LAN RPC. `local`: reads stay on this node's replica — fast, at most one replication push behind the owner |
| `durability` | `"none"` \| `"async"` (default) \| `"sync"` | What a write survives: `sync` fsyncs before the ack (a full-fleet restart loses nothing), `async` is group-fsynced every `--cluster-flow-fsync-interval-ms` (bounded loss), `none` never touches disk |
| `contextScope` | `"imposter"` (default) \| `"fleet"` | Which imposters share a flow-id namespace. `imposter`: this imposter's flow ids are its own — two imposters resolving the same id (the ordinary result of both using `flowIdSource: "header:X-Session"`) stay isolated, matching single-node behaviour. `fleet`: one namespace across every imposter, so a suite spanning two mocks carries one context through both |

### `contextScope` and the isolation it restores (#152)

Flow ids are caller-chosen: `flowIdSource: "header:X-Session"` turns a request
header into one. Two unrelated imposters reading the same header therefore
produce the *same* flow id as a matter of course. Single-node Rift isolates them
without anyone asking, because each imposter builds its own flow store; before
this knob existed the clustered store passed those ids into one fleet-wide
namespace, so the two imposters silently shared state — no error, just wrong
reads.

`imposter` is the default because it is the parity-restoring answer. This is a
**behaviour change** for a fleet that was relying on the old sharing: set
`contextScope: "fleet"` on those imposters to keep it, explicitly.

The two namespaces are disjoint by construction — `fleet` carries its own
prefix rather than using bare ids — so no caller-chosen flow id can be crafted
to read across the boundary.

**One residual difference from single-node.** The namespace is keyed by the
imposter's *port*, not by the store instance, so deleting an imposter and
recreating it on the same port inherits whatever flow state the old one left,
up to `ttlSeconds`. Single-node gives the replacement a fresh store and
therefore an empty one. This is strictly better than the pre-`contextScope`
behaviour (where the state was shared with every *other* imposter as well), but
a suite that recreates an imposter between runs and expects clean state should
set a short `ttlSeconds` or use distinct flow ids per run.

**Upgrading.** The scope is a key prefix, so flows written by an older build are
keyed differently and are **orphaned** on upgrade, not corrupted: they are
simply never looked up again, and the per-flow TTL (`ttlSeconds`, default 300 s)
reaps them. Two consequences worth planning for:

- Flows in flight at the moment of upgrade are lost; `ttlSeconds` bounds the
  window in which that matters.
- During a **rolling** upgrade, old and new nodes address disjoint namespaces
  for the duration, and nothing will flag it — the prefix also changes each flow
  id's owner in the ring. Upgrade the fleet together, or accept a bounded blip
  for in-flight flows. There is deliberately no dual-read compatibility path:
  one would have to guess which namespace a bare id belonged to, and guessing
  wrong is the bug this change exists to remove.

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

## The web console (`--features console`, #186)

The binary can serve an embedded web console at `GET /console` — a single-page
app built from `web/` and compiled into the executable. It is behind a **cargo
feature that is off by default**, and the default has to stay that way:

```sh
# Every ordinary build. No node required, no console in the binary, and
# /console proxies upstream and 404s exactly as it did before.
cargo build

# The release build that carries the console. `pnpm build` FIRST — the assets
# are embedded at compile time, so cargo cannot produce them for you.
cd web && pnpm install --frozen-lockfile && pnpm build && cd ..
cargo build --release --features console
```

Enabling the feature without a built `web/dist/` is a **compile error**, by
design: a release that silently shipped without its console would be discovered
by users rather than by CI (RFC-006 §7). One caveat worth knowing, because it
bit during implementation — `rust-embed` is a derive macro, and a derive macro
cannot declare `cargo:rerun-if-changed`. Cargo therefore has no dependency edge
from the crate to `web/dist/`: rebuilding after changing the bundle **without**
recompiling the crate leaves the old assets embedded. The release lane covers
this by asserting the finished binary contains the current build's content-hashed
asset name (`scripts/check-console-embed.sh embedded`); if you are iterating
locally, use a debug build, where `rust-embed` reads from disk instead.

Serving is read-only and **unauthenticated**, deliberately: the console shell is
the login UI (RFC-006 §5.3), so requiring a credential to fetch the page that
collects one is a closed loop. The bundle holds no secrets, and every API call it
then makes goes through the same authorization chokepoint as any other client.
Every response — including the 404 and the 405 — carries

```
Content-Security-Policy: default-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'
```

which is enforceable precisely because everything is embedded: no CDN, no remote
fonts, no inline scripts. Hashed assets under `/console/assets/` are served
`max-age=31536000, immutable`; `index.html` is `no-cache`, so an upgrade is never
served a shell pointing at the previous build's assets.

### Working on the console

`pnpm dev` in `web/` runs Vite with every admin path proxied to a real node, so
console work needs no Rust rebuild at all:

```sh
cd web
pnpm install
pnpm dev                                  # proxies to http://127.0.0.1:2525
RIFT_ADMIN_URL=http://localhost:12525 pnpm dev   # ...or the compose stack's node 1
```

The TypeScript client is **generated** from `docs/api/openapi-ee.yaml`
(`pnpm run generate:client`) and committed, so `web/` builds without the binary
present. CI regenerates it and fails on any difference, which is what keeps the
contract, the client and the server from drifting apart silently.

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
