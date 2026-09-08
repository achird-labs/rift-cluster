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

## Installing from a release

Tagging `vX.Y.Z` publishes two kinds of artifact, both carrying the web console
at `/console` (#265, #266). Neither needs a checkout, a submodule or a toolchain.

**Container image** — `linux/amd64` and `linux/arm64`, built on native runners:

```
docker pull ghcr.io/achird-labs/rift-cluster-server:vX.Y.Z
```

`:latest` follows the newest *non-prerelease* tag. A release candidate is
reachable only by its exact tag, so `latest` cannot quietly become an rc.

**Binaries** — attached to the GitHub Release as
`rift-cluster-server-<version>-<target>.tar.gz` for `x86_64`/`aarch64` Linux and
macOS, with one `SHA256SUMS` covering all of them:

```
curl -sSLO https://github.com/achird-labs/rift-cluster/releases/download/vX.Y.Z/SHA256SUMS
curl -sSLO https://github.com/achird-labs/rift-cluster/releases/download/vX.Y.Z/rift-cluster-server-X.Y.Z-<target>.tar.gz
sha256sum -c --ignore-missing SHA256SUMS
```

Every published binary has been checked to genuinely embed the console bundle —
per platform, not just on the one the lane happens to build first. A build that
silently shipped consoleless is the specific failure
`scripts/check-console-embed.sh` exists to catch, so checking one of four would
have read like coverage while leaving three unverified.

Then unpack and run it:

```
tar -xzf rift-cluster-server-X.Y.Z-<target>.tar.gz
./rift-cluster-server --port 2525 --datadir ./data
```

**On macOS the binaries are unsigned and unnotarised**, so Gatekeeper quarantines
anything downloaded through a browser and the first run is refused with a dialog
rather than an error you can read. Stated here rather than omitted, because the
alternative is a user concluding the download is broken:

```
xattr -d com.apple.quarantine ./rift-cluster-server
```

`curl` does not set the quarantine attribute, so a download made with the command
above needs none of this. Signing is not currently part of the release lane; if
that changes, this paragraph goes away rather than being softened.

**The console and the probe endpoints need `--cluster`.** They are part of the
clustered composition, so the command above — the open-source server, byte for
byte — answers `404` at `/console` and binds nothing on the probe port. For a
single node with both, run a cluster of one:

```
./rift-cluster-server --port 2525 --datadir ./data \
  --cluster --cluster-bind 127.0.0.1:4790 --cluster-allow-solo \
  --cluster-secret-file ./cluster-secret \
  --cluster-probe-bind 127.0.0.1:2526
```

The container form of the same thing is in `deploy/README.md` → *A single
node* — including why a bare `docker run` still reports `healthy` (#297): the
image's health check follows the mode instead of assuming the probe port.

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
this as `rift_cluster_no_principals` (see below), so it is something to check
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

### Upgrading a fleet

Quota enforcement takes effect **inside the replicated apply path**, which makes
it upgrade-sensitive:

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
2. **leaves the Raft membership** — one voter removal, performed by the leader
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
departure: openraft keeps a removed leader replicating until the entry that
removes it is *committed*, so the leaver drives its own departure to completion
and then steps down. Note the asymmetry this creates — the joint entry commits
under the joint quorum, in which the leaving leader still counts, but the uniform
entry commits under the **survivors'** quorum, so a departing leader needs every
remaining voter to acknowledge it.

A voter's departure is a **single** membership change (`RemoveVoters`, not
retaining the node as a learner), which openraft commits as a joint entry then a
uniform one. It is deliberately not demote-then-remove: that shape left the
departing node a caught-up learner still in membership between its two halves,
and the leader's promotion sweep would vote it straight back into the quorum
(D-59).

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
| `GET /_cluster/health` | readiness state and pending gates, whether this node is an isolated owner, the ownership ring (`m_idx` + members), this node's parked-write depth, and `blob_fetch_stall` — non-`null` while this node's apply loop is parked on a blob no member can supply (#439; degraded, not not-ready — see `FleetHealth` in `docs/api/openapi-ee.yaml` for the shape and the reasoning) |

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

> **Retired by D-71** (RFC-007 §3.2, #548): the operator observability pack —
> dashboards, rules, the compose overlay and the CI lanes that checked them — is
> removed, along with every `rift_cluster_*` family nothing but a dashboard read.
> What remains is **correctness instrumentation**, listed with its readers in the
> module doc of `crates/rift-cluster/src/metrics.rs`.

**Membership and leadership are read from `GET /_fleet/members`** on the admin
port, not from metrics: `voters` is the effective voter set, `current_leader` the
node every member should name, `is_leader` whether the answering node holds it,
and `bind_failures` this node's own map of port → reason for a committed config
it could not realize locally (#369). It is served off the node's Raft state at
request time, so — unlike the 5 s-sampled gauges it replaced — there is no
sampler to lag readiness. Whether this node's cluster port runs unauthenticated
is a **startup log line** (`cluster port started WITHOUT authentication`), which
is what a fleet is audited against.

The surviving families are published on the existing metrics port
(`--metrics-port`, default 9090), alongside the open-source metrics: they are
registered into the `prometheus` crate's global default registry, which is what
`GET /metrics` already serves, so there is nothing extra to scrape. That works
only while the whole build links **one** copy of that crate; a second one would
carry its own registry and these families would silently reach no endpoint.
`scripts/check-single-prometheus.sh` enforces it in CI.

Config sync (issue #9): `rift_cluster_config_revision{port}` (applied revision
per imposter — two nodes disagreeing have not converged) and
`rift_cluster_dedup_hits_total` (ops collapsed by op-id — a replay and a retry
proving to be one operation), plus the R4 ledger's `rift_cluster_intents_pending`
(resampled by every replay sweep, so a restart's carried-over ledger reads true).
`rift_cluster_no_principals` is `1` when the fleet has no principal defined at
all (issue #161) — the condition under which the admin plane's open-by-default
bypass applies; resampled continuously, because a `PrincipalPut` can flip it at
any moment the fleet is running.

The pull-on-miss net (issue #49) keeps `rift_cluster_pull_on_miss_retries_total`
— requests sent back through the matcher once, which is what C16 reads. There is
deliberately no `rescues_total`: the hook cannot see the retry's outcome, so such
a counter would be a guess; use the response header below as rescue evidence.

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
any node, forwarded automatically — and most everything else (reads, scenario
state) is reverse-proxied to the local engine unchanged. The recorded-request
journal is the one read that is neither: `GET …/savedRequests`/`…/requests`
with no `since` is a fleet-wide merge-on-read rather than a proxy, and its
`DELETE` best-effort fans out to every peer — see *Merge-on-read: the fleet
request journal* below. A pause replicates and survives restarts (upstream
#817): it applies in place on
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

## Merge-on-read: the fleet request journal (issues #223, #225)

Issue #222 gave every node its own writer shard of the recorded-request
journal, keyed `(node_id, seq, clear_gen)`. This is the read half: `GET
/imposters/:port/savedRequests` and its alias `.../requests`, **with no
`match`**, no longer proxy to the local engine — the front
terminates them as a fleet-wide merge instead, and `numberOfRequests` on `GET
/imposters/:port` and the listing becomes the fleet sum rather than one
node's slot. Both spellings also answer identically under the
`/admin/imposters/:port/...` alias (issue #223 review): `classify` already
gave the imposter *listing* that treatment, and the savedRequests route now
gets it too, so a caller cannot get a different answer — cursor header
included — by spelling the path differently.

**Why terminate instead of decorating the proxied body**, the way the
imposter listing's tenant filter does: the merged response's cursor is a
different *kind* of value from upstream's, so proxying first and then
rewriting a header upstream just set is the fragile direction, and the
issue's acceptance criteria pin the classification flipping outright.

**The mechanics.** A merge pulls this node's own shard plus every other
roster voter's, concurrently, under a single **2 s total** budget — a slow
peer eats into every other peer's share of that budget rather than adding to
it. Any peer that errors, answers something unparseable, or is still
outstanding when the budget expires stamps the response
`Rift-Cluster-Partial: true`; a fully healthy answer carries no such header
at all — never `false` — which is what a Ch.12 strict-mode gate asserts.
**Partial never means a peer's entries vanished**: whatever the last
anti-entropy pass (every 5 s by default, itself fanned out concurrently
under its own 2 s budget) cached for that peer still merges in, so the
header says "possibly missing something newer," not "this peer was
skipped." Entries are ordered by each one's own recorded timestamp, never
a node-local arrival clock — the latter would let two nodes disagree about
the merged order, which is exactly the property the issue's acceptance
criteria require they do not. Every peer pull failure — errored, unparseable,
or lost to the budget — is logged at `warn` with the peer, the port and the real
error, so an operator can tell a partition (self-healing) from a decode failure
from version skew (will not self-heal) apart, rather than seeing only the
aggregate partial rate. The replica cache this warms is bounded by the same per-shard cap the
local journal enforces on itself, and folds a peer's reply in by `seq`
rather than appending it, so it cannot grow past what a fleet's worth of
shards should hold or double-count an entry a race redelivered.

**`?since=` is a vector cursor and terminates too (issue #225).** A scalar
cursor cannot address a multi-writer merge — whose `since` would it be? —
which is why #223 left this half proxied. #225 answers it with a **vector
cursor**: an opaque, versioned, base64url token encoding a position per
writer shard plus the clear generation it was issued under. Every merged read
returns one in `x-rift-next-index`; pass it back as `?since=` to continue.

Round-trip the token verbatim — it is opaque by contract, and this node
rejects a version it does not recognize rather than guessing. Passing it back
resumes the walk **gaplessly and without duplicates per shard**, across
membership changes (a departed node's position freezes rather than rewinding;
a joining node enters at 0) and across clears (a pre-clear token
fast-forwards past retired generations instead of replaying them).
`x-rift-truncated: true` appears exactly when retention evicted entries the
reader had not yet seen.

A bare `u64` is still accepted, and read as this node's own shard position —
before #225 a merged read issued no cursor at all, so any scalar a client
holds came from a proxied per-node read of this node. This is an
upgrade-window courtesy, not a supported shape to construct. Anything that is
neither a token nor a `u64` answers **400**: defaulting it would either
replay the whole journal or silently skip everything recorded since, and both
would surface as mystery data in the client rather than an error.

Because every requests-read through the front is now a merge, there is no
longer a query string meaning "just this node". Read the node's own engine
admin address directly if you need one shard's view.

**`?match=` still proxies** (issue #223 review, B1), and #225 does not change
that: the merge-on-read path never evaluates a match predicate at all, so
terminating on it would silently answer with the *whole* fleet's requests
instead of the caller's scoped subset, and would turn a malformed filter's
upstream `400` into a `200` with everything. Proxying leaves upstream's own
clause parser, and its existing error handling, in charge — and on that path
the cursor headers carry upstream's own scalar index, not a vector token.

**`GET .../savedRequests/stream` is a merged live tail** (issue #348). It
terminates at the front door and answers `text/event-stream` carrying the whole
fleet's recorded requests, not just this node's. It is the same merged walk the
cursor read above performs, resumed on every wake instead of once: the `id:`
line after an event is a cursor token in exactly the sense `x-rift-next-index`
is, and reconnecting with `Last-Event-ID` resumes gaplessly and without
duplicates per shard. You can move between polling and tailing with one token.

The `hello` event carries `clusterTailLatencyMs`. Entries this node records
appear immediately; entries from other nodes ride the anti-entropy cadence, and
that number is the honest upper bound on how late they can be — a tail that
asked every peer per event would multiply inter-node traffic by the number of
connected clients. `partial` events mark every transition into and out of a
degraded merge, and `lagged` means retention dropped entries you had not
reached yet: reconcile by polling, the stream does not replay. `: ping` every
15 s, as single-node.

Additive differences from the single-node stream, all of them declared in
`openapi-ee.yaml`: `hello` gains `clusterTailLatencyMs` and `cursor` and omits
the engine's scalar `seq` (a merged stream has no single bus position); `id:`
is the vector cursor token; `index` appears only on entries this node wrote,
because another node's seq means nothing in this node's numbering; and
`partial` is new.

**With `?match=` the tail keeps proxying**, for the reason the read does: the
merge path evaluates no predicates, so terminating a scoped tail would answer
with the whole fleet's requests instead of your subset.

**`GET /events` still proxies per-node and is FleetAdmin-gated.** That
asymmetry is deliberate — the firehose spans every tenant and is not yet
filtered server-side (issue #163), whereas the per-port tail carries one
imposter's requests and is authorized as the port-scoped `imposter.read` it
always was. Terminating the per-port tail changed no authorization posture.

**`DELETE savedRequests`/`.../requests` is explicitly transitional, and a
`match`-scoped clear never fans out at all.** Without `?match=`, it clears
this node's own journal exactly as before, then best-effort fans an
*unconditional full* clear out to every other roster voter over the cluster
RPC port. A peer missed by the fan-out keeps the deleted entries in its own
shard and in this node's replica cache of it until it is retried or
re-observed; the response is stamped partial in that case. Every receiving
peer also drops its own replica-cache copies of `port` for every node it has
cached (issue #223 review, B2) — clearing only *that peer's own* writer
shard left every other node's stale, pre-clear cache of it untouched, which
is what let a fully successful clear still resurrect the deleted entries via
anti-entropy forever, unstamped, on every peer.

**With `?match=`**, the clear stays local-only and is **never** fanned out
(issue #223 review, B3 — a design decision, not a gap still to close): the
wire fan-out has nowhere to carry a match predicate, so propagating a scoped
clear as an unconditional full clear would over-delete whatever else that
port holds on every other node. The response is stamped
`Rift-Cluster-Partial: true` unconditionally in this case — not because a
peer was unreachable, but because the clear itself never reached them by
design, so a client cannot mistake a scoped, local clear for a fleet-complete
one.

Issue #224 replaces this whole mechanism
with a Raft-committed clear that bumps a `clear_gen` the merge already
honours (pinned at `0` today, a no-op until then) so a clear converges by
consensus instead of a best-effort broadcast a partition can simply outlast,
and can carry a real predicate so a scoped clear stops needing this
local-only carve-out.

**`numberOfRequests`** on `GET /imposters/:port` and the listing is rewritten
in place from the fleet's G-counter slots (`/_cluster/journal/counts`), one
round trip per peer for every listed port at once — the same 2 s budget and
`Rift-Cluster-Partial` contract as the merged entries read, but no separate
metric family: it is a body decoration on an otherwise-proxied response, not
a termination.

**`_rift.flowStateResolved`** on `GET /imposters/:port` (#370) carries the
three per-imposter flow-state knobs the cluster acts on, each as
`{ value, source }`:

```json
"_rift": {
  "flowState": { "backend": "inmemory", "ttlSeconds": 300 },
  "flowStateResolved": {
    "durability":      { "value": "async",            "source": "default" },
    "readConsistency": { "value": "strong",           "source": "default" },
    "flowIdSource":    { "value": "header:X-Session", "source": "set" }
  }
}
```

Two of the three are published here or nowhere. Upstream's `_rift.flowState`
is an **allowlist** — `backend`, `ttlSeconds`, and `flowIdSource` only when
set — because `flowState.redis` can carry a credentialed connection URL, so
anything added later is excluded by default rather than leaked.
`durability` and `readConsistency` live in that block's open `extra` map and
therefore never reach a client through the proxy. The decoration is built from
the *parsed* knobs rather than from the stored document, which is what keeps
upstream's redaction intact through the addition.

The block is **additive**: upstream's `_rift.flowState` is left exactly as it
arrives, `flowIdSource` included as the flat string it renders there — that
shape is what rift-verify reads to drive correlated isolation, and the `parity`
job exists to catch EE diverging from it. `flowIdSource` is repeated into the
resolved block deliberately, so a client reads all three knobs with one shape
instead of inferring provenance for the third from its absence upstream.

`source` is **presence of the key, not equality with the default value**: an
imposter that explicitly pins `durability: "async"` reads as `set`, because
that was a choice. `default` means the compiled-in default — there is no
fleet-level override for these knobs, so it never means "inherited from
somewhere you could go and change".

`contextScope` is deliberately absent: it is not a knob with a fleet default to
resolve but a namespace choice, documented in the knob table above.
Like `owner` on a space read, the decoration is additive and so a body it
cannot parse passes through unchanged and logged, rather than failing the read
the way the `numberOfRequests` correction does.

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

## `--imposters` under `--cluster` — a one-shot bootstrap (D-72, #549)

Upstream's `--imposters <uri,...>` loads imposters from URIs at startup. With
`--cluster` **off** it behaves exactly as it does in the open-source binary.
With `--cluster` **on** it is a **one-shot bootstrap** and nothing more:

1. At startup each URI is resolved through **upstream's own `SourceRegistry`**
   (U-12) — `file:` and `http(s):` only.
2. The fetched documents are parsed by upstream's config loader.
3. Each imposter is submitted as an ordinary `ControlOp::PutImposter`, so it
   lands through the replicated log and reaches every node.

That is the whole mechanism. The cluster keeps **no source record**, runs **no
poll scheduler**, reports **no poll status**, tracks **no drift**, and offers no
`/admin/sources*` surface — all of that was removed by D-71/D-72 (#549). After
startup an imposter that came in this way is indistinguishable from one written
by `PUT /imposters`, and it is edited the same way.

`git+`, `s3:` and `registry:` URIs are **refused at startup** with the scheme
named. They are not supported schemes; a fleet that needs mocks out of Git or a
registry fetches them in CI and writes them with `PUT /imposters`.

Left in place, upstream's own startup would create those imposters in *this
node's* manager, outside the log, and the reconciler — which treats the
replicated set as authoritative — would then delete them again. That is the same
failure `--configfile` is refused for (see the startup guards); `--imposters` can
be desugared instead of refused because a URI is fetchable from every node, while
a local path is not.

**Restarts are idempotent by construction.** Each submitted op carries a
deterministic `op_id` derived from `(uri, port, digest of the fetched document)`,
so a restart — or a second node booting with the same flags — re-submits the same
op ids, which the state machine's dedup map absorbs. Unchanged documents cost the
log nothing; a changed document produces a different digest, hence a different
`op_id`, hence a real write.

A URI that cannot be fetched or parsed, or an imposter that cannot be submitted,
**fails the start**. An operator who passed `--imposters` asked for those
imposters to be serving; a node that comes up healthy without them is the silently
half-configured fleet this path exists to avoid.

A bootstrap document may declare blocks that belong to other subsystems. An
`intercept` block **fails the start** — the cluster refuses the TLS-MITM intercept
listener fleet-wide, because its state is per-node and is not replicated. A
`routes` block is **ignored, with a warning**: the front door's table is its own
replicated object with its own op (`PUT /front-door/routes`, above).

## Compiling OpenAPI: `POST /specs/compile` (D-72, #549)

[`rift-cluster-spec`](../crates/rift-cluster-spec) (#277) is a pure compiler —
`(document bytes, options) → imposter JSON`. This endpoint is the only thing the
server exposes on top of it, and it is **stateless**: it compiles what you send
and answers the result. It stores nothing.

| Route | Action | What it does |
|---|---|---|
| `POST /specs/compile?port=<n>[&name=<s>]` | `imposter.write` (Editor+) | Body is an OpenAPI 3.0 document (JSON or YAML, UTF-8, **≤ 4 MiB**). Answers `{imposter, operations}` — the compiled imposter JSON and the compiled operation index. Commits nothing, writes no log entry, and leaves no record. |

- `port` is **required** — the compiled imposter needs one, and the endpoint will
  not guess.
- `name` is optional and becomes the imposter's name.
- A document that does not compile is refused with the compiler's own reason in
  the typed error envelope. There is no partial result.
- It is authorized as **`ImposterWrite`** rather than a read action or an action
  of its own: compiling is a step in writing an imposter, and the caller who may
  not write one has no use for the output.

The intended flow is two calls, and the second is an ordinary write:

```sh
curl -sX POST "$ADMIN/specs/compile?port=9090&name=petstore" \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/yaml' \
  --data-binary @petstore.yaml | jq .imposter > imposter.json

curl -sX PUT "$ADMIN/imposters" -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' --data-binary @imposter.json
```

Because the second call is a plain `PUT /imposters`, everything the write path
already provides applies unchanged: `Rift-Cluster-Revision` / `Rift-Cluster-Op-Id`,
the write barrier, `Idempotency-Key` dedup, park-and-replay, quotas and RBAC. The
compiler never runs in the apply path, and apply never parses OpenAPI.

**What deliberately does not exist.** There is no stored spec, no `/specs/{id}`,
no re-import, no drift classification, no diff-vs-deployed report, and no traffic
validation mode. An imposter compiled from a document and then hand-edited is
simply an edited imposter; the cluster holds no baseline to call that drift
against. Re-import is: compile again, `PUT` again.

## Clustered flow state (#120)

Under `--cluster`, **every** imposter's flow state (scenarios, `ctx.state`,
flow-state templating, and the declarative `_rift.stateOps` writes) is served
by the clustered store — configured or not, because scenario state on a
process-local store behind a round-robin LB is wrong for every imposter, not
just the ones that thought about it. Each flow has one owner
(rendezvous-hashed over the applied membership); writes are serialized through
it, and by default reads are answered by it, so a scenario behaves correctly
however the LB spreads its steps.

**Writing state without a script (#290, upstream `_rift.stateOps`).** An `is`
response's `_rift` block may carry `stateOps` — `set` / `increment` / `delete`
/ `clearFlow`, run in order after the response is rendered, so a body reading
`{{ state.hits }}` in the same response shows the value before this request's
bump. These are data evaluated by the template grammar, **not** a scripting
surface: a `stateOps` config is admitted with `--allowInjection` off, and
under `--cluster` the ops go through the same owner-routed, replicated,
scope-prefixed store as everything else — a counter increments correctly
behind a round-robin LB with no cluster code on the write path
(`tests/state_ops_cluster.rs`). `increment` is atomic; a `set` that reads its
own key is a compare-and-set loop; a `set` value that is a canonical integer
is stored as a number. See upstream's `docs/features/flow-state.md` for the
full grammar.

Three per-imposter knobs, under `_rift.flowState` (all validated at admission —
an unknown value is a `400` naming the key, never a silent default):

| Knob | Values | Meaning |
|---|---|---|
| `readConsistency` | `"strong"` (default) \| `"local"` | `strong`: every read is owner-answered — correct under any LB, at most one LAN RPC. `local`: reads stay on this node's replica — fast, at most one replication push behind the owner |
| `durability` | `"none"` \| `"async"` (default) \| `"sync"` | What a write survives: `sync` fsyncs before the ack (a full-fleet restart loses nothing), `async` is group-fsynced every `--cluster-flow-fsync-interval-ms` (bounded loss), `none` never touches disk |
| `contextScope` | `"imposter"` (default) \| `"tenant"` \| `"fleet"` | Which imposters share a flow-id namespace. `imposter`: this imposter's flow ids are its own — two imposters resolving the same id (the ordinary result of both using `flowIdSource: "header:X-Session"`) stay isolated, matching single-node behaviour. `tenant` (#288): one namespace across the owning tenant's imposters — a suite spanning two of *your* mocks carries one context through both, and no other tenant's imposter can reach it. `fleet`: one namespace across every imposter of every tenant; **admission requires `FleetAdmin`** (#288) — an Editor's write of a fleet-scoped config is refused with a `400` naming the requirement, nothing committed. Fleet-scoped configs admitted before the gate keep serving; re-admitting one (any config write that carries the knob) needs `FleetAdmin` |

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

The three namespaces are disjoint by construction — every scope carries its
own prefix (`i<port>:`, `t<tenant>:`, `f:`) rather than using bare ids — so no
caller-chosen flow id can be crafted to read across a boundary, not even one
shaped like another scope's prefix.

**`tenant` (#288)** is the middle ground: one namespace shared by all of a
tenant's imposters and reachable by no other tenant's. The tenant is not in the
config (the core schema carries no tenancy — open-core rule); the clustered
store learns it at provide time from the control-plane record that owns the
port, so an imposter's scope is fixed by who committed it. **`fleet` is gated:**
because it crosses every tenant's boundary, admitting a config that sets it
requires the writing principal to hold `FleetAdmin` — refused otherwise with a
`400` naming the requirement, before anything commits. Configs admitted before
the gate keep serving unchanged; the next config write that *carries the knob*
is what needs the role — a stub edit on a fleet-scoped imposter carries no
`flowState` and is not gated, whoever makes it. Under the open admin plane (no
principal configured) nothing gates, exactly as no other authorization does
there. The `--imposters` bootstrap is not a way around the gate either: it runs
before any principal is in view, so once the admin plane is enforced — an
`--api-key` is configured or any principal exists, the same predicate the
front's bypass reads — a bootstrap document that sets `contextScope: "fleet"`
fails the start with the same `400`, naming the port and the way in
(`PUT /imposters` as a `FleetAdmin`, or `tenant` scope in the document).
Configs admitted before the gate keep serving.

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

Instrumentation: `rift_cluster_flow_reads_total{path=owner|forward|local}` says
where reads are answered — it is what pins the D-13 RPC budget per read path —
and `rift_cluster_cas_conflicts_total{reason=cas|fence|misroute|isolated}` counts
owner-side refusals, so a refusal is never dropped silently. A takeover that
could not reach any holder is named in a `warn` line rather than counted.

## The web console (`--features console`, #186, #187)

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

Carrying the console is necessary but not sufficient: serving it also needs
**`--cluster` at runtime**. `/console` is mounted on the clustered admin front,
and with the master switch off this binary is deliberately the open-source
server byte for byte — no extra listener, no extra route (CI's `parity` job
asserts exactly that). A console-carrying binary without `--cluster` therefore
answers `404` at `/console` **by design**, not by defect (#297). §*Installing
from a release* above shows the cluster-of-one invocation that turns it on, and
`deploy/README.md` → *A single node* has the container-flavoured comparison.

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

### What the console shows, and what it refuses to claim (#187)

C4 ships the app shell, a read-only imposter list and detail with
enable/disable, and the cluster view. Four behaviours are worth knowing as an
operator, because each is a deliberate refusal to round a fact off:

- **Everything on screen is one node's answer, except the request journal.**
  `/imposters` is served by whichever node the browser reached, and
  `/_fleet/*` is that node reporting on itself. The screens say so rather than
  presenting either as fleet-wide, and there is no *client-side* fan-out and
  merge anywhere — that would reinvent the verification plane without its
  cursors or gap repair, producing a merged view with no way to know what it
  missed. The one exception is server-side: issue #223 landed the merged
  journal (see *Merge-on-read* above), so `numberOfRequests` and a
  `savedRequests` read are already the fleet's answer, stamped
  `Rift-Cluster-Partial` when the merge could not be sure of it — the console
  does not need to, and must not, merge those itself.
- **An empty list is not the same claim as an empty tenant.** When the answering
  node is degraded — not ready, draining, isolated, leaderless, or **evicted
  from the voter set** — the empty state says the tenant *cannot be confirmed*
  empty and names why. An imposter the node has not applied would not appear.
  The same caveat appears when the console holds the fleet scope but the
  `/_fleet/*` read itself failed: that is a *lost* signal, and reporting it as
  "nothing to report" would present the gap as a clean reading.

  Eviction is the one worth knowing about, because it has no other tell. A node
  removed from the effective membership while still running is not draining, is
  not isolated (`is_isolated` is false for any node that can see a leader), and
  its readiness gates stay satisfied — so it reports itself perfectly healthy
  while owning no part of the ring and receiving no further replication.
- **Unknown renders as `—`, never `0`.** `current_leader: null` means this node
  knows of no leader; showing `0` would name node 0.
- **A refusal from `/_fleet/*` is reported as insufficient scope, not a missing
  page.** Both statuses mean the same thing here: the route authorizes
  `Action::ClusterAdmin` with no tenant scope, so a principal bound to the
  tenant but lacking the role gets **403**, while an unbound one gets the
  RFC-002 §8.4 **404**. The console treats them alike, because the alternative
  is telling an operator their fleet has no cluster.

The tenant switcher sends `X-Rift-Tenant` and nothing else — RFC-002 §8.1's
rules apply unchanged, and the header selects among bindings the principal
already holds. It is hidden entirely for a single-tenant principal. A
`FleetAdmin` binds only to the fleet scope `*`, so its switcher list comes from
`GET /admin/tenants`, which is fleet-scoped and therefore readable by exactly
that principal; if that call fails the console says so rather than letting the
switcher quietly disappear, which would read as a fleet with one tenant.

The console always opens **in a tenant it actually sends**. An unselected
tenant would mean no `X-Rift-Tenant` — which lands in `default` — while the
switcher displayed the first tenant in its list, so every read would be
labelled with a tenant it never asked for; for a principal not bound to
`default` those reads all 404 while the console claims otherwise. It therefore
opens on the remembered tenant, else `default` when the principal is bound
there, else the first it holds.

Screens whose backend or slice has not shipped — request log (#189),
administration (#190) — appear as greyed nav entries carrying their issue
number. A visible roadmap, not a 404 and not an omission.

Reads poll every 5 seconds while the tab is visible and **stop while it is
hidden** (RFC-006 §6). SSE is deferred to v2 and will carry cache invalidation
only. A 4xx is never retried: it is a decision the fleet has already made, and
re-asking only repeats the denial.

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
