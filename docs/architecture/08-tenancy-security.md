# Chapter 8 — Multi-Tenancy & Security

A shared, always-on mock cluster is only shareable if teams cannot trample
each other's configs, and only operable if "who may do what" has a real
answer. This chapter covers the tenancy and RBAC design (RFC-002, issue #17)
and the cluster's internal security model. One framing rule up front: **the
admin plane is authorized per principal; the data plane is isolated by
port/space, not by principal** — mock traffic from a system-under-test carries
no credentials, and pretending otherwise would break every client. Stating
what tenancy does *not* isolate is part of the design.

## The tenancy model

A **tenant** is an explicit control-plane object owning imposters. Not a port
range (collides with runtime-minted ports and Kubernetes single-port
reality), not a `space` (spaces isolate *matching* within one imposter — two
teams sharing a port would share one config object, making per-team edit
rights unrepresentable). The hierarchy:

```mermaid
flowchart TB
    T1["tenant: payments-team"] --> I1["imposter :8080"]
    T1 --> I2["imposter :8443"]
    T2["tenant: search-team"] --> I3["imposter :9090"]
    I1 --> S1["space 'ci-run-441'"]
    I1 --> S2["space 'ci-run-442'"]
    I3 --> S3["space 'perf-nightly'"]

    style T1 fill:#e8f0fe,stroke:#4285f4
    style T2 fill:#e8f0fe,stroke:#4285f4
```

Ports remain globally unique (they are TCP ports); the tenant owns the port
*binding*. Config ownership keys become `(tenant, port)` — the tuple exists to
make ownership explicit, **not** to give tenants independent write paths. Every
tenancy and config write is still leader-serialized and still pays the write
barrier, so one tenant's write burst *does* queue behind another's; ADR-001's
single Raft log is the price of authorization data that is strongly consistent
(RFC-002 §3.1). The OSS config schema never learns the field — tenancy is stored
on the control-plane record and injected/stripped at the API boundary, keeping
the open-core line clean.

> **Design of record: [RFC-002](../rfc/RFC-002-multi-tenancy-and-rbac.md).** This
> chapter is the architectural overview; the RFC carries the normative model,
> role/action matrix, threat model and phasing.

**Migration:** everything pre-tenancy lands in a reserved `default` tenant;
the legacy `--api-key` maps to a synthetic principal with admin rights on it.
Tenancy becomes real the day a second tenant is created, with zero day-one
breakage.

### What T1 ships, and the one thing it deliberately does not

Slice T1 (RFC-002 §10, issue #159) has landed: `Tenant`, `Principal`,
`RoleBinding`, `Role` and `Quotas` are real records; the six reserved
`ControlOp` variants carry typed payloads; `sm_tenants`, `sm_principals` and
`sm_bindings` are state-machine tables, in snapshots in both directions; and
the single-tenant gate is lifted, so every op accepts any well-formed tenant
slug.

**Storing is not serving.** T1 delivers the records and their storage, not
tenant-aware serving. A resource op naming a non-`default` tenant is validated,
committed and stored against `(tenant, …)`, and `TenantDelete` cascades over
it — but the read and sync paths (`desired_configs`, `desired_routes`,
`read_config`, `configured_ports`, `sources`) still filter to `default`, so
nothing binds it and no operator surface reports it. T1's exit criterion —
*no observable change* — holds because the admin HTTP front constructs
`TenantId::default()` at every call site, so nothing reachable over the API can
create such a row; only a direct `RiftNode::submit` can, which is how the tests
exercise the cascade and the fleet-wide port rule.

One consequence to carry into the slice that makes serving tenant-aware:
because ports are fleet-unique across tenants, a config stored for tenant A
*does* claim its port against tenant B. That is RFC-002 §3.2's rule, not a bug,
but it means the read paths must land in the same PR as tenant-aware serving —
otherwise an operator can be refused a port that nothing is listening on and no
read path reports as taken.

Two deletion rules are security properties rather than tidiness, and both are
enforced in the cascade: **`TenantDelete` removes the tenant's bindings**, and
**`PrincipalDelete` removes that principal's bindings across every tenant.** A
tombstone records that an id existed; it does not reserve it. Bindings left
behind would come back to life the moment the name is reused — and principal
ids in particular can be external values (an OIDC `subject`, an mTLS SAN) that
identity providers recycle. Rows already committed to the log cannot be
repaired afterwards, which is why this is settled here rather than in #161.

**Quotas are stored, not enforced.** `Quotas` rides the `Tenant` record so the
shape is in the log format now; enforcement is #163.

## Principals and roles

Principals are API keys in v1 (argon2id hashes stored, key shown once at
creation), OIDC subjects and mTLS SANs in v2. Roles bind a principal to a
tenant, additive, deny-by-default:

| Role | May |
|---|---|
| `viewer` | read everything in-tenant: imposters, stubs, saved requests, scenarios, SSE streams |
| `operator` | viewer + **pause/resume** (`enable`/`disable`), clear saved requests, reset scenarios, tear down spaces — runtime control without config edits |
| `editor` | operator + create/update/delete imposters and stubs |
| `tenant-admin` | editor + manage the tenant's principals and bindings |
| `fleet-admin` | everything, cross-tenant, plus `/_cluster/*` and delete-all (a binding on the reserved tenant `*`) |

The `operator` role is why issue #15 (replicated `enabled`) mattered for
tenancy: pause/resume that silently applies to one node out of three is not a
role anyone can be given. It has since landed (upstream `EnabledChanged`,
v0.16.0), so Phase T's only hard dependency is Phase 1's control plane
(RFC-002 §10).

All tenancy records — tenants, principals, bindings — are entries in the Raft
state machine (Chapter 3). This is deliberate beyond convenience:
**authorization data is strongly consistent by construction.** An RBAC
revocation that propagates "eventually" is a security hole with a metrics
dashboard; here a revocation is a committed log entry, effective at apply on
every node, and auditable by index.

## Enforcement and the two upstream seams

Enforcement lives at every admin entry point — route handlers, SSE subscribe,
`/_cluster/*` (fleet-admin only) — via a generic upstream hook, because the
open-source admin API must stay tenancy-ignorant:

Both seams have **landed upstream** and are in the current pin — U-9 as
`rift-mock-core::extensions::authz`, U-10 as `EventContext` on the listener
signature. Both are re-exported through `rift_ee::seams` (issue #160).

- **U-9, `AdminAuthorizer`**: a trait consulted after route parsing with
  `(credential, action, port, space, scope, params)`, returning
  allow-with-principal or deny. Installing nothing changes nothing: with no
  authorizer registered the api-key comparison decides alone, exactly as before.
  The enterprise implementation resolves principal → bindings → role → action.
  Generic OSS justification: embedders fronting Rift with their own identity
  currently have to reverse-proxy and re-parse routes to get any authorization
  at all.

  Three parts of the contract enforcement (#161) is built on, stated with their
  limits rather than as slogans:

  **Ordering.** The api-key check runs *before* the route is parsed, and only
  then is the hook consulted, so `Deny` renders `403` and a bad key renders
  `401`. But the gate is `if let Some(key) = api_key` — **keyless is upstream's
  default**, and a loopback admin plane with no `--api-key` is a supported
  configuration. So "authenticated" is a precondition, not a guarantee: an EE
  deployment that installs the authorizer and drops `--api-key` on the grounds
  that RBAC now handles identity gets every request arriving with
  `credential: None`. #161 must decide explicitly whether the EE authorizer
  refuses an absent credential or whether the binary requires a key when
  clustered.

  Note also that an EE authorizer **cannot produce a `401`**: `AuthzDecision` is
  `Allow`/`Deny` only, and `Deny` maps unconditionally to `403`. Under EE RBAC a
  missing credential is therefore a `403`, not a `401` — the `401`/`403` split
  described above belongs to the built-in api-key gate, not to us.

  **What the hook does *not* bound.** When route classification returns `None`
  — an unmatched path, or the `/__rift/` gateway — the authorizer is never
  consulted and the request falls through to the ordinary `404`. Upstream states
  the consequence plainly: *"an authenticated caller can still distinguish a
  `404` from a `403`, so the hook bounds what a principal can do, not what it
  can learn about which routes exist."* Route-existence is not concealed; do not
  claim otherwise in tenancy-isolation copy. What the ordering *does* buy is
  that an **unauthenticated** caller cannot use it as an oracle when a key is
  set.

  **`scope` is caller-asserted.** It arrives in the `x-rift-scope` request
  header, so any caller can set it to any value. It names which target a create
  is *claimed* for (`POST /imposters` has no port yet); it must be cross-checked
  against what the credential entitles, never used as the authorization subject.

  Upstream also ships an `actions` module of stable action-string constants
  (`system.read`, `system.write`, `imposter.read`, `imposter.write`,
  `imposter.delete`, `imposter.verify`, `events.read`, `intercept.read`,
  `intercept.write`). `seams_resolve` names all nine, so an upstream rename is a
  compile error here rather than a silently never-matching match arm.

- **U-10, attribution on change events**: a separate `EventContext` parameter on
  `ImposterEventListener::on_event`, **not** a field on `ImposterEvent`. That
  enum is not `#[non_exhaustive]`, so adding a field to every variant would
  break every downstream `match` — the wrong trade for a seam whose premise is
  that installing nothing changes nothing. `EventContext` is itself
  `#[non_exhaustive]` so the next attribution field (scope, request id, remote
  address) is not a second breaking change; embedders build one from `Default`
  and assign.

  The principal reaches the emit site through a **task-local** scope
  (`with_principal_scope` / `current_principal`) rather than a parameter
  threaded through `create_imposter`, `delete_imposter`, `apply_config`,
  `add_stub` and every other mutating method. A task-local follows the task
  across `.await` but **not** across `tokio::spawn`. Upstream is careful about
  this: all sixteen emit sites in `ImposterManager` are direct calls inside the
  mutating method, none is inside a spawned task, so single-node attribution is
  complete.

### The task-local does not reach a clustered write — attribution rides the log

**This is the fact #163 has to be built on, so it is stated here rather than
discovered later.** In a clustered deployment the admin request does not call
the manager. It appends a `ControlOp` and returns; the mutation happens when
openraft applies the entry:

```
admin request task            openraft state-machine task
  with_principal_scope(...)     RedbStateMachine::apply
  append ControlOp        ──▶     drive_engine
  await commit                     engine.apply_config(...)
                                     ImposterManager::emit
                                       current_principal()  ──▶  None
```

`apply` is driven by openraft's own task, not by the task that opened the scope,
so the task-local is out of scope by the time `emit` runs. **Every replicated
write attributes `None`** — and replicated writes are the whole of the clustered
write path, which is exactly what an audit trail is for. Followers are further
still from any request: they apply entries no client ever spoke to them about.

This is not a defect in U-10. A task-local is the right mechanism for the
in-process path it was designed for, and no upstream seam could have carried a
principal across a Raft log it knows nothing about. The clustered answer is the
one the log format already anticipates: `ControlRequest.principal`
(`crates/rift-cluster/src/control.rs:61`) is in the envelope today and `None` at
every construction site. #161 populates it from `AuthzDecision::Allow`, and #163
reads it at apply time. `EventContext` remains the attribution path for the
embedded/single-node case.

  One gap neither mechanism closes: `AllDeleted` carries no port, therefore no
  tenant. Its audit record gets `tenant: null, resource: "*"` — correct, and
  stated here so the null is not later read as a bug.

Quotas (max imposters, stubs per imposter, flow-KV entries, journal retention)
enforce at the one place that sees a tenant's entire write stream — the Raft
leader's pre-append validation — plus the flow owner for KV counts.

**Audit** falls out of machinery that already exists: the intent log (Chapter
4) records *what was asked, when, with which op-id*; U-10 adds *by whom*.
Every applied ControlOp emits
`{ts, principal, tenant, action, resource, op_id, revision, outcome}` into a
retained, queryable audit table. One stream, not a bolted-on second system.

### What T2 ships — enforcement, and its two deliberate over-restrictions

Slice T2 (issue #161) turns the model into a boundary. The closed 19-action set
and the `Role → Action` table live in `rift-ee-server`'s `authz` module as a
**pure** evaluator — no I/O, no HTTP — so the whole matrix is unit-testable
without a cluster. Bindings are read fresh from the local state machine on every
request; there is **no authorization cache**, ever (§8.5), because a per-node TTL
would reintroduce exactly the revocation window consensus is being paid for.

**One evaluator, but authorization happens at the front, for every request.**
The design in the issue put the enterprise check on terminated routes and left
proxied ones to the U-9 hook. That cannot satisfy §8.4: upstream's
`AuthzDecision` is `Allow`/`Deny` only and `Deny` renders **403
unconditionally**, so a cross-tenant probe on a proxied route would answer 403
and thereby confirm the tenant exists. So `admin_front` authorizes *everything*
before the terminated/proxied split and renders 401/403/404 itself, classifying
proxied routes with **upstream's own exported `classify`** (rift#889) rather than
a second parser — upstream has already shipped a bug from exactly that
divergence. The `AdminAuthorizer` stays installed on the loopback as defence in
depth.

Two places T2 is deliberately **stricter** than RFC-002 §4.2. Both are
fail-closed responses to a capability this build does not yet have, and both lift
without a redesign:

1. **`/events` requires `ClusterAdmin`, not `StreamSubscribe`.** §4.3 point 2 has
   two halves — authorize the subscribe, and filter the stream server-side — and
   only the first is built, because the SSE payload is upstream's own and carries
   no tenant to filter on. Serving it at `StreamSubscribe` (a *Viewer* grant)
   would hand a viewer of one tenant every other tenant's recorded request
   bodies, which is worse than the 403-vs-404 oracle this slice closes. Only a
   fleet admin — entitled to all of it anyway — may subscribe until #163 adds
   filtering, at which point the route returns to `StreamSubscribe`.
2. **Resource operations are servable only for the `default` tenant.** T1 made
   the state machine *store* by tenant but not *serve* by tenant:
   `desired_configs` and `desired_routes` still skip everything that is not
   `default` when binding the local engine, and `route_table()` is the default
   tenant's. Authorizing a read for tenant `acme` and then serving it from that
   engine returns **`default`'s** data — a documented scope limit turned into a
   cross-tenant bypass. So a decided tenant other than `default` is refused with
   the same indistinguishable 404 a cross-tenant probe gets, in a single guard at
   the one choke point every admin request passes through. The terminated ops
   already thread the decided tenant through, so lifting this guard when serving
   becomes tenant-aware does not also require re-plumbing them.

The second is the honest statement of where multi-tenancy actually stands: the
records exist and are enforced against, but until the read and sync paths are
tenant-aware, `default` is the only tenant that can be served. **That work must
land the read paths and this guard's removal in the same change** — separating
them is what produced the bypass in the first place.

**Legacy migration.** The `--api-key` maps to a synthetic principal
(`legacy:api-key`, which cannot collide with a real `key:<fingerprint>` id) bound
`TenantAdmin` on `default`, plus `FleetAdmin` on `"*"` while
`--cluster-legacy-key-is-fleet-admin` is set — default **true** for one release,
then false, then removed. The key is deliberately withheld from upstream's own
builder: leaving it set would install a second, independent api-key gate on the
loopback that runs *before* the authorizer hook, and would 401 every real
principal on every route for any fleet mid-migration. A fleet with neither an
api-key nor any principal keeps the pre-T2 open admin plane, and
`rift_cluster_no_principals` reports that state for Prometheus.

### What T3 ships — the admin surface, and keys that are shown once

Slice T3 (issue #162) is what makes the model reachable: RFC-002 §5's routes on
the admin front, argon2id key issuance, and `whoami`.

```
POST/GET         /admin/tenants                      FleetAdmin
GET/PUT/DELETE   /admin/tenants/:id                  FleetAdmin
POST/GET         /admin/tenants/:id/principals       TenantAdmin
PUT/DELETE       /admin/tenants/:id/principals/:pid  FleetAdmin
PUT/DELETE       /admin/tenants/:id/bindings/:pid    TenantAdmin (FleetAdmin on "*")
GET              /admin/whoami                       any authenticated principal
```

Every one of these **terminates at the front door**, reads included — these
records live only in the clustered control plane, so there is no upstream route
to proxy to. That is the same shape `GET /front-door/routes` already has.

**The tenant comes from the path, not `X-Rift-Tenant`.** On a resource route the
header selects which of the caller's bindings they are acting under; here the
tenant *is* the record being administered. Authorizing `/admin/tenants/b/...`
against the header would let a tenant admin of `a` administer `b` by sending one
header — the confused-deputy shape §8.1 exists to close, reached through the one
surface where the header is not the subject.

**Why bindings split across two tiers.** The route
`/admin/tenants/:id/bindings/:pid` is `TenantManage` inside a real tenant and
`ClusterAdmin` on the fleet scope `"*"`. This is not a special case bolted on:
`validate` refuses every role but `FleetAdmin` on `"*"`, so a binding *there* is
by definition a grant of fleet privilege, and granting fleet privilege must
require fleet privilege (§4.2). Inside a tenant, re-binding a principal is a
tenant admin's own job and grants nothing beyond their own scope. What decides
the tier is the privilege being granted, not the shape of the path.

`PrincipalPut` and `PrincipalDelete` are fleet-only for the reason §3 gives:
`sm_principals` is keyed by principal id **alone**, so a tenant admin of A
deleting a principal would destroy a credential B also relies on. The one
exception is `POST /admin/tenants/:id/principals`, which is `TenantManage` —
minting an identity inside a tenant grants nothing outside it. It is also
refused if it asks for `fleet-admin`, for the same reason the fleet-scope
binding needs fleet privilege.

**One op, one revision.** That mint is a single `ControlOp::PrincipalCreate`,
not a `PrincipalPut` followed by a `BindingPut`. Two ops are two revisions, and
the gap between them is observable on every replica: a principal that
authenticates and is authorized for nothing, or — if the second op is lost to a
leader change — a binding naming a principal that does not exist. Neither state
is reachable through one op, because apply already runs inside a single redb
write transaction.

**Keys are shown once, and the disk is the assertion.** The response to that
mint is the only place the raw key ever exists. The control plane stores the
argon2id hash and the SHA-256 fingerprint the id is derived from, and neither
can reproduce it. The acceptance test does not merely check that a later `GET`
omits the key — that would prove only that one renderer omits it — it scans
every byte under the state directory after shutdown. The log and the snapshot
are where a leaked credential would become *permanently* unredactable, because
a committed Raft entry cannot be rewritten.

**An unknown key performs zero argon2id verifications.** A principal's id is
`key:<sha256(raw)>`, so a presented credential resolves by one keyed lookup and
argon2id verifies exactly the one candidate it finds. A credential that matches
nothing misses that lookup and is refused having hashed nothing. This is not an
optimisation: argon2id at the pinned cost allocates 19 MiB per attempt, so
hashing on every presented credential would make an endpoint anyone can reach a
memory-amplification lever. It is asserted with a counter
(`control::argon2_verifications`) rather than a timer, because a timing
assertion for this property is flaky by construction and would be the first test
anyone disabled.

The cost parameters are pinned in `control.rs` to the OWASP 2024 baseline
(m = 19456 KiB, t = 2, p = 1) rather than inherited from the argon2 crate's
default, even though the two agree today. A cost parameter is the whole strength
of a password hash and it fails silently in both directions. Raising it later
does not invalidate existing keys: the PHC string records the parameters each
hash was produced with, and verification reads them from there.

**The T2 default-tenant guard does not apply here.** T2 refuses any decided
tenant other than `default`, because resource *serving* is still default-only.
That reasoning is about `sm_configs`/`sm_routes`/`sm_sources`, which are stored
per tenant but read back through default-only paths. The tenancy tables are not
like that — `tenant`, `tenant_principals` and `principal_bindings` all take the
tenant as an argument and honour it — so keeping the guard over them would 404
the entire surface for exactly the tenants it exists to administer. The
exemption is scoped to routes that name their own tenant, so a new action stays
subject to the guard until someone states otherwise.

**No verification cache.** RFC-002 §11 raised caching
`hash(credential) → principal_id` to avoid an argon2id verify per request. T3
does **not** ship one. §8.5's ban on cached authorization holds either way, but
the cost this would save is one verify on an admin-plane request, and the
machinery — an invalidation hook on four op variants, with a correctness bug
that fails *open* — is not earned by that. Operators should expect roughly
20–50 ms of argon2id per authenticated admin request. If that becomes a real
constraint for a console or MCP client, the state-machine-invalidated cache
described in §11 is the shape to build, and it should land as its own slice with
its own revocation test.

## Cluster-internal security

The node-to-node surface (Raft RPCs, owner-forwarded ops, replication, journal
pulls) shares one model:

- **A dedicated cluster port**, explicitly configured, intended for a private
  network; binding `0.0.0.0` requires an explicit acknowledgment flag. Never
  multiplexed with data-plane or admin ports.
- **Shared-secret HMAC on every message**:
  `X-Rift-Cluster-Auth: t=<ts>,n=<nonce>,mac=HMAC-SHA256(secret, ts‖nonce‖method‖path‖body)`,
  ±30 s skew window, bounded nonce cache that **fails closed** on overflow.
  Startup refuses clustering without a secret unless `--cluster-insecure` is
  passed, which logs loudly and sets a metric (`rift_cluster_insecure 1`) so a
  fleet can be audited for it.
- **Integrity and authenticity, not confidentiality** — the threat model is
  "no unauthenticated peer joins or injects ops", with confidentiality
  delegated to network isolation (VPC/namespace/WireGuard). mTLS between nodes
  is a hardening milestone, deliberately not a Phase-1 gate.
- **Version skew**: every message carries a protocol version; majors must
  match (mismatch → clean rejection, not undefined behavior), minors are
  additive — the contract that makes rolling upgrades safe (Chapter 10).

Admin-plane auth (bearer key today, U-9 principals tomorrow) is TLS-at-the-LB
plus application auth; probe endpoints (`/readyz`, `/healthz`) are
deliberately unauthenticated and stateless-safe, because kubelets and LBs
don't hold credentials.

## Explicit non-goals

Recorded so the boundary cannot be oversold: no per-principal data-plane
authorization (see the framing rule); no per-tenant TLS identities on imposter
ports; no compute/memory isolation between tenants (quotas bound object
counts, not CPU); no cross-cluster tenancy federation. Each of these is a
conscious "no", not an omission.
