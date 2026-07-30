# RFC-002 — Multi-tenancy, RBAC and Audit (v1)

| | |
|---|---|
| **Status** | v1 — design complete, implementation-ready; ships as **Phase T**, parallel to RFC-001 Phases 2–3 |
| **Tracking issue** | [achird-labs/rift-enterprise#17](https://github.com/achird-labs/rift-enterprise/issues/17) |
| **Canonical location** | `rift-enterprise:docs/rfc/RFC-002-multi-tenancy-and-rbac.md` |
| **Depends on** | **ADR-001** (Raft control plane) and **#14** — the state machine this RFC's records live in. Nothing here works on an eventually-consistent store; see §3.1 |
| **Ground truth** | Verified absent at `919495e`; upstream citations resolve against `vendor/rift` @ v0.16.0 |
| **Author** | Mohsen Zainalpour |
| **Date** | 2026-07-23 |

---

## 1. Summary

Rift Enterprise today has exactly one admin credential: a global bearer token
compared against a single string. There is no principal, no role, and no tenant
anywhere in the system. That is adequate for one team running one fleet, and
inadequate for the moment two teams share one.

This RFC adds three things, in one coherent model:

1. **Tenants** — a named ownership boundary over imposters and the state hanging
   off them, stored as control-plane records so the boundary is strongly
   consistent rather than eventually so.
2. **RBAC** — principals, four in-tenant roles plus a fleet role, and a closed
   set of actions checked at *every* enforcement point, because a boundary
   enforced at some of its edges is decorative.
3. **Audit** — one stream, one format, covering every write, retained and
   queryable.

It also states plainly what tenancy does **not** isolate (§7), because the most
dangerous thing a multi-tenancy design can do is imply more separation than it
delivers.

## 2. Why — the verified gap

Checked at `919495e`, not assumed:

- Admin authentication is one global bearer: `AdminApiServer` holds
  `api_key: Option<Arc<String>>` (`admin_api/server.rs:36`) and gates requests on
  a single constant-time comparison — the gate at `server.rs:479-491`, the
  comparison in `api_key_matches` at `:581`. Success yields *access*, not an
  identity: there is nothing to attribute a change to.
- The words *principal*, *role* and *tenant* do not appear in `rift-mock-core`
  or `rift-http-proxy` in any authorization sense.
- RFC-001 §11.2 deliberately leaves `--api-key` unchanged; it scoped cluster
  security to the node-to-node port and left client-facing authz to this RFC.
- `Stub.space` (upstream #223) is sometimes mistaken for tenancy. It is not: it
  partitions **matching** per flow-id inside one imposter. Two tenants sharing a
  port still share one `ImposterConfig`, so "these stubs are mine and you may not
  edit them" is unrepresentable today. Spaces isolate *behaviour*; tenancy has to
  isolate *ownership*.

The gap is therefore not "authorization is weak" but "there is no subject to
authorize" — which is why this is a data-model RFC before it is a policy one.

## 3. Data model

All records live in the #14 control-plane state machine.

```rust
pub struct TenantId(String);          // slug, immutable, e.g. "payments-team"
pub struct Tenant {
    id: TenantId,
    display_name: String,
    quotas: Quotas,
    created_at: SystemTime,
    deleted: bool,                    // tombstone; see §3.3
}

pub struct PrincipalId(String);       // "key:<fingerprint>" | "oidc:<iss>#<sub>" | "mtls:<san>"
pub struct Principal {
    id: PrincipalId,
    display_name: String,
    auth: AuthSource,
    disabled: bool,
}
pub enum AuthSource {
    ApiKey { hash: [u8; 32] },              // v1 — argon2id
    Oidc { issuer: String, subject: String },   // v2
    MtlsSan { san: String },                    // v2
}

pub struct RoleBinding { principal: PrincipalId, tenant: TenantId, role: Role }
pub enum Role { Viewer, Operator, Editor, TenantAdmin, FleetAdmin }
// FleetAdmin is only valid on the reserved tenant "*"; see below.

pub struct Quotas {
    max_imposters: u32,
    max_stubs_per_imposter: u32,
    max_flow_entries: u64,
    journal_retention: Duration,      // 0 / None = unlimited
}
```

New `ControlOp` variants: `TenantCreate` / `TenantUpdate` / `TenantDelete`,
`PrincipalPut` / `PrincipalDelete`, `BindingPut` / `BindingDelete`.

**A `Principal` is a fleet-global object, and only a `RoleBinding` is
tenant-scoped.** That asymmetry has one sharp consequence, so it is a rule rather
than an observation: `PrincipalPut` and `PrincipalDelete` are **`FleetAdmin`
only**. A `TenantAdmin` may create, bind and unbind principals *within its
tenant*, which is `BindingPut` / `BindingDelete` — it may never delete the
principal object itself.

Without that rule the design has a cross-tenant hole in its normal case, not a
corner: principals may be bound to several tenants (§5, §8.1), so a `TenantAdmin`
of A issuing `DELETE /admin/tenants/A/principals/:pid` against a principal also
bound to B would destroy B's credential. Tenant-scoped routes operate on the
binding; the principal is fleet state.

The one exception is principal *creation*, which a `TenantAdmin` needs in order
to be useful: `POST /admin/tenants/:id/principals` mints a principal **and** its
binding to `:id` as a single committed operation. Minting a new identity grants
nothing outside the tenant, so it is safe; removing one is what is not.

### 3.1 Why this must be on consensus

Authorization data that is eventually consistent is a security smell, and the
reason is concrete rather than aesthetic: a revoked binding that has replicated
to two of three nodes means the revoked principal keeps its access on the third
for as long as replication lags. Under a round-robin load balancer that is not a
narrow window — it is one request in three.

Putting these records in the Raft log makes the revocation **committed or not**,
with nothing in between, and makes every node's answer the same answer. This is
one of the reasons ADR-001's decision matters beyond durability, and it is why
Phase T depends on #14 rather than merely coexisting with it.

The corollary is a cost worth stating: tenancy writes are leader-serialized and
pay the write barrier, exactly like config writes. Creating a tenant is not a
hot path, so this is the right trade — but a deployment that cannot reach a
leader cannot change authorization, and that is deliberate.

### 3.2 Tenant ↔ resources

`ImposterConfig` gains `tenant: Option<TenantId>` **enterprise-side only**. It is
stored on the control-plane record — the state machine keys configs by
`(tenant, port)` — and injected or stripped at the API boundary. The
open-source config schema is untouched and upstream never learns the field. This
follows the same open-core rule as everything else in `docs/architecture/11-open-core.md`:
cluster and tenancy vocabulary does not cross into `rift-mock-core`.

Two consequences that are easy to get wrong:

- **Ports are globally unique across tenants**, because they are TCP ports. A
  tenant owns the *binding* of a port, not a private namespace of ports. Two
  tenants cannot both hold 6001. Config-ownership keys therefore become
  `(tenant, port)` while the port itself stays fleet-unique — the tuple exists
  to make ownership explicit, not to permit collision.
- **Spaces nest under the imposter**, hence under its tenant. A space is not a
  tenancy mechanism (§2) and gains no access-control meaning here.

### 3.3 Deletion

`TenantDelete` is a tombstone plus a cascade over the tenant's imposters, applied
as one committed operation. Half-deleted tenancy — records gone, imposters still
bound and serving — would leave resources no principal can administer. The
tombstone is retained rather than the record being erased, for the same reason
config deletes are recorded rather than erased under ADR-001: a delete that leaves
no trace is indistinguishable from a write that never replicated. (RFC-001 §7.4.5
argued this for the gossip design; ADR-001 superseded its *mechanism* — the
ack-vector GC is gone — but not the reasoning, which is why the citation is to
ADR-001 and not to that section.)

### 3.4 Migration — nothing breaks on day one

A cluster upgraded from a pre-tenancy release:

- assigns every existing imposter to the reserved tenant **`default`**;
- maps requests authenticated with the legacy `--api-key` to a **synthetic
  principal**, bound `TenantAdmin` on `default`;
- and additionally binds it `FleetAdmin` when
  `--cluster-legacy-key-is-fleet-admin` is set — **default true for one release**,
  then default false, then removed.

So the day of the upgrade, every existing call keeps working and nothing observable
changes. Tenancy becomes real the day someone creates a second tenant. The
staged default on that flag is the whole migration story: it converts a breaking
change into a deprecation with a visible date.

## 4. Authorization model

`(principal, action, resource) → allow | deny`. **Deny by default.** Roles are
purely additive, so no deny-overrides rule is needed and none is provided —
introducing one later would be a breaking change to the evaluation semantics, so
it is called out here as a deliberate closed door.

### 4.1 Actions

A closed enum, one per route class:

`ImposterRead` · `ImposterWrite` · `ImposterDelete` · `StubWrite` ·
`LifecycleToggle` · `SavedRequestsRead` · `SavedRequestsClear` · `ScenarioRead` ·
`ScenarioWrite` · `ScenarioReset` · `SpaceStubWrite` · `SpaceTeardown` ·
`FlowStateRead` · `FlowStateClear` · `VerifyRun` · `StreamSubscribe` ·
`TenantManage` · `AuditRead` · `ClusterAdmin`

`AuditRead` is granted to `TenantAdmin` (scoped to its own tenant) and to
`FleetAdmin` (fleet-wide). It is deliberately **not** part of `TenantManage`:
reading who did what and changing who may do what are different powers, and
collapsing them would make every principal-manager an auditor by accident.

`GET /admin/whoami` is the one route with **no** action, by design: it reports
only the caller's own identity and bindings, so any authenticated principal may
call it and there is nothing to authorize beyond authentication itself.

Closed on purpose: an open action set means a new route can be added without
anyone deciding who may call it, which is how authorization gaps appear one
convenience at a time. Adding a route means adding an action here.

### 4.2 Roles

| Role | Grants |
|---|---|
| `Viewer` | all `*Read` + `StreamSubscribe`, in-tenant |
| `Operator` | Viewer + `LifecycleToggle`, `SavedRequestsClear`, `ScenarioReset`, `SpaceTeardown`, `FlowStateClear` |
| `Editor` | Operator + `ImposterWrite`, `ImposterDelete`, `StubWrite`, `SpaceStubWrite`, `ScenarioWrite`, `VerifyRun` |
| `TenantAdmin` | Editor + `TenantManage` (principals and bindings **within** the tenant) |
| `FleetAdmin` (only on tenant `"*"`) | everything in every tenant, plus `ClusterAdmin`: `/_cluster/*`, all-tenant `DELETE /imposters`, tenant CRUD, and `PrincipalPut`/`PrincipalDelete` (§3) |

**Encoding of `FleetAdmin`, stated because the obvious encodings are wrong.**
`FleetAdmin` is a `Role` variant like any other, valid **only** in a binding whose
tenant is the reserved id `"*"`; `BindingPut` rejects it on any other tenant, and
rejects every other role *on* `"*"`. The tempting alternatives both fail:
inferring it from "`TenantAdmin` on `*`" makes the wire value ambiguous and means
a tenant literally named `*` would mint fleet privilege, and omitting the variant
entirely (an earlier draft) leaves `PUT .../bindings/:pid {role}` with no value
that expresses it.

Consequently `/admin/tenants/*/bindings/:pid` is a `FleetAdmin`-only route even
though the path shape says `TenantAdmin`: granting fleet privilege must require
fleet privilege. Wire values for `role` are the lower-kebab forms —
`viewer`, `operator`, `editor`, `tenant-admin`, `fleet-admin`.

The split between `Operator` and `Editor` is the one worth explaining: Operator
may *disturb* state (reset a scenario, clear saved requests, toggle an imposter)
but may not *redefine* it. That matches how these systems are actually used —
the person debugging a failing test run needs to clear and re-run, and does not
need to rewrite the contract while doing it.

### 4.3 Enforcement points

All of them, or the boundary is decorative:

1. **Admin API** — via the upstream **U-9** hook (§6), consulted after route
   parse and before the handler runs.
2. **SSE streams** (`/events`, `.../savedRequests/stream`) — the same hook at
   subscribe time, and events are **tenant-filtered server-side**. Filtering in
   the client would mean the server had already sent another tenant's events.
3. **Gateway data plane** (`/__rift/:port/...`) — **not authenticated**. See §7;
   this is a stated non-goal, not an oversight.
4. **`/_cluster/*`** — `ClusterAdmin` only, or the cluster secret for
   node-to-node traffic (which is a different credential on a different port,
   RFC-001 §11.2).

### 4.4 Quotas

Enforced at the **Raft leader during `ControlOp` validation** — the one place
that sees a tenant's whole write stream and can therefore count it. Enforcing in
the handler would mean each node counting its own view, which under concurrent
writes admits over-commit.

- `max_imposters`, `max_stubs_per_imposter` — checked pre-commit at the leader.
- `max_flow_entries` — enforced by the flow owner, per tenant; the ring key
  already carries the tenant via the port→tenant map.
- `journal_retention` — applied by the shards.

Quotas bound **object counts, not compute** (§7).

## 5. Admin API surface

Enterprise routes, on the same admin port:

```
POST/GET         /admin/tenants                      FleetAdmin
GET/PUT/DELETE   /admin/tenants/:id                  FleetAdmin  (DELETE = tombstoned cascade, §3.3)
POST/GET         /admin/tenants/:id/principals       TenantAdmin
PUT/DELETE       /admin/tenants/:id/principals/:pid  TenantAdmin
PUT/DELETE       /admin/tenants/:id/bindings/:pid    TenantAdmin  body: {role}
GET              /admin/whoami                       any authenticated principal → {principal, bindings}
```

**API keys are shown once.** `POST .../principals` with `auth: {type: "apiKey"}`
returns the generated key in that response and never again; the server stores
only an argon2id hash. A key that can be re-read is a key that leaks from
whatever stores it.

**Existing imposter routes are unchanged in shape.** The tenant is derived from
the principal's binding when the principal has exactly one, or named explicitly
via `X-Rift-Tenant` when it has several. Naming a tenant the principal is **not**
bound to answers **`404`**, not `403` — see §8.4, which owns that rule; `403` is
reserved for "you are bound to this tenant but your role is insufficient". See
§8.1 for why that header is the sharpest edge in this design.

`GET /admin/whoami` exists so a principal can discover its own scope without
guessing. It is also the cheapest possible smoke test that authorization is wired
at all.

## 6. Upstream seams

Both are drafted to the Appendix-A standard of RFC-001: generic naming,
`Local`-by-default, and no tenant vocabulary crossing the boundary.

### U-9 — request authorization hook (`rift-http-proxy::admin_api`)

```rust
pub struct AuthzRequest<'a> {
    pub credential: Option<&'a str>,   // Authorization header value, verbatim
    pub action: &'static str,          // e.g. "imposter.write" — stable strings, not an enum
    pub port: Option<u16>,
    pub space: Option<&'a str>,
    /// Embedder-defined scope selector, verbatim from the request. Upstream
    /// neither parses nor interprets it; it exists because an authorizer often
    /// cannot derive the target from `port` alone.
    pub scope: Option<&'a str>,
    /// Route path parameters already parsed by the router, e.g.
    /// `[("id", "payments-team")]` for `/admin/tenants/:id/...`.
    pub params: &'a [(&'a str, &'a str)],
}

pub enum AuthzDecision {
    Allow { principal: Option<String> },
    Deny { reason: &'static str },
}

pub trait AdminAuthorizer: Send + Sync {
    fn authorize(&self, req: AuthzRequest<'_>) -> AuthzDecision;
}

// ServerBuilder::admin_authorizer(Arc<dyn AdminAuthorizer>)
```

The default implementation reproduces today's single-api-key comparison
**decision for decision**: `Allow { principal: None }` or `401`. An embedder that
installs nothing sees the same allow/deny on every route, which is the condition
for this landing upstream at all.

**Ordering, which is not a detail.** Today's gate runs in the connection service
*before* the router, so an unauthenticated request gets `401` whatever the path,
including paths that do not exist. Consulting the hook after route parse would
make an unknown path answer `404` before authentication — an unauthenticated
route-existence oracle, and exactly the kind of leak §8.4 exists to close. So the
contract is: **authenticate first, unconditionally; then parse the route; then
consult the hook for the authorization decision.** The hook receives the parsed
route class because that is what makes it useful, not because authentication may
wait for it. `AuthzDecision::Deny` on an unauthenticated request still renders
`401`, chosen by `reason`; `404` is never reachable before authentication.

`action` is a **stable string rather than an enum** deliberately: an enum would
force every embedder's action set to be upstream's, and upstream has no business
knowing that enterprise has a `TenantManage`. Strings let the hook be extended
without an upstream release.

**Why `scope` and `params` exist — the hook is useless without them.** An earlier
draft carried only `{credential, action, port, space}`, and two decisions the role
matrix requires are unmakeable from that:

- **Creates.** `POST /imposters` from a multi-tenant principal has no port yet, so
  a port→tenant map yields nothing. The only thing naming the owning tenant is the
  request's own selector — which the hook could not see.
- **Tenant management.** `/admin/tenants/:id/principals` has no port and no space.
  From `{credential, action}` alone, a `TenantAdmin` of A is indistinguishable
  from one acting on B, so the hook would authorize the *action* while being blind
  to the *object*.

`scope` carries the enterprise `X-Rift-Tenant` value and `params` carries the
already-parsed route parameters. Both stay tenant-vocabulary-free upstream: they
are an opaque string and a `&[(&str, &str)]`, and upstream never interprets either.

*Generic justification.* Embedders fronting Rift with their own identity system
currently reverse-proxy it and re-parse routes to recover enough context to make
an authorization decision. The hook hands them the already-parsed route class and
its parameters, which is precisely the thing that is annoying to reconstruct.

*Response mapping.* `Deny` → `403` with the standard error envelope; `401` stays
reserved for a missing or malformed credential, which the hook signals through
`reason`. Keeping those distinct matters for §8's enumeration analysis.

### U-10 — principal on change events (`imposter::reconcile`)

Attribution reaches listeners as `principal: Option<String>`, populated from
`AuthzDecision::Allow`.

**Not as a field on `ImposterEvent`.** That enum is
`Created(u16)` / `Replaced(u16)` / `StubsChanged(u16)` / `Deleted(u16)` /
`AllDeleted` / `EnabledChanged { .. }` (`imposter/reconcile.rs`), it is not
`#[non_exhaustive]`, and adding a field to every variant breaks every downstream
`match` — an unacceptable shape for a seam whose whole premise is that installing
nothing changes nothing. Instead the **listener signature** carries it:

```rust
pub struct EventContext { pub principal: Option<String> }

pub trait ImposterEventListener: Send + Sync {
    fn on_event(&self, event: &ImposterEvent, ctx: &EventContext);
}
```

The enum is untouched, existing matches keep compiling, and a listener that does
not care ignores the second argument. (`EnabledChanged` already landed at v0.16.0
as upstream #817, so there is nothing left to compose with — the earlier
"composes with #15" framing was future tense against a shipped variant.)

**One gap this cannot close:** `AllDeleted` carries no port, therefore no tenant.
A fleet-wide delete is a `ClusterAdmin` action and its audit record (§9) has
`tenant: null` and `resource: "*"` — correct, but worth stating so the null is
not read as a bug.

*Generic justification.* Audit logging is already a listed motivation of the
event seam; today an event says *what* changed but not *who* changed it, which
makes it unusable for the purpose it was partly built for.

## 7. Explicit non-goals

Stated here so the design is not oversold. Each of these is a real limit, not a
"later".

- **Data-plane request authorization.** `/__rift/:port/...` serves test traffic
  to unmodified systems under test; requiring a credential there would break
  every client. **Tenancy isolates configuration ownership and administrative
  access — it does not isolate the data plane.** Anyone on the network who can
  reach an imposter's port can send it traffic, regardless of tenant. If that is
  unacceptable for a deployment, the answer is network policy, not this RFC.
- **Per-tenant TLS identities on imposter ports.**
- **Noisy-neighbour isolation.** Quotas bound object counts, not CPU or memory.
  One tenant's pathological regex or `inject` script can still degrade a shared
  node.
- **Cross-cluster tenancy federation.**
- **OIDC and mTLS principal sources** — modelled in `AuthSource` from day one so
  they are not a schema break, but v1 ships API keys only.

## 8. Threat model

### 8.1 Confused deputy via `X-Rift-Tenant`

The sharpest edge in this design. A multi-tenant principal names its tenant in a
header, and a header is attacker-controlled.

The mitigation is that the header **selects among the principal's existing
bindings; it never grants one**. Evaluation order is: authenticate the principal,
resolve its binding set, then intersect with the requested tenant. An unbound
tenant answers `404` (§8.4). The header can therefore only ever narrow the
principal's authority, never widen it.

Corollary, stated because it is the failure mode: the header must be read
**after** authentication, never before. An implementation that resolved the
tenant first and then authenticated within it would have the deputy problem in
full.

**Creates are the exception to "only ever narrows", and need saying.** For a new
imposter there is no prior owner to intersect with, so the header does not select
among bindings — it decides which tenant *acquires* the resource. That is still
not a widening of authority (the principal must hold `ImposterWrite` in the named
tenant, and `404` if it is unbound there), but it is a distinct decision:
authority is checked against the named tenant, and ownership is then recorded as
that tenant. An implementation that checked authority against the principal's
*default* binding and recorded ownership from the *header* would let an Editor of
A create resources owned by B. Check and record must read the same value.

### 8.2 Key storage

argon2id, per-key salt, hash only. A key is returned once at creation (§5) and is
never recoverable from the control plane. The blast radius of a leaked control-plane
snapshot is therefore an offline attack against argon2id, not a set of usable
credentials.

Keys carry a fingerprint in their `PrincipalId` (`key:<fingerprint>`) so a key can
be identified in audit output without storing anything that can authenticate.

### 8.3 Replay

Node-to-node traffic is already HMAC'd on the cluster port (RFC-001 §11.2).
Client-facing admin traffic is bearer-over-TLS, which is replayable by anyone who
already holds the token — i.e. replay is not a distinct threat from theft, and
the mitigation is TLS plus rotation rather than nonces. Stated rather than
silently assumed.

### 8.4 Enumeration via 403-vs-404

A cross-tenant probe for a resource that exists must not be distinguishable from
a probe for one that does not, or the API becomes an oracle for other tenants'
port numbers and imposter names.

**Rule: cross-tenant access answers `404`, not `403`.** `403` is reserved for
"you are authenticated, this resource is in a tenant you are bound to, and your
role is insufficient" — a case that leaks nothing the caller did not already
know. This costs a little diagnostic clarity for out-of-tenant callers, which is
the right trade.

### 8.5 Revocation latency

Covered by §3.1: bindings are Raft-committed, so a revocation is either
fleet-visible or not applied. There is no per-node cache of authorization data
with an independent TTL, and there must not be one — that would reintroduce
exactly the window consensus is being paid for.

## 9. Audit

**One stream, two sources, one format.** Every applied `ControlOp` (which already
carries `op_id` and, with this RFC, `principal` — #14) and every U-10 event emit:

```jsonc
{ "ts": …, "principal": …, "tenant": …, "action": …,
  "resource": …, "op_id": …, "revision": …, "outcome": … }
```

- Storage: redb table `audit`, journal-style.
- Retention: `--cluster-audit-retention`, default **30 d**.
- Read: `GET /admin/audit?since=` — FleetAdmin sees the fleet, TenantAdmin sees
  its own tenant.

The #14 intent log **is** the write-path audit record; this is a projection of
it, not a second source of truth. That matters: an audit log that can disagree
with the thing it audits is worse than none.

**Reads are not audited in v1.** A stated non-goal, on log volume: the data plane
alone would dwarf every administrative event, and an audit log nobody can afford
to keep is an audit log nobody keeps. Reads-that-mutate (`ScenarioReset`,
`SavedRequestsClear`, `FlowStateClear`) are writes by this definition and *are*
audited.

## 10. Phasing

Tenancy lands as **Phase T**, parallel to RFC-001 Phases 2–3 and blocking neither.
It depends only on Phase 1's control plane (#14), which is merged.

| Slice | Contents | Exit criteria |
|---|---|---|
| **T1 — model + storage** | `ControlOp` variants, state-machine tables, `TenantId` on the config record, migration to `default` | A pre-tenancy state dir opens, every imposter reads back under `default`, legacy key still administers it |
| **T2 — U-9 + enforcement** | Upstream U-9 lands; enterprise authorizer; all four enforcement points of §4.3 | Every action in §4.1 is denied to a principal without the role, at every point; `/__rift/` provably unaffected |
| **T3 — admin surface** | §5 routes, argon2id key issuance, `whoami` | Key returned once and never again; cross-tenant probe answers 404 (§8.4) |
| **T4 — quotas + audit** | Leader-side quota validation; U-10; audit table, retention, `GET /admin/audit` | Quota refusal is a committed decision, identical on every node; every write in a session appears exactly once in the audit stream |
| **v2 (separate RFC)** | OIDC and mTLS `AuthSource` variants | — |

Upstream U-9 and U-10 are filed **after** this RFC is reviewed, in generic
wording, following the #311–#318 precedent.

## 11. Open questions

Listed rather than papered over:

1. **Quota accounting under a partition.** Quotas are validated at the leader, so
   a minority-side write is parked (RFC-001 §7.6) and validated on replay —
   against the quota as it stands *then*. A tenant at its limit may therefore see
   a parked write refused on replay, having been given a `503` + op-id rather
   than a refusal at submit time. This is correct but surprising, and the client
   contract for it needs settling in T4.
2. **`journal_retention` as a quota.** It is the only `Quotas` field that is a
   policy rather than a count, and it is enforced somewhere different (the
   shards). It may belong on the tenant record outside `Quotas`.
3. **Role granularity for `VerifyRun`.** Placed under Editor because verification
   can execute stub scripts; an argument exists for Operator. Decide with a user,
   not in the abstract.

**All three are settled**, in `docs/architecture/08-tenancy-security.md`, "RFC-002
§11 open questions, settled here" — read that section, not this list, for the
current answer. Building T2–T4 also forced two questions this RFC did not think
to ask, settled in the same place:

4. **What a stale minority node does with an authorization read** — it serves
   from its own applied state rather than refusing, and what is pinned instead is
   that the first request after a heal is refused (`c25_key_revocation_survives_a_partition`).
5. **Tenanted resource state was stored but not served — resolved (issue #182).**
   The read/sync paths now bind the union of every tenant's resources into the
   local engine, sound because ports are fleet-unique across tenants (§3.2); a
   single ownership gate at the authorization choke point refuses a request
   whose addressed port belongs to a different tenant, with the *same*
   §8.4 indistinguishable 404 the old blanket guard answered. §7's "no
   data-plane change" still holds — the gate governs administration of a port,
   not traffic through it.

---

## Appendix A — what this RFC does not change

- The OSS config schema. `tenant` is enterprise-side, injected and stripped at
  the API boundary (§3.2).
- The data plane, in any respect (§7).
- `--api-key`, for one release (§3.4).
- The cluster port's HMAC credential, which is a separate mechanism for a
  separate purpose (RFC-001 §11.2).
