# RFC-004 — Spec-driven Mocking: OpenAPI Import and Contract Validation (v1)

| | |
|---|---|
| **Status** | v1 — design draft for review |
| **Tracking issue** | [achird-labs/rift-cluster#148](https://github.com/achird-labs/rift-cluster/issues/148) (milestone M4) |
| **Canonical location** | `rift-cluster:docs/rfc/RFC-004-spec-driven-mocking.md` |
| **Depends on** | **ADR-001** (Raft control plane) — spec records live in its state machine; **RFC-002** (#17) — tenancy scoping and the closed Action set this RFC extends; **#20 / U-12** (`ImposterSource` SPI, Ch. 13) — for the `openapi+…:` source kinds only, and only that part blocks on it |
| **Ground truth** | verified at `rift-cluster@5b98fef`, `vendor/rift@v0.16.0-4-g97757f0` |
| **Author** | Mohsen Zainalpour |
| **Date** | 2026-07-26 |

---

## 1. Summary

This RFC closes the OpenAPI pillar of WireMock Cloud parity: import an
OpenAPI 3.0 spec and get a working, replicated mock; re-import and get a
drift report instead of a silent clobber; and validate live traffic against
the spec in four modes (`off` / `soft` / `hard` / `hard-spec-compliant`).

Four design commitments, each argued below:

1. **The importer is a pure compiler.** Spec in, OSS-native imposter JSON
   out — stubs, `matches` predicates, `is` responses, nothing the engine
   does not already execute. The compiled imposter enters through the same
   clustered admin write path as a hand-written one and inherits
   park/replay, revision preconditions, and RBAC for free.
2. **The spec document is a control-plane object**, content-addressed and
   committed to the Raft log with a hard size cap — every node must
   validate traffic against *identical bytes*, and the
   one-fetch-then-replicate rule of #20 already says how differing-bytes
   hazards are closed.
3. **Observe mode ships with zero engine change** — an in-process async
   validator over the engine's admin event bus. It validates **requests
   only**, because the verified event surface carries no responses (§2),
   and this RFC says so instead of pretending otherwise.
4. **Enforce mode needs one new upstream seam, U-13** — a request/response
   inspector in the imposter hot path, `None`-by-default, specified in §6
   to the U-9/U-10 standard.

## 2. Why — the verified gap

Checked at `5b98fef` / `v0.16.0-4-g97757f0`, not assumed:

- **Nothing in the system reads a spec.** No crate under `crates/` or
  `vendor/rift/crates/` references OpenAPI or Swagger in any capacity
  (workspace grep). The only spec-adjacent field upstream has is
  documentation metadata: `ImposterConfig.service_info`
  (`vendor/rift/crates/rift-mock-core/src/imposter/types.rs:937`), stored
  as-is and never interpreted.
- **The engine already executes everything a compiled spec needs.**
  `PredicateOperation` includes `Matches` (regex) alongside `Equals` et al.
  (`vendor/rift/crates/rift-types/src/predicate.rs:22–34`), and `Stub`
  carries `route_pattern` (`/users/:id`, `types.rs:222`) for path-parameter
  extraction. Response synthesis compiles to plain `is` responses. No engine
  change is needed for import.
- **The journal and SSE stream carry requests, never responses.**
  `RecordedRequest` is `{request_from, method, path, query, headers, body,
  mode, timestamp}` — no response field
  (`vendor/rift/crates/rift-mock-core/src/imposter/types.rs:84–100`). The SSE
  `Request` event carries a `Box<RecordedRequest>` byte-identical to the
  `savedRequests` projection, and fires only when `recordRequests: true`
  (`vendor/rift/crates/rift-mock-core/src/imposter/events.rs:63–78`).
  `record_matches` — the flag that would store request/response pairs in
  Mountebank — is parsed and defaulted (`types.rs:908`, `:985`) and **used
  nowhere else in the workspace** (grep over `vendor/rift/crates`, two hits,
  both in `types.rs`). Response bodies are not observable anywhere outside
  the request path itself. This single fact shapes §3.6.
- **No existing seam can reject an in-flight request.**
  `NoMatchInterceptor` fires only on a genuine no-match and its verdict is
  `Proceed | RetryMatch`
  (`vendor/rift/crates/rift-mock-core/src/extensions/no_match.rs:19–25`) — a
  matched request never reaches it. `ResponseDecorator` is headers-only by
  contract ("the body is untouched",
  `vendor/rift/crates/rift-mock-core/src/extensions/decorate.rs:62–74`) and
  cannot change a status. The injection gate
  (`config_uses_script_surface`) is admin-time. Hard mode therefore cannot
  be built from existing seams; hence U-13.
- **The write path this RFC rides is already there.** The clustered admin
  front terminates config-mutating routes into `ControlOp`s and proxies the
  rest (`crates/rift-cluster-server/src/admin_front.rs:1–48`); `ControlOp` is the
  closed op set with deterministic pre-apply `validate`
  (`crates/rift-cluster/src/control.rs:89–147`, `:225`); the state machine
  stores config JSON per `(tenant, port)` in `sm_configs`
  (`crates/rift-cluster/src/raft/store.rs:80`, `StoredImposter` at `:122`).
  There is **no per-record size guard in the store**; the effective cap on a
  terminated write is the front's `MAX_BODY_BYTES = 16 MiB`
  (`admin_front.rs:82`). Spec blobs get an explicit, smaller cap (§4.2).

The gap, in one sentence: teams with an OpenAPI contract must hand-translate
it into stubs, and nothing ever tells them their mock has drifted from the
contract or that their traffic violates it.

### What WireMock Cloud does (the parity bar)

Per docs.wiremock.io as of 2026-07-26: import OpenAPI 3.x / Swagger →
working mock; every stub validated against the schema; spec-drift
detection; Git-integrated spec sync; live validation of both requests and
responses in four modes — `off`, `soft` (log warnings to the request log;
their default), `hard` (warnings + error responses), `hard-spec-compliant`
(error body and Content-Type from the spec, Accept-negotiated) — with
violations inline in the request log. This RFC reaches that bar with two
stated deviations: the default mode is `off`, not `soft` (§3.6 — soft mode
requires `recordRequests`, and invariant 3 forbids taxing the hot path by
default), and v1 parses OpenAPI 3.0.x only (§7).

## 3. Design

### 3.1 The importer is a pure compiler

New crate `crates/rift-cluster-spec`. Input: an OpenAPI 3.0 document (JSON or
YAML). Output: **canonical imposter JSON** (`serde_json::Value`) plus a
typed operation index used for validation and diffing.

It deliberately has **no dependency on `rift-cluster-base` or anything vendored** —
not even `rift-types`. The alternative (emitting a typed `ImposterConfig`)
was checked and rejected: the facade rule says cluster crates reach the
core only through `rift-cluster-base` (`crates/rift-cluster-base/src/lib.rs:1–9`), so the "typed
output" option really means a `rift-cluster-base` dependency, which drags the whole
engine into what should be a text-to-text function. Instead the compiler
emits the same JSON a client would `PUT`, and `rift-cluster-server` parses it
through the **same admission gate as any other write** — the
`ImposterConfig` deserialize plus `control::validate` that every terminated
write already passes (`admin_front.rs`, `control.rs:225–252`). Type safety
is enforced where it is load-bearing (admission), and the compiler stays a
pure, golden-file-testable function of `(spec bytes, options)`.

```rust
// crates/rift-cluster-spec — public surface (sketch)
pub struct SpecDigest([u8; 32]);            // sha256 of the canonical spec bytes
pub struct OperationId(String);             // operationId, or synthesized METHOD+path

pub struct CompiledSpec {
    pub imposter: serde_json::Value,        // canonical ImposterConfig JSON
    pub operations: Vec<CompiledOperation>, // the diff/validation index
    pub digest: SpecDigest,
}

pub struct CompiledOperation {
    pub id: OperationId,
    pub method: String,
    pub path_template: String,              // "/users/{id}"
    pub stub_ids: Vec<String>,              // deterministic, see §3.2
}

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("unsupported spec version {found}: v1 compiles OpenAPI 3.0.x only")]
    UnsupportedVersion { found: String },
    #[error("spec exceeds {max} bytes")]
    TooLarge { max: usize },
    #[error("external $ref {reference:?} refused: remote references do not replicate")]
    ExternalRef { reference: String },
    #[error("parse: {0}")]
    Parse(String),
}
```

Parsing: the `openapiv3` crate for 3.0.x. Swagger 2.0 and OpenAPI 3.1 are
refused with `UnsupportedVersion` in v1 (§7, open question 1). External
`$ref`s (network or filesystem) are refused, not resolved: resolving at
compile time is a second fetch outside the one-fetch rule, and resolving
differently on re-import would make drift reports lie. A spec that needs
them must be bundled before import.

### 3.2 Compilation rules

Everything compiles to constructs verified present in the engine (§2).

**Path templates.** `/users/{id}` compiles to a `matches` predicate — regex
anchored, literal segments **regex-escaped**, template segments `[^/]+`:

```json
{ "matches": { "path": "^/users/[^/]+$" }, "caseSensitive": true }
```

plus `routePattern: "/users/:id"` on the stub, so `request.pathParams.id`
works in templates and scripts for free (`types.rs:217–222`). Method
compiles to `{ "equals": { "method": "GET" } }`. Required query parameters
and required headers compile to `exists` predicates; nothing optional
compiles to a predicate at all — the mock must accept what the spec permits,
not only what it illustrates.

**Ordering.** Mountebank semantics are first-match-wins in stub order, so
the compiler emits operations sorted **most-literal-first** (more literal
path segments, then longer templates, then lexical): `/users/me` always
precedes `/users/{id}`, and the deterministic sort keeps re-compiles
diffable (§3.5).

**Response synthesis.** For each operation, one stub per declared status
code, in spec order, with `default` trailing them — the unconditional one
aside, which sorts last for the reason given below:

1. **Spec examples first** — `example` / `examples` on the response media
   type, verbatim.
2. **Otherwise deterministic schema-driven generation**, seeded with
   `sha256(digest ‖ operation_id ‖ status)`: same spec bytes → same
   generated body on every node and every re-import. Generation is bounded:
   arrays render their `minItems` (default 1), recursion depth caps at 8
   with `null` at the floor, `additionalProperties` renders nothing.

The response is a plain `is` response: `statusCode`, `Content-Type` from
the media type key, body.

**Which response answers unconditionally.** The **first declared 2xx**, falling
back to the first declared response when the operation declares none. A `2XX`
range counts as a 2xx for this rule, taken in the position it is declared —
OpenAPI gives an explicit code precedence over a range when *serving* that code,
but this rule is about which response a bare request gets, not about resolution.
Every
other status — `default` included — carries a discriminating predicate
`{ "equals": { "headers": { "X-Rift-Spec-Status": "404" } } }`, so a test can
opt into any declared response without editing the imposter. The unconditional
stub is emitted **last**, because its predicates are a strict subset of the
others' and first-match-wins would otherwise let it shadow them.

> **Deviation history (issue #314).** This paragraph originally read "the first
> (default) stub answers unconditionally". That is implementable and was
> implemented, but `default` names no status of its own, so it compiles to
> `statusCode: 200` carrying whatever body it declares. For the common shape —
> a spec whose `default` is an error envelope — the out-of-the-box mock
> answered a bare request with **200 and an error body**: a success status
> wrapping an error, which is the shape client code fails to notice. Preferring
> a declared success fixes that while keeping every status reachable.
>
> Two alternatives were rejected. *Lowest numeric status wins* discards spec
> order, which this section otherwise treats as the author's only ordering
> signal. *Synthesising an error-shaped status for `default`* (say `502`)
> invents a status the spec never declared, which is a larger lie than 200 —
> and §3.6's inspector already owns that classification. An operation declaring
> only `default` therefore still compiles to 200, because its author declared
> nothing better.

**Deterministic stub ids.** Every generated stub carries
`id: "spec:<operation_id>:<status>"`. The engine's stub ids are exactly the
by-id addressing the cluster's `StubEditScript` uses
(`control.rs:158–176`), and the `spec:` prefix is what separates generated
stubs from hand-added ones during drift diffing (§3.5). Duplicate-id
admission checks already exist (`control.rs:239–246`); the compiler's id
scheme is collision-free by construction within one spec.

**Admin-time stub validation** (the "every stub validated" half of parity):
`rift-cluster-spec` also exposes `validate_stub_response(op, status, body) ->
Vec<Violation>` — run at import over the compiler's own output (a
self-check that fails compilation rather than deploying an inconsistency),
and at edit time: a config-mutating write to an imposter with a bound spec
(§4.1) gets its static `is` bodies validated, violations returned in a
`Rift-Spec-Warnings` header — warn, never refuse, because a
deliberately-divergent stub is a legitimate test fixture. Templated and
scripted responses cannot be checked statically and are skipped; they are
exactly what runtime validation (§3.6) exists for.

### 3.3 Deploy path — nothing new to trust

`POST /specs/:id/deploy` (§5) compiles and submits an ordinary
`PutImposter` through the same code path as a client `PUT /imposters`
(`admin_front.rs` termination → `ControlRequest` → Raft). Everything is
inherited rather than built: park/replay under a lost leader and op-id
dedup on retries (Ch. 4); `If-Match` revision preconditions against a
concurrently-edited imposter (`control.rs:72–79`); RBAC — deploying is an
`ImposterWrite` on the target port under RFC-002, with spec-side actions
separate (§4.3); and the write barrier, so a 2xx means every ready node
serves the compiled mock.

The stored imposter record additionally carries provenance
`{spec_id, digest}` (§4.1) — the same shape #20 stamps for sources, so
"where did this imposter come from" has one answer format regardless of
whether it came from a spec, a Git repo, or a hand `PUT`.

### 3.4 A spec is a source kind

Chapter 13's `ImposterSource` SPI (#20 / U-12) is scheme-dispatched;
this RFC adds two schemes:

- `openapi+https://…` — raw URL, ETag-aware, like the upstream `https:`
  built-in but compiling instead of parsing imposter JSON;
- `openapi+git://repo#ref:path` — riding the cluster `git+https:`
  provider; Git-integrated spec sync is then #20's poll/pull machinery, not
  new machinery.

The provider fetches the spec bytes, computes the digest, and — digest
changed — runs the compiler and returns the standard
`SourcePullResult {configs, version, digest}`, which enters the log as a
normal control-plane write. **One fetch, then replication of identical
bytes**: both the spec blob (§4.2) and the compiled configs ride the same
committed op, so no node ever compiles from bytes another node didn't see.
Drift policy (`overwrite | skip | fail`) is #20's per-source policy applied
unchanged; §3.5 defines what "drift" means for a spec-owned imposter.

**Honest dependency statement:** U-12 does not exist in the vendored pin —
no `ImposterSource` symbol anywhere under `vendor/rift/crates` at
`v0.16.0-4-g97757f0`; Chapter 11 lists it as queued, not merged. This
section is specified against #20's design and lands **after** it. Nothing
else in this RFC blocks on it: `PUT /specs/:id` with an inline document
(§5) delivers the full import/drift/validation set with no source
machinery at all.

### 3.5 Drift and diff on re-import

Re-import (a new `PUT /specs/:id`, or a source pull with a changed digest)
never silently overwrites. The flow:

1. Compile the new spec (pure, node-independent — same bytes, same output).
2. Diff against the **deployed** imposter config as stored in `sm_configs`
   — the possibly-hand-edited truth, not the previous compiler output.
3. Classify per operation, keyed on the `spec:` stub-id scheme:
   - **added** — operation in the new spec with no `spec:<op>:*` stubs
     deployed;
   - **removed** — deployed `spec:` stubs whose operation left the spec;
   - **changed** — operation present in both, compiled stubs differ from
     deployed stubs (byte-compare of canonical JSON);
   - **hand-edited** — a `spec:` stub whose deployed form differs from what
     the *previous* digest compiled to (detectable because compilation is
     deterministic and the previous digest is stored, §4.1);
   - **hand-added** — stubs without the `spec:` prefix: never touched by
     any policy, always preserved and re-ordered after generated stubs.
4. Apply the policy: `overwrite` replaces the `spec:` stub set (hand-added
   stubs preserved); `skip` deploys nothing and records the report;
   `fail` refuses the pull/import with the report as the error body.
   The report is returned synchronously on manual import and stored
   per-spec (§4.1) for `GET /specs/:id/drift` either way.

A manual edit to a spec-owned imposter flips the record's `drifted` flag at
apply time — the same observable #20 defines for source-owned imposters, so
dashboards need one concept, not two.

### 3.6 Traffic validation

Modes, per imposter: `off` (default) · `soft` · `hard` ·
`hard-spec-compliant`.

**Default `off`, deviating from WireMock's `soft` — deliberately.** Soft
mode consumes recorded-request events, which exist only when
`recordRequests: true` (`events.rs:63–67`); silently forcing recording on
every spec-deployed imposter to honor a `soft` default would tax the hot
path (invariant 3) and surprise operators with journal growth. Enabling
validation is a visible, per-imposter decision.

#### Soft (observe) — zero engine change, requests only

An EE-side validator task, composed in `rift-cluster-server`, subscribes
**in-process** to the engine's `AdminEventBus`
(`manager.event_bus()`, `events.rs:117–128`) — the same bus the SSE
handler consumes (`rift-http-proxy/src/admin_api/handlers/events.rs`), with
no HTTP hop. For each `Request` event on a port with a bound spec and mode
`soft`, it matches the request to an operation (path template + method) and
validates parameters and body against the operation's schema
(`jsonschema` crate, validators compiled once per digest and cached).

Violations are appended to a per-node redb table (§4.4) keyed
`(port, journal_index)` — `journal_index` is the stable per-port index the
event already carries (`events.rs:66–68`), which is what lets a violation
be joined back to its `savedRequests` entry: the "inline in the request
log" parity, delivered as a join key plus a merged read
(`GET /imposters/:port/validationFailures`, §5) rather than by mutating
upstream's response shape.

Two limits, stated not hidden:

- **Responses are not validated in pure-soft mode.** The event surface
  carries no responses (§2). No amount of EE-side code changes that without
  an engine seam. Partial mitigation exists at admin time (§3.2, static
  stub validation); full runtime response validation arrives with U-13 —
  and once U-13 is installed, soft mode upgrades transparently to
  full-fidelity observation (inspector logs, never rejects).
- **Soft mode is lossy under extreme load, loudly.** The bus is a bounded
  broadcast; a lagging consumer gets `Lagged(n)`
  (`handlers/events.rs:100–105`). The validator records a
  `violations_missed` counter instead of blocking the request path —
  observation must never become backpressure.

#### Hard / hard-spec-compliant (enforce) — via U-13

In-path enforcement hooks the U-13 inspector (§6) at two points in
`handle_request_inner`: request-side after body collection
(`handler.rs:456`) and before matching (`handler.rs:589`); response-side in
the single serve-loop funnel where the decorator already sits
(`handle_imposter_request_decorated`, `handler.rs:304–323`).

- `hard`: a request violation answers `400` (a response violation `502`)
  with the standard typed error envelope (`ErrorKind` /
  `error_response_typed`, re-exported at `crates/rift-cluster-base/src/lib.rs:90`),
  the violation list in the body, and header
  `x-rift-spec-violation: request|response`. Violations are also recorded
  exactly as in soft mode.
- `hard-spec-compliant`: the error body and `Content-Type` come from the
  **spec's own declared error responses** — pick the spec's `400` (request
  side) / `502`-or-`default` (response side) response, synthesize its body
  by the §3.2 rules, negotiate the media type against the request's
  `Accept` header, fall back to `hard` behavior when the spec declares no
  usable error response. This is for consumers whose client SDKs choke on
  anything off-spec — the error itself stays on-contract.

Enforcement decisions are per-request CPU work against pre-compiled
validators — no I/O, no cluster round-trip; the spec and policy were
already replicated to this node by the control plane.

**Failure semantics of the validator itself** (the swallow taxonomy applies):
a body that fails to parse *as its declared content type* is a **violation**
— that is the contract check working, not an error path. An internal
validator defect (a poisoned cache, a panic caught at the hook boundary) in
hard mode must not silently fail open per request: it logs at error level,
increments `spec_validator_errors_total`, and **proceeds without
validating** — this is a QA gate on mock traffic, not a security gate, and
serving the mock beats 500ing a test suite; but the failure is loud,
counted, and visible in `/readyz` detail if persistent.

## 4. Data model

### 4.1 On consensus (Raft state machine) — small, must-agree

New `ControlOp` variants (joining the closed set at `control.rs:89`, tags
frozen by the same stability test at `control.rs:460`):

```text
SpecPut       { tenant, id, meta, bytes }     # meta: {format, digest, source: inline|source-id}
SpecDelete    { tenant, id }
SpecBind      { tenant, id, port }            # provenance + drift baseline for the port
SpecUnbind    { tenant, port }
ValidationPolicySet { tenant, port, mode }    # off | soft | hard | hard-spec-compliant
```

New tables, following the `sm_*` conventions of `store.rs:76–91`:

- `sm_specs`: `(tenant, spec_id) → {meta, digest, drift_report?, revision}`
- `sm_spec_blobs`: `digest → bytes` — content-addressed, written once per
  digest, removed when the last referencing spec record goes;
- `sm_validation`: `(tenant, port) → {mode, spec_id, revision}`.

**Why the blob is on consensus and not per-node or re-fetched.** Every node
enforces (or observes) against the spec, so every node needs byte-identical
spec content; fetching per node is exactly the differing-bytes hazard #20's
one-fetch rule exists to close, and per-node redb is for data that may
legitimately differ per node (journals, violations) — a spec may not. The
precedent is already set: config bodies ride in log entries
(`03-control-plane.md`, "Config bodies ride in log entries (small JSON)"),
and the snapshot machinery compacts at 5k entries / 64 MiB. What the store
does **not** have is a size guard (verified, §2) — the 16 MiB
`MAX_BODY_BYTES` at the admin front is the only cap today. Rather than
inherit that accident, `SpecPut` gets an explicit deterministic check in
`control::validate`: **specs over 4 MiB are refused pre-commit**. 4 MiB
holds every real-world spec we could name while keeping a spec blob the
same order of magnitude as a large imposter config; raising it later is a
one-line change, shrinking it later is a migration. `SpecPut` with an
unchanged digest is refused as a no-op at the accepting node (mirroring
#20's digest-changed gate) so retries and unchanged Git polls cost zero log
growth.

The imposter record (`StoredImposter`, `store.rs:122`) gains cluster-side
fields `provenance: Option<{spec_id, digest}>` and `drifted: bool` — stored
on the control-plane record only, invisible to the core config schema,
exactly like RFC-002's `tenant` (§3.2 there; same open-core rule,
`11-upstream-boundary.md`).

Tenancy: every op carries `TenantId` like the existing set; until RFC-002
lands, `validate` pins it to `default` exactly as `require_default_tenant`
does today (`control.rs:283–292`). Specs are tenant-owned; `SpecBind` may
only target the tenant's own imposters (the front door set this precedent,
Ch. 13).

### 4.2 Per-node (redb, non-consensus) — large, may-differ

- `spec_violations`: `(port, journal_index) → {ts, mode, phase, operation,
  violations[]}` — one row per violating exchange, on the node that served
  it. Read via the cluster-merged read pattern of Ch. 7 (violations join a
  journal that is itself per-node and merged at read time). Retention:
  `--cluster-spec-violation-retention`, default 7 d, plus a per-port row
  cap (default 10 000, mirroring `MAX_RECORDED_REQUESTS`,
  `journal.rs:19`).
- Compiled-validator cache: in-memory only, keyed by digest; rebuilt from
  `sm_spec_blobs` on restart. Nothing durable.

### 4.3 RBAC additions (RFC-002 §4.1's closed set)

New actions — added to the closed enum, per its own rule that adding a
route means adding an action:

> **As shipped (#278).** `SpecRead` / `SpecWrite` / `SpecDelete` landed with
> **S2**, not S6 as §9 originally planned. That plan predates M2 shipping:
> the front's `action_for` is matched wildcard-free over the closed `Action`
> enum, so a terminated `/specs` route cannot compile without its action.
> The role mapping is exactly the table below (Viewer+ / Editor+ / Editor+),
> `deploy` additionally requires `ImposterWrite`, and only
> `ValidationPolicyWrite` remains for the validation slices — it moves to S4
> with its route.

| Action | Routes | Granted to |
|---|---|---|
| `SpecRead` | `GET /specs`, `GET /specs/:id`, `GET /specs/:id/drift` | Viewer + |
| `SpecWrite` | `PUT /specs/:id`, `POST /specs/:id/deploy`, bind/unbind | Editor + |
| `SpecDelete` | `DELETE /specs/:id` | Editor + |
| `ValidationPolicyWrite` | `PUT /imposters/:port/validationPolicy` | Editor + |
| (existing) `SavedRequestsRead` | `GET /imposters/:port/validationFailures` | Viewer + |

`ValidationPolicyWrite` sits at Editor, not Operator: switching to `hard`
*redefines* what the imposter answers (rejections replace mock responses),
which is RFC-002's Operator/Editor line — disturb versus redefine — landing
on the redefine side. `deploy` additionally requires `ImposterWrite` on the
target port; holding `SpecWrite` alone must not be a back door into
imposter mutation.

## 5. Admin API surface

On the admin port, terminated by the clustered front like
`/front-door/routes` (`admin_front.rs:348–349` sets the precedent for an
EE-only route with no upstream counterpart):

```text
PUT    /specs/:id                      SpecWrite   body: the spec (JSON/YAML) — import/re-import
GET    /specs                          SpecRead    list: id, digest, bound ports, drifted
GET    /specs/:id                      SpecRead    the stored document + meta
DELETE /specs/:id                      SpecDelete  refuses while bound (409) unless ?force
POST   /specs/:id/compile              SpecRead    dry run: compiled imposter JSON + diff vs deployed; commits nothing
POST   /specs/:id/deploy               SpecWrite + ImposterWrite
                                                   body: {port, policy?: overwrite|skip|fail}
                                                   compile → SpecBind + PutImposter, one barrier
GET    /specs/:id/drift                SpecRead    last drift report
PUT    /imposters/:port/validationPolicy   ValidationPolicyWrite   body: {mode, specId}
GET    /imposters/:port/validationPolicy   SpecRead
GET    /imposters/:port/validationFailures SavedRequestsRead
                                                   merged read; ?since=<journal-index> cursor,
                                                   same cursor contract as savedRequests (#603)
```

Re-import responses carry the drift report; `deploy` and `PUT /specs/:id`
carry the standard `Rift-Cluster-Revision` / op-id headers because they are
ordinary terminated writes. Error shapes are the typed envelope
(`ErrorKind`), never a new shape.

## 6. Upstream seams needed

One new seam. Drafted to the RFC-002 §6 standard: generic naming, inert by
default, no spec/OpenAPI vocabulary crossing the boundary, independently
justifiable to an OSS maintainer. U-14 remains free.

### U-13 — exchange inspector (`rift-mock-core::extensions`)

```rust
/// What the request-side hook sees: the already-collected request, borrowed.
pub struct InspectRequest<'a> {
    pub port: u16,
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub headers: &'a HashMap<String, Vec<String>>,
    /// Body as the engine's lossless string form (text, or base64 when
    /// `mode` is `Binary`) — the same representation matching already uses.
    pub body: Option<&'a str>,
    pub mode: &'a ResponseMode,
}

/// What the response-side hook sees, after behaviors ran.
pub struct InspectResponse<'a> {
    pub status: u16,
    pub headers: &'a hyper::HeaderMap,
    pub body: &'a [u8],
}

pub enum InspectVerdict {
    /// Continue unchanged. The only verdict a default build ever produces.
    Proceed,
    /// Replace the exchange's outcome with this response.
    Reject {
        status: u16,
        content_type: String,
        body: bytes::Bytes,
    },
}

pub trait ExchangeInspector: Send + Sync {
    /// After body collection, before stub matching.
    fn inspect_request(&self, req: &InspectRequest<'_>) -> InspectVerdict;
    /// After the response is built, before it is written and decorated.
    fn inspect_response(
        &self,
        req: &InspectRequest<'_>,
        resp: &InspectResponse<'_>,
    ) -> InspectVerdict;
}

// Installed per imposter, mirroring FlowStoreProvider (#312):
pub trait ExchangeInspectorProvider: Send + Sync {
    fn provide(&self, config: &ImposterConfig) -> Option<Arc<dyn ExchangeInspector>>;
}
// ImposterManager::with_exchange_inspector_provider(Arc<dyn ExchangeInspectorProvider>)
```

**Hook points, named against verified code.** Request-side: in
`handle_request_inner` immediately after the lossless body string exists
(`handler.rs:456–507`) and before `find_matching_stub_with_client_bounded`
(`handler.rs:589`) — rejecting before matching means a rejected request
never advances cyclers, scenario FSMs, or match counters, which is the only
defensible semantics for "the request was off-contract". The request *is*
still journaled (it arrived; hiding it from `savedRequests` would falsify
the record). Response-side: in the serve-loop funnel
`handle_imposter_request_decorated` (`handler.rs:304–323`), before the
decorator — one funnel instead of a hook in each of the response branches
(is/proxy/inject/fault), and the decorator then stamps whatever headers the
embedder's inspector annotated, composing with #318 rather than duplicating
it.

**Synchronous, deliberately** — unlike `NoMatchInterceptor`'s boxed future
(`no_match.rs:53–59`). That hook parks an already-failed request to rescue
it over the network; this one runs on every request of an opted-in
imposter, and an async signature is an invitation to put I/O on the hot
path. Everything an inspector legitimately needs (compiled validators,
policy) is process-local by this RFC's own data model.

**Backward compatibility.** The field is `Option`, the provider default is
`None`, and no hook means not one added branch beyond an
`Option::is_none` check per phase — the same zero-cost-when-absent shape as
`no_match_interceptor` (`core/mod.rs:143`, `handler.rs:616`). An embedder
that installs nothing gets byte-identical behavior, the condition for
landing upstream at all.

*Generic justification (for the upstream PR, no EE vocabulary):* embedders
that need policy on live exchanges — request linting, contract validation,
compliance capture, chaos veto — currently have no in-path hook at all;
`NoMatchInterceptor` sees only failed matches and `ResponseDecorator` may
only add headers. One inspector seam with an inert default covers the
class. **It is also the missing primitive for upstream's own dormant
`record_matches` flag** (`types.rs:908` — parsed, never consulted): the
response-side hook sees exactly the request/response pair that Mountebank's
`matches` array records, so the seam gives upstream a path to finishing
that feature.

### U-12 — consumed, not defined here

The `openapi+…:` schemes of §3.4 are implementations of #20's
`ImposterSource`, adding no requirements to U-12 beyond what Chapter 13
already specifies. If U-12's final shape hands providers
`fetch() -> bytes + version`, the spec compiler slots behind it; the RFC-004
slices that need it are explicitly gated on it (§9).

## 7. Explicit non-goals

- **Runtime response validation without U-13.** Cannot be built from the
  verified event surface (§2); this RFC refuses to fake it with
  response-body sniffing hacks.
- **OpenAPI 3.1 and Swagger 2.0** in v1. 3.1's JSON-Schema-2020-12 alignment
  is a different validation dialect, not a parser flag; Swagger 2.0 import
  would be a conversion pass. Both are refused loudly with a version-naming
  error, neither silently mis-parsed. (Open question 1.)
- **Spec generation from traffic** (record → OpenAPI). The reverse compiler
  is a different product surface.
- **Editing deployed stubs "through" the spec.** The spec is a source; edits
  flow spec → imposter, never back. Git sync is pull-only.
- **Per-operation validation policy.** Mode is per-imposter in v1; the
  policy object's shape leaves room (`{mode}` → `{mode, overrides?}`)
  without a wire break.
- **Protocols beyond HTTP/HTTPS** — matching the control plane's own
  protocol gate (`control.rs:235–238`).

## 8. Threat model

Spec import is a parser on network-supplied input wired to consensus, and
hard mode is a traffic kill switch; both deserve scrutiny.

- **Malicious spec documents.** A spec can be attacker-influenced (a public
  Git repo, a compromised registry). Mitigations: the 4 MiB pre-commit cap
  (§4.1); external `$ref` refusal (§3.1) — no SSRF-by-reference, no
  resolve-time divergence; bounded generation (depth 8, bounded arrays) so
  a pathological recursive schema cannot OOM the compiler; compilation runs
  on the accepting node **before** anything is committed, so a
  parser-crashing document fails one request, not the apply loop of every
  replica (apply must not run fallible spec code — the log carries the
  already-compiled configs, §3.4).
- **Regex injection via path templates.** Literal path segments are
  regex-escaped at compile time (§3.2); a path of `/a(b/{id}` produces
  `^/a\(b/[^/]+$`, not a broken or widened pattern. Property-tested in the
  compiler crate.
- **Hard mode as denial of service.** Flipping `hard` on rejects
  off-contract traffic fleet-wide within one barrier. That is the feature —
  but it is why `ValidationPolicyWrite` is Editor-gated (§4.3), why the mode
  is a committed, audited control-plane write (who flipped it is a log
  query), and why rejections carry a distinctive header rather than
  impersonating engine errors.
- **Validation-policy consistency.** Mode lives on consensus (§4.1), so
  two nodes can never disagree about whether port 9090 enforces — a
  per-node policy would make rejection a load-balancer lottery, the class
  of bug RFC-002 §3.1 rejects for authz data.
- **Violation records leak request data.** Violations quote the offending
  request fragment. They live per-node beside the journal, gated by
  `SavedRequestsRead` — the same trust level as `savedRequests`, which
  already contains full bodies. No new exposure class; stated so the
  equivalence is a decision.
- **Fail-open, bounded and loud.** §3.6's internal-defect rule (proceed,
  log at error, count) is a deliberate availability-over-enforcement call
  for a QA gate on mock traffic — not precedent for security gates; the
  counter and readiness surfacing keep it from being silent.

## 9. Phasing

Slices sized ~1 PR each, house naming. S1–S4 have no upstream dependency;
S5 is the upstream PR; S8 gates on #20.

| Slice | Contents | Exit criteria |
|---|---|---|
| **S1** `feat(spec): rift-cluster-spec — OpenAPI 3.0 compiler to imposter JSON` | pure crate; path/method/param compilation, response synthesis, deterministic ids and seeds; golden-file + property tests (regex escaping, ordering, seed stability) | same spec bytes → byte-identical imposter JSON across runs; goldens cover petstore + a template-heavy spec |
| **S2** `feat(spec): spec records on the control plane + /specs surface` — **shipped (#278)** | `SpecPut/SpecDelete/SpecBind/SpecUnbind` + `sm_specs`/`sm_spec_blobs` + 4 MiB guard + digest no-op; `/specs` CRUD, `compile`, `deploy` terminated in the front; `SpecRead/Write/Delete` actions (see §4.3's as-shipped note); `Rift-Spec-Warnings` on edit-time violations | deploy → imposter serves on every node after 2xx; unchanged re-`PUT` grows the log by zero entries; tag-stability test extended |
| **S3** `feat(spec): drift diff + re-import policy` | §3.5 classifier, `overwrite\|skip\|fail`, `drifted` flag, `GET /specs/:id/drift` | hand-added stub survives `overwrite`; hand-edited `spec:` stub is reported, not silently clobbered; `fail` refuses with the report |
| **S4** `feat(spec): soft validation — bus consumer + violations read` | in-process validator, `jsonschema` cache, per-node violations table, merged `validationFailures` read with `since=` cursor, `ValidationPolicySet` (`off`/`soft` only) | an off-contract request on a `soft` imposter yields exactly one violation row, joinable to its journal index; `off` imposters measure zero overhead |
| **S5** `feat(spec): U-13 exchange inspector — upstream PR + pin bump` | upstream: trait, provider, two hook points, inert default; here: seam re-export in `rift-cluster-base::seams`, `seams_resolve` extended | upstream suites green with no inspector installed; parity gate (#37/#139) unchanged |
| **S6** `feat(spec): hard + hard-spec-compliant enforcement` | EE inspector implementation, `hard`/`hard-spec-compliant` modes, Accept negotiation, RBAC actions wired per §4.3 | request + response violations rejected with declared shapes; policy flip visible fleet-wide at barrier; internal-defect path proceeds loudly (counter asserted) |
| **S7** `test(cluster): C19/C20 — spec state converges and survives restart` | C19: deploy + policy flip converge on every node and survive full-cluster restart (the C17/C18 pattern from `5b98fef`); C20: hard-mode rejection identical through any node under round-robin while a follower restarts | both scenarios green in the chaos tier |
| **S8** `feat(spec): openapi+https/git source kinds` — **blocked on #20/U-12** | the two schemes as `ImposterSource` providers; drift policy inherited | Git spec bump → one pull op → fleet converges; unchanged poll = zero log growth |

## 10. Open questions

1. **OpenAPI 3.1.** `openapiv3` targets 3.0.x; 3.1 likely means a second
   parser (e.g. `oas3`) or an internal downgrade pass. Decide against real
   user specs, not in the abstract — and whether Swagger 2.0 conversion is
   worth carrying at all.
2. **Should `soft` become the default once U-13 lands?** U-13-based
   observation would not need `recordRequests`, removing the §3.6 objection
   to WireMock's default. Revisit at S6 with hot-path benchmarks in hand
   (the ≤ 2 % regression gate of Ch. 5 decides, not taste).
3. **Violation volume under `soft` on a load-test fleet.** The per-port
   cap + retention may not be enough at 10k RPS of off-contract traffic;
   consider per-operation sampling if a real deployment hits it.
4. **Per-status stub selection ergonomics.** The `X-Rift-Spec-Status`
   discriminator (§3.2) is functional but bespoke; if scenario-driven error
   selection matters to users, that is a compiler option, not a schema change.
5. **`SpecBind` cardinality.** v1 binds one spec per imposter. A gateway
   imposter fronting several services (via the front door) may want N specs
   per port; the `sm_validation` record shape admits it, the compiler's
   collision story does not yet.

---

## Appendix A — what this RFC does not change

- The OSS config schema and wire format: compiled imposters are plain
  Mountebank-compatible JSON plus existing Rift extensions
  (`routePattern`, stub `id`).
- The data plane for imposters without a bound spec — and with `off` (the
  default), for bound ones too.
- The admin write path: spec deploys are ordinary `PutImposter` ops.
- RFC-002's model: actions are added to its closed set by its own extension
  rule; no parallel auth, no new principal kind, no new tenancy scoping.
- The journal and SSE contracts: soft mode consumes the existing bus, and
  `validationFailures` is a new read, not a mutation of `savedRequests`.
