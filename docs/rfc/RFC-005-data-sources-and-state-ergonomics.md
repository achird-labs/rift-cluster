# RFC-005 — Data Sources and Managed-State Ergonomics (v1)

| | |
|---|---|
| **Status** | v1 — design draft for review |
| **Tracking issue** | [achird-labs/rift-cluster#149](https://github.com/achird-labs/rift-cluster/issues/149) (milestone M5); isolation defect filed as [#152](https://github.com/achird-labs/rift-cluster/issues/152) |
| **Canonical location** | `rift-cluster:docs/rfc/RFC-005-data-sources-and-state-ergonomics.md` |
| **Depends on** | **ADR-001** (Raft control plane), **RFC-002** (tenancy, actions, 404-vs-403 rule), **#20** (ImposterSource SPI — the one-fetch-then-replicate rule this RFC mirrors) |
| **Ground truth** | verified at `rift-cluster@5b98fef`, `vendor/rift@v0.16.0-4-g97757f0` |
| **Author** | Mohsen Zainalpour |
| **Date** | 2026-07-26 |

---

## 1. Summary

WireMock Cloud sells two ergonomic features this stack almost has and does not
surface: **data sources** (upload a CSV, attach it to a stub, serve thousands
of distinct test profiles from one stub) and **managed state** (mutable values
keyed by a caller-chosen context, written by stubs, read by templates,
inspected and reset from the admin API).

The engine already contains most of the machinery. The vendored OSS core ships
a Mountebank-compatible `lookup` behavior that does per-request CSV row lookup
(§2.2), and the EE cluster ships a flagship flow-state store with owner-routed
CAS, fencing, replication, tombstones, TTL, and durability knobs (§2.3). What
is missing is not an engine — it is **lifecycle and ergonomics**:

1. **Dataset artifacts** — versioned, content-addressed, tenant-owned CSV
   tables with an upload API, cluster-wide distribution, and quota governance,
   compiling down to the lookup behavior the engine already executes. No new
   hot-path code.
2. **Context honesty** — WireMock's "Context" maps onto the existing
   `flowIdSource` / flow-id machinery, but the mapping has one verified hole
   (cross-imposter flow-id collision under `--cluster`, §2.4-G5) that this RFC
   closes.
3. **State ergonomics** — an inspector (list contexts and keys, with TTL), a
   scoped reset, declarative per-stub state operations (upstream seam
   **U-15**), and template-helper parity (upstream seam **U-14**).

Everything durable lands in the Raft state machine or per-node redb; nothing
here adds an external dependency, and nothing taxes the data plane beyond what
the engine's own behaviors already cost.

## 2. Why — the verified gap

### 2.1 The parity target

WireMock Cloud semantics, checked against docs.wiremock.io on 2026-07-26:

- **Data sources**: CSV upload (or a managed database connection). A stub
  attaches **one** data source and filters rows with an ANSI-SQL `WHERE`
  clause that may embed Handlebars against the request
  (`first_name = '{{request.query.name}}'`). Matched rows land in the template
  model as `data.items` with indexed access and iteration helpers. Marketed as
  "thousands of unique test profiles".
- **Dynamic state**: a *State Value* is a mutable string stored against a Key
  within a **Context** — a caller-chosen string (user id, test-session id) —
  in an LRU cache, so concurrent test sessions do not collide. Stubs write via
  per-stub *State Operations* (`SET` with Handlebars access to the request and
  `previousValue`, `DELETE` key, `DELETE_CONTEXT`), executed sequentially.
  Stubs read via `{{state 'key'}}`, `{{state 'key' context=...}}`,
  `{{#stateContext}}`, `{{listState ctx}}`. A per-mock-API default context is
  configurable.

### 2.2 What the engine already has — the lookup behavior

Verified at `vendor/rift/crates/rift-mock-core/src/behaviors/lookup.rs`:

- `LookupBehavior { key, fromDataSource, into }` (`lookup.rs:16-24`) is a
  first-class response behavior (`behaviors/types.rs:30-35`, the
  `behaviors.lookup` array on any stub response).
- The data source is **CSV only**, addressed by **local filesystem path**:
  `CsvDataSource { path, keyColumn, delimiter }` (`lookup.rs:44-54`).
- The key is extracted from the request via the copy machinery
  (`CopySource` over method/path/query/header/body + an `ExtractionMethod`,
  `lookup.rs:26-34`), then matched by **equality** against `keyColumn`.
- The matched row's columns replace `${token}[column]` markers in the response
  **body and headers** (`lookup.rs:188-228`), multi-value headers preserved.
- Parsed CSVs are cached in a **process-global** `CsvCache` keyed by path
  (`imposter/handler.rs:104-106`), populated on first use and **never
  evicted** — `CsvCache::clear` (`lookup.rs:110`) exists but nothing reachable
  from an embedder calls it.

So the engine does per-request CSV lookup today, and EE's data-sources job is
exactly the lifecycle: upload, version, distribute, govern, and bind — then
compile down to this behavior (invariant 3: OSS-native artifacts, zero
hot-path tax).

Two sharp edges in the engine's implementation, verified because the design
must work around them:

- **Duplicate first-column values collapse.** Rows are stored in a `HashMap`
  keyed by column 0 (`lookup.rs:120`, insert at `:148`) — two rows sharing a
  first-column value keep only the last.
- **Multi-match selection is nondeterministic.** When `keyColumn` is not
  column 0, the match scans `HashMap` iteration order (`lookup.rs:164-179`);
  with duplicate key values the served row varies run to run. §3.4 makes this
  unreachable rather than documenting it away.

### 2.3 What is already shipped — flow state

- **The store.** Under `--cluster` every imposter's flow state is served by
  the clustered store: owner-routed CAS with `(m_idx, v, origin)` fencing,
  async replication to two HRW successors, versioned tombstones, adoption and
  5 s anti-entropy (`crates/rift-cluster/src/stores/flow.rs`; semantics in
  `docs/architecture/06-flow-state.md` and `docs/rift-cluster-server.md` §"Clustered
  flow state").
- **Durability and TTL.** `FlowShard` persists `(flow_id, key) →
  Versioned { m_idx, v, origin, expires_at, value, deleted }`
  (`stores/shard.rs:108-126`), with per-imposter `_rift.flowState`
  `readConsistency` / `durability` knobs (`stores/flow_config.rs:55-99`) and
  `ttlSeconds` bounding entry life fleet-wide.
- **Context selection.** `flowIdSource: "header:<Name>"` resolves the flow id
  from a caller-chosen header, falling back to the imposter port as a string
  (`imposter/core/matching.rs:285-300`). This *is* WireMock's Context: the
  header value is the context, the port-string fallback is the per-mock
  default context.
- **Reads from templates.** `{{ state.<key> }}` in `_rift.templated`
  responses reads the request's resolved flow id
  (`extensions/template_fn.rs:275-286`).
- **Writes from stubs.** Scenario FSM transitions are declarative
  (`scenario_name` / `required_scenario_state` / `new_scenario_state`,
  `imposter/types.rs:199-207`); arbitrary-key writes exist only through the
  script surface (`docs/architecture/06-flow-state.md`: scenario FSM, script
  `flow_store:get/set/incr`, space-scoped data).
- **Admin surface.** Upstream already serves
  `GET/PUT/DELETE /admin/imposters/:port/flow-state/:flow_id/:key` and
  `DELETE .../flow-state/:flow_id` (whole-flow clear)
  (`rift-http-proxy/src/admin_api/router.rs:188-241`), and the EE clustered
  store implements `clear_flow` with tombstones (`stores/flow.rs:1256`).
- **RBAC vocabulary.** RFC-002 §4.1 already defines `FlowStateRead` and
  `FlowStateClear`. This RFC reuses them; it does not mint `StateRead` /
  `StateReset` twins.

### 2.4 The verified gaps

| # | Gap | Evidence |
|---|---|---|
| **G1** | No dataset lifecycle: the lookup behavior takes a raw filesystem path, so a clustered fleet has no way to guarantee the file exists — with identical bytes — on every node before a stub referencing it activates. No upload, no versioning, no tenancy, no quota. | `lookup.rs:44-54`; nothing in `crates/` touches `LookupBehavior` |
| **G2** | Stale-file hazard: `CsvCache` is keyed by path and never invalidated, so editing a CSV in place serves old rows until restart. | `handler.rs:104-106`, `lookup.rs:79-107` |
| **G3** | No dataset access from templates: the `{{ }}` function set is a **closed match** — `eval_base` ends in `Err("unknown template function")` with no registration hook anywhere (`ServerBuilder`, `ImposterManager`, or otherwise). | `template_fn.rs:205-289` |
| **G4** | No declarative state writes: a stub that wants to `SET counter = {{request.query.n}}` must become a script, and the clustered admin front gates every scripted config on `--allowInjection` (`config_uses_script_surface`, re-exported at `crates/rift-cluster-base/src/lib.rs:95`, enforced in `admin_front.rs`). A declarative feature that silently converts configs into scripted ones would flip that gate. | `flow_state.rs:18-115` (trait has CRUD, no op list); `types.rs:199-207` (FSM only) |
| **G5** | **Cross-imposter context collision under `--cluster`.** OSS gives each imposter its own store instance, so two imposters both using `flowIdSource: "header:X-Session"` are isolated. The clustered store passes the raw flow id into one fleet-global namespace — `ClusteredFlowStoreProvider::provide` adds no scope (`stores/flow.rs:1299-1321`), and every `FlowStore` method forwards `flow_id` verbatim (`stores/flow.rs:1190+`). Two imposters, same header value ⇒ shared keys. A parity divergence, not a feature. | `matching.rs:285-300`, `stores/flow.rs:1190-1321` |
| **G6** | No enumeration: `FlowStore` has no "list keys" or "list flows" (`flow_state.rs:18-115`), upstream has no listing route, and the shard's `flow(flow_id)` / `flow_ids()` (`shard.rs:347,371`) are reachable from no API. WireMock's `listState` / state inspector has no counterpart. TTL (`expires_at`) is stored but surfaced nowhere. | `flow_state.rs`, `router.rs:188-241`, `shard.rs:347-384` |
| **G7** | No cross-context template read (`{{state 'key' context=...}}`): `state.<key>` is hardwired to the request's resolved flow id. | `template_fn.rs:275-286` |

## 3. Design

### 3.1 Dataset artifacts

A **dataset** is a tenant-owned, named, versioned CSV table. Every version is
immutable and **content-addressed** by the SHA-256 of its bytes.

```rust
pub struct DatasetName(String);          // slug, unique per tenant
pub struct DatasetDigest([u8; 32]);      // sha256 of the raw CSV bytes

pub struct DatasetRecord {
    tenant: TenantId,                    // RFC-002 §3
    name: DatasetName,
    version: u64,                        // monotonic per name; v1, v2, …
    digest: DatasetDigest,
    key_columns: Vec<String>,            // declared at upload; see §3.4
    delimiter: char,                     // default ','
    columns: Vec<String>,                // header row, for validation & display
    rows: u64,
    bytes: u64,
    created_at: SystemTime,
    deleted: bool,                       // tombstone, RFC-002 §3.3 reasoning
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("dataset {0} exceeds the per-dataset limit of {1} bytes")]
    TooLarge(u64, u64),
    #[error("key column '{0}' is not unique: rows {1} and {2} share value '{3}'")]
    DuplicateKey(String, u64, u64, String),
    #[error("column '{0}' declared as key column is not in the header row")]
    NoSuchColumn(String),
}
```

Upload is validated at the Raft leader (the same place imposter configs are
validated, `crates/rift-cluster/src/control.rs`): well-formed CSV, header row
present, declared key columns exist and are **unique** within the file, size
and count under the tenant's quota (§4). A dataset that fails validation is
refused with a 400 naming the row and value — never accepted-but-broken.

### 3.2 Distribution — the bytes ride the log

> **As shipped (#285, D1).** `DatasetPut { tenant, record, csv }` /
> `DatasetDelete { tenant, name }`; `sm_datasets (tenant, name, version)` and
> `sm_dataset_blobs digest → csv` (referenced-by-scan, like `sm_spec_blobs`);
> the spool file `<data-dir>/datasets/<digest>.csv` is written before the
> record row and removed after the transaction that dropped the last live
> reference commits; `reconcile_engine` repairs a missing file at startup and
> never deletes. Validation runs in `control::validate` (so on every replica,
> not only the leader) and mirrors the engine's tokenizer; the quotas
> `maxDatasets` / `maxDatasetBytes` / `maxDatasetTotalBytes` are enforced at
> apply like the imposter quotas. "Delete while bound answers 409" is, in D1, a
> committed `Failed` refusal at apply (the `409` shape arrives with D3's route),
> and the guard fails closed: an unreadable stored config refuses the delete.
> `version` and `created_at` are assigned at
> apply from log order and the replicated clock, not sent by the client, so the
> op's record is the declared-and-verifiable half of §3.1's struct only.
> **§11's "openraft/redb are comfortable with quota-ceiling log entries" is
> answered, and the answer is no** (#411): openraft 0.9 bounds AppendEntries by
> `heartbeat_interval` (50 ms), so an entry above roughly 512 KiB does not
> commit today. The 8 MiB restart test ships `#[ignore]`d against #411; the
> restart/repair proof runs at 128 KiB.

**Decision: dataset bytes are committed through the Raft log and materialized
to a per-node spool file at apply time.** No blob sidecar, no fetch protocol,
no gossip.

- `ControlOp::DatasetPut { tenant, record, bytes }` commits metadata and bytes
  together. Apply, on every node, writes the bytes to
  `<data-dir>/datasets/<digest>.csv` (write-temp-then-rename, `0600`), then
  inserts the record. `ControlOp::DatasetDelete { tenant, name }` tombstones
  the record; the blob is removed when no live record references its digest
  (digests are refcounted — two tenants uploading identical bytes share one
  blob and one spool file).
- **Ordering is the correctness argument.** A stub binding (§3.3) names a
  `(name, version)` whose digest must already be applied; leader validation
  refuses a binding to an absent or tombstoned dataset. Because dataset put
  and imposter put are both log entries, *log order alone* guarantees every
  node has the bytes on disk before any config referencing them applies —
  which is #20's "one fetch, then replicate" rule with the upload as the one
  fetch and the log as the replication. No readiness handshake needed.
- **R3 for free.** The snapshot carries the dataset tables like every other
  state-machine table (`raft/store.rs:76-91` precedent); a restarted or
  freshly-joined node re-materializes spool files from the snapshot during
  apply. A missing spool file on disk is repaired from the state machine at
  startup, not fetched from a peer.

Why in-log rather than a fetched blob tier: the admin front already caps
terminated bodies at 16 MiB (`admin_front.rs:82`, `MAX_BODY_BYTES`), and this
RFC caps datasets well under that (§4, default 8 MiB per dataset). At that
scale a log entry is unremarkable — the log already carries whole
`ImposterConfig`s with inline stub bodies — and the alternative buys nothing
but a second distribution mechanism to test. Datasets that outgrow the cap are
an explicit non-goal for v1 and an open question (§10.2) pointing at the #20
fetch machinery, which exists precisely for large external artifacts.

Content-addressing also dissolves G2: a new version is a new digest is a new
path, so the engine's path-keyed `CsvCache` misses naturally and the stale-file
hazard cannot occur. No upstream cache-invalidation seam is needed. (Parsed
rows for superseded digests linger in process memory until restart; bounded by
the tenant byte quota, and noted in §8.6.)

### 3.3 Binding — compile down to the lookup behavior

A stub response binds **one** dataset (matching WireMock's one-source-per-stub
rule) via an EE-owned block:

```jsonc
{
  "is": { "statusCode": 200, "body": "{\"name\": \"${row}[first_name]\", \"tier\": \"${row}[tier]\"}" },
  "_rift": {
    "dataset": {
      "name": "customers",
      "version": 3,                  // optional; default = latest at bind time, then pinned
      "key": { "from": { "query": "id" }, "using": { "method": "regex", "selector": ".*" } },
      "keyColumn": "customer_id",
      "into": "${row}"
    }
  }
}
```

The control-plane record keeps the `_rift.dataset` block (it is the durable
intent, and what `GET` returns). At **apply time on each node**, the EE layer
compiles it into the upstream behavior the engine already executes:

```jsonc
"behaviors": { "lookup": [ {
  "key": { "from": { "query": "id" }, "using": { "method": "regex", "selector": ".*" } },
  "fromDataSource": { "csv": { "path": "<data-dir>/datasets/<digest>.csv",
                                "keyColumn": "customer_id", "delimiter": "," } },
  "into": "${row}"
} ] }
```

Compilation happens at apply rather than at admission because the spool path
is node-local (`data-dir` differs per node) while the log must stay
node-agnostic; the rewrite is deterministic given the committed digest.
`version` is resolved to a concrete digest at admission and **pinned** — a
later dataset upload never mutates a serving stub; rebinding is an explicit
config write. The hot path runs unmodified upstream code; EE's marginal
request-time cost is zero (invariant 3).

Two notes on the boundary:

- With `--cluster` off, none of this exists; a raw `behaviors.lookup` with an
  explicit path keeps working exactly as upstream — the parity bar rides on
  the untouched path, as everywhere else.
- The filter model is deliberately **key-equality plus the engine's existing
  extraction vocabulary** (`CopySource` + `ExtractionMethod`), not an SQL
  `WHERE` engine. WireMock's SQL-with-Handlebars buys arbitrary predicates at
  the price of shipping a SQL evaluator on the response path; the honest
  observation is that the dominant use — "look up the row for this
  caller-supplied id" — is key equality, which the engine already implements.
  Richer expression filters are an open question (§10.1), not a silent never.

### 3.4 Determinism — declared key columns

WireMock does not promise which row wins when a filter matches several; the
engine, as verified in §2.2, would make the answer *random*. This RFC makes
multi-match unrepresentable instead:

- Key columns are **declared at upload** (`key_columns` on the record) and
  validated unique within the file (`DatasetError::DuplicateKey`, naming the
  offending rows).
- A binding's `keyColumn` must be one of the declared key columns; leader
  validation refuses anything else.

One key, at most one row, the same row on every node and every run — the
"deterministic, seeded selection" requirement satisfied by construction rather
than by seeding. Sampling several matching rows is deferred with §10.1. The
first-column collapse quirk (§2.2) is also neutralized: uniqueness is enforced
on declared key columns, and validation additionally requires column 0 to be
duplicate-free so no upstream `HashMap` insert ever silently drops a row.

### 3.5 Context, made honest — scoped flow ids

WireMock's Context maps onto machinery that already exists — mostly:

| WireMock | Rift today | Verdict |
|---|---|---|
| Context (caller-chosen string) | `flowIdSource: "header:<Name>"` → flow id (`matching.rs:285-300`) | exists |
| Per-mock default context | port-string fallback (`matching.rs:297`) | exists |
| LRU cache, per-session isolation | per-flow TTL (default 300 s) + whole-flow LRU shedding at 100k entries/node (`06-flow-state.md`) | exists, stronger (durable, replicated, fenced) |
| Key/value CRUD + CAS | `FlowStore` (`flow_state.rs:18-115`) | exists, stronger |
| Context isolation between mock APIs | **broken under `--cluster`** (G5) | this section |

**Fix for G5:** the clustered store scopes every flow id at the provider
boundary — the one place that sees both the imposter config and the store.
`ClusteredFlowStore` gains a prefix computed once at `provide` time:

```rust
pub enum ContextScope { Imposter, Tenant, Fleet }   // _rift.flowState.contextScope

fn scoped(&self, flow_id: &str) -> String {
    match self.scope {
        ContextScope::Imposter => format!("i{}:{flow_id}", self.port),
        ContextScope::Tenant   => format!("t{}:{flow_id}", self.tenant),
        ContextScope::Fleet    => format!("f:{flow_id}"),
    }
}
```

- **Default `imposter`** — restores OSS parity: two imposters using the same
  header value are isolated, exactly as two `InMemoryFlowStore` instances are.
- **`tenant`** — opt-in shared contexts across one tenant's imposters: a test
  session spanning a payments mock and a search mock shares one context, which
  is a real suite-level want and today's accidental behavior made deliberate
  (and made *safe*: today's accidental sharing is fleet-wide, crossing
  tenants).
- **`fleet`** — the pre-RFC behavior, kept for migration, `FleetAdmin`-gated
  at admission because it deliberately crosses the tenant boundary.

> **As shipped (#152, then #288).** S1's core landed ahead of the M5 milestone
> (#152) with two deviations, both because RFC-002 had not landed yet; **#288
> closed both**: `tenant` is a real scope rendering `t<tenant>:` (the tenant is
> resolved at `provide` time from the control-plane owner of the port — never a
> field in the core config), and `fleet` is `FleetAdmin`-gated at admission on
> the admin front (a `400` naming the requirement, nothing committed; pre-gate
> configs keep serving). The `f:` namespace itself stays fleet-wide by design —
> tenant isolation is what `tenant` scope is for. The original deviations, for
> the record:
>
> - **`tenant` is parsed and refused, not implemented.** There is no source of
>   truth for a tenant at `provide` time: `ImposterConfig` carries no tenant
>   (RFC-002 §3.2 adds it), and `TenantId` is pinned to `"default"` with the
>   tenancy ops still reserved. `contextScope: "tenant"` is therefore a `400` at
>   admission naming RFC-002 (#17) — deterministic feature detection, the same
>   contract style as the reserved `ControlOp` variants, rather than a value
>   that silently means something else. `ContextScope` ships with two variants;
>   the third arrives with the tenant field it needs.
> - **`fleet` ships ungated.** The `FleetAdmin` gate is meaningful only once
>   there is a tenant boundary for `fleet` to cross; in a single-tenant fleet it
>   would refuse nothing. The gate lands in RFC-002 alongside `tenant`
>   activation, at the same admission point.
>
> Also as shipped: `fleet` renders an explicit `f:` prefix rather than passing
> ids through bare, so the namespaces are disjoint by construction — a
> caller-chosen id shaped like `i6400:x` cannot be made to read across the
> boundary.

Validation follows the `flow_config.rs` pattern: unknown value ⇒ 400 naming
the key. The prefix is invisible to configs, scripts, and templates — they name
flow ids unprefixed and the EE store applies the prefix beneath them. (*As
shipped:* nothing **strips**, because nothing hands a flow id back — no
`FlowStore` method returns one, and the admin API echoes the id from the request
path. Stripping first becomes real in §3.6's inspector, which lists stored ids.)
Migration note: on upgrade, existing
in-flight flows keyed without a prefix are orphaned; with the 300 s default
TTL this is a bounded, one-deploy blip, called out in release notes rather
than papered over with a compatibility read path.

The prefix is also what makes the inspector and reset (§3.6) *scopable*: the
store finally knows which imposter — hence which tenant — a flow belongs to,
which the repair path today explicitly cannot know (`stores/flow.rs:117`).

### 3.6 State inspector and reset

Built entirely EE-side: the shard already has the primitives
(`FlowShard::flow_ids`, `FlowShard::flow`, `shard.rs:347-384`), and the values
already carry TTL (`Versioned.expires_at`). No upstream seam.

- **List contexts** — `GET /admin/imposters/:port/flow-state` → contexts
  (flow ids, prefix-stripped) for that imposter's scope, with entry counts.
  Served by fanning `flow_ids()` across nodes over the existing cluster port
  (the anti-entropy `SyncPull` machinery already ships whole flows;
  enumeration is a strictly smaller request), filtered by prefix, deduplicated.
- **List keys** — `GET .../flow-state/:flow_id` → `{key, value, expiresAt,
  version}` per entry, owner-read for consistency (`readConsistency` applies).
  This fills the one hole in upstream's route family
  (`router.rs:188-241` has per-key GET but no listing) — the route is
  EE-terminated, since upstream has no handler to proxy to.
- **Reset** — `POST /admin/imposters/:port/flow-state/reset` (all of one
  imposter's contexts) and `POST /admin/tenants/:id/flow-state/reset`
  (everything under a tenant's prefixes). **Not a control op**: flow state is
  deliberately off the Raft log (`06-flow-state.md`'s opening argument), so a
  reset is a coordinated fan-out — enumerate matching flow ids, then
  owner-routed `clear_flow` per flow, which is already tombstoned and
  replication-safe (`stores/flow.rs:1256`, #126). Idempotent; partial failure
  reports the flows it could not clear rather than pretending.
- Single-context clear already exists upstream
  (`DELETE .../flow-state/:flow_id`) and keeps working unchanged — under
  `--cluster` it already reaches the clustered store via the proxied local
  engine.

RBAC (RFC-002 §4.1 vocabulary, no new actions): listing and reading =
`FlowStateRead` (Viewer+); every reset/clear = `FlowStateClear` (Operator+).
Tenant-wide reset additionally requires the caller's binding on *that* tenant;
cross-tenant probes answer 404 per RFC-002 §8.4.

### 3.7 Declarative state operations — upstream seam U-15

The write-side parity gap (G4). The wrong fix is compiling state operations
into generated scripts: `config_uses_script_surface` would then classify every
state-writing config as scripted and the admin front would demand
`--allowInjection` — a declarative feature must not smuggle configs through
the injection gate. So the operations become engine vocabulary, generic and
useful to OSS on their own (Mountebank users ask for exactly this):

```rust
// rift-mock-core::extensions::state_ops  (upstream, new module)

/// One post-response state mutation. Executed in order, after the response
/// is rendered, against the request's resolved flow id.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "op")]
pub enum StateOp {
    /// Set `key` to the rendered `value` template. The template evaluates in
    /// the existing `{{ }}` grammar plus one extra function, `previousValue`
    /// (the key's value before this operation), enabling counters and
    /// accumulators without a script.
    Set { key: String, value: String },
    /// Delete one key.
    Delete { key: String },
    /// Delete every key in the request's flow (WireMock DELETE_CONTEXT).
    ClearFlow,
}
```

Carried as `_rift.stateOps: [ ... ]` on a stub response — the same
config-extension shape as `_rift.templated` and `_rift.flowState`. Executed by
the imposter handler after response render, sequentially, via the imposter's
existing `FlowStore` — so in EE they are automatically owner-routed,
replicated, durable, and scoped (§3.5) with zero cluster code on the write
path. Errors follow the templating policy (`template_fn.rs:39-42`): fail the
render in debug, warn-and-continue otherwise.

Backward compatibility: absent `stateOps` ⇒ byte-identical behavior; the field
is additive config vocabulary, like every seam before it.

### 3.8 Template read parity — upstream seam U-14

The engine's `{{ }}` function set is a closed match with no registration point
(G3). Rather than upstreaming EE-specific helpers, upstream gets a generic
registration seam and EE registers what it needs:

```rust
// rift-mock-core::extensions::template_fn  (upstream, addition)

/// Embedder-supplied template functions, consulted by `eval_base` for any
/// head it does not recognize, before the "unknown template function" error.
pub trait TemplateFunctionProvider: Send + Sync {
    /// `Some(result)` if this provider owns `head`; `None` to pass. `ctx`
    /// exposes the request, resolved flow id, and read-only flow store —
    /// the same view built-ins get.
    fn eval(
        &self,
        head: &str,
        args: &[String],
        ctx: &TemplateContext<'_>,
    ) -> Option<Result<String, String>>;
}

// Registered per manager, mirroring the FlowStoreProvider precedent
// (flow_state.rs:117-126):
//   ImposterManager::with_template_functions(Arc<dyn TemplateFunctionProvider>)
```

Backward-compat default: nothing registered ⇒ the fallback arm errs exactly as
today (`template_fn.rs:287`). Built-ins always win — a provider cannot shadow
`request.*` / `state.*` / `now` / `uuid` / `randomInt`, so upstream's surface
stays upstream's.

EE registers, in a later slice (§9):

- `data '<column>'` — the bound dataset's matched row, for `templated: true`
  responses where `${row}[col]` token replacement is awkward (the WireMock
  `data.items` read path, minus iteration — §10.1).
- `stateAt '<context>' '<key>'` — cross-context read
  (`{{state 'key' context=...}}` parity, G7), resolved **within the imposter's
  scope prefix** so a template can never read across the §3.5 boundary it is
  supposed to be protected by.

v1 datasets do **not** wait for U-14: `${row}[column]` token replacement works
in bodies and headers with zero template involvement.

## 4. Data model — consensus vs per-node, and why

| Data | Where | Why |
|---|---|---|
| Dataset records (`DatasetRecord`) | Raft SM, `sm_datasets` table keyed `(tenant, name, version)` — the `SM_CONFIGS_TABLE` pattern (`raft/store.rs:80`) | Bindings validate against them at the leader; an eventually-consistent registry would admit bindings to datasets a node has not heard of — the exact class of bug the log's ordering removes |
| Dataset bytes | Raft SM, `sm_blobs` table keyed `digest`, refcounted | §3.2: log order = distribution guarantee; snapshot = R3; digest = dedup and cache-coherence. Bounded by quota, so "consensus-worthy small" holds by construction |
| Spool files `<data-dir>/datasets/<digest>.csv` | Per-node disk, derived | A pure materialization of SM state — deleted and rebuilt from the SM at will; never a source of truth |
| Flow state (contexts, keys, values, TTL) | Per-node redb `FlowShard` + ownership ring, **unchanged** | Request-path state; a quorum round per state write at 20-40k RPS "is not a design, it's an outage" (`06-flow-state.md`). This RFC adds zero flow-state storage |
| Dataset quotas | On the tenant record (`Quotas`, RFC-002 §3): `max_datasets` (default 50), `max_dataset_bytes` (default 8 MiB), `max_dataset_total_bytes` (default 64 MiB) | Enforced where the other quotas are: leader-side `ControlOp` validation (RFC-002 §4.4) |

## 5. Admin API surface

All routes EE-terminated at the clustered admin front (there is no upstream
endpoint to proxy to), same write barrier, revision and op-id headers as every
other terminated write. Actions are RFC-002 §4.1 vocabulary plus three new
entries (§6.3).

```
POST   /admin/tenants/:id/datasets                    DatasetPut      body: CSV (Content-Type: text/csv),
                                                                      name/keyColumns/delimiter in query or
                                                                      X-Rift-Dataset-* headers → {name, version, digest, rows}
GET    /admin/tenants/:id/datasets                    DatasetRead     list: name, latest version, rows, bytes, bindings-count
GET    /admin/tenants/:id/datasets/:name              DatasetRead     version history + which imposters bind it
GET    /admin/tenants/:id/datasets/:name/:ver/content DatasetRead     the bytes (audited; see §8.3)
DELETE /admin/tenants/:id/datasets/:name              DatasetDelete   409 while any live stub binds it — never break a
                                                                      serving stub by deleting its data out from under it

GET    /admin/imposters/:port/flow-state              FlowStateRead   list contexts (scope-filtered) + entry counts
GET    /admin/imposters/:port/flow-state/:flow_id     FlowStateRead   list keys: {key, value, expiresAt, version}
POST   /admin/imposters/:port/flow-state/reset        FlowStateClear  clear every context in the imposter's scope
POST   /admin/tenants/:id/flow-state/reset            FlowStateClear  clear everything under the tenant's prefixes
```

Role mapping, consistent with RFC-002 §4.2's read/disturb/redefine ladder:
`DatasetRead` → Viewer; `DatasetPut`, `DatasetDelete` → Editor (datasets
*redefine* behavior — a new customer table changes what stubs serve);
`FlowStateRead` → Viewer; `FlowStateClear` → Operator (a reset *disturbs*
state without redefining it — the person re-running a failing suite). Existing
per-key flow-state routes keep their RFC-002 assignments unchanged.

## 6. Upstream seams needed

Both follow the Appendix-A standard: generic naming, `Local`/no-op by default,
no tenant or dataset vocabulary crossing the boundary. U-9/U-10 remain
reserved by RFC-002; these take the next free numbers.

### 6.1 U-15 — declarative state operations (`rift-mock-core::extensions::state_ops`)

The `StateOp` enum and `_rift.stateOps` execution of §3.7. Upstream-generic
justification: Mountebank/Rift users writing `inject` scripts solely to bump a
counter or stash a token get a declarative, injection-gate-free alternative;
the feature is meaningful with the in-memory store and no cluster. Default:
absent field, identical behavior. **Not gated on `--allowInjection`** — that
gate exists for arbitrary code, and `StateOp` is data evaluated by the
existing template grammar, which is already served ungated.

### 6.2 U-14 — template-function registration (`rift-mock-core::extensions::template_fn`)

The `TemplateFunctionProvider` trait and
`ImposterManager::with_template_functions` of §3.8. Upstream-generic
justification: any embedder with domain vocabulary (ids, tokens, fixture
helpers) currently has one tool — scripts — for what is a pure function of the
request; the fallback-arm hook is the minimal registration point and built-ins
cannot be shadowed. Default: no provider, byte-identical rendering including
error text.

### 6.3 Not seams, but vocabulary

`DatasetPut` / `DatasetRead` / `DatasetDelete` join RFC-002 §4.1's closed
action enum (the enum is cluster-side; no upstream change). New `ControlOp`
variants `DatasetPut` / `DatasetDelete` join the reserved-op pattern
(`raft/store.rs:1203-1208`) until their slice lands.

## 7. Explicit non-goals

- **An SQL engine.** No ANSI-`WHERE` evaluator, no SQL parser dependency. The
  filter model is key-equality over declared unique keys (§3.3, §10.1).
- **Managed database connections.** WireMock Cloud's "connect a Postgres"
  path violates invariant 1 outright. Datasets are uploaded artifacts, full
  stop. External *origins* for datasets belong to the #20 source machinery.
- **Mutable datasets on the data plane.** Requests never write datasets;
  mutable per-caller data is what flow state is for. This keeps datasets
  cacheable, content-addressed, and free of write-path consistency questions.
- **Datasets above the size cap** (v1) — §10.2.
- **JSON datasets** (v1) — the engine's lookup is CSV; accepting JSON uploads
  and transcoding invites silent shape questions (nested objects, arrays) that
  deserve their own decision — §10.3.
- **Iteration / multi-row template helpers** (`data.items[n]`, `{{#each}}`) —
  requires U-14 plus a block-syntax extension to the `{{ }}` grammar, which is
  a bigger upstream conversation than a fallback arm — §10.1.
- **Changing flow-state LRU/TTL mechanics.** WireMock's LRU cache is an
  implementation detail, not a contract worth copying; Rift's TTL + LRU
  shedding already exceeds it.

## 8. Threat model

Datasets are the first EE object whose *payload* is plausibly real customer
data — teams export production rows to get "realistic test profiles". That
asymmetry drives this section.

1. **PII at rest, multiplied.** Dataset bytes land in the Raft log, the
   snapshot, the SM blob table, and a spool file on every node — the
   distribution guarantee is also a copy multiplier, stated rather than
   hidden. None of it is encrypted at rest (true of every EE store today).
   Mitigations: spool files written `0600`; documentation states plainly that
   uploading production PII places it on every node's disk and in every
   snapshot; per-tenant byte quotas bound exposure. At-rest encryption is a
   fleet-wide concern beyond datasets and stays out of scope here.
2. **PII in responses is the feature.** A bound stub serves dataset rows to
   an unauthenticated data plane (RFC-002 §7: the data plane is deliberately
   open). Anyone who can reach the imposter port and guess a key value reads
   that row. The mitigation is honesty plus governance: `DatasetPut` is
   Editor-gated and audited, so *who put which bytes where* is a log query —
   but network reachability of an imposter is, as in RFC-002, network policy's
   problem.
3. **Read-back leaks.** `GET .../content` returns the full table; RFC-002 v1
   does not audit reads. Dataset content reads are the one read class where
   that default is wrong — a bulk PII export should leave a trace — so
   `DatasetRead` **of content** (not listings) is audited, as a deliberate,
   narrow exception to RFC-002 §9's reads-are-not-audited rule.
4. **Cross-tenant access.** Datasets are tenant-keyed in the SM; routes are
   tenant-scoped; cross-tenant probes 404 (RFC-002 §8.4). Digest-level blob
   sharing (§3.2) never leaks *existence*: refcounts are internal, and no API
   reports "another tenant already has these bytes" — upload latency is
   identical on dedup hit and miss because the bytes travel the log either way.
5. **State values carry secrets too.** Stubs stash tokens and session
   material in flow state; the inspector (§3.6) now exposes values over the
   admin API where before only point reads existed. Listing is
   `FlowStateRead`-gated and tenant-scoped via §3.5 prefixes; the fleet-scope
   escape hatch is `FleetAdmin`-only. Values are returned verbatim —
   value-level redaction is unenforceable guesswork and is not pretended.
6. **Injection via CSV values.** Row values are substituted into response
   bodies by plain string replacement. Within the engine's own pipeline the
   remaining question is ordering — whether lookup substitution output can be
   re-evaluated by `{{ }}` templating (flagged unverified, §11); slice D2
   carries a test pinning that a value containing `{{ uuid }}` is served
   literally, and gains a body-side escape if the pin fails. The *client's*
   parser (JSON contexts, header CRLF) is the stub author's contract, as with
   every other substitution the engine already performs; header values are
   additionally validated by hyper before serving.
7. **Resource exhaustion.** Quotas (§4) bound bytes and counts at the leader;
   the 16 MiB admin body cap bounds any single request; `CsvCache` residency
   is bounded by the same byte quotas (plus superseded-version lingering,
   §3.2, bounded and restart-cleared).

## 9. Phasing

Slices sized ~1 PR each; cluster proofs follow the C-numbered `test(cluster)` convention (numbers assigned at filing).

| Slice | Contents | Exit criteria |
|---|---|---|
| **D1** `feat(data): dataset artifacts on the control plane` — **shipped (#285)** | `ControlOp::DatasetPut/Delete`, SM tables, leader validation (CSV shape, key uniqueness, quotas), spool materialization, refcounted blob GC | `test(cluster)`: a dataset uploaded on node A is byte-identical on every node's spool before the write's 2xx; survives full-cluster restart; delete while bound answers 409 |
| **D2** `feat(data): stub binding compiles to the lookup behavior` | `_rift.dataset` block, admission resolution + pinning, apply-time compile-down, determinism validation | Bound stub serves the correct row on every node; same key ⇒ same row across 100 runs; binding an absent dataset/column refused with 400 naming it; literal-`{{ }}`-in-CSV pin (§8.6) |
| **D3** `feat(data): dataset admin surface + RBAC + audit` | §5 dataset routes, `DatasetPut/Read/Delete` actions, content-read audit exception | Viewer lists but cannot upload; cross-tenant probe 404s; content read appears in the audit stream |
| **S1** `feat(state): context scoping` | `ContextScope` on the clustered store, `contextScope` knob + validation, `fleet` gated `FleetAdmin` | `test(cluster)`: two imposters, same header value, isolated keys under `--cluster` (the G5 parity proof); `tenant` scope shares within, never across, tenants |
| **S2** `feat(state): inspector + reset` | §3.6 routes, cross-node enumeration, fan-out reset | Listing shows keys with `expiresAt`; tenant reset clears every context under the tenant and nothing else; partial failure is reported, not swallowed |
| **U-15** upstream PR, then **S3** `feat(state): declarative state ops` | `StateOp`, `_rift.stateOps` execution, `previousValue` | A counter stub increments correctly under `--cluster` behind a round-robin LB with no script and `--allowInjection` off |
| **U-14** upstream PR, then **D4/S4** `feat(data): template helpers` | Provider registration; `data`, `stateAt` helpers | `templated: true` response reads the matched row; `stateAt` cannot cross the scope prefix |

D1-D3 and S1-S2 have no upstream dependency and can land immediately;
U-15/U-14 are filed after review, generic wording, #311-#318 precedent.

## 10. Open questions

1. **Expression filters and multi-row reads.** Key-equality covers the
   dominant case; WireMock's `WHERE` covers range and compound filters, and
   `data.items` covers iteration. If real usage demands them, the honest
   shape is a small predicate AST (reusing the engine's predicate vocabulary,
   not SQL) evaluated at request time — measured against invariant 3's
   hot-path rule before acceptance. Decide on evidence from D2 adoption, not
   in the abstract.
2. **Datasets beyond the cap.** The #20 `ImposterSource` machinery already
   solves "one fetch, replicate the result" for large external artifacts; a
   `dataset://` scheme or a source-owned dataset record is the natural
   extension. Needs the blob tier to move out of the log — a different
   durability design, deferred until someone actually has a 100 MiB table.
3. **JSON dataset uploads.** Accept-and-transcode flattens nested shapes
   silently; accept-natively needs engine work. Punt until asked, but the
   record's `columns`/`delimiter` fields deliberately do not preclude it.
4. **Scope-prefix visibility.** §3.5 hides prefixes everywhere. The inspector
   strips them on output; should a `FleetAdmin` fleet-scope listing show raw
   prefixed ids for debugging? Leaning yes-behind-a-query-flag; decide in S2
   review.
5. **`previousValue` under concurrency.** U-15 `Set` with `previousValue` is
   a read-then-write, not a CAS; two concurrent requests can interleave.
   `FlowStore::compare_and_set` exists (`flow_state.rs:100-114`) — should
   `Set`-with-`previousValue` compile to a bounded CAS retry loop? Decide in
   the U-15 upstream review, where the in-memory store's answer must match
   the clustered one.

## 11. Claims not verified

Stated so review knows where the ice is thin:

- **Substitution ordering** (§8.6): whether `{{ }}` templating runs before or
  after lookup token replacement in `imposter/handler.rs` was not traced;
  slice D2 pins it with a test either way.
- **Script-surface state API names**: `flow_store:get/set/incr` is cited from
  `docs/architecture/06-flow-state.md`, not re-verified against the Rhai/JS
  bindings.
- **WireMock Cloud semantics** are as summarized in §2.1 from docs.wiremock.io
  (2026-07-26); not re-tested against a live WireMock Cloud instance.
- **openraft/redb comfort with multi-MiB log entries** (§3.2) is asserted from
  the existing whole-`ImposterConfig` precedent, not benchmarked; D1's exit
  criteria include a fleet-restart test at the quota ceiling.

---

## Appendix A — WireMock Cloud ↔ RiftCluster mapping

| WireMock Cloud | RiftCluster (this RFC) | Status after v1 |
|---|---|---|
| CSV data source upload | Dataset artifact, content-addressed, tenant-owned (§3.1) | parity |
| Managed DB connection | — (invariant 1) | declined, §7 |
| One data source per stub | one `_rift.dataset` per response (§3.3) | parity |
| SQL `WHERE` + Handlebars | key-equality via existing extraction vocabulary | partial, §10.1 |
| `data.items` indexed access | `${row}[column]` tokens; `data '<col>'` via U-14 | partial (single row) |
| "Thousands of test profiles" | bounded by quota (default 8 MiB ≈ 40k × 200 B rows) | parity |
| Context (caller-chosen) | `flowIdSource: "header:<Name>"`, scoped (§3.5) | parity, hardened |
| Default context per mock API | port-string fallback | parity |
| LRU cache isolation | TTL + whole-flow LRU shedding, durable + replicated | exceeds |
| State Operations SET/DELETE/DELETE_CONTEXT | U-15 `StateOp::{Set, Delete, ClearFlow}` (§3.7) | parity (post-U-15) |
| `previousValue` in SET | U-15 `previousValue` template function | parity (§10.5 caveat) |
| `{{state 'key'}}` | `{{ state.<key> }}` — shipped upstream today | parity |
| `{{state 'key' context=...}}` | `stateAt` via U-14, scope-bounded | parity (post-U-14) |
| `{{listState ctx}}` / `{{#stateContext}}` | inspector API (§3.6); template-side listing deferred | partial |
| State inspector UI | `GET .../flow-state[/:flow_id]` (§3.6) | parity (API) |
| State reset | scoped reset endpoints (§3.6) | parity |
