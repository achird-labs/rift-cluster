# RFC-003 — WireMock Cloud Parity Program (milestones M1–M6)

| | |
|---|---|
| **Status** | v1 — program plan for review; capability designs split into RFC-004/005/006 |
| **Tracking issue** | Milestones M1–M6 (repo milestones 1–6); epics [#20](https://github.com/achird-labs/rift-enterprise/issues/20) (M1), [#146](https://github.com/achird-labs/rift-enterprise/issues/146) (M2), [#147](https://github.com/achird-labs/rift-enterprise/issues/147) (M3), [#148](https://github.com/achird-labs/rift-enterprise/issues/148) (M4), [#149](https://github.com/achird-labs/rift-enterprise/issues/149) (M5), [#150](https://github.com/achird-labs/rift-enterprise/issues/150)/[#151](https://github.com/achird-labs/rift-enterprise/issues/151) (M6a/b) |
| **Canonical location** | `rift-enterprise:docs/rfc/RFC-003-wiremock-cloud-parity-program.md` |
| **Depends on** | ADR-001 (Raft control plane, merged); RFC-001 Phase 1 (merged); RFC-002 (design complete, unbuilt); Ch.7 verification plane (designed); Ch.13 sources (epic #20, sliced `ready`) |
| **Ground truth** | rift-enterprise @ `5b98fef`; `vendor/rift` @ `v0.16.0-4-g97757f0`; WireMock Cloud feature set verified live against `wiremock.io` / `docs.wiremock.io` on **2026-07-26** |
| **Author** | Mohsen Zainalpour |
| **Date** | 2026-07-26 |

---

## 1. Summary

WireMock Cloud is the nearest commercial competitor. This RFC is the audited
answer to "do we match it, and where we don't, what do we build next?" It
resolves a three-way comparison — WireMock Cloud as it ships **today** (not the
2023-era view), this repo as it ships today, and the designs already written
but unbuilt — into six milestones with owners, dependencies and exit criteria.

Three findings shape everything below:

1. **The architecture race is already won.** WireMock's biggest post-2024 move
   is "WireMock Runner": a containerized data plane pulling mock definitions
   from their cloud control plane into your infra. That is precisely the
   control-plane/data-plane shape this repo already ships — except ours is
   self-hosted end-to-end, zero-external-dependency, and consensus-backed.
   We do not need to chase the architecture; we need to close **product
   surface** gaps on top of it.
2. **The real gaps are six, and four already have written designs.**
   Tenancy/RBAC/audit (RFC-002), fleet-wide verification/request log (Ch.7),
   sources/Git sync (Ch.13 + epic #20) are designed and unbuilt; spec-driven
   mocking, data sources, and the web console + MCP surface were undesigned
   until the sibling RFCs 004–006 filed with this program.
3. **One gap is new since our last look:** WireMock's 2026 headline is an
   **MCP server + agent skills** so coding agents author mocks. Parity there is
   cheap for us — the clustered admin API already exists — and strategically
   urgent, because it is now their top-of-funnel.

## 2. Method — three verifications, no assumptions

- **Competitor**: live fetch of `wiremock.io`, `docs.wiremock.io` (product
  docs, pricing, changelog posts) on 2026-07-26. Corrects a 2023-era internal
  matrix that predates Runner, MCP, first-class gRPC/GraphQL, and the removal
  of the public mid-tier.
- **This repo**: full crate inventory at `5b98fef` (~30k first-party Rust
  lines; capability-by-capability status with file:line evidence, §4).
- **Designs on file**: `docs/architecture/` chapters 1–14, ADR-001, RFC-001/002,
  and the six external capability studies in the planning vault (compiled
  2026-07-23) — used as input, **not** adopted wholesale: those studies assume
  a Postgres/ClickHouse/Redis control plane with a sidecar agent, which
  contradicts this repo's zero-dependency embedded control plane (Ch.3, D-6).
  Every capability they describe is re-grounded here on the real architecture.

## 3. The competitor, corrected

What a 2023-era view of WireMock Cloud misses (all verified 2026-07-26):

| Change | Consequence for us |
|---|---|
| **MCP server in their CLI + "Agent Skills"** — coding agents (Cursor, Copilot, Claude Code) create/update mocks; free tier; now the marketing headline | New gap, not in any prior matrix. Cheap to close (M6b), high leverage |
| **WireMock Runner** — containerized data plane pulls definitions from their cloud; serve + record-many modes; k8s docs; Git/CI promotion | Validates our two-plane architecture; kills "hosted-only" as their differentiator. Our counterpart is epic #20 sources + the cluster itself (M1) |
| **gRPC + GraphQL first-class in Cloud**, incl. GraphQL Federation | Engine-level gap for us (`control.rs:235-238` restricts protocols to `http|https`). Upstream track (§7), not an EE milestone |
| **Git sync** — every simulation change versioned, diffable, syncable; OpenAPI Git integration | Epic #20 (`git+https:` sources, drift policy) is the direct counterpart — already sliced `ready` |
| **Dynamic state** = context-scoped mutable KV with Handlebars helpers + per-stub SET/DELETE operations | Our clustered flow store already exceeds it on guarantees (owner CAS, fencing, durability); gap is **ergonomics** (contexts, reset, inspector) → RFC-005 |
| **Data sources** = CSV uploads **and live DB connections**, filtered by Handlebars-templated SQL `WHERE`, rows exposed as `data.items` | Genuine gap → RFC-005 (CSV/dataset first; DB connectors deliberately deferred, §9) |
| **Audit events ship to a customer-owned S3 bucket** (Enterprise) | RFC-002 §9 stores audit on-cluster; an export sink is a small M2 follow-on, noted there |
| **No public mid-tier anymore** — Free + Enterprise only; SSO, RBAC, chaos, stateful mocking all Enterprise-gated | Packaging insight: the features they paywall hardest are exactly M2/M5 territory |
| OSS 4.0 still beta; contract validation still has **no first-party OSS extension** | Spec-driven validation remains a commercial differentiator on both sides → M4 |

## 4. Verdicts — the resolved gap matrix

Status of every claimed gap, checked against `crates/` at `5b98fef`:

| Capability | WireMock Cloud (verified) | Rift EE today (evidence) | Verdict | Milestone |
|---|---|---|---|---|
| Multi-tenancy | org → team → mock-API ACLs | `require_default_tenant()` rejects all but `"default"` (`rift-cluster/src/control.rs:283-292`); tenant/principal/binding ControlOps reserved (`:272-279`) | **Gap — design complete (RFC-002), unbuilt** | M2 |
| RBAC | 3-level roles, Enterprise-gated | Only occurrence of "rbac" in `crates/` is a rejection message | **Gap — RFC-002, unbuilt** | M2 |
| API tokens | account API keys | One optional global static bearer (`rift-ee-server/src/admin_front.rs:98-101,450-463`); no argon2 anywhere | **Gap — RFC-002 T3, unbuilt** | M2 |
| Audit log | events → customer S3 | No audit table; `ControlRequest.principal` is a documented placeholder (`control.rs:56-61`) | **Gap — RFC-002 §9, unbuilt** | M2 (+S3 sink follow-on) |
| SSO (SAML) / SCIM | Enterprise | Absent; RFC-002 defers OIDC/mTLS to v2, SAML/SCIM never designed | **Gap — deliberately sequenced after M2** (§9) | post-M6 RFC |
| Request log / inspector | live log with match diagnostics | Per-node in-memory journal only; no `RequestJournal` seam impl in `crates/` (grep: none); Ch.7 merge-on-read design unbuilt | **Gap — design complete (Ch.7), unbuilt** | M3 |
| Verification (`numberOfRequests`, cursors, SSE) fleet-wide | n/a (single mock instance semantics) | Same as above — reads are per-node and silently partial today | **Gap — Ch.7, unbuilt** (also a correctness debt independent of parity) | M3 |
| Git sync / definition versioning | Enterprise | Ch.13 `ImposterSource` SPI + drift policy; epic #20 sliced A–E, all `ready`, zero code yet (`imposter_source` absent from `crates/`) | **Gap — in flight** | M1 |
| OpenAPI import → mocks | core Cloud feature | Zero hits for `openapi` in `crates/` and `docs/`; not designed | **Gap — newly designed** | M4 (RFC-004) |
| Contract/traffic validation | 4 modes (off/soft/hard/hard-spec-compliant) | Absent | **Gap — newly designed** | M4 (RFC-004) |
| Dynamic state | context-scoped KV, LRU, Handlebars helpers | **Ahead on substance**: `ClusteredFlowStore` — owner CAS, fencing, replication, tombstones, anti-entropy, durable `FlowShard` with TTL (`rift-cluster/src/stores/{flow,shard}.rs`) | **Partial gap — ergonomics only** (contexts, reset API, inspector) | M5 (RFC-005) |
| Data sources | CSV + DB connections, SQL WHERE | Absent in EE; engine-side `behaviors/lookup` status verified in RFC-005 | **Gap — newly designed** | M5 (RFC-005) |
| Web console | the product's face | No web UI anywhere (`crates/`, `deploy/`); OSS ships a TUI | **Gap — newly designed** | M6a (RFC-006) |
| MCP / AI agent surface | 2026 headline | Absent | **Gap — newly designed; pull-forward candidate** | M6b (RFC-006), unblocked after M2 |
| Latency analytics | usage metering + basic analytics | Engine exposes per-request latency on `/metrics`; cluster gauges exist (`rift-cluster/src/metrics.rs`) but no fleet aggregation/dashboards | **Partial gap — closed by packaging, not by building a TSDB** (§6, M3) | M3 |
| Alerting | n/a beyond usage | Docs advise which metric to alert on; nothing shipped | Same as above — ship rules, don't build an alerter | M3 |
| Record & playback | cloud recording, record-many | OSS records single-node; `ProxyRecordingStore/ClaimToken` seams re-exported but unimplemented; Ch.7 `proxyOnce` claim state machine unbuilt | **Gap — Ch.7, unbuilt** | M3 |
| Chaos / fault injection | Enterprise-gated, UI-configured | Engine ships faults natively; EE has a chaos *test* tier | **No gap at engine level**; console exposure rides M6 | — |
| gRPC / GraphQL / webhooks | first-class | Engine is HTTP/HTTPS-only (`control.rs:235-238`) | **Engine gap — upstream track**, not an EE milestone | §7 |
| Hosted SaaS, uptime SLAs | the business model | Explicit non-goal of the current design (Ch.14 is self-host reference architecture) | **Not a gap — a strategic decision**, kept open in §9 | — |

**Verdict on the vault matrix:** directionally right — every `TBD/❓` it flagged
resolves to a real gap or an in-flight design — but its architecture proposals
(Postgres control plane, sidecar agent, ClickHouse telemetry) are superseded by
what this repo actually built, and it missed MCP, Runner, and first-class
gRPC/GraphQL entirely.

## 5. What we deliberately do not chase

- **A hosted SaaS control plane.** Ch.14 ships self-host reference
  architectures; D-6 (Ch.11) states the moat as distributed correctness for
  self-hosted fleets. Runner's existence shows the market accepts — wants —
  the data plane in the customer's infra; we extend that to the control plane.
  Revisit only as a business decision (§9), not as parity reflex.
- **A time-series database.** Zero-dependency rules out embedding one, and
  every target customer already runs Prometheus. We ship correct fleet
  dashboards, recording rules (histogram-bucket merges, never averaged
  percentiles) and alert rule packs; the console links to them (M3c).
- **Cross-region, per-principal data-plane auth, per-tenant compute isolation**
  — restated non-goals (Ch.1 §non-goals, Ch.8, RFC-002 §7); parity does not
  override them.
- **Enterprise support/SLA offerings** — real parity items, not engineering
  deliverables; out of scope for this program.

## 6. Milestones

Delivery-ordered. Each is one GitHub milestone; epics and slices follow the
house convention (epic issue + lettered ~1-PR slices, `ready` label at triage).

### M1 — Sources & Git Sync *(in flight — epic #20, slices A–E `ready`)*
The `ImposterSource` SPI (`file:`/`https:` upstream; `git+https:`/`s3://`/
`registry://` enterprise), fetch-then-submit through the clustered write path,
leader-only tracking polls, provenance + drift policy, chaos scenarios C20–C23.
**Parity delivered:** Git sync / versioned definitions; the "Runner promotion
workflow" equivalent. **Exit:** #20's own bar — C20–C23 green, drift policy
enforced, provenance visible in `/_cluster/imposters`.

### M2 — Tenancy, RBAC & Audit *(RFC-002 Phases T1–T4)*
Tenants/principals/bindings on consensus; argon2id API keys shown once;
enforcement at every admin entry point via upstream seam U-9; audit as a
derivation of the Raft intent log with principal attribution via U-10;
quotas. **Pre-req:** U-9/U-10 filed upstream **now** (§7) — a two-repo round
trip sits in front of T2. **Follow-on slice:** audit export sink (S3/webhook)
for parity with WireMock's customer-owned-bucket story.
**Exit:** RFC-002's own acceptance list; `GET /admin/audit` serving attributed,
hash-honest history; `rift_cluster_insecure`-style gauge for "running with no
principals defined".

### M3 — Verification Plane & Fleet Request Log *(Ch.7 implementation)*
Per-writer journal shards with merge-on-read and `Rift-Cluster-Partial`
honesty; G-counter `numberOfRequests`; clear-generations on the Raft log;
vector cursors + SSE tail; `proxyOnce` exactly-once owner claims. Plus the
packaging half of analytics: Grafana dashboard pack + Prometheus recording/
alert rules for fleet latency and match-rate. **Parity delivered:** the
request log/inspector backend, correct record-once, "why did my mock 404"
debuggability. **Exit:** cluster suite proves fleet-wide `savedRequests` /
counts / clears / cursors under node kill (new C-scenarios); dashboards render
against the 3-node compose.

### M4 — Spec-Driven Mocking *(RFC-004)*
OpenAPI 3.x import as a pure spec→imposter compiler entering the normal
clustered write path; specs as a source kind (`openapi+https:`/`openapi+git:`)
riding M1's fetch/drift machinery; re-import diff; traffic validation observe
mode off-path first (zero engine change — requests only, since the journal
carries no responses; RFC-004 states this plainly), full request+response
enforcement behind upstream seam U-13. **Exit:** import a real-world spec →
running imposter in one command; violation records queryable fleet-wide
(RFC-004 S4); drift report on spec bump.

### M5 — Data Sources & State Ergonomics *(RFC-005)*
Tenant-owned, content-addressed CSV/JSON dataset artifacts distributed to
every node before activation (M1's fetch-then-replicate rule); stub bindings
compiling down to engine-native lookup/templating; state contexts mapped onto
the existing flow machinery; state inspector + RBAC-gated reset; TTL
surfacing. **Exit:** the CRUD-mock demo — import dataset, GET reads it, POST
mutates state, parallel test sessions don't collide, reset restores.

### M6 — Web Console (a) & MCP Server (b) *(RFC-006)*
**a:** SPA embedded in the binary (air-gap friendly, zero runtime deps),
tenant-scoped IA over the public admin API only: imposter/stub editors with
form⟷JSON parity and lint, request log (M3), fleet view, state inspector (M5),
sources (M1), specs (M4), tenants/roles/tokens/audit (M2), front-door route
editor (shipped). **b:** MCP server wrapping the same API with RFC-002
principals, tool list adapting to role; imposter/stub CRUD, log query, spec
import, state reset, cluster health. **b is unblocked after M2 alone** — pull
it forward; it is small and strategically loud. **Exit:** console served from
the binary behind `/console` with RBAC-correct visibility; MCP session in
Claude Code/Cursor can author and verify a mock end-to-end, fully audited.

## 7. Upstream seam & feature pipeline (vendor/rift)

File early — every one is a two-repo round trip and M2 is blocked on the first
two:

| Seam / ask | Needed by | Status |
|---|---|---|
| **U-9** — admin request authorization hook | M2 (T2) | Specified in RFC-002 §6; filed as [achird-labs/rift#854](https://github.com/achird-labs/rift/issues/854) |
| **U-10** — principal on `ImposterEvent` | M2 (T4 audit) | Specified in RFC-002 §6; filed as [achird-labs/rift#855](https://github.com/achird-labs/rift/issues/855) |
| **U-13** — `ExchangeInspector` in-path request/response inspection | M4 enforce mode only (soft mode needs no engine change) | Specified in RFC-004 §6 (hook points named to line numbers; inert default) |
| **U-14** — template-function registration (`TemplateFunctionProvider`) | M5 `data`/`stateAt` helpers (v1 datasets work without it via `${row}[column]`) | Specified in RFC-005 §6.2 |
| **U-15** — declarative state operations (`_rift.stateOps`) | M5 state-write parity without `--allowInjection` | Specified in RFC-005 §6.1 |
| Engine feature asks: gRPC, GraphQL, webhooks | post-M6 protocol parity | Park as upstream feature issues; EE inherits via seams; `control.rs` protocol validation lifts when upstream ships |

## 8. Sequencing

```
now ──► M1 (#20, ready) ─────────────────────────────►
        file U-9/U-10 upstream ──► M2 (RFC-002 T1–T4) ──► M3 (Ch.7) ──► M4 ─┐
                                          │                        └► M5 ─┼──► M6a
                                          └────────────► M6b (MCP) ◄──────┘
```

- **M1 starts now** — it is sliced, triaged, and blocks nothing else.
- **U-9/U-10 get filed upstream in parallel with M1**, so M2's only hard
  dependency is moving while #20 runs.
- **M3 before M4/M5**: observe-mode validation (M4) and per-variant analytics
  want the fleet journal; the console's highest-value screen reads it.
- **M4 ∥ M5** — independent of each other; both compile down to engine-native
  artifacts.
- **M6b (MCP) needs only M2** (principals to authenticate as, audit to attribute
  to). Pulling it ahead of M6a is explicitly sanctioned.
- **M6a last** — every screen is a client of APIs the earlier milestones harden.

## 9. Open questions

1. **Hosted SaaS**: does a managed offering ever make business sense, or is
   "your infra, our correctness" the permanent position? Affects nothing in
   M1–M6; affects everything after.
2. **SSO ordering**: RFC-002 v2 planned OIDC before SAML. Enterprise
   procurement usually demands SAML+SCIM. Decide when M2 lands; likely one
   RFC covering OIDC (protocol) + SAML (federation) + SCIM (lifecycle).
3. **DB-backed data sources**: WireMock connects to live databases. That
   breaks zero-dependency *optionally* (customer opts in, like the Redis flow
   backend). Deferred from RFC-005 v1; revisit on demand signal.
4. **Journal retention**: Ch.7 keeps journals test-run-scoped and volatile by
   design. WireMock retains request logs. If users ask for retention, the
   segment-file extension (Ch.9 note) is the path — do not silently promise it.
5. **Packaging/tiers**: WireMock gates SSO/RBAC/chaos/state behind Enterprise
   with no public mid-tier. Our open-core line (Ch.11) already draws
   cluster-vs-single-node; where M2–M6 features sit on that line is a
   commercial call to make before M6 ships.

## Appendix A — inputs

- Vault studies (2026-07-23): feature-gap matrix + findings + capability docs
  01–06 — capability requirements adopted; architecture proposals superseded.
- Live competitor verification (2026-07-26): wiremock.io product/pricing/docs;
  docs.wiremock.io dynamic-state, data-sources, openapi-validation,
  teams-and-collaboration, audit-events; Runner and MCP announcement posts.
- Repo inventory (2026-07-26): capability checklist with file:line evidence at
  `5b98fef`, including per-crate module map of `rift-cluster`,
  `rift-ee-server`, `rift-ee`.
