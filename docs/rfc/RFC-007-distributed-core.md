# RFC-007 — The Distributed Core (v1)

| | |
|---|---|
| **Status** | v1.1 — decided as **D-71**; removals in progress. **Amended 2026-09-06:** the cluster-wide flow-state tier stays (§3.1); #551 withdrawn |
| **Tracking issue** | [achird-labs/rift-cluster#544](https://github.com/achird-labs/rift-cluster/issues/544) (epic) |
| **Canonical location** | `rift-cluster:docs/rfc/RFC-007-distributed-core.md` |
| **Depends on** | **ADR-001** (the Raft control plane stays exactly as decided) |
| **Retires, as its children land** | RFC-002 (tenancy and RBAC), RFC-005 (data sources and state), RFC-001 §7.5 and phase 4, RFC-004 §3.4–§3.6, RFC-006 §8 |
| **Ground truth** | `rift-cluster@5c8dbfb`, `vendor/rift@de0ab0f` (v0.17.0-42); live fleet baseline 57/57 on 2026-09-06 |
| **Author** | Mohsen Zainalpour |
| **Date** | 2026-09-06 |

---

## 1. Summary

RiftCluster set out to make a fleet of Rift nodes behave like one. Along the way it grew a
multi-tenant admin plane with roles and quotas, an audit projection with an export loop, an MCP
server, a thousand lines of metric families, per-route hit counters, three cloud source providers
with a fleet-wide poller, a content-addressed blob store with eleven decisions about its
replication, and a distributed request journal with vector cursors. Each was defensible on its
own. Together they are much of the code, half of the admin API, and the part of the system that
has produced the open bugs — while the thing the fleet exists to do is comparatively small and
already works.

This RFC narrows the project to **the distributed core** and records the narrowing as **D-71**:

- a fleet that **forms and heals itself** — bootstrap, seeds, join, leave, snapshots, restart;
- **replicated configuration** — imposters, stubs and the route table as Raft log entries, with
  read-after-write and a revision the client can pin;
- a **router** on every node that dispatches a request to the local imposter the replicated route
  table names;
- **cluster-wide flow state** — owner-authoritative scenarios and flow KV, the sequencer,
  proxyOnce claims and spaces, so a stateful mock behaves as one across the fleet without an
  external store;
- the **admin API and console to add and manage imposters and stubs**, including a one-shot
  OpenAPI import.

Everything else is either open-source Rift's own feature, used as shipped, or removed. The
removals are ten tracked issues (§4). Each is verified before merge against a running fleet,
by driving both the surface that leaves and the surfaces that stay (§5).

## 2. Why — what the measurement showed

Numbers are `wc -l` over `master@5c8dbfb`, first-party code only (`crates/`, `web/`, `tests/`,
`deploy/`); `vendor/rift` is excluded. Integration-test files count wholly as test.

### 2.1 Where the code is

| Classification | Source lines | Test lines | Share of source |
|---|---|---|---|
| Cluster core — Raft, membership, flow state, journal merge, proxyOnce, front door, their console screens | 49,092 | 60,586 | 52 % |
| Imposter management — CRUD, spec compiler, sources, stub editor, recording | 15,352 | 15,687 | 16 % |
| Peripheral — tenancy, RBAC, sessions, audit, MCP, metrics, route hits, deploy observability | 15,681 | 18,562 | 17 % |
| Shared console infrastructure (generated API client, app shell) | 14,481 | 3,959 | 15 % |
| **Total** | **94,606** | **98,794** | |

Inside "cluster core", one subsystem this RFC removes is itself large:

| Subsystem | Source | Test |
|---|---|---|
| Journal shards and merge-on-read (`stores/{journal,journal_net,journal_seq}.rs`, `pull_on_miss.rs`) | ~4,000 | ~4,200 in-file + `tests/fleet_journal.rs` 1,519 |

The flow-state tier (`stores/{flow,shard,flow_config,sequencer,proxy}.rs`, `raft/ring.rs`,
`bridge.rs`; ~6,100 source, ~10,500 test) **stays** — see §3.1 and the 2026-09-06 amendment.

What remains after every removal is the Raft store and node (~13,800 source), the RPC layer
(~1,600), the flow-state tier, the control-op set, the admin front's dispatch and CRUD proxying,
the router installation, probes, fleet reads, the CLI, the spec compiler (1,750) and five console
screens — on the order of a third of today's source.

### 2.2 Where the API is

`docs/api/openapi-ee.yaml` publishes 88 operations over 56 paths.

| Group | Operations |
|---|---|
| Imposter and stub CRUD, try, reload, sources, specs | 30 |
| Cluster: requests and verification, scenarios, spaces, flow state, front door, fleet, health | 30 |
| Peripheral: tenants, datasets, principals, bindings, audit, session, whoami, route hits, metrics, openapi | 28 |

After the removals the contract is roughly: the Mountebank-compatible imposter surface, `/stubs`
CRUD, `/front-door/routes`, `/specs/compile`, `/session`, `/_fleet/*`, `/_cluster/*`, `/health`.

### 2.3 Where the console is

Seven live navigation entries and one greyed "planned" entry. The Admin screen alone is 1,355
lines across five tabs (tenants, principals, bindings, audit, audit sink). The Sources screen is
653. The Scenarios screen, 1,382, is the console face of the flow-state tier.

### 2.4 What is broken, and where

- The only open issue labelled `bug` is **#537**: space-scoped stubs answer `201`, are node-local,
  and are erased by the next reconcile. It sits in the flow-state and spaces surface, which stays —
  so it is a core bug to fix (PR #541), not a surface to remove.
- **D-68** exists because only the *default* tenant's route table is ever compiled into the
  listener; a tenant's `PUT` is stored, replicated, read back, and never dispatches. Tenancy
  leaked into router correctness, and the fix so far is to say so in the response.
- The audit projection has a documented hole: reads that mutate (scenario reset, journal clear,
  flow-state clear) never become control ops and never reach it
  (`docs/architecture/08-tenancy-security.md`, "a known gap, not an oversight").
- The journal cursor carries a generation it does not act on
  (`docs/architecture/07-verification-plane.md`); a clear mid-walk neither rewinds nor
  re-delivers.
- The flow-state chapter still says no clustered sequencer exists, contradicting **D-47**, which
  is active and shipped. RFC-001 §7.1's supersession banner names a bootstrap flag and an identity
  scheme the control-plane chapter contradicts.

None of these is in membership, replication or routing. Those surfaces passed 57 of 57 live
assertions on the day this was written (§5.1).

### 2.5 What WireMock Cloud shows its users

Checked against `docs.wiremock.io` on 2026-09-04. The product this project measures itself
against exposes, per mock API: a **stubs page** with Import, Export and Record buttons; a
**request log** with unmatched-request diagnosis; **scenarios** with a Reset button; a key-value
**dynamic state** with a Reset button; response templating; chaos; settings. At the organisation
level: teams with Read/Write/Admin per mock API, one API token per user, SSO.

What it does **not** show: any cluster topology, node list or replica count (the only
infrastructure object is a "mock host" a mock API is assigned to); any consistency or replication
control; any in-app audit log (audit events are shipped to S3 on the Enterprise plan and never
browsed in the UI); any state inspector beyond Reset. Its state store is documented as a
best-effort LRU cache with a sequential-SET guarantee — a shared store, not an owner-routed
replicated one.

Import is a button accepting OpenAPI, Swagger, Postman, HAR, Mountebank and WireMock JSON — one
shot. Git integration exists for OpenAPI specs only.

### 2.6 What upstream Rift already provides

Read at `vendor/rift@de0ab0f`:

- **The front door is upstream's** (U-11, `rift-http-proxy::front_door`): the listener, the
  route table compiler and the `RouteObserver`. The cluster contributes replication of the table
  (`ControlOp::PutRoutes`) and its installation on every node.
- **Imposter sources are upstream's** (U-12, `rift-http-proxy::sources`): `file:` and `https:`
  ship there; the cluster's `git+`, `s3:` and `registry:` providers attach through the seam.
- **Flow state is upstream's.** The scenario FSM, the flow KV and `_rift.flowState` live behind
  `rift-mock-core::extensions::flow_state::FlowStore`. The in-memory backend is per node;
  `rift-store-redis` attaches through `FlowStoreBackendFactory` and is selected per imposter with
  `_rift.flowState.backend: "redis"`. A scenario spans nodes with one config key and one Redis.
- **The request journal is upstream's**, per node, with `GET /imposters/:port/requests` and
  `savedRequests` semantics unchanged from Mountebank.
- **The metrics server is upstream's** (`rift-http-proxy`'s `ServerBuilder`).
- **The admin credential is upstream's**: `--api-key` / `MB_APIKEY`.

The cluster re-implemented the journal, the metrics and the credential at fleet scale; that is
surface this RFC removes. It also built a flow-state tier stronger than upstream's Redis backend
(owner-authoritative reads, fencing, durability modes, no external store) — that one stays
(§3.1), because a Redis in the path is exactly the external dependency the fleet exists to avoid.

## 3. The core, defined

**D-71** records the decision. This section is its rationale and its boundary.

### 3.1 In

| Capability | Where it lives today | Why it is the core |
|---|---|---|
| Bootstrap, seeds, join, leave, `departed`, voter floor, promotion sweep | `raft/{node,network,identity}.rs`, `config.rs`, D-21, D-25, D-26, D-27, D-28, D-59 | A fleet that cannot form and heal is not a fleet |
| Raft log and snapshots on `redb` | `raft/store.rs`, D-15, D-16, D-24 | The one source of truth for configuration |
| Replicated imposters, stubs, route table; `op_id` dedup; read-after-write barrier; `Rift-Cluster-Revision` | `control.rs`, `admin_front.rs`, `compose.rs`, D-5 | R1–R4 for configuration |
| The router: replicated route table installed on every node, in-process dispatch to the local imposter, `installed` reported | upstream U-11 + `control.rs` (`routes_installed_for`), D-11, D-54, D-68 | A request reaching any node reaches the right imposter |
| Cluster-wide flow state: owner-authoritative scenarios and flow KV on the HRW ring, fencing, the durable flow shard, the sequencer, proxyOnce claims, spaces | `stores/{flow,shard,flow_config,sequencer,proxy}.rs`, `raft/ring.rs`, `bridge.rs`, D-3, D-7–D-10, D-13, D-17, D-20, D-36, D-40, D-47, D-57, D-63, D-65, D-66 | A stateful mock behaves as one across the fleet, with no external store. **Kept on 2026-09-06 after review** — Rift's own Redis-backed flow store is not the model this project wants for distributed state |
| Admin API and console for imposters and stubs; recording; one-shot OpenAPI import (compile → `PUT /imposters`) | `admin_front.rs`, `crates/rift-cluster-spec`, console Imposters/detail/editor | What a user does with a mock server |
| Readiness and liveness probes, `/_fleet/*`, `/_cluster/*`, the Fleet screen | `probes.rs`, `readiness.rs`, `fleet.rs`, `cluster_api.rs`, D-22, D-61 | Operating the fleet |
| Compose, Helm, image, the chaos tier's membership and replication scenarios | `deploy/`, `tests/cluster-chaos`, D-33, D-35, D-41, D-58 | Shipping and proving the fleet |
| Design-code sync: the register, RFCs, `design-check` | `docs/` | Unchanged |

### 3.2 Out

| Surface | Replaced by | Decisions retired | Issue |
|---|---|---|---|
| Route hits | The request log | D-70 superseded; D-68 amended | #545 |
| Audit log, export loop, sink, retention flag | The Raft log; a `tracing` line at apply | none defined it; RFC-002 §9 retired | #546 |
| MCP server | Nothing, for now | RFC-006 §8 retired | #547 |
| Cluster metric families, observability overlay, dashboards, rule tests | Upstream's metrics server, untouched; `/_fleet/members` for tests | RFC-001 metrics section retired | #548 |
| Tracking sources (`git+`, `s3:`, `registry:`, scheduler, `auth_ref`), datasets, stored specs with drift and validation, the blob store | Upstream `file:`/`https:` sources; stateless `POST /specs/compile` | D-18, D-19, D-23, D-29, D-30, D-31, D-34, D-48, D-49, D-50, D-51, D-52, D-53, D-55, D-56; RFC-005; RFC-004 §3.4–§3.6 | #549 |
| Tenants, principals, bindings, roles, quotas, `X-Rift-Tenant`, the tenant half of every key and of the revision header | One API key; `POST /session` exchanges it | D-44, D-45, D-46, D-68 superseded; RFC-002 superseded; chapter 08 retired — all by **D-73** | #550 |
| Journal shards, merge-on-read, anti-entropy, generation clears, vector cursors, fleet request tail | Upstream per-node journal | D-32, D-37, D-38, D-39; RFC-001 §7.5 | #552 |
| Console: Admin, Sources, Scenarios screens; planned Specs entry | Four screens: Imposters, Requests, Routes, Cluster | RFC-006 §4 amended | #553 |

### 3.3 The trade, stated once

- **Stateful mocks stay cluster-wide.** Scenarios, flow KV, sequencing and proxyOnce keep their
  owner-routed semantics and the "no external datastore" promise stands in full. (v1 of this RFC
  proposed handing this to upstream's Redis-backed store; withdrawn 2026-09-06 — a Redis in the
  path is the dependency the fleet exists to avoid, and the cluster's tier is the stronger one.)
- **Verification is per node.** `GET /imposters/:port/requests` answers for the node you reached.
  A test that needs fleet-wide verification pins a node or reads all of them. If Rift ever grows a
  shared journal, it grows it in the engine, once, for every deployment shape.
- **One credential.** Everyone who can administer the fleet can administer all of it. Isolation
  between teams is a deployment (two fleets), not a feature. **Landed by #550 (D-73):**
  `--api-key` set closes the whole admin plane, unset leaves it open, and `POST /session`
  exchanges the key for the console's cookie. Nothing in the fleet is tenant-scoped — not a redb
  key, not a `ControlOp`, not the `Rift-Cluster-Revision` header, not a flow-id namespace. The
  trade is stated in full there, including what it costs: a keylogging XSS at login time takes
  the fleet's credential and there is no narrower key to mint instead (RFC-006 §9.3).

These are losses. They are accepted because each of the removed subsystems was a second
distributed system riding inside the first, and the first one — configuration, routing, and the
state a mock needs to be one mock — is the product.

## 4. Removal plan

Ten issues under epic **#544**, in landing order (an eleventh, #551, was withdrawn on 2026-09-06 —
see §3.1). 1–5 are independent. 6 needs 2 and 5 (audit rows and datasets are per tenant) and drops
the tenant component of `ContextScope`. 8 is simpler after 6. 9 needs 6 and 8. 10 closes the
epic. 0 lands first so every later child has an in-repo before/after check.

| Order | Issue | Surface | Depends on |
|---|---|---|---|
| 0 | #555 | In-repo core smoke check (from the vault harness) | — |
| 1 | #545 | Route hits | — |
| 2 | #546 | Audit log, export, sink | — |
| 3 | #547 | MCP server | — |
| 4 | #548 | Cluster metrics, observability overlay | — |
| 5 | #549 | Tracking sources, datasets, stored specs, blob store | — |
| 6 | #550 | Tenancy, RBAC, principals | #546, #549 |
| 7 | ~~#551~~ | ~~Clustered flow state~~ — withdrawn, stays | — |
| 8 | #552 | Fleet journal merge | (#550) |
| 9 | #553 | Console trimmed to four screens | #550, #552 |
| 10 | #554 | Design docs retired; the router named | all |

Each child PR carries `Design: amended — D-71` and marks its retired decisions `superseded` with
`Superseded by: D-71` in the same PR, so `design-check --strict` stays green at every step and the
register never describes code that is gone.

## 5. Verification protocol

The instruction this work runs under: *before committing to a removal, run the feature being
removed and the features being kept.* Concretely:

### 5.1 Baseline

On 2026-09-06 the live three-node fleet (compose, built from `master@5c8dbfb` on 2026-09-01)
was re-seeded and checked with the vault harness
(`~/Documents/remote-vault/tasks/rift-enterprise/run-locally/{seed,check}.sh`): **57 passed,
0 failed**. The sections, by fate:

| Section | Assertions | Fate |
|---|---|---|
| cluster — one leader, three voters, same applied index, console on every node, no imposter port published | 8 | **keep** |
| imposter through the front door on all three nodes; predicates; 401/200/404 | 8 | **keep** (router) |
| proxy — proxyOnce replays on another node; proxyTransparent; generated predicate | 4 | **keep** |
| front door — host, prefix, strip, priority, method, proxy target, replicated to node 3, ~~`installed`~~ | 10 | **keep**; the tenant-table assertion and `installed` both **went** (#550, D-73) |
| scenarios — state crossed nodes | 2 | **keep** (added to the smoke check) |
| spaces — node-local space stubs, erased on reconcile, flow KV across nodes | 5 | **keep**; the node-local assertions describe bug #537, fixed by PR #541 |
| tenancy — cross-tenant 404, header borrowing refused, data plane not isolated | 5 | **went** (#550); the data-plane half survives as `the_gateway_stays_open_and_never_carries_the_admin_key` |
| console session — bootstrap key refused, minted principal accepted, cookie on node 3 | 6 | **went** (#550); replaced by `the_api_key_mints_a_session_and_the_cookie_is_accepted_on_every_node` |
| RBAC ladder | 6 | **went** (#550) |
| observability — journal records the serving node; audit has entries; one proxyOnce claim | 3 | journal per node **keep**; audit **goes** (#546); claim **keep** |

### 5.2 Per child

1. **Before:** drive the surface being removed on the live fleet and record what it did — the
   harness section named in the issue. This is the evidence that a working thing was removed on
   purpose, not a broken one by accident.
2. **After:** the removed routes answer `404` (or the flag is rejected at startup); every kept
   section still passes — the cluster forms, imposters replicate, the router dispatches on all
   three nodes, a stopped node catches up, a stopped leader is replaced, a new node joins from a
   seed.
3. `scripts/design-check.py --strict` passes with the retired decisions marked and the RFC
   callouts in place.
4. Whole-crate suites for every crate whose surface moved — not a hand-picked subset.

The harness itself moves into the repo as #555 so the protocol does not depend on a laptop.

## 6. Naming: the router

The feature has been called the "front door" since upstream issue #19. In the reduced system it
is the cluster's only data-plane contribution and the word should say what it does. Docs,
console labels and CLI help call it the **router**. The upstream module
(`rift-http-proxy::front_door`) is not ours to rename, and the wire path `/front-door/routes` is
left alone in this pass — renaming a path every client has to follow is its own decision, to be
taken once the API has stopped shrinking (#554).

## 7. Explicit non-goals

- **Sharding.** The fleet stays replicated: every node binds every imposter and serves every
  request locally. D-20 stands unchanged: only a flow has an owner; imposters, stubs and config
  own nothing.
- **Re-adding any removed surface behind a feature flag.** A flag keeps the code, the tests, the
  decisions and the maintenance. Removed means removed; it can be re-proposed as a new RFC.
- **Changing the upstream repository.** Everything here is subtraction on this side of the
  vendor boundary. Where upstream has a gap this exposes (a shared journal, a shared proxy store),
  it is filed upstream, not patched here.
- **Performance work.** The standing gate — an unclustered node is indistinguishable from the
  open-source binary (D-33) — stays and is easier to hold with less code in the path.

## 8. Open questions

1. **Should `POST /session` survive?** With one API key the console could send the key on every
   request. A cookie keeps the key out of the browser's storage after login; that is the only
   reason to keep the exchange. Default: keep it, simplified (#550).
   **Resolved by #550 (D-73):** kept, simplified. The default was taken, and a second reason
   emerged for it — rotating the signing key is the *only* revocation a one-credential fleet has,
   so dropping the cookie would have left none. The payload's subject is now the constant
   `"admin"` and `verify` answers `Result<(), _>`; a session proves authentication and resolves
   to no identity.
2. **Does the Requests screen offer a node picker or only show the node it reached?** Default:
   show the node it reached and say so (#553). A picker is a later addition if anyone asks.
3. **Is `Rift-Cluster-Partial` still needed anywhere?** Fleet reads that fan out (`/_fleet/members`)
   still are partial when a peer is down. Default: keep it on those reads only (#552).
4. **Should the OpenAPI compile endpoint live on the server at all, or only in the console?** A
   server endpoint lets curl users import too. Default: server endpoint, stateless (#549).
   **Resolved by #549 (D-72):** the server keeps a stateless `POST /specs/compile` that compiles
   a submitted OpenAPI document and answers the imposter JSON plus its operation index, retaining
   nothing — the caller writes the result with an ordinary `PUT /imposters`.

## 9. Progress

Updated as PRs merge. Status is one of `open`, `in progress`, `merged`.

| Issue | Surface | Status | PR |
|---|---|---|---|
| #544 | Epic | open | — |
| #555 | In-repo core smoke check | open | — |
| #545 | Route hits | in progress | — |
| #546 | Audit | open | — |
| #547 | MCP | open | — |
| #548 | Metrics and observability | open | — |
| #549 | Sources, datasets, specs, blobs | in progress | — |
| #550 | Tenancy and RBAC | in progress | — |
| ~~#551~~ | Flow state, sequencer, proxyOnce, spaces | withdrawn 2026-09-06 — stays | — |
| #552 | Journal merge | open | — |
| #553 | Console | open | — |
| #554 | Docs and naming | open | — |

Closed as out of scope on 2026-09-06, with the reason on each: #148, #149, #151, #279, #280,
#282, #283, #284, #289, #291, #294, #380, #448, #456, #457. Rescoped: #394.

## Appendix A — every decision, by fate

**Keep (unchanged):** D-3, D-4, D-5, D-6, D-7, D-8, D-9, D-10, D-11, D-13, D-14, D-15, D-16,
D-17, D-20, D-21, D-22, D-24, D-25, D-26, D-27, D-28, D-33, D-35, D-36, D-40, D-41, D-42, D-43,
D-47, D-54, D-57, D-58, D-59, D-60, D-61, D-62, D-63, D-64, D-65, D-66, D-67.

**Amend:** ~~D-68 (`installed` from the route-table endpoints only)~~ — **superseded** by D-73
(#550) instead: with one fleet-wide route table every stored route is installed, so `installed`
would be a constant `true` and is removed from both endpoints.

**Supersede, by the child that removes the code:**

| Child | Decisions |
|---|---|
| #545 | D-70 |
| #549 | D-18, D-19, D-23, D-29, D-30, D-31, D-34, D-48, D-49, D-50, D-51, D-52, D-53, D-55, D-56 |
| #550 | D-44, D-45, D-46, D-68 |
| #552 | D-32, D-37, D-38, D-39 |

Already superseded before this RFC: D-1, D-2, D-12. D-69 (space stubs replicate, #541) landed after v1 and is unaffected.
