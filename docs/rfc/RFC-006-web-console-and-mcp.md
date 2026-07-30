# RFC-006 — Web Console and MCP Server (v1)

| | |
|---|---|
| **Status** | v1 — design draft for review |
| **Tracking issue** | [achird-labs/rift-cluster#150](https://github.com/achird-labs/rift-cluster/issues/150) (console, M6a) · [#151](https://github.com/achird-labs/rift-cluster/issues/151) (MCP, M6b) |
| **Canonical location** | `rift-cluster:docs/rfc/RFC-006-web-console-and-mcp.md` |
| **Depends on** | **RFC-002** (principals, roles, API keys — the console's auth substrate); **ADR-001 / #14** (the control plane every write lands in). References, without depending on for v1: RFC-003 (parity umbrella, sibling in review), RFC-004 (spec-driven mocking), RFC-005 (data sources & state), `docs/architecture/07-verification-plane.md`, `docs/architecture/13-front-door-and-sources.md` |
| **Ground truth** | verified at `rift-cluster@5b98fef`, `vendor/rift@v0.16.0-4-g97757f0` |
| **Author** | Mohsen Zainalpour |
| **Date** | 2026-07-26 |

---

## 1. Summary

RiftCluster is administered today by curl, SDKs, and a terminal UI that ships with
the core engine. That is the right *foundation* — everything is an API — but it
is not the whole product. WireMock Cloud's most visible surfaces (verified
2026-07-26 against their public product pages) are a web stub editor with a
matcher UI, a live request log with match diagnostics, org/RBAC admin screens,
and — their 2026 headline — a native MCP server so coding agents create and
update mocks directly.

This RFC adds both, as **clients of the existing admin API**:

1. **A web console** — a React/TypeScript SPA embedded in the `rift-cluster-server`
   binary at build time and served at `/console` from the admin front. No node
   process at runtime, no CDN, works air-gapped.
2. **An MCP server** — `rift-cluster-server mcp`, a stdio MCP endpoint wrapping the
   clustered admin API with a scoped API key, so an agent's imposter writes go
   through the same park/replay/If-Match write path as everyone else's.

Neither surface gets a private API. The console and the MCP server call the
same routes curl calls, authenticated as RFC-002 principals, attributed in the
same audit stream. Where the console needs something the API does not have
(a session exchange, a fleet-status read on the admin port), the addition is a
general API feature that any client can use — that rule is load-bearing and
restated in §3.

## 2. Why — the verified gap

Checked at `5b98fef`, not assumed:

- **There is no UI.** The EE binary serves the admin front
  (`crates/rift-cluster-server/src/admin_front.rs`), the cluster operator surface
  (`crates/rift-cluster-server/src/cluster_api.rs`), and probes
  (`crates/rift-cluster-server/src/probes.rs`). No route serves HTML. The only
  interactive surface in the workspace is upstream's terminal UI
  (`vendor/rift/crates/rift-tui`) — useful precedent (§4), but SSH-only,
  single-node, and invisible to a browser.
- **The admin API is browser-hostile as authenticated today.** The front holds
  one optional static bearer (`FrontConfig::api_key: Option<String>`,
  `admin_front.rs:99-101`) checked by whole-header constant-time comparison
  (`authorize`, `admin_front.rs:453-465`). A browser would have to keep that
  key in JS-reachable storage to send it — an XSS away from leaking the fleet
  credential. §5.3 designs around this.
- **The fleet view is on the wrong port for a browser.** `/_cluster/members`,
  `/_cluster/config`, `/_cluster/imposters`, `/_cluster/health`,
  `/_cluster/ops/:id` exist (`cluster_api.rs:63-191`) but ride the cluster
  port under the cluster credential (`cluster_api.rs:1-8`) — a node-to-node
  secret that must never reach a browser. Probes are unauthenticated but
  per-node and answer only liveness/readiness (`probes.rs:1-6,140,155`).
- **The building blocks for a stub editor already exist and are proven.**
  `rift-lint` is a pure library — `lint_json`, `lint_value`
  (`vendor/rift/crates/rift-lint/src/lib.rs:128,153`), `validate_stub`
  (`src/validator.rs:538`) — and the TUI already validates editor buffers
  with exactly those calls (`rift-tui/src/validation.rs:6,12-30`). The console
  reuses the same library, not a reimplementation.
- **Realtime push exists upstream.** `GET /events` and
  `GET /imposters/{port}/savedRequests/stream` are SSE
  (`vendor/rift/crates/rift-http-proxy/src/admin_api/handlers/events.rs:1-52`),
  and the front proxies streaming bodies without buffering
  (`admin_front.rs:490-493` — "buffering here would break the admin SSE
  streams"). The console does not need a new push channel; it needs to decide
  how much of this one to use in v1 (§6).
- **The agent story is zero.** Nothing in the workspace speaks MCP. Meanwhile
  the write path an agent would want — idempotency keys, `If-Match`
  preconditions, park-then-replay durability, `202 + opId` async mode — is
  already built (`admin_front.rs:24-48,553-569,707-765`). The MCP server is
  cheap because it is a thin client of that.

## 3. Design

Three rules, in priority order:

1. **The binary is the deployment.** The console is static assets compiled
   into `rift-cluster-server`; the MCP server is a subcommand of it. A customer who
   has the binary has the whole product. No node runtime, no separate web
   service, no CDN fetch — the SPA's CSP can be `default-src 'self'` because
   nothing legitimate ever leaves the origin (§9.1).
2. **Nothing UI-only.** The console is a client of the public admin API. Every
   endpoint added by this RFC (§5.2, §5.3) is specified in the published
   OpenAPI schema and usable from curl. If a screen needs data the API cannot
   give, the fix is an API feature, and it goes through review as one.
3. **The API is the security boundary; the UI is a convenience.** The console
   hides buttons a role cannot use, but hiding is UX. Every enforcement
   decision is RFC-002's, made server-side per request. The MCP tool list
   adapts to the session's role the same way, with the same caveat (§8.4).

## 4. Information architecture

Tenant-scoped, one tenant in view at a time. A tenant switcher (top-level,
persisted per browser) sets the `X-Rift-Tenant` header on every API call for
multi-tenant principals — RFC-002 §8.1's rules apply unchanged; the console
adds no header logic of its own beyond sending the selection.

| Screen | Backend | Availability |
|---|---|---|
| **Imposters** — list, per-imposter detail, enable/disable | `GET/POST/DELETE /imposters*` (terminated routes, `admin_front.rs:348-398`; reads proxied to the engine) | v1 |
| **Stub editor** — form ⟷ raw JSON, monaco, lint-on-save | stub CRUD incl. by-id routes (`admin_front.rs:378-395`); lint via `rift-lint` in-browser (§4.1) | v1 |
| **Request log** — recorded requests, match diagnostics | v1: `GET /imposters/:port/requests` per node, labelled **per-node view**; converges to the doc-07 merged journal when the verification plane ships, same screen, no redesign | v1 (degraded) |
| **Cluster** — members, leader, ring epoch, readiness, pending ops | §5.2's admin-port fleet reads (projection of `cluster_api.rs:63-191`) | v1 |
| **Front-door routes** — table editor | `GET/PUT /front-door/routes`, `DELETE /front-door/routes/:id` (`admin_front.rs:348-360,405,419-438`) | v1 |
| **Tenants / principals / roles / tokens / audit** | RFC-002 §5 admin surface + `GET /admin/audit` | with RFC-002 T3/T4 |
| **Scenarios & flow state** — inspect, reset | RFC-005 state API | with RFC-005 |
| **Sources** — imposter sources status (#20) | doc-13 sources admin surface | with #20 |
| **Specs** — imported OpenAPI/proto specs | RFC-004 admin surface | with RFC-004 |

Screens whose backend has not shipped render as a named, greyed nav entry with
a link to the tracking issue — visible roadmap, not a 404.

The TUI (`rift-tui/src/api.rs:193-505`, `ui/`, `app/`) is the interaction
precedent, deliberately: its `ApiClient` method set (list/get/create/delete
imposter, stub CRUD by index, enable/disable, clear requests, export) is the
minimum viable verb set a UI needs, and its editor-validates-before-submit
loop (`validation.rs`) is the loop the console reproduces. Where the TUI
edits stubs **by index**, the console prefers the **by-id** routes the front
added (`admin_front.rs:383-387`) — index edits are the documented lost-update
window (`admin_front.rs:27-44`), and a UI holding stale state open for minutes
is the worst possible client for them.

### 4.1 Editor validation

JSON is canonical; the form view is a projection of it, and round-trips
losslessly or refuses to open (unknown keys put the editor in raw-only mode
with a banner, never silently dropped — the config the user saves is the
config they wrote). On save: `rift-lint` runs **in the browser**, compiled to
`wasm32-unknown-unknown`. This adds no API surface and works offline; the
server's own validation (`validate_stub`/`validate_stubs`, already enforced on
terminated writes — `admin_front.rs:66-70`) remains the authority, so a wasm
gap is a cosmetic gap, not a correctness one. `rift-lint`'s JS-syntax check is
already feature-gated with a no-op fallback (`validator.rs:53,86`), which is
the wasm-unfriendly part pre-neutralized — but the full dependency tree
compiling to wasm is **to-verify** (§12 Q1); the fallback is calling a
dry-run lint endpoint, which would then be added to the schema as a general
API feature per rule 2.

## 5. API contract

### 5.1 OpenAPI schema — dogfooding, then generating

The EE admin API gets a **committed, hand-authored** OpenAPI 3.1 document at
`docs/api/openapi-ee.yaml`, served by the binary at `GET /openapi.json`.
Hand-authored because the front's router is hand-rolled hyper
(`classify()`, `admin_front.rs:348-398`), not a framework with derive-based
schema extraction — annotation tooling (utoipa et al.) assumes axum/actix
shapes we do not have. The schema is kept honest mechanically, not by
discipline: a golden test enumerates `classify()`'s route table plus the §5.2
additions and fails when the YAML's path set drifts from it.

Every operation carries `x-rift-origin: ee | upstream` — the schema documents
the *whole* surface a client sees through the front (terminated + proxied),
and the marker records which side owns each contract. It also documents the
cluster headers (`Rift-Cluster-Revision`, op id, warnings —
`admin_front.rs:64,895-920`) and the `If-Match` / `Idempotency-Key` request
semantics, which until now live only in code comments.

The console's TypeScript client is **generated** from this schema
(`openapi-typescript` + a thin fetch wrapper; exact tool pinned in `web/`,
version to-verify) in CI, and the generated output is committed so `web/`
builds without the binary present. The MCP server's tool input schemas derive
from the same document (§8.2). One contract, three consumers — drift between
them becomes a CI failure.

### 5.2 Fleet reads on the admin port

The console cannot hold the cluster secret (§2), so the front terminates a
read-only projection of the operator surface on the admin port:

```
GET /_fleet/members     → cluster_api.rs "members" body, verbatim shape
GET /_fleet/health      → cluster_api.rs "health" body (ready, ring m_idx/members, isolated)
GET /_fleet/ops/:id     → op status: applied | failed | pending (poll target for 202/parked writes)
```

Same JSON shapes as `cluster_api.rs:115-190` — one projection, not a second
report. Authorization: the admin credential today; `ClusterAdmin` (fleet) or
in-tenant `Viewer` for a tenant-filtered subset once RFC-002 lands — the
precise split is settled in the RFC-002 T2 review, noted in §12 Q3.
`/_cluster/*` on the cluster port is unchanged; node-vs-node comparison
(its stated purpose, `cluster_api.rs:5-8`) still requires asking each node.

### 5.3 Sessions — how a browser holds a credential

The API keeps accepting `Authorization` bearers unchanged (curl, SDKs, MCP).
Browsers get a **session exchange**:

```
POST   /session        body: {apiKey}  → Set-Cookie: rift_session=…; HttpOnly; Secure; SameSite=Strict; Max-Age=28800
DELETE /session        → cookie cleared
GET    /admin/whoami   → identity + bindings (RFC-002 §5; pre-RFC-002: synthetic single principal)
```

The user pastes an API key once, at login. The server verifies it (today:
the static-key comparison; post-RFC-002: argon2id lookup), then mints a
session token the key never rides again — the cookie is `HttpOnly`, so no
script in the page, injected or otherwise, can read either the key or the
session (§9).

**Token shape: stateless, fleet-verifiable.** The token is
`{principal_id, issued_at, expiry}` HMAC-signed with a **session-signing key**
minted once by the leader and stored as a control-plane record — every node
verifies without coordination, and a login is not a Raft write. Revocation is
honest about its bounds: the cookie only proves *authentication*; every
request still resolves the principal and its bindings from local applied
state, so disabling a principal or deleting a binding cuts access with
RFC-002 §3.1's committed-or-not semantics. The 8-hour `Max-Age` bounds only
the window in which a *stolen cookie* outlives its theft, and rotating the
signing key (a `FleetAdmin` control-plane write) invalidates every session at
once. What v1 does not have is per-session server-side revocation — stated in
§10, not hidden.

**CSRF.** `SameSite=Strict` plus a double-submit custom header: the SPA sends
`X-Rift-CSRF: 1` on every state-changing call, and the front rejects
cookie-authenticated mutations without it. Cross-origin HTML cannot set
custom headers without a CORS preflight, and the front answers no permissive
CORS. Bearer-authenticated requests are exempt — a bearer cannot be attached
by a victim's browser, which is the whole attack.

## 6. Realtime strategy

**v1 is polling, on purpose.** TanStack Query refetches per screen: imposter
list and cluster view at 5s while visible, the request-log screen at 2s,
paused on hidden tabs. That is ~1 req/s/screen against reads that are local
state or a loopback proxy hop — noise. Polling needs no reconnect logic, no
gap-repair protocol, and degrades to nothing when the tab sleeps.

**v2 uses the SSE that already exists, for invalidation only.** The console
subscribes to `GET /events` (proxied through the front unbuffered,
`admin_front.rs:490-493`) and maps event kinds to query-cache invalidations —
an imposter-lifecycle event invalidates the imposter list; a recorded-request
event invalidates that port's log query. Events trigger refetch; they never
*carry* the data, so a `lagged` drop (the stream's documented backpressure
mode, `events.rs:4-6`) costs one poll interval of staleness, not correctness.
Fleet-wide push waits for the verification plane's fan-in (doc-07); one
node's `/events` shows one node's traffic, and the v1 request-log screen says
so on screen (§4).

Not designed here, deliberately: WebSockets, server push of diffs, CRDT-style
live cursors. Nothing in §4 needs them.

## 7. Embedding & build pipeline

The sharpest build-system decision in this RFC, so it gets its options table:

| Option | `cargo build` needs node? | Air-gapped binary? | Rejected because |
|---|---|---|---|
| A. build.rs runs pnpm when present | sometimes (worst answer) | yes | non-hermetic; "works on my machine" as a build system |
| B. commit `web/dist/` to git | no | yes | generated artifacts in review diffs; drift between `web/src` and committed dist is invisible until runtime |
| C. separate web service / container | no | **no** | violates invariant 1 outright |
| **D. feature-gated embed of a prebuilt dist** | **no** | **yes** | — |

**Decision: D.** A `console` cargo feature (default **off**) gates a
`rust-embed` embed of `web/dist/`:

- `cargo build` / `cargo test` in every dev and CI lane: feature off, `web/`
  never touched, node never required. The `--cluster-off` parity lanes (#139)
  are untouched by construction.
- Release CI only: `pnpm install --frozen-lockfile && pnpm build` in `web/`,
  then `cargo build --release --features console`. The feature with no
  `web/dist` present fails **at compile time** with rust-embed's missing-folder
  error — a release cannot silently ship consoleless.
- `rust-embed` over `include_dir` for one reason: its debug-mode
  loads-from-disk behavior gives `cargo run --features console` live asset
  reload during console development without a rebuild. (Crate version pinned
  at implementation; to-verify.)

Serving: the front's `handle()` (`admin_front.rs:400-412`) gains a
`GET /console` / `GET /console/*` arm ahead of `classify()` — static assets
with content-type by extension, SPA-fallback to `index.html` for pathless
routes, `Cache-Control: max-age=31536000, immutable` for hashed assets and
`no-cache` for `index.html`. Feature off ⇒ the arm does not exist and
`/console` proxies upstream and 404s exactly as today — zero new code on the
parity path.

Dev loop: `pnpm dev` runs Vite with a proxy target of a local cluster's admin
front, so console work needs no Rust rebuild at all; the generated client
(§5.1) keeps the two sides honest.

## 8. MCP server design

### 8.1 Shape: a subcommand, not a second binary

**`rift-cluster-server mcp` speaking stdio.** Rejected alternative: a standalone
`rift-mcp` crate/binary — a second artifact to version, sign, and distribute,
for zero capability gain, against a CLI that is a flat clap parser today
(`EeCli`, `crates/rift-cluster-server/src/cli.rs:165-173`) where an optional
subcommand is a purely additive change. The MCP process is a **client** of
the admin front over HTTP — it holds no node state, embeds no engine, and can
run on a laptop against a remote fleet:

```
rift-cluster-server mcp --url https://fleet.example:2525 --api-key-file ~/.rift/agent.key
```

Implementation lives in `crates/rift-cluster-server/src/mcp/`, on the official
Rust MCP SDK — **rmcp** (crate name and version to-verify at implementation;
pinned in the slice-M1 PR). stdio transport only in v1: it is what every
coding agent launches today, it inherits the parent process's environment for
credential delivery, and it opens no listening port to threat-model.

### 8.2 Tools

Tool input schemas derive from the §5.1 OpenAPI document — the MCP surface is
the admin API re-projected, not re-specified. v1 set:

| Tool | Wraps | Notes |
|---|---|---|
| `imposter_list` / `imposter_get` | `GET /imposters[/:port]` | |
| `imposter_create` / `imposter_delete` | `POST/DELETE /imposters[/:port]` | explicit port required, as the front already enforces (`admin_front.rs:938-944`) |
| `imposter_set_enabled` | `POST /imposters/:port/{enable,disable}` | |
| `stub_add` / `stub_replace` / `stub_delete` | by-id stub routes | by-id only — agents must not inherit the index-edit lost-update window (§4) |
| `routes_get` / `routes_put` / `route_delete` | `/front-door/routes*` | |
| `requests_query` | `GET /imposters/:port/requests` (+ doc-07 params when it ships) | v1 answer carries `"scope": "node"` until the merged journal exists |
| `verify` | request-count/match assertions over the same read | |
| `fleet_health` / `op_status` | §5.2 `/_fleet/*` | `op_status` closes the async-write loop below |
| `lint` | in-process `rift_lint::lint_json` | dry-run; no network, no side effects |
| `spec_import` | RFC-004 surface | ships with RFC-004, listed for shape |
| `state_inspect` / `state_reset` | RFC-005 surface | ships with RFC-005, listed for shape |

**Write semantics surface as tool behavior, not as prose in a description.**
Every write tool sends an `Idempotency-Key` derived deterministically from the
MCP tool-call id, so an agent retrying a timed-out call dedups instead of
double-applying (`base_op_id`, `admin_front.rs:1293-1302`). Mutating tools
accept an optional `expected_revision` and pass it as `If-Match`; a `409`
comes back as a structured tool error — `{conflict: true, current_revision}` —
telling the agent to re-read and rebase, which is precisely the loop the
header was built for (`admin_front.rs:24-48`). A `503`-parked or `202` answer
returns `{parked: true, op_id}` and points at `op_status` — the agent learns
the fleet's durability model by using it.

### 8.3 Credential scope

The MCP process authenticates with **one API key = one RFC-002 principal**,
and is nothing more than that principal. The recommended setup is a dedicated
`agent` principal bound `Editor` in exactly the tenants the agent should
touch — never a `FleetAdmin` key in an agent's environment, and the docs say
so in those words. Every MCP-originated write is attributed in the audit
stream (RFC-002 §9) as that principal, indistinguishable in mechanism from a
human's curl — which is the point: no special agent pathway to audit
separately, and revoking the agent is deleting one binding.

### 8.4 Role-adaptive tool list

On startup the server calls `GET /admin/whoami` and registers only the tools
the principal's role can ever succeed at: a `Viewer` key yields the read and
`verify`/`lint` tools only; `Editor` adds the write set; route-table and
fleet tools follow their §5.2/RFC-002 gating. Fewer dead-end tools means less
agent flailing — but this is UX, not security: the API re-checks every call
(§3 rule 3), so a stale tool list after a role change degrades to a clean
`403` tool error, never to an unauthorized write. Pre-RFC-002 (single static
key) the full list registers, honestly reflecting that today's key is
all-or-nothing.

## 9. Threat model

### 9.1 XSS

The console renders attacker-influenced data constantly — stub bodies,
recorded request payloads, imposter names. Defenses, in depth order: React's
default escaping with `dangerouslySetInnerHTML` banned by lint; recorded
payloads rendered as text in monaco/`<pre>`, never interpreted as HTML; and a
strict CSP delivered on every console response —
`default-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'`
— which the all-embedded design makes *actually enforceable*: no CDN, no
inline scripts, monaco bundled. Even a successful injection cannot read the
credential (HttpOnly cookie, §5.3) or exfiltrate to a foreign origin
(`connect-src 'self'`); its blast radius is same-origin API calls for the
life of the open tab — real, bounded, and audited as the victim principal.

### 9.2 CSRF and session theft

Covered in §5.3: `SameSite=Strict` + custom-header double-submit for
cookie-authenticated mutations; no permissive CORS; `Secure` cookie so the
session never crosses plaintext HTTP. Session fixation is not applicable —
the server only ever mints the cookie itself at `POST /session`, never adopts
a client-presented one. Residual risk: no per-session revocation in v1
(§10); the bound is `Max-Age` + signing-key rotation + principal disable.

### 9.3 The login form holds the real key, briefly

`POST /session` is the one moment the long-lived API key transits the page.
It is held in component state only (never localStorage, never a URL), sent
once over TLS, and dropped. A keylogging XSS *at login time* could take it —
that is irreducible for any paste-a-secret flow and is why the docs
recommend minting a console-specific key per RFC-002 (shown once, scoped,
individually revocable) rather than pasting a fleet-admin key.

### 9.4 MCP credential scope

The stdio transport means the key lives in the agent-host process
environment or a key file — outside this system's control, which is exactly
why §8.3 insists the key be a narrowly-bound principal: the threat model
assumes the agent host *will* eventually leak it, and makes the loss a
one-tenant `Editor`, not the fleet. `--api-key-file` is preferred over an
env var in all documentation (env vars leak into crash dumps, `/proc`, and
child processes). The MCP server never logs the key, and its tool errors
relay the API's error bodies — which never echo credentials — verbatim.

### 9.5 The fleet-read projection

§5.2 moves cluster topology (node ids, ring membership, readiness) from a
secret-gated port to the admin port. That is new exposure and is priced in:
it is read-only, it names infrastructure but no tenant data, and it sits
behind admin auth today and `ClusterAdmin`/scoped-viewer rules under
RFC-002. The cluster *secret* and the write/RPC surface stay where they are.

## 10. Explicit non-goals

- **No standalone web service, ever, in any phase.** Closed door, not a
  deferral.
- **No UI-only API.** Restated from §3 because it is the invariant most
  eroded by convenient exceptions.
- **No OIDC / SSO login in v1.** The session exchange takes an API key;
  OIDC arrives with RFC-002 v2's `AuthSource::Oidc` and slots in as a second
  way to mint the same session cookie.
- **No per-session server-side revocation in v1** (§5.3). Bounds: TTL,
  signing-key rotation, principal disable.
- **No collaborative editing** — no presence, no locks beyond `If-Match`.
  Two humans editing one imposter get the same 409-and-rebase a lagging
  agent gets.
- **No mobile layout.** Desktop-width operator tooling.
- **No MCP HTTP/SSE transport in v1** — stdio only; a network-listening MCP
  endpoint is a new authenticated surface and waits for demand.
- **No "Agent Skills" packaging** (WireMock's companion feature): tool
  descriptions in v1 carry the workflow guidance; a skills bundle is a
  docs artifact we can ship later without design work here.

## 11. Phasing

Console slices `feat(console): …`, MCP slices `feat(mcp): …`, ~1 PR each.
C1–C3 are strictly ordered; C4+ and M1+ parallelize.

| Slice | Contents | Exit criteria |
|---|---|---|
| **C1 — contract** | `docs/api/openapi-ee.yaml`, `GET /openapi.json`, golden route-parity test against `classify()` + §5.2 | Schema path set == served route set, enforced in CI |
| **C2 — fleet reads + sessions** | `/_fleet/{members,health,ops/:id}`; `POST/DELETE /session`, signing-key control-plane record, CSRF check | curl can log in, hold a cookie, read fleet health; bearer path byte-identical to before |
| **C3 — embed pipeline** | `web/` scaffold (Vite, React, TanStack Query, generated client), `console` feature, rust-embed serving, release-CI wiring | `cargo build` with no node succeeds feature-off; release binary serves `/console` air-gapped; parity lanes untouched |
| **C4 — imposters read-only + cluster screen** | list/detail, enable/disable, fleet view on §5.2 | Every displayed field traceable to a schema'd endpoint |
| **C5 — editors** | monaco stub/imposter editors, form ⟷ JSON, wasm rift-lint on save, by-id writes with `If-Match` + 409 rebase UX | Lost-update test: two tabs, second save gets a visible conflict, no silent clobber |
| **C6 — request log + route table** | per-node request log (labelled), front-door route editor | Degraded-mode label verified; route CRUD round-trips against `admin_front.rs` routes |
| **C7 — admin screens** | tenants/principals/tokens/audit UI over RFC-002 §5 | Gated on RFC-002 T3/T4 shipping; key shown once in UI, exactly as the API behaves |
| **M1 — MCP scaffold** | `mcp` subcommand, rmcp pinned, read tools + `lint`, `--api-key-file` | An agent lists imposters against a live cluster via stdio |
| **M2 — MCP writes** | write tools with idempotency-from-call-id, `expected_revision`/409 shape, `{parked, op_id}` + `op_status` | Retry-after-timeout dedups (one commit); conflict returns structured rebase error |
| **M3 — MCP scoping** | `whoami`-driven tool registration, audit attribution verified end-to-end | Viewer key exposes no write tools; MCP write appears in audit as its principal |
| **v2 (follow-ups, not this RFC)** | SSE cache invalidation (§6); scenario/state screens (RFC-005); specs screen (RFC-004); merged-journal request log (doc-07); OIDC session minting; MCP HTTP transport | — |

## 12. Open questions

1. **Does `rift-lint` compile to `wasm32-unknown-unknown`?** The JS-syntax
   validator is already feature-gated with a no-op fallback
   (`validator.rs:53,86`), which removes the likely blocker, but the full
   dependency tree is unverified. Decides whether C5 bundles wasm lint or
   falls back to a dry-run lint endpoint added to the schema.
2. **Form-view coverage.** Which predicate/behavior shapes get first-class
   form controls versus raw-JSON-only? Proposal: the shapes `rift-tui`'s
   dialogs already model, then demand-driven. Decide in C5 review with real
   configs.
3. **Fleet-read authorization split** (§5.2): under RFC-002, is
   `/_fleet/health` in-tenant-`Viewer`-visible (tenant-filtered) or
   `ClusterAdmin`-only? Topology is infrastructure, not tenant data — but
   "which nodes exist" may itself be sensitive in some shops. Settle in the
   RFC-002 T2 review, where the enforcement matrix lives.
4. **Session-signing-key rotation cadence** — operator-triggered only, or
   scheduled? Scheduled rotation logs everyone out on a timer; v1 leans
   operator-triggered with a documented runbook.
5. **rmcp maturity.** If the official SDK's stdio server support is not
   release-grade at M1 time, the fallback is implementing the (small) stdio
   framing directly; the tool layer above it is transport-agnostic either way.
   Verify at M1.

---

## Appendix A — what this RFC does not change

- The vendored engine. No `vendor/rift` patches; the console and MCP consume
  upstream routes through the front exactly as curl does.
- The terminated write path — `If-Match`, idempotency, park/replay
  (`admin_front.rs`) are consumed, not modified.
- `/_cluster/*` on the cluster port, its credential, and its
  compare-two-nodes purpose (`cluster_api.rs`).
- The probe listener (`probes.rs`) — stays unauthenticated, per-node, and
  console-independent.
- The `--cluster-off` parity bar (#37/#139): feature-off builds add zero code
  to that path.
