# `web/` — the Rift console

The single-page app the cluster binary serves at `GET /console`. Vite + React +
TypeScript + TanStack Query, with a TypeScript client generated from the
published OpenAPI contract.

C3 (#186) delivered the pipeline; **C4 (#187) delivers the first screens** — the
app shell, the tenant switcher, a read-only imposter list and detail with
enable/disable, and the cluster/fleet view. C5–C7 (#188–#190) follow.

## Layout

```
src/api/       schema.ts (generated, committed) · client.ts (the only fetch) · paths.ts
src/app/       Shell · session · rbac · nav · routing · queries · contract · fleetView
src/screens/   Login · Imposters · ImposterDetail · Fleet · RequestLog · Routes
src/features/  requests/source.ts (the #147 seam) · routes/order.ts (front-door ordering)
src/components/primitives (Status, Truncated, Ident, ErrorNote)
```

Six modules carry the decisions worth knowing before changing anything:

- **`app/contract.ts`** — the single declaration of *which* schema fields any
  screen renders. Keys are typed as `keyof` the generated schema type with its
  index signature stripped, so a field the contract does not publish fails
  `tsc`. This is RFC-006 §11's "every displayed field is traceable to a schema'd
  endpoint" made mechanical rather than aspirational. It is why the prototype's
  `numberOfRequests` chart is **not** here: that value reaches the body only
  through `Imposter`'s non-exhaustive index signature. Its home is the request
  log (#189).
- **`app/rbac.ts`** — a hand transcription of
  `crates/rift-cluster-server/src/authz.rs::role_allows`, with a test that
  mirrors the table. It decides which controls are *drawn*. RFC-006 §3 rule 3
  still holds: hiding is UX, the API is the boundary, and a hidden button is
  never the only thing preventing a call. Note `LifecycleToggle` is an
  **Operator** grant — gating enable/disable on "is an editor" would hide a
  control Operator is entitled to.
- **`app/fleetView.ts`** — derives the degraded/partial label from `/_fleet/*`.
  Three states, kept distinct: read it, never asked, asked and failed.
  `not-asked` claims **neither** partial nor complete — the projection is
  fleet-scoped, so most principals are simply refused it and treating that
  absence as evidence would put a permanent warning on a healthy console. But
  `unavailable` must not fold into it: a FleetAdmin whose read failed has *lost*
  the signal, which is a different thing from never having been entitled to it.

  Note what is deliberately **not** a degradation: `voters ⊄ ring.members`. Both
  arrive from the same `membership_config.voter_ids()` — `members_body` sends it
  directly, `health_body` sends `Ring::new` of it, which only sorts and dedups —
  so within one snapshot they are the same set and the divergence is
  unrepresentable. Comparing them across this view's two requests would report a
  sub-second read skew as a persistent fleet degradation. What *is* checked, and
  has no other tell, is `node_id ∉ voters`: a node evicted from the membership
  while still running looks healthy by every other measure.
- **`app/query.ts`** — the polling contract (RFC-006 §6). 5s, and
  `refetchIntervalInBackground: false` so a forgotten tab stops asking. That one
  is verified by counting fetches across a real `visibilitychange`, not by
  asserting the option is set. The request log overrides the cadence only
  (`REQUEST_POLL_INTERVAL_MS`, 2s) — it is the screen someone watches while
  re-running a test — and keeps the same hidden-tab pause.
- **`features/requests/source.ts`** — the request log's data source, and **the
  convergence seam for #147 H**. The screen renders only through `Coverage` and
  `Page`; today's implementation reads one node's journal and says so. When the
  merged journal (#147 B) and cursors (#147 D) land, slice H implements the same
  shapes with `{ kind: "fleet" }`, the per-node banner disappears on its own, and
  no presentation code changes.

  Two distinctions here are load-bearing rather than stylistic. `unrepresented:
  null` means **could not be determined**, never zero — `/_fleet/*` is
  fleet-scoped, so most principals cannot learn how many nodes exist, and
  reporting "0 others" to them would assert something nothing supports. And
  `LogState` keeps *unknown* apart from *empty*: a node that could not answer has
  an unknown journal, and rendering it as an empty table tells an operator their
  system under test never called the mock.

  v1 pages client-side. That bounds the DOM, which is what a busy imposter
  threatens; it does **not** bound the response — the node still serves its whole
  journal in one body. Closing that needs the server's `?since=` cursor and
  `x-rift-next-index` header, which is the same seam #147 D widens, so paging is
  expressed as a `Cursor` rather than an array slice inlined in the screen.
- **`features/routes/order.ts`** — `effectiveOrder` and `validateTable`, ported
  from `vendor/rift/.../front_door/route_table.rs`. Ported rather than fetched
  because there is no endpoint that answers either question about a draft that
  does not exist on the server yet: the editor has to show evaluation order while
  the operator is still typing, and say why a table will be refused *before*
  sending it. The server stays the authority — everything here is advisory, and
  when the two disagree the screen shows the fleet's own words.

  The mirror must not be *stricter* than the server or it blocks a table the
  fleet would accept. Two ways that bit already: `hyper::Method` takes any valid
  HTTP token, so `PURGE` is legal and only a malformed token is refused; and the
  server compares `headers: Vec<HeaderMatch>` with a derived `PartialEq`, which
  is **order-sensitive**, so sorting the clauses before comparing reported an
  `AmbiguousMatch` the fleet would never raise.

  **The route schema is snake_case, and it is the one place in this contract
  that is.** `Route`, `RouteMatch` and `RouteTarget`
  (`front_door/route_table.rs`) carry no `serde(rename_all)`, so the wire is
  `path_prefix`, `strip_prefix`, `set_host` — as
  `crates/rift-cluster-server/tests/front_door.rs` has always asserted. The
  hand-authored contract declared them camelCase, and this slice was the first
  code to depend on it; the symptom was not a type error but a screen that
  silently read `undefined` for every path prefix, so it ranked routes in an
  order the front door does not use and called two distinct routes ambiguous.
  Corrected in `openapi-ee.yaml` — if you add a front-door field, check the Rust
  struct rather than assuming the house camelCase.

## Testing

`pnpm test` runs vitest. Node is the default environment; component tests opt
into jsdom with a `/** @vitest-environment jsdom */` docblock — not the other
way round, because under jsdom `import.meta.url` is an `http:` URL and the two
tests that read repository files could not resolve them.

`src/__tests__/harness.tsx` renders through the **real** `createQueryClient()`.
A test-local client with polling and retries disabled would pass while the
shipped configuration polled a hidden tab forever.

## Lint

```sh
pnpm run lint
```

One rule, deliberately: `dangerouslySetInnerHTML` is banned in both its JSX and
its property form (RFC-006 §9.1). A broad recommended-set config would bury a
security gate among style nits reviewers learn to skim. CI runs this, and
`contract-traceability.test.ts` asserts the same thing, so dropping the workflow
step alone does not silently un-ban it.

## Dev loop

No Rust rebuild is needed to work on the console. Vite proxies every admin path
to a running node:

```sh
pnpm install
pnpm dev                                         # → http://127.0.0.1:2525
RIFT_ADMIN_URL=http://localhost:12525 pnpm dev   # → the compose stack's node 1
```

The proxy table lives in `vite.config.ts` and is **tested**: a contract path
that no prefix covers fails `pnpm test`, because the alternative is a 404 in the
browser that reads like a server bug.

## The generated client

`src/api/schema.ts` is generated from `../docs/api/openapi-ee.yaml` and
**committed**, so this package builds without the Rust binary present:

```sh
pnpm run generate:client
```

CI regenerates it and fails on any diff, so it cannot silently go stale. Do not
hand-edit it — edit the contract.

`src/api/client.ts` is the thin wrapper around it, and carries the three things
the schema cannot express: the session cookie rides along, mutations carry the
`X-Rift-CSRF` header (RFC-006 §5.3), and a non-2xx becomes a thrown `ApiError`
rather than a value a screen renders as a result.

## Constraints you cannot design around

Everything here is embedded into the binary and served under a strict CSP
(RFC-006 §9.1):

```
default-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'
```

Which means, concretely:

- **No CDN anything.** No Google Fonts, no icon CDN, no remote images. Self-host
  or use system font stacks.
- **Build-time CSS only.** Tailwind, CSS Modules and Vanilla Extract emit a
  static stylesheet and are fine. Runtime CSS-in-JS (emotion,
  styled-components) injects `<style>` at runtime; with no `style-src` declared
  it falls back to `default-src 'self'` and is **blocked**. Adding
  `'unsafe-inline'` to work around it would undo part of §9.1's argument — treat
  that as a design change needing review, not a fix.
- **No inline scripts**, including anything a build plugin might inline.

These are enforced, not just documented: `crates/rift-cluster-server/tests/console.rs`
asserts the *served* page declares no off-origin subresource, no inline
`<script>`, and no markup-level style. A library that violates any of them turns
that test red rather than turning the console blank in a browser.

**Animation libraries still need a spike.** CSP governs markup-level styles but
not CSSOM property assignment, so a library that animates via
`element.style.foo = …` (Framer Motion among them) may well be fine — but that
has not been demonstrated, because this scaffold ships no animation library to
demonstrate it with. Whoever adds one in C4/C5 owns that spike; the test above
is what will answer it.

## Build

```sh
pnpm build     # → dist/, which the release lane embeds via rust-embed
```

`dist/` is **not** committed (RFC-006 §7 rejected that as option B). The release
lane builds it before `cargo build --release --features console`; that ordering
is not optional, because the assets are embedded at compile time.
