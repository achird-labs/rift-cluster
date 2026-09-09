# `web/` — the Rift console

The single-page app the cluster binary serves at `/console/`. Vite + React +
TypeScript + TanStack Query, with a TypeScript client generated from the
published OpenAPI contract, Monaco bundled rather than fetched, and an optional
wasm `rift-lint` pane.

It shows **one fleet, to one administrator**. Since #550 (D-73) there is a single
credential and a single identity behind it: no tenants, no principals, no roles,
no capability table. Every screen shows everything and every control is offered
unconditionally.

## Layout

```
src/api/         schema.ts (generated, committed) · client.ts (the only fetch) · paths.ts
src/app/         Shell · session · SignOut · nav · routing · queries · query · contract · fleetView · pending
src/screens/     Login · Imposters · ImposterDetail · Fleet · RequestLog · Routes · Scenarios · StubEditor · RecordingPanel
src/features/    imposters/ · recording/ · requests/ · routes/ · scenarios/ · stubs/ · writes/
src/components/  primitives · CodeEditor · imposterList · imposterFields · detailRail · fleetRail · exportDialog · toast · pending
src/fonts/       self-hosted IBM Plex faces
e2e/             Playwright specs and their committed visual baselines
```

## Screens

Five navigation entries, in two groups, and nothing greyed out — `plannedEntries()`
is empty (`specs` was the last one, and #549 removed the stored-spec surface). The
mechanism is kept: an empty roadmap is a state, not a reason to delete the shape.

| Group | Label | Route | Screen |
|---|---|---|---|
| Mocks | Imposters | `#/imposters` | `Imposters.tsx` |
| Mocks | Front door routes | `#/routes` | `Routes.tsx` |
| Mocks | Flow state & scenarios | `#/scenarios[/:port[/:flowId]]` | `Scenarios.tsx` |
| Mocks | Requests | `#/requests[/:port]` | `RequestLog.tsx` |
| Fleet | Cluster & fleet | `#/cluster` | `Fleet.tsx` |

Plus one screen reached by drill-down rather than the nav: `#/imposters/:port`
(`ImposterDetail.tsx`), which hosts the stub editor and the recording panel.

> The route-table screen is still labelled "Front door" here. RFC-007 §6 renames
> the feature to the **router** in prose, console labels and CLI help; #553 is the
> issue and #569 the PR that does it in this package. The wire path
> `/front-door/routes` does not change.

Routing is **hash-based**: `/console/` is served by `rust-embed` with an SPA
fallback, and a hash never reaches the server. An unknown hash falls back to
`#/imposters` rather than a 404 — there is no server round trip that could
legitimately produce one. Screen state (filters, sort) rides in a query string
*after* the hash via `useHashQuery` + `replaceState`, so it is not part of
`Route` and does not push history entries.

## Authentication — one credential, one identity

`Login.tsx` takes an API key and calls `POST /session`, which returns an
`HttpOnly` `rift_session` cookie. The key lives in component state and nowhere
else — never `localStorage`, never `sessionStorage`, never a URL — and is cleared
on success and on failure alike.

`SignOut.tsx` calls `DELETE /session`, and that is load-bearing rather than
decorative: no script can clear an `HttpOnly` cookie, so the server has to send
the clearing `Set-Cookie` itself.

`useSession()` (`app/session.tsx`) probes with `GET /_fleet/health` — the cheapest
read behind the authentication gate — and `App.tsx` branches on it: pending → a
signing-in state, `401` → the login screen, any other error → "Could not reach the
admin front", otherwise the shell. There is no principal to name, no role to hold
and no tenant to select, so the probe asks only *are these credentials good*.

Transport rules live in `api/client.ts`: `credentials: "same-origin"`, the
`X-Rift-CSRF` header on every mutation, `If-Match` where a write is
revision-guarded, and `Idempotency-Key` where it is retryable.

## Modules that carry a decision

- **`app/contract.ts`** — the single declaration of *which* schema fields any
  screen renders. Keys are typed as `keyof` the generated schema type with its
  index signature stripped, so a field the contract does not publish fails `tsc`.
  This is RFC-006 §11's "every displayed field is traceable to a schema'd
  endpoint" made mechanical rather than aspirational.

- **`app/fleetView.ts`** — derives the degraded label from `/_fleet/*`.
  `Degradation` is `"not-ready" | "draining" | "isolated" | "no-leader" |
  "evicted"`. Three read states are kept distinct: read it, never asked, asked and
  failed. `not-asked` claims **neither** partial nor complete, and `unavailable`
  must not fold into it — a read that failed has *lost* the signal, which is not
  the same as never having asked for it.

  Note what is deliberately **not** a degradation: `voters ⊄ ring.members`. Both
  arrive from the same `membership_config.voter_ids()` — `members_body` sends it
  directly, `health_body` sends `Ring::new` of it, which only sorts and dedups —
  so within one snapshot they are the same set and the divergence is
  unrepresentable. Comparing them across this view's two requests would report a
  sub-second read skew as a persistent fleet degradation. What *is* checked, and
  has no other tell, is `node_id ∉ voters`: a node evicted from the membership
  while still running looks healthy by every other measure.

  `nodeId` stays a **string** all the way through. It is a `u64` on the wire and
  `Number(id)` would silently round the large ones.

- **`app/query.ts`** — the polling contract (RFC-006 §6). `POLL_INTERVAL_MS` is
  5 s with `refetchIntervalInBackground: false`, so a forgotten tab stops asking;
  that one is verified by counting fetches across a real `visibilitychange`, not
  by asserting the option is set. The request log overrides the cadence only
  (`REQUEST_POLL_INTERVAL_MS`, 2 s) — it is the screen someone watches while
  re-running a test — and keeps the same hidden-tab pause.
  `retryTransportFailures` declines to retry any 4xx.

- **`features/requests/source.ts`** — the request log's data source, **per node,
  full stop**. D-74 (#552) removed the fleet journal merge: `GET
  /imposters/:port/requests` answers for the node the browser reached, and the
  `Coverage` / `unrepresented` machinery that existed to describe a partial merge
  is gone with it. `LogState` is `rows` or `unknown`, and the remaining
  distinction is load-bearing: a node that could not answer has an *unknown*
  journal, and rendering that as an empty table tells an operator their system
  under test never called the mock.

  `page()` is a **display** pager over accumulated rows; the network request is
  bounded by the server's own `?since=` cursor and `x-rift-next-index` header,
  which are upstream's, scalar, and shipped.

- **`features/requests/diagnostics.ts`** — why a recorded request was served by
  the stub it was, or by nothing (#208). A pure presenter: `describeOutcome`
  turns the journal's `matchOutcome` into sentences and `RequestLog.tsx` only
  places them.

  Three states that look alike stay apart. An **absent** outcome is "no
  diagnostics recorded" — an entry from an engine predating the field, an
  `X-Rift-Debug` request, or a matcher error — and never "did not match", which
  would tell an operator their stub was rejected when nothing judged it. An
  outcome in a shape the console cannot read is **unreadable**, which says the
  node answered with something wrong rather than that nothing was recorded. And
  an unrecognised `reason` from a newer engine is shown as the engine spelled it
  rather than dropped, because dropping it would under-report what was tried —
  the one claim the panel makes.

  Typed from the contract and validated anyway: `apiGet` asserts response shapes
  rather than checking them, and this one is recorded from whatever called the
  mock, so every unexpected shape lands on `unreadable` instead of throwing
  inside the screen an operator opened to diagnose something else.

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

  **The route schema is snake_case, and it is the one place in this contract that
  is.** `Route`, `RouteMatch` and `RouteTarget` (`front_door/route_table.rs`)
  carry no `serde(rename_all)`, so the wire is `path_prefix`, `strip_prefix`,
  `set_host` — as `crates/rift-cluster-server/tests/front_door.rs` has always
  asserted. The hand-authored contract declared them camelCase, and the symptom
  was not a type error but a screen that silently read `undefined` for every path
  prefix, ranked routes in an order the router does not use, and called two
  distinct routes ambiguous. If you add a route field, check the Rust struct
  rather than assuming the house camelCase.

- **`features/writes/commit.ts`** — one wire fact that costs a debugging cycle if
  assumed: `ControlOutcome` serializes as the bare lowercase string `"applied"`,
  never `{"Applied": null}` (it is `snake_case` with a unit variant). Parked
  writes (`202`) are polled through `/_fleet/ops/{opId}`.

## Testing

`pnpm test` runs vitest over 48 test files. Node is the default environment;
component tests opt into jsdom with a `/** @vitest-environment jsdom */`
docblock — not the other way round, because under jsdom `import.meta.url` is an
`http:` URL and the two tests that read repository files could not resolve them.

`src/__tests__/harness.tsx` renders through the **real** `createQueryClient()`.
A test-local client with polling and retries disabled would pass while the
shipped configuration polled a hidden tab forever.

Three guard suites are worth knowing before adding a dependency:
`contract-traceability.test.ts` (every displayed field traces to the contract;
`fetch` is called from nowhere but `api/client.ts`; no `dangerouslySetInnerHTML`),
`bundle-offline.test.ts` (no emitted asset loads from another origin, no package
CDN string in the shipped bytes, Monaco from the bundle), and
`vite-config.test.ts` (the dev proxy covers every path the contract publishes).

End-to-end is Playwright against **the binary serving `/console/`**, not the Vite
dev server:

```sh
pnpm run e2e          # smoke · visual · a11y, plus interaction and oracle specs
pnpm run e2e:ui
pnpm run e2e:update   # regenerate visual baselines locally
scripts/e2e-console.sh up     # a seeded node on :3525 for hand-driving
scripts/e2e-console.sh down
```

`e2e/README.md` describes the three layers. `smoke.spec.ts` fails any test that
produced a console error or an unhandled rejection; `visual.spec.ts` compares
against committed baselines in `e2e/visual.spec.ts-snapshots/`; `a11y.spec.ts`
runs axe per screen and fails on `serious`/`critical` only.

In CI, new visual baselines need the **`update-baselines`** label on the PR:
`.github/workflows/console-baselines.yml` regenerates them, pushes the images,
and then removes the label, so a later push cannot silently re-accept a real
regression.

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
pnpm dev                                         # Vite's default, http://localhost:5173/console/
RIFT_ADMIN_URL=http://localhost:12525 pnpm dev   # proxy to the compose stack's node 1
```

`DEV_ADMIN_URL` defaults to `http://127.0.0.1:2525` — that is the **admin front
being proxied to**, not the dev server's own address. The proxy table
(`ADMIN_PROXY_PREFIXES` and `ADMIN_PROXY_EXACT`) lives in `vite.config.ts` and is
**tested**: a contract path that no prefix covers fails `pnpm test`, because the
alternative is a 404 in the browser that reads like a server bug. `"/"` is turned
into an anchored regex so Vite does not forward `/console/` itself.

## The generated client

`src/api/schema.ts` is generated from `../docs/api/openapi-ee.yaml` and
**committed**, so this package builds without the Rust binary present:

```sh
pnpm run generate:client
```

CI regenerates it and fails on any diff — and, before that, asserts the file is
*tracked* (`git ls-files --error-unmatch`), because an untracked `schema.ts`
would make `git diff` compare against nothing and the gate would pass on a file
nobody wrote. Do not hand-edit it; edit the contract.

`src/api/client.ts` is the thin wrapper around it and carries what the schema
cannot express: the session cookie rides along, mutations carry `X-Rift-CSRF`
(RFC-006 §5.3), and a non-2xx becomes a thrown `ApiError` rather than a value a
screen renders as a result. `src/api/paths.ts` types every route as `keyof paths`,
so a path the contract does not publish fails `tsc`.

## Constraints you cannot design around

Everything here is embedded into the binary and served under a strict CSP
(`crates/rift-cluster-server/src/console.rs`):

```
default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'
```

Which means, concretely:

- **No CDN anything.** No Google Fonts, no icon CDN, no remote images. Self-host
  or use system font stacks. The console self-hosts: `src/fonts/` carries the
  IBM Plex faces `styles.css` declares, vite emits them under `/assets`, and
  `src/__tests__/bundle-offline.test.ts` fails the build if any emitted asset
  ever loads from another origin. Adding a face means adding a file there — a
  `<link>` to fonts.googleapis.com is not a shortcut, it is a broken console on
  an air-gapped network.
- **`'wasm-unsafe-eval'` is on `script-src` for one reason**: browsers gate
  `WebAssembly` compilation behind it once any `script-src` is declared, and the
  bundled `rift-lint` pane needs it. It permits no inline script.
- **`'unsafe-inline'` is on `style-src` only**, added in C5 (#188) because
  Monaco's standalone editor injects a `<style>` element at runtime. Runtime
  CSS-in-JS is therefore not *blocked* any more — but it is still not wanted, and
  widening `script-src` would undo part of §9.1's argument. Treat that as a design
  change needing review, not a fix.
- **No inline scripts**, including anything a build plugin might inline.

These are enforced, not just documented: `crates/rift-cluster-server/tests/console.rs`
asserts the *served* page declares no off-origin subresource and no inline
`<script>`. A library that violates any of them turns that test red rather than
turning the console blank in a browser.

CSP governs markup-level styles but not CSSOM property assignment, so a library
that animates via `element.style.foo = …` may well be fine — but nothing here
demonstrates it, because the console ships no animation library. Whoever adds one
owns that spike; the test above is what will answer it.

## Build

```sh
pnpm build     # tsc --noEmit && vite build → dist/, which the release lane embeds
```

`dist/` is **not** committed (RFC-006 §7 rejected that as option B). The release
lane builds it before `cargo build --release --features console`; that ordering is
not optional, because the assets are embedded at compile time. `web/public/` — the
wasm linter, produced by `wasm-pack` in the release lane — is copied into `dist/`
by the same `pnpm build`, which is why the wasm step has to precede it. In a plain
dev checkout `web/public/` does not exist and the lint pane resolves to
`"unavailable"`, a sentence, rather than to an empty finding list that would read
as "your stub is clean".
