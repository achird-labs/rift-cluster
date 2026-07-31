# `web/` — the Rift console

The single-page app the cluster binary serves at `GET /console`. Vite + React +
TypeScript + TanStack Query, with a TypeScript client generated from the
published OpenAPI contract.

C3 (#186) delivers the **pipeline**, not the console: the shell here renders a
placeholder and one reachability check. The screens arrive with C4–C7
(#187–#190).

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
