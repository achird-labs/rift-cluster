# `rift-lint`, compiled to wasm

This crate's build artifact lands in `web/public/lint/` — that directory is where the console's advisory lint pane looks for its linter
(`web/src/features/stubs/lint.ts` imports `/console/lint/rift_lint_wasm.js`).

**It is empty in a checkout, and that is not a bug.** The artifact is built by
one step in `.github/workflows/release.yml`:

```sh
wasm-pack build crates/rift-lint-wasm --release --target web --out-dir ../../web/public/lint
```

Vite copies `web/public/` verbatim into `dist/`, and the console is served under
`/console/` (`vite.config.ts`'s `base`), so the built files land at the URL the
loader asks for.

The build output is `.gitignore`d rather than committed, for the same reason
`web/dist/` is (RFC-006 §7 option D): a committed binary artifact can drift from
the source that produced it, and the drift hides in a review diff.

## What happens without it

The dynamic import fails and `lintStub` resolves `"unavailable"`. The pane then
says so in as many words — *"lint unavailable — the server still validates every
save"* — rather than rendering an empty finding list, which would read as a
clean bill of health from a linter that never ran.

That is the honest degraded mode, and it is the mode the whole test suite runs
in: nothing in `web/` depends on the artifact existing.

## Why it is advisory

The server validates every write and its refusal is the authority
(`admin_front.rs`). This linter exists to catch an obvious mistake before the
round trip, never to decide whether a save may proceed — the save button is not
gated on it, and a stub this finds no fault with can still be refused.
