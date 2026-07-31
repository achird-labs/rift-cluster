#!/usr/bin/env bash
# Assert the web console stays entirely OFF the default build graph (RFC-006 §7, issue #186).
#
# The hard invariant C3 buys is that `cargo build` and `cargo test` never require node — in any dev
# lane, any CI lane, and above all the `--cluster-off` parity lanes (#139). That invariant is not
# expressible to the compiler: it is a property of the *feature graph*, and every way of breaking it
# looks like an ordinary, working change locally.
#
# The three ways it breaks, each of which this script catches:
#
#   1. `console` is added to a `default = [...]` feature list. Now every `cargo build` in the tree
#      needs `web/dist/` to exist, because `rust-embed` resolves its folder at compile time. On a
#      developer machine that just ran `pnpm build` this is invisible; on a clean checkout — and in
#      the parity lanes — it is a hard compile error with a confusing message.
#   2. `rust-embed` is made a non-optional dependency, or reached from a non-feature-gated path. The
#      embed then joins the default graph even with the feature nominally off.
#   3. A `build.rs` under `crates/` starts shelling out to node/pnpm. That is option A from RFC-006
#      §7's table — explicitly rejected as "non-hermetic; works on my machine as a build system" —
#      and it would reintroduce the dependency the feature exists to avoid. Note this is about what
#      build scripts *invoke*, not whether they exist: `rift-cluster-base/build.rs` legitimately
#      shells out to `git` to record the vendored Rift pin, and predates all of this.
#
# Run from the repo root. Exits non-zero with a specific message on the first violation.
set -euo pipefail

cd "$(dirname "$0")/.."

fail() {
  echo "check-console-hermetic: $*" >&2
  exit 1
}

manifest="crates/rift-cluster-server/Cargo.toml"
[[ -f $manifest ]] || fail "$manifest not found — run this from the repo root"

# (1) The feature must exist and must not be on by default.
grep -qE '^console = ' "$manifest" ||
  fail "no 'console' feature in $manifest — the embed must stay behind a feature"

# The `default` list, if there is one, must not name `console`. Matched on the line because the
# workspace writes single-line feature lists; a multi-line list would not match here, so the
# `cargo tree` check below is the one that actually cannot be evaded.
if grep -qE '^default = .*\bconsole\b' "$manifest"; then
  fail "'console' is in the default feature list — every cargo build would then require web/dist"
fi

# (2) No first-party build script may invoke a JS toolchain. `vendor/` is upstream's business.
#
# Matched on *invocation shape* plus the unambiguous tool names, deliberately NOT on a bare `node`:
# "node" is this codebase's central noun (cluster nodes, node ids, node health), so a bare-word match
# would eventually fail a build script that merely mentions one in a comment — and a check that cries
# wolf gets deleted rather than heeded.
while IFS= read -r script; do
  if grep -qE 'Command::new\("(node|npm|npx|pnpm|yarn|bun|vite)"|\b(pnpm|npx|yarn|bun|vite)\b' "$script"; then
    fail "$script invokes a JS toolchain — RFC-006 §7 rejected build-time node invocation (option A);
       the bundle is built by the release lane and embedded, never built by cargo"
  fi
done < <(find crates -name build.rs -not -path '*/target/*')

# (3) The decisive check: resolve the DEFAULT feature graph and assert rust-embed is absent from it.
#     This is the one that cannot be satisfied by a manifest that merely looks right — it asks cargo
#     what it would actually build.
if ! tree=$(cargo tree -p rift-cluster-server --edges normal,build --prefix none 2>/dev/null); then
  fail "could not resolve the dependency tree for rift-cluster-server"
fi

# Guard the guard: an empty or truncated tree would make the grep below vacuously pass.
if [[ $(printf '%s\n' "$tree" | grep -c .) -lt 20 ]]; then
  fail "cargo tree returned implausibly little output — refusing to certify on it"
fi

if printf '%s\n' "$tree" | grep -qE '^rust-embed( |$)'; then
  fail "rust-embed is in the DEFAULT dependency graph — the console embed is no longer feature-gated"
fi

# The same question asked of the WHOLE workspace, because cargo unifies features across it. The
# `-p` resolution above sees only this crate's own defaults: if another member ever declared
# `rift-cluster-server = { ..., features = ["console"] }`, a plain `cargo build --workspace` would
# turn the console on while the check above stayed green.
if ! tree_ws=$(cargo tree --workspace --edges normal,build --prefix none 2>/dev/null); then
  fail "could not resolve the workspace dependency tree"
fi
if [[ $(printf '%s\n' "$tree_ws" | grep -c .) -lt 20 ]]; then
  fail "workspace cargo tree returned implausibly little output — refusing to certify on it"
fi
if printf '%s\n' "$tree_ws" | grep -qE '^rust-embed( |$)'; then
  fail "rust-embed is enabled somewhere in the WORKSPACE default graph — some member turns the \
console feature on, so 'cargo build --workspace' now requires web/dist"
fi

# And the positive control: with the feature on it MUST appear. Without this, a typo in the grep
# above (or a rename of the crate) would make every run pass while checking nothing.
if ! tree_on=$(cargo tree -p rift-cluster-server --features console --edges normal,build --prefix none 2>/dev/null); then
  fail "could not resolve the dependency tree for rift-cluster-server --features console"
fi
if ! printf '%s\n' "$tree_on" | grep -qE '^rust-embed( |$)'; then
  fail "rust-embed is absent even WITH --features console — this check is not testing what it claims"
fi

echo "check-console-hermetic: ok — console is feature-gated, rust-embed is off the default graph, no build script invokes node"
