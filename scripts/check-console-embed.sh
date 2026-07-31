#!/usr/bin/env bash
# The two console-embed properties that only a *build* can demonstrate (RFC-006 §7, issue #186).
# Run from the release lane, where `web/dist/` exists.
#
#   missing-dist          `--features console` with no `web/dist/` must fail at COMPILE time.
#   embedded <binary>     the release binary must carry the bundle's bytes inside it.
#
# Why each is a script rather than a test:
#
# `missing-dist` asserts a *compilation failure*, which a test inside the crate cannot observe — by
# the time a test runs, the thing under test compiled. It is the criterion that stops a release
# silently shipping consoleless, and RFC-006 §7 makes it load-bearing, so it is checked rather than
# assumed from rust-embed's documented behaviour.
#
# `embedded` is the honest form of "air-gapped". In a *debug* build rust-embed deliberately reads
# from disk (that is why it was chosen over include_dir — live asset reload under `cargo run`), so a
# debug test proving `/console` serves assets proves nothing about the release artifact. Only
# inspecting the release binary distinguishes "embedded" from "read the repo it was built in".
set -euo pipefail

cd "$(dirname "$0")/.."

fail() {
  echo "check-console-embed: $*" >&2
  exit 1
}

mode=${1:-}

case $mode in
missing-dist)
  [[ -d web/dist ]] || fail "web/dist does not exist — run 'pnpm build' in web/ first"

  stash=$(mktemp -d)
  # Restore on every exit path. Leaving the tree without its bundle would break every later step in
  # the lane, and the failure would look like an unrelated build error.
  trap 'if [[ -d "$stash/dist" ]]; then rm -rf web/dist && mv "$stash/dist" web/dist; fi; rm -rf "$stash"' EXIT
  mv web/dist "$stash/dist"

  # Force a real compile of the crate, and do NOT treat this as a detail of the harness.
  #
  # `rust-embed` is a derive macro, and a derive macro cannot emit `cargo:rerun-if-changed` — only a
  # build script can. So cargo has no dependency edge from `rift-cluster-server` to `web/dist/`:
  # with a warm cache it happily reuses the previously compiled artifact, and this check passed
  # "successfully" against a build that never re-expanded the macro. Measured, not assumed — that is
  # exactly what happened the first time this script ran.
  #
  # The same gap has a second, worse consequence on the release lane, which is why
  # `check-console-embed.sh embedded` exists and greps for *this* build's content hash: a cached
  # artifact would otherwise let a release ship a stale console bundle with every check green.
  cargo clean -p rift-cluster-server >/dev/null 2>&1 || true

  echo "check-console-embed: building --features console with no web/dist (this MUST fail)…"
  if cargo build -p rift-cluster-server --features console >"$stash/build.log" 2>&1; then
    fail "the build SUCCEEDED without web/dist — the embed is not compile-time required, so a \
release could ship with no console in it"
  fi

  # Fail for the *right* reason. Any build error would satisfy a bare non-zero exit, including one
  # caused by an unrelated compile break — which would leave this check green-by-accident forever.
  if ! grep -qiE 'web/dist|does not exist|no such file' "$stash/build.log"; then
    echo "--- build output ---" >&2
    tail -30 "$stash/build.log" >&2
    fail "the build failed, but not with a missing-folder error — this check cannot confirm the \
embed is what stopped it"
  fi

  echo "check-console-embed: ok — --features console requires web/dist at compile time"
  ;;

embedded)
  binary=${2:-}
  [[ -n $binary ]] || fail "usage: $0 embedded <path-to-release-binary>"
  [[ -x $binary ]] || fail "$binary is not an executable file"
  [[ -f web/dist/index.html ]] || fail "web/dist/index.html missing — nothing to compare against"

  # A content-hashed asset filename: it exists only in this build's output, so finding it inside the
  # binary cannot be a coincidence or a leftover string from the source tree.
  # `grep -m1` rather than `| head -1`: under `pipefail`, `head` closing the pipe early can kill
  # `grep` with SIGPIPE (141), which `set -e` then turns into a silent abort *before* the explicit
  # failure message below ever runs.
  marker=$(grep -m1 -oE 'assets/[A-Za-z0-9._-]+\.js' web/dist/index.html)
  [[ -n $marker ]] || fail "no hashed asset reference in web/dist/index.html — is this a real build?"

  if ! grep -qa "$marker" "$binary"; then
    fail "the release binary does not contain '$marker' — the console was NOT embedded, so this \
artifact depends on web/dist being present at runtime"
  fi

  echo "check-console-embed: ok — $binary embeds the console bundle ($marker)"
  ;;

*)
  fail "usage: $0 {missing-dist|embedded <binary>}"
  ;;
esac
