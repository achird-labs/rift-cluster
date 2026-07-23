#!/usr/bin/env bash
# Decide whether a change set warrants running the container chaos tier
# (the `cluster-smoke` job in .github/workflows/ci.yml).
#
# Reads `git diff --name-only` output on stdin, writes `run=true` or
# `run=false` on stdout — the shape `$GITHUB_OUTPUT` wants.
#
# Why this is a script with a self-test rather than four lines of shell inline
# in the workflow YAML:
#
# The filter's failure mode is *skipping*, and a skipped tier is a green check
# that tested nothing. That is invisible in exactly the way a red check is not,
# so the set of paths it matches has to be pinned by something executable. It
# was not, and it was wrong: the original pattern listed only prefixes ending
# in `/`, while a submodule pointer bump appears in `git diff --name-only` as
# the bare path `vendor/rift` with no trailing slash. So a pure vendor bump —
# the single highest-risk change class the cluster has, since `vendor/rift` IS
# the mock engine, and the exact class `sync-upstream.yml` produces on a
# schedule — matched nothing and skipped the whole tier (issue #93).
#
# The two match kinds below are the fix and are deliberately distinct:
#
#   * prefix matches  — a directory; anything beneath it counts
#   * whole-line ones  — a single file or a submodule pointer, which is one
#                        complete path and must not match by prefix (that would
#                        make `vendor/rift` also match `vendor/rift-other`)
#
# Whole-line matching uses `grep -x` rather than a `$` anchor inside an
# alternation group: whether `$` is an anchor mid-pattern is implementation
# defined across grep flavours, and this is not a place to depend on that.
set -euo pipefail

# Directories whose contents can plausibly break a cluster.
#
# `crates/rift-ee/` is here for the same reason as `vendor/rift`, not as a
# generic "first-party crate" entry: both watched crates declare it the ONLY
# path into the open-source core (see the fence comment in each of their
# Cargo.tomls), so it is the facade every engine change reaches the cluster
# through. Watching the submodule but not the seam that re-exports it would
# leave the same hole one layer up.
WATCHED_PREFIXES='^(crates/rift-cluster/|crates/rift-ee/|crates/rift-ee-server/|deploy/|tests/cluster-chaos/|vendor/rift/)'

# Whole paths, matched in full.
#
# `vendor/rift` is the submodule pointer — the form a bump actually takes in a
# diff, and the omission that was issue #93.
#
# `Cargo.lock` is here for the same reason as the submodule pointer: a
# dependency change can alter engine behaviour without touching a single
# enterprise source file, which is this same blind spot in a different shape.
# The workspace root lockfile is the only one that resolves the graph, so this
# is deliberately root-only — a `Cargo.lock` deeper in the tree is not it.
#
# The two scripts are the tier's own machinery (issue #107). Everything above
# is watched because changing it can break the *cluster*; these two are watched
# because changing them can break the *tier that would have caught that* —
# which is worse, because it fails green:
#
#   * `chaos-quarantine.sh list` emits the `--skip` arguments the runner hands
#     to `cargo test`, so its output IS the set of scenarios that will not run.
#     The `check` subcommand in the `build` job validates tag *syntax*; it says
#     nothing about whether `list` emits the right skips. A parser bug there
#     shrinks the tier fleet-wide and the job still reports success.
#   * this file's own `--self-test` pins the patterns against the case table —
#     but a PR editing pattern and table together passes it by construction.
#     Only a real tier run exercises such an edit end to end, and a PR that
#     changes when the tier runs is exactly the PR that should carry that
#     evidence.
#
# #104 raised the stakes: `cluster-smoke` is now a *required* status check, so
# a silently-skipped tier is no longer merely a green tick — the ruleset
# actively certifies the thing that did not run.
#
# Deliberately NOT watched, so the boundary is a decision rather than an
# oversight:
#
#   * `.github/workflows/ci.yml` — every unrelated CI tweak touches it, and
#     taxing all of them ~25 min is how a gate trains people to route around
#     it. The compensating control is real: `--self-test` runs as the first
#     step of `cluster-smoke` itself (a required check that fails closed).
#   * `.github/workflows/nightly-chaos.yml` — the nightly soak exercises the
#     full runner path daily. The accepted residual is that a broken runner-step
#     edit is caught within a day rather than at review.
#
# `scripts/` as a whole stays out; only these two files are in. The
# `check-public-api.sh` case in the table below pins that.
WATCHED_EXACT='vendor/rift|Cargo\.lock|scripts/chaos-quarantine\.sh|scripts/cluster-smoke-paths\.sh'

# Does any changed path match? Returns 0 on match, 1 on no match.
#
# grep exits 0 on match, 1 on no match, and **>1 on error** — a malformed
# pattern, an unreadable input. Writing this as a bare `if grep ...` folds all
# three into one boolean, so a broken classifier reads as "nothing watched
# changed" and the tier is skipped: exactly the fail-open this script exists to
# prevent, moved one layer down. Anything above 1 is therefore fatal, never a
# non-match — when the filter cannot tell, it must not answer "skip".
#
# Reads `$changed` via a here-string rather than a pipe on purpose: a function
# on the right of a `|` runs in a subshell, where `exit` would kill only that
# subshell and the caller would read the status as an ordinary no-match.
matches() {
  local rc=0
  grep -q "$@" <<<"$changed" || rc=$?

  if [ "$rc" -gt 1 ]; then
    printf 'cluster-smoke-paths: grep failed (rc=%s) with args: %s\n' "$rc" "$*" >&2
    printf 'Refusing to answer run=false on a broken filter — that is a green\n' >&2
    printf 'check that tested nothing. Fix the pattern.\n' >&2
    exit 1
  fi

  return "$rc"
}

# stdin: paths, one per line. stdout: `run=true` / `run=false`.
decide() {
  # Global, not local: `matches` reads it (see the subshell note above).
  changed="$(cat)"

  if matches -E "$WATCHED_PREFIXES" || matches -xE "$WATCHED_EXACT"; then
    echo "run=true"
  else
    echo "run=false"
  fi
}

# Each case is `expected|space-separated paths`, where `expected` is the bare
# word true/false. The paths are fed in as separate lines, the way a real diff
# arrives.
#
# What this table does NOT pin, so it is not mistaken for exhaustive:
#
#   * a changed path containing a space — the encoding would split it in two.
#     `decide` handles one correctly (real diff output is newline-delimited);
#     it is the table that cannot describe one.
#   * input size. Every case here is a few bytes, so no case would notice the
#     filter mishandling a change set larger than a pipe buffer.
#   * how git renders the paths in the first place — quoting of non-ASCII names
#     and rename reporting are properties of the `git diff` invocation in
#     ci.yml, upstream of anything this script sees.
self_test() {
  local failures=0 total=0 expect paths actual want

  while IFS='|' read -r expect paths; do
    [ -z "${expect:-}" ] && continue
    case "$expect" in \#*) continue ;; esac

    total=$((total + 1))
    want="run=${expect}"
    actual="$(printf '%s\n' "$paths" | tr ' ' '\n' | decide)"

    if [ "$actual" = "$want" ]; then
      printf '  ok   %-12s %s\n' "$want" "$paths"
    else
      printf '  FAIL want=%-9s got=%-9s %s\n' "$want" "$actual" "$paths"
      failures=$((failures + 1))
    fi
  done <<'CASES'
# A pure submodule pointer bump — one bare path, no trailing slash. This is
# issue #93: `vendor/rift` IS the mock engine, so this must run the tier.
true|vendor/rift
# A vendor bump also rewrites the lockfile, and a dependency change alone can
# alter engine behaviour with no enterprise source touched.
true|Cargo.lock
true|vendor/rift Cargo.lock
# Files inside the submodule, should upstream ever be worked on in-tree.
true|vendor/rift/crates/rift-mock-core/src/imposter/manager.rs
# The four originally-watched directories.
true|crates/rift-cluster/src/lib.rs
true|crates/rift-ee-server/src/main.rs
true|deploy/compose/docker-compose.yml
true|tests/cluster-chaos/src/lib.rs
# One watched path is enough, however much unwatched noise rides along.
true|README.md docs/adr/ADR-001-raft-control-plane.md vendor/rift
# Pins the whole-line boundary: a sibling directory is NOT the submodule.
false|vendor/rift-other/src/lib.rs
false|vendor/rift-other
# The same boundary for the prefix entries. These pass today because each
# prefix ends in `/` — and dropping that slash is precisely the edit that
# caused #93, so the cases exist to catch it being made again.
false|crates/rift-cluster-extra/src/lib.rs
false|tests/cluster-chaos-old/scenarios.rs
false|deployment/compose.yml
# Root lockfile only: a Cargo.lock deeper in the tree does not resolve the
# workspace graph, so it is not the signal being watched for.
false|crates/foo/Cargo.lock
# Prose changes cannot break a cluster.
false|README.md
false|docs/rfc/RFC-001-self-clustering-rift.md docs/adr/ADR-001-raft-control-plane.md
# The seam crate: a `vendor/rift` bump reaches the cluster through it, so
# excluding it would leave the #93 hole one layer up.
true|crates/rift-ee/src/lib.rs
# Nothing changed at all.
false|
# A lockfile-named file that is not THE lockfile.
false|docs/Cargo.lock.md
# The tier's own machinery (issue #107). `chaos-quarantine.sh list` produces
# the `--skip` arguments the runner passes, so its output IS the set of
# scenarios that do not run; a parser bug silently shrinks the tier while it
# still reports green.
true|scripts/chaos-quarantine.sh
# This file. Its self-test pins the patterns against the table — but a change
# that edits both cannot fail it by construction, and only a real tier run
# exercises the edit end to end.
true|scripts/cluster-smoke-paths.sh
# Pins the boundary: `scripts/` as a whole stays unwatched. Only the two files
# above are in, and this case fails if someone widens it to a prefix.
false|scripts/check-public-api.sh
CASES

  echo
  if [ "$failures" -eq 0 ]; then
    echo "OK: ${total} path cases behave as specified"
    return 0
  fi

  echo "FAIL: ${failures}/${total} path cases wrong."
  echo
  echo "This table is the specification of which changes run the container"
  echo "chaos tier. A case failing here means the filter would skip (or run)"
  echo "the tier for a change class it should not — and a wrongly-skipped tier"
  echo "is a green check that tested nothing. Fix the patterns, not the table,"
  echo "unless the change class genuinely no longer warrants the tier."
  return 1
}

case "${1:---decide}" in
  --self-test) self_test ;;
  --decide) decide ;;
  *)
    echo "usage: $0 [--decide | --self-test]" >&2
    echo "  --decide     (default) read changed paths on stdin, write run=true/false" >&2
    echo "  --self-test  check the patterns against the case table" >&2
    exit 2
    ;;
esac
