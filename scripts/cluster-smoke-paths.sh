#!/usr/bin/env bash
# Decide whether a change set warrants running a path-gated CI job.
# Two callers today: `cluster-smoke` (the container chaos tier) and `parity`
# (issue #37 — the upstream behavioural suites run against `rift-cluster-server`),
# both in .github/workflows/ci.yml. `--job` selects which watched set decides;
# it defaults to `cluster-smoke` so every call site that predates the parity
# job — and the `run=true`/`run=false` contract those sites read — is
# unchanged.
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
# `crates/rift-cluster-base/` is here for the same reason as `vendor/rift`, not as a
# generic "first-party crate" entry: both watched crates declare it the ONLY
# path into the open-source core (see the fence comment in each of their
# Cargo.tomls), so it is the facade every engine change reaches the cluster
# through. Watching the submodule but not the seam that re-exports it would
# leave the same hole one layer up.
CLUSTER_SMOKE_PREFIXES='^(crates/rift-cluster/|crates/rift-cluster-base/|crates/rift-cluster-server/|deploy/|tests/cluster-chaos/|vendor/rift/)'

# Whole paths, matched in full.
#
# `vendor/rift` is the submodule pointer — the form a bump actually takes in a
# diff, and the omission that was issue #93.
#
# `Cargo.lock` is here for the same reason as the submodule pointer: a
# dependency change can alter engine behaviour without touching a single
# cluster source file, which is this same blind spot in a different shape.
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
CLUSTER_SMOKE_EXACT='vendor/rift|Cargo\.lock|scripts/chaos-quarantine\.sh|scripts/cluster-smoke-paths\.sh'

# The `parity` job (issue #37): the upstream process-spawning suites, run
# against `rift-cluster-server` with `--cluster` off. Its watched set is narrower
# and different in kind from `cluster-smoke`'s — it is not "can this break a
# cluster", it is "can this break byte-for-byte parity with the open-source
# server" — so it is its own table rather than folded into the one above:
#
#   * `vendor/rift` (bare, no slash) — the pin bump. The highest-risk event:
#     it IS the upstream behaviour the suites assert parity against, and it is
#     exactly the change class `sync-upstream.yml` produces unattended. Same
#     bare-path form as the cluster-smoke case, same reason (#93): a submodule
#     pointer bump has no trailing slash in `git diff --name-only`.
#   * `crates/rift-cluster-server/` — the composition layer under test: CLI parse,
#     `ServerBuilder` wiring, bootstrap. This is the code the parity job exists
#     to catch drifting from upstream's own guarantees.
#   * `Cargo.lock` (root only, same reasoning as cluster-smoke's case) — a
#     dependency shift can change behaviour with no source file touched.
#   * `.github/` — the workflow that builds and runs this job is itself part of
#     what "parity is verified, not asserted" depends on; a change there can
#     silently stop the suites from running or from testing the right binary.
#     Deliberately broader tha cluster-smoke's stance on `.github/` (which
#     excludes it to avoid taxing every unrelated CI tweak): parity's own job
#     definition living in that directory is exactly the kind of edit this
#     watched set exists to catch, and the wall-clock cost here is one release
#     build plus four suites, not a multi-minute container tier.
PARITY_PREFIXES='^(crates/rift-cluster-server/|\.github/)'
PARITY_EXACT='vendor/rift|Cargo\.lock'

# Resolves `--job NAME` to the prefix/exact pair `decide` matches against, into
# the globals `watched_prefixes`/`watched_exact`. Not run inside `$(...)`: an
# unknown job must fail the whole script (see the grep-error handling below for
# why "cannot tell" must never collapse into "no match"), and `exit` inside a
# command substitution only kills the subshell, leaving the caller to read a
# bogus empty string as success.
select_job() {
  case "$1" in
    cluster-smoke)
      watched_prefixes="$CLUSTER_SMOKE_PREFIXES"
      watched_exact="$CLUSTER_SMOKE_EXACT"
      ;;
    parity)
      watched_prefixes="$PARITY_PREFIXES"
      watched_exact="$PARITY_EXACT"
      ;;
    *)
      printf 'cluster-smoke-paths: unknown --job %s (want: cluster-smoke, parity)\n' "$1" >&2
      exit 2
      ;;
  esac
}

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

# stdin: paths, one per line. stdout: `run=true` / `run=false`. Matches against
# `$watched_prefixes`/`$watched_exact`, set by `select_job` for the job named
# on the command line (default `cluster-smoke`).
decide() {
  # Global, not local: `matches` reads it (see the subshell note above).
  changed="$(cat)"

  if matches -E "$watched_prefixes" || matches -xE "$watched_exact"; then
    echo "run=true"
  else
    echo "run=false"
  fi
}

# Each case is `expected|job|space-separated paths`, where `expected` is the
# bare word true/false and `job` selects which watched set (`cluster-smoke` or
# `parity`) the case is checked against — the two jobs disagree on some paths
# (`.github/` in particular, watched by `parity` and deliberately not by
# `cluster-smoke`), so a case is not meaningful without saying which job it
# pins. `--self-test` always runs every case in this table regardless of the
# `--job` the script itself was invoked with: the table is the specification
# for the whole file, and splitting it per invocation would let a broken
# `parity` case go unnoticed by call sites (`build`'s unconditional
# `--self-test` step in particular) that never pass `--job parity`.
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
  local failures=0 total=0 expect job paths actual want

  while IFS='|' read -r expect job paths; do
    [ -z "${expect:-}" ] && continue
    case "$expect" in \#*) continue ;; esac

    select_job "$job"
    total=$((total + 1))
    want="run=${expect}"
    actual="$(printf '%s\n' "$paths" | tr ' ' '\n' | decide)"

    if [ "$actual" = "$want" ]; then
      printf '  ok   %-12s %-13s %s\n' "$want" "$job" "$paths"
    else
      printf '  FAIL want=%-9s job=%-13s got=%-9s %s\n' "$want" "$job" "$actual" "$paths"
      failures=$((failures + 1))
    fi
  done <<'CASES'
# --- cluster-smoke ---------------------------------------------------------
# A pure submodule pointer bump — one bare path, no trailing slash. This is
# issue #93: `vendor/rift` IS the mock engine, so this must run the tier.
true|cluster-smoke|vendor/rift
# A vendor bump also rewrites the lockfile, and a dependency change alone can
# alter engine behaviour with no cluster source touched.
true|cluster-smoke|Cargo.lock
true|cluster-smoke|vendor/rift Cargo.lock
# Files inside the submodule, should upstream ever be worked on in-tree.
true|cluster-smoke|vendor/rift/crates/rift-mock-core/src/imposter/manager.rs
# The four originally-watched directories.
true|cluster-smoke|crates/rift-cluster/src/lib.rs
true|cluster-smoke|crates/rift-cluster-server/src/main.rs
true|cluster-smoke|deploy/compose/docker-compose.yml
true|cluster-smoke|tests/cluster-chaos/src/lib.rs
# One watched path is enough, however much unwatched noise rides along.
true|cluster-smoke|README.md docs/adr/ADR-001-raft-control-plane.md vendor/rift
# Pins the whole-line boundary: a sibling directory is NOT the submodule.
false|cluster-smoke|vendor/rift-other/src/lib.rs
false|cluster-smoke|vendor/rift-other
# The same boundary for the prefix entries. These pass today because each
# prefix ends in `/` — and dropping that slash is precisely the edit that
# caused #93, so the cases exist to catch it being made again.
false|cluster-smoke|crates/rift-cluster-extra/src/lib.rs
false|cluster-smoke|tests/cluster-chaos-old/scenarios.rs
false|cluster-smoke|deployment/compose.yml
# Root lockfile only: a Cargo.lock deeper in the tree does not resolve the
# workspace graph, so it is not the signal being watched for.
false|cluster-smoke|crates/foo/Cargo.lock
# Prose changes cannot break a cluster.
false|cluster-smoke|README.md
false|cluster-smoke|docs/rfc/RFC-001-self-clustering-rift.md docs/adr/ADR-001-raft-control-plane.md
# The web console's sources (#186). A CSS or component edit cannot affect a cluster, and the console
# is not even compiled into the binary unless `--features console` is on — which no CI lane here
# enables. Pinned in both directions: the tier must not start running on every console commit (it
# costs minutes and would train people to ignore it), and `web/` must not become a way to change the
# serving code without the tier noticing — the Rust half of the console lives under `crates/`, which
# the cases above already cover.
false|cluster-smoke|web/src/App.tsx
false|cluster-smoke|web/package.json web/pnpm-lock.yaml
# The seam crate: a `vendor/rift` bump reaches the cluster through it, so
# excluding it would leave the #93 hole one layer up.
true|cluster-smoke|crates/rift-cluster-base/src/lib.rs
# Nothing changed at all.
false|cluster-smoke|
# A lockfile-named file that is not THE lockfile.
false|cluster-smoke|docs/Cargo.lock.md
# The tier's own machinery (issue #107). `chaos-quarantine.sh list` produces
# the `--skip` arguments the runner passes, so its output IS the set of
# scenarios that do not run; a parser bug silently shrinks the tier while it
# still reports green.
true|cluster-smoke|scripts/chaos-quarantine.sh
# This file. Its self-test pins the patterns against the table — but a change
# that edits both cannot fail it by construction, and only a real tier run
# exercises the edit end to end.
true|cluster-smoke|scripts/cluster-smoke-paths.sh
# Pins the boundary: `scripts/` as a whole stays unwatched. Only the two files
# above are in, and this case fails if someone widens it to a prefix.
false|cluster-smoke|scripts/check-public-api.sh
# `.github/` is deliberately NOT watched by cluster-smoke (see the comment
# above the exclusion) — pinned here so `parity`'s opposite stance on the same
# path (below) is a decision visible in the table, not an accident.
false|cluster-smoke|.github/workflows/ci.yml
# --- parity (issue #37) -----------------------------------------------------
# The pin bump, bare and no trailing slash — the same #93 shape, same reason:
# it is the single highest-risk event for this job, since the suites assert
# parity against exactly what this path bumps.
true|parity|vendor/rift
# The boundary that makes the case above meaningful: a sibling directory is
# not the submodule.
false|parity|vendor/rift-other
false|parity|vendor/rift-other/src/lib.rs
# The composition layer under test.
true|parity|crates/rift-cluster-server/src/main.rs
true|parity|crates/rift-cluster-server/tests/passthrough.rs
# The prefix boundary: a similarly-named sibling crate is not it.
false|parity|crates/rift-cluster-server-old/src/lib.rs
# Root lockfile, same reasoning as cluster-smoke's case.
true|parity|Cargo.lock
false|parity|crates/foo/Cargo.lock
# The workflow directory — watched here (unlike cluster-smoke) because the
# job definition that runs the parity suites lives in it.
true|parity|.github/workflows/ci.yml
# A change class parity does not need to watch: the raft/cluster crate can
# break the cluster without touching the open-source composition surface at
# all, which is exactly the boundary `cluster-smoke` exists to cover instead.
false|parity|crates/rift-cluster/src/lib.rs
# A docs-only change cannot affect either the binary or the suites that
# exercise it.
false|parity|README.md
false|parity|docs/rift-cluster-server.md
# The console's own sources (#186) reach no part of the composition parity measures: the console is
# behind a feature no parity lane enables, so with it off there is not even an arm in the front's
# `handle` for these files to affect.
false|parity|web/src/App.tsx
# Nothing changed at all.
false|parity|
CASES

  echo
  if [ "$failures" -eq 0 ]; then
    echo "OK: ${total} path cases behave as specified"
    return 0
  fi

  echo "FAIL: ${failures}/${total} path cases wrong."
  echo
  echo "This table is the specification of which changes run each path-gated"
  echo "job (cluster-smoke, parity). A case failing here means some filter"
  echo "would skip (or run) a change class it should not — and a wrongly-"
  echo "skipped job is a green check that tested nothing. Fix the patterns,"
  echo "not the table, unless the change class genuinely no longer warrants"
  echo "the job."
  return 1
}

job="cluster-smoke"
mode="--decide"

while [ $# -gt 0 ]; do
  case "$1" in
    --job)
      [ $# -ge 2 ] || { echo "usage: $0 [--job cluster-smoke|parity] [--decide | --self-test]" >&2; exit 2; }
      job="$2"
      shift 2
      ;;
    --job=*)
      job="${1#--job=}"
      shift
      ;;
    --self-test|--decide)
      mode="$1"
      shift
      ;;
    *)
      echo "usage: $0 [--job cluster-smoke|parity] [--decide | --self-test]" >&2
      echo "  --job        which watched set --decide uses (default: cluster-smoke)." >&2
      echo "               --self-test ignores this: it always checks every case for" >&2
      echo "               every job, so one call site (build's unconditional step)" >&2
      echo "               still catches a broken table for a job it never selects." >&2
      echo "  --decide     (default) read changed paths on stdin, write run=true/false" >&2
      echo "  --self-test  check the patterns against the case table" >&2
      exit 2
      ;;
  esac
done

select_job "$job"

case "$mode" in
  --self-test) self_test ;;
  --decide) decide ;;
  *)
    echo "usage: $0 [--job cluster-smoke|parity] [--decide | --self-test]" >&2
    exit 2
    ;;
esac
