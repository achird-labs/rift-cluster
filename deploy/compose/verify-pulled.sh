#!/usr/bin/env bash
# Assert the *pull-not-build* path works — the one a user with only Docker has.
#
#     deploy/compose/verify-pulled.sh --check      # static; no daemon, no pull
#     deploy/compose/verify-pulled.sh              # stands the stack up for real
#     RIFT_CLUSTER_VERSION=0.2.0 deploy/compose/verify-pulled.sh
#
# `verify.sh` proves the manifests in this directory work *when built from this
# checkout*. It cannot prove the published path: it passes `--build`, so a
# `cluster.yml` that named a nonexistent image, or drifted from the topology the
# built variant asserts, would never fail it. This script is that second proof.
#
# The two modes exist because they can run in different places. The full run
# needs a published image, so its only honest home is the release lane after the
# tag is pushed (`.github/workflows/release.yml`, job `image-manifest`). The
# static half needs nothing at all, so it runs on every PR (`ci.yml`, job
# `build`) and is what stops `cluster.yml` rotting in between releases — the
# window where nothing else looks at it.
set -euo pipefail

cd "$(dirname "$0")"

PULLED="cluster.yml"
BUILT="docker-compose.yml"

fail() { echo "FAIL: $*" >&2; exit 1; }

# The tag `cluster.yml` must default to — derived from the workspace version, not
# written down twice. `release.yml`'s `guard` job already refuses to publish a tag
# whose release core disagrees with this version, so tying the quick start's pin
# to the same number is what stops it silently aging into a release nobody can
# pull: bumping the workspace version now fails this check until `cluster.yml` is
# bumped with it.
#
# **No `v`.** `guard` publishes `${GITHUB_REF_NAME#v}`, so git tag `v0.1.0` becomes
# image tag `0.1.0`. Pinning `v0.1.0` here would name a tag that never exists, and
# the documented `docker compose up` would fail with `manifest unknown` for every
# reader — while every check in this file passed, because they would all have been
# agreeing with each other about the wrong string.
#
# Extracted from between the quotes rather than by deleting unwanted characters:
# stripping non-digits turns `0.2.0-rc.1` into `0.2.0.1` and a trailing `# see #266`
# into `0.1.0266`, both of which are wrong but plausible enough to send whoever
# reads the error message the wrong way. Validated as a plain release triple
# afterwards, so anything unexpected fails loudly and says what it read.
read_workspace_version() {
  awk '
    /^\[workspace\.package\]/ { in_wp = 1; next }
    /^\[/                     { in_wp = 0 }
    in_wp && /^version[[:space:]]*=/ {
      if (match($0, /"[^"]+"/)) { print substr($0, RSTART + 1, RLENGTH - 2); exit }
    }' ../../Cargo.toml
}

PINNED_DEFAULT="$(read_workspace_version)"
[ -n "$PINNED_DEFAULT" ] ||
  fail "could not read [workspace.package] version from Cargo.toml"
case "$PINNED_DEFAULT" in
  *[!0-9.]* | '' | *..*) fail "workspace version '${PINNED_DEFAULT}' is not a plain X.Y.Z release version" ;;
esac

# ---------------------------------------------------------------------------
# Static checks — no daemon, no network, no image
# ---------------------------------------------------------------------------
check_static() {
  echo "--- static: $PULLED is a pull-not-build manifest ---"
  [ -f "$PULLED" ] || fail "$PULLED does not exist"

  # The whole point of the file. A `build:` key anywhere means it needs the
  # checkout after all, which is the property this file exists to not have.
  if grep -qE '^\s*build:' "$PULLED"; then
    fail "$PULLED contains a 'build:' key — it must pull, not build"
  fi
  if grep -qE 'dockerfile:|context:|vendor/rift' "$PULLED"; then
    fail "$PULLED references the build context or the vendored submodule"
  fi
  echo "PASS: no build context in $PULLED"

  echo "--- static: every node runs the published image, at the pinned tag ---"
  # Pinned by default, overridable by env — `latest` must be a choice the reader
  # makes, never what they get by default. A file that silently followed `latest`
  # would make "the same file brought up a working cluster last week" untrue
  # without anything changing in it.
  if ! grep -qE "\\\$\{RIFT_CLUSTER_VERSION:-${PINNED_DEFAULT}\}" "$PULLED"; then
    fail "$PULLED must default to the workspace version via \${RIFT_CLUSTER_VERSION:-${PINNED_DEFAULT}}"
  fi
  echo "PASS: image tag defaults to ${PINNED_DEFAULT}, overridable via RIFT_CLUSTER_VERSION"

  echo "--- static: the drain coupling survives ---"
  # The rule deploy/README.md states: the kill deadline must exceed the leave
  # timeout, or the graceful leave is cut short and silently becomes a hard one.
  # Checked here because this file is a copy of a topology, and a copy is exactly
  # where a coupling gets half-updated.
  leave="$(awk -F'"' '/RIFT_CLUSTER_LEAVE_TIMEOUT:/ { print $2; exit }' "$PULLED")"
  grace="$(awk '/stop_grace_period:/ { gsub(/[^0-9]/, "", $2); print $2; exit }' "$PULLED")"
  [ -n "$leave" ] && [ -n "$grace" ] ||
    fail "$PULLED is missing RIFT_CLUSTER_LEAVE_TIMEOUT or stop_grace_period"
  [ "$grace" -gt "$leave" ] ||
    fail "stop_grace_period (${grace}s) must exceed RIFT_CLUSTER_LEAVE_TIMEOUT (${leave}s)"
  echo "PASS: stop_grace_period ${grace}s > leave timeout ${leave}s"

  echo "--- static: the two manifests describe the same cluster ---"
  # A user who reads `deploy/README.md` gets one set of ports; whichever file
  # they picked must honour them. Drift here is silent: both files parse, both
  # stand something up, and only the documented curl commands stop working.
  #
  # Per service, not as one pooled bag of strings: comparing sorted `"host:ctr"`
  # across the whole file passes when two services swap port blocks, which is a
  # real regression (`deploy/README.md` documents ports as `N2525` where `N` is
  # the node number) that a pooled comparison reports as identical.
  #
  # `|| true` on the extraction, not on the comparison: `grep`/`awk` finding
  # nothing is a legitimate value to compare — and under `pipefail` an unguarded
  # no-match would abort the script here, before the diff and the named failure
  # below could run. That is the one input this check exists to explain.
  service_ports() {
    awk '
      /^  [a-z][a-z0-9_-]*:$/ { svc = $1; sub(/:$/, "", svc) }
      /"[0-9]+:[0-9]+"/ {
        line = $0
        while (match(line, /"[0-9]+:[0-9]+"/)) {
          print svc " " substr(line, RSTART + 1, RLENGTH - 2)
          line = substr(line, RSTART + RLENGTH)
        }
      }' "$1" | sort || true
  }
  pulled_ports="$(service_ports "$PULLED")"
  built_ports="$(service_ports "$BUILT")"
  # A floor, so the two extractions cannot agree by both being empty. That is the
  # shape this file is most likely to fail as: reindent both manifests, the awk
  # matches nothing in either, `[ "" = "" ]` is true, and a check that verified
  # nothing prints PASS.
  [ -n "$pulled_ports" ] || fail "no published ports matched in $PULLED — the awk pattern broke"
  [ -n "$built_ports" ] || fail "no published ports matched in $BUILT — the awk pattern broke"
  if [ "$pulled_ports" != "$built_ports" ]; then
    echo "published ports differ between $PULLED and $BUILT:" >&2
    diff <(echo "$built_ports") <(echo "$pulled_ports") >&2 || true
    fail "port maps drifted"
  fi
  echo "PASS: same services and published ports as $BUILT"

  # Parse it for real, and resolve the `x-node` anchor while doing so. This is
  # what makes "every node runs the published image" an assertion about all three
  # services rather than about one `image:` line: the anchor means the file has
  # exactly one, so a per-service override added below it would be invisible to
  # grep and would still be what actually ran.
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    docker compose -f "$PULLED" config -q || fail "$PULLED is not valid compose YAML"
    resolved="$(RIFT_CLUSTER_VERSION="$PINNED_DEFAULT" docker compose -f "$PULLED" config --images | sort -u)"
    [ -n "$resolved" ] || fail "docker compose config --images resolved no images for $PULLED"
    expected="ghcr.io/achird-labs/rift-cluster-server:${PINNED_DEFAULT}"
    if [ "$resolved" != "$expected" ]; then
      echo "resolved images:" >&2
      echo "$resolved" >&2
      fail "every service must resolve to ${expected}"
    fi
    echo "PASS: all services resolve to ${expected}"
  elif [ -n "${CI:-}" ]; then
    # Only a skip off CI. On a runner the plugin is always present, so a skip
    # there means the environment changed under the one enforcement point this
    # check has — and a silent downgrade to "no structural check ran" is exactly
    # the green-that-tested-nothing this file is built to avoid.
    fail "docker compose is unavailable in CI — the structural check cannot be skipped here"
  else
    echo "SKIP: no docker compose available locally for the parse and image checks"
  fi

  check_quick_starts
}

# No quick start may begin from a checkout unless it says so in its own heading.
# This is the acceptance criterion that is easiest to regress by accident: adding
# a clone back to a "Quick start" is a one-line edit that reads as helpful.
check_quick_starts() {
  echo "--- static: no quick start begins with a clone ---"
  local bad=0 scanned=0 doc

  while IFS= read -r doc; do
    scanned=$((scanned + 1))
    # A quick-start section runs until the next heading of the SAME OR SHALLOWER
    # depth. Clearing on every heading — which is the obvious way to write this —
    # ends the section at its own first sub-heading, so a clone under
    # `## Quick start` → `### Docker` would sail through. `deploy/README.md` has
    # exactly that shape (`## Quick start` → `### A single node`), so the obvious
    # version would have left half of this PR's own quick start unguarded.
    #
    # A sub-heading naming "from source" opts its subsection out again, which is
    # the documented escape hatch rather than an exception to the rule.
    awk -v doc="$doc" '
      /^#+/ {
        match($0, /^#+/)
        lvl = RLENGTH
        if (in_qs && lvl <= qs_lvl) in_qs = 0
        if (tolower($0) ~ /quick start/ && tolower($0) !~ /from source/) {
          in_qs = 1
          qs_lvl = lvl
        } else if (in_qs && tolower($0) ~ /from source/) {
          in_qs = 0
        }
        next
      }
      in_qs && /git clone/ { print doc ": " $0; found = 1 }
      END { exit(found ? 1 : 0) }
    ' "$doc" || bad=1
  done < <(find ../.. -name '*.md' -not -path '*/vendor/*' -not -path '*/node_modules/*' -not -path '*/target/*' -not -path '*/.claude/*')

  # `find` runs in a process substitution, so its exit status is unreachable and a
  # wrong root would read as "no offending files" — a clean pass over nothing.
  # Assert the scan actually happened, and that it covered the two files the
  # criterion is really about.
  [ "$scanned" -gt 0 ] || fail "scanned no markdown at all — the find root is wrong"
  for required in ../../README.md ../../deploy/README.md; do
    [ -f "$required" ] || fail "expected ${required} to exist and be scanned"
  done

  [ "$bad" -eq 0 ] ||
    fail "a 'Quick start' section clones the repo — retitle it 'from source' or lead with the pull path"
  echo "PASS: ${scanned} markdown files scanned; every clone lives under a 'from source' heading"
}

# ---------------------------------------------------------------------------
# Full run — needs the published image
# ---------------------------------------------------------------------------
run_stack() {
  local version="${RIFT_CLUSTER_VERSION:-$PINNED_DEFAULT}"
  export RIFT_CLUSTER_VERSION="$version"
  local compose=(docker compose -f "$PULLED")

  cleanup() {
    echo "--- tearing down ---"
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  }
  trap cleanup EXIT

  echo "--- pulling and starting 3 nodes at ${version} ---"
  # `up -d` alone would use a stale local image of the same tag if one exists,
  # which on a release runner is precisely the image just built — so the "pulled"
  # in this script's name would be a claim rather than a fact.
  "${compose[@]}" pull -q
  "${compose[@]}" up -d --no-build

  # Readiness, not liveness: a node answers /healthz long before it has joined,
  # so waiting on that would prove nothing about the cluster forming.
  echo "--- waiting for all three to report ready ---"
  ready=0
  for _ in $(seq 1 90); do
    ready=0
    for port in 12526 22526 32526; do
      if curl -fsS --max-time 2 "http://127.0.0.1:${port}/readyz" >/dev/null 2>&1; then
        ready=$((ready + 1))
      fi
    done
    [ "$ready" -eq 3 ] && break
    sleep 2
  done
  if [ "$ready" -ne 3 ]; then
    "${compose[@]}" ps
    "${compose[@]}" logs --tail=40
    fail "only ${ready}/3 nodes became ready"
  fi
  echo "PASS: 3/3 ready"

  # One cluster, not three single-node clusters that each happen to be ready —
  # what a broken seed configuration produces, and what readiness alone misses.
  # Polled because the fleet gauges are sampled on a 5s timer, so asserting
  # immediately races the sampler and fails on a healthy cluster.
  #
  # `|| true` on every metrics read below is load-bearing, not defensive noise:
  # `$(curl | awk)` under `set -euo pipefail` aborts the whole script on the first
  # connection refusal, which is the *expected* state in the seconds between
  # readiness and the exporter being live. Without it the retry loop these reads
  # sit inside can never reach its second iteration, and a healthy-but-slow
  # cluster fails the release with a bare curl exit status instead of the named
  # diagnostic below. An empty read is not mistaken for success — it fails the
  # `case` match and leaves the counter short.
  echo "--- asserting one cluster of three voters ---"
  voters=""
  for _ in $(seq 1 20); do
    voters="$(curl -fsS --max-time 5 "http://127.0.0.1:19090/metrics" 2>/dev/null \
      | awk '/^rift_cluster_members\{state="voter"\}/ { print $2 }' || true)"
    case "$voters" in 3 | 3.0) break ;; esac
    sleep 2
  done
  case "$voters" in
    3 | 3.0) echo "PASS: single 3-voter cluster" ;;
    *) fail "expected 3 voters, got '${voters:-<none>}'" ;;
  esac

  echo "--- asserting exactly one leader across the fleet ---"
  leaders=0
  for _ in $(seq 1 20); do
    leaders=0
    for port in 19090 29090 39090; do
      v="$(curl -fsS --max-time 5 "http://127.0.0.1:${port}/metrics" 2>/dev/null \
        | awk '/^rift_cluster_members\{state="leader"\}/ { print $2 }' || true)"
      case "$v" in 1 | 1.0) leaders=$((leaders + 1)) ;; esac
    done
    [ "$leaders" -eq 1 ] && break
    sleep 2
  done
  [ "$leaders" -eq 1 ] || fail "expected exactly 1 leader, found ${leaders}"
  echo "PASS: exactly 1 leader"

  echo "--- asserting the console is served on every node ---"
  for port in 12525 22525 32525; do
    # Deliberately not `curl -f`: under `set -e` a non-2xx aborts before the
    # diagnostic below could name which of the two failures it was.
    status="$(curl -sS -o "/tmp/pulled-console.$$" -w '%{http_code}' --max-time 5 \
      "http://127.0.0.1:${port}/console/" || echo 000)"
    body="$(cat "/tmp/pulled-console.$$" 2>/dev/null || true)"
    rm -f "/tmp/pulled-console.$$"
    if [ "$status" != "200" ]; then
      echo "      404 => published image built without --features console" >&2
      echo "      500 => built with the feature but an empty web/dist" >&2
      fail "node on ${port} answered ${status} for /console/"
    fi
    # On the shell, not merely the status: rust-embed over an empty web/dist
    # compiles happily and answers 200 with an explanatory body, which would
    # satisfy a status-only check while serving no console at all.
    case "$body" in
      *"<div id=\"root\""*) ;;
      *) fail "node on ${port} answered 200 but served no SPA shell" ;;
    esac
  done
  echo "PASS: console served on 3/3"

  echo
  echo "ALL CHECKS PASSED (pulled image ${version})"
}

case "${1:---all}" in
  --check) check_static ;;
  --all)
    check_static
    run_stack
    ;;
  *)
    echo "usage: $0 [--check | --all]" >&2
    exit 2
    ;;
esac
