#!/usr/bin/env bash
# Stand the compose cluster up and assert it actually forms, then tear it down.
#
#     deploy/compose/verify.sh              # build the image, start 3 nodes, assert
#     deploy/compose/verify.sh --no-build   # reuse an image already in the docker store
#
# This is the check that the manifests in this directory *work*, as opposed to
# merely parsing. It is deliberately a script rather than a test in the Rust
# suite: it needs a container runtime, so it cannot run in the workspace's
# `cargo test` and must not be able to fail CI for an unrelated reason.
#
# CI runs it in the `compose-smoke` job (`.github/workflows/ci.yml`), which is
# what stops this file rotting: it went one whole release cycle with no invoker
# at all after the lane that used to run it was retired, and nothing noticed
# because a script nobody runs cannot go red. `--no-build` exists for that lane —
# `cluster-smoke-prepare` has already built the image once (D-58) and a rebuild
# there would cost the ten minutes that job exists to pay only once.
set -euo pipefail

# Every assertion below reads `/_fleet/members` through `jq`. Without it every
# read yields the empty string, every comparison fails, and the script reports a
# broken cluster — a diagnosis that would send the reader after the wrong thing.
command -v jq >/dev/null || { echo "verify.sh requires jq" >&2; exit 2; }

# Before the `cd` below, deliberately: `--help` reads this file back through
# `$0`, and after changing directory a relatively-invoked `$0` no longer names
# anything. `smoke.sh` parses in the same order, for the same reason.
BUILD=1
for arg in "$@"; do
  case "$arg" in
    --no-build) BUILD=0 ;;
    -h|--help)  sed -n '2,5p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

cd "$(dirname "$0")"
COMPOSE=(docker compose -f docker-compose.yml)

cleanup() {
  echo "--- tearing down ---"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Discover the pin here rather than in the Dockerfile: this script runs on a
# host that has the checkout, the build does not.
RIFT_UPSTREAM_VERSION="$(git -C ../../vendor/rift describe --tags --always 2>/dev/null || echo unknown)"
export RIFT_UPSTREAM_VERSION
if [ "$BUILD" -eq 1 ]; then
  echo "--- building and starting 3 nodes (upstream pin: ${RIFT_UPSTREAM_VERSION}) ---"
  "${COMPOSE[@]}" up -d --build
else
  echo "--- starting 3 nodes on the image already in the store ---"
  "${COMPOSE[@]}" up -d --no-build
fi

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
  echo "FAIL: only ${ready}/3 nodes became ready"
  "${COMPOSE[@]}" ps
  "${COMPOSE[@]}" logs --tail=40
  exit 1
fi
echo "PASS: 3/3 ready"

# One cluster, not three single-node clusters that each happen to be ready —
# which is exactly what a broken seed configuration produces, and what a
# readiness check alone would not catch. Read from `GET /_fleet/members` on
# the admin port, the same view `smoke.sh` asserts on (#555): a voter list and
# an agreed leader id are stronger facts than the two gauges this used to sum
# (retired by D-71, #548). Polled, because the fleet forms asynchronously after
# every node reports ready.
#
# `|| true` is load-bearing under `set -euo pipefail`: a node that has not
# started answering yet makes `curl` exit non-zero, which would otherwise kill
# the script before the retry loop could retry. An empty read is not mistaken
# for success — it fails the comparison and the loop goes round again.
members() { curl -fsS --max-time 5 "http://127.0.0.1:${1}/_fleet/members" 2>/dev/null; }
ADMIN_PORTS=(12525 22525 32525)

# One node's whole answer as `voters|leaders|reachable`, or the empty string when
# it could not be read. Three facts from one request rather than three, so the
# node cannot be observed mid-change between them.
#
# `.members[] | select(.is_leader)` counted, not `.current_leader` tested: a fleet
# where two members each believe they lead is a split brain that a non-null
# leader id reads straight past. These are the shapes `smoke.sh` asserts at its
# `== cluster ==` section, deliberately identical so the two files cannot drift
# into checking different things.
fleet_shape() {
  members "$1" | jq -r '
      "\(.voters | length)"
      + "|\([.members[] | select(.is_leader)] | length)"
      + "|\([.members[] | select(.reachable)] | length)"
    ' 2>/dev/null || true
}

echo "--- asserting the three agree on one cluster ---"
shapes=""
for _ in $(seq 1 20); do
  shapes=""
  for port in "${ADMIN_PORTS[@]}"; do
    shapes="${shapes}${shapes:+ }$(fleet_shape "$port")"
  done
  [ "$shapes" = "3|1|3 3|1|3 3|1|3" ] && break
  sleep 2
done
echo "voters|leaders|reachable, per node: ${shapes:-<none>}"
if [ "$shapes" = "3|1|3 3|1|3 3|1|3" ]; then
  echo "PASS: every node sees 3 voters, exactly one leader and 3 reachable members"
else
  echo "FAIL: expected '3|1|3 3|1|3 3|1|3' (voters|leaders|reachable per node),"
  echo "      got '${shapes:-<none>}'"
  echo "      an empty field is a node that could not be read at all; a '0' in the"
  echo "      middle column is a fleet still electing, a '2' is a split brain"
  "${COMPOSE[@]}" logs --tail=40
  exit 1
fi

# One agreed leader: every node names the same, non-null `current_leader`.
# Two nodes naming different leaders is a split brain; a node naming none is
# still electing.
agreed=""
for _ in $(seq 1 20); do
  l1="$(members 12525 | jq -r '.current_leader // ""' 2>/dev/null || true)"
  l2="$(members 22525 | jq -r '.current_leader // ""' 2>/dev/null || true)"
  l3="$(members 32525 | jq -r '.current_leader // ""' 2>/dev/null || true)"
  if [ -n "$l1" ] && [ "$l1" = "$l2" ] && [ "$l1" = "$l3" ]; then
    agreed="$l1"
    break
  fi
  sleep 2
done
if [ -z "$agreed" ]; then
  echo "FAIL: the nodes do not agree on one leader (rift-1: '${l1:-}', rift-2: '${l2:-}', rift-3: '${l3:-}')"
  exit 1
fi
echo "PASS: every node names leader ${agreed}"

# An imposter created on one node is the config-sync deliverable and does NOT
# replicate yet (that lands with the config-sync work), so this only asserts
# the admin API is live on every node — not that the imposter appears on all.
echo "--- asserting the admin API serves on every node ---"
for port in 12525 22525 32525; do
  curl -fsS --max-time 5 "http://127.0.0.1:${port}/imposters" >/dev/null
done
echo "PASS: admin API live on 3/3"

# The console, on every node (#265).
#
# This is what makes "the image carries the console" a *verified* row in
# `deploy/README.md` rather than an assumed one. The build already has its own
# guards — `wasm-pack` output asserted, `dist/index.html` asserted,
# `--features console` compiled — but every one of those is a statement about
# the build, and a binary that compiled the feature can still serve nothing if
# the embed folder was empty. Asking a *running* node is the only check that
# cannot pass vacuously.
#
# Asserted on the SPA shell, not merely on a 200: `rust-embed` over an empty
# `web/dist` compiles perfectly happily, and `console.rs` answers that case with
# a deliberate explanatory body. A status-only check would pass on exactly the
# image this assertion exists to catch.
echo "--- asserting the console is served on every node ---"
for port in 12525 22525 32525; do
  # Deliberately NOT `curl -f`. Under `set -e` a non-2xx would abort the script
  # before the diagnostic below could run, so the two failures this check exists
  # to name — a 404 from an image built without `--features console`, and a 500
  # from one built with an empty `web/dist` — would both die with no message but
  # the teardown trap. Capturing the status separately is what makes the
  # explanation reachable on exactly the runs that need it.
  status="$(curl -sS -o /tmp/console-body.$$ -w '%{http_code}' --max-time 5 \
    "http://127.0.0.1:${port}/console/" || echo 000)"
  body="$(cat /tmp/console-body.$$ 2>/dev/null || true)"
  rm -f "/tmp/console-body.$$"

  if [ "$status" != "200" ]; then
    echo "FAIL: node on ${port} answered ${status} for /console/"
    echo "      404 => image built without --features console"
    echo "      500 => built with the feature but an empty web/dist"
    exit 1
  fi
  # Asserted on the shell, not merely on the status: `rust-embed` over an empty
  # `web/dist` compiles happily, and a 200 carrying an explanatory body would
  # satisfy a status-only check while serving no console at all.
  case "$body" in
    *"<div id=\"root\""*) ;;
    *)
      echo "FAIL: node on ${port} answered 200 but served no SPA shell at /console/"
      exit 1
      ;;
  esac
done
echo "PASS: console served on 3/3"

# The image must identify itself, including which engine is inside it — the
# whole point of plumbing the pin through the build.
echo "--- asserting the image reports its identity ---"
banner="$("${COMPOSE[@]}" exec -T rift-1 rift-cluster-server --version)"
echo "$banner"
case "$banner" in
  *"cluster"*) ;;
  *) echo "FAIL: --version does not name the edition"; exit 1 ;;
esac
# Only when this run built the image. Under `--no-build` the bytes came from
# somewhere else — in CI, from `cluster-smoke-prepare`, which deliberately does
# not pass `RIFT_UPSTREAM_VERSION` — so the pin read out of the checkout above is
# not a claim this script has any basis to make about the image it just started.
if [ "$BUILD" -eq 1 ] && [ "$RIFT_UPSTREAM_VERSION" != "unknown" ]; then
  case "$banner" in
    *"$RIFT_UPSTREAM_VERSION"*) echo "PASS: image reports upstream ${RIFT_UPSTREAM_VERSION}" ;;
    *) echo "FAIL: image did not report upstream pin ${RIFT_UPSTREAM_VERSION}"; exit 1 ;;
  esac
fi

echo
echo "ALL CHECKS PASSED"
