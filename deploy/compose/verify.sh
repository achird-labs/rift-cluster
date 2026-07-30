#!/usr/bin/env bash
# Stand the compose cluster up and assert it actually forms, then tear it down.
#
#     deploy/compose/verify.sh
#
# This is the check that the manifests in this directory *work*, as opposed to
# merely parsing. It is deliberately a script rather than a test in the Rust
# suite: it needs a container runtime, so it cannot run in the workspace's
# `cargo test` and must not be able to fail CI for an unrelated reason.
set -euo pipefail

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
echo "--- building and starting 3 nodes (upstream pin: ${RIFT_UPSTREAM_VERSION}) ---"
"${COMPOSE[@]}" up -d --build

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
# readiness check alone would not catch.
# Poll rather than read once: the fleet gauges are SAMPLED on a timer (5s), so
# a node can be ready a moment before its own metrics catch up. Asserting
# immediately races the sampler and fails on a healthy cluster.
echo "--- asserting the three agree on one cluster ---"
voters=""
for _ in $(seq 1 20); do
  voters="$(curl -fsS --max-time 5 "http://127.0.0.1:19090/metrics" 2>/dev/null \
    | awk '/^rift_cluster_members\{state="voter"\}/ { print $2 }')"
  case "$voters" in 3|3.0) break ;; esac
  sleep 2
done
echo "rift-1 reports voters=${voters:-<none>}"
case "$voters" in
  3|3.0) echo "PASS: single 3-voter cluster" ;;
  *) echo "FAIL: expected 3 voters, got '${voters:-<none>}'"
     "${COMPOSE[@]}" logs --tail=40
     exit 1 ;;
esac

# Exactly one leader across the fleet. Summing the gauge is the query the
# metric was shaped for; two leaders means a split brain.
leaders=0
for _ in $(seq 1 20); do
  leaders=0
  for port in 19090 29090 39090; do
    v="$(curl -fsS --max-time 5 "http://127.0.0.1:${port}/metrics" 2>/dev/null \
      | awk '/^rift_cluster_members\{state="leader"\}/ { print $2 }')"
    case "$v" in 1|1.0) leaders=$((leaders + 1)) ;; esac
  done
  [ "$leaders" -eq 1 ] && break
  sleep 2
done
if [ "$leaders" -ne 1 ]; then
  echo "FAIL: expected exactly 1 leader, found ${leaders}"
  exit 1
fi
echo "PASS: exactly 1 leader"

# An imposter created on one node is the config-sync deliverable and does NOT
# replicate yet (that lands with the config-sync work), so this only asserts
# the admin API is live on every node — not that the imposter appears on all.
echo "--- asserting the admin API serves on every node ---"
for port in 12525 22525 32525; do
  curl -fsS --max-time 5 "http://127.0.0.1:${port}/imposters" >/dev/null
done
echo "PASS: admin API live on 3/3"

# The image must identify itself, including which engine is inside it — the
# whole point of plumbing the pin through the build.
echo "--- asserting the image reports its identity ---"
banner="$("${COMPOSE[@]}" exec -T rift-1 rift-cluster-server --version)"
echo "$banner"
case "$banner" in
  *"cluster"*) ;;
  *) echo "FAIL: --version does not name the edition"; exit 1 ;;
esac
if [ "$RIFT_UPSTREAM_VERSION" != "unknown" ]; then
  case "$banner" in
    *"$RIFT_UPSTREAM_VERSION"*) echo "PASS: image reports upstream ${RIFT_UPSTREAM_VERSION}" ;;
    *) echo "FAIL: image did not report upstream pin ${RIFT_UPSTREAM_VERSION}"; exit 1 ;;
  esac
fi

echo
echo "ALL CHECKS PASSED"
