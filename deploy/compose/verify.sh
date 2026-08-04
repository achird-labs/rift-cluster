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

# The observability overlay (#227) is not part of the default compose smoke:
# it pulls two extra images and the default smoke has to stay dependency-free.
# Gated on RIFT_OBSERVABILITY=1 so nothing below runs, and nothing above
# changes, on a plain `deploy/compose/verify.sh`.
if [ "${RIFT_OBSERVABILITY:-0}" = "1" ]; then
  echo "--- RIFT_OBSERVABILITY=1: bringing up the observability overlay ---"
  OBS_COMPOSE=(docker compose -f docker-compose.yml -f observability.overlay.yml)
  # rift-1/2/3 are already up from the `up -d --build` above; this layers in
  # only prometheus and grafana. `down -v --remove-orphans` in the cleanup
  # trap still tears both of them down even though it only knows about
  # docker-compose.yml -- that is exactly what --remove-orphans is for.
  "${OBS_COMPOSE[@]}" up -d prometheus grafana

  # Fixed dev-only credential, matching observability.overlay.yml's
  # GF_SECURITY_ADMIN_PASSWORD. Not production guidance -- see that file.
  GRAFANA_AUTH="admin:rift-observability-dev-only"

  echo "--- asserting Prometheus scrapes 3/3 targets ---"
  up_targets=0
  for _ in $(seq 1 30); do
    # `|| true` is load-bearing under this script's `set -euo pipefail`. On the
    # first poll Prometheus has not completed a scrape, so `grep` matches
    # nothing and exits 1; `pipefail` promotes that to the pipeline, the
    # assignment fails, and `set -e` kills the script *before* the retry loop
    # can retry and before the FAIL branch below can print. The symptom is the
    # worst kind: the run dies straight after the header with no diagnostic at
    # all. `wc -l` still emits `0` for empty input, so the rescued value is the
    # honest count. The Grafana loop below was already written this way
    # (`|| echo 000`); this one was not, and nothing noticed because until
    # issue #316 no CI lane ever ran this block.
    up_targets="$(curl -fsS --max-time 5 "http://127.0.0.1:19091/api/v1/targets" 2>/dev/null \
      | grep -o '"health":"up"' | wc -l | tr -d ' ' || true)"
    [ "${up_targets:-0}" -eq 3 ] && break
    sleep 2
  done
  if [ "${up_targets:-0}" -ne 3 ]; then
    echo "FAIL: expected 3 up targets, got '${up_targets:-0}'"
    curl -fsS --max-time 5 "http://127.0.0.1:19091/api/v1/targets" || true
    # Dumped here, not by the caller: `trap cleanup EXIT` above runs
    # `compose down` on every exit path, so by the time any wrapper could ask
    # for logs the containers are already gone and it gets silence.
    "${OBS_COMPOSE[@]}" logs --tail=100 prometheus || true
    exit 1
  fi
  echo "PASS: Prometheus reports 3/3 targets up"

  echo "--- asserting Grafana serves the three dashboards ---"
  for uid in rift-fleet-overview rift-latency-analytics rift-verification-plane; do
    status=0
    for _ in $(seq 1 30); do
      status="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 \
        -u "$GRAFANA_AUTH" "http://127.0.0.1:13000/api/dashboards/uid/${uid}" || echo 000)"
      [ "$status" = "200" ] && break
      sleep 2
    done
    if [ "$status" != "200" ]; then
      echo "FAIL: dashboard uid '${uid}' answered ${status}, expected 200"
      # Same reason as the Prometheus branch: the EXIT trap tears the stack
      # down, so logs have to be taken before it fires. A provisioning error is
      # only ever visible in Grafana's own log.
      "${OBS_COMPOSE[@]}" logs --tail=100 grafana || true
      exit 1
    fi
  done
  echo "PASS: Grafana serves 3/3 dashboards"
fi

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
if [ "$RIFT_UPSTREAM_VERSION" != "unknown" ]; then
  case "$banner" in
    *"$RIFT_UPSTREAM_VERSION"*) echo "PASS: image reports upstream ${RIFT_UPSTREAM_VERSION}" ;;
    *) echo "FAIL: image did not report upstream pin ${RIFT_UPSTREAM_VERSION}"; exit 1 ;;
  esac
fi

echo
echo "ALL CHECKS PASSED"
