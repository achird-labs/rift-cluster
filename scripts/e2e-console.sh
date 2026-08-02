#!/usr/bin/env bash
# The fixture the browser tests drive: one console-enabled node, seeded deterministically.
#
#     scripts/e2e-console.sh up     # build if needed, start, seed, write the fixture file, return
#     scripts/e2e-console.sh serve  # the same, then block — what Playwright's webServer runs
#     scripts/e2e-console.sh down   # stop and wipe
#
# **One node, not three.** Every console screen is per-node — `/_fleet/*` is one node answering
# about itself — so a single node exercises all seven screens, and it removes the whole class of
# flakiness that leader election and voter-count convergence introduce. The cluster's *own*
# behaviour is covered by `crates/rift-cluster/tests/cluster.rs` and the container chaos tier, which
# are the right places for it. `--cluster-allow-solo` makes this a real one-voter cluster rather
# than a non-clustered node, so the fleet screen renders its single-node state rather than 404ing.
#
# Everything below is fixed — ports, keys, imposters, traffic — because a visual baseline diffed
# against a fixture that varies is a test that fails for reasons nobody changed.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
RUN="${RUN_DIR:-${TMPDIR:-/tmp}/rift-e2e}"
BIN="${REPO}/target/debug/rift-cluster-server"
FIXTURE="${REPO}/web/e2e/.fixture.json"

ADMIN_PORT=3525
PROBE_PORT=3526
METRICS_PORT=3591
PEER_PORT=3790
BOOTSTRAP_KEY="e2e-bootstrap-fleet-admin"

api() {
  method="$1"; path="$2"; shift 2
  curl -fsS -X "$method" "http://127.0.0.1:${ADMIN_PORT}${path}" \
    -H "Authorization: ${BOOTSTRAP_KEY}" -H 'Content-Type: application/json' "$@"
}

mint() {
  api POST "/admin/tenants/$1/principals" -d "{\"displayName\":\"$3\",\"role\":\"$2\"}" \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["apiKey"])'
}

case "${1:-up}" in
  down)
    if [ -f "${RUN}/node.pid" ]; then
      # SIGKILL, not SIGTERM: a graceful stop LEAVES the raft membership and writes a `departed`
      # marker, so the state dir cannot be restarted from. A fixture is torn down, never retired.
      kill -9 "$(cat "${RUN}/node.pid")" 2>/dev/null || true
      rm -f "${RUN}/node.pid"
    fi
    rm -rf "${RUN}" "${FIXTURE}"
    echo "e2e fixture down"
    ;;

  up)
    [ -x "${BIN}" ] || {
      echo "building rift-cluster-server --features console (needs web/dist)…"
      cargo build --locked -p rift-cluster-server --features console --manifest-path "${REPO}/Cargo.toml"
    }
    bash "$0" down >/dev/null 2>&1 || true
    mkdir -p "${RUN}/data" "$(dirname "${FIXTURE}")"

    "${BIN}" \
      --host 127.0.0.1 --port "${ADMIN_PORT}" --datadir "${RUN}/data" \
      --api-key "${BOOTSTRAP_KEY}" --metrics-port "${METRICS_PORT}" \
      --cluster --cluster-bind "127.0.0.1:${PEER_PORT}" \
      --cluster-advertise "127.0.0.1:${PEER_PORT}" \
      --cluster-secret e2e-cluster-secret \
      --cluster-probe-bind "127.0.0.1:${PROBE_PORT}" \
      --cluster-state-dir "${RUN}/_cluster" \
      --cluster-node-name e2e-1 --cluster-allow-solo \
      >"${RUN}/node.log" 2>&1 &
    echo $! > "${RUN}/node.pid"

    # Readiness, not liveness: /healthz answers long before the node can serve admin routes.
    ready=""
    for _ in $(seq 1 120); do
      if curl -fsS --max-time 2 "http://127.0.0.1:${PROBE_PORT}/readyz" >/dev/null 2>&1; then
        ready=yes; break
      fi
      sleep 0.5
    done
    [ -n "$ready" ] || { echo "FAIL: node never became ready"; tail -30 "${RUN}/node.log"; exit 1; }

    # --- tenants and one principal per role ------------------------------------
    api POST /admin/tenants -d '{"id":"acme","displayName":"Acme Corp"}' >/dev/null
    VIEWER=$(mint default viewer "E2E Viewer")
    OPERATOR=$(mint default operator "E2E Operator")
    EDITOR=$(mint default editor "E2E Editor")
    TENANT_ADMIN=$(mint default tenant-admin "E2E Tenant Admin")
    ACME_EDITOR=$(mint acme editor "E2E Acme Editor")

    # fleet-admin cannot be minted: `*` is not a valid tenant id for the principals route, and
    # `fleet-admin` is refused inside an ordinary tenant. Mint at the floor, promote on the fleet
    # scope, then drop the tenant binding so the principal holds `*` alone.
    resp=$(api POST /admin/tenants/default/principals \
      -d '{"displayName":"E2E Fleet Admin","role":"viewer"}')
    pid=$(printf '%s' "$resp" | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
    FLEET_ADMIN=$(printf '%s' "$resp" | python3 -c 'import sys,json;print(json.load(sys.stdin)["apiKey"])')
    api PUT "/admin/tenants/*/bindings/${pid}" -d '{"role":"fleet-admin"}' >/dev/null
    api DELETE "/admin/tenants/default/bindings/${pid}" >/dev/null

    # --- imposters, stubs, and traffic ------------------------------------------
    api POST /imposters -d '{
      "port": 4645, "protocol": "http", "name": "checkout-api", "recordRequests": true,
      "stubs": [
        {"id":"get-order","predicates":[{"equals":{"method":"GET","path":"/orders/42"}}],
         "responses":[{"is":{"statusCode":200,"headers":{"Content-Type":"application/json"},"body":"{\"id\":42}"}}]},
        {"id":"create-order","predicates":[{"equals":{"method":"POST","path":"/orders"}}],
         "responses":[{"is":{"statusCode":201}}]}
      ]}' >/dev/null
    api POST /imposters -d '{
      "port": 4646, "protocol": "http", "name": "inventory-api", "recordRequests": true,
      "stubs": [{"id":"stock","predicates":[{"equals":{"method":"GET","path":"/stock"}}],
                 "responses":[{"is":{"statusCode":200,"body":"{\"qty\":7}"}}]}]}' >/dev/null

    # A fixed number of requests, including deliberate misses so the match diagnostics have both
    # an "matched" and an "unmatched" row to render.
    for _ in 1 2 3; do curl -s -o /dev/null "http://127.0.0.1:4645/orders/42" || true; done
    curl -s -o /dev/null -X POST "http://127.0.0.1:4645/orders" -d '{}' || true
    curl -s -o /dev/null "http://127.0.0.1:4645/orders/99" || true
    curl -s -o /dev/null "http://127.0.0.1:4646/stock" || true

    # --- a front-door route, so that screen has a row ----------------------------
    api PUT /front-door/routes -d '{"routes":[{"id":"checkout","priority":10,
      "match":{"path_prefix":"/checkout"},"target":{"port":4645,"strip_prefix":true},"enabled":true}]}' \
      >/dev/null 2>&1 || true

    # --- the readiness sentinel, created LAST ------------------------------------
    #
    # `playwright.config.ts` waits on this imposter, not on `/console/`. The console is served the
    # moment the node binds a socket — before a single tenant, principal or imposter exists — so
    # waiting on it starts the suite mid-seed. That is not theoretical: it raced in CI and failed
    # the first two visual specs while every later one passed, which reads like two flaky tests
    # rather than a fixture that had not finished.
    #
    # An imposter is the right sentinel because gateway traffic is auth-exempt (RFC-002 §7), so the
    # probe needs no key, and because it can only answer once every step above it has committed.
    # Created in `acme`, not `default`, so it stays out of the imposter table the visual specs
    # capture — a sentinel that changed the baseline would be a fixture detail leaking into the
    # thing under test. Its port still serves traffic regardless of tenant.
    api POST /imposters -H "X-Rift-Tenant: acme" -d '{
      "port": 4699, "protocol": "http", "name": "e2e-ready", "recordRequests": false,
      "stubs": [{"responses":[{"is":{"statusCode":200,"body":"seeded"}}]}]}' >/dev/null

    python3 - "$FIXTURE" "$ADMIN_PORT" "$BOOTSTRAP_KEY" \
      "$VIEWER" "$OPERATOR" "$EDITOR" "$TENANT_ADMIN" "$FLEET_ADMIN" "$ACME_EDITOR" <<'PY'
import json, sys
out, port, bootstrap, viewer, operator, editor, tadmin, fadmin, acme = sys.argv[1:10]
json.dump({
    "baseURL": f"http://127.0.0.1:{port}",
    "keys": {
        "bootstrap": bootstrap, "viewer": viewer, "operator": operator, "editor": editor,
        "tenant-admin": tadmin, "fleet-admin": fadmin, "acme-editor": acme,
    },
    "imposters": [4645, 4646],
}, open(out, "w"), indent=2)
PY
    echo "e2e fixture up on http://127.0.0.1:${ADMIN_PORT}/console/ (keys in ${FIXTURE})"
    ;;

  serve)
    # `up`, then block on the node.
    #
    # Playwright's `webServer` requires a command that stays in the foreground; `up` backgrounds the
    # node and returns, which it reports as "Process from config.webServer exited early". It only
    # showed up in CI: locally `reuseExistingServer` is true, so Playwright found a fixture already
    # running and never executed the command at all.
    #
    # Polled, not `wait`: the node is backgrounded inside the `bash "$0" up` subshell, so it is not
    # a child of this one and `wait` answers "not a child of this shell" (exit 127, which Playwright
    # reports as "webServer was not able to start"). Polling also still exits when the node dies, so
    # Playwright fails fast rather than running every spec against a dead port.
    bash "$0" up
    node_pid="$(cat "${RUN}/node.pid")"
    while kill -0 "$node_pid" 2>/dev/null; do sleep 1; done
    echo "e2e fixture: node ${node_pid} exited"
    ;;

  *) echo "usage: $0 {up|serve|down}"; exit 2 ;;
esac
