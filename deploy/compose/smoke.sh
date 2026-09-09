#!/usr/bin/env bash
# The core smoke check (issue #555, RFC-007 §5): stand the compose fleet up and
# assert the things the distributed core promises, on a real three-node cluster.
#
#     deploy/compose/smoke.sh              # build, start 3 nodes, assert, tear down
#     deploy/compose/smoke.sh --no-build   # reuse the local image
#     deploy/compose/smoke.sh --keep       # leave the fleet running afterwards
#     deploy/compose/smoke.sh --attach     # assert against a fleet that is already up
#
# `verify.sh` proves the cluster *forms*. This proves it *works*: an imposter
# written on one node answers on every node through the router; a route table
# written on one node is installed on every node; a stopped node catches up; a
# stopped leader is replaced and writes keep landing; a new node joins from a
# seed and serves the same imposters; a departed node leaves the voter set; and
# a scenario's state written on one node gates the next request on another.
#
# Every removal under epic #544 runs this before and after (RFC-007 §5.2). It
# asserts only what the core promises, so it must keep passing as the
# peripheral surfaces leave — a section here that depends on one of them is a
# bug in this script. Flow state is core (D-17, D-20): its section stays.
#
# That makes this the executable statement of **D-71**: the sections below are
# the distributed core, and the fact that every one of them still passes with
# tenancy, RBAC, the audit log, MCP, the operator metrics product, route hits,
# tracking sources and the fleet journal merge all removed is the evidence the
# narrowing took nothing the product needed. It is a shell script, so
# `design-check` counts it as a citation and not as a test pin; the gate is the
# CI lane that runs it, not a `#[test]`.
#
# A script rather than a `cargo test` for the same reason `verify.sh` is: it
# needs a container runtime, so it cannot run in the workspace's `cargo test`
# and must not be able to fail CI for an unrelated reason.
set -uo pipefail

# Before the `cd`, deliberately: `--help` reads this file back through `$0`, and
# after changing directory a relatively-invoked `$0` names nothing.
BUILD=1; KEEP=0; ATTACH=0
for arg in "$@"; do
  case "$arg" in
    --no-build) BUILD=0 ;;
    --keep)     KEEP=1 ;;
    --attach)   ATTACH=1; KEEP=1 ;;
    -h|--help)  sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

# Roughly half the assertions below read JSON through `jq`. Without it they all
# compare against the empty string and fail, and a run with no jq installed looks
# exactly like a fleet that replicates nothing — the most misleading failure this
# script has. `set -e` is deliberately off here (see `eq`), so this is a hard
# exit rather than a first failing command. After argument parsing, so `--help`
# works on a machine without jq.
command -v jq >/dev/null || { echo "smoke.sh requires jq" >&2; exit 2; }

cd "$(dirname "$0")"
export RIFT_SMOKE_KEY="${RIFT_SMOKE_KEY:-rift-smoke-key}"
K="$RIFT_SMOKE_KEY"
COMPOSE=(docker compose -f docker-compose.yml -f smoke.overlay.yml)

# Node n: admin 2525, probes 2526, router 2527, each published as n<port>.
admin()  { echo "http://127.0.0.1:${1}2525"; }
probe()  { echo "http://127.0.0.1:${1}2526"; }
router() { echo "http://127.0.0.1:${1}2527"; }

pass=0; fail=0
# eq LABEL EXPECTED ACTUAL
eq() {
  if [ "$2" = "$3" ]; then printf '  ok    %s\n' "$1"; pass=$((pass+1))
  else printf '  FAIL  %s\n          expected: %s\n          actual:   %s\n' "$1" "$2" "$3"; fail=$((fail+1)); fi
}
body() { curl -s --max-time 10 "$@"; }
code() { curl -s --max-time 10 -o /dev/null -w '%{http_code}' "$@"; }
auth() { body -H "Authorization: $K" "$@"; }
acode() { code -H "Authorization: $K" "$@"; }
# until_eq SECONDS EXPECTED COMMAND... — poll a command until it prints EXPECTED; prints the last value.
until_eq() {
  local secs="$1" want="$2" got=""; shift 2
  for _ in $(seq 1 "$secs"); do
    got="$("$@" 2>/dev/null)"
    [ "$got" = "$want" ] && break
    sleep 1
  done
  printf '%s' "$got"
}
members() { auth "$(admin "$1")/_fleet/members"; }
leader_of() { members "$1" | jq -r '.current_leader // ""'; }
node_id_of() { members "$1" | jq -r '.node_id // ""'; }
voters() { members "$1" | jq -r '.voters | length'; }
reachable() { members "$1" | jq -r '[.members[] | select(.reachable)] | length'; }
ready_count() {
  local n=0
  for node in "$@"; do curl -fsS --max-time 2 "$(probe "$node")/readyz" >/dev/null 2>&1 && n=$((n+1)); done
  echo "$n"
}

cleanup() {
  if [ "$KEEP" -eq 0 ]; then
    echo "--- tearing down ---"
    "${COMPOSE[@]}" --profile join down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------- start
if [ "$ATTACH" -eq 0 ]; then
  RIFT_UPSTREAM_VERSION="$(git -C ../../vendor/rift describe --tags --always 2>/dev/null || echo unknown)"
  export RIFT_UPSTREAM_VERSION
  echo "--- starting 3 nodes (upstream pin: ${RIFT_UPSTREAM_VERSION}) ---"
  if [ "$BUILD" -eq 1 ]; then "${COMPOSE[@]}" up -d --build; else "${COMPOSE[@]}" up -d; fi
fi
echo "--- waiting for 3/3 ready ---"
eq "3/3 nodes ready" "3" "$(until_eq 120 3 ready_count 1 2 3)"
[ "$fail" -eq 0 ] || { "${COMPOSE[@]}" ps; "${COMPOSE[@]}" logs --tail=40; exit 1; }

# Whatever a previous --keep run left behind. Deleting an imposter that does not
# exist is a 404 and fine; the route table is replaced wholesale below.
for p in 7101 7102 7103 7104 7105; do curl -s -o /dev/null -X DELETE "$(admin 1)/imposters/$p" -H "Authorization: $K"; done
curl -s -o /dev/null -X PUT "$(admin 1)/front-door/routes" -H "Authorization: $K" -H 'Content-Type: application/json' -d '{"routes":[]}'

# -------------------------------------------------------------------- cluster
echo
echo "== cluster =="
L="$(until_eq 30 "$(leader_of 1)" leader_of 2)"
eq "every node names the same leader" "$(leader_of 1)|$(leader_of 1)" "$(leader_of 2)|$(leader_of 3)"
eq "and it is somebody" "1" "$([ -n "$(leader_of 1)" ] && echo 1 || echo 0)"
eq "exactly one member claims to lead" "1" "$(members 1 | jq '[.members[] | select(.is_leader)] | length')"
eq "three voters" "3" "$(voters 1)"
eq "all three members reachable" "3" "$(reachable 1)"
eq "same applied index on all nodes" "1" \
   "$(for n in 1 2 3; do members "$n" | jq -r .last_applied; done | sort -u | wc -l | tr -d ' ')"
eq "no imposter port is published by docker" "0" "$(docker port rift-1 2>/dev/null | grep -c '^71[0-9][0-9]/' || true)"
eq "the API key is accepted on every node" "200 200 200" "$(acode "$(admin 1)/imposters") $(acode "$(admin 2)/imposters") $(acode "$(admin 3)/imposters")"
eq "and required" "401" "$(code "$(admin 1)/imposters")"

# ---------------------------------------------------------------- replication
echo
echo "== replication: write on one node, serve on every node =="
eq "POST /imposters 7101 on node 1" "201" "$(acode -X POST "$(admin 1)/imposters" -H 'Content-Type: application/json' -d '{
  "port": 7101, "protocol": "http", "name": "smoke-api", "recordRequests": true,
  "stubs": [
    {"predicates":[{"equals":{"method":"GET","path":"/health"}}],
     "responses":[{"is":{"statusCode":200,"headers":{"Content-Type":"application/json"},"body":"{\"status\":\"up\"}"}}]},
    {"predicates":[{"matches":{"path":"^/orders/[0-9]+$"}}],
     "responses":[{"is":{"statusCode":200,"body":"matched-by-regex"}}]},
    {"predicates":[{"equals":{"method":"POST","path":"/orders"}},{"contains":{"body":"widget"}}],
     "responses":[{"is":{"statusCode":201,"body":"{\"id\":42}"}}]},
    {"predicates":[{"exists":{"headers":{"X-Api-Key":true}}},{"equals":{"path":"/secure"}}],
     "responses":[{"is":{"statusCode":200,"body":"authorised"}}]},
    {"predicates":[{"equals":{"path":"/secure"}}],
     "responses":[{"is":{"statusCode":401,"body":"missing X-Api-Key"}}]},
    {"predicates":[{"matches":{"path":"^/[0-9]+$"}}],
     "responses":[{"is":{"statusCode":200,"body":"strip ON: /api/orders/N reached me as /N"}}]},
    {"predicates":[{"startsWith":{"path":"/api/"}}],
     "responses":[{"is":{"statusCode":200,"body":"strip OFF: I got the whole path"}}]},
    {"responses":[{"is":{"statusCode":404,"body":"no stub matched"}}]}
  ]}')"
eq "GET /imposters/7101 on node 2" "smoke-api" "$(auth "$(admin 2)/imposters/7101" | jq -r .name)"
eq "GET /imposters/7101 on node 3" "smoke-api" "$(auth "$(admin 3)/imposters/7101" | jq -r .name)"
eq "the write carries a cluster revision" "1" \
   "$(curl -s -D - -o /dev/null "$(admin 1)/imposters/7101" -H "Authorization: $K" | grep -ci '^rift-cluster-revision:')"

echo
echo "== router: a route table written on node 2 dispatches on every node =="
eq "PUT /front-door/routes on node 2" "200" "$(acode -X PUT "$(admin 2)/front-door/routes" -H 'Content-Type: application/json' -d '{"routes":[
  {"id":"smoke","priority":50,"enabled":true,"match":{"path_prefix":"/smoke"},"target":{"port":7101,"strip_prefix":true}},
  {"id":"by-host","priority":100,"enabled":true,"match":{"host":"smoke.test"},"target":{"port":7101}},
  {"id":"ingest-post","priority":60,"enabled":true,"match":{"method":"POST","path_prefix":"/ingest"},"target":{"port":7101,"strip_prefix":true}},
  {"id":"orders-strip","priority":50,"enabled":true,"match":{"path_prefix":"/api/orders"},"target":{"port":7101,"strip_prefix":true}},
  {"id":"api-catch","priority":10,"enabled":true,"match":{"path_prefix":"/api"},"target":{"port":7101}},
  {"id":"origin","priority":50,"enabled":true,"match":{"path_prefix":"/origin"},"target":{"port":7102,"strip_prefix":true}},
  {"id":"proxy","priority":50,"enabled":true,"match":{"path_prefix":"/proxy"},"target":{"port":7103,"strip_prefix":true}},
  {"id":"once","priority":50,"enabled":true,"match":{"path_prefix":"/once"},"target":{"port":7104,"strip_prefix":true}}
]}')"
eq "the table reads back on node 3" "8" "$(auth "$(admin 3)/front-door/routes" | jq '.routes | length')"
eq "eight routes replicated to node 1" "8" "$(auth "$(admin 1)/front-door/routes" | jq '.routes | length')"
for n in 1 2 3; do
  eq "node $n routes /smoke/health to 7101" '{"status":"up"}' "$(body "$(router "$n")/smoke/health")"
done
eq "regex predicate"            'matched-by-regex' "$(body "$(router 2)/smoke/orders/99")"
eq "method+body predicate"      '201'              "$(code -X POST "$(router 3)/smoke/orders" -d 'a widget')"
eq "header absent  -> 401"      '401'              "$(code "$(router 1)/smoke/secure")"
eq "header present -> 200"      '200'              "$(code "$(router 1)/smoke/secure" -H 'X-Api-Key: k')"
eq "no stub matched -> 404"     '404'              "$(code "$(router 2)/smoke/nothing-here")"
eq "host match"                 '{"status":"up"}'  "$(body "$(router 3)/health" -H 'Host: smoke.test')"
eq "method match hits"          '201'              "$(code -X POST "$(router 1)/ingest/orders" -d 'widget')"
eq "method match misses -> 404" '404'              "$(code "$(router 1)/ingest/orders")"
eq "higher priority prefix strips" 'strip ON: /api/orders/N reached me as /N' "$(body "$(router 2)/api/orders/7")"
eq "lower priority prefix does not" 'strip OFF: I got the whole path'         "$(body "$(router 2)/api/other")"
eq "unrouted -> 404"            '404'              "$(code "$(router 3)/unrouted")"

echo
echo "== stub CRUD replicates =="
eq "POST /imposters/7101/stubs on node 3" "200" "$(acode -X POST "$(admin 3)/imposters/7101/stubs" -H 'Content-Type: application/json' \
   -d '{"stub":{"predicates":[{"equals":{"path":"/added-later"}}],"responses":[{"is":{"statusCode":200,"body":"added on node 3"}}]},"index":0}')"
eq "the new stub answers on node 1" "added on node 3" "$(body "$(router 1)/smoke/added-later")"
eq "and on node 2"                  "added on node 3" "$(body "$(router 2)/smoke/added-later")"

# ---------------------------------------------------------------------- proxy
echo
echo "== proxying (as open-source Rift does it) =="
acode -X POST "$(admin 1)/imposters" -H 'Content-Type: application/json' -d '{
  "port": 7102, "protocol": "http", "name": "origin",
  "stubs": [{"responses":[{"is":{"statusCode":200,"body":"live #1"}},{"is":{"statusCode":200,"body":"live #2"}},{"is":{"statusCode":200,"body":"live #3"}}]}]}' >/dev/null
acode -X POST "$(admin 1)/imposters" -H 'Content-Type: application/json' -d '{
  "port": 7103, "protocol": "http", "name": "transparent",
  "stubs": [{"responses":[{"proxy":{"to":"http://127.0.0.1:7102","mode":"proxyTransparent"}}]}]}' >/dev/null
acode -X POST "$(admin 1)/imposters" -H 'Content-Type: application/json' -d '{
  "port": 7104, "protocol": "http", "name": "once",
  "stubs": [{"responses":[{"proxy":{"to":"http://127.0.0.1:7102","mode":"proxyOnce","predicateGenerators":[{"matches":{"method":true,"path":true}}]}}]}]}' >/dev/null
eq "three proxies replicated to node 3" "origin|transparent|once" \
   "$(for p in 7102 7103 7104; do auth "$(admin 3)/imposters/$p" | jq -r .name; done | paste -sd'|' -)"
eq "proxyTransparent forwards every call (origin cycles)" "live #1|live #2|live #3" \
   "$(for i in 1 2 3; do body "$(router 1)/proxy/x"; echo; done | paste -sd'|' -)"
eq "proxyOnce records once and replays on the node that recorded" "1" \
   "$(for i in 1 2; do body "$(router 2)/once/replay"; echo; done | sort -u | wc -l | tr -d ' ')"

# ----------------------------------------------------------------- flow state
echo
echo "== flow state is cluster-wide: a scenario written on one node gates on every node =="
eq "POST /imposters 7105 (scenario) on node 3" "201" "$(acode -X POST "$(admin 3)/imposters" -H 'Content-Type: application/json' -d '{
  "port": 7105, "protocol": "http", "name": "checkout",
  "stubs": [
    {"scenarioName":"checkout","requiredScenarioState":"Started","newScenarioState":"paid",
     "predicates":[{"equals":{"path":"/pay"}}],"responses":[{"is":{"statusCode":200,"body":"payment accepted"}}]},
    {"scenarioName":"checkout","requiredScenarioState":"paid",
     "predicates":[{"equals":{"path":"/pay"}}],"responses":[{"is":{"statusCode":409,"body":"already paid"}}]}
  ]}')"
# Append a route for it: the table is replaced wholesale, so read, extend, write.
with_checkout="$(auth "$(admin 1)/front-door/routes" | jq -c '{routes: (.routes + [{"id":"checkout","priority":50,"enabled":true,"match":{"path_prefix":"/checkout"},"target":{"port":7105,"strip_prefix":true}}])}')"
eq "PUT the table with a checkout route (node 1)" "200" "$(acode -X PUT "$(admin 1)/front-door/routes" -H 'Content-Type: application/json' -d "$with_checkout")"
eq "first /pay on node 1  -> accepted"                           "200" "$(until_eq 10 200 code "$(router 1)/checkout/pay")"
eq "second /pay on node 2 -> already paid (state crossed nodes)" "409" "$(code "$(router 2)/checkout/pay")"
eq "third /pay on node 3  -> still paid"                           "409" "$(code "$(router 3)/checkout/pay")"
eq "scenario state reads back on node 2 as paid" "paid" "$(auth "$(admin 2)/imposters/7105/scenarios" | jq -r '[(.scenarios // .)[] | select(.name=="checkout") | .state][0]' 2>/dev/null)"

# -------------------------------------------------------- restart a follower
echo
echo "== a stopped node catches up =="
# Pick the victim by role, not by number: stopping a follower proves catch-up;
# stopping the leader (below) proves election. (No associative arrays: macOS
# ships bash 3.2 and this script has to run on a laptop.)
leader_node=""; for n in 1 2 3; do [ "$(node_id_of "$n")" = "$(leader_of 1)" ] && leader_node="$n"; done
follower_node=""; for n in 1 2 3; do [ "$n" != "$leader_node" ] && [ -z "$follower_node" ] && follower_node="$n"; done
survivor_node=""; for n in 1 2 3; do [ "$n" != "$leader_node" ] && [ "$n" != "$follower_node" ] && survivor_node="$n"; done
echo "   leader=rift-$leader_node follower=rift-$follower_node survivor=rift-$survivor_node"
"${COMPOSE[@]}" stop "rift-$follower_node" >/dev/null 2>&1
eq "the fleet reports two reachable" "2" "$(until_eq 20 2 reachable "$survivor_node")"
eq "a write lands while it is down" "200" "$(acode -X POST "$(admin "$survivor_node")/imposters/7101/stubs" -H 'Content-Type: application/json' \
   -d '{"stub":{"predicates":[{"equals":{"path":"/after-restart"}}],"responses":[{"is":{"statusCode":200,"body":"caught up"}}]},"index":0}')"
"${COMPOSE[@]}" start "rift-$follower_node" >/dev/null 2>&1
eq "the node comes back ready" "1" "$(until_eq 60 1 ready_count "$follower_node")"
eq "and serves the write it missed" "caught up" "$(until_eq 30 "caught up" body "$(router "$follower_node")/smoke/after-restart")"
eq "three reachable again" "3" "$(until_eq 20 3 reachable 1)"

# ---------------------------------------------------------- lose the leader
echo
echo "== a stopped leader is replaced and writes continue =="
old_leader="$(leader_of "$survivor_node")"
"${COMPOSE[@]}" stop "rift-$leader_node" >/dev/null 2>&1
new_leader=""
for _ in $(seq 1 30); do
  new_leader="$(leader_of "$survivor_node")"
  [ -n "$new_leader" ] && [ "$new_leader" != "null" ] && [ "$new_leader" != "$old_leader" ] && break
  sleep 1
done
eq "a new leader was elected" "changed" "$([ -n "$new_leader" ] && [ "$new_leader" != "$old_leader" ] && echo changed || echo "stuck:$new_leader")"
eq "writes continue on the survivors" "200" "$(acode -X POST "$(admin "$survivor_node")/imposters/7101/stubs" -H 'Content-Type: application/json' \
   -d '{"stub":{"predicates":[{"equals":{"path":"/after-failover"}}],"responses":[{"is":{"statusCode":200,"body":"survived"}}]},"index":0}')"
eq "and dispatch on the other survivor" "survived" "$(until_eq 10 survived body "$(router "$follower_node")/smoke/after-failover")"
"${COMPOSE[@]}" start "rift-$leader_node" >/dev/null 2>&1
eq "the old leader comes back ready" "1" "$(until_eq 60 1 ready_count "$leader_node")"
eq "and serves the write it missed" "survived" "$(until_eq 30 survived body "$(router "$leader_node")/smoke/after-failover")"
eq "three reachable, three voters" "3 3" "$(until_eq 20 3 reachable 1) $(voters 1)"

# ----------------------------------------------------------------- join/leave
echo
echo "== a new node joins from a seed, serves the same imposters, and leaves =="
"${COMPOSE[@]}" --profile join up -d rift-4 >/dev/null 2>&1
eq "rift-4 becomes ready" "1" "$(until_eq 120 1 ready_count 4)"
eq "rift-4 is promoted to voter (four voters)" "4" "$(until_eq 60 4 voters 1)"
eq "rift-4 serves an imposter it never received directly" '{"status":"up"}' "$(until_eq 30 '{"status":"up"}' body "$(router 4)/smoke/health")"
eq "and the stub added after failover" "survived" "$(body "$(router 4)/smoke/after-failover")"
eq "rift-4 is at the fleet's applied index" "1" "$(for n in 1 4; do members "$n" | jq -r .last_applied; done | sort -u | wc -l | tr -d ' ')"
"${COMPOSE[@]}" --profile join stop rift-4 >/dev/null 2>&1
"${COMPOSE[@]}" --profile join rm -f -v rift-4 >/dev/null 2>&1
eq "a graceful leave returns the fleet to three voters" "3" "$(until_eq 30 3 voters 1)"
eq "and three reachable" "3" "$(until_eq 20 3 reachable 1)"

# -------------------------------------------------------------------- delete
echo
echo "== a delete on one node is a 404 on every node =="
eq "DELETE /imposters/7104 on node 2" "200" "$(acode -X DELETE "$(admin 2)/imposters/7104")"
eq "GET on node 1 -> 404" "404" "$(acode "$(admin 1)/imposters/7104")"
eq "the route to it now answers 404 on node 3" "404" "$(code "$(router 3)/once/replay")"

echo
echo "──────────────────────────────"
printf "  %d passed, %d failed\n" "$pass" "$fail"
[ "$fail" -eq 0 ]
