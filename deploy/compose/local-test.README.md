# A driveable 3-node test fleet

Three real `rift-cluster-server` nodes in containers, preloaded with five imposters and a
front-door route table, each carrying a different family of Rift behaviour.

Containers rather than three local processes for one reason: every node materialises **every**
imposter, so three nodes on one host would all try to bind `4545`. Each container gets its own
IP, so they don't collide — and publishing the same imposter port once per node is what lets you
ask "did this write actually reach node 3?"

```sh
# build + start
docker compose -f deploy/compose/docker-compose.yml \
               -f deploy/compose/local-test.overlay.yml up -d --build

# preload imposters, stubs and routes (wipes first, then verifies)
python3 deploy/compose/local-test.seed.py

# verify only — every imposter, on every node, through both its port and the router
python3 deploy/compose/local-test.seed.py --check

# the per-stub sweep: every stub, both doors, all three nodes
python3 deploy/compose/local-test.matrix.py
```

The two checks answer different questions. `--check` asks whether the seed landed everywhere, one
probe per imposter. `matrix.py` asks whether each individual **stub** answers the same way
whichever door the request came in by — 156 checks, and the interesting failures are the ones that
show up in only one column. It is a separate tool because the stateful stubs cannot be asserted
with one request each: `cat-cycle` shares its response cursor between both doors on a node, so the
claim is that consecutive calls *alternate*; the scenario and flow-state walks each take a fresh
flow id per transport and node, so six runs do not walk one state machine six steps.

`stop` / `start` preserve the Raft log, imposters and routes. **`up -d` after editing any compose
file replaces the containers and starts from nothing** — the stack declares no volumes. Re-run the
seed after any such reset.

Admin credential: `Authorization: rift-local-key` (override with `RIFT_LOCAL_KEY`).

## Ports

Convention is rift-N over the container port, so node 2's admin API is `22525`.

| | node 1 | node 2 | node 3 |
|---|---|---|---|
| admin API | 12525 | 22525 | 32525 |
| probes (`/healthz`, `/readyz`) | 12526 | 22526 | 32526 |
| front door | 12527 | 22527 | 32527 |
| catalog-api `4545` | 14545 | 24545 | 34545 |
| orders-api `4546` | 14546 | 24546 | 34546 |
| payments-api `4547` (https) | 14547 | 24547 | 34547 |
| edge-proxy `4548` | 14548 | 24548 | 34548 |
| ledger-api `4549` | 14549 | 24549 | 34549 |

Console: <http://localhost:12525/console>. It wants a session cookie, not a header:

```sh
curl -X POST -H 'Content-Type: application/json' \
     -d '{"apiKey":"rift-local-key"}' localhost:12525/session
```

The field is `apiKey`; `key` gets a `400`. In the browser the console asks for the key itself.

This fleet is also the deployment that shows why a stub's `Copy curl` offers two addresses (D-91).
Every imposter port here is published, so the direct form works — and the front door is published
on `12527` while the node bound `2527`, so the routed form's port is the node's own and not yours.
The console detects that: it reports its admin port as `2525` and you reached it on `12525`, which
is a difference it can see and therefore say out loud.

## What is loaded, and what each piece is for

### `catalog-api` — 4545, the predicate and response gallery

```sh
curl localhost:14545/products                  # equals (method + path)
curl 'localhost:14545/products?q=widget'       # deepEquals on the query
curl localhost:14545/products/SKU-1001         # matches (regex)
curl localhost:14545/products/SKU-1001/detail  # _rift.templated
curl -d 'a needle here' localhost:14545/x      # contains, on the body
curl localhost:14545/feed.json                 # endsWith
curl -i localhost:14545/admin                  # 401 — not + exists
curl -i -H 'Authorization: x' localhost:14545/admin   # 200 — exists
curl localhost:14545/a                         # or
curl -i localhost:14545/cycle                  # 202, then 200, then 202 …
time curl localhost:14545/slow                 # _behaviors.wait 250ms
time curl localhost:14545/flaky                # latency fault, 800ms
curl -i localhost:14545/nope                   # 404 defaultResponse
```

The templated response is the interesting one — it expands in the body **and** in header values
(`X-Request-Id: {{ uuid }}`), and mixes `request.*` lookups, a `regex` capture filter, `now` with
an offset and `randomInt`. Date tokens (`{{NOW}}`, `{{DAYS+7}}`) need no opt-in; everything else
needs `_rift.templated`.

**Stub order is load-bearing.** First stub whose predicates all match wins, so every narrow case
precedes the broad one that would swallow it. `equals` on `path` ignores the query string, which
is why `cat-search` has to sit above `cat-list`.

### `orders-api` — 4546, a scenario FSM

```sh
for i in 1 2 3 4; do curl -s -X POST -H 'X-Mock-Space: alpha' localhost:14546/orders; echo; done
curl -s -X POST -H 'X-Mock-Space: beta' localhost:14546/orders     # own timeline
```

`Started` → `paid` → `fulfilled`, and the third stub gates on `fulfilled` without advancing it.
`flowIdSource: header:X-Mock-Space` is what gives each caller an independent timeline; without it
the flow id is the imposter port and every caller shares one state machine.

Note `GET /imposters/4546/scenarios` reports the **default** flow (`flowId: "4546"`), not
`alpha` — reading a per-caller timeline back means asking with that flow id.

### `payments-api` — 4547, https and the faults that are not a status

```sh
curl -k -X POST https://localhost:14547/charge   # engine-generated cert
curl -ki https://localhost:14547/outage          # 503 + Retry-After, forced error fault
curl -k  https://localhost:14547/reset           # connection dies, no HTTP response at all
```

`/reset` is the case client retry logic almost never gets tested against.

### `edge-proxy` — 4548, record / replay

```sh
curl localhost:14548/products    # first call proxies to 4545 and records
curl -H 'Authorization: rift-local-key' localhost:12525/imposters/4548 | jq '[.stubs[].id]'
```

A `proxy-recorded-…` stub appears at the front of the list. **It replicates**: node 3's admin API
shows the same recorded stub, and hitting `localhost:24548/products` replays it on node 2 rather
than recording a second one.

### `ledger-api` — 4549, flow state three ways

One header (`X-Flow-Id`) drives all of it — the scope of the script's `ctx.state`, the key
`stateOps` writes under, and the space a `space`-scoped stub matches against.

```sh
# Rhai script: two 503s then 200, per flow
for i in 1 2 3; do curl -s -o /dev/null -w '%{http_code} ' -H 'X-Flow-Id: t1' localhost:14549/warmup; done
curl -H 'X-Flow-Id: t2' localhost:14549/warmup        # fresh flow, counter restarts

# the same idea declaratively, no script engine
curl -H 'X-Flow-Id: c1' 'localhost:14549/counter?id=req-1'
curl -H 'X-Flow-Id: c1' 'localhost:14549/counter?id=req-2'   # hitsBefore grows

# spaces: one port, isolated stubs
curl -H 'X-Flow-Id: team-a' localhost:14549/balance
curl -H 'X-Flow-Id: team-b' localhost:14549/balance
curl -i localhost:14549/balance    # no flow id: 200, empty, x-rift-no-match: true
```

That last one is worth knowing: a request matching **no** stub gets a `200` with an empty body and
`x-rift-no-match: true`, not a 404. Check the header before reading a blank 200 as a match.

The space stubs are declared **inside the imposter document** (a stub's `space` field), not via
`POST /imposters/{port}/spaces/{flowId}/stubs`. That route is proxied to the node's embedded
engine, so what it writes lives on one node and is erased the next time any imposter is created
anywhere. A `space` field in the config is ordinary replicated state.

### Front door — the replicated route table

One listener per node dispatching to imposters in-process, so a route works whether or not Docker
ever published the target port.

```sh
curl localhost:12527/catalog/products               # path_prefix, strip_prefix
curl localhost:12527/ledger/health
curl -H 'Host: edge.test' localhost:12527/health    # host match, the "no nginx" case
curl -i localhost:12527/nope                        # 404, no route
curl localhost:32527/orders-api/health              # same table, node 3
```

Every imposter has a route, `payments` included — the router reaches an https imposter over plain
`http`, because it dispatches in-process and 4547's TLS listener is never involved:

```sh
curl -k -X POST https://localhost:14547/charge   # direct: TLS
curl    -X POST  http://localhost:12527/pay/charge  # router: plain http, same stub
```

**Two stubs answer differently through the two doors**, and both differences are correct:

| | direct | router |
|---|---|---|
| `payments` `/reset` (TCP reset fault) | connection dies, `curl` exits 52 | `502` + `x-rift-fault: CONNECTION_RESET_BY_PEER` |
| `payments` anything | https, needs `-k` | plain http |

The front door terminates its own connection, so it cannot forward a raw reset — it turns the
fault into a gateway error and names the cause in a header. A retry test written against the
direct port and then pointed at the router will see a clean `502` where it expected a dead socket.

`path_prefix`, `strip_prefix` and `set_host` are **snake_case** while the rest of the admin API is
camelCase — `RouteMatch`/`RouteTarget` carry no serde rename, so that is what the fleet accepts.

## Seeing that it is really a cluster

```sh
# membership, leader, per-voter applied index
curl -s -H 'Authorization: rift-local-key' localhost:12525/_fleet/members | jq

# write on node 1, read on node 3. Pick a port the overlay publishes (4545-4549)
# if you also want to reach it on the data plane; 4600 is admin-visible only.
curl -s -H 'Authorization: rift-local-key' -X POST -H 'Content-Type: application/json' \
  -d '{"port":4600,"protocol":"http","name":"probe",
       "stubs":[{"responses":[{"is":{"statusCode":200,"body":"hello"}}]}]}' \
  localhost:12525/imposters
curl -s -H 'Authorization: rift-local-key' localhost:32525/imposters/4600 | jq .name
curl -s -H 'Authorization: rift-local-key' -X DELETE localhost:32525/imposters/4600

# kill the leader and watch a new one take over
docker stop rift-1
curl -s -H 'Authorization: rift-local-key' localhost:22525/_fleet/members | jq .current_leader
curl -s -o /dev/null -w '%{http_code}\n' localhost:24545/products   # data plane unaffected

# writes keep working on the surviving quorum
curl -s -H 'Authorization: rift-local-key' -X POST -H 'Content-Type: application/json' \
  -d '{"port":4601,"protocol":"http","name":"during-outage",
       "stubs":[{"responses":[{"is":{"statusCode":200,"body":"x"}}]}]}' \
  localhost:22525/imposters

# and the returning node catches up on what it missed
docker start rift-1
curl -s -H 'Authorization: rift-local-key' localhost:12525/imposters/4601 | jq .name
```

`docker stop` is a *graceful* leave — the node hands leadership over on the way out, so the new
leader is already elected by the time the next request lands. Use `docker kill rift-1` for the
ungraceful case, where the remaining two have to notice the silence and run an election.

The seed writes everything to node 1 and reads everything back from node 3, for the same reason: a
clustered write that answered `200` and never left the node that took it looks identical from that
node.

## Teardown

```sh
docker compose -f deploy/compose/docker-compose.yml \
               -f deploy/compose/local-test.overlay.yml down
```
