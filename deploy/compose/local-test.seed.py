#!/usr/bin/env python3
"""Seed the local 3-node test fleet with imposters, stubs and a route table.

    python3 deploy/compose/local-test.seed.py          # wipe, seed, verify
    python3 deploy/compose/local-test.seed.py --check   # verify only

Pairs with `local-test.overlay.yml`. Every write goes to **node 1** and every
read-back goes to **node 3**, because that is the only way to see that a write
was replicated rather than merely accepted: a clustered write that answered
`200` and never left the node it arrived on looks identical from the node that
took it.

The five imposters are not five copies of the same thing — each one carries a
different family of Rift behaviour, so there is something real to exercise:

    4545  catalog-api   every predicate operator, response cycling, behaviours,
                        `_rift.templated` response templating, a latency fault
    4546  orders-api    a scenario FSM (`Started` -> paid -> fulfilled), with
                        per-caller timelines off a header
    4547  payments-api  https, plus the two fault families that are not a
                        status code: a forced error and a TCP reset
    4548  edge-proxy    a `proxyOnce` record/replay stub in front of 4545
    4549  ledger-api    flow state: a Rhai `_rift.script` stub, declarative
                        `_rift.stateOps`, and space-scoped stubs

Space-scoped stubs are declared **inside the imposter document** (a stub's
`space` field), not through `POST /imposters/{port}/spaces/{flowId}/stubs`.
That route is proxied to the node's embedded engine, so what it writes lives on
one node and is erased the next time any imposter is created anywhere; a `space`
field in the config is ordinary replicated state and survives both.
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import urllib.error
import urllib.request

KEY = "rift-local-key"
WRITE = "http://localhost:12525"  # node 1
READ = "http://localhost:32525"  # node 3
FRONT = "http://localhost:12527"  # node 1's front door

CATALOG, ORDERS, PAYMENTS, EDGE, LEDGER = 4545, 4546, 4547, 4548, 4549

# One list, so "what was seeded" and "what node 3 should see" cannot drift apart.
IMPOSTER_PORTS = [CATALOG, ORDERS, PAYMENTS, EDGE, LEDGER]

failures: list[str] = []


def call(method: str, path: str, body=None, base: str = WRITE, expect=(200, 201, 204)):
    """One admin API call. A refusal is data, not a crash."""
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method=method)
    req.add_header("Authorization", KEY)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        raw, status = e.read(), e.code
    except Exception as e:
        failures.append(f"{method} {path}: {e}")
        return 0, None
    try:
        parsed = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        parsed = raw.decode(errors="replace")
    if status not in expect:
        detail = json.dumps(parsed)[:300] if parsed is not None else ""
        failures.append(f"{method} {path} -> {status} (wanted {expect}) {detail}")
    return status, parsed


# ── imposter documents ──────────────────────────────────────────────────────


def catalog_api() -> dict:
    """The predicate and response gallery.

    Stub order is load-bearing: the engine takes the first stub whose predicates
    all match, so every narrow case has to precede the broad one it would
    otherwise be swallowed by. `/products?q=widget` is the one worth pointing
    at — `equals` on `path` ignores the query string, so `cat-list` would answer
    it if `cat-search` did not come first.
    """
    return {
        "port": CATALOG,
        "protocol": "http",
        "name": "catalog-api",
        "recordRequests": True,
        "stubs": [
            {
                "id": "cat-search",
                "predicates": [{"deepEquals": {"query": {"q": "widget"}}}],
                "responses": [
                    {
                        "is": {
                            "statusCode": 200,
                            "headers": {"Content-Type": "application/json"},
                            "body": {"matched": "deepEquals", "results": ["widget-1", "widget-2"]},
                        }
                    }
                ],
            },
            {
                "id": "cat-list",
                "predicates": [{"equals": {"method": "GET", "path": "/products"}}],
                "responses": [
                    {
                        "is": {
                            "statusCode": 200,
                            "headers": {"Content-Type": "application/json"},
                            "body": {
                                "matched": "equals",
                                "products": [
                                    {"sku": "SKU-1001", "name": "Anvil"},
                                    {"sku": "SKU-1002", "name": "Rocket skates"},
                                ],
                            },
                        }
                    }
                ],
            },
            {
                "id": "cat-sku-regex",
                "predicates": [{"matches": {"path": "^/products/SKU-[0-9]+$"}}],
                "responses": [
                    {"is": {"statusCode": 200, "body": {"matched": "matches (regex)"}}}
                ],
            },
            {
                # `_rift.templated` opts this response into the function grammar,
                # in the body AND in every header value. The date tokens
                # ({{NOW}}, {{DAYS+N}}) would expand without it; `{{ uuid }}`,
                # `{{ request.* }}` and `{{ now offset=... }}` would not.
                "id": "cat-templated",
                "routePattern": "/products/:sku/detail",
                "predicates": [{"startsWith": {"path": "/products/"}}],
                "responses": [
                    {
                        "is": {
                            "statusCode": 200,
                            "headers": {
                                "Content-Type": "application/json",
                                "X-Request-Id": "{{ uuid }}",
                            },
                            "body": (
                                '{"matched":"templated",'
                                '"path":"{{ request.path | json }}",'
                                # `last_segment` would say "detail" here — the
                                # sku is the middle segment, so it takes a
                                # capture group. `routePattern` names it too,
                                # but path params reach the script engines, not
                                # this grammar.
                                '"sku":"{{ request.path | regex \'^/products/([^/]+)/detail$\' 1 | json }}",'
                                '"tail":"{{ request.path | last_segment | json }}",'
                                '"method":"{{ request.method }}",'
                                '"agent":"{{ request.header \'User-Agent\' | json }}",'
                                '"servedAt":"{{ now }}",'
                                '"expiresAt":"{{ now offset=\'+1h\' }}",'
                                '"roll":{{ randomInt 1 6 }}}'
                            ),
                        },
                        "_rift": {"templated": True},
                    }
                ],
            },
            {
                "id": "cat-contains",
                "predicates": [{"contains": {"body": "needle"}}],
                "responses": [{"is": {"statusCode": 200, "body": {"matched": "contains"}}}],
            },
            {
                "id": "cat-endswith",
                "predicates": [{"endsWith": {"path": ".json"}}],
                "responses": [{"is": {"statusCode": 200, "body": {"matched": "endsWith"}}}],
            },
            {
                # `not` around `exists`: /admin WITHOUT an Authorization header.
                # It has to precede `cat-admin`, which matches /admin either way.
                "id": "cat-admin-unauth",
                "predicates": [
                    {"equals": {"path": "/admin"}},
                    {"not": {"exists": {"headers": {"Authorization": True}}}},
                ],
                "responses": [
                    {
                        "is": {
                            "statusCode": 401,
                            "headers": {"WWW-Authenticate": "Bearer"},
                            "body": {"matched": "not + exists", "error": "no credentials"},
                        }
                    }
                ],
            },
            {
                "id": "cat-admin",
                "predicates": [
                    {"equals": {"path": "/admin"}},
                    {"exists": {"headers": {"Authorization": True}}},
                ],
                "responses": [{"is": {"statusCode": 200, "body": {"matched": "exists"}}}],
            },
            {
                "id": "cat-or",
                "predicates": [
                    {"or": [{"equals": {"path": "/a"}}, {"equals": {"path": "/b"}}]}
                ],
                "responses": [{"is": {"statusCode": 200, "body": {"matched": "or"}}}],
            },
            {
                # Several responses on one stub cycle round-robin per request:
                # how "202 Accepted, then 200 once it is done" gets mocked.
                "id": "cat-cycle",
                "predicates": [{"equals": {"path": "/cycle"}}],
                "responses": [
                    {"is": {"statusCode": 202, "body": {"cycle": 1, "state": "accepted"}}},
                    {"is": {"statusCode": 200, "body": {"cycle": 2, "state": "done"}}},
                ],
            },
            {
                # `_behaviors.wait` is a fixed delay the response declares;
                # `repeat` serves this response N times before the cycle moves on.
                "id": "cat-slow",
                "predicates": [{"equals": {"path": "/slow"}}],
                "responses": [
                    {
                        "is": {"statusCode": 200, "body": {"matched": "_behaviors.wait 250ms"}},
                        "_behaviors": {"wait": 250, "repeat": 2},
                    }
                ],
            },
            {
                # A fault, not a status: the response is fine, the transport is
                # slow. `probability: 1.0` makes it deterministic to poke at.
                "id": "cat-flaky",
                "predicates": [{"equals": {"path": "/flaky"}}],
                "responses": [
                    {
                        "is": {"statusCode": 200, "body": {"matched": "latency fault"}},
                        "_rift": {"fault": {"latency": {"probability": 1.0, "ms": 800}}},
                    }
                ],
            },
        ],
        "defaultResponse": {
            "statusCode": 404,
            "headers": {"Content-Type": "application/json"},
            "body": {"error": "no stub matched", "imposter": "catalog-api"},
        },
    }


def orders_api() -> dict:
    """A scenario FSM, with one timeline per caller.

    `flowIdSource: header:X-Mock-Space` is what makes the timelines independent
    — without it the flow id defaults to the imposter port and all callers share
    one state machine, which is the right default and the wrong thing for two
    test runs against one fleet.
    """
    return {
        "port": ORDERS,
        "protocol": "http",
        "name": "orders-api",
        "recordRequests": True,
        "_rift": {"flowState": {"flowIdSource": "header:X-Mock-Space", "ttlSeconds": 600}},
        "stubs": [
            {
                "id": "ord-pay-first",
                "scenarioName": "order-lifecycle",
                "requiredScenarioState": "Started",  # the implicit initial state
                "newScenarioState": "paid",
                "predicates": [{"equals": {"method": "POST", "path": "/orders"}}],
                "responses": [
                    {"is": {"statusCode": 402, "body": {"state": "Started", "next": "paid"}}}
                ],
            },
            {
                "id": "ord-pay-second",
                "scenarioName": "order-lifecycle",
                "requiredScenarioState": "paid",
                "newScenarioState": "fulfilled",
                "predicates": [{"equals": {"method": "POST", "path": "/orders"}}],
                "responses": [
                    {"is": {"statusCode": 201, "body": {"state": "paid", "next": "fulfilled"}}}
                ],
            },
            {
                # No `newScenarioState`: gates on the state without advancing it,
                # which is how a repeatable step in the middle of a flow is modelled.
                "id": "ord-fulfilled",
                "scenarioName": "order-lifecycle",
                "requiredScenarioState": "fulfilled",
                "predicates": [{"equals": {"method": "POST", "path": "/orders"}}],
                "responses": [
                    {"is": {"statusCode": 200, "body": {"state": "fulfilled", "next": None}}}
                ],
            },
            {
                "id": "ord-health",
                "predicates": [{"equals": {"path": "/health"}}],
                "responses": [{"is": {"statusCode": 200, "body": {"ok": True}}}],
            },
        ],
    }


def payments_api() -> dict:
    """https, and the two faults that are not a status code.

    The engine generates the PEM pair, so nothing here needs a certificate on
    disk. `curl -k` is required, which is the point of having one.
    """
    return {
        "port": PAYMENTS,
        "protocol": "https",
        "name": "payments-api",
        "recordRequests": True,
        "stubs": [
            {
                "id": "pay-charge",
                "predicates": [{"equals": {"method": "POST", "path": "/charge"}}],
                "responses": [
                    {
                        "is": {
                            "statusCode": 201,
                            "headers": {"Content-Type": "application/json"},
                            "body": {"status": "captured", "tls": True},
                        }
                    }
                ],
            },
            {
                # An error fault replaces the response the stub would have sent.
                "id": "pay-outage",
                "predicates": [{"equals": {"path": "/outage"}}],
                "responses": [
                    {
                        "is": {"statusCode": 200, "body": {"never": "served"}},
                        "_rift": {
                            "fault": {
                                "error": {
                                    "probability": 1.0,
                                    "status": 503,
                                    "body": '{"error":"upstream unavailable"}',
                                    "headers": {"Retry-After": "30"},
                                }
                            }
                        },
                    }
                ],
            },
            {
                # No HTTP response at all — the connection dies. This is the
                # case a client's retry logic almost never gets tested against.
                "id": "pay-reset",
                "predicates": [{"equals": {"path": "/reset"}}],
                "responses": [
                    {
                        "is": {"statusCode": 200, "body": {"never": "served"}},
                        "_rift": {"fault": {"tcp": "CONNECTION_RESET_BY_PEER"}},
                    }
                ],
            },
        ],
    }


def edge_proxy() -> dict:
    """Record/replay in front of catalog-api.

    `proxyOnce` forwards the first request for a given predicate set, saves the
    real response as a new stub, and answers every later request from that
    recording. `127.0.0.1` is correct here rather than a peer address: each node
    serves every imposter itself, so the origin is always in-process.
    """
    return {
        "port": EDGE,
        "protocol": "http",
        "name": "edge-proxy",
        "recordRequests": True,
        "stubs": [
            {
                "id": "edge-proxyonce",
                "predicates": [{"startsWith": {"path": "/products"}}],
                "responses": [
                    {
                        "proxy": {
                            "to": f"http://127.0.0.1:{CATALOG}",
                            "mode": "proxyOnce",
                            "predicateGenerators": [{"matches": {"method": True, "path": True}}],
                        }
                    }
                ],
            },
            {
                "id": "edge-health",
                "predicates": [{"equals": {"path": "/health"}}],
                "responses": [{"is": {"statusCode": 200, "body": {"ok": True}}}],
            },
        ],
    }


RETRY_SCRIPT = (
    "fn respond(ctx) {"
    "  let n = ctx.state.incr(\"attempts\");"
    "  if n <= 2 {"
    "    http(503, #{ error: \"warming up\", attempt: n }).header(\"Retry-After\", \"1\")"
    "  } else {"
    "    http(200, #{ ok: true, succeededOnAttempt: n })"
    "  }"
    "}"
)


def ledger_api() -> dict:
    """Flow state three ways, all keyed on the same `X-Flow-Id` header.

    One header drives everything here: it is the flow id the script's
    `ctx.state` is pre-scoped to, the key `stateOps` writes under, AND the space
    a `space`-scoped stub is matched against. Send a different value and you get
    a clean slate without touching the fleet.
    """
    return {
        "port": LEDGER,
        "protocol": "http",
        "name": "ledger-api",
        "recordRequests": True,
        "_rift": {
            "flowState": {
                "backend": "inmemory",
                "ttlSeconds": 600,
                "flowIdSource": "header:X-Flow-Id",
            }
        },
        "stubs": [
            {
                # Two 503s then a 200, per flow id. The classic thing you cannot
                # mock with a static stub and cannot test a retry policy without.
                "id": "led-retry",
                "predicates": [{"equals": {"method": "GET", "path": "/warmup"}}],
                "responses": [{"_rift": {"script": {"engine": "rhai", "code": RETRY_SCRIPT}}}],
            },
            {
                # The same idea declaratively: no script engine involved. The ops
                # run after the body is rendered, so `hits` in the body is the
                # count BEFORE this request.
                "id": "led-counter",
                "predicates": [{"equals": {"path": "/counter"}}],
                "responses": [
                    {
                        "is": {
                            "statusCode": 200,
                            "headers": {
                                "Content-Type": "application/json",
                                "X-Hits-Before": "{{ state.hits }}",
                            },
                            "body": '{"hitsBefore":"{{ state.hits }}","lastId":"{{ state.lastId }}"}',
                        },
                        "_rift": {
                            "templated": True,
                            "stateOps": [
                                {"op": "increment", "key": "hits"},
                                {"op": "set", "key": "lastId", "value": "{{ request.query.id }}"},
                            ],
                        },
                    }
                ],
            },
            {
                "id": "led-balance-team-a",
                "space": "team-a",
                "predicates": [{"equals": {"path": "/balance"}}],
                "responses": [
                    {"is": {"statusCode": 200, "body": {"owner": "team-a", "balance": 1200}}}
                ],
            },
            {
                "id": "led-balance-team-b",
                "space": "team-b",
                "predicates": [{"equals": {"path": "/balance"}}],
                "responses": [
                    {"is": {"statusCode": 200, "body": {"owner": "team-b", "balance": 47}}}
                ],
            },
            {
                # No `space`: globally eligible, matches whatever the flow id is.
                "id": "led-health",
                "predicates": [{"equals": {"path": "/health"}}],
                "responses": [{"is": {"statusCode": 200, "body": {"ok": True}}}],
            },
        ],
    }


ROUTE_TABLE = {
    "routes": [
        # `path_prefix`, `strip_prefix` and `set_host` are snake_case on purpose:
        # RouteMatch/RouteTarget carry no serde rename, so this is what the fleet
        # actually accepts. Everything else in this API is camelCase.
        {
            "id": "catalog",
            "priority": 10,
            "match": {"path_prefix": "/catalog"},
            "target": {"port": CATALOG, "strip_prefix": True},
            "enabled": True,
        },
        {
            "id": "orders",
            "priority": 10,
            "match": {"path_prefix": "/orders-api"},
            "target": {"port": ORDERS, "strip_prefix": True},
            "enabled": True,
        },
        {
            # The https imposter, reached over plain http on the front door: the router
            # dispatches to an imposter in-process, so 4547's TLS listener is bypassed
            # entirely. Without this route the router could not reach payments at all,
            # which is the one imposter a route-table check would otherwise miss.
            "id": "payments",
            "priority": 10,
            "match": {"path_prefix": "/pay"},
            "target": {"port": PAYMENTS, "strip_prefix": True},
            "enabled": True,
        },
        {
            "id": "ledger",
            "priority": 10,
            "match": {"path_prefix": "/ledger"},
            "target": {"port": LEDGER, "strip_prefix": True},
            "enabled": True,
        },
        # Host-based, the "no nginx" case: one listener, two virtual services
        # told apart only by the Host header they arrive with.
        {
            "id": "edge-by-host",
            "priority": 20,
            "match": {"host": "edge.test"},
            "target": {"port": EDGE},
            "enabled": True,
        },
        # Higher priority than `catalog` and narrower: GET-only, and it rewrites
        # the Host on the way through. Proves priority ordering is observable.
        {
            "id": "catalog-readonly",
            "priority": 30,
            "match": {"path_prefix": "/catalog/ro", "method": "GET"},
            "target": {"port": CATALOG, "strip_prefix": True, "set_host": "catalog.internal"},
            "enabled": True,
        },
    ]
}


# ── phases ──────────────────────────────────────────────────────────────────


def _probe(url: str, label: str, extra: dict[str, str]) -> None:
    """One data-plane request. A non-2xx is a finding, not an exception.

    `POST` where the stub wants it — `/charge` is POST-only, and a GET would fall
    through to the imposter's no-match answer, which is a `200` with an empty body
    and `x-rift-no-match: true`. That header is checked precisely because a blank
    `200` would otherwise read as a pass.
    """
    method = "POST" if url.endswith("/charge") else "GET"
    req = urllib.request.Request(url, method=method)
    for name, value in extra.items():
        req.add_header(name, value)
    # The https imposter's cert is generated by the engine and signed by nobody.
    context = ssl._create_unverified_context() if url.startswith("https") else None
    try:
        with urllib.request.urlopen(req, timeout=10, context=context) as r:
            if r.status // 100 != 2:
                failures.append(f"{label} -> {r.status}")
            elif r.headers.get("x-rift-no-match") == "true":
                failures.append(f"{label} -> {r.status} but matched no stub")
    except urllib.error.HTTPError as e:
        failures.append(f"{label} -> {e.code}")
    except Exception as e:
        failures.append(f"{label}: {e}")


def wipe() -> None:
    _, body = call("GET", "/imposters", base=WRITE)
    for imp in (body or {}).get("imposters", []):
        call("DELETE", f"/imposters/{imp['port']}", base=WRITE, expect=(200, 404))


def seed() -> None:
    for doc in (catalog_api(), orders_api(), payments_api(), edge_proxy(), ledger_api()):
        call("POST", "/imposters", doc, base=WRITE, expect=(201,))
    call("PUT", "/front-door/routes", ROUTE_TABLE, base=WRITE, expect=(200,))


def check() -> None:
    """Read back from node 3, and drive the front door on node 1."""
    _, body = call("GET", "/imposters", base=READ)
    ports = sorted(i["port"] for i in (body or {}).get("imposters", []))
    want = IMPOSTER_PORTS
    if ports != want:
        failures.append(f"node 3 sees imposters {ports}, wanted {want}")

    _, table = call("GET", "/front-door/routes", base=READ)
    ids = sorted(r["id"] for r in (table or {}).get("routes", []))
    want_ids = sorted(r["id"] for r in ROUTE_TABLE["routes"])
    if ids != want_ids:
        failures.append(f"node 3 sees routes {ids}, wanted {want_ids}")

    # Every imposter, on every node, through BOTH doors.
    #
    # Both, because they are different code paths to the same stub and they do not
    # always agree: the router dispatches to an imposter in-process, which is how a
    # route reaches the https imposter over plain http, and why a TCP-reset fault
    # arrives as a `502` with `x-rift-fault` here instead of killing the connection.
    # A check that used one door would call the fleet healthy while the other was
    # dark — which it was, for payments, until a route existed for it.
    #
    # One probe per imposter rather than per stub: this is a "did the seed land
    # everywhere" check, not the stub matrix. The per-stub sweep is a thing a person
    # runs (see the README), because most of the interesting stubs are stateful.
    probes = [
        # (imposter port, path on the imposter, router path, extra headers)
        (CATALOG, "/products", "/catalog/products", {}),
        (ORDERS, "/health", "/orders-api/health", {}),
        (PAYMENTS, "/charge", "/pay/charge", {}),
        (EDGE, "/health", "/health", {"Host": "edge.test"}),
        (LEDGER, "/health", "/ledger/health", {}),
    ]
    for node in (1, 2, 3):
        for port, direct_path, router_path, extra in probes:
            # The published imposter port follows the rift-N convention: 4545 -> 14545.
            direct = f"http://localhost:{node}{port % 10000}{direct_path}"
            # https imposters answer TLS on their own port; the router does not.
            if port == PAYMENTS:
                direct = direct.replace("http://", "https://", 1)
            _probe(direct, f"node{node} direct {port}{direct_path}", extra)
            _probe(
                f"http://localhost:{node}2527{router_path}",
                f"node{node} router {router_path}",
                extra,
            )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="verify only, do not write")
    args = ap.parse_args()

    if not args.check:
        wipe()
        seed()
    check()

    if failures:
        print(f"FAILED ({len(failures)})")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(
        f"OK — {len(IMPOSTER_PORTS)} imposters and {len(ROUTE_TABLE['routes'])} routes, "
        "replicated; "
        "every imposter answers on all three nodes through both its own port and the router"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
