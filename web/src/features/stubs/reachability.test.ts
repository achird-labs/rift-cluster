import { describe, expect, it } from "vitest";

import type { Route } from "../routes/order.ts";
import type { Sample } from "./sample.ts";
import { directReach, frontDoorOrigin, frontDoorReach, splitAddr } from "./reachability.ts";

const LOCATION = { protocol: "http:", hostname: "console.test" };

function sample(overrides: Partial<Sample> = {}): Sample {
  return {
    method: "GET",
    target: "/products",
    headers: [],
    body: null,
    caveats: [],
    pinned: { method: true, path: true },
    ...overrides,
  };
}

function route(overrides: Partial<Route> = {}): Route {
  return {
    id: "catalog",
    priority: 0,
    enabled: true,
    match: { path_prefix: "/catalog" },
    target: { port: 4545, strip_prefix: true },
    ...overrides,
  };
}

describe("splitAddr", () => {
  it("splits an ipv4 address", () => {
    expect(splitAddr("0.0.0.0:2527")).toEqual({ host: "0.0.0.0", port: "2527" });
  });

  /*
   * The reason the parse is a regex over a bracketed alternative rather than a `split(":")`: an
   * unbracketed ipv6 literal has colons of its own, and taking the text before the first one reads
   * `::1` as an empty host on port `:1`. Upstream fixed the mirror image of this in rift#1144.
   */
  it("splits a bracketed ipv6 address without mistaking a hextet for the port", () => {
    expect(splitAddr("[::1]:2527")).toEqual({ host: "[::1]", port: "2527" });
  });

  it("refuses a value with no port rather than inventing one", () => {
    expect(splitAddr("0.0.0.0")).toBeNull();
    expect(splitAddr("")).toBeNull();
    expect(splitAddr("host:notaport")).toBeNull();
  });
});

describe("frontDoorOrigin", () => {
  /*
   * Pins the half of D-91 that is easiest to get wrong: a node reports the address it BOUND, and
   * `0.0.0.0` is not somewhere a client can connect to. The host the browser already used to reach
   * this node demonstrably resolves, so only the port is taken.
   */
  it("keeps the page's hostname when the node bound a wildcard", () => {
    expect(frontDoorOrigin("0.0.0.0:2527", LOCATION)).toBe("http://console.test:2527");
    expect(frontDoorOrigin("[::]:2527", LOCATION)).toBe("http://console.test:2527");
  });

  it("keeps a specific bind host, which names the interface the operator chose", () => {
    expect(frontDoorOrigin("10.0.0.7:2527", LOCATION)).toBe("http://10.0.0.7:2527");
  });

  it("is null when the node has no front door, or reported one it cannot parse", () => {
    expect(frontDoorOrigin(null, LOCATION)).toBeNull();
    expect(frontDoorOrigin("nonsense", LOCATION)).toBeNull();
  });
});

describe("frontDoorReach", () => {
  it("prefixes the path for a stripping route", () => {
    const reach = frontDoorReach(4545, sample(), [route()], "0.0.0.0:2527", LOCATION);
    expect(reach).not.toBeNull();
    expect(reach?.origin).toBe("http://console.test:2527");
    expect(reach?.target).toBe("/catalog/products");
    expect(reach?.routeId).toBe("catalog");
  });

  /*
   * The asymmetry that produces a 404 if it is inverted. A non-stripping route forwards the whole
   * path, so the path to SEND is the imposter's own — and the route can only carry it when it
   * already starts with the prefix.
   */
  it("sends the path unchanged through a non-stripping route", () => {
    const keep = route({ target: { port: 4545, strip_prefix: false } });
    const reach = frontDoorReach(
      4545,
      sample({ target: "/catalog/products" }),
      [keep],
      "0.0.0.0:2527",
      LOCATION,
    );
    expect(reach?.target).toBe("/catalog/products");
  });

  it("declines a non-stripping route that cannot carry this path at all", () => {
    const keep = route({ target: { port: 4545, strip_prefix: false } });
    expect(frontDoorReach(4545, sample(), [keep], "0.0.0.0:2527", LOCATION)).toBeNull();
  });

  it("does not double the slash when the prefix carries a trailing one", () => {
    const trailing = route({ match: { path_prefix: "/catalog/" } });
    const reach = frontDoorReach(4545, sample(), [trailing], "0.0.0.0:2527", LOCATION);
    expect(reach?.target).toBe("/catalog/products");
  });

  it("adds the Host header a host-matched route needs", () => {
    const byHost = route({
      id: "edge-by-host",
      match: { host: "edge.test" },
      target: { port: 4545, strip_prefix: false },
    });
    const reach = frontDoorReach(4545, sample(), [byHost], "0.0.0.0:2527", LOCATION);
    expect(reach?.headers).toEqual([{ name: "Host", value: "edge.test" }]);
  });

  /*
   * A wildcard clause matches a family, not a name. Emitting `Host: *.edge.test` would send a value
   * the matcher rejects, so a concrete label is filled in — and the caveat says so, because the
   * operator is the only one who knows which of their names is real.
   */
  it("fills in a label for a wildcard host and says that it did", () => {
    const wild = route({
      match: { host: "*.edge.test" },
      target: { port: 4545, strip_prefix: false },
    });
    const reach = frontDoorReach(4545, sample(), [wild], "0.0.0.0:2527", LOCATION);
    expect(reach?.headers).toEqual([{ name: "Host", value: "any.edge.test" }]);
    expect(reach?.caveats.some((c) => c.includes("wildcard host"))).toBe(true);
  });

  it("carries a route's header clauses", () => {
    const withHeaders = route({
      match: { path_prefix: "/catalog", headers: [{ name: "X-Tier", value: "gold" }] },
    });
    const reach = frontDoorReach(4545, sample(), [withHeaders], "0.0.0.0:2527", LOCATION);
    expect(reach?.headers).toEqual([{ name: "X-Tier", value: "gold" }]);
  });

  it("skips a route whose method clause excludes this sample", () => {
    const postOnly = route({ match: { path_prefix: "/catalog", method: "POST" } });
    expect(frontDoorReach(4545, sample(), [postOnly], "0.0.0.0:2527", LOCATION)).toBeNull();
  });

  it("skips a disabled route and a route aimed at another imposter", () => {
    expect(
      frontDoorReach(4545, sample(), [route({ enabled: false })], "0.0.0.0:2527", LOCATION),
    ).toBeNull();
    expect(
      frontDoorReach(
        4545,
        sample(),
        [route({ target: { port: 4546, strip_prefix: true } })],
        "0.0.0.0:2527",
        LOCATION,
      ),
    ).toBeNull();
  });

  /*
   * The route named has to be the route that would actually TAKE the request, or the command is
   * built from one rule while the front door applies another. `effectiveOrder` is the front door's
   * own precedence ported, so this defers to it rather than re-deriving "higher priority wins".
   */
  it("picks the route the front door would, not merely one that could", () => {
    const low = route({ id: "catalog", priority: 10 });
    const high = route({ id: "catalog-ro", priority: 30 });
    const reach = frontDoorReach(4545, sample(), [low, high], "0.0.0.0:2527", LOCATION);
    expect(reach?.routeId).toBe("catalog-ro");
  });

  it("is null when the node reported no front door, however many routes exist", () => {
    expect(frontDoorReach(4545, sample(), [route()], null, LOCATION)).toBeNull();
  });

  it("warns generically when it cannot tell whether ports are translated", () => {
    const reach = frontDoorReach(4545, sample(), [route()], "0.0.0.0:2527", LOCATION);
    expect(reach?.caveats.some((c) => c.includes("load balancer"))).toBe(true);
  });

  /*
   * The case this whole caveat exists for, and the one the local compose fleet is in: the node
   * reports `2525`, the browser reached it on `12525`, so ports are remapped and the front-door
   * port in the command (`2527`) is the node's own — the operator's is something else. A generic
   * "may not be reachable" is a sentence people scroll past; naming both numbers is not.
   */
  it("names both ports when the admin port it reached is not the one the node reports", () => {
    const reach = frontDoorReach(
      4545,
      sample(),
      [route()],
      "0.0.0.0:2527",
      { ...LOCATION, port: "12525" },
      2525,
    );
    const caveat = reach?.caveats.find((c) => c.includes("translated"));
    expect(caveat).toContain("2525");
    expect(caveat).toContain("12525");
  });

  it("says the trip is most likely direct when the admin port was not translated", () => {
    const reach = frontDoorReach(
      4545,
      sample(),
      [route()],
      "0.0.0.0:2527",
      { ...LOCATION, port: "2525" },
      2525,
    );
    expect(reach?.caveats.some((c) => c.includes("most likely direct"))).toBe(true);
  });
});

describe("directReach", () => {
  it("addresses the imposter port on the page's own host", () => {
    const reach = directReach(4545, sample(), LOCATION, false);
    expect(reach.origin).toBe("http://console.test:4545");
    expect(reach.target).toBe("/products");
    expect(reach.routeId).toBeNull();
  });

  /* The caveat is a dead end on its own and a signpost when a routed form is beside it. */
  it("points at the routed sibling only when there is one", () => {
    expect(directReach(4545, sample(), LOCATION, true).caveats[0]).toContain("front-door form");
    expect(directReach(4545, sample(), LOCATION, false).caveats[0]).not.toContain(
      "front-door form",
    );
  });

  it("adds the translation warning when the admin port proves ports are remapped", () => {
    const reach = directReach(
      4545,
      sample(),
      { ...LOCATION, port: "12525" },
      false,
      2525,
    );
    expect(reach.caveats.some((c) => c.includes("translated"))).toBe(true);
  });

  it("adds no translation warning when the admin port came through untouched", () => {
    const reach = directReach(4545, sample(), { ...LOCATION, port: "2525" }, false, 2525);
    expect(reach.caveats.some((c) => c.includes("translated"))).toBe(false);
  });
});
