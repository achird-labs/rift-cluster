import { describe, expect, it } from "vitest";

import { type Route, parseHash, toHash } from "./routing.ts";

describe("parseHash", () => {
  it("defaults to the imposters screen", () => {
    expect(parseHash("")).toEqual({ screen: "imposters" });
    expect(parseHash("#")).toEqual({ screen: "imposters" });
    expect(parseHash("#/")).toEqual({ screen: "imposters" });
  });

  it("parses the screens C4 ships", () => {
    expect(parseHash("#/imposters")).toEqual({ screen: "imposters" });
    expect(parseHash("#/cluster")).toEqual({ screen: "cluster" });
    expect(parseHash("#/imposters/4545")).toEqual({ screen: "imposter", port: 4545 });
  });

  it("parses the scenarios screen, with and without a flow (#232)", () => {
    // Three shapes, because the surface has three: no imposter chosen, an imposter under its own
    // default flow, and an imposter under a named space.
    expect(parseHash("#/scenarios")).toEqual({ screen: "scenarios", port: null, flow: null });
    expect(parseHash("#/scenarios/4545")).toEqual({ screen: "scenarios", port: 4545, flow: null });
    expect(parseHash("#/scenarios/4545/checkout-1")).toEqual({
      screen: "scenarios",
      port: 4545,
      flow: "checkout-1",
    });
  });

  it("does not invent a flow named by a bad port", () => {
    // `#/scenarios/abc/f` must not become "port null, flow f" — that would read the default flow of
    // no imposter and render a screen for a route the operator never asked for.
    expect(parseHash("#/scenarios/abc/f")).toEqual({ screen: "imposters" });
    expect(parseHash("#/scenarios/0")).toEqual({ screen: "imposters" });
  });

  it("survives a malformed percent-escape instead of white-screening the console", () => {
    /*
     * `decodeURIComponent` throws `URIError` on a lone `%` or an incomplete escape, and `parseHash`
     * runs inside `useRoute`'s state initializer and its `hashchange` listener. With no
     * ErrorBoundary in this console an uncaught throw there paints nothing at all, and leaves the
     * listener throwing too — so the operator cannot even navigate back out. A bad escape is a
     * stale bookmark like any other, and gets the same fallback.
     */
    for (const bad of ["#/scenarios/4545/%", "#/scenarios/4545/100%discount", "#/scenarios/4545/%E0%A4%A"]) {
      expect([bad, parseHash(bad)]).toEqual([bad, { screen: "imposters" }]);
    }
  });

  it("round-trips a flow id that needs escaping in a hash", () => {
    // Flow ids are operator-chosen and reach the URL, so a `/` in one must survive the round trip
    // rather than silently becoming an extra segment the parser rejects.
    const route = { screen: "scenarios", port: 4545, flow: "a/b" } as const;
    expect(parseHash(toHash(route))).toEqual(route);
  });

  it("falls back to the imposters screen for an unknown hash", () => {
    // A stale bookmark to a screen that does not exist yet is a navigation miss, not an error page:
    // the nav already tells the operator which screens are unbuilt.
    expect(parseHash("#/specs")).toEqual({ screen: "imposters" });
    expect(parseHash("#/imposters/4545/stubs/0")).toEqual({ screen: "imposters" });
  });

  it("rejects a port that is not a plain integer in range rather than fetching a nonsense route", () => {
    // `port` becomes a path segment on `/imposters/{port}`, so anything that is not a port would be
    // sent to the admin front as one.
    for (const bad of ["#/imposters/abc", "#/imposters/-1", "#/imposters/0", "#/imposters/70000", "#/imposters/45.5", "#/imposters/"]) {
      expect(parseHash(bad)).toEqual({ screen: "imposters" });
    }
  });

  it("round-trips every route through toHash", () => {
    const routes: Route[] = [
      { screen: "imposters" },
      { screen: "cluster" },
      { screen: "imposter", port: 65535 },
      { screen: "scenarios", port: null, flow: null },
      { screen: "scenarios", port: 4545, flow: null },
      { screen: "scenarios", port: 4545, flow: "checkout-1" },
    ];
    for (const route of routes) {
      expect(parseHash(toHash(route))).toEqual(route);
    }
  });
});
