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
    ];
    for (const route of routes) {
      expect(parseHash(toHash(route))).toEqual(route);
    }
  });
});
