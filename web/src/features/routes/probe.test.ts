import { describe, expect, it } from "vitest";

import type { Route } from "./order.ts";
import { type Probe, probeRoutes } from "./probe.ts";

const route = (fields: Partial<Route> & { id: string }): Route =>
  ({
    priority: 0,
    enabled: true,
    target: { port: 4545, strip_prefix: false },
    ...fields,
  }) as Route;

const probe = (fields: Partial<Probe> = {}): Probe => ({
  host: "example.com",
  path: "/",
  method: "GET",
  headers: [],
  ...fields,
});

describe("the route tester walks the table the way the front door does", () => {
  it("dispatches to the first route in evaluation order whose every clause matches", () => {
    const result = probeRoutes(
      [
        route({ id: "low", priority: 0, match: { path_prefix: "/api" } }),
        route({ id: "high", priority: 10, match: { path_prefix: "/api" } }),
      ],
      probe({ path: "/api/v1" }),
    );
    // Priority wins before anything else, so the table's own order is irrelevant.
    expect(result.winner?.id).toBe("high");
  });

  it("stops at the winner rather than tracing routes the request never reaches", () => {
    // Saying "would have matched" about a route the front door never evaluates is noise dressed as
    // information — the operator's question is which one took it, and why the ones before did not.
    const result = probeRoutes(
      [
        route({ id: "first", priority: 10, match: {} }),
        route({ id: "never", priority: 0, match: {} }),
      ],
      probe(),
    );
    expect(result.trace.map((t) => t.id)).toEqual(["first"]);
  });

  it("matches a host clause exactly, and a wildcard on exactly one label", () => {
    const table = [route({ id: "wild", match: { host: "*.example.com" } })];
    expect(probeRoutes(table, probe({ host: "api.example.com" })).winner?.id).toBe("wild");
    // The bare domain is not one label under the wildcard.
    expect(probeRoutes(table, probe({ host: "example.com" })).winner).toBeNull();
    // Nor is a deeper name — `*.` is one label, not any number.
    expect(probeRoutes(table, probe({ host: "a.b.example.com" })).winner).toBeNull();
  });

  it("matches a path prefix on segment boundaries, never mid-segment", () => {
    const table = [route({ id: "api", match: { path_prefix: "/api" } })];
    expect(probeRoutes(table, probe({ path: "/api" })).winner?.id).toBe("api");
    expect(probeRoutes(table, probe({ path: "/api/v1" })).winner?.id).toBe("api");
    // The one that matters: a substring match would send an unrelated service's traffic here.
    expect(probeRoutes(table, probe({ path: "/apiary" })).winner).toBeNull();
  });

  it("requires every header clause, and compares the name case-insensitively", () => {
    const table = [
      route({ id: "canary", match: { headers: [{ name: "X-Env", value: "canary" }] } }),
    ];
    expect(
      probeRoutes(table, probe({ headers: [{ name: "x-env", value: "canary" }] })).winner?.id,
    ).toBe("canary");
    // Values are not case-folded — a header value is data, not an identifier.
    expect(probeRoutes(table, probe({ headers: [{ name: "X-Env", value: "CANARY" }] })).winner)
      .toBeNull();
  });

  it("never dispatches to a disabled route, and does not trace one", () => {
    const result = probeRoutes([route({ id: "off", enabled: false, match: {} })], probe());
    expect(result.winner).toBeNull();
    expect(result.trace).toEqual([]);
  });

  it("names the clause that failed, so the trace answers why rather than only whether", () => {
    const result = probeRoutes(
      [route({ id: "api", match: { host: "other.test", path_prefix: "/api" } })],
      probe({ host: "example.com", path: "/api" }),
    );
    expect(result.trace[0]?.hit).toBe(false);
    // The host is checked first, so that is the clause reported — not a generic "no match".
    expect(result.trace[0]?.why).toContain("other.test");
  });
});
