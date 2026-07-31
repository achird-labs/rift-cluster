import { describe, expect, it } from "vitest";

import { ISSUE_URL, NAV, liveEntries, plannedEntries } from "./nav.ts";
import { toHash } from "./routing.ts";

describe("nav model", () => {
  it("ships the screens C4, C6 and C7 actually built as live entries", () => {
    // C6 (#189) turns `requests` live and adds the front-door route editor beside it. C7 (#190)
    // turns `administration` live.
    expect(liveEntries().map((e) => e.id)).toEqual([
      "imposters",
      "cluster",
      "requests",
      "routes",
      "administration",
    ]);
  });

  it("renders every unshipped screen as a planned entry rather than omitting it", () => {
    // RFC-006 §4: "a visible roadmap, not a 404". The issue names scenarios (#149), sources (#20)
    // and specs (#148) explicitly; administration (#190) is now built and no longer planned.
    const planned = Object.fromEntries(plannedEntries().map((e) => [e.id, e.issue]));
    expect(planned).toEqual({
      scenarios: 149,
      sources: 20,
      specs: 148,
    });
  });

  it("gives every planned entry an issue number and no route", () => {
    for (const entry of plannedEntries()) {
      expect(Number.isInteger(entry.issue)).toBe(true);
      expect(entry.issue).toBeGreaterThan(0);
      expect(entry).not.toHaveProperty("route");
      expect(entry.label.length).toBeGreaterThan(0);
    }
  });

  it("gives every live entry a route the router can parse back", () => {
    for (const entry of liveEntries()) {
      expect(toHash(entry.route).startsWith("#/")).toBe(true);
    }
  });

  it("uses unique ids and labels across the whole nav", () => {
    expect(new Set(NAV.map((e) => e.id)).size).toBe(NAV.length);
    expect(new Set(NAV.map((e) => e.label)).size).toBe(NAV.length);
  });

  it("builds issue links against this repository", () => {
    expect(ISSUE_URL(189)).toBe("https://github.com/achird-labs/rift-cluster/issues/189");
  });
});
