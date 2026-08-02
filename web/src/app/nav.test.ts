import { describe, expect, it } from "vitest";

import { ISSUE_URL, NAV, NAV_GROUPS, groupOf, liveEntries, plannedEntries } from "./nav.ts";
import { toHash } from "./routing.ts";

describe("nav model", () => {
  it("ships the screens C4, C6 and C7 actually built as live entries", () => {
    // C6 (#189) turns `requests` live and adds the front-door route editor beside it. C7 (#190)
    // turns `administration` live.
    //
    // The order is the rail's section grouping, not authoring order: the three mock screens, then
    // fleet, then administration.
    expect(liveEntries().map((e) => e.id)).toEqual([
      "imposters",
      "requests",
      "routes",
      "cluster",
      "administration",
    ]);
  });

  it("declares entries already sorted into rail group order", () => {
    // The rail renders each group by filtering `NAV` in place, so an entry declared out of group
    // order would silently render under the wrong heading rather than fail anywhere visible.
    const positions = NAV.map((entry) => NAV_GROUPS.indexOf(groupOf(entry)));
    expect(positions).toEqual([...positions].sort((a, b) => a - b));
  });

  it("puts every unbuilt screen last, together", () => {
    // "A visible roadmap" reads after the things that work, never interleaved with them.
    const firstPlanned = NAV.findIndex((entry) => entry.kind === "planned");
    expect(NAV.slice(firstPlanned).every((entry) => entry.kind === "planned")).toBe(true);
  });

  it("renders every unshipped screen as a planned entry rather than omitting it", () => {
    /*
     * RFC-006 §4: "a visible roadmap, not a 404" — so each chip must name work that is actually
     * outstanding, which is not the same as naming the epic the feature belongs to.
     *
     * `scenarios` points at #232 rather than the RFC-005 epic #149, and `sources` at #233 rather
     * than #20: both backends already ship, so what is missing is a console slice. #20 in
     * particular is **closed**, and the chip went on telling operators "not yet shipped, see #20" —
     * the exact dead end this design exists to avoid. `specs` still names #148 because there
     * genuinely is nothing to render until RFC-004 lands.
     */
    const planned = Object.fromEntries(plannedEntries().map((e) => [e.id, e.issue]));
    expect(planned).toEqual({
      scenarios: 232,
      sources: 233,
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
