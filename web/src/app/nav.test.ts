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
      "scenarios",
      "cluster",
      "administration",
    ]);
  });

  it("offers scenarios on the read capability, not on a write one (#232)", () => {
    // The screen gates each control separately — reset is Operator, set-state is Editor — so the
    // entry itself must gate on the weakest thing it can do. Requiring a write capability here
    // would hide the whole screen from a viewer entitled to read every scenario on it.
    const scenarios = liveEntries().find((entry) => entry.id === "scenarios");
    expect(scenarios?.requires).toBe("scenario.read");
    expect(scenarios?.group).toBe("mocks");
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
     * `sources` points at #233 rather than #20: the provider SPI shipped and the chip went on
     * telling operators "not yet shipped, see #20", which is **closed** — the exact dead end this
     * design exists to avoid. `specs` still names #148 because there genuinely is nothing to render
     * until RFC-004 lands.
     *
     * `scenarios` is no longer here: #232 built it, so the chip became a live entry. That is the
     * whole lifecycle this list is meant to have — a planned entry is a promise, and the promise is
     * discharged by the entry graduating rather than by the number being edited again.
     */
    const planned = Object.fromEntries(plannedEntries().map((e) => [e.id, e.issue]));
    expect(planned).toEqual({
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
