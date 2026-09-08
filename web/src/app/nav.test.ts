import { describe, expect, it } from "vitest";

import { ISSUE_URL, NAV, NAV_GROUPS, groupOf, liveEntries } from "./nav.ts";
import { toHash } from "./routing.ts";

describe("nav model", () => {
  it("ships exactly the five screens RFC-007 §3.2 leaves the console (#553)", () => {
    // The order is the nav bar's section grouping, not authoring order — and it is the design's:
    // the four mock screens, then the fleet-scoped one. Asserted as a whole list rather than as
    // "contains", because a sixth entry reappearing here is precisely what the trim decided against.
    expect(liveEntries().map((e) => e.id)).toEqual([
      "imposters",
      "requests",
      "scenarios",
      "routes",
      "cluster",
    ]);
  });

  it("labels the screens Imposters, Requests, Scenarios, Router and Cluster", () => {
    // "Router", not the feature's old front-door name: RFC-007 §6 names it for what it does, and
    // the label follows while the API path (`/front-door/routes`) waits on #554.
    expect(liveEntries().map((e) => e.label)).toEqual([
      "Imposters",
      "Requests",
      "Scenarios",
      "Router",
      "Cluster",
    ]);
  });

  it("files the four mock screens under Mocks and the cluster screen under Fleet", () => {
    const byGroup = Object.fromEntries(liveEntries().map((e) => [e.id, e.group]));
    expect(byGroup).toEqual({
      imposters: "mocks",
      requests: "mocks",
      scenarios: "mocks",
      routes: "mocks",
      cluster: "fleet",
    });
  });

  it("has no roadmap run: every group is a live one", () => {
    // RFC-006 §4's greyed "planned" chips are gone with #553 — every screen the reduced console
    // promises is built, so a group for unbuilt ones would only ever be empty.
    expect([...NAV_GROUPS]).toEqual(["mocks", "fleet"]);
  });

  it("declares entries already sorted into nav group order", () => {
    // The bar renders each group by filtering `NAV` in place, so an entry declared out of group
    // order would silently render in the wrong run rather than fail anywhere visible.
    const positions = NAV.map((entry) => NAV_GROUPS.indexOf(groupOf(entry)));
    expect(positions).toEqual([...positions].sort((a, b) => a - b));
  });

  it("keeps every short label a substring of the full label it stands in for", () => {
    /*
     * WCAG 2.5.3, Label in Name. The bar prints `short` and names the control with `label`, so a
     * `short` that is not contained in `label` produces a link whose visible text is not in its
     * accessible name — "click Fleet" then addresses nothing, for every speech-input user.
     *
     * Asserted here rather than trusted to review because the two strings live on the same object
     * and diverge silently: shortening a label is exactly the edit that breaks this. No entry
     * carries a `short` today; the rule is kept for the next one that does.
     *
     * Case-insensitively, which is the criterion rather than a loosening of it: speech input
     * matches without regard to case, so requiring an exact substring would reject a label that
     * satisfies 2.5.3 and push the fix toward a lowercased word in the bar.
     */
    for (const entry of liveEntries()) {
      if (entry.short === undefined) continue;
      expect(entry.short.length).toBeGreaterThan(0);
      expect(entry.label.toLowerCase()).toContain(entry.short.toLowerCase());
    }
  });

  it("gives every entry a route the router can parse back", () => {
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
