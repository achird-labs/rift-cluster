import { describe, expect, it } from "vitest";

import { ISSUE_URL, NAV, NAV_GROUPS, groupOf, liveEntries, plannedEntries } from "./nav.ts";
import { toHash } from "./routing.ts";

describe("nav model", () => {
  it("ships the screens C4 and C6 actually built as live entries", () => {
    // C6 (#189) turns `requests` live and adds the front-door route editor beside it. #550 removed
    // the `administration` entry along with the tenancy and principal surfaces it opened.
    //
    // The order is the nav bar's section grouping, not authoring order — and it is the design's:
    // the four mock screens, then the fleet-scoped one.
    expect(liveEntries().map((e) => e.id)).toEqual([
      "imposters",
      "routes",
      "scenarios",
      "requests",
      "cluster",
    ]);
  });

  it("files scenarios under the mocks run, beside the screens it is read with (#232)", () => {
    const scenarios = liveEntries().find((entry) => entry.id === "scenarios");
    expect(scenarios?.group).toBe("mocks");
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
     * and diverge silently: shortening a label is exactly the edit that breaks this.
     *
     * Case-insensitively, which is the criterion rather than a loosening of it: "Routes" stands in
     * for "Front-door routes" and speech input matches without regard to case, so requiring an
     * exact substring would reject a label that satisfies 2.5.3 and push the fix toward a
     * lowercased word in the bar.
     */
    for (const entry of liveEntries()) {
      if (entry.short === undefined) continue;
      expect(entry.short.length).toBeGreaterThan(0);
      expect(entry.label.toLowerCase()).toContain(entry.short.toLowerCase());
    }
  });

  it("names the screens that are promised but unbuilt, and nothing else", () => {
    /*
     * RFC-006 §4: "a visible roadmap, not a 404". The roadmap is empty right now — `specs` was the
     * last chip and #549 removed the stored-spec surface it was promising, rather than the chip
     * graduating to a live entry the way `sources` (#233) and `scenarios` (#232) did.
     *
     * Asserted rather than deleted, because "no planned entries" is the claim: a chip that reappears
     * here without a live screen behind it is exactly what the ordering and shape rules below guard.
     */
    expect(plannedEntries()).toEqual([]);
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
