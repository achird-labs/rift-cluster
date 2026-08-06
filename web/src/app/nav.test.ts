import { describe, expect, it } from "vitest";

import { ISSUE_URL, NAV, NAV_GROUPS, groupOf, liveEntries, plannedEntries } from "./nav.ts";
import { toHash } from "./routing.ts";

describe("nav model", () => {
  it("ships the screens C4, C6 and C7 actually built as live entries", () => {
    // C6 (#189) turns `requests` live and adds the front-door route editor beside it. C7 (#190)
    // turns `administration` live. #233 turns `sources` live.
    //
    // The order is the rail's section grouping, not authoring order: the four mock screens, then
    // fleet, then administration.
    expect(liveEntries().map((e) => e.id)).toEqual([
      "imposters",
      "requests",
      "routes",
      "scenarios",
      "sources",
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

  it("puts every unbuilt screen last, together", () => {
    // "A visible roadmap" reads after the things that work, never interleaved with them.
    const firstPlanned = NAV.findIndex((entry) => entry.kind === "planned");
    expect(NAV.slice(firstPlanned).every((entry) => entry.kind === "planned")).toBe(true);
  });

  it("renders every unshipped screen as a planned entry rather than omitting it", () => {
    /*
     * RFC-006 §4: "a visible roadmap, not a 404" — so each chip must name work that is actually
     * outstanding, which is not the same as naming the epic the feature belongs to. `specs` names
     * #148 because there genuinely is nothing to render until RFC-004 lands.
     *
     * `sources` is no longer here: #233 built it, so the chip became a live entry. `scenarios` left
     * the same way for #232. That is the whole lifecycle this list is meant to have — a planned
     * entry is a promise, and the promise is discharged by the entry graduating rather than by the
     * number being edited again.
     */
    const planned = Object.fromEntries(plannedEntries().map((e) => [e.id, e.issue]));
    expect(planned).toEqual({
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
