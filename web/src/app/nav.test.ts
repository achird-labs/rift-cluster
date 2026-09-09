import { describe, expect, it } from "vitest";

import { ISSUE_URL, NAV, NAV_GROUPS, groupOf, liveEntries, shortLabelStandsIn } from "./nav.ts";
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

  /*
   * WCAG 2.5.3, Label in Name — see `shortLabelStandsIn`'s own doc for the rule and why it is a
   * named predicate rather than a loop body here: **no entry carries a `short` today**, so a test
   * shaped as `for (…) if (short === undefined) continue` asserts nothing at all and would keep
   * passing after the rule stopped holding. The fixtures below are what actually exercises it; the
   * sweep over `NAV` is what applies it to the shipped bar.
   */
  it("accepts a short label the full label contains, and refuses one it does not", () => {
    expect(shortLabelStandsIn({ label: "Cluster & fleet", short: "Fleet" })).toBe(true);
    // Case is not the criterion: speech input matches without regard to it.
    expect(shortLabelStandsIn({ label: "Front door routes", short: "front DOOR" })).toBe(true);
    // No `short` at all is the compliant case — the bar prints the full label.
    expect(shortLabelStandsIn({ label: "Router" })).toBe(true);

    // The edit this rule exists to catch: a label shortened out from under its own `short`.
    expect(shortLabelStandsIn({ label: "Router", short: "Front door" })).toBe(false);
    // A word that only overlaps is not containment.
    expect(shortLabelStandsIn({ label: "Cluster", short: "Cluster & fleet" })).toBe(false);
    // An empty `short` would print nothing while claiming to stand in for something.
    expect(shortLabelStandsIn({ label: "Router", short: "" })).toBe(false);
  });

  it("keeps every short label in the shipped bar a substring of the label it stands in for", () => {
    for (const entry of liveEntries()) {
      expect(shortLabelStandsIn(entry)).toBe(true);
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
