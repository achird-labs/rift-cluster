import type { Route } from "./routing.ts";

export const ISSUE_URL = (issue: number): string =>
  `https://github.com/achird-labs/rift-cluster/issues/${issue}`;

/**
 * The nav bar's sections, in render order.
 *
 * The bar is horizontal, so a group is not a heading over a stack — it is a run of entries between
 * two hairlines. The grouping still decides the order and still shows in the layout; it just does
 * not spend a line of vertical space on a label.
 *
 * Two groups and five entries, which is the whole console since #553 (RFC-007 §3.2): the four
 * screens about mocks, then the one about the fleet they run on. There is no "planned" run any
 * more — RFC-006 §4's greyed roadmap chips carried screens that were promised and unbuilt, and
 * every screen the reduced console promises is built.
 */
export const NAV_GROUPS = ["mocks", "fleet"] as const;
export type NavGroup = (typeof NAV_GROUPS)[number];

export const GROUP_LABEL: Record<NavGroup, string> = {
  mocks: "Mocks",
  fleet: "Fleet",
};

/**
 * What the nav bar prints, when the full label is too long for a horizontal strip.
 *
 * Constrained to a substring of `label`, and asserted so in `nav.test.ts`. The entry keeps its full
 * label as its accessible name, and WCAG 2.5.3 (Label in Name) requires the visible text to appear
 * in that name — otherwise "click Fleet" names a control no speech-input user can address. Omitted
 * where the label already fits, which since #553 is every entry; the mechanism stays for the next
 * label that does not.
 */
export type ShortLabel = string;

/**
 * A screen this console ships.
 *
 * No `requires`: since #550 there is one credential and one identity, so there is no role that
 * could be offered a smaller nav than another. Every entry is offered to whoever is signed in.
 */
export type NavEntry = {
  id: string;
  label: string;
  short?: ShortLabel;
  route: Route;
  group: NavGroup;
  /**
   * A geometric mark, not an icon font: `default-src 'self'` blocks a CDN and self-hosting an icon
   * set is weight the console does not need. It is decorative — every entry carries its label as
   * text — so it is `aria-hidden` at the render site.
   */
  glyph: string;
};

/** The group an entry renders under. */
export function groupOf(entry: NavEntry): NavGroup {
  return entry.group;
}

/**
 * The five screens (RFC-007 §3.2, #553), in the order the bar draws them.
 *
 * **Router** is the screen that edits `/front-door/routes`. The feature has been called the "front
 * door" since upstream issue #19; RFC-007 §6 names it for what it does, and the console label
 * follows. The API path is unchanged — renaming a path every client has to follow is its own
 * decision (#554).
 */
export const NAV: readonly NavEntry[] = [
  {
    id: "imposters",
    label: "Imposters",
    route: { screen: "imposters" },
    group: "mocks",
    glyph: "▤",
  },
  {
    id: "requests",
    label: "Requests",
    route: { screen: "requests", port: null },
    group: "mocks",
    glyph: "▤",
  },
  {
    id: "scenarios",
    label: "Scenarios",
    route: { screen: "scenarios", port: null, flow: null },
    group: "mocks",
    glyph: "▤",
  },
  {
    id: "routes",
    label: "Router",
    route: { screen: "routes" },
    group: "mocks",
    glyph: "▤",
  },
  {
    id: "cluster",
    label: "Cluster",
    route: { screen: "cluster" },
    group: "fleet",
    glyph: "◈",
  },
];

/** Every entry. Kept as a function so call sites read the same as before the roadmap run went. */
export function liveEntries(): NavEntry[] {
  return [...NAV];
}
