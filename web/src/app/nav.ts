import type { Route } from "./routing.ts";

export const ISSUE_URL = (issue: number): string =>
  `https://github.com/achird-labs/rift-cluster/issues/${issue}`;

/**
 * The nav bar's sections, in render order.
 *
 * The bar is horizontal, so a group is no longer a heading over a stack — it is a run of entries
 * between two hairlines. The grouping still decides the order and still shows in the layout; it
 * just stops spending a line of vertical space on a label. The unbuilt group is last on purpose:
 * it is a roadmap, so it reads after the things that work.
 */
export const NAV_GROUPS = ["mocks", "fleet", "planned"] as const;
export type NavGroup = (typeof NAV_GROUPS)[number];

export const GROUP_LABEL: Record<NavGroup, string> = {
  mocks: "Mocks",
  fleet: "Fleet",
  planned: "Not yet shipped",
};

/**
 * What the nav bar prints, when the full label is too long for a horizontal strip.
 *
 * Constrained to a substring of `label`, and asserted so in `nav.test.ts`. The entry keeps its full
 * label as its accessible name, and WCAG 2.5.3 (Label in Name) requires the visible text to appear
 * in that name — otherwise "click Fleet" names a control no speech-input user can address. Omitted
 * where the label already fits.
 */
export type ShortLabel = string;

/**
 * A screen this slice built.
 *
 * No `requires`: since #550 there is one credential and one identity, so there is no role that
 * could be offered a smaller nav than another. Every live entry is offered to whoever is signed in.
 */
export type LiveEntry = {
  kind: "live";
  id: string;
  label: string;
  short?: ShortLabel;
  route: Route;
  group: Exclude<NavGroup, "planned">;
  /**
   * A geometric mark, not an icon font: `default-src 'self'` blocks a CDN and self-hosting an icon
   * set is weight the console does not need. It is decorative — every entry carries its label as
   * text — so it is `aria-hidden` at the render site.
   */
  glyph: string;
};

/** A screen RFC-006 §4 names but nothing has built yet. It is shown, greyed, with its issue. */
export type PlannedEntry = {
  kind: "planned";
  id: string;
  label: string;
  issue: number;
  note: string;
  glyph: string;
};

export type NavEntry = LiveEntry | PlannedEntry;

/** The group an entry renders under. Planned entries are always last, together. */
export function groupOf(entry: NavEntry): NavGroup {
  return entry.kind === "planned" ? "planned" : entry.group;
}

/**
 * The full RFC-006 §4 screen list, built and unbuilt together — "a visible roadmap, not a 404".
 *
 * The unbuilt half is the point. Omitting those entries would present C4's two screens as the
 * whole console; a 404 on a nav click would be worse still. A greyed entry carrying its issue
 * number answers "where is X?" without anyone having to ask.
 *
 * The list currently has no planned entries: `specs` was the last one, and #549 removed the stored
 * spec surface it was promising. The mechanism stays — an empty roadmap is a state, not a reason to
 * delete the shape RFC-006 §4 asks for — and `PlannedEntry` is what the next unbuilt screen uses.
 */
export const NAV: readonly NavEntry[] = [
  {
    kind: "live",
    id: "imposters",
    label: "Imposters",
    route: { screen: "imposters" },
    group: "mocks",
    glyph: "▤",
  },
  {
    kind: "live",
    id: "routes",
    label: "Front door routes",
    short: "Front door",
    route: { screen: "routes" },
    group: "mocks",
    glyph: "▤",
  },
  {
    kind: "live",
    id: "scenarios",
    label: "Flow state & scenarios",
    short: "Flow state",
    route: { screen: "scenarios", port: null, flow: null },
    group: "mocks",
    glyph: "▤",
  },
  {
    kind: "live",
    id: "requests",
    label: "Requests",
    route: { screen: "requests", port: null },
    group: "mocks",
    glyph: "▤",
  },
  {
    kind: "live",
    id: "cluster",
    label: "Cluster & fleet",
    short: "Fleet",
    route: { screen: "cluster" },
    group: "fleet",
    glyph: "◈",
  },
];

export function liveEntries(): LiveEntry[] {
  return NAV.filter((entry): entry is LiveEntry => entry.kind === "live");
}

export function plannedEntries(): PlannedEntry[] {
  return NAV.filter((entry): entry is PlannedEntry => entry.kind === "planned");
}
