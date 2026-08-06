import type { Capability } from "./rbac.ts";
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
export const NAV_GROUPS = ["mocks", "fleet", "administration", "planned"] as const;
export type NavGroup = (typeof NAV_GROUPS)[number];

export const GROUP_LABEL: Record<NavGroup, string> = {
  mocks: "Mocks",
  fleet: "Fleet",
  administration: "Administration",
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

/** A screen this slice built. `requires` decides whether it is offered, not whether it is permitted. */
export type LiveEntry = {
  kind: "live";
  id: string;
  label: string;
  short?: ShortLabel;
  route: Route;
  requires: Capability;
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
 */
export const NAV: readonly NavEntry[] = [
  {
    kind: "live",
    id: "imposters",
    label: "Imposters",
    route: { screen: "imposters" },
    requires: "imposter.read",
    group: "mocks",
    glyph: "▤",
  },
  {
    kind: "live",
    id: "requests",
    label: "Request log",
    route: { screen: "requests", port: null },
    requires: "imposter.read",
    group: "mocks",
    glyph: "▤",
  },
  {
    kind: "live",
    id: "routes",
    label: "Front-door routes",
    short: "Routes",
    route: { screen: "routes" },
    // The table is read with `Action::ImposterRead`; writing it needs `imposter.write`, which the
    // screen gates per control rather than hiding the whole screen from an operator who may read it.
    requires: "imposter.read",
    group: "mocks",
    glyph: "▤",
  },
  {
    kind: "live",
    id: "scenarios",
    label: "Scenarios & state",
    short: "Scenarios",
    route: { screen: "scenarios", port: null, flow: null },
    // The weakest thing the screen can do, deliberately. Its controls gate individually — reset is
    // Operator, set-state and the flow-state write are Editor — so requiring a write capability
    // here would hide the whole screen from a viewer entitled to read every scenario on it.
    requires: "scenario.read",
    group: "mocks",
    glyph: "▤",
  },
  {
    kind: "live",
    id: "sources",
    label: "Sources",
    // #233 shipped: the backend has carried sources, provenance and drift for a while, and this
    // entry used to tell an operator "not yet shipped, see #233" while pointing at the very issue
    // that built the screen it was missing.
    route: { screen: "sources" },
    requires: "source.read",
    group: "mocks",
    glyph: "▤",
  },
  {
    kind: "live",
    id: "cluster",
    label: "Cluster & fleet",
    short: "Fleet",
    route: { screen: "cluster" },
    requires: "fleet.read",
    group: "fleet",
    glyph: "◈",
  },
  {
    kind: "live",
    id: "administration",
    label: "Tenants & principals",
    short: "Tenants",
    // `principals`, not `tenants`: the tenants tab is `ClusterAdmin`, while this entry is offered to
    // anyone holding `tenant.manage`. Landing a TenantAdmin on a tab its role cannot open — and
    // whose probe renders a bare refusal with no tab bar — left the role with no route to the two
    // surfaces it exists for. `principals` requires exactly the capability that gates this entry.
    route: { screen: "admin", tab: "principals", tenant: null },
    // Gates on the weakest admin capability, not `imposter.*`: viewer/operator/editor hold none of
    // `CAPABILITY_MATRIX`'s admin capabilities, so the entry (and every control inside the screen)
    // is invisible below tenant-admin, where `tenant.manage` and `audit.read` both start.
    requires: "tenant.manage",
    group: "administration",
    glyph: "◇",
  },
  {
    kind: "planned",
    id: "specs",
    label: "Specs",
    issue: 148,
    note: "OpenAPI import, drift and contract validation (RFC-004).",
    glyph: "○",
  },
];

export function liveEntries(): LiveEntry[] {
  return NAV.filter((entry): entry is LiveEntry => entry.kind === "live");
}

export function plannedEntries(): PlannedEntry[] {
  return NAV.filter((entry): entry is PlannedEntry => entry.kind === "planned");
}
