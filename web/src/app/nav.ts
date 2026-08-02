import type { Capability } from "./rbac.ts";
import type { Route } from "./routing.ts";

export const ISSUE_URL = (issue: number): string =>
  `https://github.com/achird-labs/rift-cluster/issues/${issue}`;

/**
 * The rail's section headings, in render order.
 *
 * Grouping is the design prototype's (`docs/design/console/console-prototype.html`), and the
 * unbuilt group is last on purpose: it is a roadmap, so it reads after the things that work.
 */
export const NAV_GROUPS = ["mocks", "fleet", "administration", "planned"] as const;
export type NavGroup = (typeof NAV_GROUPS)[number];

export const GROUP_LABEL: Record<NavGroup, string> = {
  mocks: "Mocks",
  fleet: "Fleet",
  administration: "Administration",
  planned: "Not yet shipped",
};

/** A screen this slice built. `requires` decides whether it is offered, not whether it is permitted. */
export type LiveEntry = {
  kind: "live";
  id: string;
  label: string;
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
    route: { screen: "routes" },
    // The table is read with `Action::ImposterRead`; writing it needs `imposter.write`, which the
    // screen gates per control rather than hiding the whole screen from an operator who may read it.
    requires: "imposter.read",
    group: "mocks",
    glyph: "▤",
  },
  {
    kind: "live",
    id: "cluster",
    label: "Cluster & fleet",
    route: { screen: "cluster" },
    requires: "fleet.read",
    group: "fleet",
    glyph: "◈",
  },
  {
    kind: "live",
    id: "administration",
    label: "Tenants & principals",
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
    id: "scenarios",
    label: "Scenarios & state",
    // #232, not the RFC-005 epic #149. The scenario, space and flow-state routes already ship and
    // serve — what is missing is a console slice, so the chip must name the work that is actually
    // outstanding rather than an epic this screen does not depend on.
    issue: 232,
    note: "Scenarios, spaces and flow state. The backend ships; the screen does not.",
    glyph: "○",
  },
  {
    kind: "planned",
    id: "sources",
    label: "Sources",
    // #233, not #20 — which is **closed**. The source providers shipped and the chip went on
    // telling operators "not yet shipped, see #20", which is precisely the dead end RFC-006 §4's
    // "a visible roadmap, not a 404" exists to avoid.
    issue: 233,
    note: "Imposter sources, provenance and drift. The backend ships; the screen does not.",
    glyph: "○",
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
