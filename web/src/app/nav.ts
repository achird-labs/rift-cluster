import type { Capability } from "./rbac.ts";
import type { Route } from "./routing.ts";

export const ISSUE_URL = (issue: number): string =>
  `https://github.com/achird-labs/rift-cluster/issues/${issue}`;

/** A screen this slice built. `requires` decides whether it is offered, not whether it is permitted. */
export type LiveEntry = {
  kind: "live";
  id: string;
  label: string;
  route: Route;
  requires: Capability;
};

/** A screen RFC-006 §4 names but nothing has built yet. It is shown, greyed, with its issue. */
export type PlannedEntry = {
  kind: "planned";
  id: string;
  label: string;
  issue: number;
  note: string;
};

export type NavEntry = LiveEntry | PlannedEntry;

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
  },
  {
    kind: "live",
    id: "cluster",
    label: "Cluster",
    route: { screen: "cluster" },
    requires: "fleet.read",
  },
  {
    kind: "live",
    id: "requests",
    label: "Request log",
    route: { screen: "requests", port: null },
    requires: "imposter.read",
  },
  {
    kind: "live",
    id: "routes",
    label: "Front-door routes",
    route: { screen: "routes" },
    // The table is read with `Action::ImposterRead`; writing it needs `imposter.write`, which the
    // screen gates per control rather than hiding the whole screen from an operator who may read it.
    requires: "imposter.read",
  },
  {
    kind: "planned",
    id: "scenarios",
    label: "Scenarios",
    issue: 149,
    note: "Scenario and flow state (RFC-005).",
  },
  {
    kind: "planned",
    id: "sources",
    label: "Sources",
    issue: 20,
    note: "Data sources and tracked corpora.",
  },
  {
    kind: "planned",
    id: "specs",
    label: "Specs",
    issue: 148,
    note: "OpenAPI import, drift and contract validation (RFC-004).",
  },
  {
    kind: "live",
    id: "administration",
    label: "Administration",
    route: { screen: "admin", tab: "tenants", tenant: null },
    // Gates on the weakest admin capability, not `imposter.*`: viewer/operator/editor hold none of
    // `CAPABILITY_MATRIX`'s admin capabilities, so the entry (and every control inside the screen)
    // is invisible below tenant-admin, where `tenant.manage` and `audit.read` both start.
    requires: "tenant.manage",
  },
];

export function liveEntries(): LiveEntry[] {
  return NAV.filter((entry): entry is LiveEntry => entry.kind === "live");
}

export function plannedEntries(): PlannedEntry[] {
  return NAV.filter((entry): entry is PlannedEntry => entry.kind === "planned");
}
