import type { ReactNode } from "react";

import { Fleet } from "../screens/Fleet.tsx";
import { ImposterDetail } from "../screens/ImposterDetail.tsx";
import { Imposters } from "../screens/Imposters.tsx";
import { RequestLog } from "../screens/RequestLog.tsx";
import { RouteTableScreen } from "../screens/Routes.tsx";
import { Scenarios } from "../screens/Scenarios.tsx";
import { GROUP_LABEL, ISSUE_URL, NAV, NAV_GROUPS, type NavGroup, groupOf } from "./nav.ts";
import { ToastHost } from "../components/toast.tsx";
import { SignOut } from "./SignOut.tsx";
import { useFleetView } from "./queries.ts";
import { type Route, toHash, useRoute } from "./routing.ts";

export function Shell(): ReactNode {
  const route = useRoute();

  return (
    <ToastHost>
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <b>Rift</b>
          <span>CLUSTER</span>
        </div>
        <Nav current={route} />
        <div className="who">
          <FleetName />
          <SignOut />
        </div>
      </header>
      <main>
        <Screen route={route} />
      </main>
    </div>
    </ToastHost>
  );
}

function Screen({ route }: { route: Route }): ReactNode {
  switch (route.screen) {
    case "imposters":
      return <Imposters />;
    case "imposter":
      return <ImposterDetail port={route.port} />;
    case "cluster":
      return <Fleet />;
    case "requests":
      return <RequestLog port={route.port} />;
    case "routes":
      return <RouteTableScreen />;
    case "scenarios":
      return <Scenarios port={route.port} flow={route.flow} />;
  }
}

/**
 * The full §4 screen list. A screen this slice has not built is greyed and carries its issue
 * number — "a visible roadmap, not a 404". Omitting them would present two screens as the whole
 * console.
 *
 * Horizontal since the warm-paper redesign, which changes what a group can be: there is no line to
 * spend on a heading, so a group is a run of entries between two hairlines and carries its name to
 * assistive tech through `role="group"` instead.
 *
 * Every entry is drawn. Since #550 there is one credential and one identity, so there is no role
 * for which a screen could be unreachable and nothing left to filter on.
 */
function Nav({ current }: { current: Route }): ReactNode {
  return (
    <nav className="nav" aria-label="Console sections">
      {NAV_GROUPS.map((group: NavGroup) => {
        const entries = NAV.filter((entry) => groupOf(entry) === group);
        // An empty group draws nothing — not even its separator. Today that is only the roadmap
        // run, which is empty whenever every promised screen has shipped.
        if (entries.length === 0) return null;
        return (
          <div className="nav-group" key={group} role="group" aria-label={GROUP_LABEL[group]}>
            {entries.map((entry) => {
              if (entry.kind === "planned") {
                return (
                  // No `aria-disabled` here: it has no defined meaning on a non-interactive
                  // element, and putting it on a container whose only child IS interactive claims
                  // the issue link is unavailable when it works. The entry is simply not a nav
                  // link — it is a label plus a reference — which is what "greyed, not a 404" is.
                  <div
                    key={entry.id}
                    data-testid={`nav-${entry.id}`}
                    data-planned="true"
                    className="nav-item pending"
                  >
                    <span className="glyph" aria-hidden="true">
                      {entry.glyph}
                    </span>
                    <span className="planned-label">{entry.label}</span>
                    <a
                      className="issue"
                      href={ISSUE_URL(entry.issue)}
                      target="_blank"
                      rel="noreferrer"
                      title={entry.note}
                    >
                      <span className="visually-hidden">
                        {entry.label} is not built yet — issue{" "}
                      </span>
                      #{entry.issue}
                    </a>
                  </div>
                );
              }
              const hash = toHash(entry.route);
              return (
                <a
                  key={entry.id}
                  className="nav-item"
                  data-testid={`nav-${entry.id}`}
                  href={hash}
                  aria-current={toHash(current) === hash ? "page" : undefined}
                  /* The bar prints `short` where the full label would not fit, but the entry is
                     still named by its full label. `short` is a substring of it (asserted in
                     nav.test.ts), so the visible text appears in the accessible name and WCAG
                     2.5.3 holds — "click Fleet" still addresses this control. */
                  aria-label={entry.short === undefined ? undefined : entry.label}
                >
                  <span className="glyph" aria-hidden="true">
                    {entry.glyph}
                  </span>
                  {entry.short ?? entry.label}
                </a>
              );
            })}
          </div>
        );
      })}
    </nav>
  );
}

/**
 * The fleet's operator-set name (#373) — the sharper use the issue calls out: an operator with
 * staging and production open in two tabs can otherwise tell them apart only by port, while every
 * destructive act this console offers is fleet-wide.
 *
 * Renders nothing rather than "Unnamed" here, unlike the Fleet screen's Ring card. That card is
 * the one place this fact has a card to itself and can afford to state absence as a fact; on
 * every other screen a global "Unnamed" fleet label would be noise before an operator has ever
 * named anything.
 */
function FleetName(): ReactNode {
  const fleet = useFleetView({ polled: false });

  if (!fleet.isSuccess || fleet.data.fleetName === null) return null;

  return (
    <div className="fleet-name-badge" data-testid="topbar-fleet-name">
      <span className="eyebrow">Fleet</span>
      <span className="ident">{fleet.data.fleetName}</span>
    </div>
  );
}
