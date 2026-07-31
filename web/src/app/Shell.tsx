import type { ReactNode } from "react";

import { Fleet } from "../screens/Fleet.tsx";
import { ImposterDetail } from "../screens/ImposterDetail.tsx";
import { Imposters } from "../screens/Imposters.tsx";
import { RequestLog } from "../screens/RequestLog.tsx";
import { RouteTableScreen } from "../screens/Routes.tsx";
import { ISSUE_URL, NAV } from "./nav.ts";
import { useSession } from "./session.tsx";
import { type Route, toHash, useRoute } from "./routing.ts";

export function Shell(): ReactNode {
  const route = useRoute();

  return (
    <div className="shell">
      <header className="topbar">
        <span className="brand">Rift</span>
        <TenantSwitcher />
        <Identity />
      </header>
      <div className="body">
        <Nav current={route} />
        <main>
          <Screen route={route} />
        </main>
      </div>
    </div>
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
  }
}

/**
 * The full §4 screen list. A screen this slice has not built is greyed and carries its issue
 * number — "a visible roadmap, not a 404". Omitting them would present two screens as the whole
 * console.
 */
function Nav({ current }: { current: Route }): ReactNode {
  const { can } = useSession();

  return (
    <nav>
      <ul>
        {NAV.map((entry) => {
          if (entry.kind === "planned") {
            return (
              // No `aria-disabled` here: it has no defined meaning on a non-interactive element,
              // and putting it on a container whose only child IS interactive claims the issue
              // link is unavailable when it works. The entry is simply not a nav link — it is a
              // label plus a reference — which is what "greyed, not a 404" actually is.
              <li key={entry.id} data-testid={`nav-${entry.id}`} data-planned="true" className="planned">
                <span className="planned-label">{entry.label}</span>
                <a href={ISSUE_URL(entry.issue)} target="_blank" rel="noreferrer" title={entry.note}>
                  <span className="visually-hidden">{entry.label} is not built yet — issue </span>#
                  {entry.issue}
                </a>
              </li>
            );
          }
          // Hiding a screen the principal's role cannot read is UX only — the API refuses the same
          // principal either way (RFC-006 §3 rule 3) — but offering one that can only ever render
          // an authorization error is worse than not offering it.
          if (!can(entry.requires)) return null;
          const hash = toHash(entry.route);
          return (
            <li key={entry.id} data-testid={`nav-${entry.id}`}>
              <a href={hash} aria-current={toHash(current) === hash ? "page" : undefined}>
                {entry.label}
              </a>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}

/**
 * One tenant in view at a time (RFC-002 §8.1). The switcher only *selects* among bindings the
 * principal already holds — it grants nothing, and the console adds no header logic beyond sending
 * the selection.
 */
function TenantSwitcher(): ReactNode {
  const { tenant, tenants, setTenant } = useSession();

  // Nothing to switch between. An inert control would imply there is.
  if (tenants.length < 2 || tenant === null) return null;

  return (
    <label className="tenant">
      <span>Tenant</span>
      {/* `value={tenant}` with no fallback, deliberately: the control must display the tenant the
          requests actually carry. Substituting `tenants[0]` for an unset selection would label
          every read with a tenant the header never named. */}
      <select
        data-testid="tenant-switcher"
        value={tenant}
        onChange={(event) => setTenant(event.target.value)}
      >
        {tenants.map((name) => (
          <option key={name} value={name}>
            {name}
          </option>
        ))}
      </select>
    </label>
  );
}

function Identity(): ReactNode {
  const { whoami, role } = useSession();

  return (
    <span className="identity" data-testid="identity">
      {whoami.authorizationDisabled ? (
        // Distinct from "an authenticated principal with zero bindings": the fleet defines no
        // principals and no API key, so the admin plane is open. Rendering that as an ordinary
        // identity would hide it.
        <>Authorization disabled — this fleet enforces no principals</>
      ) : (
        <>
          {whoami.principalId ?? "unidentified principal"}
          {role === null ? " · no binding here" : ` · ${role}`}
        </>
      )}
    </span>
  );
}
