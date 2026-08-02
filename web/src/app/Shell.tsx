import type { ReactNode } from "react";

import { Admin } from "../screens/Admin.tsx";
import { Fleet } from "../screens/Fleet.tsx";
import { ImposterDetail } from "../screens/ImposterDetail.tsx";
import { Imposters } from "../screens/Imposters.tsx";
import { RequestLog } from "../screens/RequestLog.tsx";
import { RouteTableScreen } from "../screens/Routes.tsx";
import { GROUP_LABEL, ISSUE_URL, NAV, NAV_GROUPS, type NavGroup, groupOf } from "./nav.ts";
import { SignOut } from "./SignOut.tsx";
import { useSession } from "./session.tsx";
import { type Route, toHash, useRoute } from "./routing.ts";

export function Shell(): ReactNode {
  const route = useRoute();

  return (
    <div className="app">
      <Nav current={route} />
      <div className="app-main">
        <header className="topbar">
          <TenantSwitcher />
          <div className="who">
            <Identity />
            <SignOut />
          </div>
        </header>
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
    case "admin":
      return <Admin tab={route.tab} tenant={route.tenant} />;
  }
}

/**
 * The full §4 screen list. A screen this slice has not built is greyed and carries its issue
 * number — "a visible roadmap, not a 404". Omitting them would present two screens as the whole
 * console.
 */
function Nav({ current }: { current: Route }): ReactNode {
  const { can } = useSession();

  // Hiding a screen the principal's role cannot read is UX only — the API refuses the same
  // principal either way (RFC-006 §3 rule 3) — but offering one that can only ever render an
  // authorization error is worse than not offering it.
  const visible = NAV.filter((entry) => entry.kind === "planned" || can(entry.requires));

  return (
    <nav className="rail" aria-label="Console sections">
      <div className="brand">
        <b>Rift</b>
        <span>CONSOLE</span>
      </div>
      {NAV_GROUPS.map((group: NavGroup) => {
        const entries = visible.filter((entry) => groupOf(entry) === group);
        // A group whose every entry the role cannot read draws no heading. An empty labelled
        // section would advertise a category the principal can never open.
        if (entries.length === 0) return null;
        return (
          <div className="nav-group" key={group}>
            <div className="eyebrow">{GROUP_LABEL[group]}</div>
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
                >
                  <span className="glyph" aria-hidden="true">
                    {entry.glyph}
                  </span>
                  {entry.label}
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
 * One tenant in view at a time (RFC-002 §8.1). The switcher only *selects* among bindings the
 * principal already holds — it grants nothing, and the console adds no header logic beyond sending
 * the selection.
 */
function TenantSwitcher(): ReactNode {
  const { tenant, tenants, setTenant } = useSession();

  // Genuinely nothing to report: no selection means requests carry no `X-Rift-Tenant` and there is
  // no tenant name that would be true of them.
  if (tenant === null) return null;

  /*
   * One tenant still gets a label, just not a control.
   *
   * This used to render nothing at all below two tenants — "an inert control would imply there is
   * something to switch to", which is right about the *control* and wrong about the *fact*. Every
   * read on every screen is scoped to a tenant, and a single-tenant principal (the common case for
   * a TenantAdmin) could not see which one anywhere in the console. A static value states the
   * scope without pretending to offer a choice.
   */
  if (tenants.length < 2) {
    return (
      <div className="tenant-switch" data-testid="tenant-current">
        <span className="eyebrow">Tenant</span>
        <span className="ident">{tenant}</span>
      </div>
    );
  }

  return (
    <label className="tenant-switch">
      <span className="eyebrow">Tenant</span>
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

/**
 * Up to two letters for the avatar mark, from the principal's name.
 *
 * Decorative — the name is rendered in full beside it — so it is `aria-hidden` at the render site
 * and needs no fallback beyond an empty string.
 */
export function initials(name: string): string {
  return name
    .split(/\s+/)
    .filter((word) => word.length > 0)
    .slice(0, 2)
    .map((word) => word[0]?.toUpperCase() ?? "")
    .join("");
}

/**
 * Who is signed in.
 *
 * Renders `displayName` and falls back to `principalId` only when the fleet has no row to carry a
 * name — the legacy `--api-key` identity, whose id is the readable `legacy:api-key`. It is
 * deliberately not the other way round: for a minted key `principalId` is `key:<sha256-hex>`,
 * which is not a credential (the raw key cannot be recovered from it, and argon2id is the real
 * boundary) but is indistinguishable from one on screen — and a console that displays
 * key-shaped strings teaches operators the wrong instinct about what is safe to share.
 */
function Identity(): ReactNode {
  const { whoami, role } = useSession();

  if (whoami.authorizationDisabled) {
    // Distinct from "an authenticated principal with zero bindings": the fleet defines no
    // principals and no API key, so the admin plane is open. Rendering that as an ordinary
    // identity would hide it.
    return (
      <span className="identity unenforced" data-testid="identity">
        Authorization disabled — this fleet enforces no principals
      </span>
    );
  }

  const name = whoami.displayName ?? whoami.principalId ?? "unidentified principal";

  return (
    <>
      <div className="identity" data-testid="identity">
        <div className="id">{name}</div>
        <div className="role">{role === null ? "no binding here" : role}</div>
      </div>
      <div className="avatar" aria-hidden="true">
        {initials(name)}
      </div>
    </>
  );
}
