import { createContext, useCallback, useContext, useMemo, useState } from "react";
import type { ReactNode } from "react";

import type { components } from "../api/schema.ts";
import { DEFAULT_TENANT, type Capability, can as canDo, roleForTenant } from "./rbac.ts";
import { preferenceStore } from "./storage.ts";

type WhoAmI = components["schemas"]["WhoAmI"];
type Role = components["schemas"]["Role"];

/** Per-browser, per-origin. A tenant selection is a view preference, not a credential. */
export const TENANT_STORAGE_KEY = "rift-console.tenant";

export type Session = {
  whoami: WhoAmI;
  /** `null` means "send no `X-Rift-Tenant`", i.e. act under the principal's default tenant. */
  tenant: string | null;
  /** The tenants the switcher may offer. Fewer than two means no switcher is drawn. */
  tenants: string[];
  role: Role | null;
  setTenant: (tenant: string) => void;
  can: (capability: Capability) => boolean;
};

const SessionContext = createContext<Session | null>(null);

export function useSession(): Session {
  const session = useContext(SessionContext);
  if (session === null) {
    // Not a swallow: a screen rendered outside the provider would otherwise silently act as an
    // unauthenticated principal with no tenant, which reads as an empty fleet.
    throw new Error("useSession was called outside a SessionProvider");
  }
  return session;
}

/**
 * The tenant to open in, given the tenants this principal can actually select.
 *
 * Never `null` when there is anything to pick, and that is the point. A `null` selection sends no
 * `X-Rift-Tenant`, so the request lands in `default` — while the switcher, having nothing else to
 * display, would show the *first* tenant in the list. The label and the header would then disagree
 * on every read, and for a principal not bound to `default` every one of those reads 404s while
 * the console claims to be showing a tenant it never asked for. Worse, the operator could not
 * correct it: re-selecting the already-displayed option fires no change event.
 */
export function initialTenant(available: string[]): string | null {
  const stored = preferenceStore().getItem(TENANT_STORAGE_KEY);
  // A remembered tenant the principal is no longer bound to would 404 every read (RFC-002 §8.4),
  // and the console would look broken rather than re-defaulted.
  if (stored !== null && available.includes(stored)) return stored;
  // `default` is where an unscoped request lands, so preferring it keeps the opening view the same
  // one the API would have chosen on its own.
  if (available.includes(DEFAULT_TENANT)) return DEFAULT_TENANT;
  return available[0] ?? null;
}

export function SessionProvider({
  whoami,
  tenants,
  initialTenant = null,
  children,
}: {
  whoami: WhoAmI;
  tenants: string[];
  initialTenant?: string | null;
  children: ReactNode;
}): ReactNode {
  const [tenant, setTenantState] = useState<string | null>(initialTenant);

  const setTenant = useCallback((next: string) => {
    setTenantState(next);
    preferenceStore().setItem(TENANT_STORAGE_KEY, next);
  }, []);

  const value = useMemo<Session>(
    () => ({
      whoami,
      tenant,
      tenants,
      role: roleForTenant(whoami, tenant),
      setTenant,
      can: (capability) => canDo(whoami, tenant, capability),
    }),
    [whoami, tenant, tenants, setTenant],
  );

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}
