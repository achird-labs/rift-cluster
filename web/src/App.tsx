import { useQuery, useQueryClient } from "@tanstack/react-query";
import type { ReactNode } from "react";

import { ApiError, apiGet } from "./api/client.ts";
import { API_PATHS } from "./api/paths.ts";
import type { components } from "./api/schema.ts";
import { Shell } from "./app/Shell.tsx";
import { FLEET_SCOPE, selectableTenants } from "./app/rbac.ts";
import { SessionProvider, initialTenant } from "./app/session.tsx";
import { ErrorNote } from "./components/primitives.tsx";
import { Login } from "./screens/Login.tsx";

type WhoAmI = components["schemas"]["WhoAmI"];
type Tenant = components["schemas"]["Tenant"];

export function App(): ReactNode {
  const client = useQueryClient();
  const whoami = useQuery({
    queryKey: ["whoami"],
    queryFn: () => apiGet<WhoAmI>(API_PATHS.whoami),
    // No `retry` override: the shared policy already declines to retry a 4xx, so a 401 goes
    // straight to the login screen while a dropped connection still gets its one retry. Turning
    // retries off here would make a transient blip on the very first request look like an
    // unreachable admin front.
  });

  if (whoami.isPending) return <p className="muted">Signing in…</p>;

  if (whoami.isError) {
    if (whoami.error instanceof ApiError && whoami.error.status === 401) {
      return (
        <Login onAuthenticated={() => void client.invalidateQueries({ queryKey: ["whoami"] })} />
      );
    }
    return (
      <main className="screen">
        <ErrorNote error={whoami.error} context="Could not reach the admin front" />
      </main>
    );
  }

  return <Authenticated whoami={whoami.data} />;
}

function Authenticated({ whoami }: { whoami: WhoAmI }): ReactNode {
  const bound = selectableTenants(whoami);
  const holdsFleetScope = whoami.bindings.some((binding) => binding.tenant === FLEET_SCOPE);

  /*
   * A FleetAdmin binds only to `*`, so its own bindings name no tenant to switch between. The list
   * then has to come from `GET /admin/tenants` — fleet-scoped, so exactly that principal may read
   * it, and no other role asks for it. A failure is not fatal: the console falls back to the
   * principal's default tenant rather than refusing to render.
   */
  const tenants = useQuery({
    queryKey: ["tenants"],
    queryFn: async () => {
      const list = await apiGet<Tenant[]>(API_PATHS.tenants);
      return list.filter((tenant) => tenant.deleted !== true).map((tenant) => tenant.id);
    },
    enabled: holdsFleetScope,
  });

  if (holdsFleetScope && tenants.isPending) return <p className="muted">Reading tenants…</p>;

  const available = holdsFleetScope ? (tenants.data ?? []) : bound;

  return (
    <>
      {/* A failed tenant list is NOT an empty fleet, and must not read as one: the switcher would
          simply vanish and the operator would be left with a console that looks healthy and has
          silently lost every tenant but the default. Non-fatal — the default tenant still works —
          so it is said out loud and the console renders anyway. */}
      {tenants.isError ? (
        <ErrorNote error={tenants.error} context="Could not list tenants, so the tenant switcher is unavailable" />
      ) : null}
      <SessionProvider whoami={whoami} tenants={available} initialTenant={initialTenant(available)}>
        <Shell />
      </SessionProvider>
    </>
  );
}
