import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { UseMutationResult, UseQueryResult } from "@tanstack/react-query";

import { apiGet, apiSend } from "../api/client.ts";
import {
  API_PATHS,
  auditPath,
  bindingPath,
  frontDoorRoutePath,
  lifecyclePath,
  principalPath,
  principalsPath,
  requestsPath,
  tenantPath,
} from "../api/paths.ts";
import type { components } from "../api/schema.ts";
import { type AuditRow, auditPage, readAuditRows } from "../features/admin/audit.ts";
import { stripApiKey } from "../features/admin/key.ts";
import { type LogState, readLog } from "../features/requests/source.ts";
import { type Route, normalizeTable } from "../features/routes/order.ts";
import { type FleetView, fleetView } from "./fleetView.ts";
import { POLLED, POLLED_REQUESTS } from "./query.ts";
import { useSession } from "./session.tsx";

type Imposter = components["schemas"]["Imposter"];
type FleetMembers = components["schemas"]["FleetMembers"];
type FleetHealth = components["schemas"]["FleetHealth"];
type RouteTable = components["schemas"]["RouteTable"];
type Tenant = components["schemas"]["Tenant"];
type TenantWrite = components["schemas"]["TenantWrite"];
type Principal = components["schemas"]["Principal"];
type PrincipalCreate = components["schemas"]["PrincipalCreate"];
type PrincipalUpdate = components["schemas"]["PrincipalUpdate"];
type IssuedPrincipal = components["schemas"]["IssuedPrincipal"];
type Role = components["schemas"]["Role"];

/**
 * The tenant is part of every query key, not just the request headers.
 *
 * Without it, switching tenants would show the previous tenant's imposters from cache until the
 * refetch landed — one tenant's data rendered under another tenant's name, which is the worst
 * possible way to be briefly wrong in a multi-tenant console.
 */
function key(parts: readonly unknown[], tenant: string | null): unknown[] {
  return [...parts, { tenant }];
}

export function useImposters(): UseQueryResult<Imposter[]> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["imposters"], tenant),
    queryFn: async () => {
      const body = await apiGet<{ imposters?: Imposter[] }>(API_PATHS.imposters, { tenant });
      // `imposters` is optional in the contract, so an absent array is a shape the schema permits —
      // a domain-optional read, not a swallowed failure. A non-2xx has already thrown in `client`.
      return body.imposters ?? [];
    },
    ...POLLED,
  });
}

/**
 * This node's own fleet reading.
 *
 * `enabled` is a caller's decision, not a fixed capability check, because the two callers want
 * opposite things from a principal that lacks the scope. The Cluster screen asks anyway and renders
 * the 404 as "fleet-scoped, not available to you" — someone who followed a bookmark deserves that
 * sentence. The imposter list only wants the reading to *qualify* what it shows, so it does not
 * ask: two guaranteed 404s behind every list load would be noise that means nothing.
 */
export function useFleetView(
  options: { enabled?: boolean; polled?: boolean } = {},
): UseQueryResult<FleetView> {
  return useQuery({
    queryKey: ["fleet"],
    queryFn: async () => {
      const [members, health] = await Promise.all([
        apiGet<FleetMembers>(API_PATHS.fleetMembers),
        apiGet<FleetHealth>(API_PATHS.fleetHealth),
      ]);
      return fleetView(members, health);
    },
    enabled: options.enabled ?? true,
    /*
     * `polled: false` reads the fleet once per mount instead of every 5s. The request log needs
     * this reading only to name its coverage — which changes on membership events, not on traffic —
     * and it must ask even as a principal that will be refused, since the per-node label is the
     * screen's exit criterion. Polling it there would put two guaranteed 404s every 5s behind a
     * screen most roles use, for a sentence that would not change.
     */
    ...(options.polled === false ? {} : POLLED),
  });
}

export function useLifecycleToggle(): UseMutationResult<
  unknown,
  Error,
  { port: number; enable: boolean }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ port, enable }) =>
      apiSend("POST", lifecyclePath(port, enable), undefined, { tenant }),
    // Re-read rather than patch the cache: `SetEnabled` is a replicated op, so what the fleet
    // actually applied is the only thing worth showing. This is also what makes the list reflect
    // the change immediately instead of at the next poll tick.
    onSettled: () => client.invalidateQueries({ queryKey: ["imposters"] }),
  });
}

/**
 * One node's recorded requests.
 *
 * A failed read resolves to `{ kind: "unknown" }` rather than rejecting, because on this screen the
 * two outcomes are different sentences and the query's own error state cannot tell them apart: an
 * empty array and an unreachable node both arrive here as "no rows to show". `readLog` is the only
 * place that decision is made.
 *
 * Resolving instead of rejecting opts this query out of `retryTransportFailures` — a `queryFn` that
 * never rejects is never retried. That is a deliberate trade and not a free one: a transient blip
 * shows the "unknown" alert immediately rather than after one silent retry. The 2s poll heals it on
 * the next tick, and on this screen an honest "could not read" for two seconds beats a retry that
 * delays the distinction this whole screen is built to preserve.
 */
export function useRequestLog(port: number): UseQueryResult<LogState> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["requests", port], tenant),
    queryFn: async (): Promise<LogState> => {
      try {
        return readLog(await apiGet<unknown>(requestsPath(port), { tenant }));
      } catch (error) {
        return {
          kind: "unknown",
          reason: error instanceof Error ? error.message : "this node could not be reached",
        };
      }
    },
    ...POLLED_REQUESTS,
  });
}

export function useRouteTable(): UseQueryResult<Route[]> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["front-door-routes"], tenant),
    queryFn: async () =>
      normalizeTable(await apiGet<RouteTable>(API_PATHS.frontDoorRoutes, { tenant })),
    ...POLLED,
  });
}

/** Raised when the table moved underneath the editor, so the screen can offer refresh-and-reapply. */
export class RouteTableConflict extends Error {
  readonly current: Route[];

  constructor(current: Route[]) {
    super("the route table changed since it was loaded");
    this.name = "RouteTableConflict";
    this.current = current;
  }
}

/**
 * Replace the whole table, refusing to overwrite a concurrent edit.
 *
 * `If-Match` is not available here — `admin_front.rs:1811` restricts it to single-imposter
 * operations — so the precondition is a re-read compared against the table the draft was based on.
 *
 * This narrows the lost-update window; it does not close it. A write that commits between this
 * re-read and the `PUT` is still lost, and nothing client-side can prevent that. Closing it needs a
 * server-side precondition on this route (filed as a follow-up).
 */
export function usePutRoutes(): UseMutationResult<
  unknown,
  Error,
  { draft: Route[]; base: Route[] }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ draft, base }) => {
      const current = normalizeTable(
        await apiGet<RouteTable>(API_PATHS.frontDoorRoutes, { tenant }),
      );
      if (JSON.stringify(current) !== JSON.stringify(base)) {
        throw new RouteTableConflict(current);
      }
      return apiSend<RouteTable>("PUT", API_PATHS.frontDoorRoutes, { routes: draft }, { tenant });
    },
    /*
     * Adopt the stored table the `PUT` returns straight into the cache.
     *
     * Without this the cached read stays at the pre-save table until the invalidation refetch
     * lands, and in that window the editor's adopt-when-clean effect sees a clean draft beside an
     * older `loaded` and reverts the screen to it. It converges, but a save that briefly shows as
     * undone — and stays that way if the refetch fails — is exactly the kind of quiet lie this
     * console is being careful about elsewhere.
     */
    onSuccess: (stored) => client.setQueryData(key(["front-door-routes"], tenant), () =>
      normalizeTable(stored as RouteTable),
    ),
    onSettled: () => client.invalidateQueries({ queryKey: ["front-door-routes"] }),
  });
}

/**
 * Remove one route by id.
 *
 * Preferred over a whole-table `PUT` whenever a single removal is what the operator meant: it
 * cannot take an unrelated concurrent edit down with it.
 */
export function useDeleteRoute(): UseMutationResult<unknown, Error, { routeId: string }> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ routeId }) =>
      apiSend("DELETE", frontDoorRoutePath(routeId), undefined, { tenant }),
    onSettled: () => client.invalidateQueries({ queryKey: ["front-door-routes"] }),
  });
}

/**
 * The admin plane (RFC-002). Every one of these routes addresses its tenant through the URL path,
 * never `X-Rift-Tenant` — see `paths.ts` — so, unlike the hooks above, none of these pass `tenant`
 * to `apiGet`/`apiSend`. `getAudit` is the one exception: it has no tenant path segment, so the
 * header is how a non-fleet-admin's rows are scoped at all.
 */

const ADMIN_TENANTS_KEY = ["admin-tenants"];
const adminTenantKey = (tenantId: string): unknown[] => ["admin-tenant", tenantId];
const adminPrincipalsKey = (tenantId: string): unknown[] => ["admin-principals", tenantId];

/**
 * `enabled` is the caller's decision because `TenantList` is `Action::ClusterAdmin` scoped to the
 * **fleet**, not to the caller's tenant. A tenant-admin holds no `*` binding, so this read is a
 * permanent 404 for them — asking anyway turns their Administration landing into a red error every
 * five seconds, which is the failure the `cluster.admin` capability was introduced to stop.
 */
export function useTenants(options: { enabled?: boolean } = {}): UseQueryResult<Tenant[]> {
  return useQuery({
    queryKey: ADMIN_TENANTS_KEY,
    queryFn: () => apiGet<Tenant[]>(API_PATHS.tenants),
    enabled: options.enabled ?? true,
    ...POLLED,
  });
}

/**
 * A pure existence-and-permission probe for one tenant (RFC-002 §8.4).
 *
 * The screen must never render anything from this query's data — only whether it errored, and with
 * which status. The API's anti-oracle (a cross-tenant probe and a nonexistent tenant answer
 * byte-identical `404`s) only holds if the console does not rebuild a distinguishing signal on top
 * of it by rendering content that happens to differ between the two.
 */
export function useTenantProbe(
  tenantId: string,
  options: { enabled: boolean },
): UseQueryResult<Tenant> {
  return useQuery({
    queryKey: adminTenantKey(tenantId),
    queryFn: () => apiGet<Tenant>(tenantPath(tenantId)),
    enabled: options.enabled,
    ...POLLED,
  });
}

export function useCreateTenant(): UseMutationResult<unknown, Error, TenantWrite> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: (body) => apiSend("POST", API_PATHS.tenants, body),
    onSettled: () => client.invalidateQueries({ queryKey: ADMIN_TENANTS_KEY }),
  });
}

export function useSaveTenant(): UseMutationResult<
  unknown,
  Error,
  { tenantId: string; body: TenantWrite }
> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ tenantId, body }) => apiSend("PUT", tenantPath(tenantId), body),
    onSettled: (_data, _error, vars) => {
      client.invalidateQueries({ queryKey: ADMIN_TENANTS_KEY });
      client.invalidateQueries({ queryKey: adminTenantKey(vars.tenantId) });
    },
  });
}

export function useDeleteTenant(): UseMutationResult<unknown, Error, { tenantId: string }> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ tenantId }) => apiSend("DELETE", tenantPath(tenantId)),
    onSettled: () => client.invalidateQueries({ queryKey: ADMIN_TENANTS_KEY }),
  });
}

export function usePrincipals(tenantId: string): UseQueryResult<Principal[]> {
  return useQuery({
    queryKey: adminPrincipalsKey(tenantId),
    queryFn: () => apiGet<Principal[]>(principalsPath(tenantId)),
    ...POLLED,
  });
}

/**
 * Mint a principal, handing the raw key to the caller **out of band**.
 *
 * `onIssued` receives the one-time `apiKey`; the mutation itself resolves to the *stripped* record,
 * so React Query never stores the key anywhere. Returning the full response and sanitising it in
 * `onSuccess` is not enough: `useMutation` keeps its own copy of the resolved value in the
 * MutationCache, where `setQueryData` cannot reach it, and it stays readable from the client (and
 * React Query Devtools) for `gcTime` after the panel is dismissed. The key exists for one moment,
 * and the only place it lives is the component state `onIssued` writes it into.
 */
export function useCreatePrincipal(
  tenantId: string,
  onIssued: (issued: IssuedPrincipal) => void,
): UseMutationResult<Omit<IssuedPrincipal, "apiKey">, Error, PrincipalCreate> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body) => {
      const issued = await apiSend<IssuedPrincipal>("POST", principalsPath(tenantId), body);
      onIssued(issued);
      return stripApiKey(issued);
    },
    onSuccess: (created) => {
      client.setQueryData<Principal[]>(adminPrincipalsKey(tenantId), (existing) =>
        existing === undefined
          ? existing
          : [...existing, { ...created, auth: "apiKey", disabled: false }],
      );
    },
  });
}

export function useSavePrincipal(): UseMutationResult<
  unknown,
  Error,
  { tenantId: string; principalId: string; body: PrincipalUpdate }
> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ tenantId, principalId, body }) =>
      apiSend("PUT", principalPath(tenantId, principalId), body),
    onSettled: (_data, _error, vars) =>
      client.invalidateQueries({ queryKey: adminPrincipalsKey(vars.tenantId) }),
  });
}

export function useDeletePrincipal(): UseMutationResult<
  unknown,
  Error,
  { tenantId: string; principalId: string }
> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ tenantId, principalId }) =>
      apiSend("DELETE", principalPath(tenantId, principalId)),
    onSettled: (_data, _error, vars) =>
      client.invalidateQueries({ queryKey: adminPrincipalsKey(vars.tenantId) }),
  });
}

export function usePutBinding(): UseMutationResult<
  unknown,
  Error,
  { tenantId: string; principalId: string; role: Role }
> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ tenantId, principalId, role }) =>
      apiSend("PUT", bindingPath(tenantId, principalId), { role }),
    onSettled: (_data, _error, vars) =>
      client.invalidateQueries({ queryKey: adminPrincipalsKey(vars.tenantId) }),
  });
}

export function useDeleteBinding(): UseMutationResult<
  unknown,
  Error,
  { tenantId: string; principalId: string }
> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ tenantId, principalId }) =>
      apiSend("DELETE", bindingPath(tenantId, principalId)),
    onSettled: (_data, _error, vars) =>
      client.invalidateQueries({ queryKey: adminPrincipalsKey(vars.tenantId) }),
  });
}

/**
 * `since` is caller-owned state (RFC-002 §8's cursor is client-driven), not derived here — the
 * screen advances it with `nextSince` once a page has rendered, and that decision does not belong
 * inside the hook that reads one page.
 */
/** The page size the audit viewer asks for. Exported so the pager can tell a short page from a full one. */
export const AUDIT_PAGE_SIZE = 100;

export function useAuditRows(tenant: string | null, since: number): UseQueryResult<AuditRow[]> {
  return useQuery({
    queryKey: ["admin-audit", tenant, since],
    queryFn: async () =>
      auditPage(readAuditRows(await apiGet<unknown>(auditPath(since, AUDIT_PAGE_SIZE), { tenant }))),
    ...POLLED,
  });
}
