import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { UseMutationResult, UseQueryResult } from "@tanstack/react-query";

import { apiGet, apiSend } from "../api/client.ts";
import {
  API_PATHS,
  frontDoorRoutePath,
  lifecyclePath,
  requestsPath,
} from "../api/paths.ts";
import type { components } from "../api/schema.ts";
import { type LogState, readLog } from "../features/requests/source.ts";
import { type Route, normalizeTable } from "../features/routes/order.ts";
import { type FleetView, fleetView } from "./fleetView.ts";
import { POLLED, POLLED_REQUESTS } from "./query.ts";
import { useSession } from "./session.tsx";

type Imposter = components["schemas"]["Imposter"];
type FleetMembers = components["schemas"]["FleetMembers"];
type FleetHealth = components["schemas"]["FleetHealth"];
type RouteTable = components["schemas"]["RouteTable"];

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
