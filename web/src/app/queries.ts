import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { UseMutationResult, UseQueryResult } from "@tanstack/react-query";

import { apiGet, apiSend } from "../api/client.ts";
import { API_PATHS, lifecyclePath } from "../api/paths.ts";
import type { components } from "../api/schema.ts";
import { type FleetView, fleetView } from "./fleetView.ts";
import { POLLED } from "./query.ts";
import { useSession } from "./session.tsx";

type Imposter = components["schemas"]["Imposter"];
type FleetMembers = components["schemas"]["FleetMembers"];
type FleetHealth = components["schemas"]["FleetHealth"];

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
export function useFleetView(options: { enabled?: boolean } = {}): UseQueryResult<FleetView> {
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
    ...POLLED,
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
