import { useQuery } from "@tanstack/react-query";
import type { UseQueryResult } from "@tanstack/react-query";

import { apiGet } from "../api/client.ts";
import { API_PATHS } from "../api/paths.ts";
import type { components } from "../api/schema.ts";

type FleetHealth = components["schemas"]["FleetHealth"];

/**
 * Is this browser signed in?
 *
 * That is the whole of a session since #550. There is one credential — the fleet's admin API key,
 * exchanged for the `rift_session` cookie by `POST /session` — and one identity behind it, so there
 * is no principal to name, no role to hold and no tenant to select. Whoever is signed in is *the*
 * administrator, and every control the console draws is one they may use. What used to be a
 * `whoami` read plus a capability table is now a single yes/no.
 *
 * There is no endpoint whose purpose is to answer it. `GET /_fleet/health` is the cheapest thing
 * that goes through the front's `authenticate` gate: it renders this node's own readiness view
 * without consulting the ring, so it costs a fan-out nothing and still answers `401` for a browser
 * holding no session. The console reads the same projection again on the Fleet screen; that read
 * has its own key and its own polling, so the two do not share a cache entry and neither can make
 * the other stale.
 */
export const SESSION_KEY = ["session"] as const;

export function useSession(): UseQueryResult<FleetHealth> {
  return useQuery({
    queryKey: SESSION_KEY,
    queryFn: () => apiGet<FleetHealth>(API_PATHS.fleetHealth),
    // No `retry` override: the shared policy already declines to retry a 4xx, so a 401 goes
    // straight to the login screen while a dropped connection still gets its one retry. Turning
    // retries off here would make a transient blip on the very first request look like an
    // unreachable admin front.
  });
}
