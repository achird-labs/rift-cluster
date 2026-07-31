import { QueryClient } from "@tanstack/react-query";

import { ApiError } from "../api/client.ts";

/**
 * RFC-006 §6: **v1 is polling, on purpose.** 5 seconds against local state or a loopback proxy hop
 * is roughly one request per second per screen. SSE is deferred to v2 and, when it lands, carries
 * *cache invalidation only* — events trigger a refetch and never carry data, so a dropped event
 * costs one poll interval of staleness rather than correctness.
 */
export const POLL_INTERVAL_MS = 5_000;

/**
 * The polling contract for a screen that should track the fleet while someone is looking at it.
 *
 * `refetchIntervalInBackground: false` is the load-bearing half and the reason it is written out
 * rather than left to the default: without it, every tab an operator ever opened and forgot keeps
 * asking the admin front for the imposter list every five seconds, forever.
 */
export const POLLED = {
  refetchInterval: POLL_INTERVAL_MS,
  refetchIntervalInBackground: false,
} as const;

/**
 * RFC-006 §6: the request log refetches faster than everything else, because it is the screen
 * someone watches while re-running a test — five seconds is long enough to make them wonder whether
 * the call arrived at all, which is the exact question the screen exists to answer.
 */
export const REQUEST_POLL_INTERVAL_MS = 2_000;

/** Same hidden-tab pause as `POLLED`; only the cadence differs. */
export const POLLED_REQUESTS = {
  refetchInterval: REQUEST_POLL_INTERVAL_MS,
  refetchIntervalInBackground: false,
} as const;

/**
 * Retry a flaky hop; never re-ask a question the fleet has already answered.
 *
 * A 4xx from the admin front is a decision, not a hiccup — 401 means the session lapsed, 403 that
 * the role refuses it, and 404 on a fleet-scoped route that the principal lacks the scope (RFC-002
 * §8.4). Retrying those doubles the audited denials and delays the honest message on screen by a
 * backoff interval, for an outcome that cannot change.
 */
export function retryTransportFailures(failureCount: number, error: Error): boolean {
  if (error instanceof ApiError && error.status >= 400 && error.status < 500) return false;
  return failureCount < 1;
}

export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        // Admin state is edited by other operators and by the fleet itself, so a cached read is
        // stale the moment it lands.
        staleTime: 0,
        retry: retryTransportFailures,
        // A tab returning to the foreground should show current state immediately rather than
        // whatever was true when it was backgrounded — and it is the other half of the pause above.
        refetchOnWindowFocus: true,
      },
    },
  });
}
