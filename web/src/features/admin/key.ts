import type { components } from "../../api/schema.ts";

type IssuedPrincipal = components["schemas"]["IssuedPrincipal"];

/**
 * The API mints the raw key once, in the `201` from `POST /admin/tenants/:id/principals`, and
 * stores only an argon2id hash — a later `GET` has nothing to return, so there is no "reveal
 * later" to build.
 */
export const KEY_NOT_SHOWN_AGAIN =
  "This key will not be shown again. Copy it now — the fleet stores only its hash.";

/**
 * A copy of `issued` without `apiKey`, for anything that could be cached: React Query's cache, a
 * later `setQueryData`, anything that survives past the moment the mint panel is dismissed.
 *
 * Returns a new object rather than mutating `issued` — the caller still needs the raw key to show
 * the mint panel itself, and mutating the argument out from under it would be the same bug in a
 * different order.
 */
export function stripApiKey(issued: IssuedPrincipal): Omit<IssuedPrincipal, "apiKey"> {
  const { apiKey: _apiKey, ...rest } = issued;
  return rest;
}
