import type { ApiPath } from "./client.ts";

/**
 * Every admin route C4 asks for, in one place and typed as `ApiPath` — `keyof paths` from the
 * generated contract. A route the contract does not publish will not typecheck here, which is the
 * cheap half of RFC-006 §11's "no field sourced from a UI-only endpoint".
 */
export const API_PATHS = {
  whoami: "/admin/whoami",
  imposters: "/imposters",
  tenants: "/admin/tenants",
  fleetMembers: "/_fleet/members",
  fleetHealth: "/_fleet/health",
  session: "/session",
  frontDoorRoutes: "/front-door/routes",
  audit: "/admin/audit",
} as const satisfies Record<string, ApiPath>;

/** Path builders for the templated routes, so a port is interpolated in exactly one place. */
export const imposterPath = (port: number): string => `/imposters/${port}`;
export const lifecyclePath = (port: number, enabled: boolean): string =>
  `/imposters/${port}/${enabled ? "enable" : "disable"}`;
export const requestsPath = (port: number): string => `/imposters/${port}/requests`;

/**
 * The poll target for a write that answered `202` (parked). Fleet-scoped, so a caller without
 * fleet-admin sees a 404 here whatever the write actually did — see `features/writes/commit.ts`.
 */
export const fleetOpPath = (opId: string): string =>
  `/_fleet/ops/${encodeURIComponent(opId)}`;
export const frontDoorRoutePath = (routeId: string): string =>
  `/front-door/routes/${encodeURIComponent(routeId)}`;

/**
 * The admin plane addresses a tenant through the path, never `X-Rift-Tenant` — every
 * `/admin/tenants/*` route below takes `tenantId` as a path segment, so these calls send no tenant
 * header at all.
 */
export const tenantPath = (tenantId: string): string =>
  `/admin/tenants/${encodeURIComponent(tenantId)}`;
export const principalsPath = (tenantId: string): string => `${tenantPath(tenantId)}/principals`;

/*
 * The principal id goes in **raw**, deliberately — this is not a missing `encodeURIComponent`.
 *
 * `tenancy::classify` splits `req.uri().path()` as hyper delivers it, and hyper does not normalise
 * (see the note in `console.rs`), so the server compares the percent-encoded text literally. Every
 * console-minted principal is `key:<sha256-hex>` (`api_key_principal_id`), and encoding it to
 * `key%3A…` matches no stored principal: disable, delete and every binding write answer 404/400.
 * Tenant ids are safe to encode only because they are constrained to `[a-z0-9-]`.
 */
export const principalPath = (tenantId: string, principalId: string): string =>
  `${principalsPath(tenantId)}/${principalId}`;
export const bindingPath = (tenantId: string, principalId: string): string =>
  `${tenantPath(tenantId)}/bindings/${principalId}`;

/**
 * Can this principal id be addressed in a URL path at all?
 *
 * The server matches the raw path, so the id cannot be encoded — and that leaves ids the console
 * simply cannot target. `require_principal_id` permits any non-control string up to 256 bytes
 * (deliberately wide, to hold an OIDC `subject`), so ids arrive here that a URL will mangle:
 *
 * - `#` and `?` **silently truncate** the path — `alice#bob` addresses `alice`, so "Delete
 *   alice#bob" would delete a *different, existing* principal with no error to correlate. This is
 *   the case that makes the check load-bearing rather than cosmetic.
 * - `/` adds a segment, so `classify` stops matching and the request falls through to a 404.
 * - whitespace and non-ASCII get encoded by the URL parser and then match nothing.
 *
 * Everything else in the RFC 3986 `pchar` set — including the `:` every minted id carries — is safe
 * unencoded. Callers use this to render a write control inert with a stated reason rather than
 * offering an action that would hit the wrong record.
 */
const PATH_SAFE_ID = /^[A-Za-z0-9\-._~:@!$&'()*+,;=%]+$/;

export const isAddressablePrincipalId = (principalId: string): boolean =>
  PATH_SAFE_ID.test(principalId);
export const auditPath = (since: number, limit: number): string =>
  `${API_PATHS.audit}?since=${since}&limit=${limit}`;
