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
} as const satisfies Record<string, ApiPath>;

/** Path builders for the templated routes, so a port is interpolated in exactly one place. */
export const imposterPath = (port: number): string => `/imposters/${port}`;
export const lifecyclePath = (port: number, enabled: boolean): string =>
  `/imposters/${port}/${enabled ? "enable" : "disable"}`;
