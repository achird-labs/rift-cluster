import type { components } from "../api/schema.ts";

type Role = components["schemas"]["Role"];
type WhoAmI = components["schemas"]["WhoAmI"];

/**
 * The scope a `FleetAdmin` binding names. `BindingPut` refuses `FleetAdmin` anywhere else and
 * refuses every other role here (`authz.rs`), so this is a scope, never a tenant — it is not
 * offerable in the tenant switcher and must not be sent as `X-Rift-Tenant`.
 */
export const FLEET_SCOPE = "*";

/**
 * The tenant a request lands in when it carries no `X-Rift-Tenant`.
 *
 * A literal id (`control.rs`'s `DEFAULT_TENANT`), not "whichever tenant this principal has".
 * `requested_tenant` returns `TenantId::default()` and `decide` matches it against bindings by
 * value, so a principal bound only to `acme` is **not** bound to the tenant an unscoped request
 * reaches.
 */
export const DEFAULT_TENANT = "default";

/**
 * The subset of `authz.rs`'s `Action` this console can actually reach in C4, named for what the UI
 * decides rather than for the wire action, plus the two admin capabilities the nav consults.
 */
export type Capability =
  | "imposter.read"
  | "imposter.lifecycle"
  | "imposter.write"
  | "tenant.manage"
  | "audit.read"
  | "fleet.read";

/**
 * A transcription of `crates/rift-cluster-server/src/authz.rs::role_allows`, kept in the same
 * shape — each role adds to the one below it — so the two can be read side by side.
 *
 * This is a *UX* table. RFC-006 §3 rule 3: hiding a control is presentation, the API is the
 * boundary, and it re-checks every call. The failure this guards against is therefore the mirror
 * image of a security hole — offering an operator a button their role cannot use, or hiding one it
 * can (which is why `imposter.lifecycle` is Operator's, not Editor's).
 */
export function roleAllows(role: Role, capability: Capability): boolean {
  switch (role) {
    case "viewer":
      return capability === "imposter.read";
    case "operator":
      return roleAllows("viewer", capability) || capability === "imposter.lifecycle";
    case "editor":
      return roleAllows("operator", capability) || capability === "imposter.write";
    case "tenant-admin":
      return (
        roleAllows("editor", capability) ||
        capability === "tenant.manage" ||
        capability === "audit.read"
      );
    case "fleet-admin":
      return true;
  }
}

/**
 * The role the principal holds in `tenant`, or `null` when it holds none there.
 *
 * The fleet-scope arm is not a shortcut: a `FleetAdmin` is bound to `*` and to no named tenant, so
 * a literal lookup finds nothing and would render the most privileged principal as unprivileged.
 */
export function roleForTenant(whoami: WhoAmI, tenant: string | null): Role | null {
  const fleetAdmin = whoami.bindings.some(
    (b) => b.tenant === FLEET_SCOPE && b.role === "fleet-admin",
  );
  if (fleetAdmin) return "fleet-admin";
  // No selection means no `X-Rift-Tenant`, which lands in `default` — so the binding that governs
  // is the one for `default`, not the strongest binding the principal holds somewhere else. A
  // principal bound only to `acme` has no role here, and drawing `acme`'s controls would offer
  // buttons whose every call answers 404.
  const wanted = tenant ?? DEFAULT_TENANT;
  return whoami.bindings.find((binding) => binding.tenant === wanted)?.role ?? null;
}

/** Whether the console should draw a control for `capability` in the tenant currently in view. */
export function can(whoami: WhoAmI, tenant: string | null, capability: Capability): boolean {
  // The open-admin-plane bypass: no principals and no API key, so the fleet enforces nothing.
  // Hiding controls here would present an unsecured admin plane as a restricted one.
  if (whoami.authorizationDisabled) return true;
  const role = roleForTenant(whoami, tenant);
  return role !== null && roleAllows(role, capability);
}

/** The tenants the switcher may offer: the principal's own bindings, fleet scope excluded. */
export function selectableTenants(whoami: WhoAmI): string[] {
  const named = whoami.bindings.flatMap((b) =>
    b.tenant === undefined || b.tenant === FLEET_SCOPE ? [] : [b.tenant],
  );
  return [...new Set(named)].sort();
}
