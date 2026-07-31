import type { components } from "../../api/schema.ts";
import { FLEET_SCOPE, type Capability, roleAllows } from "../../app/rbac.ts";

type Role = components["schemas"]["Role"];

/** The four roles `BindingPut` accepts on any tenant scope other than `*`, in ladder order. */
const TENANT_ROLES: readonly Role[] = ["viewer", "operator", "editor", "tenant-admin"];

/** `*` is a scope, never a tenant — see `rbac.ts`'s `FLEET_SCOPE`. */
export function isFleetScope(scope: string): boolean {
  return scope === FLEET_SCOPE;
}

/**
 * The roles a binding picker may offer for `scope`.
 *
 * `fleet-admin` binds only on the fleet scope — `BindingPut` refuses it everywhere else — and the
 * fleet scope refuses every other role in turn, so the two lists are disjoint, not a subset of one
 * another. Offering `fleet-admin` in a tenant picker would teach an operator to attempt a grant the
 * API always refuses.
 */
export function assignableRoles(scope: string): readonly Role[] {
  return isFleetScope(scope) ? ["fleet-admin"] : TENANT_ROLES;
}

/**
 * A transcription of `role_allows` (`crates/rift-cluster-server/src/authz.rs`), narrowed to the
 * capabilities this console's `Capability` type names — RFC-002 §4.2 as a table a reviewer can read
 * beside the Rust, rather than a rank comparison that hides which capability a role actually adds.
 *
 * Two placements worth stating twice, because they are easy to "tidy" into the wrong place: `audit.
 * read` starts at `tenant-admin`, not `viewer` and not folded into `tenant.manage`; and `imposter.
 * write` starts at `editor` — `operator` gets lifecycle control, not write.
 *
 * **Derived from `rbac.ts::roleAllows`, never hand-written.** An independent second transcription of
 * `authz.rs::role_allows` would be a second copy of a security table, and two copies drift — the
 * screen would then teach a reader a ladder the console itself no longer enforces. This is a *view*
 * of the one transcription, so it cannot disagree with it.
 */
export const ROLE_LADDER = [
  "viewer",
  "operator",
  "editor",
  "tenant-admin",
  "fleet-admin",
] as const satisfies readonly Role[];

export const CAPABILITIES = [
  "imposter.read",
  "imposter.lifecycle",
  "imposter.write",
  "tenant.manage",
  "audit.read",
  "fleet.read",
  "cluster.admin",
] as const satisfies readonly Capability[];

const held = (role: Role): readonly Capability[] =>
  CAPABILITIES.filter((capability) => roleAllows(role, capability));

// Written out per role rather than built with `Object.fromEntries`, whose `{[k: string]: …}` return
// needs a cast to become a `Record<Role, …>` — and a cast here would be the one place this table
// could silently lose a role.
export const CAPABILITY_MATRIX: Record<Role, readonly Capability[]> = {
  viewer: held("viewer"),
  operator: held("operator"),
  editor: held("editor"),
  "tenant-admin": held("tenant-admin"),
  "fleet-admin": held("fleet-admin"),
};
