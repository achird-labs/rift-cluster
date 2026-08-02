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
  /**
   * `Action::ImposterDelete` — its own action server-side, not a shade of `ImposterWrite`.
   *
   * The two are granted together today (both start at Editor), so folding them into one capability
   * here would decide identically and read as harmless. It is the shape of drift this table exists
   * to prevent: `authz.rs` keeps them separate precisely so the grant can move, and the day it does
   * a delete button gated on `imposter.write` is drawn for a role the server refuses. Transcribing
   * the action that actually authorizes the call costs one line and cannot go stale silently —
   * `rbac.test.ts` mirrors the Rust table.
   */
  | "imposter.delete"
  /**
   * `Action::SourceRead` — Viewer, alongside `ImposterRead`.
   *
   * Its own action server-side, not a shade of `imposter.read`: `GET /admin/sources` authorizes
   * directly against `SourceRead` rather than folding onto `ImposterRead` the way `scenario.read`'s
   * wire route does. A source is a distinct resource from the imposters it produces — it can be
   * declared and read before it has ever pulled anything — so it earns its own capability rather
   * than reusing the one for the imposters it may one day own.
   */
  | "source.read"
  /**
   * `Action::SavedRequestsClear` — Operator-tier, alongside `LifecycleToggle`.
   *
   * `authz.rs` groups the Operator arm as "disturb" actions (clear a log, reset a scenario, tear
   * down a space) against Editor's "redefine" ones. Clearing a request log changes no
   * configuration, which is why it sits below `imposter.write` rather than above it — and why it
   * is transcribed as its own capability rather than reusing `imposter.lifecycle`, which
   * authorizes a different call.
   */
  | "requests.clear"
  /**
   * `Action::ScenarioRead` — Viewer, alongside the other reads.
   *
   * Server-side every read under `/imposters/:port` folds onto `Action::ImposterRead`
   * (`principal.rs::map_action`), so this capability names a distinction the wire does not make.
   * It is transcribed anyway because this table mirrors `role_allows`, where `ScenarioRead` is its
   * own arm — the same reasoning as `imposter.delete` above. Both sit at Viewer, so the two agree
   * today and this row is what fails if that stops being true.
   */
  | "scenario.read"
  /**
   * `Action::ScenarioReset` — Operator-tier "disturb", from `POST .../scenarios/reset`.
   *
   * Deliberately not folded into `scenario.write`: `role_allows` grants them from different arms,
   * so an operator gets the reset button and not the set-state control beside it.
   */
  | "scenario.reset"
  /** `Action::ScenarioWrite` — Editor-tier "redefine", from `PUT .../scenarios/{name}/state`. */
  | "scenario.write"
  /** `Action::SpaceTeardown` — Operator-tier, from `DELETE .../spaces/{flowId}`. */
  | "space.teardown"
  /**
   * `Action::SpaceStubWrite` — Editor-tier, and it authorizes **two** controls that do not look
   * related: scoping a stub into a space, and *setting a flow-state value*.
   *
   * There is no `FlowStateWrite` action. `admin_front.rs` classifies
   * `PUT /admin/imposters/{port}/flow-state/{flowId}/{key}` as upstream's `imposter.write` carrying
   * a space, and `principal.rs::map_action` maps that pair to `SpaceStubWrite` — its comment names
   * this route explicitly ("both redefine behaviour rather than merely disturbing it").
   *
   * The consequence is visible on screen and looks like a bug until you read that mapping: on the
   * flow-state panel an operator may *clear* an entry (`flowState.clear`, Operator) but may not
   * *set* one. Inventing a `flowState.write` capability to make the panel symmetrical would draw a
   * control the server refuses — the exact drift this table exists to prevent.
   */
  | "space.stubWrite"
  /** `Action::FlowStateRead` — Viewer. Same wire-folding note as `scenario.read`. */
  | "flowState.read"
  /**
   * `Action::FlowStateClear` — Operator-tier.
   *
   * Covers both flow-state deletes: `map_action` returns it for any `imposter.delete` under
   * `/admin/imposters/`, whether the path names a single key or the whole flow.
   */
  | "flowState.clear"
  | "tenant.manage"
  | "audit.read"
  | "fleet.read"
  /**
   * `Action::ClusterAdmin` — held by `fleet-admin` alone.
   *
   * Distinct from `tenant.manage`, which is where a tenant-admin stops. The tenancy routes split
   * across the two: `PrincipalList`/`PrincipalCreate`/`BindingPut` are `TenantManage`, but the whole
   * `/admin/tenants` CRUD *and* `PrincipalPut`/`PrincipalDelete` are `ClusterAdmin`. Without this
   * capability the console can only gate on `tenant.manage` and ends up drawing a tenant-admin
   * buttons that answer 403 or 404 every time — the exact thing this table exists to prevent.
   */
  | "cluster.admin";

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
      return (
        capability === "imposter.read" ||
        capability === "scenario.read" ||
        capability === "flowState.read" ||
        capability === "source.read"
      );
    case "operator":
      return (
        roleAllows("viewer", capability) ||
        capability === "imposter.lifecycle" ||
        capability === "requests.clear" ||
        capability === "scenario.reset" ||
        capability === "space.teardown" ||
        capability === "flowState.clear"
      );
    case "editor":
      return (
        roleAllows("operator", capability) ||
        capability === "imposter.write" ||
        capability === "imposter.delete" ||
        capability === "scenario.write" ||
        capability === "space.stubWrite"
      );
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
