import { describe, expect, it } from "vitest";

import type { components } from "../api/schema.ts";
import {
  DEFAULT_TENANT,
  FLEET_SCOPE,
  type Capability,
  can,
  roleAllows,
  roleForTenant,
  selectableTenants,
} from "./rbac.ts";

type Role = components["schemas"]["Role"];
type WhoAmI = components["schemas"]["WhoAmI"];

const ROLES: Role[] = ["viewer", "operator", "editor", "tenant-admin", "fleet-admin"];

/**
 * Mirrored by hand from `crates/rift-cluster-server/src/authz.rs::role_allows`, which is written as
 * explicit per-role arms precisely so it can be read as a table. If the two ever disagree the UI is
 * hiding something the API permits, or — the direction that matters — offering something it refuses.
 */
const EXPECTED: Record<Capability, Role[]> = {
  "imposter.read": ["viewer", "operator", "editor", "tenant-admin", "fleet-admin"],
  "imposter.lifecycle": ["operator", "editor", "tenant-admin", "fleet-admin"],
  "imposter.write": ["editor", "tenant-admin", "fleet-admin"],
  // `Action::ImposterDelete`. Identical to `imposter.write`'s row today, and kept as its own row
  // anyway: `authz.rs` grants them from separate arms, so the day one moves this table is what
  // fails rather than a delete button quietly appearing for a role the server refuses.
  "imposter.delete": ["editor", "tenant-admin", "fleet-admin"],
  // `Action::SourceRead`. Viewer, alongside `ImposterRead` — its own action server-side rather
  // than a wire-folding onto it, transcribed as its own row for the same reason `imposter.delete`
  // above gets one: the day the grant moves is what this row exists to catch.
  "source.read": ["viewer", "operator", "editor", "tenant-admin", "fleet-admin"],
  // `Action::SavedRequestsClear`. Operator-tier: clearing a log disturbs state without redefining
  // configuration, which is the line `authz.rs` draws between the Operator and Editor arms.
  "requests.clear": ["operator", "editor", "tenant-admin", "fleet-admin"],
  // `Action::ScenarioRead`. Its own Viewer row even though every read folds onto
  // `Action::ImposterRead` server-side (`principal.rs::map_action`: "reads fold onto imposter_read
  // regardless of route"), because this table transcribes `role_allows` — where `ScenarioRead` is a
  // separate arm — and not the coarser wire classification. Both are Viewer, so the two agree today.
  "scenario.read": ["viewer", "operator", "editor", "tenant-admin", "fleet-admin"],
  // `Action::ScenarioReset`. Operator-tier "disturb": resetting every scenario in a flow changes
  // running state without redefining configuration.
  "scenario.reset": ["operator", "editor", "tenant-admin", "fleet-admin"],
  // `Action::ScenarioWrite`. Editor-tier "redefine" — and the reason `scenario.reset` above cannot
  // be folded into it: the reset button and the set-state control are offered to different roles.
  "scenario.write": ["editor", "tenant-admin", "fleet-admin"],
  // `Action::SpaceTeardown`. Operator-tier, alongside the other "disturb" actions.
  "space.teardown": ["operator", "editor", "tenant-admin", "fleet-admin"],
  // `Action::SpaceStubWrite`. Editor-tier — and it authorizes **two** controls, see `rbac.ts`.
  "space.stubWrite": ["editor", "tenant-admin", "fleet-admin"],
  // `Action::FlowStateRead`. Viewer, same reasoning as `scenario.read`.
  "flowState.read": ["viewer", "operator", "editor", "tenant-admin", "fleet-admin"],
  // `Action::FlowStateClear`. Operator-tier, and it covers both the clear-one-entry delete and the
  // clear-the-whole-flow delete: `map_action` returns it for any `imposter.delete` under
  // `/admin/imposters/`, whether or not a key is named.
  "flowState.clear": ["operator", "editor", "tenant-admin", "fleet-admin"],
  "tenant.manage": ["tenant-admin", "fleet-admin"],
  "audit.read": ["tenant-admin", "fleet-admin"],
  "fleet.read": ["fleet-admin"],
  // `Action::ClusterAdmin`. Deliberately not `tenant-admin`: the whole `/admin/tenants` CRUD and
  // `PrincipalPut`/`PrincipalDelete` sit behind it, which is why it has to be separable from
  // `tenant.manage` rather than folded into it.
  "cluster.admin": ["fleet-admin"],
};

describe("roleAllows mirrors authz.rs::role_allows", () => {
  for (const [capability, granted] of Object.entries(EXPECTED) as [Capability, Role[]][]) {
    for (const role of ROLES) {
      const expected = granted.includes(role);
      it(`${role} ${expected ? "may" : "may NOT"} ${capability}`, () => {
        expect(roleAllows(role, capability)).toBe(expected);
      });
    }
  }

  it("does not let Viewer toggle lifecycle, and does let Operator", () => {
    // The distinction the console's write affordances hinge on: enable/disable is `LifecycleToggle`,
    // which Operator gains and Viewer does not. Gating the button on "is an editor" would hide a
    // control Operator is entitled to.
    expect(roleAllows("viewer", "imposter.lifecycle")).toBe(false);
    expect(roleAllows("operator", "imposter.lifecycle")).toBe(true);
  });

  it("splits the scenario controls across two tiers rather than gating them together", () => {
    // The two scenario writes are NOT one permission. `map_action` returns `ScenarioReset` for
    // `POST .../scenarios/reset` and `ScenarioWrite` for `PUT .../scenarios/{name}/state`, and
    // `role_allows` grants them from different arms — so an operator gets the reset button and not
    // the set-state control. Gating both on "may write" would offer an operator a control whose
    // every call answers 403.
    expect(roleAllows("operator", "scenario.reset")).toBe(true);
    expect(roleAllows("operator", "scenario.write")).toBe(false);
    expect(roleAllows("editor", "scenario.write")).toBe(true);
  });

  it("gates tearing a space down below scoping a stub into it", () => {
    // Same split on the space controls: `SpaceTeardown` is Operator, `SpaceStubWrite` is Editor.
    expect(roleAllows("operator", "space.teardown")).toBe(true);
    expect(roleAllows("operator", "space.stubWrite")).toBe(false);
  });

  it("gates *writing* flow state on space.stubWrite, because that is what the server checks", () => {
    /*
     * The one mapping that is not guessable from the route, and the reason this row exists.
     *
     * There is no `FlowStateWrite` action. `admin_front.rs` classifies
     * `PUT /admin/imposters/{port}/flow-state/{flowId}/{key}` as upstream's `imposter.write` with a
     * space, and `principal.rs::map_action` maps that to **`Action::SpaceStubWrite`** — its comment
     * names this exact route. So the set-value control is Editor-tier via the *space* capability,
     * while the delete beside it is Operator-tier via `flowState.clear`.
     *
     * An operator therefore gets "clear" and not "set" on the same panel. That asymmetry looks like
     * a bug until you read `map_action`, which is precisely why it is pinned here: inventing a
     * `flowState.write` capability to make the panel symmetrical would draw a control the server
     * refuses, and it is the drift this whole table exists to prevent.
     */
    expect(roleAllows("operator", "flowState.clear")).toBe(true);
    expect(roleAllows("operator", "space.stubWrite")).toBe(false);
    expect(roleAllows("editor", "space.stubWrite")).toBe(true);
  });

  it("lets a viewer read scenarios and flow state without disturbing either", () => {
    for (const capability of ["scenario.read", "flowState.read"] as const) {
      expect([capability, roleAllows("viewer", capability)]).toEqual([capability, true]);
    }
    for (const capability of ["scenario.reset", "flowState.clear", "space.teardown"] as const) {
      expect([capability, roleAllows("viewer", capability)]).toEqual([capability, false]);
    }
  });

  it("grants the fleet projection to fleet-admin only", () => {
    // `/_fleet/*` is ClusterAdmin-gated and answers 404 to everyone else (RFC-002 §8.4).
    for (const role of ROLES) {
      expect(roleAllows(role, "fleet.read")).toBe(role === "fleet-admin");
    }
  });
});

describe("roleForTenant", () => {
  it("reads the role bound to the tenant in view", () => {
    const whoami: WhoAmI = {
      principalId: "p-1",
      authorizationDisabled: false,
      bindings: [
        { tenant: "acme", role: "viewer" },
        { tenant: "globex", role: "editor" },
      ],
    };
    expect(roleForTenant(whoami, "acme")).toBe("viewer");
    expect(roleForTenant(whoami, "globex")).toBe("editor");
  });

  it("returns null for a tenant the principal is not bound to", () => {
    const whoami: WhoAmI = {
      principalId: "p-1",
      authorizationDisabled: false,
      bindings: [{ tenant: "acme", role: "viewer" }],
    };
    expect(roleForTenant(whoami, "initech")).toBeNull();
  });

  it("resolves no selection to the binding for `default`, not the strongest binding anywhere", () => {
    // No selection means no `X-Rift-Tenant`, and `requested_tenant` resolves that to the literal
    // tenant `default`. A principal bound only to `acme` therefore has no role where the request
    // will land — reporting `tenant-admin` would draw every one of that role's controls over calls
    // that all answer 404.
    const elsewhere: WhoAmI = {
      principalId: "p-1",
      authorizationDisabled: false,
      bindings: [{ tenant: "acme", role: "tenant-admin" }],
    };
    expect(roleForTenant(elsewhere, null)).toBeNull();
    expect(can(elsewhere, null, "imposter.lifecycle")).toBe(false);

    const here: WhoAmI = {
      principalId: "p-2",
      authorizationDisabled: false,
      bindings: [{ tenant: DEFAULT_TENANT, role: "operator" }],
    };
    expect(roleForTenant(here, null)).toBe("operator");
  });

  it("resolves any tenant to fleet-admin when the principal holds the fleet scope", () => {
    // FleetAdmin binds only on `*` (authz.rs: `BindingPut` refuses it anywhere else), so a literal
    // tenant lookup would find nothing and the console would render a fleet admin as unprivileged.
    const whoami: WhoAmI = {
      principalId: "root",
      authorizationDisabled: false,
      bindings: [{ tenant: FLEET_SCOPE, role: "fleet-admin" }],
    };
    expect(roleForTenant(whoami, "any-tenant-at-all")).toBe("fleet-admin");
  });
});

describe("can", () => {
  it("permits everything when the fleet enforces nothing", () => {
    // `authorizationDisabled` is the open-admin-plane bypass: no principals, no API key. Hiding
    // controls here would misreport an unsecured fleet as a restricted one.
    const open: WhoAmI = { principalId: null, authorizationDisabled: true, bindings: [] };
    expect(can(open, "default", "imposter.lifecycle")).toBe(true);
    expect(can(open, "default", "fleet.read")).toBe(true);
  });

  it("denies every capability when the principal has no binding in the tenant in view", () => {
    const whoami: WhoAmI = {
      principalId: "p-1",
      authorizationDisabled: false,
      bindings: [{ tenant: "acme", role: "fleet-admin" }],
    };
    expect(can(whoami, "initech", "imposter.read")).toBe(false);
  });
});

describe("selectableTenants", () => {
  it("lists the principal's bound tenants, fleet scope excluded", () => {
    // `*` is a scope, not a tenant; offering it in a switcher would send `X-Rift-Tenant: *`.
    const whoami: WhoAmI = {
      principalId: "p-1",
      authorizationDisabled: false,
      bindings: [
        { tenant: "acme", role: "viewer" },
        { tenant: FLEET_SCOPE, role: "fleet-admin" },
        { tenant: "globex", role: "editor" },
      ],
    };
    expect(selectableTenants(whoami)).toEqual(["acme", "globex"]);
  });

  it("de-duplicates and sorts so the switcher order does not depend on binding order", () => {
    const whoami: WhoAmI = {
      principalId: "p-1",
      authorizationDisabled: false,
      bindings: [
        { tenant: "globex", role: "editor" },
        { tenant: "acme", role: "viewer" },
        { tenant: "globex", role: "viewer" },
      ],
    };
    expect(selectableTenants(whoami)).toEqual(["acme", "globex"]);
  });

  it("drops bindings with no tenant rather than rendering an unnamed entry", () => {
    // `tenant` is optional in the WhoAmI schema, so a binding without one is representable.
    const whoami: WhoAmI = {
      principalId: "p-1",
      authorizationDisabled: false,
      bindings: [{ role: "viewer" }, { tenant: "acme", role: "viewer" }],
    };
    expect(selectableTenants(whoami)).toEqual(["acme"]);
  });
});
