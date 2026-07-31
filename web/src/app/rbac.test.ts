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
  "tenant.manage": ["tenant-admin", "fleet-admin"],
  "audit.read": ["tenant-admin", "fleet-admin"],
  "fleet.read": ["fleet-admin"],
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
