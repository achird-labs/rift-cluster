import { describe, expect, it } from "vitest";

import { FLEET_SCOPE } from "../../app/rbac.ts";
import { CAPABILITY_MATRIX, assignableRoles, isFleetScope } from "./roles.ts";

describe("assignableRoles", () => {
  // `fleet-admin` binds only on the fleet scope `*`; `BindingPut` refuses it anywhere else. Offering
  // it in a tenant picker would teach operators to attempt a grant the API always refuses.
  it("never offers fleet-admin as an in-tenant role", () => {
    expect(assignableRoles("acme")).not.toContain("fleet-admin");
    expect(assignableRoles("default")).not.toContain("fleet-admin");
  });

  it("offers the four in-tenant roles, in ladder order", () => {
    expect(assignableRoles("acme")).toEqual(["viewer", "operator", "editor", "tenant-admin"]);
  });

  // The mirror of the same rule: the API refuses every *other* role on the fleet scope, so the
  // fleet-scope picker offers exactly one.
  it("offers only fleet-admin on the fleet scope", () => {
    expect(assignableRoles(FLEET_SCOPE)).toEqual(["fleet-admin"]);
  });

  it("recognises the fleet scope, and does not mistake a tenant named similarly", () => {
    expect(isFleetScope(FLEET_SCOPE)).toBe(true);
    expect(isFleetScope("acme")).toBe(false);
    expect(isFleetScope("**")).toBe(false);
    expect(isFleetScope("")).toBe(false);
  });
});

describe("CAPABILITY_MATRIX — readable against RFC-002 §4.2", () => {
  it("is a strict superset ladder: each role keeps everything the one below it has", () => {
    const ladder = ["viewer", "operator", "editor", "tenant-admin", "fleet-admin"] as const;
    // Built as explicit pairs rather than indexed in the loop body: `noUncheckedIndexedAccess` is on
    // project-wide, so `ladder[i - 1]` is `T | undefined` and cannot key the matrix directly.
    const rungs = ladder.flatMap((role, index) => {
      const below = ladder[index - 1];
      return below === undefined ? [] : [[below, role] as const];
    });
    expect(rungs).toHaveLength(ladder.length - 1);

    for (const [below, above] of rungs) {
      for (const capability of CAPABILITY_MATRIX[below]) {
        expect(CAPABILITY_MATRIX[above]).toContain(capability);
      }
    }
  });

  // Two deliberate placements the design note asks to keep visible, because both are easy to
  // "tidy" into the wrong place.
  it("starts audit.read at tenant-admin — not viewer, and not part of tenant.manage", () => {
    expect(CAPABILITY_MATRIX.viewer).not.toContain("audit.read");
    expect(CAPABILITY_MATRIX.editor).not.toContain("audit.read");
    expect(CAPABILITY_MATRIX["tenant-admin"]).toContain("audit.read");
  });

  it("gives lifecycle to operator but withholds imposter.write until editor", () => {
    expect(CAPABILITY_MATRIX.operator).toContain("imposter.lifecycle");
    expect(CAPABILITY_MATRIX.operator).not.toContain("imposter.write");
    expect(CAPABILITY_MATRIX.editor).toContain("imposter.write");
  });
});
