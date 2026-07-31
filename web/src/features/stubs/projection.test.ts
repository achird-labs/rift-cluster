import fc from "fast-check";
import { describe, expect, it } from "vitest";

import { STUB_FIELDS, type StubForm, blankForm, project, render } from "./projection.ts";

/**
 * An arbitrary form whose every field is in the modelled set — the input the round-trip property is
 * about. `null` is generated for every field because "this stub has no path predicate" is a form
 * state, not a missing test case: it is the state that decides whether `render` emits the key at
 * all, and therefore the state most likely to break the projection.
 */
const anyForm: fc.Arbitrary<StubForm> = fc.record({
  id: fc.option(fc.string(), { nil: null }),
  method: fc.option(fc.constantFrom("GET", "POST", "PUT", "DELETE", "PATCH"), { nil: null }),
  path: fc.option(fc.string(), { nil: null }),
  statusCode: fc.option(fc.integer({ min: 100, max: 599 }), { nil: null }),
  contentType: fc.option(fc.string(), { nil: null }),
  body: fc.option(fc.string(), { nil: null }),
});

describe("the form ⟷ JSON projection round-trips", () => {
  it("projects everything it renders back to a form, never to raw-only", () => {
    fc.assert(
      fc.property(anyForm, (form) => {
        expect(project(render(form)).kind).toBe("form");
      }),
    );
  });

  it("renders the projection of a rendered form to the same JSON", () => {
    // The lossless half of AC3, stated as `render ∘ project ∘ render == render`. Comparing the two
    // *forms* instead would be the weaker claim: two different forms can render the same JSON (a
    // field the model cannot distinguish), and it is the JSON the fleet stores.
    fc.assert(
      fc.property(anyForm, (form) => {
        const json = render(form);
        const projected = project(json);
        if (projected.kind !== "form") throw new Error("expected a form");
        expect(render(projected.form)).toEqual(json);
      }),
    );
  });

  it("emits no key for a field the operator left empty", () => {
    // A rendered stub with every field null must be `{}` and not a scaffold of empty containers —
    // an empty `predicates: []` is a different stub from one with no `predicates` at all.
    expect(render(blankForm())).toEqual({});
  });
});

describe("a stub the form does not model is raw-only, with every unmodelled key named", () => {
  it("names a top-level field outside the modelled set", () => {
    const projected = project({
      id: "s-1",
      space: "tenant-a",
      scenarioName: "checkout",
      behaviors: [{ wait: 50 }],
      predicates: [{ equals: { method: "GET", path: "/x" } }],
      responses: [{ is: { statusCode: 200 } }],
    });

    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    // Every one of them, not merely the first: an operator told about `space` alone would edit the
    // form, save, and silently drop `scenarioName` and `behaviors` — the exact loss AC2 forbids.
    expect(projected.unmodelledKeys).toContain("space");
    expect(projected.unmodelledKeys).toContain("scenarioName");
    expect(projected.unmodelledKeys).toContain("behaviors[0].wait");
  });

  it("names a second predicate rather than modelling only the first", () => {
    const projected = project({
      predicates: [{ equals: { path: "/a" } }, { contains: { body: "hello" } }],
    });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(["predicates[1].contains.body"]);
  });

  it("names a second response, which the single-`is` model cannot hold", () => {
    const projected = project({
      responses: [{ is: { statusCode: 200 } }, { is: { statusCode: 500 } }],
    });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(["responses[1].is.statusCode"]);
  });

  it("names a header the model does not carry, keeping the Content-Type one it does", () => {
    const projected = project({
      responses: [{ is: { headers: { "Content-Type": "application/json", "X-Trace": "1" } } }],
    });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(['responses[0].is.headers["X-Trace"]']);
  });

  it("treats a modelled key holding the wrong JSON type as unmodelled, not as a coercion", () => {
    // `statusCode: "200"` is a string. Coercing it would rewrite the operator's stub on the way
    // through the form; naming it keeps the raw text authoritative.
    const projected = project({ responses: [{ is: { statusCode: "200" } }] });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(["responses[0].is.statusCode"]);
  });

  it("refuses anything that is not a JSON object at the root", () => {
    for (const value of [null, 42, "a stub", [1, 2]]) {
      const projected = project(value);
      expect([value, projected.kind]).toEqual([value, "rawOnly"]);
    }
  });

  it("accepts an empty container on the way to a modelled field", () => {
    // `predicates: []` carries no key the model would drop, so it is projectable — and projects to
    // a form with no method and no path.
    const projected = project({ id: "s-1", predicates: [], responses: [] });
    expect(projected.kind).toBe("form");
    if (projected.kind !== "form") return;
    expect(projected.form).toEqual({ ...blankForm(), id: "s-1" });
  });

  it("reads a fully modelled stub into the form's fields", () => {
    const projected = project({
      id: "s-1",
      predicates: [{ equals: { method: "POST", path: "/orders" } }],
      responses: [{ is: { statusCode: 201, headers: { "Content-Type": "application/json" }, body: "{}" } }],
    });
    expect(projected.kind).toBe("form");
    if (projected.kind !== "form") return;
    expect(projected.form).toEqual({
      id: "s-1",
      method: "POST",
      path: "/orders",
      statusCode: 201,
      contentType: "application/json",
      body: "{}",
    });
  });
});

describe("the modelled set is data, not code", () => {
  it("describes every form field in one table, so widening it is an edit to that table", () => {
    // RFC-006 §12 Q2's answer lives here: the shipped set is whatever `STUB_FIELDS` lists, and both
    // directions of the projection are driven by it. A field added to the table needs no change to
    // `project` or `render`.
    expect(STUB_FIELDS.map((field) => field.key)).toEqual([
      "id",
      "method",
      "path",
      "statusCode",
      "contentType",
      "body",
    ]);
    for (const field of STUB_FIELDS) {
      expect([field.key, field.at.length]).toEqual([field.key, expect.any(Number)]);
      expect(field.at.length).toBeGreaterThan(0);
    }
  });
});
