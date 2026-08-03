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

  it("no longer inspects predicates content at all — that responsibility moved to predicates.ts", () => {
    // Before #247, `walk` modelled a single `equals.method`/`equals.path` predicate directly, and
    // a second predicate object (or an operator this table didn't know) fell out as unmodelled.
    // Predicates are now the sibling `predicates.ts` projection's whole job — including refusing a
    // shape like this one (see its "refuses an operator it does not know" and "carrying two
    // operators" tests) — so `project` treats the entire `predicates` subtree as out of its scope,
    // even a shape it used to refuse. The editor composes both projections; this file's contract is
    // only ever about the *other* keys.
    const projected = project({
      predicates: [{ equals: { path: "/a" } }, { contains: { body: "hello" } }],
    });
    expect(projected.kind).toBe("form");
  });

  it("no longer inspects responses content at all — that responsibility moved to responses.ts", () => {
    // The mirror of the `predicates` test above, and the whole point of #248. This table used to
    // model `responses[0].is` alone, so a second response, a second header, or a JSON-object body
    // each sent the stub to raw-only from HERE. All three are now `responses.ts`'s to accept or
    // refuse (see its round-trip and AC5 tests), so `project` stops at the key — even for shapes it
    // used to refuse. The editor composes all three projections; this file's contract is only ever
    // about the *other* keys.
    const projected = project({
      responses: [
        { is: { statusCode: 200, headers: { "X-Trace": "1" }, body: { ok: true } } },
        { proxy: { to: "http://api.example.com" } },
      ],
    });
    expect(projected.kind).toBe("form");
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
    // Neither `predicates` nor `responses` is in scope for this projection now — they do not, and
    // should not, appear in the form. Their own round trips are `predicates.test.ts`'s and
    // `responses.test.ts`'s jobs.
    expect(projected.form).toEqual({ id: "s-1" });
  });
});

describe("the modelled set is data, not code", () => {
  it("describes every form field in one table, so widening it is an edit to that table", () => {
    // RFC-006 §12 Q2's answer lives here: the shipped set is whatever `STUB_FIELDS` lists, and both
    // directions of the projection are driven by it. A field added to the table needs no change to
    // `project` or `render`.
    expect(STUB_FIELDS.map((field) => field.key)).toEqual(["id"]);
    for (const field of STUB_FIELDS) {
      expect([field.key, field.at.length]).toEqual([field.key, expect.any(Number)]);
      expect(field.at.length).toBeGreaterThan(0);
    }
  });
});

describe("#257 — the metadata the server adds on read", () => {
  it("opens a stub carrying `_links`, which every stub read from the API has", () => {
    /*
     * `StubWithLinks` flattens the stub and appends `_links`, so this is what a real
     * `GET /imposters/{port}` returns for every stub. Naming it unmodelled was a second,
     * independent reason the editor opened raw-only for everything — fixing the string
     * `statusCode` alone left the symptom unchanged, which the new e2e caught.
     */
    const projected = project({
      id: "s-1",
      predicates: [{ equals: { method: "GET", path: "/orders/42" } }],
      responses: [{ is: { statusCode: "200" } }],
      _links: { self: { href: "http://node-1:2525/imposters/4545/stubs/0" } },
    });
    expect(projected.kind).toBe("form");
    if (projected.kind !== "form") return;
    expect(projected.form).toEqual({ id: "s-1" });
  });

  it("does not write `_links` back — the server owns it and regenerates it", () => {
    // Dropping it is not the silent rewrite the module forbids: it is not part of the document the
    // operator authored, and a PUT carrying one would assert a self-link the console does not own.
    expect(render({ id: "s-1" })).toEqual({ id: "s-1" });
  });
});
