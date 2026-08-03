import fc from "fast-check";
import { describe, expect, it } from "vitest";

import {
  PREDICATE_FIELDS,
  PREDICATE_OPERATORS,
  type PredicateClause,
  type PredicateItem,
  projectPredicates,
  renderPredicates,
} from "./predicates.ts";

/**
 * The domain the round-trip property is quantified over: **predicates the builder can produce**.
 *
 * That qualifier is the whole reason this is provable. The builder always emits one entry per
 * predicate object, so `render → project → render` is exact. Reading is wider than writing — a
 * hand-written multi-entry object projects too (see the legacy test below) — but a property over
 * *every* document the engine accepts would be a property about the engine, not about this form.
 */
const anEntry = fc.record({
  field: fc.constantFrom(...PREDICATE_FIELDS),
  key: fc.option(fc.stringMatching(/^[A-Za-z][A-Za-z0-9-]{0,12}$/), { nil: null }),
  value: fc.oneof(fc.string(), fc.integer(), fc.boolean()),
});

const aSelector = fc.option(
  fc.record({
    kind: fc.constantFrom("jsonpath" as const, "xpath" as const),
    expression: fc.stringMatching(/^\$?[A-Za-z.[\]0-9$]{1,20}$/),
    ns: fc.option(fc.dictionary(fc.stringMatching(/^[a-z]{1,4}$/), fc.webUrl()), { nil: null }),
  }),
  { nil: null },
);

const aClause: fc.Arbitrary<PredicateClause> = fc.record({
  operator: fc.constantFrom(...PREDICATE_OPERATORS),
  // Exactly one, because that is what the builder writes. Multi-entry clauses are a *read* shape.
  entries: anEntry.map((entry) => [entry]),
  caseSensitive: fc.option(fc.boolean(), { nil: null }),
  except: fc.option(fc.stringMatching(/^[a-z.]{1,20}$/), { nil: null }),
  selector: aSelector,
});

const anItem: fc.Arbitrary<PredicateItem> = fc.oneof(
  aClause.map((clause) => ({ kind: "clause" as const, clause })),
  fc
    .record({
      op: fc.constantFrom("or" as const, "not" as const),
      clauses: fc.array(aClause, { minLength: 1, maxLength: 3 }),
    })
    .map(({ op, clauses }) => ({ kind: "group" as const, op, clauses })),
);

describe("the predicate projection round-trips", () => {
  it("renders, projects and re-renders to byte-identical JSON", () => {
    /*
     * AC3, stated as `render ∘ project ∘ render == render` over the builder's own domain. Compared
     * on the JSON rather than on the model because two models can be distinguishable while their
     * documents are not, and it is the document the fleet stores — the same reasoning the existing
     * `projection.test.ts` round-trip gives.
     */
    fc.assert(
      fc.property(fc.array(anItem, { maxLength: 4 }), (items) => {
        const json = renderPredicates(items);
        const projected = projectPredicates({ predicates: json });
        if (projected.kind !== "predicates") {
          throw new Error(`expected a projection, got ${JSON.stringify(projected)}`);
        }
        expect(JSON.stringify(renderPredicates(projected.items))).toBe(JSON.stringify(json));
      }),
      { numRuns: 300 },
    );
  });

  it("never sends its own output to raw-only", () => {
    // The other half: anything the builder can write, it must be able to read back. A builder that
    // produces a document its own projection refuses would strand the operator in raw mode on the
    // next open, with no way back.
    fc.assert(
      fc.property(fc.array(anItem, { maxLength: 4 }), (items) => {
        expect(projectPredicates({ predicates: renderPredicates(items) }).kind).toBe("predicates");
      }),
      { numRuns: 300 },
    );
  });

  it("emits no predicates key at all for an empty set", () => {
    // `predicates: []` and no `predicates` are different documents; an untouched builder means the
    // second, exactly as a null form field emits no key.
    expect(renderPredicates([])).toEqual([]);
  });
});

describe("the shapes the builder covers", () => {
  it("covers every operator and every request field", () => {
    expect([...PREDICATE_OPERATORS]).toEqual([
      "equals",
      "deepEquals",
      "contains",
      "startsWith",
      "endsWith",
      "matches",
      "exists",
    ]);
    expect([...PREDICATE_FIELDS]).toEqual(["method", "path", "query", "headers", "body"]);
  });

  it("keys a query or header entry under its own name", () => {
    const json = renderPredicates([
      {
        kind: "clause",
        clause: {
          operator: "equals",
          entries: [{ field: "headers", key: "Authorization", value: "Bearer x" }],
          caseSensitive: null,
          except: null,
          selector: null,
        },
      },
    ]);
    expect(json).toEqual([{ equals: { headers: { Authorization: "Bearer x" } } }]);
  });

  it("puts a selector beside the operator, not inside it", () => {
    // The sibling form (`{jsonpath: {...}, equals: {body: …}}`), which is the one the engine's
    // JSONPath examples use and the only one this builder claims.
    const json = renderPredicates([
      {
        kind: "clause",
        clause: {
          operator: "equals",
          entries: [{ field: "body", key: null, value: "admin" }],
          caseSensitive: null,
          except: null,
          selector: { kind: "jsonpath", expression: "$.user.name", ns: null },
        },
      },
    ]);
    expect(json).toEqual([{ jsonpath: { selector: "$.user.name" }, equals: { body: "admin" } }]);
  });

  it("carries xpath namespaces when there are any, and omits the key when there are none", () => {
    const withNs = renderPredicates([
      {
        kind: "clause",
        clause: {
          operator: "equals",
          entries: [{ field: "body", key: null, value: "99.99" }],
          caseSensitive: null,
          except: null,
          selector: {
            kind: "xpath",
            expression: "//ns:item/ns:price",
            ns: { ns: "http://example.com/schema" },
          },
        },
      },
    ]);
    expect(withNs).toEqual([
      {
        xpath: { selector: "//ns:item/ns:price", ns: { ns: "http://example.com/schema" } },
        equals: { body: "99.99" },
      },
    ]);
  });

  it("omits caseSensitive entirely when the toggle was never touched", () => {
    // `null` is not `false`. An absent key and an explicit `false` are different documents, and only
    // the first is what an untouched toggle means — the same distinction `render` makes for a null
    // form field.
    const untouched = renderPredicates([
      {
        kind: "clause",
        clause: {
          operator: "equals",
          entries: [{ field: "path", key: null, value: "/x" }],
          caseSensitive: null,
          except: null,
          selector: null,
        },
      },
    ]);
    expect(untouched).toEqual([{ equals: { path: "/x" } }]);

    const explicitlyFalse = renderPredicates([
      {
        kind: "clause",
        clause: {
          operator: "equals",
          entries: [{ field: "path", key: null, value: "/x" }],
          caseSensitive: false,
          except: null,
          selector: null,
        },
      },
    ]);
    expect(explicitlyFalse).toEqual([{ equals: { path: "/x" }, caseSensitive: false }]);
  });

  it("wraps a group as or-array or not-object, matching the engine's own shapes", () => {
    const clause = (path: string): PredicateClause => ({
      operator: "equals",
      entries: [{ field: "path", key: null, value: path }],
      caseSensitive: null,
      except: null,
      selector: null,
    });

    expect(
      renderPredicates([{ kind: "group", op: "or", clauses: [clause("/a"), clause("/b")] }]),
    ).toEqual([{ or: [{ equals: { path: "/a" } }, { equals: { path: "/b" } }] }]);

    // `not` takes a single predicate object, not an array — the engine's own doc is explicit.
    expect(renderPredicates([{ kind: "group", op: "not", clauses: [clause("/a")] }])).toEqual([
      { not: { equals: { path: "/a" } } },
    ]);
  });
});

describe("what the builder refuses to model, it names", () => {
  it("keeps reading the two-field equals this console has always written", () => {
    /*
     * The compatibility case that dictates the whole model. `STUB_FIELDS` wrote
     * `{"equals":{"method":…,"path":…}}` — one object, two fields — so every stub saved through this
     * form until now looks like that. A one-entry-per-object model would either refuse them all or
     * silently rewrite them into two objects; a clause with two entries reads *and* re-renders them
     * unchanged.
     */
    const original = [{ equals: { method: "GET", path: "/orders" } }];
    const projected = projectPredicates({ predicates: original });

    expect(projected.kind).toBe("predicates");
    if (projected.kind !== "predicates") return;
    expect(JSON.stringify(renderPredicates(projected.items))).toBe(JSON.stringify(original));
  });

  it("refuses a predicate object carrying two operators", () => {
    const projected = projectPredicates({
      predicates: [{ equals: { path: "/a" }, contains: { body: "x" } }],
    });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys.join(" ")).toMatch(/predicates\[0\]/);
  });

  it("refuses nesting deeper than one level, naming where it gave up", () => {
    // A group inside a group. Half-rendering it would drop the inner one on save, which is the
    // failure this whole module is built to prevent.
    const projected = projectPredicates({
      predicates: [{ or: [{ equals: { path: "/a" } }, { and: [{ equals: { path: "/b" } }] }] }],
    });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys.join(" ")).toMatch(/predicates\[0\]/);
  });

  it("refuses an operator it does not know", () => {
    const projected = projectPredicates({ predicates: [{ soundsLike: { path: "/a" } }] });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys.join(" ")).toMatch(/soundsLike|predicates\[0\]/);
  });

  it("refuses a field it does not know", () => {
    const projected = projectPredicates({ predicates: [{ equals: { cookies: "a=1" } }] });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys.join(" ")).toMatch(/cookies|predicates\[0\]/);
  });

  it("refuses the nested-xpath variant, which is ambiguous against the sibling form", () => {
    // `{"xpath": {"selector": …, "equals": "admin"}}` puts the operator *inside* the selector
    // object, unlike the jsonpath examples. Rather than guess which reading was meant, this is
    // raw-only — the operator's own JSON stays authoritative.
    const projected = projectPredicates({
      predicates: [{ xpath: { selector: "//user/name", equals: "admin" } }],
    });
    expect(projected.kind).toBe("rawOnly");
  });

  it("refuses a top-level explicit and, while accepting the one inside a not group", () => {
    /*
     * These two look alike and are not. A top-level `and` is refused because the builder's own list
     * *is* the implicit `and` — reading one in would render back a different document. But `not`
     * takes a single predicate object, so a `not` over several clauses has nowhere to put them
     * except an `and`, and `{not: {and: […]}}` is the engine's own shape for it.
     *
     * Pinned because the distinction is easy to collapse in either direction, and collapsing it
     * silently changes what a saved stub matches.
     */
    expect(projectPredicates({ predicates: [{ and: [{ equals: { path: "/a" } }] }] }).kind).toBe(
      "rawOnly",
    );

    const clause = (path: string): PredicateClause => ({
      operator: "equals",
      entries: [{ field: "path", key: null, value: path }],
      caseSensitive: null,
      except: null,
      selector: null,
    });
    const json = renderPredicates([
      { kind: "group", op: "not", clauses: [clause("/a"), clause("/b")] },
    ]);
    expect(json).toEqual([
      { not: { and: [{ equals: { path: "/a" } }, { equals: { path: "/b" } }] } },
    ]);

    const back = projectPredicates({ predicates: json });
    expect(back.kind).toBe("predicates");
    if (back.kind !== "predicates") return;
    expect(JSON.stringify(renderPredicates(back.items))).toBe(JSON.stringify(json));
  });

  it("keeps a JSON-object body opaque rather than decomposing it into per-key entries", () => {
    // `equals: { body: { username: "admin" } }` is a body match against an object, not a match on a
    // field called `username`. Decomposing it would be a category error that happens to round-trip,
    // and would then render a *different* meaning the moment the operator edited the row.
    const original = [{ equals: { body: { username: "admin", role: "ops" } } }];
    const projected = projectPredicates({ predicates: original });

    expect(projected.kind).toBe("predicates");
    if (projected.kind !== "predicates") return;
    const [item] = projected.items;
    expect(item?.kind).toBe("clause");
    if (item?.kind !== "clause") return;
    expect(item.clause.entries).toEqual([
      { field: "body", key: null, value: { username: "admin", role: "ops" } },
    ]);
    expect(JSON.stringify(renderPredicates(projected.items))).toBe(JSON.stringify(original));
  });

  it("treats an absent predicates key as an empty set, not as a refusal", () => {
    const projected = projectPredicates({ responses: [] });
    expect(projected.kind).toBe("predicates");
    if (projected.kind !== "predicates") return;
    expect(projected.items).toEqual([]);
  });
});
