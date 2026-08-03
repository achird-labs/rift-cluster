import { describe, expect, it } from "vitest";

import { DEFAULT_GENERATOR_FIELDS } from "../recording/state.ts";
import { projectPredicates } from "../stubs/predicates.ts";
import { projectResponses } from "../stubs/responses.ts";
import type { RecordedRequest } from "./source.ts";
import {
  type FieldSelection,
  defaultSelection,
  hasCatchAll,
  rowActionFor,
  stubFromRequest,
} from "./stubFromRequest.ts";

const GET: RecordedRequest = {
  requestFrom: "127.0.0.1:5000",
  method: "GET",
  path: "/users/42",
  query: { page: "2", sort: "name" },
  headers: { Accept: "application/json", "X-Trace": ["a", "b"] },
  timestamp: "2026-08-03T00:00:00Z",
};

const POST: RecordedRequest = {
  ...GET,
  method: "POST",
  path: "/orders",
  query: {},
  body: '{"id":1,"items":["a"]}',
};

const RESPONSE = [
  { is: { statusCode: 200, headers: { "Content-Type": "application/json" }, body: "{}" } },
];

function withSelection(over: Partial<FieldSelection>): FieldSelection {
  return { ...defaultSelection(), ...over };
}

describe("the default selection agrees with the recording flow's", () => {
  it("is the SAME set of fields the recording panel defaults to", () => {
    /*
     * Asserted against the other module, not against a literal.
     *
     * The previous version of this test compared `defaultSelection()` to a hard-coded tuple and
     * carried a comment claiming the two flows agree — so it passed while they disagreed (recording
     * had `query` on, this had it off). A test that restates the value it is checking cannot catch
     * drift; only one that reads BOTH sides can.
     */
    const selection = defaultSelection();
    const selected = new Set(
      ["method", "path", "query", "body"].filter(
        (field) => selection[field as "method" | "path" | "query" | "body"],
      ),
    );
    expect(selected).toEqual(new Set(DEFAULT_GENERATOR_FIELDS));
    // Headers are opt-in one at a time and are not part of the shared default set.
    expect(selection.headers.size).toBe(0);
  });

  it("derives a stub matching the method, path and query", () => {
    expect(stubFromRequest(GET, defaultSelection())).toEqual({
      predicates: [
        { equals: { method: "GET", path: "/users/42", query: { page: "2", sort: "name" } } },
      ],
      responses: RESPONSE,
    });
  });
});

describe("the response is a default, because the journal has no response to reconstruct", () => {
  it("always seeds the same 200, whatever the request was", () => {
    // `RecordedRequest` carries no response field at all. Anything else here would be invented.
    for (const request of [GET, POST]) {
      const stub = stubFromRequest(request, defaultSelection()) as { responses: unknown };
      expect(stub.responses).toEqual(RESPONSE);
    }
  });
});

describe("opt-in fields", () => {
  it("takes the whole recorded query map when query is selected", () => {
    const stub = stubFromRequest(GET, withSelection({ query: true })) as {
      predicates: { equals: Record<string, unknown> }[];
    };
    expect(stub.predicates[0]?.equals.query).toEqual({ page: "2", sort: "name" });
  });

  it("omits an empty query rather than predicating on `{}`", () => {
    // `query: {}` as a predicate means "has no query parameters at all", which is a stronger claim
    // than the request supports — the recorded map is empty for every request without a query.
    const stub = stubFromRequest(POST, withSelection({ query: true })) as {
      predicates: { equals: Record<string, unknown> }[];
    };
    expect(stub.predicates[0]?.equals.query).toBeUndefined();
  });

  it("takes only the headers that were opted in, one at a time", () => {
    const stub = stubFromRequest(GET, withSelection({ headers: new Set(["Accept"]) })) as {
      predicates: { equals: Record<string, unknown> }[];
    };
    expect(stub.predicates[0]?.equals.headers).toEqual({ Accept: "application/json" });
  });

  it("predicates on the single value the MATCHER sees for a multi-valued header", () => {
    /*
     * The `Vec<String>` shape belongs to the journal, not the matcher. `header_map_to_hashmap`
     * collects hyper's per-value iterator into a `HashMap<String, String>`, so a header sent twice
     * collapses and the LAST value wins — `handler.rs` says so outright ("stays the single-value
     * view used for matching/context").
     *
     * So an array-valued expectation is routed to `compare_json_recursive`, which tries to parse
     * that one header string as JSON and returns false. A `["a","b"]` predicate could never match
     * the very request it was derived from — the one field this feature exists to get right would
     * be the one that silently cannot fire.
     */
    const stub = stubFromRequest(GET, withSelection({ headers: new Set(["X-Trace"]) })) as {
      predicates: { equals: Record<string, unknown> }[];
    };
    expect(stub.predicates[0]?.equals.headers).toEqual({ "X-Trace": "b" });
  });
});

describe("bodies", () => {
  it("predicates on the PARSED value for a JSON body, using equals rather than deepEquals", () => {
    /*
     * `equals` already deep-compares a parsed JSON body (`compare_json_recursive` is reached for any
     * object expectation). The only thing `deepEquals` adds is exact-key matching, which is too
     * strict for a stub derived from one observed request: the next call carrying an extra field is
     * the same call as far as the developer is concerned.
     */
    const stub = stubFromRequest(POST, withSelection({ body: true })) as {
      predicates: { equals: Record<string, unknown> }[];
    };
    expect(stub.predicates[0]?.equals.body).toEqual({ id: 1, items: ["a"] });
    expect(Object.keys(stub.predicates[0] ?? {})).toEqual(["equals"]);
  });

  it("predicates on the raw string when the body is not JSON", () => {
    const request: RecordedRequest = { ...POST, body: "name=ada&role=admin" };
    const stub = stubFromRequest(request, withSelection({ body: true })) as {
      predicates: { equals: Record<string, unknown> }[];
    };
    expect(stub.predicates[0]?.equals.body).toBe("name=ada&role=admin");
  });

  it("treats a binary body as opaque bytes, never parsing the base64 as JSON", () => {
    /*
     * `_mode: "binary"` means `body` is base64. `"MTIz"` decodes to "123" but parses AS base64 text
     * into nothing meaningful — and some base64 strings are themselves valid JSON (`"null"`,
     * `"123"`). Parsing one would silently predicate on a value the request never contained.
     */
    const request: RecordedRequest = { ...POST, body: "123", _mode: "binary" };
    const stub = stubFromRequest(request, withSelection({ body: true })) as {
      predicates: { equals: Record<string, unknown> }[];
    };
    expect(stub.predicates[0]?.equals.body).toBe("123");
  });

  it("omits the body predicate entirely when the request had no body", () => {
    const stub = stubFromRequest(GET, withSelection({ body: true })) as {
      predicates: { equals: Record<string, unknown> }[];
    };
    expect(stub.predicates[0]?.equals.body).toBeUndefined();
  });
});

describe("what it derives is something the stub editor can actually open", () => {
  it("produces a document both projections accept, for every selection", () => {
    /*
     * The point of seeding the EXISTING editor rather than building a second one: the derived stub
     * has to survive the same projections every other stub goes through, or the operator is handed
     * a document that immediately drops to raw-only. Worth asserting directly — the two modules
     * know nothing about this one.
     */
    const selections: FieldSelection[] = [
      defaultSelection(),
      withSelection({ query: true }),
      withSelection({ body: true }),
      withSelection({ headers: new Set(["Accept"]) }),
      withSelection({ method: false, path: false, query: false }),
    ];
    for (const selection of selections) {
      for (const request of [GET, POST]) {
        const stub = stubFromRequest(request, selection);
        expect([selection, projectPredicates(stub).kind]).toEqual([selection, "predicates"]);
        expect([selection, projectResponses(stub).kind]).toEqual([selection, "responses"]);
      }
    }
  });

  it("opens a multi-valued header in the form too, now that it derives a single value", () => {
    // Taking the value the matcher sees fixes a second problem for free: `predicates.ts` refuses an
    // array-valued header predicate, so the array form would also have dropped the stub to raw-only.
    const stub = stubFromRequest(GET, withSelection({ headers: new Set(["X-Trace"]) }));
    expect(projectPredicates(stub).kind).toBe("predicates");
    expect(projectResponses(stub).kind).toBe("responses");
  });

  it("keeps a scalar JSON body as its raw text, since the engine compares those as strings", () => {
    /*
     * `check_string_field` routes to `compare_json_recursive` only for objects and arrays; every
     * other parsed value is compared as a STRING against the raw body. Re-serializing would break
     * exactly the cases where JSON's text and its value differ.
     */
    for (const [body, expected] of [
      ['"hello"', '"hello"'],
      ["1.0", "1.0"],
      ["1e3", "1e3"],
      ["  12  ", "  12  "],
      ["null", "null"],
    ] as const) {
      const stub = stubFromRequest({ ...POST, body }, withSelection({ body: true })) as {
        predicates: { equals: Record<string, unknown> }[];
      };
      expect([body, stub.predicates[0]?.equals.body]).toEqual([body, expected]);
    }
  });

  it("emits no predicates key at all when nothing is selected", () => {
    // Not `predicates: []` — that is a different document, and the editor's own convention is that
    // an empty selection emits no key.
    const stub = stubFromRequest(GET, withSelection({ method: false, path: false, query: false }));
    expect(stub).toEqual({ responses: RESPONSE });
  });
});

describe("the shadowing warning", () => {
  it("spots a stub with no predicates, which answers everything", () => {
    // Stubs append and matching is first-match-wins, so anything added below this never fires.
    expect(hasCatchAll([{ responses: RESPONSE }])).toBe(true);
    expect(hasCatchAll([{ predicates: [], responses: RESPONSE }])).toBe(true);
  });

  it("does not cry wolf over a stub that does have predicates", () => {
    expect(hasCatchAll([{ predicates: [{ equals: { path: "/a" } }], responses: RESPONSE }])).toBe(false);
    expect(hasCatchAll([])).toBe(false);
  });

  it("ignores junk in the stub list rather than throwing on it", () => {
    // The list comes off the wire; a malformed entry must not take the request log down.
    expect(hasCatchAll([null, 42, "x", []])).toBe(false);
  });
});

describe("which action a row offers is decided by the match outcome", () => {
  it("offers to stub an unmatched request, and one with no outcome recorded at all", () => {
    expect(rowActionFor(undefined)).toEqual({ kind: "stub" });
    expect(rowActionFor({ matched: false })).toEqual({ kind: "stub" });
  });

  it("offers to open the stub that answered, when it declares an id", () => {
    // The useful verb on a matched row is not "make a new stub" — one already answered.
    expect(rowActionFor({ matched: true, stubId: "s-1" })).toEqual({ kind: "open", stubId: "s-1" });
  });

  it("treats a null outcome as absence rather than throwing on it", () => {
    // `diagnostics.ts` folds null into absence for a stated reason; reading `.matched` off it here
    // would throw and take down the screen an operator opened because something was already wrong.
    expect(rowActionFor(null)).toEqual({ kind: "stub" });
  });

  it("offers no action at all when the outcome cannot be read", () => {
    // The diagnostics panel above already says "unreadable". A confident action beside it would be
    // the same screen asserting two different things about one row.
    for (const outcome of ["broken", 42, [], { matched: "yes" }, {}]) {
      expect([outcome, rowActionFor(outcome)]).toEqual([
        outcome,
        { kind: "none", reason: "unreadable" },
      ]);
    }
  });

  it("offers nothing when the winner has no id, rather than editing by index", () => {
    // By-index editing is the documented lost-update window; the console does not do it.
    expect(rowActionFor({ matched: true, stubIndex: 2 })).toEqual({
      kind: "none",
      reason: "matched-without-id",
    });
    expect(rowActionFor({ matched: true, stubId: "" })).toEqual({
      kind: "none",
      reason: "matched-without-id",
    });
  });
});
