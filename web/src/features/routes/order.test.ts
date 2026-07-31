import { describe, expect, it } from "vitest";

import type { Route } from "./order.ts";
import { effectiveOrder, validateTable } from "./order.ts";

function route(id: string, overrides: Partial<Route> = {}): Route {
  return {
    id,
    priority: 0,
    match: {},
    target: { port: 4545, strip_prefix: false },
    enabled: true,
    ...overrides,
  };
}

/** Every ordering of `items`. Used to assert the sort is a total function of the routes alone. */
function permutations<T>(items: readonly T[]): T[][] {
  if (items.length <= 1) return [[...items]];
  return items.flatMap((item, index) =>
    permutations([...items.slice(0, index), ...items.slice(index + 1)]).map((rest) => [
      item,
      ...rest,
    ]),
  );
}

describe("effectiveOrder", () => {
  it("puts higher priority first", () => {
    const table = [route("a", { priority: 1 }), route("b", { priority: 9 })];
    expect(effectiveOrder(table).map((r) => r.id)).toEqual(["b", "a"]);
  });

  it("ranks host specificity exact > wildcard > absent", () => {
    const table = [
      route("none"),
      route("wild", { match: { host: "*.payments.test" } }),
      route("exact", { match: { host: "payments.test" } }),
    ];
    expect(effectiveOrder(table).map((r) => r.id)).toEqual(["exact", "wild", "none"]);
  });

  it("puts a longer path prefix first", () => {
    const table = [
      route("short", { match: { path_prefix: "/api" } }),
      route("long", { match: { path_prefix: "/api/v1/payments" } }),
    ];
    expect(effectiveOrder(table).map((r) => r.id)).toEqual(["long", "short"]);
  });

  it("puts more header clauses first", () => {
    const table = [
      route("one", { match: { headers: [{ name: "x-a", value: "1" }] } }),
      route("two", {
        match: {
          headers: [
            { name: "x-a", value: "1" },
            { name: "x-b", value: "2" },
          ],
        },
      }),
    ];
    expect(effectiveOrder(table).map((r) => r.id)).toEqual(["two", "one"]);
  });

  it("breaks the final tie on id, so no pair is ever left to arbitrary order", () => {
    expect(effectiveOrder([route("z"), route("a")]).map((r) => r.id)).toEqual(["a", "z"]);
  });

  // The whole reason the editor renders effective order rather than authoring order: a table that
  // resolved differently depending on the order it arrived in would make this screen a liar.
  it("is independent of input order", () => {
    const table = [
      route("a", { priority: 5, match: { host: "payments.test" } }),
      route("b", { priority: 5, match: { host: "*.payments.test" } }),
      route("c", { priority: 9 }),
      route("d", { match: { path_prefix: "/api/v1" } }),
      route("e", { match: { headers: [{ name: "x", value: "1" }] } }),
    ];
    const forward = effectiveOrder(table).map((r) => r.id);
    // Every permutation, not just the reversal: a non-transitive comparator can survive a single
    // reversed input and still resolve two other orderings differently.
    for (const permutation of permutations(table)) {
      expect(effectiveOrder(permutation).map((r) => r.id)).toEqual(forward);
    }
  });

  // Rust measures the prefix in UTF-8 bytes. `/é` is 3 bytes but only 2 UTF-16 units, so a naive
  // `.length` port ties it with `/ab` differently from the front door.
  it("measures path prefix length in UTF-8 bytes, as the server does", () => {
    const table = [
      route("b-ascii", { match: { path_prefix: "/ab" } }),
      route("a-accent", { match: { path_prefix: "/é" } }),
    ];
    // Both are 3 bytes, so the length term ties and the id tiebreak decides.
    expect(effectiveOrder(table).map((r) => r.id)).toEqual(["a-accent", "b-ascii"]);
  });

  // Rust's `String: Ord` is UTF-8 byte order; JS `<` is UTF-16 code-unit order, and the two
  // disagree above the BMP.
  it("breaks the id tie in UTF-8 byte order, as the server does", () => {
    const table = [route("\u{10000}"), route("�")];
    expect(effectiveOrder(table).map((r) => r.id)).toEqual(["�", "\u{10000}"]);
  });

  it("excludes disabled routes entirely — they are not dispatched, so they have no rank", () => {
    const table = [route("on"), route("off", { enabled: false, priority: 100 })];
    expect(effectiveOrder(table).map((r) => r.id)).toEqual(["on"]);
  });
});

describe("validateTable — mirrors the server's whole-table refusal", () => {
  it("accepts a clean table", () => {
    expect(validateTable([route("a"), route("b", { priority: 1 })])).toEqual([]);
  });

  it("rejects an empty id", () => {
    expect(validateTable([route("")])[0]?.kind).toBe("EmptyId");
  });

  it("rejects a duplicate id", () => {
    const errors = validateTable([route("dup"), route("dup", { priority: 1 })]);
    expect(errors[0]?.kind).toBe("DuplicateId");
    expect(errors[0]?.message).toContain("dup");
  });

  it("rejects strip_prefix with no path_prefix to strip", () => {
    const errors = validateTable([route("a", { target: { port: 1, strip_prefix: true } })]);
    expect(errors[0]?.kind).toBe("StripWithoutPrefix");
  });

  it("rejects a host with an interior wildcard", () => {
    expect(validateTable([route("a", { match: { host: "pay*.test" } })])[0]?.kind).toBe(
      "MalformedHost",
    );
  });

  it("rejects a bare '*.' host with nothing after it", () => {
    expect(validateTable([route("a", { match: { host: "*." } })])[0]?.kind).toBe("MalformedHost");
  });

  it("accepts one leading wildcard label", () => {
    expect(validateTable([route("a", { match: { host: "*.payments.test" } })])).toEqual([]);
  });

  it("rejects a path prefix that does not start with '/'", () => {
    expect(validateTable([route("a", { match: { path_prefix: "api" } })])[0]?.kind).toBe(
      "MalformedPathPrefix",
    );
  });

  it("rejects a method that is not a valid HTTP token", () => {
    expect(validateTable([route("a", { match: { method: "GET POST" } })])[0]?.kind).toBe(
      "MalformedMethod",
    );
    expect(validateTable([route("a", { match: { method: "" } })])[0]?.kind).toBe("MalformedMethod");
  });

  // `hyper::Method` accepts any valid token as an extension method, so an unfamiliar-but-well-formed
  // method is one the fleet would accept. A mirror stricter than the server blocks a legal table.
  it("accepts an extension method the server would accept", () => {
    expect(validateTable([route("a", { match: { method: "PURGE" } })])).toEqual([]);
  });

  it("rejects two enabled routes that match identically at the same priority", () => {
    const errors = validateTable([
      route("first", { match: { path_prefix: "/api" } }),
      route("second", { match: { path_prefix: "/api" } }),
    ]);
    expect(errors[0]?.kind).toBe("AmbiguousMatch");
    expect(errors[0]?.message).toContain("first");
    expect(errors[0]?.message).toContain("second");
  });

  // The server compares `headers: Vec<HeaderMatch>` with a derived `PartialEq`, which is
  // order-sensitive. Sorting before comparing made the mirror stricter than the fleet and refused a
  // table the fleet accepts.
  it("treats the same header clauses in a different order as different matches", () => {
    expect(
      validateTable([
        route("a", {
          match: {
            headers: [
              { name: "x-a", value: "1" },
              { name: "x-b", value: "2" },
            ],
          },
        }),
        route("b", {
          match: {
            headers: [
              { name: "x-b", value: "2" },
              { name: "x-a", value: "1" },
            ],
          },
        }),
      ]),
    ).toEqual([]);
  });

  // `${name}:${value}` collides for these two, so an encoding-based comparison called them equal.
  it("does not confuse a name/value pair that would collide when concatenated", () => {
    expect(
      validateTable([
        route("a", { match: { headers: [{ name: "x", value: "a:b" }] } }),
        route("b", { match: { headers: [{ name: "x:a", value: "b" }] } }),
      ]),
    ).toEqual([]);
  });

  // Staging a replacement route beside the one it will replace is a normal workflow, and the
  // server permits it; a client mirror that refused would block a table the fleet would accept.
  it("allows an identical pair when one is disabled", () => {
    expect(
      validateTable([
        route("live", { match: { path_prefix: "/api" } }),
        route("spare", { match: { path_prefix: "/api" }, enabled: false }),
      ]),
    ).toEqual([]);
  });

  it("allows an identical match at different priorities", () => {
    expect(
      validateTable([
        route("a", { match: { path_prefix: "/api" }, priority: 1 }),
        route("b", { match: { path_prefix: "/api" }, priority: 2 }),
      ]),
    ).toEqual([]);
  });
});
