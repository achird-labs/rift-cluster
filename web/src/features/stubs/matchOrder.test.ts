import { describe, expect, it } from "vitest";

import type { components } from "../../api/schema.ts";
import { matchOrder } from "./matchOrder.ts";

type Stub = components["schemas"]["Stub"];

// Stubs are written as the fleet returns them, so the loose shapes the contract does not name
// (`or`, `matches`) reach `matchOrder` exactly as they do in production.
const stub = (value: unknown): Stub => value as Stub;

describe("matchOrder", () => {
  it("is empty when the imposter has not loaded its stubs", () => {
    expect(matchOrder(undefined)).toEqual([]);
  });

  it("summarises an exact stub: method, path plus query, first answer", () => {
    const [entry] = matchOrder([
      stub({
        id: "orders",
        predicates: [
          { equals: { method: "post", path: "/orders", query: { dry: "true" } } },
        ],
        responses: [{ is: { statusCode: 201 } }, { is: { statusCode: 409 } }],
      }),
    ]);
    expect(entry).toEqual({
      index: 0,
      id: "orders",
      method: "POST",
      target: "/orders?dry=true",
      answer: "201",
      kind: "is",
      responses: 2,
      catchAll: false,
    });
  });

  it("keeps every stub in its real position, including one the form cannot model", () => {
    const entries = matchOrder([
      stub({ id: "a", responses: [{ is: { statusCode: 200 } }] }),
      // An `inject` predicate projects raw-only; the stub must still be listed, second.
      stub({ id: "b", predicates: [{ inject: "function () { return true; }" }], responses: [{ fault: "RESET" }] }),
      stub({ id: "c", responses: [{ proxy: { to: "http://up:8080" } }] }),
    ]);
    expect(entries.map((e) => [e.index, e.id, e.kind, e.answer])).toEqual([
      [0, "a", "is", "200"],
      [1, "b", "fault", "RESET"],
      [2, "c", "proxy", "http://up:8080"],
    ]);
    expect(entries[1]).toMatchObject({ method: null, target: null, catchAll: false });
  });

  it("marks a stub with no predicates as the catch-all it is, and names no request for it", () => {
    for (const predicates of [undefined, []]) {
      const [entry] = matchOrder([stub({ predicates, responses: [{ is: {} }] })]);
      expect(entry).toMatchObject({ catchAll: true, method: null, target: null });
    }
  });

  it("reads an empty id as no id", () => {
    const [entry] = matchOrder([stub({ id: "", responses: [] })]);
    expect(entry).toMatchObject({ id: null, answer: null, kind: null, responses: 0 });
  });

  /*
   * The documented invariant (`matchOrder.ts`): `sampleRequest` fills `GET` and `/` so it can build
   * a request to SEND, and here a default would be a claim about the stub. Each case below is a
   * stub that names a method or a path somewhere without pinning one.
   */
  describe("never shows a sampleRequest default as if the stub pinned it", () => {
    it("a path pinned only through a regex", () => {
      const [entry] = matchOrder([
        stub({ predicates: [{ matches: { method: "^(GET|HEAD)$", path: "^/orders/\\d+$" } }] }),
      ]);
      expect(entry).toMatchObject({ method: null, target: null });
    });

    it("a path pinned only inside an `or` group", () => {
      const [entry] = matchOrder([
        stub({
          predicates: [{ or: [{ equals: { path: "/a" } }, { equals: { path: "/b" } }] }],
        }),
      ]);
      expect(entry).toMatchObject({ method: null, target: null });
    });

    it("a query parameter or header that happens to be called `path` or `method`", () => {
      const [entry] = matchOrder([
        stub({
          predicates: [{ equals: { query: { path: "x" }, headers: { method: "y" } } }],
        }),
      ]);
      expect(entry).toMatchObject({ method: null, target: null });
    });

    it("a body whose text mentions them", () => {
      const [entry] = matchOrder([
        stub({ predicates: [{ equals: { body: '{"method":"PUT","path":"/z"}' } }] }),
      ]);
      expect(entry).toMatchObject({ method: null, target: null });
    });

    it("but a pinned method alongside an unpinned path still shows the method", () => {
      const [entry] = matchOrder([
        stub({ predicates: [{ equals: { method: "delete" } }, { contains: { path: "/o" } }] }),
      ]);
      expect(entry).toMatchObject({ method: "DELETE", target: null });
    });

    it("and a startsWith path is shown, because that path satisfies the predicate", () => {
      const [entry] = matchOrder([stub({ predicates: [{ startsWith: { path: "/v1/" } }] })]);
      expect(entry).toMatchObject({ method: null, target: "/v1/" });
    });
  });
});
