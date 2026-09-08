import { describe, expect, it } from "vitest";

import { page, readLog } from "./source.ts";

describe("readLog", () => {
  it("reads the bare array the endpoint serves", () => {
    expect(readLog([{ method: "GET" }])).toEqual({ kind: "rows", rows: [{ method: "GET" }] });
  });

  it("reads an empty array as an answered-but-empty log", () => {
    expect(readLog([])).toEqual({ kind: "rows", rows: [] });
  });

  // A 200 carrying something that is not a request list is a broken contract. Folding it into an
  // empty log would tell an operator their system under test never called the mock.
  it("treats a body that is not a request list as unknown, never as empty", () => {
    for (const body of [{}, null, "rows", 7]) {
      expect(readLog(body).kind).toBe("unknown");
    }
  });
});

describe("page", () => {
  const rows = Array.from({ length: 2500 }, (_, i) => i);

  it("returns only the requested page", () => {
    const first = page(rows, { offset: 0, size: 50 });
    expect(first.rows).toHaveLength(50);
    expect(first.rows[0]).toBe(0);
    expect(first.total).toBe(2500);
    expect(first.hasMore).toBe(true);
  });

  it("stops at the end rather than over-reading", () => {
    const last = page(rows, { offset: 2480, size: 50 });
    expect(last.rows).toHaveLength(20);
    expect(last.hasMore).toBe(false);
  });

  it("reports an empty source as empty with no more pages", () => {
    expect(page([], { offset: 0, size: 50 })).toEqual({ rows: [], total: 0, hasMore: false });
  });
});
