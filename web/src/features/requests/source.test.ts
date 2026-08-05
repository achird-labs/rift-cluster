import { describe, expect, it } from "vitest";

import { coverageFor, describeCoverage, page, readLog } from "./source.ts";

describe("coverageFor", () => {
  // Coverage now comes straight off the merge's own `Rift-Cluster-Partial` bit (#147 H), not from
  // this node's view of fleet topology — so the input is the bit itself, and the mapping is total.
  it("reports a complete merge as fleet coverage", () => {
    expect(coverageFor(false)).toEqual({ kind: "fleet" });
  });

  it("reports a merge that could not reach every node as partial coverage", () => {
    expect(coverageFor(true)).toEqual({ kind: "partial" });
  });
});

describe("describeCoverage", () => {
  // The only case left to describe — a complete merge renders no label at all, so there is nothing
  // for this function to say about it.
  it("says the merge could not reach every node, not that one node was reached", () => {
    const text = describeCoverage().toLowerCase();
    expect(text).toMatch(/could not reach|may be missing/);
    expect(text).not.toContain("one node");
    expect(text).not.toContain("whole fleet");
  });
});

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
