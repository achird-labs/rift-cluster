import { describe, expect, it } from "vitest";

import { coverageFor, describeCoverage, page, readLog } from "./source.ts";

const A = "3342140982834931156";
const B = "3481475601826307430";
const C = "17445687154000629855";

const VIEW = {
  nodeId: C,
  isLeader: false,
  leader: A,
  lastApplied: 0,
  voters: [A, B, C],
  ringEpoch: 0,
  ringMembers: [A, B, C],
  ready: true,
  state: "ready" as const,
  pendingGates: [],
  isolated: false,
  singleNode: false,
  degraded: [],
};

describe("coverageFor", () => {
  it("names the node and counts the nodes it does not represent", () => {
    expect(coverageFor({ kind: "read", view: VIEW })).toEqual({
      kind: "per-node",
      node: C,
      unrepresented: 2,
    });
  });

  // A one-node fleet has nothing to be partial about; labelling it degraded would train operators
  // to ignore the label on the fleets where it matters.
  it("reports a single-node fleet as the whole fleet, not as a partial view", () => {
    const single = { ...VIEW, voters: [C], ringMembers: [C], singleNode: true };
    expect(coverageFor({ kind: "read", view: single })).toEqual({ kind: "fleet" });
  });

  // `null` is "could not be determined", never zero. Reporting 0 unrepresented nodes to a principal
  // who simply cannot read the fleet projection would be an assertion nothing supports.
  // Reaching this with an empty ring means the members/health reads disagree, or the node has no
  // applied membership at all. Either way "0 other nodes are not represented" asserts complete
  // coverage from a node that has none.
  it("reports a multi-voter node whose ring shows no peers as unknown, not as zero others", () => {
    const skewed = { ...VIEW, voters: [A, B], ringMembers: [C], singleNode: false };
    expect(coverageFor({ kind: "read", view: skewed })).toEqual({
      kind: "per-node",
      node: C,
      unrepresented: null,
    });
  });

  it("reports an unread fleet as unknown, not as zero others", () => {
    expect(coverageFor({ kind: "not-asked" })).toEqual({
      kind: "per-node",
      node: null,
      unrepresented: null,
    });
    expect(coverageFor({ kind: "unavailable" })).toEqual({
      kind: "per-node",
      node: null,
      unrepresented: null,
    });
  });
});

describe("describeCoverage", () => {
  it("names the node and the count of nodes not represented", () => {
    const text = describeCoverage({ kind: "per-node", node: C, unrepresented: 2 });
    // The label truncates a 20-digit id, so it carries the head rather than the whole value — what
    // matters is that it names THIS node and not some rounded neighbour.
    expect(text).toContain(C.slice(0, 10));
    expect(text).toContain("2");
  });

  it("says plainly that a single-node fleet is the whole fleet", () => {
    expect(describeCoverage({ kind: "fleet" }).toLowerCase()).toContain("whole fleet");
  });

  it("admits when it cannot identify the node rather than implying completeness", () => {
    const text = describeCoverage({ kind: "per-node", node: null, unrepresented: null });
    expect(text.toLowerCase()).toContain("one node");
    expect(text.toLowerCase()).not.toContain("whole fleet");
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
