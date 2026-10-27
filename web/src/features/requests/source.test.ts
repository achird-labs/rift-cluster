import { describe, expect, it } from "vitest";

import { formatQuery, missingBodyNote, page, readLog } from "./source.ts";

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

describe("formatQuery", () => {
  // rift#1553: a repeated key is an array of its values. Printing it as `k=a,b` would read as the
  // old comma-joined value the engine no longer means.
  it("prints one name=value line per value, a repeated key on several lines", () => {
    expect(formatQuery({ k: ["a", "b"], one: "1" })).toBe("k=a\nk=b\none=1");
  });

  it("says (none) for an empty or missing query", () => {
    expect(formatQuery({})).toBe("(none)");
    expect(formatQuery(undefined)).toBe("(none)");
  });
});

describe("missingBodyNote", () => {
  // rift#1584: the only way `body` is absent is a declared gzip/br body that did not decode.
  it("names the encoding that did not decode, reading the header case-insensitively", () => {
    expect(missingBodyNote({ "Content-Encoding": "br" })).toBe(
      "Body not recorded: it did not decode as br",
    );
    expect(missingBodyNote({ "content-encoding": ["gzip"] })).toBe(
      "Body not recorded: it did not decode as gzip",
    );
  });

  it("falls back to a neutral note when no encoding was declared (an older journal)", () => {
    expect(missingBodyNote({ accept: "*/*" })).toBe("No body recorded");
    expect(missingBodyNote(undefined)).toBe("No body recorded");
  });
});
