import { describe, expect, it } from "vitest";

import type { AuditRow } from "./audit.ts";
import {
  auditPage,
  failureReason,
  hasMorePages,
  isApplied,
  nextSince,
  readAuditRows,
} from "./audit.ts";

function row(revision: number, overrides: Partial<AuditRow> = {}): AuditRow {
  return {
    tsSecs: 1_700_000_000 + revision,
    principal: "p-1",
    tenant: "acme",
    action: "imposter.write",
    resource: "4545",
    opId: `op-${revision}`,
    revision,
    outcome: "applied",
    ...overrides,
  };
}

describe("outcome — the wire is snake_case and `applied` is a bare string", () => {
  // `ControlOutcome` is `#[serde(rename_all = "snake_case")]` with a unit `Applied` variant, so the
  // wire carries the string "applied". The contract used to document `{"Applied": null}`; code that
  // believed it would silently classify every row as a failure.
  it("reads the bare string form", () => {
    expect(isApplied("applied")).toBe(true);
    expect(failureReason("applied")).toBeNull();
  });

  it("reads a refusal and surfaces its reason", () => {
    const outcome = { failed: { reason: "quota exceeded" } };
    expect(isApplied(outcome)).toBe(false);
    expect(failureReason(outcome)).toBe("quota exceeded");
  });

  // A refusal is a *committed* outcome — in the log, deduped like any other, it just changed
  // nothing. It must render as a row, not be filtered out of the audit trail.
  it("keeps refusals in the rows rather than dropping them", () => {
    const rows = readAuditRows([
      row(1),
      row(2, { outcome: { failed: { reason: "denied" } } }),
    ]);
    expect(rows).toHaveLength(2);
  });

  // The capitalised spellings are what the old contract described. Treating them as applied would
  // reinstate exactly the bug this module exists to prevent.
  it("does not accept the capitalised shapes the contract wrongly documented", () => {
    expect(isApplied("Applied" as unknown as AuditRow["outcome"])).toBe(false);
    expect(isApplied({ Applied: null } as unknown as AuditRow["outcome"])).toBe(false);
  });

  // An unreadable outcome must degrade to a named string, never throw: there is no error boundary
  // in this tree, so a throw here blanks the whole admin screen — and the audit trail is least
  // dispensable during the incident that would produce an unfamiliar variant.
  it("names an unrecognised outcome instead of throwing", () => {
    for (const shape of [undefined, null, {}, { failed: {} }, { other: 1 }, "Applied"]) {
      const outcome = shape as unknown as AuditRow["outcome"];
      expect(isApplied(outcome)).toBe(false);
      expect(() => failureReason(outcome)).not.toThrow();
      expect(failureReason(outcome)).toBe("unrecognised outcome");
    }
  });
});

describe("readAuditRows", () => {
  it("reads the bare array the endpoint serves", () => {
    expect(readAuditRows([row(1)])).toHaveLength(1);
  });

  // The endpoint has no envelope. If it ever grows one, rendering zero rows silently would tell an
  // auditor nothing happened.
  it("treats an enveloped or non-array body as unreadable, never as an empty trail", () => {
    for (const body of [{ rows: [row(1)] }, {}, null, "rows"]) {
      expect(() => readAuditRows(body)).toThrow();
    }
  });
});

describe("pagination over since/limit", () => {
  // `since` is INCLUSIVE server-side (`range((since, "")..)` — "rows at or after since"), so the
  // cursor must be one past the highest revision seen. Using the revision itself re-serves the row
  // the page ended on, and on a final page the table collapses to that one already-seen row.
  it("takes the next cursor one past the highest revision, because since is inclusive", () => {
    expect(nextSince([row(7), row(11), row(9)])).toBe(12);
  });

  it("has no next cursor for an empty page", () => {
    expect(nextSince([])).toBeNull();
  });

  it("retires the pager once a page comes back shorter than the limit", () => {
    expect(hasMorePages([row(1), row(2)], 100)).toBe(false);
    expect(hasMorePages([row(1), row(2)], 2)).toBe(true);
    expect(hasMorePages([], 100)).toBe(false);
  });

  it("orders rows by revision, so the display order is the fleet's total order", () => {
    const ordered = auditPage([row(3), row(1), row(2)]);
    expect(ordered.map((r) => r.revision)).toEqual([1, 2, 3]);
  });

  it("breaks a revision tie on opId, so the order is total", () => {
    const ordered = auditPage([
      row(5, { opId: "op-b" }),
      row(5, { opId: "op-a" }),
    ]);
    expect(ordered.map((r) => r.opId)).toEqual(["op-a", "op-b"]);
  });
});
