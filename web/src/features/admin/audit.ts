import type { components } from "../../api/schema.ts";

/**
 * One row of `GET /admin/audit`. `outcome` is `ControlOutcome`, externally tagged and
 * **snake_case** — `Applied` is a unit variant, so the wire carries the bare string `"applied"`,
 * never `{"Applied": null}`. A refusal is a *committed* outcome (`{"failed": {"reason": "..."}}`):
 * the op is in the log and deduped like any other, it just changed nothing.
 */
export type AuditRow = components["schemas"]["AuditRow"];

export function isApplied(outcome: AuditRow["outcome"]): boolean {
  return outcome === "applied";
}

/**
 * `null` for an applied op; the refusal's reason otherwise.
 *
 * Total by construction. Reaching into `outcome.failed.reason` would throw a `TypeError` on any
 * shape this console does not yet know — a variant added upstream before the schema catches up, say
 * — and with no error boundary in the tree that throw blanks the **entire** admin screen for every
 * operator. Losing the audit trail is worst precisely during the incident it exists to explain, so
 * an unreadable outcome degrades to a named string and stays a row.
 */
export function failureReason(outcome: AuditRow["outcome"]): string | null {
  if (outcome === "applied") return null;
  const reason: unknown = (outcome as { failed?: { reason?: unknown } })?.failed?.reason;
  return typeof reason === "string" ? reason : "unrecognised outcome";
}

/**
 * The endpoint answers a bare array, no envelope. Anything else — an envelope, `null`, a string —
 * is unreadable, and must throw rather than render as an empty trail: a silent zero-row page would
 * tell an auditor nothing happened when the truth is the response could not be read at all.
 */
export function readAuditRows(body: unknown): AuditRow[] {
  if (!Array.isArray(body)) {
    throw new Error("GET /admin/audit answered with a body that is not the bare array it always serves");
  }
  return body as AuditRow[];
}

/** Ascending by `(revision, opId)` — oldest first, the fleet's own total order over the journal. */
export function auditPage(rows: readonly AuditRow[]): AuditRow[] {
  return [...rows].sort((a, b) => a.revision - b.revision || a.opId.localeCompare(b.opId));
}

/**
 * The next `since` cursor, or `null` for an empty page.
 *
 * **`since` is inclusive server-side** — `store.rs` scans `range((since, "")..)`, documented as
 * "rows at or after `since`" — so the cursor is one past the highest revision seen. Passing the
 * highest revision itself re-fetches the row the page ended on, which on a final page means the
 * table collapses to a single already-seen row and the pager never retires.
 */
export function nextSince(rows: readonly AuditRow[]): number | null {
  if (rows.length === 0) return null;
  return rows.reduce((max, row) => Math.max(max, row.revision), Number.NEGATIVE_INFINITY) + 1;
}

/**
 * Is there another page to ask for?
 *
 * A page shorter than the limit is the end of the journal. Without this the "next" control stays
 * live forever, because a cursor exists for any non-empty page.
 */
export function hasMorePages(rows: readonly AuditRow[], limit: number): boolean {
  return rows.length >= limit;
}
