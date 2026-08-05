import type { components } from "../../api/schema.ts";

/**
 * The request log's data source — **the landing point for #147 H**.
 *
 * The screen renders only through this module. It now reads the fleet's merged journal
 * (`GET /imposters/:port/requests`, admin-front-side fan-out per #147 B/D) rather than one node's
 * own, so `Coverage` names what the *response* says about that merge rather than what this node's
 * view of fleet topology implies — `coverageFor` takes the `Rift-Cluster-Partial` bit straight off
 * the read, and `useRequestLog` (`app/queries.ts`) is the only caller.
 */

/**
 * How much of the fleet's traffic the rows on screen actually represent.
 *
 * A two-valued type on purpose: a merge either reached every node in its budget or it did not, and
 * the server says which via one additive-only header. There is no third "could not be determined"
 * case here the way there was for the old topology-derived coverage — a read that fails outright is
 * `LogState`'s `unknown`, not a `Coverage` at all, so the ambiguity that used to live in this type
 * cannot arise.
 */
export type Coverage = { kind: "fleet" } | { kind: "partial" };

export type Cursor = { offset: number; size: number };

export type Page<T> = { rows: T[]; total: number; hasMore: boolean };

/** What the merge's `Rift-Cluster-Partial` bit says about this read's coverage. */
export function coverageFor(partial: boolean): Coverage {
  return partial ? { kind: "partial" } : { kind: "fleet" };
}

/**
 * The sentence the scope label shows. Only ever called for `{ kind: "partial" }` — `RequestLog.tsx`
 * renders the label at all only in that case, so there is no complete-merge sentence to branch to:
 * a complete merge says nothing here, on the theory that a permanent label with nothing wrong to
 * report trains operators to stop reading it before the day it matters.
 */
export function describeCoverage(): string {
  return (
    "This merge could not reach every node in its budget, so it may be missing entries a slower " +
    "node was still holding. If a call looks missing, check again — the next poll re-merges and " +
    "often catches up."
  );
}

/**
 * One page of rows, for the table's own pager.
 *
 * This is a *display* page, over whatever the screen has already accumulated in memory — a separate
 * concern from the network-level `?since=` cursor `useRequestLog` sends the merge (the one that
 * keeps a single poll from re-fetching the whole journal). That one bounds the request; this one
 * bounds the DOM on a busy imposter by slicing client-side, which is unaffected by where the array
 * it slices came from.
 */
export function page<T>(rows: readonly T[], cursor: Cursor): Page<T> {
  const start = Math.max(0, cursor.offset);
  const end = start + cursor.size;
  return {
    rows: rows.slice(start, end),
    total: rows.length,
    hasMore: end < rows.length,
  };
}

/**
 * One recorded request as the engine serves it
 * (`rift-mock-core/src/imposter/types.rs::RecordedRequest`).
 *
 * Derived from the contract rather than hand-written, so the field list has one source. The
 * bare-string-vs-array shape of a header value and the `_mode` spelling are documented on the
 * schema itself, which is now the one place they are stated.
 *
 * What this does **not** buy is compiler rejection of an invented field. The schema is
 * `additionalProperties: true` (the engine's shape is non-exhaustive and #208 will add to it), so
 * the generated type ends in `& { [key: string]: unknown }` and that index signature survives
 * `Partial` — `request.invented` still compiles, exactly as `contract-traceability.test.ts` already
 * observes of `Imposter.numberOfRequests`. The field list is pinned by the `declares(...)` test in
 * that file, not by `tsc`. Worth stating plainly, because the hand-written type this replaced was a
 * closed literal and *did* reject invented reads: on that one axis the derivation is looser, and
 * the traceability test is what pays for it.
 *
 * `Partial` is the deliberate part, and it is not a hedge against the schema being wrong — the six
 * required fields really are always emitted. It is that the type is an *assertion*, not a
 * validation: `apiGet` does not check the body against the schema, this is attacker-influenced data,
 * and a node running an older engine can answer with a field missing. Absent must render as absent
 * rather than crash the screen an operator opened to diagnose something else.
 */
export type RecordedRequest = Partial<components["schemas"]["RecordedRequest"]>;

/**
 * One header's values, whatever shape the wire used.
 *
 * Written defensively on purpose: this is an untyped endpoint carrying attacker-influenced data,
 * and a header that is neither a string nor an array must render as text rather than throw — a
 * crash here takes down the screen an operator opened to diagnose something else.
 */
export function headerValues(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(asText);
  return [asText(value)];
}

/**
 * `String(x)` is not total: an object whose `toString` is not callable throws
 * `Cannot convert object to primitive value`. The engine cannot produce that shape from
 * `HashMap<String, Vec<String>>`, but this function's contract is that it does not throw, and a
 * contract that holds only for well-behaved input is the kind that fails on the one request an
 * operator most needs to read.
 */
function asText(value: unknown): string {
  try {
    return String(value);
  } catch {
    return JSON.stringify(value) ?? "(unreadable value)";
  }
}

/**
 * The merge's answer, as a value.
 *
 * `unknown` is not `empty`, and this type is where that distinction is made unrepresentable-as-one:
 * a merge that could not answer has an unknown journal, and rendering it as an empty table tells an
 * operator their system under test never called the mock.
 */
export type LogState =
  | { kind: "rows"; rows: RecordedRequest[] }
  | { kind: "unknown"; reason: string };

/** The endpoint returns a bare array; anything else is a shape this screen will not invent rows from. */
export function readLog(body: unknown): LogState {
  if (Array.isArray(body)) return { kind: "rows", rows: body as RecordedRequest[] };
  return {
    kind: "unknown",
    reason: "the merge answered with a body that is not a request list",
  };
}
