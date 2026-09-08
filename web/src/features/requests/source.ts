import type { components } from "../../api/schema.ts";

/**
 * The request log's data source.
 *
 * The screen renders only through this module. It reads `GET /imposters/:port/requests` — upstream's
 * own journal route, proxied verbatim to the engine embedded in whichever node the browser reached
 * (**D-74**, #552). There is no coverage to describe any more: the fleet merge that used to sit
 * behind this route is gone, so a read either answers for that one node or it fails, and the only
 * two states left are `LogState`'s `rows` and `unknown`.
 *
 * `Coverage`/`coverageFor`/`describeCoverage` lived here to carry the merge's `Rift-Cluster-Partial`
 * bit onto the screen. That header no longer rides a requests read at all — it survives only on the
 * reads that genuinely fan out (`/_fleet/*` and the spaces listing) — so keeping the type would mean
 * a screen branching on a fact nothing can ever report.
 */

export type Cursor = { offset: number; size: number };

export type Page<T> = { rows: T[]; total: number; hasMore: boolean };

/**
 * One page of rows, for the table's own pager.
 *
 * This is a *display* page, over whatever the screen has already accumulated in memory — a separate
 * concern from the network-level `?since=` cursor `useRequestLog` sends the engine (the one that
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
 * The node's answer, as a value.
 *
 * `unknown` is not `empty`, and this type is where that distinction is made unrepresentable-as-one:
 * a node that could not answer has an unknown journal, and rendering it as an empty table tells an
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
    reason: "this node answered with a body that is not a request list",
  };
}
