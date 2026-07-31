import type { FleetReadState } from "../../app/fleetView.ts";

/**
 * The request log's data source — **the convergence seam for #147 H**.
 *
 * The screen renders only through this module. Today the one implementation reads a single node's
 * journal (`GET /imposters/:port/requests`), and `coverageFor` reports that fact honestly. When the
 * verification plane's merged journal (#147 B) and cursors (#147 D) land, slice H implements the
 * same `Coverage`/`Page` shapes with `{ kind: "fleet" }`, the per-node banner disappears on its own,
 * and no presentation code changes.
 *
 * That is the whole design constraint: *the degraded label disappears, the screen does not.*
 */

/**
 * How much of the fleet's traffic the rows on screen actually represent.
 *
 * `null` means **could not be determined** and never zero — the `/_fleet/*` projection is
 * fleet-admin-gated, so most principals cannot learn how many nodes exist. Reporting "0 other
 * nodes" to them would assert something nothing supports; reporting `null` is what lets the screen
 * say "one node's traffic" without also implying it is the whole picture.
 */
export type Coverage =
  | { kind: "per-node"; node: string | null; unrepresented: number | null }
  | { kind: "fleet" };

export type Cursor = { offset: number; size: number };

export type Page<T> = { rows: T[]; total: number; hasMore: boolean };

/**
 * What the fleet reading says about this node's coverage.
 *
 * A one-voter fleet is a supported deployment, not a fleet missing two nodes, so it reports
 * `fleet` — this node's journal genuinely is the whole fleet's traffic, and labelling it partial
 * would train operators to ignore the label on the fleets where it means something.
 */
export function coverageFor(state: FleetReadState): Coverage {
  if (state.kind !== "read") {
    return { kind: "per-node", node: null, unrepresented: null };
  }
  const { view } = state;
  if (view.singleNode) return { kind: "fleet" };
  // The ring is the set this node believes exists; everything in it but this node is traffic these
  // rows do not cover.
  const others = view.ringMembers.filter((id) => id !== view.nodeId).length;
  /*
   * Zero peers here is not "no other nodes exist" — `singleNode` already answered that, from
   * `voters`. Reaching this line with an empty ring means the two `/_fleet/*` reads disagree (a
   * skew `fleetView.ts` documents as expected and deliberately does not call a degradation) or the
   * node has no applied membership at all, which that module names as a fault. Reporting `0` would
   * assert complete coverage from a node that has none — the precise claim `null` exists to avoid.
   */
  return {
    kind: "per-node",
    node: String(view.nodeId),
    unrepresented: others === 0 ? null : others,
  };
}

/** The sentence above the table. It is the §11 exit criterion, so it is data, not decoration. */
export function describeCoverage(coverage: Coverage): string {
  if (coverage.kind === "fleet") {
    return "This node is the whole fleet — these are all the requests the fleet has recorded.";
  }
  if (coverage.node === null || coverage.unrepresented === null) {
    // Deliberately does not say *why* the fleet reading is absent. This branch covers both a
    // principal refused `/_fleet/*` and a reading that has not landed yet, and naming either cause
    // would be wrong half the time — whereas the per-node fact is true in both.
    return (
      "One node's traffic — this node. Which node this is, and how many other nodes exist, is not " +
      "shown here. If a call is missing, check the other nodes before concluding it never arrived."
    );
  }
  const others = coverage.unrepresented;
  return (
    `One node's traffic — node ${coverage.node}. ${others} other ` +
    `${others === 1 ? "node is" : "nodes are"} not represented. If a call is missing, check the ` +
    "other nodes before concluding it never arrived."
  );
}

/**
 * One page of rows.
 *
 * v1 pages client-side over the array the node returns, which is what keeps the DOM bounded on a
 * busy imposter. It does **not** bound the response: the node still serves its whole journal in one
 * body. Closing that needs the server's `?since=` cursor and `x-rift-next-index` header, which is
 * the same seam #147 D widens — so it is deliberately expressed as a `Cursor` here rather than an
 * array slice inlined in the screen.
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
 * Hand-written rather than taken from `schema.ts` because the contract declares this response as an
 * untyped `object` — see the API-gap note in the PR. Every field is optional on read: this is
 * attacker-adjacent data from an untyped endpoint, and a missing field must render as absent rather
 * than crash the screen an operator opened to diagnose something else.
 */
export type RecordedRequest = {
  requestFrom?: string;
  method?: string;
  path?: string;
  query?: Record<string, string>;
  /**
   * A single-valued header is a **bare string**, not a one-element array — see
   * `multi_value_headers::serialize` in `rift-mock-core/src/imposter/types.rs`, which emits the
   * scalar for `[single]` and an array only for `many`. Its deserializer also tolerates JSON
   * numbers and booleans, because real recorded imposters carry `"Content-Length": 124`.
   */
  headers?: Record<string, unknown>;
  body?: string;
  /** `"base64"` when `body` is an encoded binary payload (`ResponseMode`); absent means text. */
  _mode?: string;
  timestamp?: string;
};

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
