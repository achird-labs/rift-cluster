/**
 * Reads the design draws that the fleet does not answer yet.
 *
 * Every function here is a real call site for a real panel, returning "the fleet said nothing"
 * until the endpoint behind it exists. They are gathered in one file rather than scattered as
 * `null`s at each panel for three reasons:
 *
 * 1. **The UI is built to the design.** A panel wired to one of these renders its full layout —
 *    heading, columns, empty row — exactly as it will when the data arrives. Nothing is stubbed out
 *    of the tree and nothing has to be rebuilt later; the call site stays, only its body changes.
 * 2. **Each one names its issue.** A reader who wonders why a column is empty gets the answer and
 *    the ticket in the same place, and the day the endpoint lands there is a single obvious file
 *    listing everything that can now be filled in.
 * 3. **Empty, never invented.** These return absence, not a plausible figure. This console's whole
 *    contract is that what it shows is true of a live fleet (RFC-006 §3 rule 2, made mechanical by
 *    `app/contract.ts`), and a fabricated "PARKED INTENTS · 3" is worse than a blank one because an
 *    operator would act on it.
 *
 * The convention: return `Pending<T>`, never throw, never poll. A screen renders `value` when it is
 * there and the panel's own empty state when it is not.
 */

/** Absent because nothing publishes it yet, with the issue that will. */
export type Pending<T> = { value: T; pending: false } | { value: null; pending: true; issue: number };

/** The shape every function below returns until its endpoint exists. */
function pending<T>(issue: number): Pending<T> {
  return { value: null, pending: true, issue };
}

/** `https://github.com/achird-labs/rift-cluster/issues/<n>` — the console links to these in title text. */
export const ISSUE_URL = (issue: number): string =>
  `https://github.com/achird-labs/rift-cluster/issues/${String(issue)}`;

/*
 * `flowOwner(key)` used to live here, and both its callers asserted something untrue: that a port
 * has a flow owner. It does not — imposters, stubs and config are replicated to every node and are
 * owned by none of them; only a *flow* is owned, keyed by an opaque flow id under its
 * `ContextScope` prefix, so a port has as many owners as it has flows.
 *
 * It is deleted rather than re-pointed because the honest version cannot have this signature: an
 * owner is resolved from a flow within its imposter's scope, not from a bare key. #359 lands it on
 * the flow-state surface, where flows are actually enumerated. The fleet rail's hash-space panel
 * keeps its own marker — "which key lands on which member" is still unpublished, and still #359.
 */

/**
 * How many writes the fleet has accepted and not yet replayed.
 *
 * `--cluster-admin-async` answers `202` the moment a write is parked; #9 built the durable-intent
 * path underneath. Nothing reports the depth of that queue.
 *
 * @see https://github.com/achird-labs/rift-cluster/issues/360
 */
export function parkedIntents(): Pending<number> {
  return pending(360);
}

/*
 * `peerApplied(nodeId)` lived here until #361 built the projection it was waiting for.
 *
 * Its reason for existing is worth keeping, because it is the shape of every remaining entry in
 * this file: the console is served under `default-src 'self'`, so the page can only ever dial the
 * node that served it, and a peer's applied index was therefore unreachable from the browser *by
 * construction* — not merely unbuilt. `/_fleet/members` now folds each voter's own report into one
 * body, and `FleetView.members` carries it.
 */

/** One line of the merged tail, once there is one to read. */
export type TailLine = {
  timestamp: string;
  node: string;
  port: number;
  request: string;
  status: number;
};

/**
 * The fleet-wide request tail, across every imposter.
 *
 * Per-imposter merged reads exist (#223, and #348's SSE tail). One ordered stream across all of them
 * does not, and fanning out client-side would produce an ordering that is an artifact of network
 * timing rather than of the journal — the exact thing the vector cursor exists to prevent.
 *
 * @see https://github.com/achird-labs/rift-cluster/issues/362
 */
export function mergedTail(): Pending<readonly TailLine[]> {
  return pending(362);
}

/**
 * How many requests an imposter has served.
 *
 * `numberOfRequests` reaches the body only through its non-exhaustive index signature, which
 * `app/contract.ts` refuses on purpose. It needs declaring in the schema before it can be rendered.
 *
 * @see https://github.com/achird-labs/rift-cluster/issues/363
 */
export function requestCount(_port: number): Pending<number> {
  return pending(363);
}

/**
 * The node, status and latency of a recorded request.
 *
 * A `RecordedRequest` carries when it arrived, what it asked for and which stub answered. It does
 * not carry which node served it, what went back, or how long it took.
 *
 * @see https://github.com/achird-labs/rift-cluster/issues/364
 */
export type RequestOutcome = { node: string; status: number; latencyMs: number };

export function requestOutcome(_index: number): Pending<RequestOutcome> {
  return pending(364);
}

/** The last snapshot, once a route reports one. */
export type Snapshot = { index: number; bytes: number; ageSeconds: number; complete: boolean };

/**
 * Snapshot state and the operations over it.
 *
 * Snapshotting and compaction run on the node's own thresholds; nothing reports the last one or
 * triggers a new one.
 *
 * @see https://github.com/achird-labs/rift-cluster/issues/365
 */
export function lastSnapshot(): Pending<Snapshot> {
  return pending(365);
}

/**
 * Whether membership can be changed from here.
 *
 * The voter floor is real and enforced (#69, #71) — what is missing is a route to *request* a
 * change, so today membership moves by starting and stopping nodes rather than by asking the fleet.
 *
 * @see https://github.com/achird-labs/rift-cluster/issues/366
 */
export function membershipOps(): Pending<{ canAddLearner: boolean; canRemoveVoter: boolean }> {
  return pending(366);
}

/**
 * The durability settings a write actually rode.
 *
 * The write barrier, its timeout, the flow fsync policy and the admin-write mode are all command
 * line flags on the node and none is read back by any endpoint. Folded into the snapshot issue's
 * neighbourhood rather than filed separately — it is the same "the fleet does not describe its own
 * configuration" gap.
 *
 * @see https://github.com/achird-labs/rift-cluster/issues/365
 */
export function durability(): Pending<{ barrier: string; timeoutMs: number; fsync: string }> {
  return pending(365);
}
