import { ApiError, type SendResult, apiGet } from "../../api/client.ts";
import { fleetOpPath } from "../../api/paths.ts";
import type { components } from "../../api/schema.ts";

/**
 * Resolving a parked write — the second half of `202 AcceptedParked`.
 *
 * Under `--cluster-admin-async` a mutating admin route answers `202` as soon as the write is
 * durably *parked*, before it has committed. The contract's own words: "poll the returned op id
 * rather than assuming this response's absence of a body reflects the final state."
 */

type FleetOpStatus = components["schemas"]["FleetOpStatus"];

/**
 * What became of a write, once we stopped guessing.
 *
 * The third case is the one worth defending. `GET /_fleet/ops/{opId}` is fleet-scoped
 * (`ClusterAdmin`/FleetAdmin only) and its `404` deliberately conflates "unknown op", "malformed
 * id" and "caller lacks fleet scope" — so an ordinary tenant admin toggling an imposter cannot poll
 * at all, and their write has very likely committed. Reporting that as `failed` would be the same
 * mistake this module exists to fix, just inverted: asserting an outcome nobody observed. So it gets
 * its own name and its own sentence on screen.
 */
export type CommitOutcome =
  | { kind: "applied" }
  | { kind: "failed"; detail: string }
  | { kind: "unobservable"; reason: string };

export type PollOptions = {
  tenant?: string | null | undefined;
  /** Poll cadence. Tests pass `0`; no caller overrides it, so production uses the default below. */
  intervalMs?: number;
  /** Attempts per op before giving up as unobservable. Default gives roughly ten seconds. */
  attempts?: number;
};

const DEFAULT_INTERVAL_MS = 500;
const DEFAULT_ATTEMPTS = 20;

const sleep = (ms: number): Promise<void> =>
  ms <= 0 ? Promise.resolve() : new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Assert that a route which cannot park did not park, and unwrap it.
 *
 * Most of the console's mutations hit routes the contract never gives a `202` — the RFC-002 admin
 * plane and the session exchange. This is how they say so out loud. Throwing rather than
 * returning a default is the point: if one of them ever does park, the write is genuinely in
 * flight and its outcome unknown, and handing the caller a value would recreate the original bug
 * somewhere new.
 */
export function applied<T>(result: SendResult<T>): T {
  if (result.kind === "parked") {
    throw new Error(
      `this route answered 202 (parked, op ${result.opIds.join(", ") || "unknown"}) but the ` +
        "contract does not declare a parked response for it — the write is still in flight and " +
        "its outcome is unknown",
    );
  }
  return result.data;
}

/**
 * Read one op's status.
 *
 * "Cannot see the fleet projection" becomes the `"unreadable"` sentinel rather than an error —
 * distinct from `null`, which is an empty body and reads as still-pending.
 */
async function readOp(
  opId: string,
  options: PollOptions,
): Promise<FleetOpStatus | null | "unreadable"> {
  try {
    return await apiGet<FleetOpStatus>(fleetOpPath(opId), { tenant: options.tenant });
  } catch (error) {
    if (error instanceof ApiError && (error.status === 403 || error.status === 404)) {
      // Not a failure of the write — a limit on what this principal can see. `404` covers an
      // unknown id too, but we only ever poll ids the server just minted, so scope is the
      // overwhelmingly likelier cause and neither is evidence the write did not land.
      return "unreadable";
    }
    /*
     * `401` is deliberately NOT in that bucket. It says the session died, which is both actionable
     * and something the operator has to be told — "accepted, not yet confirmed" would read as a
     * property of the write when it is really a property of their login. It propagates so
     * `primitives.describe` can render "sign in again".
     */
    throw error;
  }
}

/**
 * Poll every op id a `202` handed back until all are terminal.
 *
 * Pass the ids from `SendResult`'s `parked` branch verbatim: for a multi-op mutation those are the
 * *derived* ids, which are the only ones the server parks.
 */
export async function pollCommit(
  opIds: readonly string[],
  options: PollOptions = {},
): Promise<CommitOutcome> {
  if (opIds.length === 0) {
    return {
      kind: "unobservable",
      reason: "the fleet accepted the write but returned no op id to follow it by",
    };
  }

  const interval = options.intervalMs ?? DEFAULT_INTERVAL_MS;
  const attempts = options.attempts ?? DEFAULT_ATTEMPTS;
  const pending = new Set(opIds);

  for (let attempt = 0; attempt < attempts && pending.size > 0; attempt += 1) {
    if (attempt > 0) await sleep(interval);

    for (const opId of [...pending]) {
      const status = await readOp(opId, options);
      if (status === "unreadable") {
        return {
          kind: "unobservable",
          reason:
            "the fleet accepted the write, but reading its progress needs fleet-admin scope this " +
            "session does not have",
        };
      }
      if (status === null) continue;
      if (status.state === "failed") {
        /*
         * Name what did land. The server has no batch transaction, so the ops that already applied
         * are not rolled back — reporting a bare "failed" for a multi-op mutation would tell the
         * operator nothing happened when part of it did, which is the same overclaim this module
         * exists to prevent.
         */
        const landed = opIds.filter((id) => !pending.has(id));
        const partial =
          landed.length > 0 ? ` (${landed.length} of ${opIds.length} had already applied)` : "";
        return {
          kind: "failed",
          detail: `${status.detail ?? "the fleet refused the write"}${partial}`,
        };
      }
      if (status.state === "applied") pending.delete(opId);
    }
  }

  if (pending.size > 0) {
    return {
      kind: "unobservable",
      reason: "the write is still committing — it has not landed yet, and has not been refused",
    };
  }
  return { kind: "applied" };
}

/**
 * Send-then-settle: the shape the three parked-capable mutations want.
 *
 * An applied write resolves immediately; a parked one resolves only once polling settles, so a
 * caller's in-flight state covers "committing" without needing a second one.
 */
export async function settle<T>(
  result: SendResult<T>,
  options: PollOptions = {},
): Promise<CommitOutcome> {
  return result.kind === "applied" ? { kind: "applied" } : pollCommit(result.opIds, options);
}
