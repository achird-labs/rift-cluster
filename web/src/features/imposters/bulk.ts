import type { CommitOutcome } from "../writes/commit.ts";

/**
 * Running one admin call per selected imposter, and telling the truth about each one.
 *
 * There is no bulk endpoint, deliberately (#252): a set-level write would need its own
 * partial-failure semantics inside the state machine, and the honest client-side version is N calls
 * whose individual outcomes are reported individually. Everything that makes that honest lives here,
 * because every one of these properties is easy to lose in a component and impossible to test there:
 *
 *   - the batch does NOT stop at the first refusal;
 *   - a parked (`202`) write is reported as still committing, never as applied;
 *   - the result names every item, so "17 deleted, 3 refused" can name the three.
 */

/**
 * What became of one item.
 *
 * `in-flight` is `CommitOutcome`'s `unobservable` under the name this screen uses for it. The
 * distinction it preserves is the one #211 exists for: the fleet accepted the write and this session
 * could not watch it land. Folding that into `done` would report a success nobody observed; folding
 * it into `refused` would report a failure that did not happen.
 */
export type BulkItemOutcome =
  | { kind: "done" }
  | { kind: "in-flight"; reason: string }
  | { kind: "refused"; detail: string };

export type BulkItemResult = {
  port: number;
  outcome: BulkItemOutcome;
};

export type BulkResult = {
  results: BulkItemResult[];
  done: number;
  inFlight: number;
  refused: number;
};

/** The message to show for a thrown error — the fleet's own words when it has them. */
function refusalDetail(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

export function summarize(results: readonly BulkItemResult[]): BulkResult {
  return {
    results: [...results],
    done: results.filter((r) => r.outcome.kind === "done").length,
    inFlight: results.filter((r) => r.outcome.kind === "in-flight").length,
    refused: results.filter((r) => r.outcome.kind === "refused").length,
  };
}

/**
 * Run `call` once per port, in order, collecting every outcome.
 *
 * **Serial, not `Promise.all`.** Three reasons, in order of how much they matter: a hundred
 * concurrent admin writes against one fleet is a load test nobody asked for; `Promise.all` rejects on
 * the first failure, which is precisely the halting behaviour this must not have; and progress is
 * only meaningful — "31 of 80" — if the work happens in a known order.
 *
 * `onProgress` is called after each item so the bar can move. It is not called before the first
 * call, because "0 of N" is what the caller already rendered.
 */
export async function runBulk(
  ports: readonly number[],
  call: (port: number) => Promise<CommitOutcome>,
  onProgress?: (completed: number, total: number) => void,
): Promise<BulkResult> {
  const results: BulkItemResult[] = [];

  for (const port of ports) {
    results.push({ port, outcome: await runOne(port, call) });
    onProgress?.(results.length, ports.length);
  }

  return summarize(results);
}

async function runOne(
  port: number,
  call: (port: number) => Promise<CommitOutcome>,
): Promise<BulkItemOutcome> {
  try {
    const outcome = await call(port);
    /*
     * The mutation hooks in `queries.ts` throw on a `failed` commit, so `applied` and `unobservable`
     * are the only two that arrive here — but this switches on all three rather than assuming that.
     * The assumption is one refactor away from being wrong, and the failure mode of being wrong is
     * reporting a refused write as applied, which is the exact thing this module exists to prevent.
     */
    switch (outcome.kind) {
      case "applied":
        return { kind: "done" };
      case "unobservable":
        return { kind: "in-flight", reason: outcome.reason };
      case "failed":
        return { kind: "refused", detail: outcome.detail };
    }
  } catch (error) {
    return { kind: "refused", detail: refusalDetail(error) };
  }
}

/**
 * The sentence the screen shows when a batch finishes.
 *
 * Never "N succeeded" alone: a summary that omits the refusals reads as a clean run, and the operator
 * closes the bar without discovering that three of their twenty imposters are still there.
 */
export function summaryLine(result: BulkResult, verb: string): string {
  const parts: string[] = [`${result.done} ${verb}`];
  if (result.inFlight > 0) parts.push(`${result.inFlight} still committing`);
  if (result.refused > 0) parts.push(`${result.refused} refused`);
  return parts.join(", ");
}
