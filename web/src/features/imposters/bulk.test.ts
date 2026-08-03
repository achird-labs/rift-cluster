import { describe, expect, it, vi } from "vitest";

import type { CommitOutcome } from "../writes/commit.ts";
import { type BulkItemResult, runBulk, summarize, summaryLine } from "./bulk.ts";

const applied: CommitOutcome = { kind: "applied" };

describe("runBulk", () => {
  it("calls once per port and reports each one", async () => {
    const call = vi.fn(async (_port: number) => applied);
    const result = await runBulk([1, 2, 3], call);

    expect(call.mock.calls.map(([port]) => port)).toEqual([1, 2, 3]);
    expect(result.done).toBe(3);
    expect(result.results.map((r) => r.port)).toEqual([1, 2, 3]);
  });

  it("DOES NOT stop at the first refusal", async () => {
    // The property the whole module exists for. A batch that halts leaves the operator with a
    // partially-applied set and no record of which part.
    const call = vi.fn(async (port: number) => {
      if (port === 2) throw new Error("port 2 is owned by source `mocks` and cannot be deleted");
      return applied;
    });

    const result = await runBulk([1, 2, 3], call);

    expect(call).toHaveBeenCalledTimes(3);
    expect(result.done).toBe(2);
    expect(result.refused).toBe(1);
    expect(result.results[1]).toEqual({
      port: 2,
      outcome: { kind: "refused", detail: "port 2 is owned by source `mocks` and cannot be deleted" },
    });
  });

  it("reports a parked write as in-flight, never as done", async () => {
    // #211: `unobservable` is "the fleet accepted it and this session cannot watch it land".
    // Counting it as done would claim an outcome nobody observed.
    const call = async (): Promise<CommitOutcome> => ({
      kind: "unobservable",
      reason: "the write is still committing — it has not landed yet, and has not been refused",
    });

    const result = await runBulk([1], call);

    expect(result.done).toBe(0);
    expect(result.inFlight).toBe(1);
    expect(result.results[0]?.outcome).toEqual({
      kind: "in-flight",
      reason: "the write is still committing — it has not landed yet, and has not been refused",
    });
  });

  it("maps a returned `failed` outcome to refused, not to done", async () => {
    // The hooks throw on `failed` today. This asserts the module does not depend on that.
    const call = async (): Promise<CommitOutcome> => ({ kind: "failed", detail: "the fleet refused" });
    const result = await runBulk([1], call);
    expect(result.refused).toBe(1);
    expect(result.results[0]?.outcome).toEqual({ kind: "refused", detail: "the fleet refused" });
  });

  it("handles a mixed batch, naming every port", async () => {
    const call = async (port: number): Promise<CommitOutcome> => {
      if (port === 2) throw new Error("refused");
      if (port === 3) return { kind: "unobservable", reason: "needs fleet-admin scope" };
      return applied;
    };

    const result = await runBulk([1, 2, 3, 4], call);

    expect({ done: result.done, inFlight: result.inFlight, refused: result.refused }).toEqual({
      done: 2,
      inFlight: 1,
      refused: 1,
    });
    expect(result.results.map((r) => `${r.port}:${r.outcome.kind}`)).toEqual([
      "1:done",
      "2:refused",
      "3:in-flight",
      "4:done",
    ]);
  });

  it("reports progress after each item, never before the first", async () => {
    const seen: [number, number][] = [];
    await runBulk([1, 2, 3], async () => applied, (done, total) => seen.push([done, total]));
    expect(seen).toEqual([
      [1, 3],
      [2, 3],
      [3, 3],
    ]);
  });

  it("runs serially, not concurrently", async () => {
    // `Promise.all` would both flood the fleet and reject on the first failure.
    let inFlight = 0;
    let peak = 0;
    const call = async (): Promise<CommitOutcome> => {
      inFlight += 1;
      peak = Math.max(peak, inFlight);
      await Promise.resolve();
      inFlight -= 1;
      return applied;
    };

    await runBulk([1, 2, 3, 4], call);

    expect(peak).toBe(1);
  });

  it("is a no-op on an empty selection", async () => {
    const call = vi.fn(async (_port: number) => applied);
    const result = await runBulk([], call);
    expect(call).not.toHaveBeenCalled();
    expect(result).toEqual({ results: [], done: 0, inFlight: 0, refused: 0 });
  });

  it("survives a thrown non-Error", async () => {
    const result = await runBulk([1], async () => {
      throw "just a string";
    });
    expect(result.results[0]?.outcome).toEqual({ kind: "refused", detail: "just a string" });
  });
});

describe("summaryLine", () => {
  const results = (kinds: BulkItemResult["outcome"]["kind"][]): BulkItemResult[] =>
    kinds.map((kind, index) => ({
      port: 4500 + index,
      outcome:
        kind === "done"
          ? { kind: "done" }
          : kind === "in-flight"
            ? { kind: "in-flight", reason: "r" }
            : { kind: "refused", detail: "d" },
    }));

  it("names refusals rather than reporting a clean run", () => {
    expect(summaryLine(summarize(results(["done", "done", "refused"])), "deleted")).toBe(
      "2 deleted, 1 refused",
    );
  });

  it("names in-flight writes separately from applied ones", () => {
    expect(summaryLine(summarize(results(["done", "in-flight"])), "disabled")).toBe(
      "1 disabled, 1 still committing",
    );
  });

  it("says only the count when everything applied", () => {
    expect(summaryLine(summarize(results(["done", "done"])), "enabled")).toBe("2 enabled");
  });

  it("still reports zero applied when the whole batch was refused", () => {
    expect(summaryLine(summarize(results(["refused", "refused"])), "deleted")).toBe(
      "0 deleted, 2 refused",
    );
  });
});
