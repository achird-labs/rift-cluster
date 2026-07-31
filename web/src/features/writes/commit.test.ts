import { afterEach, describe, expect, it, vi } from "vitest";

import { applied, pollCommit } from "./commit.ts";

afterEach(() => {
  vi.unstubAllGlobals();
});

/** A fetch stub that answers each op-status poll from a scripted queue, keyed by op id. */
function stubOpStatus(script: Record<string, Array<{ body?: unknown; status?: number }>>): {
  calls: string[];
} {
  const calls: string[] = [];
  const remaining = Object.fromEntries(
    Object.entries(script).map(([id, steps]) => [id, [...steps]]),
  );
  vi.stubGlobal(
    "fetch",
    vi.fn().mockImplementation((path: string) => {
      calls.push(path);
      const id = path.split("/").pop() ?? "";
      const step = remaining[id]?.shift() ?? { status: 404 };
      const status = step.status ?? 200;
      const body = step.body === undefined ? "" : JSON.stringify(step.body);
      return Promise.resolve(new Response(body, { status }));
    }),
  );
  return { calls };
}

describe("resolving a parked write", () => {
  it("reports applied once the op has committed", async () => {
    stubOpStatus({ "op-1": [{ body: { state: "applied", revision: 17 } }] });
    await expect(pollCommit(["op-1"], { intervalMs: 0 })).resolves.toEqual({ kind: "applied" });
  });

  it("keeps polling while the op is still pending", async () => {
    const { calls } = stubOpStatus({
      "op-1": [
        { body: { state: "pending" } },
        { body: { state: "pending" } },
        { body: { state: "applied", revision: 9 } },
      ],
    });
    await expect(pollCommit(["op-1"], { intervalMs: 0 })).resolves.toEqual({ kind: "applied" });
    expect(calls.length).toBe(3);
  });

  it("reports failed with the server's own reason", async () => {
    stubOpStatus({
      "op-1": [{ body: { state: "failed", revision: 4, detail: "revision conflict" } }],
    });
    await expect(pollCommit(["op-1"], { intervalMs: 0 })).resolves.toEqual({
      kind: "failed",
      detail: "revision conflict",
    });
  });

  it("polls every derived id of a multi-op mutation, and applies only when all have", async () => {
    const { calls } = stubOpStatus({
      "op-a": [{ body: { state: "applied", revision: 1 } }],
      "op-b": [{ body: { state: "pending" } }, { body: { state: "applied", revision: 2 } }],
    });
    await expect(pollCommit(["op-a", "op-b"], { intervalMs: 0 })).resolves.toEqual({
      kind: "applied",
    });
    expect(calls.filter((c) => c.endsWith("op-a")).length).toBeGreaterThan(0);
    expect(calls.filter((c) => c.endsWith("op-b")).length).toBe(2);
  });

  it("fails the whole write when any one of the derived ops failed, and says what landed", async () => {
    // The server has no batch transaction, so `op-a` is not rolled back. A bare "failed" would tell
    // the operator nothing happened when half of it did — the same overclaim, pointing the other
    // way, that this module exists to stop.
    stubOpStatus({
      "op-a": [{ body: { state: "applied", revision: 1 } }],
      "op-b": [{ body: { state: "failed", detail: "port claimed by another tenant" } }],
    });
    const outcome = await pollCommit(["op-a", "op-b"], { intervalMs: 0 });
    expect(outcome.kind).toBe("failed");
    expect(outcome).toMatchObject({
      detail: "port claimed by another tenant (1 of 2 had already applied)",
    });
  });

  it("does not claim a partial application when nothing landed", async () => {
    stubOpStatus({
      "op-a": [{ body: { state: "failed", detail: "refused" } }],
      "op-b": [{ body: { state: "pending" } }],
    });
    await expect(pollCommit(["op-a", "op-b"], { intervalMs: 0 })).resolves.toEqual({
      kind: "failed",
      detail: "refused",
    });
  });
});

describe("a write this principal cannot observe is not a failed write", () => {
  /*
   * `GET /_fleet/ops/{opId}` is fleet-scoped (ClusterAdmin/FleetAdmin only) and its 404
   * deliberately conflates "unknown op", "malformed id" and "caller lacks fleet scope". So an
   * ordinary tenant admin toggling an imposter cannot poll at all — and the write has very likely
   * committed. Calling that `failed` would be the same collapse this issue is about, inverted:
   * asserting an outcome nobody observed.
   */
  it("reports a 404 from the fleet projection as unobservable, not failed", async () => {
    stubOpStatus({ "op-1": [{ status: 404 }] });
    const outcome = await pollCommit(["op-1"], { intervalMs: 0 });
    expect(outcome.kind).toBe("unobservable");
  });

  it("reports a 403 the same way", async () => {
    stubOpStatus({ "op-1": [{ status: 403 }] });
    expect((await pollCommit(["op-1"], { intervalMs: 0 })).kind).toBe("unobservable");
  });

  it("does NOT absorb a 401 — a dead session is the operator's problem to act on", async () => {
    // "Accepted, not yet confirmed" would describe the write, when what actually happened is that
    // the login expired. That is actionable and has to reach the screen as an error.
    stubOpStatus({ "op-1": [{ status: 401 }] });
    await expect(pollCommit(["op-1"], { intervalMs: 0 })).rejects.toThrow();
  });

  it("gives up as unobservable rather than polling a pending op forever", async () => {
    stubOpStatus({ "op-1": Array.from({ length: 50 }, () => ({ body: { state: "pending" } })) });
    const outcome = await pollCommit(["op-1"], { intervalMs: 0, attempts: 3 });
    expect(outcome.kind).toBe("unobservable");
  });

  it("has nothing to poll when the 202 carried no op id at all", async () => {
    stubOpStatus({});
    expect((await pollCommit([], { intervalMs: 0 })).kind).toBe("unobservable");
  });
});

describe("a route that cannot park must not park silently", () => {
  /*
   * Seven of the console's ten mutations hit routes the contract never gives a 202 — the admin
   * plane and the session exchange. `applied` is how they say so. If one ever does park, the write
   * is genuinely in flight and unresolved, and answering the caller with a value would be the
   * original bug in a new place; it throws instead.
   */
  it("unwraps an applied result", () => {
    expect(applied({ kind: "applied", data: { id: "acme" } })).toEqual({ id: "acme" });
  });

  it("throws when a route that should never park does", () => {
    expect(() => applied({ kind: "parked", opIds: ["op-1"] })).toThrow(/parked/i);
  });
});
