import { describe, expect, it } from "vitest";

import {
  hasRequestCount,
  isEmptySpace,
  readFlowStateEntry,
  readScenarios,
  readSpace,
} from "./space.ts";

describe("readScenarios — unknown is not empty", () => {
  it("reads a well-formed list under the flow the node named", () => {
    const state = readScenarios({
      flowId: "checkout-1",
      scenarios: [{ name: "checkout", state: "awaiting-payment" }],
    });
    expect(state).toEqual({
      kind: "scenarios",
      flowId: "checkout-1",
      scenarios: [{ name: "checkout", state: "awaiting-payment" }],
    });
  });

  it("reads a genuinely empty list as an empty list, not as unknown", () => {
    // An imposter whose stubs declare no scenarios is a real, reportable answer. Collapsing it into
    // `unknown` would be the mirror of the bug this type exists to prevent.
    const state = readScenarios({ flowId: "default", scenarios: [] });
    expect(state).toEqual({ kind: "scenarios", flowId: "default", scenarios: [] });
  });

  it("refuses to display states attributed to no flow", () => {
    // Scenario state is per-space. A list rendered without the flow it was read under invites the
    // operator to act on it as though it were the imposter's global state, which it never is.
    expect(readScenarios({ scenarios: [] }).kind).toBe("unknown");
    expect(readScenarios({ flowId: 7, scenarios: [] }).kind).toBe("unknown");
  });

  it("reads a body that is not a scenario list as unknown rather than as no scenarios", () => {
    for (const body of [null, undefined, "nope", 42, [], { flowId: "f" }, { flowId: "f", scenarios: {} }]) {
      expect([body, readScenarios(body).kind]).toEqual([body, "unknown"]);
    }
  });
});

describe("readSpace", () => {
  it("reads a populated space", () => {
    const state = readSpace({
      space: "checkout-1",
      stubs: [{ responses: [] }],
      scenarios: [{ name: "checkout", state: "start" }],
      numberOfRequests: 3,
    });
    expect(state.kind).toBe("space");
    if (state.kind !== "space") throw new Error("unreachable");
    expect(state.space.numberOfRequests).toBe(3);
    expect(isEmptySpace(state.space)).toBe(false);
    expect(hasRequestCount(state.space)).toBe(true);
  });

  it("distinguishes a space that holds nothing from a space that could not be read", () => {
    // The distinction the issue calls out. Both render, and they render different sentences.
    const empty = readSpace({ space: "f", stubs: [], scenarios: [], numberOfRequests: 0 });
    expect(empty.kind).toBe("space");
    if (empty.kind !== "space") throw new Error("unreachable");
    expect(isEmptySpace(empty.space)).toBe(true);

    expect(readSpace(null).kind).toBe("unknown");
    expect(readSpace("nope").kind).toBe("unknown");
    expect(readSpace({ space: "f" }).kind).toBe("unknown");
    expect(readSpace({ space: "f", stubs: [], scenarios: {} }).kind).toBe("unknown");
  });

  it("never invents a request count of zero for a body that carried none", () => {
    /*
     * `0` is a claim — "nothing reached this space" — and that is precisely the question an
     * operator opens this screen to answer. A body missing the field must render as "—".
     */
    const state = readSpace({ space: "f", stubs: [], scenarios: [] });
    expect(state.kind).toBe("space");
    if (state.kind !== "space") throw new Error("unreachable");
    expect(hasRequestCount(state.space)).toBe(false);
  });
});

describe("readFlowStateEntry", () => {
  it("reads an entry, including one whose stored value is null", () => {
    // The contract declares `value` as any JSON *including* `null`, so a null value is a stored
    // value. Treating it as a missing field would report a legitimately-null entry as unreadable.
    const state = readFlowStateEntry({ flowId: "f", key: "cart", value: null });
    expect(state).toEqual({ kind: "value", entry: { flowId: "f", key: "cart", value: null } });
  });

  it("reads scalar and document values alike", () => {
    for (const value of [0, false, "", [], { nested: true }]) {
      const state = readFlowStateEntry({ flowId: "f", key: "k", value });
      expect([value, state.kind]).toEqual([value, "value"]);
    }
  });

  it("reads a body that is not an entry as unknown", () => {
    for (const body of [null, "nope", 42, {}, { key: "k" }, { flowId: "f" }]) {
      expect([body, readFlowStateEntry(body).kind]).toEqual([body, "unknown"]);
    }
  });
});
