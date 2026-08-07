import { describe, expect, it } from "vitest";

import type { components } from "../../api/schema.ts";
import { INITIAL_STATE, scenarioDefinitions } from "./definitions.ts";

type Stub = components["schemas"]["Stub"];

/** The shape a real imposter sends — verified against a live fleet, not assumed. */
const stub = (fields: Partial<Stub>): Stub => ({ responses: [], ...fields }) as Stub;

describe("scenario definitions, derived from the stubs that declare them", () => {
  it("groups by scenario name and skips stubs that name none", () => {
    const defs = scenarioDefinitions([
      stub({ id: "a", scenarioName: "checkout" }),
      stub({ id: "b", scenarioName: "refund" }),
      // An ordinary stub. Not part of any machine, so it contributes nothing.
      stub({ id: "c" }),
    ]);
    expect(defs.map((d) => d.name)).toEqual(["checkout", "refund"]);
  });

  it("reads an edge from the state a stub requires to the state it moves to", () => {
    const [def] = scenarioDefinitions([
      stub({ id: "start", scenarioName: "checkout", newScenarioState: "started" }),
      stub({
        id: "finish",
        scenarioName: "checkout",
        requiredScenarioState: "started",
        newScenarioState: "done",
      }),
    ]);
    expect(def?.transitions).toEqual([
      // No `requiredScenarioState` means it matches from wherever the machine begins.
      { from: INITIAL_STATE, to: "started", stub: "start" },
      { from: "started", to: "done", stub: "finish" },
    ]);
  });

  it("names every state in first-seen order, beginning at the initial one", () => {
    // First-seen, never sorted: match order is the imposter's own, and alphabetising would hide
    // which state the traffic actually reaches first.
    const [def] = scenarioDefinitions([
      stub({ scenarioName: "s", requiredScenarioState: "zeta", newScenarioState: "alpha" }),
    ]);
    expect(def?.states).toEqual([INITIAL_STATE, "zeta", "alpha"]);
  });

  it("does not draw a self-loop for a stub that answers without advancing", () => {
    /*
     * A stub requiring a state and moving to none is a terminal match: it answers there and leaves
     * the machine alone. Recorded as `from -> from` it would render as a cycle, which reads as a
     * scenario that can never finish.
     */
    const [def] = scenarioDefinitions([
      stub({ id: "terminal", scenarioName: "s", requiredScenarioState: "done" }),
    ]);
    expect(def?.transitions).toEqual([]);
    // The state it answers in still belongs on the chip row.
    expect(def?.states).toContain("done");
  });

  it("keeps a transition whose stub has no id, and says so", () => {
    // A stub without an id cannot be addressed by the by-id routes, so the console cannot link to
    // it. That is a fact about the stub, not a reason to drop the edge it declares.
    const [def] = scenarioDefinitions([
      stub({ scenarioName: "s", requiredScenarioState: "a", newScenarioState: "b" }),
    ]);
    expect(def?.transitions).toEqual([{ from: "a", to: "b", stub: null }]);
  });

  it("returns nothing for an imposter whose stubs the response omitted", () => {
    // `undefined` is "the response did not include them", which is not the same fact as an imposter
    // with no scenarios — the caller renders a different thing for each.
    expect(scenarioDefinitions(undefined)).toEqual([]);
  });
});
