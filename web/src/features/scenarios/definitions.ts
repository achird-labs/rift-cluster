import type { components } from "../../api/schema.ts";

type Stub = components["schemas"]["Stub"];

/** One `from --on--> to` edge, and the stub that drives it. */
export type Transition = {
  from: string;
  to: string;
  /** The stub whose match advances the machine. `null` when the stub carries no id. */
  stub: string | null;
};

/** A scenario's finite-state machine, as the imposter's own stubs declare it. */
export type ScenarioDefinition = {
  name: string;
  /** Every state named anywhere in the machine, in first-seen order. */
  states: readonly string[];
  /** The state the machine sits in before any stub has advanced it. */
  initial: string;
  transitions: readonly Transition[];
};

/**
 * The state a scenario is in before anything has moved it.
 *
 * Upstream's convention, not a choice made here: a stub with no `requiredScenarioState` matches
 * whatever state the scenario is in, and a scenario that has never been advanced reads as this.
 * Named rather than inlined because it appears both as a state chip and as the `from` of every
 * unconditional transition, and those two have to agree.
 */
export const INITIAL_STATE = "(initial)";

/**
 * Derive every scenario's FSM from an imposter's stubs.
 *
 * The machine is not published as a document — it is *implied* by the stubs, which each declare the
 * state they require and the state they move to. So this reads the same fields the match gate reads
 * (`scenarioName`, `requiredScenarioState`, `newScenarioState`) and reassembles the graph they
 * describe.
 *
 * That makes it a derivation rather than a guess, and the distinction matters: every edge here
 * corresponds to a stub the operator can open, which is why `Transition.stub` carries the id. A
 * transition with nothing to point at would be this module inventing structure.
 *
 * Stubs that name no scenario are skipped — they are ordinary stubs, not part of any machine.
 * Order is first-seen throughout, because match order is the imposter's own and re-sorting states
 * alphabetically would hide which one the traffic reaches first.
 */
export function scenarioDefinitions(stubs: readonly Stub[] | undefined): ScenarioDefinition[] {
  if (stubs === undefined) return [];

  const byName = new Map<string, { states: string[]; transitions: Transition[] }>();

  for (const stub of stubs) {
    const name = stub.scenarioName;
    if (typeof name !== "string" || name === "") continue;

    let entry = byName.get(name);
    if (entry === undefined) {
      entry = { states: [INITIAL_STATE], transitions: [] };
      byName.set(name, entry);
    }

    const from = typeof stub.requiredScenarioState === "string" ? stub.requiredScenarioState : INITIAL_STATE;
    const to = typeof stub.newScenarioState === "string" ? stub.newScenarioState : from;

    for (const state of [from, to]) {
      if (!entry.states.includes(state)) entry.states.push(state);
    }

    /*
     * A stub that requires a state and moves to none is a *terminal* match, not an edge: it answers
     * in that state and leaves the machine where it was. Recording it as `from -> from` would draw a
     * self-loop the operator would read as a cycle. It still contributes its state to the chip row
     * above, which is where it belongs.
     */
    if (to === from) continue;

    entry.transitions.push({
      from,
      to,
      stub: typeof stub.id === "string" ? stub.id : null,
    });
  }

  return [...byName.entries()].map(([name, entry]) => ({
    name,
    states: entry.states,
    initial: INITIAL_STATE,
    transitions: entry.transitions,
  }));
}
