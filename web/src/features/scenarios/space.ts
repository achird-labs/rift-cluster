import type { components } from "../../api/schema.ts";

/**
 * The scenarios/spaces/flow-state screen's read model.
 *
 * Every read here answers about **one space**, and the surface has a shape the screens have to
 * respect rather than paper over: there is no route that lists spaces and no route that lists a
 * flow's state entries. A space is read by naming its flow id; an entry is read by naming its key.
 * So this module models "what the node said about the thing you named", never "everything there
 * is" — and the screen asks for an identifier instead of offering a list it cannot build.
 */

export type ScenarioEntry = components["schemas"]["ScenarioEntry"];
export type FlowStateEntry = components["schemas"]["FlowStateEntry"];
type Stub = components["schemas"]["Stub"];

/**
 * One space as `getSpace` serves it.
 *
 * `stubs` are the space's **own** stubs, not the imposter's — see `SPACE_STUB_CAVEAT`.
 */
export type Space = {
  space: string;
  stubs: Stub[];
  scenarios: ScenarioEntry[];
  numberOfRequests: number;
};

/**
 * The node's answer about a space's scenarios.
 *
 * `unknown` is not `empty`, and this type is where the two are made unrepresentable as one. An
 * imposter whose scenarios could not be read has an *unknown* scenario set; rendering that as an
 * empty table tells an operator their stubs declare no scenarios, which is a different — and
 * confidently wrong — statement about their configuration.
 */
export type ScenarioState =
  | { kind: "scenarios"; flowId: string; scenarios: ScenarioEntry[] }
  | { kind: "unknown"; reason: string };

/** Same discipline for the space read: a space that could not be read is not an empty space. */
export type SpaceState = { kind: "space"; space: Space } | { kind: "unknown"; reason: string };

/**
 * One flow-state entry's read, as the **three** outcomes it actually has.
 *
 * The middle case is the one worth naming. `getFlowStateEntry` documents `404` as "no such entry",
 * so an absent key is an ordinary domain answer rather than a failure — but RFC-002 §8.4 renders
 * `NotBoundToTenant` as `404` as well, and a `404` for "no such imposter" is in the same status.
 * A screen that renders every 404 as "not set" would tell an operator their key is unset when the
 * truth may be that they are reading someone else's imposter. So `absent` states what the status
 * licenses and no more, and `readFlowStateEntry` is the only place that reasoning lives.
 */
export type FlowStateRead =
  | { kind: "value"; entry: FlowStateEntry }
  | { kind: "absent" }
  | { kind: "unknown"; reason: string };

/**
 * The sentence the space's stub table carries, permanently.
 *
 * A space's stubs partition matching per flow id; they are not the imposter's stubs and do not
 * appear in `/imposters/{port}/stubs`. Rendering them in the same table as the imposter's would
 * imply an ownership that does not exist — an operator would reasonably conclude that deleting one
 * here changes what the imposter serves to everyone, and it does not.
 */
export const SPACE_STUB_CAVEAT =
  "Scoped to this flow only. These are not the imposter's stubs — they match alongside them for " +
  "requests that resolve to this space, and they do not appear on the imposter's own stub list.";

/** The node's `listScenarios` body, or an unknown state naming why it could not be believed. */
export function readScenarios(body: unknown): ScenarioState {
  const payload = body as { flowId?: unknown; scenarios?: unknown } | null;
  if (payload === null || typeof payload !== "object") {
    return { kind: "unknown", reason: "this node answered with a body that is not a scenario list" };
  }
  /*
   * `flowId` is required by the contract and is load-bearing on screen: every scenario state is
   * per-flow, so a list rendered without naming the flow it was read under is a set of states
   * attributed to no space in particular. Missing it means the body is not the one the contract
   * describes, which is an unknown read rather than a list to display under a guessed flow.
   */
  if (typeof payload.flowId !== "string") {
    return {
      kind: "unknown",
      reason: "this node answered without naming the flow the states were read under",
    };
  }
  if (!Array.isArray(payload.scenarios)) {
    return { kind: "unknown", reason: "this node answered with a body carrying no scenario list" };
  }
  return {
    kind: "scenarios",
    flowId: payload.flowId,
    scenarios: payload.scenarios as ScenarioEntry[],
  };
}

/** The node's `getSpace` body. Absent collections read as unknown, never as an empty space. */
export function readSpace(body: unknown): SpaceState {
  const payload = body as Partial<Space> | null;
  if (payload === null || typeof payload !== "object") {
    return { kind: "unknown", reason: "this node answered with a body that is not a space" };
  }
  if (!Array.isArray(payload.stubs) || !Array.isArray(payload.scenarios)) {
    return {
      kind: "unknown",
      reason: "this node answered with a body carrying no stub or scenario list",
    };
  }
  // `space` is required by the contract and is checked like the other required fields rather than
  // defaulted to `""`. Defaulting it would be the one shape this module exists to reject, quietly
  // reintroduced in the module that rejects it — harmless only for as long as nobody renders it.
  if (typeof payload.space !== "string") {
    return { kind: "unknown", reason: "this node answered with a space that does not name itself" };
  }
  return {
    kind: "space",
    space: {
      space: payload.space,
      stubs: payload.stubs,
      scenarios: payload.scenarios,
      /*
       * A count this console will not invent. The field is required by the contract, so its
       * absence is a contract violation — but `0` is a *claim* ("nothing reached this space"), and
       * an operator checking whether their system under test hit the mock would read it as an
       * answer. `-1` is the sentinel the renderer maps to "—" rather than to a number.
       */
      numberOfRequests:
        typeof payload.numberOfRequests === "number" ? payload.numberOfRequests : -1,
    },
  };
}

/** Whether a space read carries a count worth rendering as one. */
export function hasRequestCount(space: Space): boolean {
  return space.numberOfRequests >= 0;
}

/**
 * A space holding nothing at all — no stubs, no scenarios.
 *
 * Its own sentence on screen, distinct from a space that could not be read: this one is an answer
 * ("nothing has been scoped to this flow"), and that one is the absence of an answer.
 */
export function isEmptySpace(space: Space): boolean {
  return space.stubs.length === 0 && space.scenarios.length === 0;
}

/** The node's `getFlowStateEntry` body once the status has already been classified as 2xx. */
export function readFlowStateEntry(body: unknown): FlowStateRead {
  const payload = body as Partial<FlowStateEntry> | null;
  if (payload === null || typeof payload !== "object") {
    return {
      kind: "unknown",
      reason: "this node answered with a body that is not a flow-state entry",
    };
  }
  if (typeof payload.key !== "string" || typeof payload.flowId !== "string") {
    return {
      kind: "unknown",
      reason: "this node answered with an entry naming neither its key nor its flow",
    };
  }
  // `value` is deliberately not checked: the contract declares it as **any** JSON including `null`,
  // so `null` is a stored value and not a missing field. Requiring it to be non-null here would
  // report a legitimately-null entry as unreadable.
  return {
    kind: "value",
    entry: { flowId: payload.flowId, key: payload.key, value: payload.value },
  };
}

/**
 * What a `404` on a flow-state entry read licenses the screen to say.
 *
 * Split out so the ambiguity is stated once, in prose, next to the code that depends on it.
 */
export const ABSENT_ENTRY_CAVEAT =
  "The node answered 404. That is how an unset key reads — and also how an imposter that is not " +
  "yours reads (RFC-002 §8.4), so this does not by itself prove the key is unset.";
